%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_db_core).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Read-side substrate primitive (`MST_DB_DESIGN.md`).

Composes a projection adapter (any persistent KV implementing
`bondy_oplog_projection_adapter`), a cache adapter (any read cache
implementing `bondy_oplog_cache_adapter`), and a per-shard overlay
(`bondy_oplog_db_overlay`) into a single read API parameterised by
the namespace's fold strategy.

The read path is:

1. **Cache hit** — return immediately. Sub-microsecond.
2. **Cache miss** — read the projection cell, decode the frame,
   merge overlay events whose HLC is newer than the cell's HLC,
   apply the fold, populate the cache, return.

This module is read-only; writes flow through `bondy_oplog_instance`
and reach the projection via the applier. Writers can call
`write_through/4` after appending an event to keep the hot cache
coherent.

See `bondy_db_core_registry` for how shard handles are published. The
substrate does not own shard lifecycles — owners (writers, applier,
test setups) register the four-tuple
`{cache_handle, projection_handle, overlay, fold_module}` for each
`(namespace, index, shard)` they manage.
""").

-export([read/3]).
-export([read/4]).
-export([read_batch/2]).
-export([range/4]).
-export([read_at_hlc/3]).
-export([write_through/4]).
-export([shard_for/3]).
-export([ensure_fresh/2]).
-export([ensure_fresh_for_keys/2]).
-export([freshness/1]).
-export([subscribe/2]).
-export([unsubscribe/1]).
-export([publish/4]).

-export_type([read_opts/0]).
-export_type([read_result/0]).
-export_type([read_batch_opts/0]).
-export_type([read_batch_result/0]).
-export_type([batch_key/0]).
-export_type([consistency/0]).
-export_type([range_opts/0]).
-export_type([range_result/0]).
-export_type([range_spec/0]).

-type read_opts()   :: map().
-type read_result() :: {Value :: term(), Hlc :: bondy_oplog_hlc:hlc()}
                     | undefined.

-type consistency()       :: eventual | causal | snapshot.
-type batch_key()         :: {atom(), atom(), term()}.
-type read_batch_opts()   :: #{
    fence              => bondy_oplog_hlc:hlc(),
    max_lag            => non_neg_integer() | infinity,
    require_skew_below => non_neg_integer(),
    consistency        => consistency()
}.
-type read_batch_result() :: #{batch_key() := read_result()}.

-type range_spec()  :: {Low :: term(), High :: term()}.
-type range_opts()  :: #{
    limit           => pos_integer(),
    direction       => asc | desc,
    include_overlay => boolean(),
    fence           => bondy_oplog_hlc:hlc() | infinity,
    shard           => non_neg_integer()
}.
-type range_row()    :: {Key :: term(), Value :: term(), Hlc :: bondy_oplog_hlc:hlc()}.
-type range_result() :: [range_row()].

%% =============================================================================
%% API
%% =============================================================================

-spec read(
    Namespace :: atom(),
    Index :: atom(),
    Key :: term()
) -> read_result() | {error, term()}.

read(NS, Index, Key) ->
    read(NS, Index, Key, #{}).


-spec read(
    Namespace :: atom(),
    Index :: atom(),
    Key :: term(),
    Opts :: read_opts()
) -> read_result() | {error, term()}.

read(NS, Index, Key, _Opts) ->
    case resolve_shard(NS, Index, Key) of
        {ok, Entry} ->
            {_NS, _Idx, Shard} = bondy_db_core_registry:entry_key(Entry),
            T0 = erlang:monotonic_time(microsecond),
            {Result, Source} = do_read_traced(Entry, Key),
            DurUs = erlang:monotonic_time(microsecond) - T0,
            emit_read_event(NS, Index, Shard, Source, DurUs, Result),
            Result;
        {error, _} = Err ->
            Err
    end.


-doc("""
Coalesced multi-cell read with optional fence and skew constraints
(`MST_DB_DESIGN.md` §8).

`Reads` is a list of `{Namespace, Index, Key}` triples. The result is a
map keyed by the same triple, mapping to the per-cell `read_result()`.

## Opts

- `fence` — HLC defining the as-of point. Overlay events with
  `HLC > fence` are excluded. Projection cells whose `last_modified_hlc`
  has already advanced past the fence are returned as-is — the caller
  observes the higher HLC in the result. Defaults to "no fence" (no
  upper bound on overlay events).
- `max_lag` — bound on the staleness window for the freshness predicate.
  Defaults to `infinity`. Finite values invoke `ensure_fresh/2` against
  the per-shard AE counters; on staleness the batch returns
  `{error, {stale, [Namespace]}}`.
- `require_skew_below` — if set, the batch returns only if the spread
  between the highest and lowest HLC in the result is below the bound
  (millisecond units, derived from the HLC's physical part). On breach,
  returns `{error, {skew_too_large, Skew, Bound}}`.
- `consistency` — `eventual` (no freshness check; cheapest), `causal`
  (enforce `max_lag`), or `snapshot` (enforce `max_lag` + skew bound at
  `max_lag / 2`).

Returns `{ok, Results, Fence}` on success, where `Fence` is the resolved
fence (caller-supplied or substrate-chosen). On any error returns
`{error, _}`.
""").
-spec read_batch([batch_key()], read_batch_opts()) ->
    {ok, read_batch_result(), bondy_oplog_hlc:hlc()} | {error, term()}.

read_batch(Reads, Opts) when is_list(Reads), is_map(Opts) ->
    Consistency = maps:get(consistency, Opts, eventual),
    Fence = maps:get(fence, Opts, infinity),
    {EffectiveMaxLag, EffectiveSkew} = apply_consistency(Consistency, Opts),
    T0 = erlang:monotonic_time(microsecond),
    %% Per-namespace policy (§15): `cp` namespaces refuse `eventual`
    %% reads to prevent unfenced staleness. `causal` and `snapshot`
    %% are allowed under either class.
    Result = case check_consistency_class(Reads, Consistency) of
        {error, _} = ClassErr ->
            ClassErr;
        ok ->
            %% Per-key freshness: only the shards the batch actually
            %% touches participate in the staleness check. A cold
            %% shard elsewhere in the namespace cannot fail a batch
            %% that does not read from it.
            case ensure_fresh_predicate_for_keys(Reads, EffectiveMaxLag) of
                {error, _} = Err ->
                    Err;
                ok ->
                    Results = compute_batch(Reads, Fence),
                    case check_skew(Results, EffectiveSkew) of
                        ok ->
                            {ok, Results, Fence};
                        {error, _} = SkewErr ->
                            SkewErr
                    end
            end
    end,
    DurUs = erlang:monotonic_time(microsecond) - T0,
    emit_read_batch_event(Reads, Fence, Result, DurUs),
    Result.


-doc("""
Refresh the cache for `Key` after an event is accepted by the writer.

If the key is currently cached, the writer's fold is applied to the
cached value and the new `{Value, Hlc}` is written back. If the key
is not cached, this is a no-op — the next read will populate it from
the projection + overlay.

This is the §5.1 write-through path. Callers are the writer process
(after a local append) and the applier (after a remote enqueue) —
both run on a per-shard ownership discipline so concurrent writers
for the same key do not exist.
""").
-spec write_through(
    Namespace :: atom(),
    Index :: atom(),
    Key :: term(),
    Event :: bondy_oplog_event:t()
) -> ok | {error, term()}.

write_through(NS, Index, Key, Event) ->
    case resolve_shard(NS, Index, Key) of
        {ok, Entry} ->
            do_write_through(Entry, Key, Event);
        {error, _} = Err ->
            Err
    end.


-doc("""
Shard selector. Uses `phash2/2` over the substrate's full shard count
for the `(namespace, index)` pair. Returns `{error, no_shards}` if no
shards are registered for `(NS, Index)`.
""").
-spec shard_for(atom(), atom(), term()) ->
    {ok, non_neg_integer()} | {error, no_shards}.

shard_for(NS, Index, Key) ->
    case bondy_db_core_registry:shard_count(NS, Index) of
        {ok, Count} -> {ok, erlang:phash2(Key, Count)};
        not_found   -> {error, no_shards}
    end.


-doc("""
Single-shard range scan (`MST_DB_DESIGN.md` §9).

Returns all cells whose key lies in `[Low, High)` within one shard,
merging the projection's materialised state with any pending overlay
events. The result is sorted by key (ascending unless `direction => desc`)
and trimmed to `limit` rows.

## Shard selection

By default the shard is inferred from `Low` via `shard_for/3` —
appropriate when the caller knows all keys in the range hash to the
same shard (e.g., when the range is itself derived from a shard key).
For hash-sharded namespaces where the range spans multiple shards, the
caller must scatter the call across shards and merge results.

Callers can override the shard with `shard => N` in the opts.

## Opts

- `limit` — max rows in the result (default `1000`).
- `direction` — `asc` (default) or `desc`.
- `include_overlay` — set `false` to exclude pending events (default `true`).
- `fence` — HLC ceiling for overlay events (default `infinity` — no fence).
- `shard` — explicit shard override (default: `phash2(Low, ShardCount)`).

## Result

`{ok, [{Key, Value, Hlc}]}`. The result is **single-shot**: trimmed to
`limit` rows. Pagination is not supported by the substrate — overlay
merging interacts poorly with stateful continuation and the current
consumer base does not need it. Callers that need more must either
raise `limit` or scatter via `shard => N` and merge themselves.
""").
-spec range(atom(), atom(), range_spec(), range_opts()) ->
    {ok, range_result()} | {error, term()}.

range(NS, Index, {Low, High}, Opts) when is_map(Opts) ->
    case resolve_shard_for_range(NS, Index, Low, Opts) of
        {ok, Entry} ->
            {_NS, _Idx, Shard} = bondy_db_core_registry:entry_key(Entry),
            T0 = erlang:monotonic_time(microsecond),
            Result = do_range(Entry, Low, High, Opts),
            DurUs = erlang:monotonic_time(microsecond) - T0,
            emit_range_event(NS, Index, Shard, Result, DurUs),
            Result;
        {error, _} = Err ->
            Err
    end.


-doc("""
Point-in-time read against the primary index (`MST_DB_DESIGN.md` §10).

Returns the cell value as of HLC `T`. The projection is consulted first:

- If the projection's `last_modified_hlc` is `=< T`, overlay events with
  HLC in `(ProjHlc, T]` are folded onto the projection state and the
  result is returned with its computed HLC.
- If the projection has already advanced past `T`, the substrate refuses
  with `{error, {historical_read_unavailable, ProjHlc, T}}`. True MVCC
  retention is opt-in per namespace and deferred.
- If neither the projection nor any overlay event matches, the fold's
  initial value is returned at HLC `0`.

This API is restricted to the primary index; secondary-index historical
reads require richer machinery (per-secondary version retention) and are
out of scope.
""").
-spec read_at_hlc(
    Namespace :: atom(),
    Key :: term(),
    T :: bondy_oplog_hlc:hlc()
) -> {ok, Value :: term(), Hlc :: bondy_oplog_hlc:hlc()}
   | {error, term()}.

read_at_hlc(NS, Key, T) when is_integer(T), T >= 0 ->
    T0 = erlang:monotonic_time(microsecond),
    Result = case resolve_shard(NS, primary, Key) of
        {ok, Entry} ->
            do_read_at_hlc(Entry, Key, T);
        {error, _} = Err ->
            Err
    end,
    DurUs = erlang:monotonic_time(microsecond) - T0,
    emit_read_at_hlc_event(NS, Result, DurUs),
    Result.


-doc("""
Freshness predicate (`MST_DB_DESIGN.md` §11). Returns `ok` iff every
shard of every supplied namespace has had a `bump_ae/3` within
`MaxLag` milliseconds of "now".

Wait-free: each per-shard check is a single `atomics:get/2` plus a
subtraction. With `MaxLag = infinity`, returns `ok` immediately.

`{stale, NSs}` lists the namespaces with at least one shard whose
last-AE timestamp is older than the bound. The list is sorted and
deduplicated.

Namespaces with no registered shards are treated as vacuously fresh —
there are no shards to fail the check. Callers that need
"unknown namespace = stale" semantics should consult
`bondy_db_core_registry:namespaces/0` before calling.
""").
-spec ensure_fresh([atom()], non_neg_integer() | infinity) ->
    ok | {stale, [atom()]}.

ensure_fresh(_NSs, infinity) ->
    ok;
ensure_fresh(NSs, MaxLag)
        when is_list(NSs), is_integer(MaxLag), MaxLag >= 0 ->
    T0 = erlang:monotonic_time(microsecond),
    Now = erlang:monotonic_time(millisecond),
    Stale = lists:usort(
        [NS
         || NS <- NSs,
            Entry <- bondy_db_core_registry:shards_for(NS),
            (Now - atomics:get(
                bondy_db_core_registry:entry_ae_atomics(Entry), 1)) > MaxLag]),
    DurUs = erlang:monotonic_time(microsecond) - T0,
    emit_ensure_fresh_event(length(NSs), length(Stale), DurUs),
    case Stale of
        [] -> ok;
        _  -> {stale, Stale}
    end.


-doc("""
Like `ensure_fresh/2` but only inspects the shards actually touched by
the supplied keys. Cheaper when the read set covers a small fraction
of a hash-sharded namespace.

Returns `ok` or `{stale, [Namespace]}` — same shape as `ensure_fresh/2`.
""").
-spec ensure_fresh_for_keys(
    [{atom(), atom(), term()}],
    non_neg_integer() | infinity
) -> ok | {stale, [atom()]}.

ensure_fresh_for_keys(_Reads, infinity) ->
    ok;
ensure_fresh_for_keys(Reads, MaxLag)
        when is_list(Reads), is_integer(MaxLag), MaxLag >= 0 ->
    T0 = erlang:monotonic_time(microsecond),
    Now = erlang:monotonic_time(millisecond),
    Touched = touched_shards(Reads),
    Stale = lists:usort(
        [NS
         || {NS, Ae} <- Touched,
            (Now - atomics:get(Ae, 1)) > MaxLag]),
    DurUs = erlang:monotonic_time(microsecond) - T0,
    %% `namespaces_checked` here is the count of distinct shards we
    %% actually inspected — not the namespace count — because
    %% `ensure_fresh_for_keys/2` operates per-shard, not per-namespace.
    %% A consumer comparing the two events should treat this number as
    %% "shards inspected" semantically.
    emit_ensure_fresh_event(length(Touched), length(Stale), DurUs),
    case Stale of
        [] -> ok;
        _  -> {stale, Stale}
    end.


-doc("""
Return per-shard freshness lag for the namespace as a map keyed by
`{Index, Shard}` with values in milliseconds (`Now - last_ae_at`).
A never-bumped shard returns the time since the monotonic epoch — a
large positive number — which is intentional: it surfaces the
unbumped state rather than hiding it as `0`.

Returns `#{}` for an unknown namespace.
""").
-spec freshness(atom()) ->
    #{{atom(), non_neg_integer()} := integer()}.

freshness(NS) when is_atom(NS) ->
    Now = erlang:monotonic_time(millisecond),
    maps:from_list(
        [begin
             {_NS, Index, Shard} = bondy_db_core_registry:entry_key(Entry),
             Ae = bondy_db_core_registry:entry_ae_atomics(Entry),
             {{Index, Shard}, Now - atomics:get(Ae, 1)}
         end
         || Entry <- bondy_db_core_registry:shards_for(NS)]
    ).


-doc("""
Subscribe the caller to events on `Namespace` matching `Pattern`
(`MST_DB_DESIGN.md` §12). Returns a `SubRef` the caller can pass to
`unsubscribe/1`. Subscriptions are local-only (do not cross nodes).

Subscribers receive

```erlang
{bondy_db_core_event, Namespace, Key, Hlc, Operation}
```

messages whenever `publish/4` is invoked with a matching `(NS, Key)`
pair. Patterns: `all`, `{prefix, P}`, `{match, F}`, or `{exact, T}`.
The pattern type is closed — bare terms are not accepted. See
`bondy_db_core_dispatcher` for the reference implementation.

If the subscriber process exits, its subscription is dropped
automatically via a monitor held by the dispatcher.
""").
-spec subscribe(atom(), bondy_db_core_dispatcher:pattern()) ->
    {ok, reference()}.

subscribe(NS, Pattern) ->
    bondy_db_core_dispatcher:subscribe(NS, Pattern).


-spec unsubscribe(reference()) -> ok.

unsubscribe(SubRef) ->
    bondy_db_core_dispatcher:unsubscribe(SubRef).


-doc("""
Publish a post-projection-commit event. Wired by the applier in a
follow-on PR; exposed here as the public publishing surface so the
reference dispatcher can be exercised without going through the
applier.

Delivery is best-effort: every subscriber whose pattern matches
receives the message via `erlang:send/2`. The walk runs in the caller
process (no gen_server round-trip).
""").
-spec publish(atom(), term(), bondy_oplog_hlc:hlc(), term()) -> ok.

publish(NS, Key, Hlc, Op) ->
    bondy_db_core_dispatcher:publish(NS, Key, Hlc, Op).


%% =============================================================================
%% Read path
%% =============================================================================

resolve_shard(NS, Index, Key) ->
    case shard_for(NS, Index, Key) of
        {ok, Shard} ->
            case bondy_db_core_registry:lookup(NS, Index, Shard) of
                {ok, Entry} -> {ok, Entry};
                not_found   -> {error, shard_not_registered}
            end;
        {error, _} = Err ->
            Err
    end.


do_read_traced(Entry, Key) ->
    CA = bondy_db_core_registry:entry_cache_adapter(Entry),
    CH = bondy_db_core_registry:entry_cache_handle(Entry),
    case CA:get(CH, Key) of
        {ok, {Value, Hlc}} ->
            {{Value, Hlc}, cache};
        not_found ->
            slow_read_traced(Entry, Key)
    end.


slow_read_traced(Entry, Key) ->
    Strategy = bondy_db_core_registry:entry_fold_module(Entry),
    {ProjValue, ProjHlc, ProjHadFrame} = read_projection(Entry, Key, Strategy),
    OverlayEvents = read_overlay(Entry, Key, ProjHlc),
    OverlayApplied = OverlayEvents =/= [],
    {Value, Hlc} = fold_events(Strategy, ProjValue, ProjHlc, OverlayEvents),
    Source = source_for(ProjHadFrame, OverlayApplied),
    case Value of
        undefined ->
            {undefined, Source};
        _ ->
            CA = bondy_db_core_registry:entry_cache_adapter(Entry),
            CH = bondy_db_core_registry:entry_cache_handle(Entry),
            ok = CA:put(CH, Key, {Value, Hlc}),
            {{Value, Hlc}, Source}
    end.


%% `projection` covers both "projection had a frame, no overlay" and the
%% degenerate "neither projection nor overlay" case — the projection was
%% the last source consulted in either path.
source_for(true,  false) -> projection;
source_for(true,  true)  -> projection_with_overlay;
source_for(false, true)  -> overlay_only;
source_for(false, false) -> projection.


read_projection(Entry, Key, Strategy) ->
    PA = bondy_db_core_registry:entry_projection_adapter(Entry),
    PH = bondy_db_core_registry:entry_projection_handle(Entry),
    case PA:get(PH, Key) of
        not_found ->
            {bondy_oplog_fold:initial_value(Strategy), 0, false};
        {ok, Frame} ->
            {Hlc, Body} = bondy_oplog_cell_frame:decode(Frame),
            {bondy_oplog_fold:decode_state(Strategy, Body), Hlc, true}
    end.


read_overlay(Entry, Key, AfterHlc) ->
    case bondy_db_core_registry:entry_overlay(Entry) of
        undefined -> [];
        Tab -> bondy_oplog_db_overlay:events_for(Tab, Key, AfterHlc)
    end.


fold_events(_Strategy, Value, Hlc, []) ->
    {Value, Hlc};
fold_events(Strategy, Value0, _Hlc0, Events) ->
    NewValue = lists:foldl(
        fun(Event, Acc) ->
            Op = bondy_oplog_event:op(Event),
            bondy_oplog_fold:apply_event(Strategy, Acc, Op)
        end,
        Value0,
        Events
    ),
    NewHlc = bondy_oplog_fold:hlc(Strategy, NewValue),
    {NewValue, NewHlc}.


%% =============================================================================
%% Batch read path
%% =============================================================================

%% `consistency` is a preset that adjusts which checks fire; explicit
%% per-call options (`max_lag`, `require_skew_below`) layer on top. For
%% `eventual`, freshness is always skipped (the whole point) but an
%% explicit skew bound is still honoured.
apply_consistency(eventual, Opts) ->
    {infinity, maps:get(require_skew_below, Opts, undefined)};
apply_consistency(causal, Opts) ->
    {maps:get(max_lag, Opts, infinity),
     maps:get(require_skew_below, Opts, undefined)};
apply_consistency(snapshot, Opts) ->
    MaxLag = maps:get(max_lag, Opts, infinity),
    Bound = case maps:get(require_skew_below, Opts, undefined) of
        undefined when is_integer(MaxLag) -> MaxLag div 2;
        undefined -> undefined;
        Explicit -> Explicit
    end,
    {MaxLag, Bound}.


%% Per-key freshness predicate: defers to `ensure_fresh_for_keys/2`,
%% then maps the `{stale, _}` return shape to `{error, _}` for the
%% batch caller.
ensure_fresh_predicate_for_keys(_Reads, infinity) ->
    ok;
ensure_fresh_predicate_for_keys(Reads, MaxLag) ->
    case ensure_fresh_for_keys(Reads, MaxLag) of
        ok               -> ok;
        {stale, _} = Err -> {error, Err}
    end.


%% For each Read, resolve (NS, Index, Key) → (NS, AeAtomics). Dedupes by
%% `(NS, Index, Shard)` BEFORE the per-shard registry lookup so a batch
%% with many keys hashing to the same shard pays one lookup, not N.
%% Reads that map to an unregistered shard are skipped — the per-cell
%% `compute_batch` surfaces those as `{error, no_shards}` and they are
%% not freshness questions.
touched_shards(Reads) ->
    ShardSet = lists:foldl(
        fun({NS, Index, Key}, Acc) ->
            case shard_for(NS, Index, Key) of
                {ok, Shard} -> Acc#{{NS, Index, Shard} => []};
                {error, _}  -> Acc
            end
        end,
        #{},
        Reads
    ),
    lists:foldl(
        fun({NS, Index, Shard}, Acc) ->
            case bondy_db_core_registry:lookup(NS, Index, Shard) of
                {ok, Entry} ->
                    Ae = bondy_db_core_registry:entry_ae_atomics(Entry),
                    [{NS, Ae} | Acc];
                not_found ->
                    Acc
            end
        end,
        [],
        maps:keys(ShardSet)
    ).


compute_batch(Reads, Fence) ->
    maps:from_list([
        {{NS, Idx, Key}, read_at_fence(NS, Idx, Key, Fence)}
        || {NS, Idx, Key} <- Reads
    ]).


read_at_fence(NS, Index, Key, Fence) ->
    case resolve_shard(NS, Index, Key) of
        {ok, Entry} ->
            fenced_read(Entry, Key, Fence);
        {error, _} = Err ->
            Err
    end.


fenced_read(Entry, Key, Fence) ->
    %% Fenced reads bypass the cache: the cache holds the "now" value,
    %% not the as-of-fence value. The slow path always runs.
    Strategy = bondy_db_core_registry:entry_fold_module(Entry),
    {ProjValue, ProjHlc, _ProjHadFrame} = read_projection(Entry, Key, Strategy),
    OverlayEvents = fenced_overlay(Entry, Key, ProjHlc, Fence),
    {Value, Hlc} = fold_events(Strategy, ProjValue, ProjHlc, OverlayEvents),
    case Value of
        undefined -> undefined;
        _ -> {Value, Hlc}
    end.


fenced_overlay(Entry, Key, AfterHlc, infinity) ->
    %% No fence: behave like a regular slow read.
    read_overlay(Entry, Key, AfterHlc);
fenced_overlay(Entry, Key, AfterHlc, Fence) ->
    case bondy_db_core_registry:entry_overlay(Entry) of
        undefined -> [];
        Tab -> bondy_oplog_db_overlay:events_for_window(Tab, Key, AfterHlc, Fence)
    end.


%% Skew = max(physical) - min(physical) across the result HLCs.
check_skew(_Results, undefined) ->
    ok;
check_skew(_Results, infinity) ->
    ok;
check_skew(Results, Bound) when is_integer(Bound) ->
    Hlcs = collect_hlcs(maps:values(Results)),
    case Hlcs of
        [] -> ok;
        _ ->
            Physicals = [physical(H) || H <- Hlcs],
            Skew = lists:max(Physicals) - lists:min(Physicals),
            case Skew =< Bound of
                true -> ok;
                false -> {error, {skew_too_large, Skew, Bound}}
            end
    end.


collect_hlcs(Values) ->
    lists:foldl(
        fun
            ({_, Hlc}, Acc) when is_integer(Hlc) -> [Hlc | Acc];
            (_, Acc) -> Acc
        end,
        [],
        Values
    ).


physical(Hlc) ->
    {Phys, _Log} = bondy_oplog_hlc:decode(Hlc),
    Phys.


%% =============================================================================
%% Range
%% =============================================================================

resolve_shard_for_range(NS, Index, Low, Opts) ->
    case maps:get(shard, Opts, undefined) of
        undefined ->
            case shard_for(NS, Index, Low) of
                {ok, Shard} -> registry_lookup(NS, Index, Shard);
                {error, _} = Err -> Err
            end;
        Shard when is_integer(Shard) ->
            registry_lookup(NS, Index, Shard)
    end.


registry_lookup(NS, Index, Shard) ->
    case bondy_db_core_registry:lookup(NS, Index, Shard) of
        {ok, Entry} -> {ok, Entry};
        not_found -> {error, shard_not_registered}
    end.


do_range(Entry, Low, High, Opts) ->
    Limit          = maps:get(limit, Opts, 1000),
    Direction      = maps:get(direction, Opts, asc),
    IncludeOverlay = maps:get(include_overlay, Opts, true),
    Fence          = maps:get(fence, Opts, infinity),
    Strategy       = bondy_db_core_registry:entry_fold_module(Entry),
    PA             = bondy_db_core_registry:entry_projection_adapter(Entry),
    PH             = bondy_db_core_registry:entry_projection_handle(Entry),

    case PA:range(PH, Low, High, Opts) of
        {ok, ProjEntries} ->
            OverlayEntries = overlay_for_range(Entry, Low, High,
                                               Fence, IncludeOverlay),
            Merged = merge_range(Strategy, ProjEntries, OverlayEntries),
            Ordered = case Direction of
                asc  -> Merged;
                desc -> lists:reverse(Merged)
            end,
            {ok, lists:sublist(Ordered, Limit)};
        {error, _} = Err ->
            Err
    end.


overlay_for_range(_Entry, _Low, _High, _Fence, false) ->
    [];
overlay_for_range(Entry, Low, High, Fence, true) ->
    case bondy_db_core_registry:entry_overlay(Entry) of
        undefined -> [];
        Tab       -> bondy_oplog_db_overlay:range_window(Tab, Low, High, Fence)
    end.


merge_range(Strategy, ProjEntries, OverlayEntries) ->
    %% Group: KeyMap = #{Key => {ProjFrame | undefined, [Event]}}
    Init = maps:from_list([{K, {F, []}} || {K, F} <- ProjEntries]),
    Grouped = lists:foldl(
        fun({K, E}, M) ->
            case maps:get(K, M, undefined) of
                undefined        -> M#{K => {undefined, [E]}};
                {Frame, Events}  -> M#{K => {Frame, [E | Events]}}
            end
        end,
        Init,
        OverlayEntries
    ),
    Cells = lists:keysort(1, maps:to_list(Grouped)),
    lists:filtermap(
        fun({K, {Frame, Events}}) ->
            case emit_range_cell(Strategy, Frame, lists:reverse(Events)) of
                undefined  -> false;
                {V, H}     -> {true, {K, V, H}}
            end
        end,
        Cells
    ).


emit_range_cell(Strategy, Frame, Events) ->
    {ProjValue, ProjHlc} = case Frame of
        undefined ->
            {bondy_oplog_fold:initial_value(Strategy), 0};
        Bin when is_binary(Bin) ->
            {H, Body} = bondy_oplog_cell_frame:decode(Bin),
            {bondy_oplog_fold:decode_state(Strategy, Body), H}
    end,
    %% Per-cell: only events newer than the projection's HLC apply.
    Applicable = [
        E || E <- Events,
             bondy_oplog_event:key_hlc(bondy_oplog_event:key(E)) > ProjHlc
    ],
    {Value, Hlc} = fold_events(Strategy, ProjValue, ProjHlc, Applicable),
    case Value of
        undefined -> undefined;
        _         -> {Value, Hlc}
    end.


%% =============================================================================
%% Point-in-time read
%% =============================================================================

do_read_at_hlc(Entry, Key, T) ->
    Strategy = bondy_db_core_registry:entry_fold_module(Entry),
    {ProjValue, ProjHlc, _ProjHadFrame} = read_projection(Entry, Key, Strategy),
    case ProjHlc > T of
        true ->
            {error, {historical_read_unavailable, ProjHlc, T}};
        false ->
            OverlayEvents = fenced_overlay(Entry, Key, ProjHlc, T),
            {Value, Hlc} = fold_events(Strategy, ProjValue, ProjHlc, OverlayEvents),
            case Value of
                undefined ->
                    {ok, bondy_oplog_fold:initial_value(Strategy), 0};
                _ ->
                    {ok, Value, Hlc}
            end
    end.


%% =============================================================================
%% Write-through
%% =============================================================================

%% Walk distinct namespaces in the batch; the first one that declares
%% `cp` and conflicts with an `eventual` request short-circuits with the
%% violation. `causal` and `snapshot` always pass — they invoke the
%% freshness predicate which is sufficient for `cp`'s guarantee.
check_consistency_class(_Reads, Consistency)
        when Consistency =/= eventual ->
    ok;
check_consistency_class(Reads, eventual) ->
    NSs = lists:usort([NS || {NS, _Idx, _K} <- Reads]),
    case lists:dropwhile(
        fun(NS) -> bondy_db_core_registry:consistency_class(NS) =/= cp end,
        NSs
    ) of
        [] -> ok;
        [CpNs | _] ->
            {error, {consistency_class_violation, CpNs, cp, eventual}}
    end.


%% =============================================================================
%% Telemetry (`MST_DB_DESIGN.md` §16)
%% =============================================================================

emit_read_event(NS, Index, Shard, Source, DurUs, Result) ->
    {Hit, ValueBytes} = case Result of
        {Value, _Hlc} when Value =/= undefined ->
            {Source =:= cache, erlang:external_size(Value)};
        _ ->
            {false, 0}
    end,
    telemetry:execute(
        [bondy_db_core, read],
        #{duration_us => DurUs, hit => Hit, value_bytes => ValueBytes},
        #{namespace => NS, index => Index, shard => Shard, source => Source}
    ).


emit_read_batch_event(Reads, Fence, Result, DurUs) ->
    NSs = lists:usort([NS || {NS, _Idx, _K} <- Reads]),
    {ReadCount, TotalBytes, SkewMs} = batch_summary(Result),
    telemetry:execute(
        [bondy_db_core, read_batch],
        #{duration_us => DurUs,
          read_count => ReadCount,
          total_bytes => TotalBytes,
          skew_ms => SkewMs},
        #{namespaces => NSs, fence_hlc => Fence}
    ).


%% On error we report the request shape but cannot describe values: the
%% batch was rejected before any cells were read. `read_count` falls back
%% to the requested length so the event still reflects what was asked.
batch_summary({ok, Results, _Fence}) ->
    Values = maps:values(Results),
    Bytes = lists:foldl(
        fun
            ({V, _H}, Acc) when V =/= undefined -> Acc + erlang:external_size(V);
            (_, Acc) -> Acc
        end,
        0,
        Values
    ),
    Hlcs = collect_hlcs(Values),
    Skew = case Hlcs of
        [] -> 0;
        _ ->
            Phys = [physical(H) || H <- Hlcs],
            lists:max(Phys) - lists:min(Phys)
    end,
    {length(Values), Bytes, Skew};
batch_summary(_Err) ->
    {0, 0, 0}.


emit_range_event(NS, Index, Shard, Result, DurUs) ->
    {Entries, Bytes} = case Result of
        {ok, Rows} ->
            B = lists:foldl(
                fun({_K, V, _H}, Acc) -> Acc + erlang:external_size(V) end,
                0,
                Rows
            ),
            {length(Rows), B};
        _ ->
            {0, 0}
    end,
    telemetry:execute(
        [bondy_db_core, range],
        #{duration_us => DurUs,
          entries_returned => Entries,
          scanned_bytes => Bytes},
        #{namespace => NS, index => Index, shard => Shard}
    ).


emit_read_at_hlc_event(NS, Result, DurUs) ->
    {Refused, Reason} = case Result of
        {ok, _, _}                                 -> {false, undefined};
        {error, {historical_read_unavailable, _, _}} ->
            {true, historical_read_unavailable};
        {error, {Tag, _, _}}                       -> {true, Tag};
        {error, {Tag, _}}                          -> {true, Tag};
        {error, Tag} when is_atom(Tag)             -> {true, Tag};
        {error, _}                                 -> {true, unknown};
        _                                          -> {true, unknown}
    end,
    telemetry:execute(
        [bondy_db_core, read_at_hlc],
        #{duration_us => DurUs, refused => Refused},
        #{namespace => NS, refusal_reason => Reason}
    ).


emit_ensure_fresh_event(NSsChecked, StaleCount, DurUs) ->
    telemetry:execute(
        [bondy_db_core, ensure_fresh],
        #{duration_us => DurUs,
          namespaces_checked => NSsChecked,
          stale_count => StaleCount},
        #{}
    ).


do_write_through(Entry, Key, Event) ->
    CA = bondy_db_core_registry:entry_cache_adapter(Entry),
    CH = bondy_db_core_registry:entry_cache_handle(Entry),
    case CA:get(CH, Key) of
        not_found ->
            ok;
        {ok, {OldValue, _OldHlc}} ->
            Strategy = bondy_db_core_registry:entry_fold_module(Entry),
            Op = bondy_oplog_event:op(Event),
            case bondy_oplog_fold:apply_event(Strategy, OldValue, Op) of
                undefined ->
                    %% Fold collapsed the cell to absent (e.g., delete-
                    %% style strategy). Invalidate the cache rather than
                    %% calling `hlc/2` on `undefined`, which would crash.
                    ok = CA:delete(CH, Key);
                NewValue ->
                    NewHlc = bondy_oplog_fold:hlc(Strategy, NewValue),
                    ok = CA:put(CH, Key, {NewValue, NewHlc})
            end
    end.
