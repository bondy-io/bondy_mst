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
implementing `bondy_oplog_cache_adapter`), and an optional overlay
(`bondy_oplog_db_overlay`) into a single read API parameterised by
the namespace's fold strategy.

## Address dimensions

Cells are addressed by **four** dimensions:

- **NS** — a config-group identifier; the registry maps `(NS, Index, Shard)`
  to `{fold_module, shard_count, projection_adapter, cache_adapter, ...}`.
  NS does not appear in the data itself; it is purely routing/config.
- **Index** — `primary` for the cell store, or one of the secondary
  indexes (different projections of the same events).
- **Bucket** — the storage-layer partition (leveled/Riak-native). Bucket
  is a call-time parameter; many buckets share one `(NS, Index, Shard)`
  registry entry. Adding or removing a bucket is data-plane work, not
  registry work.
- **Key** — the cell identifier inside `(Bucket)`.

Shard is derived as `phash2({Bucket, Key}, shard_count(NS, Index))`
(Riak-style composite hashing) so a single bucket spreads evenly across
the NS's shards.

## Read path

1. **Cache hit** — `Cache:get(Handle, Bucket, Key)` returns immediately.
2. **Cache miss** — read the projection cell, decode the frame,
   merge overlay events whose HLC is newer than the cell's HLC,
   apply the fold, populate the cache, return.

This module is read-only; writes flow through `bondy_oplog_instance`
and reach the projection via the applier. Writers can call
`write_through/5` after appending an event to keep the hot cache
coherent.

See `bondy_db_core_registry` for how shard handles are published. The
substrate does not own shard lifecycles — owners (writers, applier,
test setups) register the four-tuple `{cache_handle, projection_handle,
overlay, fold_module}` for each `(NS, Index, Shard)` they manage.
""").

-export([read/3]).
-export([read/4]).
-export([read/5]).
-export([read_batch/2]).
-export([range/4]).
-export([range/5]).
-export([range_all/4]).
-export([range_all/5]).
-export([read_at_hlc/3]).
-export([read_at_hlc/4]).
-export([write_through/4]).
-export([write_through/5]).
-export([shard_for/3]).
-export([shard_for/4]).
-export([ensure_fresh/2]).
-export([ensure_fresh_for_keys/2]).
-export([freshness/1]).
-export([subscribe/2]).
-export([unsubscribe/1]).
-export([publish/4]).

-export_type([bucket/0]).
-export_type([read_opts/0]).
-export_type([read_result/0]).
-export_type([read_batch_opts/0]).
-export_type([read_batch_result/0]).
-export_type([batch_key/0]).
-export_type([consistency/0]).
-export_type([range_opts/0]).
-export_type([range_result/0]).
-export_type([range_spec/0]).

-type bucket()      :: term().
-type read_opts()   :: map().
-type read_result() :: {Value :: term(), Hlc :: bondy_oplog_hlc:hlc()}
                     | undefined.

-type consistency()       :: eventual | causal | snapshot.
-type batch_key()         :: {atom(), atom(), bucket(), term()}.
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

-doc("""
Backward-compatible point read in the default `(NS, primary, '', Key)`
slot — `Bucket = <<>>` is the canonical empty bucket for single-tenant
consumers.

Prefer `read/4` (with explicit Bucket) for any multi-tenant or
multi-bucket setup.
""").
-spec read(
    Namespace :: atom(),
    Index :: atom(),
    Key :: term()
) -> read_result() | {error, term()}.

read(NS, Index, Key) ->
    read(NS, Index, <<>>, Key, #{}).


-spec read(
    Namespace :: atom(),
    Index :: atom(),
    Bucket :: bucket(),
    Key :: term()
) -> read_result() | {error, term()}.

read(NS, Index, Bucket, Key) ->
    read(NS, Index, Bucket, Key, #{}).


-spec read(
    Namespace :: atom(),
    Index :: atom(),
    Bucket :: bucket(),
    Key :: term(),
    Opts :: read_opts()
) -> read_result() | {error, term()}.

read(NS, Index, Bucket, Key, _Opts) ->
    case resolve_shard(NS, Index, Bucket, Key) of
        {ok, Entry} ->
            {_NS, _Idx, Shard} = bondy_db_core_registry:entry_key(Entry),
            T0 = erlang:monotonic_time(microsecond),
            {Result, Source} = do_read_traced(Entry, Bucket, Key),
            DurUs = erlang:monotonic_time(microsecond) - T0,
            emit_read_event(NS, Index, Shard, Bucket, Source, DurUs, Result),
            Result;
        {error, _} = Err ->
            Err
    end.


-doc("""
Coalesced multi-cell read with optional fence and skew constraints
(`MST_DB_DESIGN.md` §8).

`Reads` is a list of `{Namespace, Index, Bucket, Key}` four-tuples. The
result is a map keyed by the same four-tuple, mapping to the per-cell
`read_result()`.

See module doc for `Opts` semantics.
""").
-spec read_batch([batch_key()], read_batch_opts()) ->
    {ok, read_batch_result(), bondy_oplog_hlc:hlc()} | {error, term()}.

read_batch(Reads, Opts) when is_list(Reads), is_map(Opts) ->
    Consistency = maps:get(consistency, Opts, eventual),
    Fence = maps:get(fence, Opts, infinity),
    {EffectiveMaxLag, EffectiveSkew} = apply_consistency(Consistency, Opts),
    T0 = erlang:monotonic_time(microsecond),
    Result = case check_consistency_class(Reads, Consistency) of
        {error, _} = ClassErr ->
            ClassErr;
        ok ->
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
Backward-compatible write-through in the default `<<>>` bucket slot.
Prefer `write_through/5` for any Bucket-aware setup.
""").
-spec write_through(
    Namespace :: atom(),
    Index :: atom(),
    Key :: term(),
    Event :: bondy_oplog_event:t()
) -> ok | {error, term()}.

write_through(NS, Index, Key, Event) ->
    write_through(NS, Index, <<>>, Key, Event).


-spec write_through(
    Namespace :: atom(),
    Index :: atom(),
    Bucket :: bucket(),
    Key :: term(),
    Event :: bondy_oplog_event:t()
) -> ok | {error, term()}.

write_through(NS, Index, Bucket, Key, Event) ->
    case resolve_shard(NS, Index, Bucket, Key) of
        {ok, Entry} ->
            do_write_through(Entry, Bucket, Key, Event);
        {error, _} = Err ->
            Err
    end.


-doc("""
Shard selector. Hashes `{Bucket, Key}` (Riak-style) over the substrate's
shard count for `(NS, Index)`. Returns `{error, no_shards}` if no shards
are registered for the namespace.
""").
-spec shard_for(atom(), atom(), bucket(), term()) ->
    {ok, non_neg_integer()} | {error, no_shards}.

shard_for(NS, Index, Bucket, Key) ->
    case bondy_db_core_registry:shard_count(NS, Index) of
        {ok, Count} -> {ok, erlang:phash2({Bucket, Key}, Count)};
        not_found   -> {error, no_shards}
    end.


-doc("""
Backward-compatible shard selector for the default `<<>>` bucket.
Hashes `{<<>>, Key}` which is identical to the historical Key-only
behaviour modulo the constant prefix — kept so legacy callers that
never used Bucket still address the same shards.
""").
-spec shard_for(atom(), atom(), term()) ->
    {ok, non_neg_integer()} | {error, no_shards}.

shard_for(NS, Index, Key) ->
    shard_for(NS, Index, <<>>, Key).


-doc("""
Backward-compatible range scan in the default `<<>>` bucket slot. See
`range/5` for the Bucket-aware version.
""").
-spec range(atom(), atom(), range_spec(), range_opts()) ->
    {ok, range_result()} | {error, term()}.

range(NS, Index, Spec, Opts) ->
    range(NS, Index, <<>>, Spec, Opts).


-doc("""
Single-shard range scan over `[Low, High)` inside `Bucket`
(`MST_DB_DESIGN.md` §9).

The shard is selected by `phash2({Bucket, Low}, ShardCount)` unless the
caller passes `Opts#{shard => N}`. Callers whose `[Low, High)` spans
more than one shard MUST scatter across shards themselves and merge
the results.

## Opts

- `limit` — max rows in the result (default `1000`).
- `direction` — `asc` (default) or `desc`.
- `include_overlay` — set `false` to exclude pending events (default `true`).
- `fence` — HLC ceiling for overlay events (default `infinity`).
- `shard` — explicit shard override.
""").
-spec range(atom(), atom(), bucket(), range_spec(), range_opts()) ->
    {ok, range_result()} | {error, term()}.

range(NS, Index, Bucket, {Low, High}, Opts) when is_map(Opts) ->
    case resolve_shard_for_range(NS, Index, Bucket, Low, Opts) of
        {ok, Entry} ->
            {_NS, _Idx, Shard} = bondy_db_core_registry:entry_key(Entry),
            T0 = erlang:monotonic_time(microsecond),
            Result = do_range(Entry, Bucket, Low, High, Opts),
            DurUs = erlang:monotonic_time(microsecond) - T0,
            emit_range_event(NS, Index, Shard, Bucket, Result, DurUs),
            Result;
        {error, _} = Err ->
            Err
    end.


-doc("""
Backward-compatible cross-shard range over the default `<<>>` bucket
slot. See `range_all/5` for the Bucket-aware version.
""").
-spec range_all(atom(), atom(), range_spec(), range_opts()) ->
    {ok, range_result()} | {error, term()}.

range_all(NS, Index, Spec, Opts) ->
    range_all(NS, Index, <<>>, Spec, Opts).


-doc("""
Cross-shard range scan over `[Low, High)` inside `Bucket`
(`MST_DB_DESIGN.md` §18 item 2).

Scatters the range to every shard registered under `(NS, Index)`, runs
the single-shard `range/5` per shard with `Opts#{shard => Shard}`, then
merges the per-shard results into a single globally-sorted list.

## Opts

- `limit` — global cap on rows in the result (default `1000`). Applied
  after merge.
- `direction` — `asc` (default) or `desc`.
- `include_overlay` — set `false` to exclude pending events (default
  `true`). Propagated to every per-shard scan.
- `fence` — HLC ceiling for overlay events (default `infinity`).
  Propagated to every per-shard scan.

## Correctness of per-shard limit propagation

Per-shard calls pass the caller's `limit` verbatim. Because each shard's
result is already globally sorted on the shard, and the merged result
is bounded above by the union of the per-shard top-`Limit`s, every key
that would appear in the global top-`Limit` is present in at least one
per-shard top-`Limit`. Truncating the merged list to `Limit` is
therefore correct.

## Error semantics

If any shard returns `{error, _}` the call surfaces that error and
discards results from other shards. No partial results are returned.

If `(NS, Index)` has no registered shards the call returns `{ok, []}`.
""").
-spec range_all(atom(), atom(), bucket(), range_spec(), range_opts()) ->
    {ok, range_result()} | {error, term()}.

range_all(NS, Index, Bucket, {Low, High}, Opts)
        when is_atom(NS), is_atom(Index), is_map(Opts) ->
    Direction = maps:get(direction, Opts, asc),
    Limit     = maps:get(limit, Opts, 1000),
    Shards = shards_in(NS, Index),
    T0 = erlang:monotonic_time(microsecond),
    Result = scatter_range(Shards, NS, Index, Bucket, Low, High, Opts),
    DurUs = erlang:monotonic_time(microsecond) - T0,
    case Result of
        {ok, Rows} ->
            Merged = merge_sorted_ranges(Rows, Direction),
            Truncated = lists:sublist(Merged, Limit),
            emit_range_all_event(
                NS, Index, Bucket, length(Shards),
                length(Truncated), DurUs
            ),
            {ok, Truncated};
        {error, _} = Err ->
            emit_range_all_error_event(
                NS, Index, Bucket, length(Shards), Err, DurUs
            ),
            Err
    end.


-doc("""
Backward-compatible point-in-time read in the default `<<>>` bucket
slot. See `read_at_hlc/4` for the Bucket-aware version.
""").
-spec read_at_hlc(
    Namespace :: atom(),
    Key :: term(),
    T :: bondy_oplog_hlc:hlc()
) -> {ok, Value :: term(), Hlc :: bondy_oplog_hlc:hlc()}
   | {error, term()}.

read_at_hlc(NS, Key, T) ->
    read_at_hlc(NS, <<>>, Key, T).


-doc("""
Point-in-time read against the primary index (`MST_DB_DESIGN.md` §10).

Returns the cell value as of HLC `T`. See module doc for the historical-
read semantics; the substrate refuses with
`{error, {historical_read_unavailable, ProjHlc, T}}` if the projection
has already advanced past `T`.
""").
-spec read_at_hlc(
    Namespace :: atom(),
    Bucket :: bucket(),
    Key :: term(),
    T :: bondy_oplog_hlc:hlc()
) -> {ok, Value :: term(), Hlc :: bondy_oplog_hlc:hlc()}
   | {error, term()}.

read_at_hlc(NS, Bucket, Key, T) when is_integer(T), T >= 0 ->
    T0 = erlang:monotonic_time(microsecond),
    Result = case resolve_shard(NS, primary, Bucket, Key) of
        {ok, Entry} ->
            do_read_at_hlc(Entry, Bucket, Key, T);
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
the supplied keys.
""").
-spec ensure_fresh_for_keys(
    [batch_key()],
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
    emit_ensure_fresh_event(length(Touched), length(Stale), DurUs),
    case Stale of
        [] -> ok;
        _  -> {stale, Stale}
    end.


-doc("""
Return per-shard freshness lag for the namespace as a map keyed by
`{Index, Shard}` with values in milliseconds.
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


-spec subscribe(atom(), bondy_db_core_dispatcher:pattern()) ->
    {ok, reference()}.

subscribe(NS, Pattern) ->
    bondy_db_core_dispatcher:subscribe(NS, Pattern).


-spec unsubscribe(reference()) -> ok.

unsubscribe(SubRef) ->
    bondy_db_core_dispatcher:unsubscribe(SubRef).


-spec publish(atom(), term(), bondy_oplog_hlc:hlc(), term()) -> ok.

publish(NS, Key, Hlc, Op) ->
    bondy_db_core_dispatcher:publish(NS, Key, Hlc, Op).


%% =============================================================================
%% Read path
%% =============================================================================

resolve_shard(NS, Index, Bucket, Key) ->
    case shard_for(NS, Index, Bucket, Key) of
        {ok, Shard} ->
            case bondy_db_core_registry:lookup(NS, Index, Shard) of
                {ok, Entry} -> {ok, Entry};
                not_found   -> {error, shard_not_registered}
            end;
        {error, _} = Err ->
            Err
    end.


do_read_traced(Entry, Bucket, Key) ->
    CA = bondy_db_core_registry:entry_cache_adapter(Entry),
    CH = bondy_db_core_registry:entry_cache_handle(Entry),
    case CA:get(CH, Bucket, Key) of
        {ok, {Value, Hlc}} ->
            {{Value, Hlc}, cache};
        not_found ->
            slow_read_traced(Entry, Bucket, Key)
    end.


slow_read_traced(Entry, Bucket, Key) ->
    Strategy = bondy_db_core_registry:entry_fold_module(Entry),
    {ProjValue, ProjHlc, ProjHadFrame} =
        read_projection(Entry, Bucket, Key, Strategy),
    OverlayEvents = read_overlay(Entry, Bucket, Key, ProjHlc),
    OverlayApplied = OverlayEvents =/= [],
    {Value, Hlc} = fold_events(Strategy, ProjValue, ProjHlc, OverlayEvents),
    Source = source_for(ProjHadFrame, OverlayApplied),
    case Value of
        undefined ->
            {undefined, Source};
        _ ->
            CA = bondy_db_core_registry:entry_cache_adapter(Entry),
            CH = bondy_db_core_registry:entry_cache_handle(Entry),
            ok = CA:put(CH, Bucket, Key, {Value, Hlc}),
            {{Value, Hlc}, Source}
    end.


source_for(true,  false) -> projection;
source_for(true,  true)  -> projection_with_overlay;
source_for(false, true)  -> overlay_only;
source_for(false, false) -> projection.


read_projection(Entry, Bucket, Key, Strategy) ->
    PA = bondy_db_core_registry:entry_projection_adapter(Entry),
    PH = bondy_db_core_registry:entry_projection_handle(Entry),
    case PA:get(PH, Bucket, Key) of
        not_found ->
            {bondy_oplog_fold:initial_value(Strategy), 0, false};
        {ok, Frame} ->
            {Hlc, Body} = bondy_oplog_cell_frame:decode(Frame),
            {bondy_oplog_fold:decode_state(Strategy, Body), Hlc, true}
    end.


read_overlay(Entry, Bucket, Key, AfterHlc) ->
    case bondy_db_core_registry:entry_overlay(Entry) of
        disabled -> [];
        Tab -> bondy_oplog_db_overlay:events_for(Tab, Bucket, Key, AfterHlc)
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


ensure_fresh_predicate_for_keys(_Reads, infinity) ->
    ok;
ensure_fresh_predicate_for_keys(Reads, MaxLag) ->
    case ensure_fresh_for_keys(Reads, MaxLag) of
        ok               -> ok;
        {stale, _} = Err -> {error, Err}
    end.


%% Dedup shards before per-shard registry lookups.
touched_shards(Reads) ->
    ShardSet = lists:foldl(
        fun({NS, Index, Bucket, Key}, Acc) ->
            case shard_for(NS, Index, Bucket, Key) of
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
        {{NS, Idx, Bucket, Key},
         read_at_fence(NS, Idx, Bucket, Key, Fence)}
        || {NS, Idx, Bucket, Key} <- Reads
    ]).


read_at_fence(NS, Index, Bucket, Key, Fence) ->
    case resolve_shard(NS, Index, Bucket, Key) of
        {ok, Entry} ->
            fenced_read(Entry, Bucket, Key, Fence);
        {error, _} = Err ->
            Err
    end.


fenced_read(Entry, Bucket, Key, Fence) ->
    Strategy = bondy_db_core_registry:entry_fold_module(Entry),
    {ProjValue, ProjHlc, _ProjHadFrame} =
        read_projection(Entry, Bucket, Key, Strategy),
    OverlayEvents = fenced_overlay(Entry, Bucket, Key, ProjHlc, Fence),
    {Value, Hlc} = fold_events(Strategy, ProjValue, ProjHlc, OverlayEvents),
    case Value of
        undefined -> undefined;
        _ -> {Value, Hlc}
    end.


fenced_overlay(Entry, Bucket, Key, AfterHlc, infinity) ->
    read_overlay(Entry, Bucket, Key, AfterHlc);
fenced_overlay(Entry, Bucket, Key, AfterHlc, Fence) ->
    case bondy_db_core_registry:entry_overlay(Entry) of
        disabled -> [];
        Tab ->
            bondy_oplog_db_overlay:events_for_window(
                Tab, Bucket, Key, AfterHlc, Fence
            )
    end.


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

resolve_shard_for_range(NS, Index, Bucket, Low, Opts) ->
    case maps:get(shard, Opts, undefined) of
        undefined ->
            case shard_for(NS, Index, Bucket, Low) of
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


do_range(Entry, Bucket, Low, High, Opts) ->
    Limit          = maps:get(limit, Opts, 1000),
    Direction      = maps:get(direction, Opts, asc),
    IncludeOverlay = maps:get(include_overlay, Opts, true),
    Fence          = maps:get(fence, Opts, infinity),
    Strategy       = bondy_db_core_registry:entry_fold_module(Entry),
    PA             = bondy_db_core_registry:entry_projection_adapter(Entry),
    PH             = bondy_db_core_registry:entry_projection_handle(Entry),

    case PA:range(PH, Bucket, Low, High, Opts) of
        {ok, ProjEntries} ->
            OverlayEntries = overlay_for_range(
                Entry, Bucket, Low, High, Fence, IncludeOverlay
            ),
            Merged = merge_range(Strategy, ProjEntries, OverlayEntries),
            Ordered = case Direction of
                asc  -> Merged;
                desc -> lists:reverse(Merged)
            end,
            {ok, lists:sublist(Ordered, Limit)};
        {error, _} = Err ->
            Err
    end.


overlay_for_range(_Entry, _Bucket, _Low, _High, _Fence, false) ->
    [];
overlay_for_range(Entry, Bucket, Low, High, Fence, true) ->
    case bondy_db_core_registry:entry_overlay(Entry) of
        disabled -> [];
        Tab ->
            bondy_oplog_db_overlay:range_window(
                Tab, Bucket, Low, High, Fence
            )
    end.


merge_range(Strategy, ProjEntries, OverlayEntries) ->
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


shards_in(NS, Index) ->
    [E
     || E <- bondy_db_core_registry:shards_for(NS),
        begin
            {_NS, Idx, _Sh} = bondy_db_core_registry:entry_key(E),
            Idx =:= Index
        end].


scatter_range([], _NS, _Index, _Bucket, _Low, _High, _Opts) ->
    {ok, []};
scatter_range(Shards, NS, Index, Bucket, Low, High, Opts) ->
    scatter_range_loop(Shards, NS, Index, Bucket, Low, High, Opts, []).


scatter_range_loop([], _NS, _Index, _Bucket, _Low, _High, _Opts, Acc) ->
    {ok, Acc};
scatter_range_loop([Entry | Rest], NS, Index, Bucket, Low, High, Opts, Acc) ->
    {_NS, _Idx, Shard} = bondy_db_core_registry:entry_key(Entry),
    PerShardOpts = Opts#{shard => Shard},
    case range(NS, Index, Bucket, {Low, High}, PerShardOpts) of
        {ok, Rows} ->
            scatter_range_loop(
                Rest, NS, Index, Bucket, Low, High, Opts, [Rows | Acc]
            );
        {error, _} = Err ->
            Err
    end.


%% Multi-way merge of per-shard range results into a single globally
%% sorted list. Each input list is already sorted by Key for the
%% requested direction; shards partition the keyspace under
%% `phash2({Bucket, Key}, ShardCount)` so the union has no Key
%% collisions, and a flat sort over the concatenation is correct.
merge_sorted_ranges([], _Direction) ->
    [];
merge_sorted_ranges(PerShardRows, Direction) ->
    Flat = lists:append(PerShardRows),
    Comparator = case Direction of
        asc  -> fun({K1, _, _}, {K2, _, _}) -> K1 =< K2 end;
        desc -> fun({K1, _, _}, {K2, _, _}) -> K1 >= K2 end
    end,
    lists:sort(Comparator, Flat).


emit_range_cell(Strategy, Frame, Events) ->
    {ProjValue, ProjHlc} = case Frame of
        undefined ->
            {bondy_oplog_fold:initial_value(Strategy), 0};
        Bin when is_binary(Bin) ->
            {H, Body} = bondy_oplog_cell_frame:decode(Bin),
            {bondy_oplog_fold:decode_state(Strategy, Body), H}
    end,
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

do_read_at_hlc(Entry, Bucket, Key, T) ->
    Strategy = bondy_db_core_registry:entry_fold_module(Entry),
    {ProjValue, ProjHlc, _ProjHadFrame} =
        read_projection(Entry, Bucket, Key, Strategy),
    case ProjHlc > T of
        true ->
            {error, {historical_read_unavailable, ProjHlc, T}};
        false ->
            OverlayEvents = fenced_overlay(Entry, Bucket, Key, ProjHlc, T),
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

check_consistency_class(_Reads, Consistency)
        when Consistency =/= eventual ->
    ok;
check_consistency_class(Reads, eventual) ->
    NSs = lists:usort([NS || {NS, _Idx, _B, _K} <- Reads]),
    case lists:dropwhile(
        fun(NS) -> bondy_db_core_registry:consistency_class(NS) =/= cp end,
        NSs
    ) of
        [] -> ok;
        [CpNs | _] ->
            {error, {consistency_class_violation, CpNs, cp, eventual}}
    end.


do_write_through(Entry, Bucket, Key, Event) ->
    CA = bondy_db_core_registry:entry_cache_adapter(Entry),
    CH = bondy_db_core_registry:entry_cache_handle(Entry),
    case CA:get(CH, Bucket, Key) of
        not_found ->
            ok;
        {ok, {OldValue, _OldHlc}} ->
            Strategy = bondy_db_core_registry:entry_fold_module(Entry),
            Op = bondy_oplog_event:op(Event),
            case bondy_oplog_fold:apply_event(Strategy, OldValue, Op) of
                undefined ->
                    ok = CA:delete(CH, Bucket, Key);
                NewValue ->
                    NewHlc = bondy_oplog_fold:hlc(Strategy, NewValue),
                    ok = CA:put(CH, Bucket, Key, {NewValue, NewHlc})
            end
    end.


%% =============================================================================
%% Telemetry (`MST_DB_DESIGN.md` §16)
%% =============================================================================

emit_read_event(NS, Index, Shard, Bucket, Source, DurUs, Result) ->
    {Hit, ValueBytes} = case Result of
        {Value, _Hlc} when Value =/= undefined ->
            {Source =:= cache, erlang:external_size(Value)};
        _ ->
            {false, 0}
    end,
    telemetry:execute(
        [bondy_db_core, read],
        #{duration_us => DurUs, hit => Hit, value_bytes => ValueBytes},
        #{namespace => NS, index => Index, shard => Shard,
          bucket => Bucket, source => Source}
    ).


emit_read_batch_event(Reads, Fence, Result, DurUs) ->
    NSs = lists:usort([NS || {NS, _Idx, _B, _K} <- Reads]),
    {ReadCount, TotalBytes, SkewMs} = batch_summary(Result),
    telemetry:execute(
        [bondy_db_core, read_batch],
        #{duration_us => DurUs,
          read_count => ReadCount,
          total_bytes => TotalBytes,
          skew_ms => SkewMs},
        #{namespaces => NSs, fence_hlc => Fence}
    ).


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


emit_range_event(NS, Index, Shard, Bucket, Result, DurUs) ->
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
        #{namespace => NS, index => Index, shard => Shard,
          bucket => Bucket}
    ).


emit_range_all_event(NS, Index, Bucket, ShardCount, EntriesReturned, DurUs) ->
    telemetry:execute(
        [bondy_db_core, range_all],
        #{duration_us => DurUs,
          shards_scanned => ShardCount,
          entries_returned => EntriesReturned},
        #{namespace => NS, index => Index, bucket => Bucket}
    ).


emit_range_all_error_event(NS, Index, Bucket, ShardCount, {error, Reason}, DurUs) ->
    telemetry:execute(
        [bondy_db_core, range_all],
        #{duration_us => DurUs,
          shards_scanned => ShardCount,
          entries_returned => 0},
        #{namespace => NS, index => Index, bucket => Bucket,
          refused => true, reason => Reason}
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
