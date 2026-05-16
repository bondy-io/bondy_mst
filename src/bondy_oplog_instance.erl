%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_instance).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").
-include("bondy_oplog.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
The per-instance Merkle Search Tree owner
(`_design/10_new_design.md` §11.3).

Exactly one process per running instance; one MST per instance; one
storage backend handle per instance.

## Responsibilities

- Own the MST handle (which itself owns the storage backend).
- Generate fresh `{HLC, Origin, Seq}` event keys on local appends.
- Sign local events through the configured validator.
- Install verified peer events into the MST. Signature verification
  for both locally appended events (WAL drain path) and peer-received
  events (`append_remote/2`) runs in the per-instance applier
  process; this gen_server is the install point but not the verify
  point.
- Expose the MST root hash, key-range reads, and prefix truncation
  hooks for compaction.
- Run compaction cycles: stability frontier → `interpret_cog` →
  snapshot → MST truncate → watermark advance.
- Own the page-level anti-entropy primitives (`merge_pages`,
  `integrate_peer_root`) and snapshot-load operations. These are not
  event-stream operations; the public façade drains the applier
  before invoking them so installs already in flight are visible.

## What this module is *not*

The library is **agnostic** to lifecycle policy. This module does not
do lazy loading, LRU eviction, cold-tier offload, or per-tenant
naming. Those are *consumer* concerns (`_design/10_new_design.md`
§1.2, §12). The instance is started eagerly via
`bondy_oplog:start_instance/1,2`.

Anti-entropy and GC scheduling live in dedicated modules; they *use*
this one.

## Concurrency model

All operations currently round-trip the gen_server. Per-instance HLC
and Seq counters live in `atomics` cells inside the state record so
a future lock-free local-append path can move out of the gen_server
without protocol changes.
""").

-record(state, {
    instance_id :: binary(),
    origin :: bondy_oplog_origin:t(),
    hlc :: bondy_oplog_hlc:t(),
    seq :: atomics:atomics_ref(),
    mst :: bondy_mst:t(),
    backend :: backend(),
    validator_module :: module(),
    validator_state :: term(),
    merge_strategy :: module(),
    crdt_module :: module() | undefined,
    snapshot_store :: module(),
    snapshot_state :: term(),
    watermark :: undefined | bondy_oplog_event:event_key(),
    %% Cached `{Watermark, Snapshot}` from the snapshot store so the
    %% registry can publish it without re-reading the store on every
    %% mutation. Refreshed on init, compact, and load_snapshot.
    cached_snapshot :: undefined | {bondy_oplog_event:event_key(), term()},
    max_working_set :: pos_integer() | infinity,
    %% Cached size of the live MST (avoids a fold per append). Updated
    %% on every state-mutating handle_call.
    live_size :: non_neg_integer(),
    last_event_key :: undefined | bondy_oplog_event:event_key(),
    %% Compaction-in-flight tracking. While set, refuse new compact /
    %% load_snapshot requests (idempotent re-arm on next tick).
    compaction ::
        undefined
        | #{
            pid := pid(),
            from := gen_server:from(),
            started_at := integer()
        },
    %% Cached per-instance WAL writer pid. Refreshed lazily from the
    %% registry on the first append after a `'DOWN'` from the previous
    %% writer (one_for_all restarts swap in a new pid).
    wal_pid :: undefined | pid(),
    %% Monitor reference for the cached `wal_pid`; cleared when the
    %% monitored process dies.
    wal_pid_monitor :: undefined | reference(),
    %% Per-instance overlay (`ordered_set`, public). Receives every
    %% successfully WAL-appended local event so callers reading back
    %% the key see the entry before the applier promotes it to the
    %% MST. Rows are `{Key, Value, Hlc, Origin}`; entries are evicted
    %% atomically with the MST insert via HLC-conditional
    %% `ets:select_delete/2`. Created in `init/1`, deleted in
    %% `terminate/2`; no heir.
    overlay :: undefined | ets:tid(),
    %% Overlay backpressure caps. `max_overlay_events` defaults to
    %% 10_000; `max_overlay_bytes` to 5 MB; `throttle_strategy`
    %% defaults to `drop` and is the only supported value
    %% (`block` reserved).
    max_overlay_events :: pos_integer(),
    max_overlay_bytes :: pos_integer(),
    overlay_throttle :: drop
}).

-type backend() :: map | ets | module().

-type opts() :: #{
    backend => backend(),
    backend_options => map() | list(),
    storage_path => binary(),
    path_strategy => module(),
    hash_algorithm => sha256 | sha512,
    origin => bondy_oplog_origin:t(),
    hlc_seed => non_neg_integer(),
    seq_seed => non_neg_integer(),
    validator => module(),
    validator_opts => map(),
    merge_strategy => module(),
    crdt_module => module(),
    snapshot_store => module(),
    snapshot_store_opts => map(),
    max_working_set => pos_integer() | infinity,
    %% Overlay backpressure caps. Either threshold triggers
    %% `{error, backpressure}` from `append/2,3` and `append_many/2`.
    max_overlay_events => pos_integer(),
    max_overlay_bytes => pos_integer(),
    %% Throttle strategy on overlay-cap breach. `drop` returns
    %% `{error, backpressure}` immediately; `block` is reserved for a
    %% follow-on PR and currently behaves like `drop`.
    overlay_throttle => drop,
    %% Per-instance applier tuning. See `bondy_oplog_applier:opts/0`.
    %% Recognised keys:
    %%   commit_every     :: pos_integer()   (default 64)
    %%   poll_interval_ms :: pos_integer()   (default 5)
    applier => #{
        commit_every => pos_integer(),
        poll_interval_ms => pos_integer()
    }
}.

-export_type([opts/0]).
-export_type([backend/0]).

%% Lifecycle
-export([start_link/2]).
-export([child_spec/2]).
-export([stop/1]).

%% Public API (typically called via `bondy_oplog`)
-export([append/2]).
-export([append/3]).
-export([append_many/2]).
-export([append_remote/2]).
-export([await_apply/1]).
-export([await_apply/2]).
-export([get/2]).
-export([root_hash/1]).
-export([fold_range/5]).
-export([range/3]).
-export([truncate_prefix/2]).
-export([size/1]).
-export([first_key/1]).
-export([latest_key/1]).
-export([origin/1]).
-export([info/1]).

%% Applier handshake — applier reads `{validator_module, validator_state}`
%% once at its `init/1` so it can re-verify signatures (S1) in its own
%% process before dispatching events to the instance.
-export([get_validator/1]).

%% Operator-facing trigger that asks the applier to refresh its
%% validator snapshot by calling the optional
%% `bondy_oplog_validator:refresh/1` callback.
-export([refresh_validator/1, refresh_validator/2]).

%% Page-level API (sync protocol)
-export([get_pages/2]).
-export([merge_pages/2]).
-export([missing_set/2]).
-export([integrate_peer_root/2]).

%% GC / compaction API
-export([current_watermark/1]).
-export([crdt_module/1]).
-export([snapshot/1]).
-export([compact/2]).

%% Bootstrap
-export([load_snapshot/3]).

%% Registry helpers
-export([whereis/1]).
-export([lookup_origin/1]).

%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).

%% =============================================================================
%% LIFECYCLE
%% =============================================================================

-spec start_link(instance_id(), opts()) ->
    {ok, pid()} | {error, term()}.

start_link(InstanceId, Opts) when
    is_binary(InstanceId), is_map(Opts)
->
    gen_server:start_link(?MODULE, {InstanceId, Opts}, []).

-spec child_spec(instance_id(), opts()) -> supervisor:child_spec().

child_spec(InstanceId, Opts) ->
    #{
        id => {?MODULE, InstanceId},
        start => {?MODULE, start_link, [InstanceId, Opts]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
    }.

-spec stop(instance_id() | pid()) -> ok.

stop(Target) ->
    gen_server:stop(target(Target)).

%% =============================================================================
%% API
%% =============================================================================

-spec append(instance_id() | pid(), bondy_oplog_event:op()) ->
    bondy_oplog_event:event_key().

append(Target, Op) ->
    append(Target, Op, undefined).

-spec append(
    instance_id() | pid(),
    bondy_oplog_event:op(),
    bondy_oplog_event:meta()
) -> bondy_oplog_event:event_key().

append(Target, Op, Meta) ->
    gen_server:call(target(Target), {append, Op, Meta}, infinity).

?DOC("""
Appends a batch of operations atomically (all-or-nothing within the
instance). Returns the assigned keys in input order.
""").
-spec append_many(
    instance_id() | pid(),
    [{bondy_oplog_event:op(), bondy_oplog_event:meta()}]
) -> [bondy_oplog_event:event_key()].

append_many(_Target, []) ->
    [];
append_many(Target, OpsAndMetas) when is_list(OpsAndMetas) ->
    gen_server:call(target(Target), {append_many, OpsAndMetas}, infinity).

?DOC("""
Inserts an event received from a peer. Idempotent. Validation runs in
the caller's process: an Origin matching the instance's local Origin
raises `error/1` without disturbing the instance gen_server. The
configured validator's `verify_event/2` runs in the per-instance
applier process, which then forwards the verified event to the
instance for origin-ban / backpressure / watermark filtering and the
MST install. The applier is therefore the sole verify+dispatch origin
for both locally appended and peer-received events.

**Pass an `instance_id()` (binary)** for hot-path callers. The binary
form resolves origin and applier pid via lock-free registry reads
before issuing the verify call. The `pid()` form is supported for
test/internal convenience only: it pays two extra `gen_server:call`
round trips (origin lookup, then instance-id reverse lookup) before
the verify call begins.
""").
-spec append_remote(instance_id() | pid(), bondy_oplog_event:t()) ->
    ok | {error, term()}.

append_remote(Target, Event) ->
    Key = bondy_oplog_event:key(Event),
    PeerOrigin = bondy_oplog_event:key_origin(Key),
    case lookup_origin(Target) of
        {ok, PeerOrigin} ->
            error({remote_event_with_local_origin, PeerOrigin});
        _ ->
            case applier_pid_for(Target) of
                {ok, ApplierPid} ->
                    bondy_oplog_applier:enqueue_remote(ApplierPid, Event);
                {error, _} = Err ->
                    Err
            end
    end.

%% @private
%% Resolves the applier pid for a `Target` (instance id or instance
%% pid). The applier is published in the registry by its own
%% `init/1`; during a subtree mid-restart it can briefly be absent,
%% in which case callers see `{error, applier_unavailable}` and can
%% retry.
applier_pid_for(InstanceId) when is_binary(InstanceId) ->
    case bondy_oplog_registry:applier_pid(InstanceId) of
        undefined -> {error, applier_unavailable};
        Pid when is_pid(Pid) -> {ok, Pid}
    end;
applier_pid_for(Pid) when is_pid(Pid) ->
    case lookup_instance_id(Pid) of
        undefined -> {error, applier_unavailable};
        Id -> applier_pid_for(Id)
    end.

?DOC("""
Blocks until the per-instance applier has promoted every overlay row
into the MST, or until `Timeout` ms have elapsed.

After the write path returns from `append/2`, the event is durable
in the WAL and visible in the overlay but not yet in the MST. The
per-instance applier drains the WAL and dispatches `install_local_batch`
casts to the instance; each cast installs the events in the MST and
evicts the matching overlay rows.

Operations that read the MST directly (`root_hash/1`, `compact/2`,
`sync/2`) see the post-applier state only — callers that need
read-after-write consistency on the MST itself should call this
function as a synchronisation point.

Returns `ok` once the overlay is empty, or `{error, timeout}` on
expiry. A missing overlay (subtree mid-restart) returns `ok`.
""").
-spec await_apply(instance_id() | pid()) -> ok | {error, timeout}.

await_apply(Target) ->
    await_apply(Target, 5000).

-spec await_apply(instance_id() | pid(), timeout()) -> ok | {error, timeout}.

await_apply(Target, Timeout) when
    is_binary(Target) orelse is_pid(Target)
->
    Deadline = case Timeout of
        infinity -> infinity;
        Ms when is_integer(Ms), Ms >= 0 ->
            erlang:monotonic_time(millisecond) + Ms
    end,
    do_await_apply(Target, Deadline).

%% @private
do_await_apply(Target, Deadline) ->
    case overlay_drained(Target) of
        true -> ok;
        false ->
            case past_deadline(Deadline) of
                true -> {error, timeout};
                false ->
                    timer:sleep(5),
                    do_await_apply(Target, Deadline)
            end
    end.

%% @private
overlay_drained(Target) when is_binary(Target) ->
    overlay_size(Target) =:= 0;
overlay_drained(Target) when is_pid(Target) ->
    case lookup_instance_id(Target) of
        undefined -> true;
        Id -> overlay_size(Id) =:= 0
    end.

%% @private
past_deadline(infinity) -> false;
past_deadline(Deadline) ->
    erlang:monotonic_time(millisecond) >= Deadline.

%% @private
%% Best-effort reverse lookup from gen_server pid to instance_id.
%% Returns `undefined` when the pid is not registered, in which case
%% the overlay is treated as drained.
lookup_instance_id(Pid) when is_pid(Pid) ->
    try gen_server:call(Pid, instance_id, 1000) of
        Id when is_binary(Id) -> Id;
        _ -> undefined
    catch
        _:_ -> undefined
    end.

-spec get(instance_id() | pid(), bondy_oplog_event:event_key()) ->
    {ok, bondy_oplog_event:t()} | not_found.

get(Target, Key) when is_binary(Target) ->
    %% Overlay-first, then registry MST. The overlay holds events
    %% that landed in the WAL but have not yet been promoted by the
    %% applier. Reading the overlay before the MST handle closes the
    %% race where the applier publishes a new handle and then evicts
    %% the overlay row: if we miss the overlay, the new handle is
    %% already in the registry (MST publish strictly precedes overlay
    %% evict).
    case overlay_lookup(Target, Key) of
        {ok, _} = Hit ->
            Hit;
        not_found ->
            case bondy_oplog_registry:mst(Target) of
                undefined ->
                    error({noproc, {?MODULE, Target}});
                MST ->
                    case bondy_mst:get(MST, Key) of
                        undefined -> not_found;
                        Value -> {ok, event_from_value(Key, Value)}
                    end
            end
    end;
get(Target, Key) ->
    gen_server:call(target(Target), {get, Key}).

-spec root_hash(instance_id() | pid()) -> binary() | undefined.

root_hash(Target) when is_binary(Target) ->
    case bondy_oplog_registry:mst(Target) of
        undefined -> error({noproc, {?MODULE, Target}});
        MST -> bondy_mst:root(MST)
    end;
root_hash(Target) ->
    gen_server:call(target(Target), root_hash).

-spec fold_range(
    instance_id() | pid(),
    From :: bondy_oplog_event:event_key(),
    To :: bondy_oplog_event:event_key(),
    fun((bondy_oplog_event:t(), Acc) -> Acc),
    Acc
) -> Acc when Acc :: term().

fold_range(Target, From, To, Fun, Acc0) when
    is_binary(Target), is_function(Fun, 2)
->
    case bondy_oplog_registry:mst(Target) of
        undefined ->
            error({noproc, {?MODULE, Target}});
        MST ->
            OverlayQueue = overlay_range(Target, From, To),
            fold_range_merged(MST, From, To, OverlayQueue, Fun, Acc0)
    end;
fold_range(Target, From, To, Fun, Acc0) when is_function(Fun, 2) ->
    gen_server:call(target(Target), {fold_range, From, To, Fun, Acc0}).

-spec range(
    instance_id() | pid(),
    From :: bondy_oplog_event:event_key(),
    To :: bondy_oplog_event:event_key()
) -> [bondy_oplog_event:t()].

range(Target, From, To) ->
    lists:reverse(
        fold_range(Target, From, To, fun(E, Acc) -> [E | Acc] end, [])
    ).

-spec truncate_prefix(instance_id() | pid(), bondy_oplog_event:event_key()) ->
    non_neg_integer().

truncate_prefix(Target, Watermark) ->
    gen_server:call(target(Target), {truncate_prefix, Watermark}, infinity).

-spec size(instance_id() | pid()) -> non_neg_integer().

size(Target) when is_binary(Target) ->
    %% Total events visible to the lock-free read path: events
    %% already promoted to the MST + events still staged in the
    %% overlay. The `install_local_batch` cast handler publishes the
    %% new MST handle and evicts the matching overlay rows in the
    %% same callback, so the two sets are disjoint at every observable
    %% state — no double-count.
    case bondy_oplog_registry:live_size(Target) of
        undefined ->
            error({noproc, {?MODULE, Target}});
        MstSize ->
            MstSize + overlay_size(Target)
    end;
size(Target) ->
    gen_server:call(target(Target), instance_size).

-spec first_key(instance_id() | pid()) ->
    {ok, bondy_oplog_event:event_key()} | empty.

first_key(Target) when is_binary(Target) ->
    case bondy_oplog_registry:mst(Target) of
        undefined ->
            error({noproc, {?MODULE, Target}});
        MST ->
            merge_first_key(Target, MST)
    end;
first_key(Target) ->
    gen_server:call(target(Target), first_key).

-spec latest_key(instance_id() | pid()) ->
    {ok, bondy_oplog_event:event_key()} | empty.

latest_key(Target) when is_binary(Target) ->
    case bondy_oplog_registry:mst(Target) of
        undefined ->
            error({noproc, {?MODULE, Target}});
        MST ->
            merge_latest_key(Target, MST)
    end;
latest_key(Target) ->
    gen_server:call(target(Target), latest_key).

?DOC("""
Returns the configured Origin for `Target`.

**Pass an `instance_id()` (binary)** for hot-path callers — this form
reads the value directly from the registry without messaging. The
`pid()` form is a test/internal convenience that issues a synchronous
`gen_server:call` to the instance.
""").
-spec origin(instance_id() | pid()) -> bondy_oplog_origin:t().

origin(Target) ->
    case lookup_origin(Target) of
        {ok, Origin} -> Origin;
        not_found -> error({noproc, Target})
    end.

?DOC("""
Returns a diagnostic summary of an instance's state. Cheap;
appropriate for status pages and operational tools.
""").
-spec info(instance_id() | pid()) -> map().

info(Target) ->
    gen_server:call(target(Target), info).

?DOC("""
Returns the validator module and a snapshot of the validator state
for the per-instance applier. The applier uses this to re-verify
event signatures (S1) in its own process before dispatching events
to the instance for install. `verify_event/2` is read-only on the
validator state, so the snapshot remains valid for the lifetime of
the applier.
""").
-spec get_validator(pid()) -> {module(), term()}.

get_validator(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, get_validator, infinity).

?DOC("""
Asks the per-instance applier to refresh its validator snapshot by
calling `bondy_oplog_validator:refresh/1` on the current snapshot.

Returns `ok` once the refresh request has been *delivered* to the
applier (fire-and-forget cast). The actual outcome — snapshot
swapped, validator returned `{error, _}`, validator raised, or
`refresh/1` not exported — is logged by the applier and surfaced
via the `[bondy_oplog, applier, validator_refresh]` telemetry event.

Returns `{error, applier_unavailable}` if the subtree is mid-restart
and the applier hasn't published its pid yet — operators / tests
should retry.

Equivalent to `refresh_validator(Target, validator_refresh)`.
""").
-spec refresh_validator(instance_id() | pid()) ->
    ok | {error, applier_unavailable}.

refresh_validator(Target) ->
    refresh_validator(Target, validator_refresh).

?DOC("""
As `refresh_validator/1` but tags the refresh request with an
operator-supplied `Reason` term. The reason is logged by the applier
and emitted on the `[bondy_oplog, applier, validator_refresh]`
telemetry event so operators can correlate the refresh with whatever
upstream change triggered it (config push, key rotation, etc.).
""").
-spec refresh_validator(instance_id() | pid(), term()) ->
    ok | {error, applier_unavailable}.

refresh_validator(Target, Reason) ->
    case applier_pid_for(Target) of
        {ok, ApplierPid} ->
            bondy_oplog_applier:refresh_validator(ApplierPid, Reason);
        {error, _} = Err ->
            Err
    end.

%% =============================================================================
%% PAGE-LEVEL API (sync protocol)
%% =============================================================================

?DOC("""
Returns the subset of `Hashes` that this instance has, as a map of
`hash => page`. Hashes the instance does not have are silently absent
from the returned map.

Used by the sync protocol on the *responder* side: a peer asks for a
set of pages, this instance returns whichever it has.
""").
-spec get_pages(instance_id() | pid(), [bondy_mst:hash()]) ->
    #{bondy_mst:hash() => bondy_mst_page:t()}.

get_pages(Target, Hashes) when is_binary(Target), is_list(Hashes) ->
    %% Lock-free: pages live in the underlying store, addressable by
    %% hash. Read directly via the registry-published MST handle.
    case bondy_oplog_registry:mst(Target) of
        undefined ->
            error({noproc, {?MODULE, Target}});
        MST ->
            Store = bondy_mst:store(MST),
            lists:foldl(
                fun(Hash, Acc) ->
                    case bondy_mst_store:get(Store, Hash) of
                        undefined -> Acc;
                        Page -> Acc#{Hash => Page}
                    end
                end,
                #{},
                Hashes
            )
    end;
get_pages(Target, Hashes) when is_list(Hashes) ->
    gen_server:call(target(Target), {get_pages, Hashes}).

?DOC("""
Inserts a batch of pages received from a peer. Each page is verified
by re-hashing on insert; a hash mismatch (peer using a different
hash algorithm or malformed page) raises an error.

Used by the sync protocol on the *initiator* side after pulling pages
from a peer.
""").
-spec merge_pages(
    instance_id() | pid(),
    #{bondy_mst:hash() => bondy_mst_page:t()} | [bondy_mst_page:t()]
) -> ok.

merge_pages(Target, Pages) when is_map(Pages) ->
    merge_pages(Target, maps:values(Pages));
merge_pages(Target, Pages) when is_list(Pages) ->
    gen_server:call(target(Target), {merge_pages, Pages}, infinity).

?DOC("""
Returns the set of page hashes reachable from `Root` that this
instance does not have locally. Used by the sync protocol's initiator
to compute what to request from the peer.
""").
-spec missing_set(instance_id() | pid(), bondy_mst:hash()) ->
    [bondy_mst:hash()].

missing_set(Target, Root) when is_binary(Target), is_binary(Root) ->
    %% Lock-free: missing_set walks the store from Root computing what
    %% pages we lack. Pure read; safe off the gen_server.
    case bondy_oplog_registry:mst(Target) of
        undefined ->
            error({noproc, {?MODULE, Target}});
        MST ->
            Set = bondy_mst:missing_set(MST, Root),
            case is_list(Set) of
                true -> Set;
                false -> sets:to_list(Set)
            end
    end;
missing_set(Target, Root) when is_binary(Root) ->
    gen_server:call(target(Target), {missing_set, Root}).

?DOC("""
Integrates a peer's tree (identified by `PeerRoot`) into the local
MST. Pre-condition: every page reachable from `PeerRoot` must already
be present in the local store (caller's responsibility — typically
ensured by `missing_set/2` returning `[]` after page loading).

After this call, all events that were in the peer's tree are visible
to local queries, and the local root is the merged root of both trees.
""").
-spec integrate_peer_root(instance_id() | pid(), bondy_mst:hash()) -> ok.

integrate_peer_root(Target, PeerRoot) when is_binary(PeerRoot) ->
    gen_server:call(
        target(Target),
        {integrate_peer_root, PeerRoot},
        infinity
    ).

%% =============================================================================
%% GC / COMPACTION API
%% =============================================================================

?DOC("""
Returns the current compaction watermark — the highest event key that
has been folded into the snapshot. Events with keys ≤ watermark are
no longer in the MST.
""").
-spec current_watermark(instance_id() | pid()) ->
    undefined | bondy_oplog_event:event_key().

current_watermark(Target) when is_binary(Target) ->
    case ets_member(Target) of
        true -> bondy_oplog_registry:watermark(Target);
        false -> error({noproc, {?MODULE, Target}})
    end;
current_watermark(Target) ->
    gen_server:call(target(Target), current_watermark).

-spec crdt_module(instance_id() | pid()) -> module() | undefined.

crdt_module(Target) when is_binary(Target) ->
    case ets_member(Target) of
        true -> bondy_oplog_registry:crdt_module(Target);
        false -> error({noproc, {?MODULE, Target}})
    end;
crdt_module(Target) ->
    gen_server:call(target(Target), crdt_module).

?DOC("""
Returns `{ok, Watermark, Snapshot}` for the latest persisted snapshot,
or `not_found` if no compaction has run yet.
""").
-spec snapshot(instance_id() | pid()) ->
    {ok, bondy_oplog_event:event_key(), term()} | not_found.

snapshot(Target) when is_binary(Target) ->
    case bondy_oplog_registry:lookup(Target) of
        not_found -> error({noproc, {?MODULE, Target}});
        {ok, #{snapshot := undefined}} -> not_found;
        {ok, #{snapshot := {W, S}}} -> {ok, W, S}
    end;
snapshot(Target) ->
    gen_server:call(target(Target), get_snapshot).

?DOC("""
Runs one compaction cycle inside the instance gen_server.

`PeerRoots` is the list of root hashes confirmed at peers (from
`bondy_oplog_peer_state`). The instance:

1. Computes the stability frontier — the largest event key K such
   that every event with key ≤ K is reachable from every peer's root.
2. Extracts events in `(currentWatermark, frontier]`.
3. Calls the configured CRDT module's `interpret_cog/2` on top of
   the previous snapshot's state.
4. Persists the new snapshot at `frontier`.
5. Truncates the MST up to and including `frontier`.
6. Updates the watermark.

Returns `{ok, no_change}` if no advance is possible (no peers,
empty intersection, frontier ≤ current watermark); otherwise
`{ok, {compacted, NewWatermark, EventCount}}`.
""").
-spec compact(instance_id() | pid(), [bondy_mst:hash()]) ->
    {ok, no_change}
    | {ok, {compacted, bondy_oplog_event:event_key(), non_neg_integer()}}
    | {error, term()}.

compact(Target, PeerRoots) when is_list(PeerRoots) ->
    gen_server:call(target(Target), {compact, PeerRoots}, infinity).

?DOC("""
Bootstraps an instance by installing a peer's snapshot at `Watermark`.
Used by `bondy_oplog_sync_session:bootstrap/3` when a fresh
or far-behind replica joins a long-running cluster.

NOTE: not transactional with respect to a VM crash between
`put_snapshot` and the MST truncate. On the next start, events ≤ the
persisted watermark are filtered out at sync/append time, so the
transient overlap is self-correcting.

Atomic:

1. Persists the snapshot via the configured snapshot store.
2. Truncates the local MST up to and including `Watermark` (events ≤
   watermark are now in the snapshot, redundant in the live tree).
3. Advances the local watermark.
4. Updates the HLC to dominate `Watermark` so subsequent local
   appends sort above it.

Refuses to install a snapshot whose watermark is `=<` the current
watermark — going backwards would break monotonicity. Returns
`{ok, NewWatermark}` on success, `{error, watermark_not_advancing}`
otherwise.
""").
-spec load_snapshot(
    instance_id() | pid(),
    bondy_oplog_event:event_key(),
    term()
) -> {ok, bondy_oplog_event:event_key()} | {error, term()}.

load_snapshot(Target, Watermark, Snapshot) ->
    gen_server:call(
        target(Target),
        {load_snapshot, Watermark, Snapshot},
        infinity
    ).

%% =============================================================================
%% REGISTRY
%% =============================================================================

?DOC("""
Returns the pid of an instance owner gen_server, or `undefined` if no
such instance is currently running.
""").
-spec whereis(instance_id()) -> pid() | undefined.

whereis(InstanceId) when is_binary(InstanceId) ->
    case bondy_oplog_registry:instance_pid(InstanceId) of
        undefined ->
            undefined;
        Pid ->
            case is_process_alive(Pid) of
                true -> Pid;
                false -> undefined
            end
    end.

%% Binary-id callers go through the lock-free registry path; pid
%% callers pay a `gen_server:call` because the registry is keyed by
%% instance_id and a pid→id reverse lookup would cost an ETS
%% `select`. Pid callers are tests/internals only — see
%% `append_remote/2` and `origin/1` docstrings.
-spec lookup_origin(instance_id() | pid()) ->
    {ok, bondy_oplog_origin:t()} | not_found.

lookup_origin(Pid) when is_pid(Pid) ->
    {ok, gen_server:call(Pid, origin, 5000)};
lookup_origin(InstanceId) when is_binary(InstanceId) ->
    case bondy_oplog_registry:origin(InstanceId) of
        undefined -> not_found;
        Origin -> {ok, Origin}
    end.

%% =============================================================================
%% gen_server CALLBACKS
%% =============================================================================

init({InstanceId, Opts}) ->
    process_flag(trap_exit, true),
    Origin = maps:get(origin, Opts, bondy_oplog_origin:default()),
    case bondy_oplog_origin:validate(Origin) of
        ok -> ok;
        {error, R0} -> error({invalid_origin, R0})
    end,
    HLC = bondy_oplog_hlc:new(maps:get(hlc_seed, Opts, 0)),
    SeqRef = atomics:new(1, [{signed, false}]),
    ok = atomics:put(SeqRef, 1, maps:get(seq_seed, Opts, 0)),
    ValidatorMod = maps:get(
        validator, Opts, bondy_oplog_validator_trust
    ),
    {ok, ValidatorState} =
        ValidatorMod:init(InstanceId, maps:get(validator_opts, Opts, #{})),
    MergeMod = maps:get(
        merge_strategy,
        Opts,
        bondy_oplog_merge_strict_uniqueness
    ),
    Backend = maps:get(backend, Opts, ets),
    CrdtModForWarn = maps:get(crdt_module, Opts, undefined),
    case Backend =:= map andalso CrdtModForWarn =/= undefined of
        true ->
            ?LOG_WARNING(#{
                description =>
                    "instance configured with map_store backend and a "
                    "crdt_module: each lock-free read copies the entire "
                    "map from the registry to the caller. Suitable for "
                    "tests only; use ets or a stateful custom backend "
                    "in production",
                instance_id => InstanceId
            });
        false ->
            ok
    end,
    MST = open_mst(InstanceId, Backend, MergeMod, Opts),
    %% Stage 5: snapshot store + watermark recovery.
    SnapshotMod = maps:get(
        snapshot_store,
        Opts,
        bondy_oplog_snapshot_store_ets
    ),
    {ok, SnapshotState} = SnapshotMod:init(
        InstanceId, maps:get(snapshot_store_opts, Opts, #{})
    ),
    Watermark = SnapshotMod:current_watermark(SnapshotState),
    CachedSnapshot =
        case SnapshotMod:get_snapshot(SnapshotState) of
            {ok, W0, S0} -> {W0, S0};
            not_found -> undefined
        end,
    CrdtMod = maps:get(crdt_module, Opts, undefined),
    %% Seed HLC from the highest persisted event key, so a restart with
    %% a durable backend doesn't issue keys below the previous high
    %% water mark. Sources, in order of precedence:
    %%   1. The MST's max event key (live events past the watermark).
    %%   2. The snapshot's compaction watermark.
    %% Fresh instances (ETS backend, no snapshot) leave the HLC at 0.
    LiveSize = compute_live_size(MST),
    LastMSTKey =
        case bondy_mst:last(MST) of
            undefined -> undefined;
            {K0, _V0} -> K0
        end,
    case LastMSTKey of
        undefined when Watermark =/= undefined ->
            _ = bondy_oplog_hlc:update(
                HLC, bondy_oplog_event:key_hlc(Watermark)
            );
        undefined ->
            ok;
        K ->
            _ = bondy_oplog_hlc:update(HLC, bondy_oplog_event:key_hlc(K))
    end,
    %% Seed Seq similarly: if the MST has local-origin events, advance
    %% the Seq counter to dominate the highest seen.
    case max_local_seq(MST, Origin) of
        undefined -> ok;
        MaxSeq -> ok = atomics:put(SeqRef, 1, MaxSeq)
    end,
    %% Per-instance overlay (`ordered_set`, public, owned by this
    %% gen_server). Rows are `{Key, Value, Hlc, Origin}`.
    %% `ordered_set` so range reads (`fold_range/5`, `first_key/1`,
    %% `latest_key/1`) can streaming-merge it with the MST in key
    %% order. `public` so the applier-driven eviction can run via
    %% `ets:select_delete/2` from any process. No heir — the table
    %% dies with this process; a one_for_all subtree restart creates
    %% a fresh one.
    Overlay = ets:new(bondy_oplog_overlay, [
        ordered_set,
        public,
        {keypos, ?OVERLAY_KEY_POS},
        {read_concurrency, true},
        {write_concurrency, true},
        {decentralized_counters, true}
    ]),
    State = #state{
        instance_id = InstanceId,
        origin = Origin,
        hlc = HLC,
        seq = SeqRef,
        mst = MST,
        backend = Backend,
        validator_module = ValidatorMod,
        validator_state = ValidatorState,
        merge_strategy = MergeMod,
        crdt_module = CrdtMod,
        snapshot_store = SnapshotMod,
        snapshot_state = SnapshotState,
        watermark = Watermark,
        cached_snapshot = CachedSnapshot,
        max_working_set = maps:get(max_working_set, Opts, infinity),
        live_size = LiveSize,
        last_event_key = LastMSTKey,
        compaction = undefined,
        wal_pid = undefined,
        wal_pid_monitor = undefined,
        overlay = Overlay,
        max_overlay_events = maps:get(max_overlay_events, Opts, 10_000),
        max_overlay_bytes = maps:get(max_overlay_bytes, Opts, 5 * 1024 * 1024),
        overlay_throttle = maps:get(overlay_throttle, Opts, drop)
    },
    ok = publish(State),
    %% Publish the overlay tid via a dedicated setter so a stale tid
    %% from a previous instance (left behind in a registry row that
    %% outlived a one_for_all restart) is overwritten. Symmetric with
    %% `set_wal_pid/2` / `set_applier_pid/2`.
    ok = bondy_oplog_registry:set_overlay_tab(InstanceId, Overlay),
    {ok, State}.

handle_call(Req, From, State0) ->
    Result = do_handle_call(Req, From, State0),
    ok = maybe_publish(State0, Result),
    Result.

%% @private
%% Publishes the registry row when the handle_call clause produced a
%% state different from the one we entered with. The explicit
%% structural `=/=` is intentional: read-only clauses return the
%% same state and skip the ETS write; idempotent mutations (e.g.,
%% re-applying an event already in the MST) also fall through to a
%% no-op because the resulting record is structurally identical.
maybe_publish(State0, {reply, _, State1}) when State1 =/= State0 ->
    publish(State1);
maybe_publish(State0, {noreply, State1}) when State1 =/= State0 ->
    publish(State1);
maybe_publish(_State0, _Result) ->
    ok.

%% @private
do_handle_call({append, Op, Meta}, _From, State0) ->
    %% Pressure check → WAL append (fsync) → overlay insert → reply.
    %% The reply happens inline as soon as the WAL is durable and
    %% the overlay row exists; the applier drains the WAL and casts
    %% `install_local_batch` back to this gen_server, which promotes
    %% the event to the MST and evicts the overlay row.
    case admit(State0, 1) of
        ok ->
            case ensure_wal_pid(State0) of
                {ok, WalPid, State1} ->
                    case do_append_local(State1, WalPid, [{Op, Meta}]) of
                        {ok, [Key], State2} ->
                            {reply, Key, State2};
                        {error, wal_unavailable} ->
                            {reply, {error, wal_unavailable},
                             invalidate_wal_pid(State1)};
                        {error, _} = Err ->
                            {reply, Err, State1}
                    end;
                {error, _} = Err ->
                    {reply, Err, State0}
            end;
        {error, _} = Err ->
            {reply, Err, State0}
    end;
do_handle_call({append_many, Items}, _From, State0) ->
    case admit(State0, length(Items)) of
        ok ->
            case ensure_wal_pid(State0) of
                {ok, WalPid, State1} ->
                    case do_append_local(State1, WalPid, Items) of
                        {ok, Keys, State2} ->
                            {reply, Keys, State2};
                        {error, wal_unavailable} ->
                            {reply, {error, wal_unavailable},
                             invalidate_wal_pid(State1)};
                        {error, _} = Err ->
                            {reply, Err, State1}
                    end;
                {error, _} = Err ->
                    {reply, Err, State0}
            end;
        {error, _} = Err ->
            {reply, Err, State0}
    end;
do_handle_call(get_validator, _From, State) ->
    {reply, {State#state.validator_module, State#state.validator_state}, State};
do_handle_call(drain_install_queue, _From, State) ->
    %% Synchronisation barrier for the applier's commit boundary.
    %% Calls jump past casts in the mailbox order, so by the time
    %% this call is processed, every prior `install_local_batch`
    %% cast has been handled. The reply itself carries no payload.
    {reply, ok, State};
do_handle_call({install_remote, Event}, _From, State0) ->
    %% Sole install path for peer-received events. Signature
    %% verification ran in the applier process (see
    %% `bondy_oplog_applier:enqueue_remote/2`) before this call, so
    %% we trust the event and run the remaining accept/reject checks:
    %% origin-ban, backpressure, watermark filter, and the
    %% `bondy_mst:get` three-way (undefined/match/equivocation).
    Origin = bondy_oplog_event:key_origin(bondy_oplog_event:key(Event)),
    case bondy_oplog_origin_bans:is_banned(Origin) of
        true ->
            telemetry:execute(
                [bondy_oplog, instance, append_remote, banned],
                #{count => 1},
                #{instance_id => State0#state.instance_id, origin => Origin}
            ),
            {reply, {error, banned_origin}, State0};
        false ->
            %% Backpressure applies to remote events too — a
            %% misbehaving peer must not be able to drive our working
            %% set arbitrarily high. Idempotent re-receives slip past
            %% this check and are treated as no-ops downstream.
            case backpressure_admit(State0, 1) of
                {error, _} = BPErr ->
                    {reply, BPErr, State0};
                ok ->
                    case do_append_remote(State0, Event) of
                        {ok, State} ->
                            {reply, ok, State};
                        {error, _} = Error ->
                            {reply, Error, State0}
                    end
            end
    end;
do_handle_call({get, Key}, _From, #state{mst = MST, overlay = Overlay} = State) ->
    %% pid-targeted path: same overlay-first → MST order as the
    %% lock-free `get/2`.
    Reply =
        case overlay_lookup_tab(Overlay, Key) of
            {ok, _} = Hit ->
                Hit;
            not_found ->
                case bondy_mst:get(MST, Key) of
                    undefined -> not_found;
                    Value -> {ok, event_from_value(Key, Value)}
                end
        end,
    {reply, Reply, State};
do_handle_call(root_hash, _From, #state{mst = MST} = State) ->
    {reply, bondy_mst:root(MST), State};
do_handle_call(
    {fold_range, From, To, Fun, Acc0},
    _From,
    #state{mst = MST, overlay = Overlay} = State
) ->
    %% Streaming merge with strict ascending key order; overlay wins
    %% on conflict. The overlay queue is materialised once
    %% (`ets:select` on `ordered_set` yields key-ordered rows) and
    %% drained as the MST fold walks.
    OverlayQueue = overlay_range_tab(Overlay, From, To),
    Result = fold_range_merged(MST, From, To, OverlayQueue, Fun, Acc0),
    {reply, Result, State};
do_handle_call({truncate_prefix, Watermark}, _From, #state{mst = MST0} = State) ->
    %% Operator-driven prefix removal: collect keys ≤ Watermark and
    %% delete one by one (descending order — see the comment on
    %% truncate_below_or_equal/2 for the underlying `bondy_mst:delete/2`
    %% bug). Structural prefix-truncate touching only the leftmost path
    %% is a future optimisation in `bondy_mst` itself.
    %%
    %% Also advances `state.watermark` so the receive-side filter in
    %% `do_append_remote/2` rejects peer events with HLC ≤ Watermark.
    %% Without this, peers that have not yet seen the truncate would
    %% keep re-shipping the events we just dropped, defeating the
    %% purpose of the call. No snapshot is written at the new
    %% watermark — operator-driven truncate is documented as lossy for
    %% bootstrap consumers (see `bondy_oplog:truncate_prefix/2`). The
    %% watermark advance is monotone: a Watermark lower than the
    %% current `state.watermark` is ignored so compaction-set values
    %% are never regressed.
    Keys = bondy_mst:fold(
        MST0,
        fun
            ({K, _V}, Acc) when K =< Watermark -> [K | Acc];
            (_, Acc) -> Acc
        end,
        []
    ),
    MST1 = lists:foldl(
        fun(K, M) -> bondy_mst:delete(M, K) end,
        MST0,
        Keys
    ),
    Removed = length(Keys),
    NewWatermark = advance_watermark(State#state.watermark, Watermark),
    _ = bondy_oplog_hlc:update(
        State#state.hlc, bondy_oplog_event:key_hlc(Watermark)
    ),
    {reply, Removed, State#state{
        mst = MST1,
        live_size = max(0, State#state.live_size - Removed),
        watermark = NewWatermark
    }};
do_handle_call(instance_size, _From, #state{overlay = Overlay} = State) ->
    %% MST live_size + overlay rows = total events (disjoint sets;
    %% see `size/1`).
    Total = State#state.live_size + overlay_size_tab(Overlay),
    {reply, Total, State};
do_handle_call(origin, _From, State) ->
    {reply, State#state.origin, State};
do_handle_call(first_key, _From, #state{mst = MST, overlay = Overlay} = State) ->
    Reply = merge_first_key_tab(Overlay, MST),
    {reply, Reply, State};
do_handle_call(latest_key, _From, #state{mst = MST, overlay = Overlay} = State) ->
    Reply = merge_latest_key_tab(Overlay, MST),
    {reply, Reply, State};
do_handle_call(instance_id, _From, State) ->
    {reply, State#state.instance_id, State};
do_handle_call(info, _From, State) ->
    Info = #{
        instance_id => State#state.instance_id,
        origin => State#state.origin,
        backend => State#state.backend,
        validator => State#state.validator_module,
        merge_strategy => State#state.merge_strategy,
        last_event_key => State#state.last_event_key
    },
    {reply, Info, State};
do_handle_call({get_pages, Hashes}, _From, #state{mst = MST} = State) ->
    Store = bondy_mst:store(MST),
    Pages = lists:foldl(
        fun(Hash, Acc) ->
            case bondy_mst_store:get(Store, Hash) of
                undefined -> Acc;
                Page -> Acc#{Hash => Page}
            end
        end,
        #{},
        Hashes
    ),
    {reply, Pages, State};
do_handle_call({merge_pages, Pages}, _From, #state{mst = MST0} = State) ->
    %% Page collection during anti-entropy. We only insert pages into
    %% the underlying store — the local root is *not* changed here, so
    %% no event becomes visible to readers and live_size is unaffected.
    %% The actual merge (root advance) is deferred to integrate_peer_root,
    %% which runs once the sync session has loaded every required page.
    %% This matches the bondy_mst_crdt anti-entropy pattern: the local
    %% tree is only mutated when the merge is fully prepared.
    MST = lists:foldl(
        fun(Page, Acc0) ->
            {_Hash, Acc1} = bondy_mst:put_page(Acc0, Page),
            Acc1
        end,
        MST0,
        Pages
    ),
    {reply, ok, State#state{mst = MST}};
do_handle_call({missing_set, Root}, _From, #state{mst = MST} = State) ->
    Set = bondy_mst:missing_set(MST, Root),
    Reply =
        case is_list(Set) of
            true -> Set;
            false -> sets:to_list(Set)
        end,
    {reply, Reply, State};
do_handle_call(
    {integrate_peer_root, PeerRoot},
    _From,
    #state{mst = MST0} = State
) ->
    MST1 = bondy_mst:merge(MST0, MST0, PeerRoot),
    %% Watermark filter: if our compaction has advanced past some of
    %% the events in PeerRoot's tree, re-truncate to drop them.
    MST2 =
        case State#state.watermark of
            undefined -> MST1;
            W -> truncate_below_or_equal(MST1, W)
        end,
    %% HLC update: events received via merge may carry HLCs higher than
    %% our local clock. Advance the HLC to dominate the merged tree's
    %% max key so subsequent local appends sort after every received
    %% event — and stay above any future watermark.
    case bondy_mst:last(MST2) of
        undefined ->
            ok;
        {LastKey, _V} ->
            _ = bondy_oplog_hlc:update(
                State#state.hlc, bondy_oplog_event:key_hlc(LastKey)
            )
    end,
    {reply, ok, State#state{
        mst = MST2,
        live_size = compute_live_size(MST2)
    }};
do_handle_call(current_watermark, _From, State) ->
    {reply, State#state.watermark, State};
do_handle_call(crdt_module, _From, State) ->
    {reply, State#state.crdt_module, State};
do_handle_call(get_snapshot, _From, State) ->
    Reply = (State#state.snapshot_store):get_snapshot(
        State#state.snapshot_state
    ),
    {reply, Reply, State};
do_handle_call({compact, PeerRoots}, From, State) ->
    do_compact_async(State, PeerRoots, From);
do_handle_call({load_snapshot, NewWatermark, Snapshot}, _From, State) ->
    do_load_snapshot(State, NewWatermark, Snapshot);
do_handle_call(_Req, _From, State) ->
    {reply, {error, badcall}, State}.

handle_cast({install_local_batch, Events}, State0) ->
    %% Sole dispatch path for local-event MST installs. The applier
    %% verifies signatures in its own process and casts the surviving
    %% events here. We fold `install_event` over them in WAL order,
    %% publish once at the end (one ETS write per batch), then
    %% HLC-conditionally evict the matching overlay rows. MST publish
    %% strictly precedes overlay evict so a reader missing the
    %% overlay row finds the entry in the MST instead.
    State1 = install_local_batch(State0, Events),
    ok = publish(State1),
    ok = evict_overlay_batch(State1#state.overlay, Events),
    {noreply, State1};
handle_cast(
    {compaction_done, Pid, Result},
    #state{compaction = #{pid := Pid}} = State0
) ->
    State = commit_compaction(State0, Result),
    ok = publish(State),
    {noreply, State};
handle_cast({compaction_done, _StalePid, _Result}, State) ->
    %% Worker pid doesn't match — must be a stale message from a
    %% previously-aborted run. Ignore.
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(
    {'DOWN', _Ref, process, Pid, Reason},
    #state{compaction = #{pid := Pid, from := From}} = State
) ->
    %% Compaction worker exited. If exit was normal, the cast already
    %% drove commit; otherwise we need to surface the failure.
    case Reason of
        normal ->
            {noreply, State};
        _ ->
            ?LOG_WARNING(#{
                description => "compaction worker died abnormally",
                instance_id => State#state.instance_id,
                reason => Reason
            }),
            gen_server:reply(From, {error, {compaction_worker_died, Reason}}),
            {noreply, State#state{compaction = undefined}}
    end;
handle_info(
    {'DOWN', Ref, process, _Pid, _Reason},
    #state{wal_pid_monitor = Ref} = State
) ->
    %% Cached WAL pid has gone down (one_for_all restart); drop the
    %% cache so the next append re-resolves the new pid via the
    %% registry.
    {noreply, State#state{wal_pid = undefined, wal_pid_monitor = undefined}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{
    mst = MST,
    snapshot_store = SnapMod,
    snapshot_state = SnapState,
    overlay = Overlay
}) ->
    %% Leave the registry row in place so that on a one_for_all subtree
    %% restart the dyn_sup mapping (`sup_pid`) survives. The row's
    %% `instance_pid` field will be stale until the new instance
    %% gen_server's init runs and republishes; lock-free read paths
    %% use `is_process_alive/1` to detect that case.
    _ = catch SnapMod:close(SnapState),
    _ = catch bondy_mst:delete(MST),
    %% Drop the overlay — it dies with the instance, no heir, no
    %% survival across subtree restart. The applier reads the tid
    %% from the registry, and the registry row's `overlay_tab`
    %% becomes stale here until the next `init/1` republishes a
    %% fresh one. Applier-side reads tolerate `undefined`.
    _ = catch ets:delete(Overlay),
    ok.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
%% Builds a signed event for each `{Op, Meta}` item, hands the resulting
%% list to the per-instance WAL as a single atomic batch frame, then
%% inserts the events into the per-instance overlay so the caller's
%% next read sees them. The MST is **not** mutated here — the per-
%% instance applier drains the WAL, re-verifies each event's
%% signature, and casts `install_local_batch` back to this gen_server
%% which performs the actual MST install and overlay eviction. The
%% overlay row closes the read-your-writes gap until the applier
%% catches up; the row is evicted via HLC-conditional
%% `ets:select_delete/2` once the install lands.
do_append_local(#state{overlay = Overlay} = State0, WalPid, Items) ->
    {Events, Keys, State1} = build_events(State0, Items),
    try bondy_oplog_wal:append_batch(WalPid, Events) of
        {ok, _Entries} ->
            ok = stage_to_overlay(Overlay, Events),
            telemetry:execute(
                [bondy_oplog, instance, append],
                #{count => length(Events)},
                #{instance_id => State1#state.instance_id}
            ),
            {ok, Keys, State1};
        {error, _} = E ->
            E
    catch
        exit:{noproc, _} -> {error, wal_unavailable};
        exit:noproc -> {error, wal_unavailable};
        exit:{normal, _} -> {error, wal_unavailable};
        exit:{shutdown, _} -> {error, wal_unavailable}
    end.

%% @private
%% Inserts every event in the batch into the per-instance overlay as
%% one atomic `ets:insert/2` call. Origin is `local` for events that
%% went through the WAL; a future eager-push receiver will insert with
%% `eager_pushed` so the applier's eviction protocol can distinguish
%% the two (§10.3 of the applier design).
stage_to_overlay(Overlay, Events) ->
    Rows = [overlay_row(E, local) || E <- Events],
    true = ets:insert(Overlay, Rows),
    ok.

%% @private
overlay_row(Event, Origin) ->
    Key = bondy_oplog_event:key(Event),
    {
        Key,
        value_from_event(Event),
        bondy_oplog_event:key_hlc(Key),
        Origin
    }.

%% @private
%% Allocates a fresh `{HLC, Origin, Seq}` for each item, signs the
%% event via the configured validator, and threads the validator state
%% forward. Returns `{Events, Keys, NewState}`.
build_events(State0, Items) ->
    {EventsRev, KeysRev, State1} = lists:foldl(
        fun({Op, Meta}, {EvAcc, KAcc, S0}) ->
            HLC = bondy_oplog_hlc:now(S0#state.hlc),
            Seq = atomics:add_get(S0#state.seq, 1, 1),
            Key = bondy_oplog_event:key(HLC, S0#state.origin, Seq),
            Event0 = bondy_oplog_event:new(Key, Op, Meta),
            {Signed, VS1} =
                (S0#state.validator_module):sign_event(
                    Event0, S0#state.validator_state
                ),
            {[Signed | EvAcc], [Key | KAcc],
             S0#state{validator_state = VS1}}
        end,
        {[], [], State0},
        Items
    ),
    {lists:reverse(EventsRev), lists:reverse(KeysRev), State1}.

%% @private
%% Sole MST-install path for local-origin events. Driven by the
%% `install_local_batch` cast from the per-instance applier. The
%% applier has already re-verified every event's signature in its
%% own process before dispatching, so this fold trusts the input
%% and runs:
%%
%% 1. `bondy_mst:get` to decide between fresh insert, idempotent
%%    re-apply (same value), and collision-with-existing (different
%%    value at the same key, recorded as equivocation, MST left
%%    unchanged so the subtree survives bad input).
%% 2. `install_event` to mutate the MST and refresh state.
%%
%% Folding the whole batch before publishing avoids N registry
%% writes per batch.
-spec install_local_batch(#state{}, [bondy_oplog_event:t()]) -> #state{}.

install_local_batch(State, []) ->
    State;
install_local_batch(#state{mst = MST0} = State0, [Event | Rest]) ->
    Key = bondy_oplog_event:key(Event),
    NewValue = value_from_event(Event),
    State1 =
        case bondy_mst:get(MST0, Key) of
            undefined ->
                install_event(State0, Key, NewValue, apply_event, true);
            NewValue ->
                install_event(State0, Key, NewValue, apply_event, false);
            ExistingValue ->
                record_equivocation(State0, Key, ExistingValue, Event),
                State0
        end,
    install_local_batch(State1, Rest).

%% @private
%% Evicts every overlay row for a freshly-installed batch via a single
%% `ets:select_delete/2`. The HLC-conditional guard preserves any
%% newer row (Hlc > the maximum event HLC in the batch) that a
%% concurrent `append` may have staged for an already-installed key.
%% Such a "newer" row is by construction a *different* event with a
%% later HLC, so leaving it in the overlay is exactly the
%% read-your-writes contract.
-spec evict_overlay_batch(undefined | ets:tid(),
                          [bondy_oplog_event:t()]) -> ok.

evict_overlay_batch(undefined, _Events) ->
    ok;
evict_overlay_batch(_Tab, []) ->
    ok;
evict_overlay_batch(Tab, Events) ->
    MaxHlc = lists:foldl(
        fun(E, Acc) ->
            H = bondy_oplog_event:key_hlc(bondy_oplog_event:key(E)),
            case H > Acc of true -> H; false -> Acc end
        end,
        0,
        Events
    ),
    Keys = [bondy_oplog_event:key(E) || E <- Events],
    KeyGuard = build_key_or_guard(Keys),
    _ = try
        ets:select_delete(Tab, [{
            {'$1', '_', '$2', '_'},
            [KeyGuard, {'=<', '$2', MaxHlc}],
            [true]
        }])
    catch
        error:badarg -> 0
    end,
    ok.

%% @private
%% Builds a `{'orelse', {'=:=', '$1', Key1}, {'=:=', '$1', Key2}, ...}`
%% match-spec guard listing every batch key. ETS `select_delete` then
%% deletes only rows whose key matches one of these AND whose Hlc is
%% `=< MaxHlc`. We construct the guard explicitly (rather than per-
%% event in a separate match-spec body) so the whole batch is
%% deleted in one ETS call.
build_key_or_guard([Key]) ->
    {'=:=', '$1', {const, Key}};
build_key_or_guard([Key | Rest]) ->
    {'orelse', {'=:=', '$1', {const, Key}}, build_key_or_guard(Rest)}.

%% @private
%% Returns `{ok, WalPid, State1}` with `State1` carrying a monitored
%% cached pid so subsequent appends skip the registry lookup. If the
%% registry does not yet have a `wal_pid` (subtree mid-restart), the
%% cache is left empty and the caller surfaces `{error, wal_unavailable}`.
ensure_wal_pid(#state{wal_pid = Pid} = State) when is_pid(Pid) ->
    {ok, Pid, State};
ensure_wal_pid(#state{instance_id = Id} = State) ->
    case bondy_oplog_registry:wal_pid(Id) of
        undefined ->
            {error, wal_unavailable};
        Pid when is_pid(Pid) ->
            Ref = erlang:monitor(process, Pid),
            {ok, Pid, State#state{wal_pid = Pid, wal_pid_monitor = Ref}}
    end.

%% @private
%% Drops the cached WAL pid + its monitor. Used after a synchronous
%% append surfaces `noproc` so the next append rolls forward to the
%% new writer once the supervisor brings it up.
invalidate_wal_pid(#state{wal_pid_monitor = undefined} = State) ->
    State#state{wal_pid = undefined};
invalidate_wal_pid(#state{wal_pid_monitor = Ref} = State) ->
    _ = erlang:demonitor(Ref, [flush]),
    State#state{wal_pid = undefined, wal_pid_monitor = undefined}.

%% @private
%% Install path for peer-received events. The applier has already
%% re-verified the signature in its own process before forwarding
%% here, so this function trusts the input and runs the remaining
%% accept/reject logic:
%%
%% - Idempotent below-watermark filter (compaction may have advanced
%%   past this key already).
%% - `bondy_mst:get` three-way:
%%   - `undefined`: fresh insert.
%%   - bit-identical existing value: idempotent re-receive, no-op.
%%   - different existing value: equivocation; record proof in the
%%     quarantine table, leave the MST unchanged, return
%%     `{error, equivocation_detected}`. Keeping the gen_server alive
%%     on bad input avoids a crash-loop on poisoned peer traffic.
do_append_remote(#state{mst = MST0} = State, Event) ->
    Key = bondy_oplog_event:key(Event),
    _ = bondy_oplog_hlc:update(
        State#state.hlc, bondy_oplog_event:key_hlc(Key)
    ),
    case below_or_equal_watermark(Key, State#state.watermark) of
        true ->
            telemetry:execute(
                [bondy_oplog, instance, append_remote, filtered],
                #{count => 1},
                #{
                    instance_id => State#state.instance_id,
                    reason => below_watermark
                }
            ),
            {ok, State};
        false ->
            NewValue = value_from_event(Event),
            case bondy_mst:get(MST0, Key) of
                undefined ->
                    {ok, install_event(State, Key, NewValue, append_remote, true)};
                NewValue ->
                    %% Idempotent re-receive (bit-identical).
                    {ok, install_event(State, Key, NewValue, append_remote, false)};
                ExistingValue ->
                    record_equivocation(State, Key, ExistingValue, Event),
                    {error, equivocation_detected}
            end
    end.

%% @private
%% Shared insert path for `install_local_batch` (local-origin events
%% dispatched by the applier after S1 re-verify) and `do_append_remote`
%% (peer-received events). Mutates the MST, refreshes `last_event_key`
%% and `live_size`, advances the HLC, and emits a
%% `[bondy_oplog, instance, Source, ok]` telemetry event so callers
%% can tell the two paths apart in dashboards.
install_event(#state{} = State, Key, Value, Source, IsNew) ->
    MST1 = bondy_mst:put(State#state.mst, Key, Value),
    LastKey =
        case State#state.last_event_key of
            undefined -> Key;
            Prev when Key > Prev -> Key;
            Prev -> Prev
        end,
    _ = bondy_oplog_hlc:update(
        State#state.hlc, bondy_oplog_event:key_hlc(Key)
    ),
    SizeDelta =
        case IsNew of
            true -> 1;
            false -> 0
        end,
    telemetry:execute(
        [bondy_oplog, instance, Source, ok],
        #{count => 1},
        #{instance_id => State#state.instance_id, new => IsNew}
    ),
    State#state{
        mst = MST1,
        last_event_key = LastKey,
        live_size = State#state.live_size + SizeDelta
    }.

%% @private
record_equivocation(#state{} = State, Key, ExistingValue, IncomingEvent) ->
    ExistingEvent = event_from_value(Key, ExistingValue),
    Proof = (State#state.validator_module):detect_equivocation(
        ExistingEvent, IncomingEvent
    ),
    bondy_oplog_quarantine:record(
        State#state.instance_id,
        Key,
        ExistingEvent,
        IncomingEvent,
        Proof
    ),
    telemetry:execute(
        [bondy_oplog, instance, append_remote, equivocation],
        #{count => 1},
        #{
            instance_id => State#state.instance_id,
            origin => bondy_oplog_event:key_origin(Key)
        }
    ),
    ok.

%% @private
below_or_equal_watermark(_Key, undefined) ->
    false;
below_or_equal_watermark(Key, Watermark) ->
    Key =< Watermark.

%% @private
%% Returns the number of items currently in the MST. Linear in tree
%% size; used after merge/integrate where the size delta is unknown.
compute_live_size(MST) ->
    bondy_mst:fold(MST, fun(_, Acc) -> Acc + 1 end, 0).

%% @private
%% MST value shape: a 4-tuple of `{Op, Meta, PrevHash, Signature}`.
value_from_event(Event) ->
    {
        bondy_oplog_event:op(Event),
        bondy_oplog_event:meta(Event),
        bondy_oplog_event:prev_hash(Event),
        bondy_oplog_event:signature(Event)
    }.

%% @private
event_from_value(Key, {Op, Meta, PrevHash, Signature}) ->
    bondy_oplog_event:new(Key, Op, Meta, PrevHash, Signature).

%% @private
%% Returns the maximum `Seq` field among events whose origin matches
%% `LocalOrigin`. `undefined` if no such events exist. Used at init to
%% seed the per-origin Seq counter from persisted state.
max_local_seq(MST, LocalOrigin) ->
    bondy_mst:fold(
        MST,
        fun({K, _V}, Acc) ->
            case bondy_oplog_event:key_origin(K) of
                LocalOrigin ->
                    Seq = bondy_oplog_event:key_seq(K),
                    case Acc of
                        undefined -> Seq;
                        N when Seq > N -> Seq;
                        N -> N
                    end;
                _ ->
                    Acc
            end
        end,
        undefined
    ).

%% @private
%% Combined admission test: overlay pressure first, then the MST
%% working-set cap. Both checks are O(1) — overlay numbers come
%% from `ets:info/2`, working-set from cached `live_size`. The order
%% does not affect correctness because either failure is decisive;
%% overlay-first surfaces the more specific `backpressure` error
%% name when both would fire.
admit(State, Delta) ->
    case overlay_admit(State, Delta) of
        ok -> backpressure_admit(State, Delta);
        Err -> Err
    end.

%% @private
%% Pressure-check before the WAL append. Returns
%% `{error, backpressure}` when either cap is breached. `drop` is the
%% only supported strategy; `block` is reserved.
overlay_admit(
    #state{
        instance_id = Id,
        overlay = Overlay,
        max_overlay_events = MaxEvents,
        max_overlay_bytes = MaxBytes
    },
    Delta
) ->
    Size = ets:info(Overlay, size),
    case Size + Delta > MaxEvents of
        true ->
            emit_overlay_backpressure(Id, events, Size, MaxEvents, Delta),
            {error, backpressure};
        false ->
            %% `memory` is in words; convert to bytes with the runtime's
            %% word size. Approximate by design — sufficient for a
            %% backpressure threshold.
            MemBytes = ets:info(Overlay, memory) * erlang:system_info(wordsize),
            case MemBytes >= MaxBytes of
                true ->
                    emit_overlay_backpressure(Id, bytes, MemBytes, MaxBytes, Delta),
                    {error, backpressure};
                false ->
                    ok
            end
    end.

%% @private
emit_overlay_backpressure(Id, Dimension, Current, Cap, Delta) ->
    telemetry:execute(
        [bondy_oplog, instance, overlay, backpressure_drop],
        #{count => 1},
        #{
            instance_id => Id,
            dimension => Dimension,
            current => Current,
            cap => Cap,
            requested => Delta
        }
    ).

%% @private
%% Backpressure admission test. Returns `ok` if the instance can
%% absorb `Delta` more events under its `max_working_set` cap, or
%% `{error, working_set_full}` otherwise. `infinity` disables the
%% cap.
%%
%% The cap is on **total events visible to readers** = MST live_size
%% + overlay rows (matching `size/1`), because events arriving via
%% `append`/`append_many` enter the overlay before the applier
%% promotes them to the MST. Counting only `live_size` would let the
%% caller burst arbitrarily many writes into the overlay before the
%% cap fires.
backpressure_admit(#state{max_working_set = infinity}, _Delta) ->
    ok;
backpressure_admit(
    #state{
        max_working_set = Cap,
        live_size = Size,
        overlay = Overlay
    } = State,
    Delta
) ->
    Total = Size + overlay_size_tab(Overlay),
    case Total + Delta =< Cap of
        true ->
            ok;
        false ->
            telemetry:execute(
                [bondy_oplog, instance, backpressure],
                #{count => 1},
                #{
                    instance_id => State#state.instance_id,
                    requested => Delta,
                    live_size => Size,
                    overlay_size => Total - Size,
                    cap => Cap
                }
            ),
            {error, working_set_full}
    end.

%% @private
%% Drops every key in MST that is `=< Watermark`. Used both by explicit
%% truncation and post-merge re-truncation. Linear in the prefix size;
%% a structural prefix-truncate that touches only the leftmost path
%% would be the long-term optimisation.
%%
%% Deletes are issued in **descending** order (highest first within the
%% to-remove prefix). The underlying `bondy_mst:delete/2` has been
%% observed to leave a page in an invalid state when keys are deleted
%% in ascending order — manifesting as a `case_clause` in
%% `bondy_mst:first/2`. Descending order avoids the pathological path.
%% @private
%% Monotone watermark advance: returns whichever of the two values is
%% higher, treating `undefined` as the bottom. Used by both compaction
%% (via direct assignment, which is safe by construction — the worker
%% rejects frontiers ≤ current watermark) and operator-driven
%% `truncate_prefix`, where the caller's value could in principle be
%% lower than a previously installed compaction watermark.
advance_watermark(undefined, New) -> New;
advance_watermark(Cur, New) when New > Cur -> New;
advance_watermark(Cur, _New) -> Cur.

%% @private
truncate_below_or_equal(MST, Watermark) ->
    %% Collect keys ≤ Watermark *in descending order* (the fold cons-es
    %% in ascending order; we keep them ascending and reverse only when
    %% we want descending — here we want descending, so we don't reverse).
    Keys = bondy_mst:fold(
        MST,
        fun
            ({K, _V}, Acc) when K =< Watermark -> [K | Acc];
            (_, Acc) -> Acc
        end,
        []
    ),
    lists:foldl(fun(K, M) -> bondy_mst:delete(M, K) end, MST, Keys).

%% @private
%% Spawns a worker that does the heavy compaction work (frontier
%% computation, event fold, `interpret_cog`, snapshot persist) off the
%% gen_server. The gen_server only runs the final atomic commit
%% (truncate + watermark advance + HLC bump) when the worker reports
%% back via {compaction_done, ...} cast. Local appends and reads
%% proceed concurrently throughout.
%%
%% Concurrency guard: only one compaction in flight per instance.
%% Repeated `compact` requests while one is running reply
%% `{ok, no_change}` immediately — compaction is idempotent and
%% scheduled-driven, so the next tick will retry.
do_compact_async(#state{compaction = InFlight} = State, _PeerRoots, _From) when
    InFlight =/= undefined
->
    {reply, {ok, no_change}, State};
do_compact_async(#state{crdt_module = undefined} = State, _PeerRoots, _From) ->
    {reply, {error, no_crdt_module}, State};
do_compact_async(#state{} = State, PeerRoots, From) ->
    Self = self(),
    %% Capture exactly the data the worker needs. The State at
    %% commit time may differ (new appends), but truncation only
    %% removes events ≤ Frontier, which by construction were already
    %% present when the worker ran.
    MST = State#state.mst,
    Watermark0 = State#state.watermark,
    SnapshotStore = State#state.snapshot_store,
    SnapshotState = State#state.snapshot_state,
    CachedSnapshot = State#state.cached_snapshot,
    CrdtMod = State#state.crdt_module,
    InstanceId = State#state.instance_id,
    {Pid, _Ref} = spawn_monitor(fun() ->
        Result = run_compaction_worker(
            InstanceId,
            MST,
            Watermark0,
            PeerRoots,
            SnapshotStore,
            SnapshotState,
            CachedSnapshot,
            CrdtMod
        ),
        gen_server:cast(Self, {compaction_done, self(), Result})
    end),
    Started = erlang:monotonic_time(),
    Compaction = #{pid => Pid, from => From, started_at => Started},
    {noreply, State#state{compaction = Compaction}}.

%% @private
run_compaction_worker(
    InstanceId,
    MST,
    Watermark0,
    PeerRoots,
    SnapshotStore,
    SnapshotState,
    CachedSnapshot,
    CrdtMod
) ->
    try
        case compute_frontier_for(MST, PeerRoots) of
            undefined ->
                {ok, no_change};
            Frontier when
                Watermark0 =/= undefined,
                Frontier =< Watermark0
            ->
                {ok, no_change};
            Frontier ->
                Events = events_in_open_range(MST, Watermark0, Frontier),
                BaseSnapshot =
                    case CachedSnapshot of
                        undefined ->
                            case SnapshotStore:get_snapshot(SnapshotState) of
                                {ok, _W, S} -> S;
                                not_found -> CrdtMod:init()
                            end;
                        {_, S0} ->
                            S0
                    end,
                NewSnapshot = CrdtMod:interpret_cog(Events, BaseSnapshot),
                ok = SnapshotStore:put_snapshot(
                    SnapshotState, Frontier, NewSnapshot
                ),
                {ok, {compacted, Frontier, NewSnapshot, length(Events)}}
        end
    catch
        Class:Reason:Stack ->
            ?LOG_ERROR(#{
                description => "compaction worker raised",
                instance_id => InstanceId,
                class => Class,
                reason => Reason,
                stacktrace => Stack
            }),
            {error, {compaction_failed, Class, Reason}}
    end.

%% @private
%% Same algorithm as compute_frontier/2 but takes the captured MST
%% directly so it can run in the worker process.
compute_frontier_for(_MST, []) ->
    undefined;
compute_frontier_for(MST, PeerRoots) ->
    PeerSets = [keys_set_at_root(MST, R) || R <- PeerRoots, is_binary(R)],
    case PeerSets of
        [] ->
            undefined;
        [_ | _] ->
            LocalKeys = lists:reverse(
                bondy_mst:fold(
                    MST,
                    fun({K, _V}, Acc) -> [K | Acc] end,
                    []
                )
            ),
            longest_common_prefix(LocalKeys, PeerSets, undefined)
    end.

%% @private
%% Commits the worker's result atomically inside the gen_server.
%% Truncation uses the *current* state.mst — events appended during
%% the worker's run sort above Frontier and are preserved.
commit_compaction(
    #state{compaction = #{from := From, started_at := Started}} = State,
    {ok, no_change}
) ->
    gen_server:reply(From, {ok, no_change}),
    Duration = erlang:monotonic_time() - Started,
    telemetry:execute(
        [bondy_oplog, compaction, ok],
        #{duration => Duration, event_count => 0},
        #{instance_id => State#state.instance_id, frontier => undefined}
    ),
    State#state{compaction = undefined};
commit_compaction(
    #state{compaction = #{from := From, started_at := Started}} = State,
    {ok, {compacted, Frontier, NewSnapshot, EventCount}}
) ->
    MST1 = truncate_below_or_equal(State#state.mst, Frontier),
    _ = bondy_oplog_hlc:update(
        State#state.hlc, bondy_oplog_event:key_hlc(Frontier)
    ),
    State1 = State#state{
        mst = MST1,
        watermark = Frontier,
        cached_snapshot = {Frontier, NewSnapshot},
        live_size = max(0, State#state.live_size - EventCount),
        compaction = undefined
    },
    Duration = erlang:monotonic_time() - Started,
    telemetry:execute(
        [bondy_oplog, compaction, ok],
        #{duration => Duration, event_count => EventCount},
        #{instance_id => State#state.instance_id, frontier => Frontier}
    ),
    gen_server:reply(From, {ok, {compacted, Frontier, EventCount}}),
    State1;
commit_compaction(
    #state{compaction = #{from := From}} = State,
    {error, _} = Error
) ->
    gen_server:reply(From, Error),
    State#state{compaction = undefined}.

%% @private
%% Bootstrap: install a peer-supplied snapshot at the given watermark.
%% See `load_snapshot/3` for the contract.
%%
%% Refuses to run while a compaction worker is in flight — the two
%% operations both mutate the watermark/snapshot, and serialising them
%% is the simplest correctness story.
do_load_snapshot(#state{compaction = InFlight} = State, _, _) when
    InFlight =/= undefined
->
    {reply, {error, compaction_in_progress}, State};
do_load_snapshot(State, NewWatermark, Snapshot) ->
    case State#state.watermark of
        undefined ->
            apply_loaded_snapshot(State, NewWatermark, Snapshot);
        Current when NewWatermark > Current ->
            apply_loaded_snapshot(State, NewWatermark, Snapshot);
        _ ->
            {reply, {error, watermark_not_advancing}, State}
    end.

%% @private
apply_loaded_snapshot(State, NewWatermark, Snapshot) ->
    ok = (State#state.snapshot_store):put_snapshot(
        State#state.snapshot_state, NewWatermark, Snapshot
    ),
    %% Drop any live events that the new snapshot already covers.
    MST1 = truncate_below_or_equal(State#state.mst, NewWatermark),
    LiveSize1 = compute_live_size(MST1),
    %% Advance HLC to keep future local appends above the watermark.
    _ = bondy_oplog_hlc:update(
        State#state.hlc, bondy_oplog_event:key_hlc(NewWatermark)
    ),
    State1 = State#state{
        mst = MST1,
        watermark = NewWatermark,
        cached_snapshot = {NewWatermark, Snapshot},
        live_size = LiveSize1
    },
    {reply, {ok, NewWatermark}, State1}.

%% @private
%% Returns events in the half-open range (Watermark0, Frontier], in
%% key order. If Watermark0 is `undefined`, the range starts at
%% min_key (inclusive of all events ≤ Frontier).
events_in_open_range(MST, undefined, Frontier) ->
    lists:reverse(
        bondy_mst:fold(
            MST,
            fun
                ({K, V}, Acc) when K =< Frontier ->
                    [event_from_value(K, V) | Acc];
                (_, Acc) ->
                    Acc
            end,
            []
        )
    );
events_in_open_range(MST, W0, Frontier) ->
    lists:reverse(
        bondy_mst:fold(
            MST,
            fun
                ({K, V}, Acc) when K > W0, K =< Frontier ->
                    [event_from_value(K, V) | Acc];
                (_, Acc) ->
                    Acc
            end,
            []
        )
    ).

%% @private
%% Stability frontier:
%%   "the largest event key K such that every event with key ≤ K
%%    is reachable from every peer's confirmed root"
%%
%% Algorithm:
%%   1. For each PeerRoot, compute the set of keys reachable.
%%   2. Intersect with the local key set in key order.
%%   3. Return the largest K such that all keys up to and including K
%%      are in every peer's set.
%%
%% See `compute_frontier_for/2` for the worker-process variant used
%% during async compaction.

%% @private
keys_set_at_root(MST, Root) ->
    bondy_mst:fold(
        MST,
        fun({K, _V}, Acc) -> sets:add_element(K, Acc) end,
        sets:new([{version, 2}]),
        [{root, Root}]
    ).

%% @private
longest_common_prefix([], _PeerSets, Acc) ->
    Acc;
longest_common_prefix([K | Rest], PeerSets, Acc) ->
    case lists:all(fun(S) -> sets:is_element(K, S) end, PeerSets) of
        true -> longest_common_prefix(Rest, PeerSets, K);
        false -> Acc
    end.

%% @private
%% Builds the underlying MST struct.
open_mst(InstanceId, Backend, MergeMod, Opts) ->
    StoreMod = backend_module(Backend),
    StoreOpts = backend_opts(Backend, InstanceId, Opts),
    HashAlgo = maps:get(hash_algorithm, Opts, sha256),
    bondy_mst:new(#{
        store => StoreMod,
        store_opts => StoreOpts,
        hash_algorithm => HashAlgo,
        merger => fun(K, V1, V2) -> MergeMod:merge(K, V1, V2) end
    }).

%% @private
backend_module(map) -> bondy_mst_map_store;
backend_module(ets) -> bondy_mst_ets_store;
backend_module(Mod) when is_atom(Mod) -> Mod.

%% @private
backend_opts(ets, InstanceId, Opts) ->
    Defaults = #{name => InstanceId},
    maps:merge(Defaults, maps:get(backend_options, Opts, #{}));
backend_opts(_, InstanceId, Opts) ->
    Base = maps:get(backend_options, Opts, #{}),
    case maps:find(storage_path, Opts) of
        {ok, BaseDir} ->
            Strategy = maps:get(
                path_strategy, Opts, bondy_oplog_path_sharded
            ),
            Path = Strategy:storage_path(InstanceId, BaseDir),
            Base#{storage_path => unicode:characters_to_binary(Path)};
        error ->
            Base
    end.

%% @private
%% Publishes the current state's read-relevant fields to the registry.
%% Called after every state-mutating handle_call so that lock-free
%% read paths see fresh data without round-tripping the gen_server.
publish(#state{} = State) ->
    bondy_oplog_registry:publish(#{
        instance_id => State#state.instance_id,
        instance_pid => self(),
        origin => State#state.origin,
        mst => State#state.mst,
        watermark => State#state.watermark,
        snapshot => State#state.cached_snapshot,
        crdt_module => State#state.crdt_module,
        live_size => State#state.live_size
    }).

%% @private
ets_member(InstanceId) ->
    bondy_oplog_registry:instance_pid(InstanceId) =/= undefined.

%% @private
%% Lock-free overlay lookup by InstanceId. Resolves the overlay tid
%% from the registry then delegates to `overlay_lookup_tab/2`.
%% Returns `not_found` if the registry has no overlay tid yet (subtree
%% mid-restart) so the caller falls through to the MST.
overlay_lookup(InstanceId, Key) when is_binary(InstanceId) ->
    case bondy_oplog_registry:overlay_tab(InstanceId) of
        undefined -> not_found;
        Tab -> overlay_lookup_tab(Tab, Key)
    end.

%% @private
%% Shape: `{Key, Value, Hlc, Origin}` per ?OVERLAY_KEY_POS macros.
overlay_lookup_tab(Tab, Key) ->
    try ets:lookup(Tab, Key) of
        [{Key, Value, _Hlc, _Origin}] -> {ok, event_from_value(Key, Value)};
        [] -> not_found
    catch
        %% Tolerates a torn-down table during one_for_all restart.
        error:badarg -> not_found
    end.

%% @private
%% Returns overlay rows in `[From, To]` as a sorted list of
%% `{Key, Event}` tuples. `ets:select/2` on `ordered_set` yields rows
%% in key order, so the result list is already sorted. Returns `[]`
%% when the overlay tid is missing (subtree mid-restart).
overlay_range(InstanceId, From, To) when is_binary(InstanceId) ->
    case bondy_oplog_registry:overlay_tab(InstanceId) of
        undefined -> [];
        Tab -> overlay_range_tab(Tab, From, To)
    end.

%% @private
overlay_range_tab(undefined, _From, _To) ->
    [];
overlay_range_tab(Tab, From, To) ->
    MatchSpec = [{
        {'$1', '$2', '_', '_'},
        [
            {'>=', '$1', {const, From}},
            {'=<', '$1', {const, To}}
        ],
        [{{'$1', '$2'}}]
    }],
    try ets:select(Tab, MatchSpec) of
        Rows -> [{K, event_from_value(K, V)} || {K, V} <- Rows]
    catch
        error:badarg -> []
    end.

%% @private
%% Streaming merge of an MST fold with a pre-sorted overlay queue.
%% Returns the user accumulator after every entry in `[From, To]`
%% from both sources has been yielded in strict ascending key order.
%% Overlay wins on tied keys.
fold_range_merged(MST, From, To, OverlayQueue, Fun, Acc0) ->
    {Leftover, Acc1} = bondy_mst:fold(
        MST,
        fun({K, V}, {Queue, A}) ->
            case K >= From andalso K =< To of
                false -> {Queue, A};
                true -> merge_step(Queue, K, V, Fun, A)
            end
        end,
        {OverlayQueue, Acc0}
    ),
    drain_overlay_queue(Leftover, Fun, Acc1).

%% @private
merge_step([{OK, OEvent} | Rest], MstK, _MstV, Fun, Acc) when OK < MstK ->
    %% Overlay key strictly precedes MST key: yield overlay, recurse
    %% so we keep emitting overlay rows below the current MST entry.
    merge_step(Rest, MstK, _MstV, Fun, Fun(OEvent, Acc));
merge_step([{MstK, OEvent} | Rest], MstK, _MstV, Fun, Acc) ->
    %% Tied keys: overlay-wins; do not also emit the MST value.
    {Rest, Fun(OEvent, Acc)};
merge_step(Queue, MstK, MstV, Fun, Acc) ->
    %% Overlay queue empty, or its head is greater than MstK: emit
    %% MST entry.
    {Queue, Fun(event_from_value(MstK, MstV), Acc)}.

%% @private
drain_overlay_queue([], _Fun, Acc) ->
    Acc;
drain_overlay_queue([{_K, Event} | Rest], Fun, Acc) ->
    drain_overlay_queue(Rest, Fun, Fun(Event, Acc)).

%% @private
%% Min over (overlay.first, MST.first). Either can be empty.
merge_first_key(InstanceId, MST) ->
    OverlayFirst = overlay_first_key(InstanceId),
    MstFirst = case bondy_mst:first(MST) of
        undefined -> undefined;
        {K, _V} -> K
    end,
    min_key(OverlayFirst, MstFirst).

%% @private
merge_first_key_tab(Tab, MST) ->
    OverlayFirst = overlay_first_key_tab(Tab),
    MstFirst = case bondy_mst:first(MST) of
        undefined -> undefined;
        {K, _V} -> K
    end,
    min_key(OverlayFirst, MstFirst).

%% @private
%% Max over (overlay.last, MST.last). Either can be empty.
merge_latest_key(InstanceId, MST) ->
    OverlayLast = overlay_last_key(InstanceId),
    MstLast = case bondy_mst:last(MST) of
        undefined -> undefined;
        {K, _V} -> K
    end,
    max_key(OverlayLast, MstLast).

%% @private
merge_latest_key_tab(Tab, MST) ->
    OverlayLast = overlay_last_key_tab(Tab),
    MstLast = case bondy_mst:last(MST) of
        undefined -> undefined;
        {K, _V} -> K
    end,
    max_key(OverlayLast, MstLast).

%% @private
overlay_first_key(InstanceId) ->
    case bondy_oplog_registry:overlay_tab(InstanceId) of
        undefined -> undefined;
        Tab -> overlay_first_key_tab(Tab)
    end.

%% @private
overlay_first_key_tab(undefined) ->
    undefined;
overlay_first_key_tab(Tab) ->
    try ets:first(Tab) of
        '$end_of_table' -> undefined;
        K -> K
    catch
        error:badarg -> undefined
    end.

%% @private
overlay_last_key(InstanceId) ->
    case bondy_oplog_registry:overlay_tab(InstanceId) of
        undefined -> undefined;
        Tab -> overlay_last_key_tab(Tab)
    end.

%% @private
overlay_last_key_tab(undefined) ->
    undefined;
overlay_last_key_tab(Tab) ->
    try ets:last(Tab) of
        '$end_of_table' -> undefined;
        K -> K
    catch
        error:badarg -> undefined
    end.

%% @private
overlay_size(InstanceId) when is_binary(InstanceId) ->
    case bondy_oplog_registry:overlay_tab(InstanceId) of
        undefined -> 0;
        Tab -> overlay_size_tab(Tab)
    end.

%% @private
overlay_size_tab(undefined) ->
    0;
overlay_size_tab(Tab) ->
    try ets:info(Tab, size) of
        N when is_integer(N) -> N;
        _ -> 0
    catch
        error:badarg -> 0
    end.

%% @private
min_key(undefined, undefined) -> empty;
min_key(undefined, K) -> {ok, K};
min_key(K, undefined) -> {ok, K};
min_key(A, B) when A =< B -> {ok, A};
min_key(_, B) -> {ok, B}.

%% @private
max_key(undefined, undefined) -> empty;
max_key(undefined, K) -> {ok, K};
max_key(K, undefined) -> {ok, K};
max_key(A, B) when A >= B -> {ok, A};
max_key(_, B) -> {ok, B}.

%% @private
target(Pid) when is_pid(Pid) ->
    Pid;
target(InstanceId) when is_binary(InstanceId) ->
    case ?MODULE:whereis(InstanceId) of
        undefined -> error({noproc, {?MODULE, InstanceId}});
        Pid -> Pid
    end;
target(Other) ->
    error({invalid_target, Other}).
