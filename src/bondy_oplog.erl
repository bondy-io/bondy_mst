%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog).

-include("bondy_mst.hrl").
-include("bondy_oplog.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Public façade for the operation-log replication framework.

Each replicated value is an **instance**: an append-only operation log
keyed by `{HLC, Origin, Seq}`, stored in a Merkle Search Tree. Stable
prefixes of the log collapse into snapshots through a
consumer-defined `interpret_cog/2` function.

## Attribution

The Concurrent Operation Group (COG) abstraction, the operation-log
framing, and the equivocation-tolerance approach via hash-chaining
are taken from Preston McCrary's *Canteen* (UC Berkeley, 2022 —
EECS-2022-160). The MST substrate underneath comes from Auvolat &
Taïani (Inria/IRISA, SRDS 2019 — HAL-02303490). See `README.md`
"Credits" for full references.

## API surface

Lifecycle primitives are intentionally minimal: `start_instance/1,2`,
`stop_instance/1,2`, `list_instances/0`, `discover_instances/1`. The
library does not impose lifecycle policy — *when* and *how often* to
call these is the consumer's choice. Lazy loading, LRU eviction,
cold-tier offload, and per-tenant policies belong to the consumer.

Per-instance event operations pass through to
`bondy_oplog_instance`.
""").

%% Lifecycle
-export([start_instance/1]).
-export([start_instance/2]).
-export([stop_instance/1]).
-export([stop_instance/2]).
-export([list_instances/0]).
-export([discover_instances/1]).
-export([discover_instances/2]).

%% Per-instance API (pass-through to bondy_oplog_instance)
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

%% Sync
-export([sync/2]).
-export([sync/3]).
-export([sync_async/2]).
-export([sync_async/3]).
-export([bootstrap/2]).
-export([bootstrap/3]).

%% GC / queries
-export([compact/1]).
-export([current_watermark/1]).
-export([snapshot/1]).
-export([query/2]).
-export([query_stable/2]).

%% =============================================================================
%% LIFECYCLE
%% =============================================================================

?DOC("""
Starts an instance with default options.
""").
-spec start_instance(instance_id()) -> {ok, pid()} | {error, term()}.

start_instance(InstanceId) when is_binary(InstanceId) ->
    start_instance(InstanceId, #{}).

?DOC("""
Starts an instance. Returns the pid of the per-instance supervisor.
Idempotent: re-starting a running instance returns its existing
supervisor pid.
""").
-spec start_instance(
    instance_id(),
    bondy_oplog_instance:opts()
) -> {ok, pid()} | {error, term()}.

start_instance(InstanceId, Opts) when
    is_binary(InstanceId), is_map(Opts)
->
    bondy_oplog_instance_dyn_sup:start_instance(InstanceId, Opts).

-spec stop_instance(instance_id()) -> ok | {error, not_found}.

stop_instance(InstanceId) ->
    stop_instance(InstanceId, #{}).

?DOC("""
Stops an instance. The `Opts` map is currently unused; a future
`destroy => true` option to also delete the instance's on-disk state
is reserved.
""").
-spec stop_instance(instance_id(), map()) -> ok | {error, not_found}.

stop_instance(InstanceId, _Opts) when is_binary(InstanceId) ->
    case bondy_oplog_instance_dyn_sup:stop_instance(InstanceId) of
        ok ->
            %% Drop node-shared registry rows for the now-gone instance.
            %% Best-effort: if a registry isn't running (e.g. tests
            %% bring up only part of the tree) we silently skip.
            _ =
                catch bondy_oplog_peer_state:forget_instance(
                    InstanceId
                ),
            _ =
                catch bondy_oplog_quarantine:forget_instance(
                    InstanceId
                ),
            ok;
        Other ->
            Other
    end.

?DOC("""
Lists currently-running instances on this node. Order unspecified.
""").
-spec list_instances() -> [instance_id()].

list_instances() ->
    Children = supervisor:which_children(
        bondy_oplog_instance_dyn_sup
    ),
    [
        InstanceId
     || {_Id, SupPid, supervisor, _} <- Children,
        is_pid(SupPid),
        InstancePid <- [bondy_oplog_instance_sup:instance_pid(SupPid)],
        is_pid(InstancePid),
        #{instance_id := InstanceId} <-
            [bondy_oplog_instance:info(InstancePid)]
    ].

?DOC("""
Discovers instances on disk under `BaseDir`, using the sharded path
strategy (the library default). Suitable for boot-time enumeration.
""").
-spec discover_instances(BaseDir :: binary()) -> [instance_id()].

discover_instances(BaseDir) ->
    discover_instances(BaseDir, bondy_oplog_path_sharded).

-spec discover_instances(BaseDir :: binary(), Strategy :: module()) ->
    [instance_id()].

discover_instances(BaseDir, Strategy) when
    is_binary(BaseDir), is_atom(Strategy)
->
    Strategy:discover(BaseDir).

%% =============================================================================
%% PER-INSTANCE API
%% =============================================================================

-spec append(instance_id(), bondy_oplog_event:op()) ->
    bondy_oplog_event:event_key().

append(InstanceId, Op) ->
    bondy_oplog_instance:append(InstanceId, Op).

-spec append(
    instance_id(),
    bondy_oplog_event:op(),
    bondy_oplog_event:meta()
) -> bondy_oplog_event:event_key().

append(InstanceId, Op, Meta) ->
    bondy_oplog_instance:append(InstanceId, Op, Meta).

-spec append_many(
    instance_id(),
    [{bondy_oplog_event:op(), bondy_oplog_event:meta()}]
) -> [bondy_oplog_event:event_key()].

append_many(InstanceId, Items) ->
    bondy_oplog_instance:append_many(InstanceId, Items).

-spec append_remote(instance_id(), bondy_oplog_event:t()) ->
    ok | {error, term()}.

append_remote(InstanceId, Event) ->
    bondy_oplog_instance:append_remote(InstanceId, Event).

-spec await_apply(instance_id()) -> ok | {error, timeout}.

await_apply(InstanceId) ->
    bondy_oplog_instance:await_apply(InstanceId).

-spec await_apply(instance_id(), timeout()) -> ok | {error, timeout}.

await_apply(InstanceId, Timeout) ->
    bondy_oplog_instance:await_apply(InstanceId, Timeout).

-spec get(instance_id(), bondy_oplog_event:event_key()) ->
    {ok, bondy_oplog_event:t()} | not_found.

get(InstanceId, Key) ->
    bondy_oplog_instance:get(InstanceId, Key).

-spec root_hash(instance_id()) -> binary() | undefined.

root_hash(InstanceId) ->
    %% Drain the applier so the returned root reflects every event
    %% the caller has already `append/2`-ed. Callers comparing roots
    %% across nodes after a write expect read-your-writes semantics.
    _ = bondy_oplog_instance:await_apply(InstanceId),
    bondy_oplog_instance:root_hash(InstanceId).

-spec fold_range(
    instance_id(),
    From :: bondy_oplog_event:event_key(),
    To :: bondy_oplog_event:event_key(),
    fun((bondy_oplog_event:t(), Acc) -> Acc),
    Acc
) -> Acc when Acc :: term().

fold_range(InstanceId, From, To, Fun, Acc0) ->
    bondy_oplog_instance:fold_range(InstanceId, From, To, Fun, Acc0).

-spec range(
    instance_id(),
    From :: bondy_oplog_event:event_key(),
    To :: bondy_oplog_event:event_key()
) -> [bondy_oplog_event:t()].

range(InstanceId, From, To) ->
    bondy_oplog_instance:range(InstanceId, From, To).

?DOC("""
Operator-driven removal of every event with key `=< Watermark` from
the live MST.

Advances `current_watermark/1` to `Watermark` (monotonically — a value
lower than the current watermark is ignored), so peer events arriving
later with HLC `=< Watermark` are rejected by the receive-side filter
instead of being re-installed. Without this, a peer that has not yet
seen the truncate would keep re-shipping the events we just dropped.

**No snapshot is written.** Events between the previous snapshot's
watermark and the new truncate watermark are *unrecoverable* by a
bootstrapping peer — that peer would receive the older snapshot and
then be rejected for every event in the gap. Use this only when the
operator has out-of-band evidence that the dropped events are safe to
lose cluster-wide. For coordinated retention with a snapshot, use
`compact/1` instead.

Returns the number of MST rows removed.
""").
-spec truncate_prefix(instance_id(), bondy_oplog_event:event_key()) ->
    non_neg_integer().

truncate_prefix(InstanceId, Watermark) ->
    %% Drain the local applier so truncation operates on the up-to-date
    %% MST. Without this, overlay-pending events whose keys are
    %% `=< Watermark` are invisible to the MST-level fold and survive
    %% the truncate — they are then installed by the applier *after*
    %% truncate_prefix has returned, leaving the caller with rows the
    %% truncate was meant to drop.
    _ = bondy_oplog_instance:await_apply(InstanceId),
    bondy_oplog_instance:truncate_prefix(InstanceId, Watermark).

-spec size(instance_id()) -> non_neg_integer().

size(InstanceId) ->
    bondy_oplog_instance:size(InstanceId).

-spec first_key(instance_id()) ->
    {ok, bondy_oplog_event:event_key()} | empty.

first_key(InstanceId) ->
    bondy_oplog_instance:first_key(InstanceId).

-spec latest_key(instance_id()) ->
    {ok, bondy_oplog_event:event_key()} | empty.

latest_key(InstanceId) ->
    bondy_oplog_instance:latest_key(InstanceId).

-spec origin(instance_id()) -> bondy_oplog_origin:t().

origin(InstanceId) ->
    bondy_oplog_instance:origin(InstanceId).

-spec info(instance_id()) -> map().

info(InstanceId) ->
    bondy_oplog_instance:info(InstanceId).

%% =============================================================================
%% SYNC
%% =============================================================================

?DOC("""
Synchronously pulls events from `Peer` into `InstanceId`.

A successful pull merges the peer's tree into ours; a converse pull
(initiated by the peer) is needed to bring the peer up to date. This
is the single-direction primitive; consumers that want full
convergence call sync in both directions or rely on the default
schedulers running on both replicas.

Returns `{ok, FinalRoot}` on success.
""").
-spec sync(instance_id(), peer_id()) ->
    {ok, bondy_mst:hash() | undefined} | {error, term()}.

sync(InstanceId, Peer) ->
    sync(InstanceId, Peer, #{}).

-spec sync(
    instance_id(),
    peer_id(),
    bondy_oplog_sync_session:opts()
) -> {ok, bondy_mst:hash() | undefined} | {error, term()}.

sync(InstanceId, Peer, Opts) ->
    %% Drain the local applier so sync operates on the up-to-date MST
    %% instead of stale state with overlay-pending events. Production
    %% callers who do many appends followed by sync would otherwise
    %% sync against an MST that doesn't yet contain those appends.
    _ = bondy_oplog_instance:await_apply(InstanceId),
    bondy_oplog_sync_session:run(InstanceId, Peer, Opts).

-spec sync_async(instance_id(), peer_id()) -> {ok, pid()}.

sync_async(InstanceId, Peer) ->
    sync_async(InstanceId, Peer, #{}).

-spec sync_async(
    instance_id(),
    peer_id(),
    bondy_oplog_sync_session:opts()
) -> {ok, pid()}.

sync_async(InstanceId, Peer, Opts) ->
    bondy_oplog_sync_session:start(InstanceId, Peer, Opts).

?DOC("""
Bootstraps `InstanceId` from `Peer` — fetches the peer's snapshot,
installs it locally, then runs a regular sync for events past the
watermark. Suitable for fresh or far-behind replicas joining a
long-running cluster.

Falls back to plain `sync/2,3` semantics if the peer reports no
snapshot.
""").
-spec bootstrap(instance_id(), peer_id()) ->
    {ok, bondy_mst:hash() | undefined} | {error, term()}.

bootstrap(InstanceId, Peer) ->
    bootstrap(InstanceId, Peer, #{}).

-spec bootstrap(
    instance_id(),
    peer_id(),
    bondy_oplog_sync_session:opts()
) -> {ok, bondy_mst:hash() | undefined} | {error, term()}.

bootstrap(InstanceId, Peer, Opts) ->
    %% Drain the local applier so bootstrap operates on the
    %% up-to-date MST (same rationale as `sync/3`).
    _ = bondy_oplog_instance:await_apply(InstanceId),
    bondy_oplog_sync_session:bootstrap(InstanceId, Peer, Opts).

%% =============================================================================
%% GC / QUERIES
%% =============================================================================

?DOC("""
Runs one compaction cycle on `InstanceId`. See
`bondy_oplog_compaction:compact/1`.
""").
-spec compact(instance_id()) ->
    {ok, no_change}
    | {ok, {compacted, bondy_oplog_event:event_key(), non_neg_integer()}}
    | {error, term()}.

compact(InstanceId) ->
    %% Drain the local applier so compaction operates on the
    %% up-to-date MST. Without this, overlay-pending events would be
    %% missed by the truncation pass and remain in the overlay after
    %% compaction completes.
    _ = bondy_oplog_instance:await_apply(InstanceId),
    bondy_oplog_compaction:compact(InstanceId).

-spec current_watermark(instance_id()) ->
    undefined | bondy_oplog_event:event_key().

current_watermark(InstanceId) ->
    bondy_oplog_instance:current_watermark(InstanceId).

-spec snapshot(instance_id()) ->
    {ok, bondy_oplog_event:event_key(), term()} | not_found.

snapshot(InstanceId) ->
    bondy_oplog_instance:snapshot(InstanceId).

?DOC("""
Hot query: snapshot + live events. See
`bondy_oplog_query:query/2`.
""").
-spec query(instance_id(), Query :: term()) -> term().

query(InstanceId, Query) ->
    %% Hot query reads the MST (and snapshot). Drain so overlay-
    %% pending events are included.
    _ = bondy_oplog_instance:await_apply(InstanceId),
    bondy_oplog_query:query(InstanceId, Query).

?DOC("""
Stable query: snapshot only. See
`bondy_oplog_query:query_stable/2`.
""").
-spec query_stable(instance_id(), Query :: term()) -> term().

query_stable(InstanceId, Query) ->
    %% query_stable reads the snapshot store only (no live MST), but
    %% the underlying compaction/load_snapshot operations must have
    %% drained the applier first. We drain defensively here so a
    %% stale read between an append and the next compaction doesn't
    %% surprise callers.
    _ = bondy_oplog_instance:await_apply(InstanceId),
    bondy_oplog_query:query_stable(InstanceId, Query).
