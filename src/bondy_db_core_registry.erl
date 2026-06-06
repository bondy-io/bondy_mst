%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_db_core_registry).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Node-shared registry of per-`(namespace, index, shard)` triples for
`bondy_db_core` (`MST_DB_DESIGN.md` §3, §5, §6).

Each shard publishes one entry containing the handles `bondy_db_core`
needs to satisfy a read:

| Field | Source |
|---|---|
| `shard_count` | namespace configuration |
| `cache_adapter` + `cache_handle` | owner's cache adapter init |
| `projection_adapter` + `projection_handle` | owner's projection open |
| `overlay` | owner's `bondy_oplog_db_overlay:new/0` |
| `fold_module` | namespace's fold strategy |

The table is a single `public set` ETS owned by this gen_server. Reads
go directly to ETS (`lookup/3` is the hot path; no roundtrip).
`register/4` and `unregister/3` are `gen_server:call/2` so the server
can monitor the registering process and tear the row down if the owner
dies. The hot read path remains lock-free.

## Restart semantics

The ETS table is owned by this gen_server; if the gen_server dies the
table dies with it. On supervisor restart, `init/1` creates a fresh
empty table — **all in-memory monitor state is lost and previously
registered shards are orphaned** (their atomics refs still exist, but
the registry has no row pointing to them). Subsequent `lookup/3` calls
will return `not_found` until owners re-register.

There is currently no recovery protocol: owners are not signalled when
the registry restarts. Operators should either set the supervisor's
`intensity` so the registry effectively never restarts, or wire the
applier to periodically validate its own registrations and re-register
on `not_found`. The substrate does not police this.

## Owner DOWN cleanup

When an owner dies, the registry deletes the ETS row and removes the
monitor. It does **not** call `close/1` on the cache, projection, or
overlay adapters — those handles were created by (and may be tied to
the lifecycle of) the owner process. For ETS-based adapters this is
correct: ETS reclaims tables owned by the dead process. Adapters that
own external resources (file handles, sub-processes, connection
pools) MUST set up their own owner-monitoring inside the adapter — the
substrate guarantees registry-row cleanup only.

## Why a separate registry from `bondy_oplog_registry`

`bondy_oplog_registry` is per-instance (effectively per-namespace —
the existing substrate uses `instance_id` as the namespace). The
MST_DB read API needs a richer key: `(namespace, index, shard)` —
indexes (primary and secondaries) are a new dimension introduced in
`MST_DB_DESIGN.md` and not present in `bondy_oplog_instance`. Keeping
the registries separate avoids retrofitting `bondy_oplog_registry`'s
record with index/shard fields that would be `undefined` for the
99% of consumers that have not opted into the read-side projection.

## Why ETS, not persistent_term

`persistent_term:put/2` triggers a global GC scan on every process on
the node. With many shards doing many config refreshes (e.g., a
secondary's lag bound changing), that's a non-starter. ETS `insert` is
constant-time, no global side effects, and `read_concurrency: true`
keeps reads parallel.
""").

-define(TABLE, bondy_db_core_registry_tab).

%% "Infinitely stale" freshness sentinel (`MST_DB_DESIGN.md` §11). Chosen
%% so that on a node whose `monotonic_time(millisecond)` offset is large
%% and negative, `Now - sentinel` is always a huge positive number — an
%% un-bumped (or deliberately invalidated) shard fails any finite
%% `max_lag` check. `-(1 bsl 62)` leaves headroom above the signed-int64
%% floor so the subtraction never wraps.
-define(STALE_SENTINEL, -(1 bsl 62)).

%% Atomics slot layout for an index shard's `inflight_ref` (IDX-4
%% back-pressure). Slot 1 counts ops dispatched to the secondary writer
%% but not yet flushed (the unbounded-mailbox bound); slot 2 is a
%% `needs_rebuild` flag (0 | 1) raised on a saturation drop or a writer
%% crash and cleared only by a completed rebuild.
-define(INFLIGHT_SLOT, 1).
-define(NEEDS_REBUILD_SLOT, 2).

-record(entry, {
    key :: shard_key(),
    shard_count :: pos_integer(),
    cache_adapter :: module(),
    cache_handle :: term(),
    projection_adapter :: module(),
    projection_handle :: term(),
    overlay :: disabled | bondy_oplog_db_overlay:tid(),
    fold_module :: bondy_oplog_fold:strategy(),
    %% Per-shard freshness counter, written by the applier on each
    %% projection commit (or by anti-entropy on each successful round).
    %% Stored as `monotonic_time(millisecond)`; read wait-free by
    %% `ensure_fresh/2` (`MST_DB_DESIGN.md` §11).
    ae_atomics :: atomics:atomics_ref(),
    %% Per-shard high-water HLC mark
    %% (`bondy_oplog_high_water`). Tracks the highest HLC of any
    %% `cell_apply` event the applier has materialised into the
    %% shard's projection. Allocated here so it can be shared between
    %% the applier (writer) and read-only consumers
    %% (catalogue-freshness reporting, bootstrap finalisation) without
    %% threading through the applier's process state.
    high_water_ref :: bondy_oplog_high_water:ref(),
    %% Per-namespace policy (§15). `ap` (default) places no constraint
    %% on reads; `cp` rejects `eventual`-consistency batch reads to
    %% prevent unfenced staleness. Owners pass this on `register/4`;
    %% the substrate trusts the value to be consistent across shards
    %% of the same namespace (consumer responsibility).
    consistency_class :: ap | cp,
    %% Secondary-index writer pid for this `(NS, IndexName, SecShard)`
    %% triple (`MST_DB_DESIGN.md` §13). `undefined` for primary shards
    %% and for index shards whose `bondy_oplog_secondary_writer` has not
    %% yet stamped itself (a brief startup window). The primary applier
    %% reads it via `entry_writer_pid/1` to dispatch index updates after
    %% a successful projection write. Set out-of-band via
    %% `set_writer_pid/4` (a single-field `ets:update_element`, no
    %% monitor change) — the projection-handle owner, not the writer,
    %% owns the registry monitor.
    writer_pid = undefined :: pid() | undefined,
    %% Per-index-shard back-pressure atomics (`MST_DB_DESIGN.md` §13,
    %% IDX-4). `undefined` for primary shards. Two slots: in-flight op
    %% count (slot `?INFLIGHT_SLOT`) and a `needs_rebuild` flag (slot
    %% `?NEEDS_REBUILD_SLOT`). The primary applier reads slot 1 at dispatch
    %% to decide whether to drop a saturating batch; the secondary writer
    %% decrements it on flush. Slot 2 gates `index_get`/`index_range`
    %% freshness so reads refuse from a saturation drop until a rebuild
    %% clears it. Allocated by the facade on index-shard registration.
    inflight_ref = undefined :: atomics:atomics_ref() | undefined,
    %% Primary shard's oplog `instance_id` (`bondy_oplog`), recorded so a
    %% secondary-index rebuild can discover the primary appliers for a
    %% namespace from the registry alone (no table handle), re-fold each
    %% one's MST, and re-dispatch the index ops. `undefined` for secondary
    %% (index) shards, which have no oplog instance.
    instance_id = undefined :: binary() | undefined
}).

-record(state, {
    %% MonitorRef -> shard_key()
    mon_to_key = #{} :: #{reference() := shard_key()},
    %% shard_key() -> MonitorRef
    key_to_mon = #{} :: #{shard_key() := reference()},
    %% Fresh `make_ref()` per gen_server start. Exposed via
    %% `current_epoch/0` and broadcast on `bondy_db_core_events`
    %% under topic `bondy_db_core_registry_started`. Owners cache the
    %% epoch and treat a change as "registry was restarted; re-register".
    epoch :: reference()
}).

-type shard_key() :: {atom(), atom(), non_neg_integer()}.
-type shard_entry() :: #entry{}.
-type config() :: #{
    shard_count := pos_integer(),
    cache_adapter := module(),
    cache_handle := term(),
    projection_adapter := module(),
    projection_handle := term(),
    fold_module := bondy_oplog_fold:strategy(),
    %% Required. Pass `disabled` to opt out of overlay-merge on the read
    %% path (the facade does this — `apply/4`'s `await_apply` step
    %% provides read-your-writes without an overlay). Pass a `tid()`
    %% when overlay-merge is desired.
    overlay := disabled | bondy_oplog_db_overlay:tid(),
    %% Optional. If absent, the registry allocates a single-counter
    %% atomics ref on register. Owners that want shared accounting
    %% (e.g., across a hot/cold reload) can pass their own ref.
    ae_atomics => atomics:atomics_ref(),
    %% Optional. Pid the registry will monitor; when this process exits
    %% the registration is torn down automatically. Defaults to the
    %% calling process.
    owner => pid(),
    %% Optional. Per-namespace consistency policy (`MST_DB_DESIGN.md`
    %% §15). Defaults to `ap`. See `read_batch/2` for the enforcement
    %% rule.
    consistency_class => ap | cp,
    %% Optional. Per-index-shard back-pressure atomics (IDX-4). Allocated
    %% by the facade for index shards (`atomics:new(2, [{signed, true}])`);
    %% absent for primary shards.
    inflight_atomics => atomics:atomics_ref(),
    %% Optional. The owning oplog `instance_id` for a primary shard, so a
    %% rebuild can find the primary applier. Absent for index shards.
    instance_id => binary()
}.

-export_type([shard_entry/0, config/0]).

-export([child_spec/0]).
-export([start_link/0]).

-export([register/4]).
-export([unregister/3]).
-export([set_writer_pid/4]).
-export([lookup/3]).
-export([shard_count/2]).
-export([list/0]).

%% Restart-recovery protocol (`MST_DB_DESIGN.md` §11.1, §18 item 11).
-export([current_epoch/0]).

%% Diagnostic / invariant-checking helper.
-export([snapshot_for_invariants/0]).

%% Freshness (`MST_DB_DESIGN.md` §11).
-export([bump_ae/3]).
-export([bump_ae/4]).
-export([high_water_hlc/3]).
-export([bump_ae_targets/1]).
-export([bump_ae_targets/2]).
-export([last_ae_at/3]).
-export([ever_freshened/3]).
-export([shards_for/1]).
-export([namespaces/0]).

%% Field accessors (so callers do not need the header).
-export([entry_key/1]).
-export([entry_cache_adapter/1]).
-export([entry_cache_handle/1]).
-export([entry_projection_adapter/1]).
-export([entry_projection_handle/1]).
-export([entry_overlay/1]).
-export([entry_fold_module/1]).
-export([entry_shard_count/1]).
-export([entry_ae_atomics/1]).
-export([entry_high_water_ref/1]).
-export([entry_consistency_class/1]).
-export([entry_writer_pid/1]).
-export([entry_inflight_ref/1]).
-export([entry_instance_id/1]).
-export([entry_last_ae/1]).
-export([entry_ever_freshened/1]).

%% Index-shard back-pressure helpers (IDX-4). Operate on the entry's
%% `inflight_ref`; all are wait-free and a strict no-op (or `false`) when
%% the ref is `undefined` (a primary shard).
-export([index_inflight_add/2]).
-export([index_inflight_sub/2]).
-export([index_inflight/1]).
-export([index_inflight_reset/1]).
-export([index_mark_rebuild/1]).
-export([index_clear_rebuild/1]).
-export([index_needs_rebuild/1]).
-export([reset_stale_ae/1]).

%% Namespace-level consistency_class lookup (`MST_DB_DESIGN.md` §15).
-export([consistency_class/1]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% =============================================================================
%% API
%% =============================================================================

child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
    }.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec register(
    Namespace :: atom(),
    Index :: atom(),
    Shard :: non_neg_integer(),
    Config :: config()
) -> ok | {error, {missing_required_field, atom()}}.

register(NS, Index, Shard, Config) when
    is_atom(NS),
    is_atom(Index),
    is_integer(Shard),
    Shard >= 0,
    is_map(Config)
->
    %% Validate required keys here, before the gen_server call. A bad
    %% config crashing inside the gen_server would wipe the monitor
    %% bookkeeping for every other registration on the node — a
    %% single misconfigured call is not allowed to take the substrate
    %% down with it.
    case validate_config(Config) of
        ok ->
            Owner = maps:get(owner, Config, self()),
            gen_server:call(
                ?MODULE, {register, NS, Index, Shard, Owner, Config}
            );
        {error, _} = Err ->
            Err
    end.

-spec unregister(atom(), atom(), non_neg_integer()) -> ok.

unregister(NS, Index, Shard) ->
    gen_server:call(?MODULE, {unregister, {NS, Index, Shard}}).

-doc """
Stamp the secondary-index writer pid onto an already-registered
`(NS, IndexName, SecShard)` row. A single-field `ets:update_element/3`:
lock-free, no monitor change (the registry monitor stays bound to the
projection-handle owner from `register/4` — the writer is a client, not
the owner, of that row). Returns `not_found` when no row exists for the
triple (e.g. the index shard was torn down, or the registry restarted
and the owner has not re-registered yet). Called by
`bondy_oplog_secondary_writer` at init and on the registry-restart epoch.
""".
-spec set_writer_pid(atom(), atom(), non_neg_integer(), pid()) ->
    ok | not_found.

set_writer_pid(NS, Index, Shard, Pid) when
    is_atom(NS), is_atom(Index), is_integer(Shard), Shard >= 0, is_pid(Pid)
->
    case
        ets:update_element(
            ?TABLE, {NS, Index, Shard}, {#entry.writer_pid, Pid}
        )
    of
        true -> ok;
        false -> not_found
    end.

-doc """
Return the current epoch reference. A new epoch is allocated on each
gen_server start and broadcast on
`bondy_db_core_events:notify(bondy_db_core_registry_started, Epoch)`.
Owners cache the epoch they last saw and treat any change as
"registry was restarted; re-register every shard I own".
""".
-spec current_epoch() -> reference().

current_epoch() ->
    gen_server:call(?MODULE, current_epoch).

-doc """
Atomic snapshot of `(ETS entries, mon_to_key, key_to_mon)` for
invariant-checking callers. Runs inside the gen_server so the ETS
read and the in-memory maps come from the same instant — an outside
observer combining `sys:get_state/1` with `lookup/3` would race against
DOWN handlers and unregister calls. Intended for tests and operator
diagnostics; ordinary callers should use `lookup/3`.
""".
-spec snapshot_for_invariants() ->
    #{
        entries := [shard_entry()],
        mon_to_key := #{reference() := shard_key()},
        key_to_mon := #{shard_key() := reference()}
    }.

snapshot_for_invariants() ->
    gen_server:call(?MODULE, snapshot_for_invariants).

-spec lookup(atom(), atom(), non_neg_integer()) ->
    {ok, shard_entry()} | not_found.

lookup(NS, Index, Shard) ->
    case ets:lookup(?TABLE, {NS, Index, Shard}) of
        [#entry{} = E] -> {ok, E};
        [] -> not_found
    end.

-spec shard_count(atom(), atom()) -> {ok, pos_integer()} | not_found.

shard_count(NS, Index) ->
    MS = [
        {
            #entry{
                key = {NS, Index, '_'},
                shard_count = '$1',
                _ = '_'
            },
            [],
            ['$1']
        }
    ],
    case ets:select(?TABLE, MS, 1) of
        {[Count], _} -> {ok, Count};
        '$end_of_table' -> not_found
    end.

-spec list() -> [shard_entry()].

list() ->
    ets:select(?TABLE, [{'_', [], ['$_']}]).

-doc """
Record on the shard's atomics counter that the shard has just had a
fresh round of applier activity (or anti-entropy convergence). Wait-free.

Uses `erlang:monotonic_time(millisecond)` as the bump timestamp. For
applier loops that bump several shards in one logical step and want
to reuse the same "now" across them, see `bump_ae/4`.
""".
-spec bump_ae(atom(), atom(), non_neg_integer()) -> ok | not_found.

bump_ae(NS, Index, Shard) ->
    bump_ae(NS, Index, Shard, erlang:monotonic_time(millisecond)).

-doc """
Like `bump_ae/3` but caller supplies the monotonic millisecond
timestamp so the same "now" can be reused across a batch of shards.
""".
-spec bump_ae(atom(), atom(), non_neg_integer(), integer()) ->
    ok | not_found.

bump_ae(NS, Index, Shard, Now) when is_integer(Now) ->
    case lookup(NS, Index, Shard) of
        {ok, #entry{ae_atomics = Ref}} ->
            atomics:put(Ref, 1, Now),
            ok;
        not_found ->
            not_found
    end.

-doc """
Read the per-shard high-water HLC mark
(`bondy_oplog_high_water`).

Returns `{ok, Hlc}` when at least one `cell_apply` event has been
materialised into the shard's projection since the shard's last
registration (or `finalize_catalogue_bootstrap/3` call), `{ok,
no_watermark}` otherwise, and `not_found` when no shard is
registered under the given key.

The watermark is *not* durable across instance restarts — see
`bondy_oplog_high_water` module docs.
""".
-spec high_water_hlc(atom(), atom(), non_neg_integer()) ->
    {ok, non_neg_integer()} | {ok, no_watermark} | not_found.

high_water_hlc(NS, Index, Shard) ->
    case lookup(NS, Index, Shard) of
        {ok, #entry{high_water_ref = Ref}} ->
            bondy_oplog_high_water:read(Ref);
        not_found ->
            not_found
    end.

-doc """
Bump every shard in `Targets` with a single shared
`erlang:monotonic_time(millisecond)` so the batch observes the same
"now". Returns `{Bumped, NotFound}` counts for telemetry. An empty
list is a strict no-op and returns `{0, 0}`.
""".
-spec bump_ae_targets([shard_key()]) ->
    {non_neg_integer(), non_neg_integer()}.

bump_ae_targets([]) ->
    {0, 0};
bump_ae_targets(Targets) when is_list(Targets) ->
    bump_ae_targets(Targets, erlang:monotonic_time(millisecond)).

-doc """
Like `bump_ae_targets/1` but caller supplies the monotonic
millisecond timestamp so the same "now" can be reused across multiple
target lists (e.g., when both the applier and an AE round complete in
the same logical tick).
""".
-spec bump_ae_targets([shard_key()], integer()) ->
    {non_neg_integer(), non_neg_integer()}.

bump_ae_targets([], _Now) ->
    {0, 0};
bump_ae_targets(Targets, Now) when is_list(Targets), is_integer(Now) ->
    lists:foldl(
        fun({NS, Index, Shard}, {B, NF}) ->
            case bump_ae(NS, Index, Shard, Now) of
                ok -> {B + 1, NF};
                not_found -> {B, NF + 1}
            end
        end,
        {0, 0},
        Targets
    ).

-doc """
Return the monotonic millisecond timestamp of the shard's last AE bump.
Wait-free.

A shard that has never been bumped reads `-(1 bsl 62)` (an
"infinitely stale" sentinel chosen so `Now - sentinel` is a very large
positive number regardless of the node's `monotonic_time` offset).
The sentinel ensures un-bumped shards reliably fail any finite
`max_lag` check until the applier or AE has driven the counter
forward at least once.
""".
-spec last_ae_at(atom(), atom(), non_neg_integer()) ->
    integer() | not_found.

last_ae_at(NS, Index, Shard) ->
    case lookup(NS, Index, Shard) of
        {ok, #entry{ae_atomics = Ref}} ->
            atomics:get(Ref, 1);
        not_found ->
            not_found
    end.

-doc """
Whether the shard has ever been freshened (AE bumped past the
"infinitely stale" sentinel). Used by a restarting secondary writer to
tell a crash-restart of a previously-populated shard (rebuild to recover
the lost buffer) from a first-ever start (the startup backfill handles
it). `false` for an unknown or never-bumped shard.
""".
-spec ever_freshened(atom(), atom(), non_neg_integer()) -> boolean().

ever_freshened(NS, Index, Shard) ->
    case last_ae_at(NS, Index, Shard) of
        not_found -> false;
        ?STALE_SENTINEL -> false;
        _ -> true
    end.

-doc """
Return all entries registered for the namespace. Used by callers that
need the atomics ref directly to avoid the second `lookup/3`.
""".
-spec shards_for(atom()) -> [shard_entry()].

shards_for(NS) when is_atom(NS) ->
    MS = [
        {
            #entry{
                key = {NS, '_', '_'},
                _ = '_'
            },
            [],
            ['$_']
        }
    ],
    ets:select(?TABLE, MS).

-doc """
List of all distinct namespaces registered. Used by callers that want
to apply a freshness check over "every namespace this node knows about"
without spelling them out.
""".
-spec namespaces() -> [atom()].

namespaces() ->
    MS = [
        {
            #entry{
                key = {'$1', '_', '_'},
                _ = '_'
            },
            [],
            ['$1']
        }
    ],
    lists:usort(ets:select(?TABLE, MS)).

%% =============================================================================
%% Accessors
%% =============================================================================

entry_key(#entry{key = V}) -> V.
entry_cache_adapter(#entry{cache_adapter = V}) -> V.
entry_cache_handle(#entry{cache_handle = V}) -> V.
entry_projection_adapter(#entry{projection_adapter = V}) -> V.
entry_projection_handle(#entry{projection_handle = V}) -> V.
entry_overlay(#entry{overlay = V}) -> V.
entry_fold_module(#entry{fold_module = V}) -> V.
entry_shard_count(#entry{shard_count = V}) -> V.
entry_ae_atomics(#entry{ae_atomics = V}) -> V.
entry_high_water_ref(#entry{high_water_ref = V}) -> V.
entry_consistency_class(#entry{consistency_class = V}) -> V.
entry_writer_pid(#entry{writer_pid = V}) -> V.
entry_inflight_ref(#entry{inflight_ref = V}) -> V.
entry_instance_id(#entry{instance_id = V}) -> V.

%% Last AE-freshness timestamp (monotonic ms), read straight off the
%% entry's atomics — the sentinel `?STALE_SENTINEL` for a never-freshened
%% shard. Lets a caller that already holds the entry compute the lag
%% without a second `lookup/3`.
entry_last_ae(#entry{ae_atomics = Ref}) -> atomics:get(Ref, 1).

%% Whether this shard has ever been freshened (AE bumped past the stale
%% sentinel).
entry_ever_freshened(#entry{ae_atomics = Ref}) ->
    atomics:get(Ref, 1) =/= ?STALE_SENTINEL.

%% =============================================================================
%% Index-shard back-pressure (IDX-4)
%% =============================================================================

-doc """
Add `N` to the index shard's in-flight op counter and return the new
value. Called by the primary applier when it accepts a batch for the
secondary writer. A no-op returning `0` for a primary shard (no
`inflight_ref`).
""".
-spec index_inflight_add(shard_entry(), non_neg_integer()) ->
    non_neg_integer().

index_inflight_add(#entry{inflight_ref = undefined}, _N) ->
    0;
index_inflight_add(#entry{inflight_ref = Ref}, N) when is_integer(N), N >= 0 ->
    atomics:add_get(Ref, ?INFLIGHT_SLOT, N).

-doc """
Subtract `N` from the index shard's in-flight op counter, flooring at
`0` (a flush can never legitimately drive it negative, but a concurrent
reset must not leave it below zero). No-op for a primary shard.
""".
-spec index_inflight_sub(shard_entry(), non_neg_integer()) -> ok.

index_inflight_sub(#entry{inflight_ref = undefined}, _N) ->
    ok;
index_inflight_sub(#entry{inflight_ref = Ref}, N) when is_integer(N), N >= 0 ->
    case atomics:sub_get(Ref, ?INFLIGHT_SLOT, N) of
        V when V < 0 -> atomics:put(Ref, ?INFLIGHT_SLOT, 0);
        _ -> ok
    end.

-doc "Current in-flight op count for the index shard (`0` for a primary).".
-spec index_inflight(shard_entry()) -> non_neg_integer().

index_inflight(#entry{inflight_ref = undefined}) ->
    0;
index_inflight(#entry{inflight_ref = Ref}) ->
    erlang:max(0, atomics:get(Ref, ?INFLIGHT_SLOT)).

-doc """
Reset the in-flight counter to `0`. Used by a rebuild before it re-folds
the primary, since the rebuild also discards the writer's buffer — the
counter and the buffer are reset together so they stay consistent.
""".
-spec index_inflight_reset(shard_entry()) -> ok.

index_inflight_reset(#entry{inflight_ref = undefined}) ->
    ok;
index_inflight_reset(#entry{inflight_ref = Ref}) ->
    atomics:put(Ref, ?INFLIGHT_SLOT, 0).

-doc """
Raise the index shard's `needs_rebuild` flag (a saturation drop or a
writer crash lost ops). While set, `index_get`/`index_range` treat the
shard as stale regardless of its AE timestamp, so reads refuse (or fall
back to the primary) until a rebuild clears the flag. No-op for a primary.
""".
-spec index_mark_rebuild(shard_entry()) -> ok.

index_mark_rebuild(#entry{inflight_ref = undefined}) ->
    ok;
index_mark_rebuild(#entry{inflight_ref = Ref}) ->
    atomics:put(Ref, ?NEEDS_REBUILD_SLOT, 1).

-doc "Clear the `needs_rebuild` flag. Called by a completed rebuild.".
-spec index_clear_rebuild(shard_entry()) -> ok.

index_clear_rebuild(#entry{inflight_ref = undefined}) ->
    ok;
index_clear_rebuild(#entry{inflight_ref = Ref}) ->
    atomics:put(Ref, ?NEEDS_REBUILD_SLOT, 0).

-doc "Whether the index shard's `needs_rebuild` flag is set (`false` for a primary).".
-spec index_needs_rebuild(shard_entry()) -> boolean().

index_needs_rebuild(#entry{inflight_ref = undefined}) ->
    false;
index_needs_rebuild(#entry{inflight_ref = Ref}) ->
    atomics:get(Ref, ?NEEDS_REBUILD_SLOT) =/= 0.

-doc """
Reset the shard's AE freshness counter to the "infinitely stale"
sentinel, so any finite `max_lag` read refuses until the shard is
freshened again. Used by a saturation drop (`MST_DB_DESIGN.md` §13). No-op
when the entry has no AE atomics.
""".
-spec reset_stale_ae(shard_entry()) -> ok.

reset_stale_ae(#entry{ae_atomics = undefined}) ->
    ok;
reset_stale_ae(#entry{ae_atomics = Ref}) ->
    atomics:put(Ref, 1, ?STALE_SENTINEL).

-doc """
Return the consistency class declared for the namespace. Reads it from
any registered shard of the namespace (the substrate trusts the value
to be consistent across shards — see `register/4`). Returns `ap` for an
unknown namespace, matching the default.
""".
-spec consistency_class(atom()) -> ap | cp.

consistency_class(NS) when is_atom(NS) ->
    MS = [
        {
            #entry{
                key = {NS, '_', '_'},
                consistency_class = '$1',
                _ = '_'
            },
            [],
            ['$1']
        }
    ],
    case ets:select(?TABLE, MS, 1) of
        {[Class], _} -> Class;
        '$end_of_table' -> ap
    end.

%% =============================================================================
%% gen_server callbacks
%% =============================================================================

init([]) ->
    _ = ets:new(?TABLE, [
        set,
        public,
        named_table,
        {keypos, #entry.key},
        {read_concurrency, true}
    ]),
    Epoch = erlang:make_ref(),
    %% Broadcast asynchronously after init returns so subscribers wake
    %% up *after* the registry is in `ready` state. Synchronous notify
    %% from inside init would still work because the subscribers are
    %% other processes, but doing the work inline keeps init fast.
    self() ! {broadcast_started, Epoch},
    {ok, #state{epoch = Epoch}}.

handle_call({register, NS, Index, Shard, Owner, Config}, _From, State0) ->
    Key = {NS, Index, Shard},
    %% If a previous registration exists for this key, demonitor it
    %% before installing the new owner.
    State1 = drop_monitor_for_key(Key, State0),
    Mon = erlang:monitor(process, Owner),
    Ae =
        case maps:find(ae_atomics, Config) of
            {ok, ExistingRef} ->
                ExistingRef;
            error ->
                NewRef = atomics:new(1, [{signed, true}]),
                %% Initialise to the "infinitely stale" sentinel so an
                %% un-bumped shard fails any finite freshness check.
                ok = atomics:put(NewRef, 1, ?STALE_SENTINEL),
                NewRef
        end,
    HighWater = bondy_oplog_high_water:new(),
    Entry = #entry{
        key = Key,
        shard_count = maps:get(shard_count, Config),
        cache_adapter = maps:get(cache_adapter, Config),
        cache_handle = maps:get(cache_handle, Config),
        projection_adapter = maps:get(projection_adapter, Config),
        projection_handle = maps:get(projection_handle, Config),
        overlay = maps:get(overlay, Config),
        fold_module = maps:get(fold_module, Config),
        ae_atomics = Ae,
        high_water_ref = HighWater,
        consistency_class = maps:get(consistency_class, Config, ap),
        inflight_ref = maps:get(inflight_atomics, Config, undefined),
        instance_id = maps:get(instance_id, Config, undefined)
    },
    true = ets:insert(?TABLE, Entry),
    State2 = State1#state{
        mon_to_key = maps:put(Mon, Key, State1#state.mon_to_key),
        key_to_mon = maps:put(Key, Mon, State1#state.key_to_mon)
    },
    {reply, ok, State2};
handle_call({unregister, Key}, _From, State0) ->
    State1 = drop_monitor_for_key(Key, State0),
    true = ets:delete(?TABLE, Key),
    {reply, ok, State1};
handle_call(current_epoch, _From, #state{epoch = E} = State) ->
    {reply, E, State};
handle_call(snapshot_for_invariants, _From, State) ->
    Snapshot = #{
        entries => ets:select(?TABLE, [{'_', [], ['$_']}]),
        mon_to_key => State#state.mon_to_key,
        key_to_mon => State#state.key_to_mon
    },
    {reply, Snapshot, State};
handle_call(_Req, _From, State) ->
    {reply, {error, unknown}, State}.

handle_cast(_, State) -> {noreply, State}.

handle_info({broadcast_started, Epoch}, State) ->
    %% `bondy_db_core_events` is started before this module in
    %% `bondy_oplog_sup`, so the notify is safe at init time. If the
    %% events module is down, swallow the error — it is a diagnostic
    %% gap, not a substrate-correctness issue.
    catch bondy_db_core_events:notify(
        bondy_db_core_registry_started,
        Epoch
    ),
    {noreply, State};
handle_info({'DOWN', Mon, process, _Pid, _Reason}, State0) ->
    case maps:take(Mon, State0#state.mon_to_key) of
        {Key, MonToKey1} ->
            true = ets:delete(?TABLE, Key),
            State1 = State0#state{
                mon_to_key = MonToKey1,
                key_to_mon = maps:remove(Key, State0#state.key_to_mon)
            },
            {noreply, State1};
        error ->
            {noreply, State0}
    end;
handle_info(_, State) ->
    {noreply, State}.

terminate(_, _) -> ok.
code_change(_, State, _) -> {ok, State}.

%% =============================================================================
%% Internal
%% =============================================================================

-define(REQUIRED_FIELDS, [
    shard_count,
    cache_adapter,
    cache_handle,
    projection_adapter,
    projection_handle,
    fold_module,
    overlay
]).

validate_config(Config) ->
    case [K || K <- ?REQUIRED_FIELDS, not maps:is_key(K, Config)] of
        [] -> validate_consistency_class(Config);
        [K | _] -> {error, {missing_required_field, K}}
    end.

validate_consistency_class(Config) ->
    case maps:find(consistency_class, Config) of
        {ok, V} when V =:= ap; V =:= cp -> ok;
        {ok, Bad} -> {error, {invalid_consistency_class, Bad}};
        error -> ok
    end.

drop_monitor_for_key(Key, State) ->
    case maps:take(Key, State#state.key_to_mon) of
        {OldMon, KeyToMon1} ->
            true = erlang:demonitor(OldMon, [flush]),
            State#state{
                mon_to_key = maps:remove(OldMon, State#state.mon_to_key),
                key_to_mon = KeyToMon1
            };
        error ->
            State
    end.
