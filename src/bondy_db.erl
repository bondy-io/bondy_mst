%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_db).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Consumer-facing **cell-mechanics facade**, substrate-backed.

`bondy_db` decouples the user-visible model (DB → tables → cells keyed
by `(Realm, Key)`) from the physical layout (which Bookie owns which
shard, which bucket holds which entity type) via the
`bondy_db_topology` behaviour, and wires writes through the substrate
WAL+applier and reads through `bondy_db_core`'s cache + projection
merge.

## Substrate wiring

`open_table/3` provisions, **per shard**:

1. A projection adapter handle from the topology
   (`Topology:route(Shard, TableState)`). The handle spans every realm
   in the shard; realm isolation is done by encoding `Realm` into the
   cell key.
2. A per-shard read cache. By default this is a `bondy_oplog_cache_ets`
   table owned by the calling process. A topology that needs its
   per-shard resources to outlive the transient `open_table/3` caller
   (an ephemeral in-memory topology) instead exports `provision_cache/5`
   and hosts the cache in a long-lived owner — see `acquire_cache/4`.
3. A registry entry in `bondy_db_core_registry` mapping
   `(Namespace, primary, Shard)` to the
   `{cache_adapter, cache_handle, projection_adapter, projection_handle,
   fold_module}` tuple. The registry's owner-monitor is bound to the
   cache's owner (the caller by default, the topology's resource owner
   when hosted), so the row's lifetime tracks the resources it points
   at.
4. A `bondy_oplog_instance` with `cell_apply_target =>
   {Namespace, primary, Shard}` so the applier writes the projection
   on every replayed `{cell_apply, _, _, _}` event.

The `Namespace` atom is derived deterministically as
`list_to_atom(atom_to_list(DbName) ++ "_" ++ atom_to_list(EntityType))`
so two DBs with a colliding `EntityType` on the same node get distinct
substrate identities.

## Realm → Bucket mapping

The facade does not bake Realm into the cell key. Instead it asks the
topology — via `Topology:bucket_for(EntityType, Realm, TableState)` —
for the storage-layer **Bucket** the substrate should use, then calls
`bondy_db_core` with `(NS, primary, Bucket, Key)`. The topology decides
the composition rule:

- `per_entity` returns `Bucket = Realm` (EntityType already implicit in
  the Bookie).
- `single_bookie` returns `Bucket = <<Realm, "/", EntityType>>`
  (one Bookie holds everything, so Bucket disambiguates both).

`Key` is the user-supplied key, **unmodified**. Range scans address
`(Bucket, [Low, High))` directly.

## Write path

`apply/4` builds `{cell_apply, Bucket, Key, FoldEvent}` and calls
`bondy_oplog:append/2`. The fold-state update happens inside the
applier (`MST_DB_DESIGN.md` §6.3): the applier reads the current cell
frame, decodes via the fold module, folds the event in via
`apply_event/3`, encodes the new state, and writes it back through the
projection adapter with Bucket and Key as separate operands. After
the append, `apply/4` calls `bondy_oplog:await_apply/1` so the next
`read/3` from the same caller sees the updated cell.

## Read path

`read/3` calls `bondy_db_core:read/4`. That goes through:

1. Per-shard cache — a hit returns immediately.
2. Cache miss — read the projection, decode, populate the cache,
   return.

Overlay merging is disabled at the facade level — the shard is
registered with `overlay = disabled`. Read-your-writes is provided by
`apply/4`'s `await_apply` step, not by an overlay merge.

## Lifecycle

```erlang
{ok, Db} = bondy_db:open(my_db, #{
    topology      => bondy_db_topology_per_entity,
    topology_opts => #{sup => MySup, dir => <<"/var/lib/bondy_db">>},
    shard_count   => 8,
    fold_module   => lww_register
}),

{ok, Users}  = bondy_db:open_table(Db, users,  #{}),
{ok, Tags}   = bondy_db:open_table(Db, tags,   #{
    fold_module => g_set
}),

H = bondy_db:tick(Users),
ok = bondy_db:apply(Users, <<"r1">>, <<"alice">>, {set, H, <<"value">>}),
{ok, {set, <<"value">>, H}, H} = bondy_db:read(Users, <<"r1">>, <<"alice">>),

ok = bondy_db:close_table(Users),
ok = bondy_db:close_table(Tags),
ok = bondy_db:close(Db).
```

`close_table/1` stops the per-shard oplog instances, unregisters the
shards from `bondy_db_core_registry`, deletes the per-shard caches,
and asks the topology to release its physical resources for the table.
`close/1` then shuts down the topology (and any Bookies still owned by
it).
""").

-export([open/2]).
-export([close/1]).
-export([open_table/3]).
-export([close_table/1]).
-export([tick/1]).
-export([apply/4]).
-export([counter_inc/4]).
-export([read/3]).
-export([range/5]).
-export([index_get/5]).
-export([index_range/6]).
-export([rebuild_index/2]).
-export([rebuild_indexes/1]).
-export([index_lag/2]).
-export([info/1]).

-export_type([db/0, table/0, realm/0]).

-define(DEFAULT_SHARD_COUNT, 8).
-define(DEFAULT_FOLD, lww_register).
-define(INDEX, primary).
%% Substrate default range cap, mirrored from `bondy_db_core`.
-define(DEFAULT_RANGE_LIMIT, 1000).
%% Upper bound on the primary-scan fallback (IDX-4): how many primary
%% cells a single stale-index fallback read will enumerate. Bounded so the
%% "slow but correct" path cannot run unbounded; a scan that hits the cap
%% logs a warning (the fallback result may be incomplete).
-define(PRIMARY_SCAN_LIMIT, 1000000).

-type realm() :: binary().

-type db() :: #{
    name := atom(),
    topology := module(),
    topology_state := bondy_db_topology:state(),
    %% DB-scoped projection provider for `projection_backend => ets`
    %% tables. `undefined` when the DB topology is itself
    %% `bondy_db_topology_memory` (it is its own provider); otherwise a
    %% `bondy_db_topology_memory` state created at `open/2`.
    ets_provider := bondy_db_topology:state() | undefined,
    opts := map(),
    hlc := bondy_oplog_hlc:t()
}.

-type projection_backend() :: leveled | ets.

-type table() :: #{
    db_name := atom(),
    %% The **effective** projection topology for this table: the DB's
    %% topology for `leveled` tables, `bondy_db_topology_memory` for
    %% `ets` (ephemeral) tables. Every read/write/range/teardown path
    %% resolves bucket + route + cache + owner through it, so an ephemeral
    %% table inside a leveled DB needs no special-casing downstream.
    db_topology := module(),
    db_hlc := bondy_oplog_hlc:t(),
    entity_type := atom(),
    namespace := atom(),
    shard_count := pos_integer(),
    fold_module := module() | atom(),
    projection_backend := projection_backend(),
    table_state := bondy_db_topology:table_state(),
    instance_ids := #{non_neg_integer() := binary()},
    cache_handles := #{non_neg_integer() := term()},
    %% Secondary indexes declared via `open_table` `indexes => [Spec]`,
    %% keyed by index name. Each is an independent ETS shard-set under
    %% `(Namespace, IndexName, SecShard)` — see `index_provision/0`.
    indexes := #{atom() := index_provision()}
}.

%% A provisioned secondary index: its declarative spec, secondary shard
%% count, the effective (always memory) topology + table state that own
%% its ETS projection tables, and the per-secondary-shard cache handles.
-type index_provision() :: #{
    spec := bondy_oplog_index_spec:spec(),
    sec_shard_count := pos_integer(),
    topology := module(),
    table_state := bondy_db_topology:table_state(),
    cache_handles := #{non_neg_integer() := term()},
    %% Per-secondary-shard `bondy_oplog_secondary_writer` pid (IDX-3).
    writer_pids := #{non_neg_integer() := pid()}
}.

%% =============================================================================
%% API
%% =============================================================================

-doc """
Open a DB instance named `Name` against the topology in `Opts`.

Required keys in `Opts`:

| Key | Type | Meaning |
|---|---|---|
| `topology` | `module()` | A module implementing `bondy_db_topology` |

Optional keys (cascade to table defaults):

| Key | Default | Meaning |
|---|---|---|
| `topology_opts` | `#{}` | Passed to `Topology:init/2` |
| `shard_count` | `8` | Default shard count for tables |
| `fold_module` | `lww_register` | Default fold strategy |

Returns the opaque `Db` handle. Callers MUST eventually call `close/1`
to release the topology's physical resources.
""".
-spec open(Name :: atom(), Opts :: map()) -> {ok, db()} | {error, term()}.

open(Name, Opts) when is_atom(Name), is_map(Opts) ->
    case maps:find(topology, Opts) of
        {ok, Topology} when is_atom(Topology) ->
            TopologyOpts = maps:get(topology_opts, Opts, #{}),
            case Topology:init(Name, TopologyOpts) of
                {ok, State} ->
                    case ensure_ets_provider(Name, Topology) of
                        {ok, EtsProvider} ->
                            Db = #{
                                name => Name,
                                topology => Topology,
                                topology_state => State,
                                ets_provider => EtsProvider,
                                opts => Opts,
                                hlc => bondy_oplog_hlc:new()
                            },
                            {ok, Db};
                        {error, _} = Err ->
                            _ = Topology:shutdown(State),
                            Err
                    end;
                {error, _} = Err ->
                    Err
            end;
        error ->
            {error, {missing_required_opt, topology}}
    end.

%% @private
%% A DB-scoped projection provider for `projection_backend => ets`
%% (ephemeral) tables. A `bondy_db_topology_memory` DB already is one —
%% its own owner serves every table — so it needs no separate provider
%% (`undefined`). Any other topology gets a dedicated
%% `bondy_db_topology_memory` state (one owner gen_server per DB), used
%% only when an ephemeral table is opened and torn down in `close/1`.
ensure_ets_provider(_Name, bondy_db_topology_memory) ->
    {ok, undefined};
ensure_ets_provider(Name, _Topology) ->
    bondy_db_topology_memory:init(Name, #{}).

-doc """
Open a logical table for `EntityType` inside `Db`.

The topology provisions the per-shard projection-adapter handles. The
facade then registers each `(Namespace, primary, Shard)` triple with
`bondy_db_core_registry`, starts a `bondy_oplog_instance` per shard
with the substrate write-path wired up, and stashes the resulting
state in the `Table` handle.

Per-table `Opts` override DB-level defaults. The merged `Opts` MUST
include `fold_module`. The chosen fold module determines:

- the cell state representation (`encode_state/1` / `decode_state/1`),
- the event shape accepted by `apply/4`,
- the conflict-resolution rules used during the applier's
  read-modify-write.

## Projection backend (durable vs ephemeral)

`projection_backend => leveled | ets` selects this table's projection
storage, independently per table — so one DB can mix durable and
ephemeral tables:

- `leveled` (default on leveled topologies) — the DB's topology
  provisions a durable leveled projection, as today.
- `ets` (default on `bondy_db_topology_memory`) — an in-RAM
  `bondy_oplog_projection_ets` projection, hosted in the DB's
  `bondy_db_topology_memory` provider; nothing for this table is
  written to disk.

A `projection_backend => ets` table is only fully **ephemeral** when the
rest of its stack is in-memory too. The knobs are low-level and the
caller is responsible for keeping them consistent — set them in the
per-table `oplog_instance_opts` (which replaces, not merges into, the
DB-level one):

```erlang
open_table(Db, registrations, #{
    projection_backend => ets,
    oplog_instance_opts => #{
        backend => ets,          %% in-memory MST store
        durability => ephemeral  %% acknowledge no durable storage;
                                 %% silences the no-storage warning
    }
    %% and NO storage_path anywhere in the cascade
}).
```

`durability => ephemeral` is the explicit "no durable storage is
intended" acknowledgement — it suppresses the loud
`bondy_oplog_instance_sup` warning that otherwise flags a missing
`storage_path` as a kill-restart footgun. It does **not** itself pin
the stack in-memory: it is the caller's `projection_backend => ets` +
`backend => ets` + absent `storage_path` that do that. The WAL still
writes (and fsyncs) to a per-PID tmp path
(`/tmp/bondy_oplog_wal/<os_pid>/...`); ephemerality across a restart
comes from that path being `os:getpid()`-namespaced — a fresh BEAM
never replays the prior run's segments — not from the WAL being
non-durable within a run.
""".
-spec open_table(
    Db :: db(),
    EntityType :: atom(),
    Opts :: map()
) -> {ok, table()} | {error, term()}.

open_table(
    #{topology := Topology} = Db,
    EntityType,
    Opts
) when
    is_atom(EntityType), is_map(Opts)
->
    Merged = merge_opts(maps:get(opts, Db), Opts),
    case maps:find(fold_module, Merged) of
        {ok, FoldModule} when is_atom(FoldModule) ->
            case resolve_backend(Topology, Merged) of
                {ok, Backend} ->
                    {EffTopology, EffState} =
                        effective_topology(Backend, Db),
                    open_table(
                        Db,
                        EntityType,
                        Merged,
                        FoldModule,
                        Backend,
                        EffTopology,
                        EffState
                    );
                {error, _} = Err ->
                    Err
            end;
        error ->
            {error, {missing_required_opt, fold_module}}
    end.

%% @private
open_table(Db, EntityType, Merged, FoldModule, Backend, Topology, State) ->
    %% Validate any declared index specs up front, before provisioning a
    %% single primary shard — a bad spec must not churn instances/Bookies.
    case validate_index_specs(maps:get(indexes, Merged, [])) of
        ok ->
            open_table_provision(
                Db, EntityType, Merged, FoldModule, Backend, Topology, State
            );
        {error, _} = Err ->
            Err
    end.

%% @private
open_table_provision(
    Db, EntityType, Merged, FoldModule, Backend, Topology, State
) ->
    ShardCount = maps:get(shard_count, Merged, ?DEFAULT_SHARD_COUNT),
    DbName = maps:get(name, Db),
    NS = namespace_atom(DbName, EntityType),
    %% A3 — default the applier's OldValue frame-cache ON for durable
    %% (leveled) projections and OFF for ephemeral (ets) ones. The cache
    %% elides the projection journal read on the per-cell write path: for
    %% leveled that read hits the on-disk journal — the dominant per-shard
    %% durable-write cost (~+47% throughput when cached, measured on Fly
    %% Linux: cell_apply 42ms → 7.5ms) — while for ets the OldValue read is
    %% already in-memory, so the cache is pure overhead. A caller-supplied
    %% `oldstate_cache` (under `oplog_instance_opts.applier`) always wins.
    OplogOpts = default_oldstate_cache_opt(
        maps:get(oplog_instance_opts, Merged, #{}), Backend
    ),
    %% Native operation-based CRDT for the cell projection. An explicit
    %% `crdt_module` wins; otherwise the `fold_module` is mapped to its
    %% native op-based twin via
    %% `bondy_oplog_cell_kernel:default_crdt_for_fold/1` (PR-Z: every former
    %% fold has a byte-identical CRDT twin, so durable cells decode either
    %% way). An unknown label maps to `undefined`; the kernel's
    %% `from_modules/2` then errors at open. Threaded only into the registry
    %% Config (not the oplog instance opts — that would engage the
    %% monolithic CRDT path).
    CrdtModule =
        case maps:get(crdt_module, Merged, undefined) of
            undefined ->
                bondy_oplog_cell_kernel:default_crdt_for_fold(FoldModule);
            ExplicitCrdt ->
                ExplicitCrdt
        end,
    %% Fail fast: a `tier_2` CRDT MUST be `order_independent` (its eager
    %% `apply_op` must equal the group `interpret_cog`, since the DVV join
    %% is commutative). Catches a mis-declared module at open, not at the
    %% first silent divergence.
    ok = assert_causal_tier_consistency(CrdtModule),
    %% Static secondary-index descriptors (already validated). The primary
    %% appliers need them at start to term-diff and dispatch index updates;
    %% the live writers they dispatch to are resolved from the registry, so
    %% the descriptors only carry the spec + secondary shard count.
    SecIndexes = index_descriptors(maps:get(indexes, Merged, []), ShardCount),
    case Topology:open_table(EntityType, ShardCount, Merged, State) of
        {ok, TableState, _NewState} ->
            case
                provision_shards(
                    NS,
                    DbName,
                    EntityType,
                    ShardCount,
                    FoldModule,
                    CrdtModule,
                    OplogOpts,
                    SecIndexes,
                    Topology,
                    TableState
                )
            of
                {ok, InstanceIds, CacheHandles} ->
                    case provision_indexes(Db, NS, Merged, ShardCount) of
                        {ok, IndexMap} ->
                            %% Mandatory startup backfill (IDX-4): the
                            %% index is always ETS and starts empty, so
                            %% re-fold the (possibly durable, possibly
                            %% peer-bootstrapped) primary into it before
                            %% returning. Also freshens every secondary
                            %% shard so a finite `max_lag` read passes even
                            %% on a shard whose working set is empty.
                            ok = backfill_indexes(NS, IndexMap),
                            {ok, #{
                                db_name => DbName,
                                db_topology => Topology,
                                db_hlc => maps:get(hlc, Db),
                                entity_type => EntityType,
                                namespace => NS,
                                shard_count => ShardCount,
                                fold_module => FoldModule,
                                crdt_module => CrdtModule,
                                causal_tier => causal_tier_of(CrdtModule),
                                projection_backend => Backend,
                                table_state => TableState,
                                instance_ids => InstanceIds,
                                cache_handles => CacheHandles,
                                indexes => IndexMap
                            }};
                        {error, _} = Err ->
                            %% Indexes failed after the primary shards came
                            %% up — roll the primary shards back too so the
                            %% caller never inherits a half-built table.
                            lists:foreach(
                                fun(S) ->
                                    teardown_shard(
                                        NS,
                                        S,
                                        InstanceIds,
                                        CacheHandles,
                                        Topology,
                                        TableState
                                    )
                                end,
                                lists:seq(0, ShardCount - 1)
                            ),
                            _ = Topology:close_table(TableState, State),
                            Err
                    end;
                {error, _} = Err ->
                    %% The effective topology's open_table already
                    %% provisioned adapter handles for this table — tear
                    %% them down so a failed provisioning does not leak
                    %% Bookies (leveled) or ETS tables (ets).
                    _ = Topology:close_table(TableState, State),
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

%% @private
%% Resolve the table's projection backend, rejecting impossible combos.
%% `bondy_db_topology_memory` has no leveled capability, so it is
%% ets-only; every other topology defaults to leveled (preserving prior
%% behaviour) and may opt a table into ets.
resolve_backend(bondy_db_topology_memory, Merged) ->
    case maps:get(projection_backend, Merged, ets) of
        ets ->
            {ok, ets};
        leveled ->
            {error,
                {unsupported_projection_backend,
                    {leveled, bondy_db_topology_memory}}};
        Other ->
            {error, {invalid_projection_backend, Other}}
    end;
resolve_backend(_Topology, Merged) ->
    case maps:get(projection_backend, Merged, leveled) of
        leveled -> {ok, leveled};
        ets -> {ok, ets};
        Other -> {error, {invalid_projection_backend, Other}}
    end.

%% @private
%% Map the resolved backend to the effective projection topology + state
%% for this table. `leveled` uses the DB's own topology; `ets` uses
%% `bondy_db_topology_memory` — the DB's own state when it already is a
%% memory DB, otherwise the dedicated provider created at `open/2`.
effective_topology(leveled, #{topology := Topology, topology_state := S}) ->
    {Topology, S};
effective_topology(ets, #{
    topology := bondy_db_topology_memory, topology_state := S
}) ->
    {bondy_db_topology_memory, S};
effective_topology(ets, #{ets_provider := S}) ->
    {bondy_db_topology_memory, S}.

-doc """
Release the resources owned by `Table`. Stops every per-shard oplog
instance, unregisters every shard from `bondy_db_core_registry`,
deletes every per-shard cache table, then asks the topology to release
its physical resources (Bookies, etc.).

Whether physical resources are actually freed is still the topology's
call — single_bookie keeps its Bookie alive across `close_table/1` and
only stops it on `close/1`.
""".
-spec close_table(Table :: table()) -> ok.

close_table(
    #{
        db_topology := Topology,
        table_state := TableState,
        namespace := NS,
        shard_count := ShardCount,
        instance_ids := InstanceIds,
        cache_handles := CacheHandles
    } = Table
) ->
    teardown_indexes(NS, maps:get(indexes, Table, #{})),
    lists:foreach(
        fun(Shard) ->
            teardown_shard(
                NS, Shard, InstanceIds, CacheHandles, Topology, TableState
            )
        end,
        lists:seq(0, ShardCount - 1)
    ),
    _ = Topology:close_table(TableState, undefined),
    ok.

-doc """
Tear down `Db`: stop every Bookie, release every resource. Calls the
topology's `shutdown/1` and, if a dedicated ETS provider was created at
`open/2` (for `projection_backend => ets` tables on a non-memory
topology), stops it too.

Callers SHOULD `close_table/1` each open table first. `close/1` does
not chase open tables — it only walks the topology.
""".
-spec close(Db :: db()) -> ok.

close(#{topology := Topology, topology_state := State} = Db) ->
    _ =
        case maps:get(ets_provider, Db, undefined) of
            undefined -> ok;
            EtsState -> bondy_db_topology_memory:shutdown(EtsState)
        end,
    Topology:shutdown(State).

-doc """
Generate a fresh HLC from the DB's clock. Callers inject this HLC into
fold-specific events before calling `apply/4`.

Strictly greater than the previous value returned by `tick/1` on the
same DB.
""".
-spec tick(Table :: table()) -> bondy_oplog_hlc:hlc().

tick(#{db_hlc := Hlc}) ->
    bondy_oplog_hlc:now(Hlc).

-doc """
Apply a fold-specific event to `(Realm, Key)` inside `Table`.

Builds `{cell_apply, Bucket, Key, FoldEvent}` (Bucket composed via
`Topology:bucket_for/3`) and appends it through the shard's oplog
instance. Once the WAL append returns, blocks on
`bondy_oplog:await_apply/1` so the projection write is visible to a
subsequent `read/3` from the same caller (read-your-writes).

The event shape is whatever the table's `fold_module:apply_event/3`
accepts. Idempotency and conflict resolution are inherited from the
fold's contract; the facade does not validate event shapes.

Returns `ok` on successful WAL durability + applier commit, or
`{error, _}` if the WAL refuses the append or the applier's drain
times out.
""".
-spec apply(
    Table :: table(),
    Realm :: realm(),
    Key :: binary(),
    Event :: term()
) -> ok | {error, term()}.

apply(
    #{
        db_topology := Topology,
        table_state := TableState,
        entity_type := EntityType
    } = Table,
    Realm,
    Key,
    Event
) when
    is_binary(Realm), is_binary(Key)
->
    Bucket = Topology:bucket_for(EntityType, Realm, TableState),
    InstanceId = instance_id_for(Table, Bucket, Key),
    case maps:get(causal_tier, Table, tier_0) of
        tier_2 ->
            apply_with_context(InstanceId, Bucket, Key, Event);
        _ ->
            %% tier_0 / tier_1 write path: the op carries whatever
            %% causality the type needs in-band, so the write is a
            %% straight WAL append with no server-side round-trip.
            append_and_await(
                InstanceId, {cell_apply, Bucket, Key, Event}, undefined
            )
    end.

%% @private
%% tier_2 write path: stamp the cell's CURRENT causal context (a version
%% vector, read in the applier's single-cell scope) into the event
%% `meta`, so `interpret_cog/2` can resolve concurrency. The op itself
%% stays pure (no state-inspecting resolution). This is the ORIGIN
%% stamp; remote events arrive
%% already-stamped via `append_remote` and are never re-stamped.
%% Read-your-writes holds because `await/1` commits each write's
%% projection before the next write reads context.
apply_with_context(InstanceId, Bucket, Key, Event) ->
    try cell_context(InstanceId, Bucket, Key) of
        {error, _} = Err ->
            Err;
        {ok, Context} ->
            Op = {cell_apply, Bucket, Key, Event},
            append_and_await(InstanceId, Op, Context)
    catch
        exit:{noproc, _} ->
            {error, {instance_unavailable, InstanceId}};
        exit:{shutdown, _} ->
            {error, {instance_unavailable, InstanceId}}
    end.

%% @private
append_and_await(InstanceId, Op, Meta) ->
    try bondy_oplog:append(InstanceId, Op, Meta) of
        {error, _} = Err ->
            Err;
        _EventKey ->
            await(InstanceId)
    catch
        exit:{noproc, _} ->
            {error, {instance_unavailable, InstanceId}};
        exit:{shutdown, _} ->
            {error, {instance_unavailable, InstanceId}}
    end.

%% @private
%% Read the cell's current causal context (`context_of/1`) in the
%% applier's single-cell scope (so it reflects committed writes for
%% read-your-writes). Returns `{ok, undefined}` when the CRDT does not
%% carry a context.
cell_context(InstanceId, Bucket, Key) ->
    case bondy_oplog_registry:applier_pid(InstanceId) of
        undefined ->
            {error, {instance_unavailable, InstanceId}};
        ApplierPid ->
            bondy_oplog_applier:cell_context(ApplierPid, Bucket, Key)
    end.

-doc """
Increment the PN-Counter at `(Realm, Key)` in `Table` by `Delta`.

Convenience wrapper over `apply/4` for tables backed by the
`pn_counter` fold. `Delta` may be negative (a "decrement" is just
`counter_inc(_, _, _, -K)`). The fold absorbs the event under the
per-Origin Seq number tracked in the WAL event key — duplicate
delivery and replay are no-ops by construction.

Returns `ok` once the WAL append is durable and the applier has
committed the projection write, or `{error, _}` on substrate failure.

The fold module is **not** validated here; using this helper against
a non-`pn_counter` table will route a `{inc, Delta}` event into a
fold that does not understand it and `apply/4` will fail at the
projection layer.
""".
-spec counter_inc(
    Table :: table(),
    Realm :: realm(),
    Key :: binary(),
    Delta :: integer()
) -> ok | {error, term()}.

counter_inc(Table, Realm, Key, Delta) when is_integer(Delta) ->
    ?MODULE:apply(Table, Realm, Key, {inc, Delta}).

-doc """
Read the decoded fold state for `(Realm, Key)` from `Table`.

Routes through `bondy_db_core:read/4`, which hits the per-shard cache
on the fast path and falls back to the projection + cache-populate on
miss. The fold-decoded state is returned together with the cell's
recorded HLC.

Returns:

- `{ok, State, Hlc}` — the cell's current fold state and HLC. `State`
  shape is fold-specific; the caller pattern-matches per their CRDT.
- `not_found` — no cell exists for `(Realm, Key)`.
- `{error, _}` — adapter or substrate failure.

A cell whose state is the fold's `initial_value/0` is **NOT** filtered
out — the facade returns whatever the substrate gives it. If a CRDT's
"empty" state should be invisible to callers, that policy lives above
this facade.
""".
-spec read(
    Table :: table(),
    Realm :: realm(),
    Key :: binary()
) ->
    {ok, Value :: term(), Hlc :: bondy_oplog_hlc:hlc()}
    | not_found
    | {error, term()}.

read(
    #{
        namespace := NS,
        db_topology := Topology,
        table_state := TableState,
        entity_type := EntityType
    },
    Realm,
    Key
) when
    is_binary(Realm), is_binary(Key)
->
    Bucket = Topology:bucket_for(EntityType, Realm, TableState),
    case bondy_db_core:read(NS, ?INDEX, Bucket, Key) of
        {Value, Hlc} when Value =/= undefined ->
            {ok, Value, Hlc};
        undefined ->
            not_found;
        {error, _} = Err ->
            Err
    end.

-doc """
Single-shard range scan over `(Realm, [Low, High))`.

The shard is selected by `phash2(Low, ShardCount)` unless the caller
passes `Opts#{shard => N}`. Callers whose `[Low, High)` spans more than
one shard MUST scatter across shards themselves and merge the results;
the facade does not do scatter-merge in v1.

Routes through `bondy_db_core:range/4`, which merges the projection
with the per-shard overlay (currently always empty at this layer).
Realm is folded into both bounds so the substrate scan stays inside
the realm's prefix.

Returns `{ok, [{Key, State, Hlc}]}` — one row per cell present in the
range, in ascending key order. `State` is the fold's decoded state.
""".
-spec range(
    Table :: table(),
    Realm :: realm(),
    Low :: binary(),
    High :: binary(),
    Opts :: map()
) ->
    {ok, [{Key :: binary(), State :: term(), Hlc :: bondy_oplog_hlc:hlc()}]}
    | {error, term()}.

range(
    #{
        namespace := NS,
        shard_count := ShardCount,
        db_topology := Topology,
        table_state := TableState,
        entity_type := EntityType
    },
    Realm,
    Low,
    High,
    Opts
) when
    is_binary(Realm),
    is_binary(Low),
    is_binary(High),
    is_map(Opts)
->
    Bucket = Topology:bucket_for(EntityType, Realm, TableState),
    Shard = maps:get(shard, Opts, erlang:phash2({Bucket, Low}, ShardCount)),
    AdapterOpts = (maps:without([shard], Opts))#{shard => Shard},
    bondy_db_core:range(NS, ?INDEX, Bucket, {Low, High}, AdapterOpts).

-doc """
Equality lookup against secondary index `IndexName`: the primary keys
(and any denormalised columns) whose indexed term equals `Term` within
`Realm`.

`Term` is normalised through the index's spec (e.g. `downcase`) so it
matches the stored terms, then resolved to the single secondary shard
that holds it (`phash2({SecBucket, Term}, SecShardCount)`) and scanned
over that term's contiguous key window.

## Opts

- `max_lag` — refuse with `{error, {stale_secondary, IndexName, Lag}}`
  unless the touched shard was freshened within `max_lag` ms (defaults to
  the spec's `max_lag`, itself `infinity` = never refuse). `Lag` is the
  shard's wall-clock ms lag, or `infinity` when it was never freshened or
  is flagged for rebuild. Since IDX-4 the startup backfill freshens every
  shard at open, so a finite `max_lag` over an up-to-date index passes;
  refusal signals a genuinely lagging or rebuilding shard.
- `fallback => primary` — instead of refusing a stale read, scan the
  primary directly and recompute the matching keys (slow but correct,
  `MST_DB_DESIGN.md` §13.1). Bounded by an internal cell cap.
- `limit`, `direction` — forwarded to the underlying range scan (and the
  fallback).

Returns `{ok, [{PrimaryKey, Columns}]}` (in `(term, primary-key)` order;
`Columns` is the decoded projection map, `#{}` for a pointer-only index),
`{error, {unknown_index, IndexName}}`, or a substrate `{error, _}`.
Retracted entries (tombstones) are filtered out by the substrate.
""".
-spec index_get(
    Table :: table(),
    Realm :: realm(),
    IndexName :: atom(),
    Term :: bondy_oplog_index_key:term_value(),
    Opts :: map()
) ->
    {ok, [{PrimaryKey :: binary(), Columns :: map()}]}
    | {error, term()}.

index_get(Table, Realm, IndexName, Term, Opts) when
    is_binary(Realm), is_atom(IndexName), is_map(Opts)
->
    with_index(Table, IndexName, fun(Spec, SecShardCount) ->
        NS = maps:get(namespace, Table),
        SecBucket = index_bucket(Table, Realm, IndexName),
        Norm = bondy_oplog_index_spec:normalize_term(Spec, Term),
        MaxLag = maps:get(max_lag, Opts, bondy_oplog_index_spec:max_lag(Spec)),
        SecShard =
            bondy_oplog_index_key:shard(SecBucket, Norm, SecShardCount),
        case ensure_shard_fresh(NS, IndexName, SecShard, MaxLag) of
            ok ->
                {Low, High} = bondy_oplog_index_key:equality_bounds(Norm),
                RangeOpts = (index_range_opts(Opts))#{shard => SecShard},
                read_index(NS, IndexName, SecBucket, Low, High, RangeOpts);
            {stale, Lag} ->
                stale_or_fallback(
                    Opts,
                    IndexName,
                    Lag,
                    fun() ->
                        primary_scan_eq(Table, Realm, Spec, Norm, Opts)
                    end
                )
        end
    end).

-doc """
Ordered range scan against secondary index `IndexName`: the primary keys
(and columns) whose indexed term is in the half-open `[LoTerm, HiTerm)`
within `Realm`.

Both bounds are normalised through the index's spec. The scan scatters
across every secondary shard (terms span all shards) and the merged
result is globally ordered by `(term, primary-key)`. `Opts` are as for
`index_get/5` (`max_lag` refusal, `limit`, `direction`); `limit` caps the
merged result.

Returns `{ok, [{PrimaryKey, Columns}]}`,
`{error, {unknown_index, IndexName}}`, or a substrate `{error, _}`
(a single failing shard aborts the whole scan — no partial results).
""".
-spec index_range(
    Table :: table(),
    Realm :: realm(),
    IndexName :: atom(),
    LoTerm :: bondy_oplog_index_key:term_value(),
    HiTerm :: bondy_oplog_index_key:term_value(),
    Opts :: map()
) ->
    {ok, [{PrimaryKey :: binary(), Columns :: map()}]}
    | {error, term()}.

index_range(Table, Realm, IndexName, LoTerm, HiTerm, Opts) when
    is_binary(Realm), is_atom(IndexName), is_map(Opts)
->
    with_index(Table, IndexName, fun(Spec, SecShardCount) ->
        NS = maps:get(namespace, Table),
        SecBucket = index_bucket(Table, Realm, IndexName),
        Lo = bondy_oplog_index_spec:normalize_term(Spec, LoTerm),
        Hi = bondy_oplog_index_spec:normalize_term(Spec, HiTerm),
        MaxLag = maps:get(max_lag, Opts, bondy_oplog_index_spec:max_lag(Spec)),
        case ensure_index_fresh(NS, IndexName, SecShardCount, MaxLag) of
            ok ->
                {Low, High} = bondy_oplog_index_key:range_bounds(Lo, Hi),
                case
                    bondy_db_core:range_all(
                        NS,
                        IndexName,
                        SecBucket,
                        {Low, High},
                        index_range_opts(Opts)
                    )
                of
                    {ok, Rows} -> {ok, index_rows(Rows)};
                    {error, _} = Err -> Err
                end;
            {stale, Lag} ->
                stale_or_fallback(
                    Opts,
                    IndexName,
                    Lag,
                    fun() ->
                        primary_scan_range(Table, Realm, Spec, Lo, Hi, Opts)
                    end
                )
        end
    end).

-doc """
Rebuild secondary index `IndexName` of `Table` from the primary (IDX-4):
wipe its ETS shards, re-fold every primary shard's MST, and re-dispatch a
`put` for every live term. Synchronous — returns once the index has been
re-materialised and its shards freshened, so a `max_lag` read issued after
this passes. `{error, {unknown_index, IndexName}}` for an unknown index.

The same recovery the substrate runs autonomously on a saturation drop or
a writer crash; exposed for operators (and tests) to force on demand.
""".
-spec rebuild_index(Table :: table(), IndexName :: atom()) ->
    ok | {error, term()}.

rebuild_index(Table, IndexName) when is_atom(IndexName) ->
    with_index(Table, IndexName, fun(_Spec, _SecShardCount) ->
        bondy_oplog_index_rebuild:rebuild_sync(
            maps:get(namespace, Table), IndexName
        )
    end).

-doc "Rebuild every secondary index declared on `Table` (see `rebuild_index/2`).".
-spec rebuild_indexes(Table :: table()) -> ok.

rebuild_indexes(Table) ->
    NS = maps:get(namespace, Table),
    maps:foreach(
        fun(IndexName, _Provision) ->
            _ = bondy_oplog_index_rebuild:rebuild_sync(NS, IndexName)
        end,
        maps:get(indexes, Table, #{})
    ).

-doc """
Per-secondary-shard lag diagnostics for `IndexName` (IDX-4). Returns
`#{SecShard => #{lag => infinity | non_neg_integer(), inflight =>
non_neg_integer(), needs_rebuild => boolean()}}`, where `lag` is the
wall-clock ms since the shard was last freshened (`infinity` when never
freshened or flagged for rebuild), `inflight` is the writer's
dispatched-but-unflushed backlog, and `needs_rebuild` whether a rebuild is
pending. `{error, {unknown_index, IndexName}}` for an unknown index.
""".
-spec index_lag(Table :: table(), IndexName :: atom()) ->
    {ok, #{non_neg_integer() := map()}} | {error, term()}.

index_lag(Table, IndexName) when is_atom(IndexName) ->
    with_index(Table, IndexName, fun(_Spec, SecShardCount) ->
        NS = maps:get(namespace, Table),
        Map = maps:from_list([
            {Shard, shard_lag_info(NS, IndexName, Shard)}
         || Shard <- lists:seq(0, SecShardCount - 1)
        ]),
        {ok, Map}
    end).

-doc """
Return an informational map about `Db` or `Table`. Intended for
operator introspection and tests; the shape is not stable across
versions.
""".
-spec info(db() | table()) -> map().

info(#{name := Name, topology := Topology, opts := Opts}) ->
    #{
        kind => db,
        name => Name,
        topology => Topology,
        opts => Opts
    };
info(
    #{
        entity_type := ET,
        shard_count := SC,
        fold_module := Fold,
        db_name := DbName,
        db_topology := Topology,
        namespace := NS
    } = Table
) ->
    #{
        kind => table,
        db_name => DbName,
        topology => Topology,
        projection_backend => maps:get(projection_backend, Table, leveled),
        entity_type => ET,
        namespace => NS,
        shard_count => SC,
        fold_module => Fold,
        crdt_module => maps:get(crdt_module, Table, undefined),
        causal_tier => maps:get(causal_tier, Table, tier_0),
        indexes => maps:map(
            fun(_Name, Provision) ->
                #{
                    sec_shard_count => maps:get(sec_shard_count, Provision),
                    projects => bondy_oplog_index_spec:projects(
                        maps:get(spec, Provision)
                    )
                }
            end,
            maps:get(indexes, Table, #{})
        )
    }.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
%% Atom is derived once per `open_table` from values supplied by the
%% caller's own code — a bounded set, no atom-leak risk from untrusted
%% input.
namespace_atom(DbName, EntityType) ->
    list_to_atom(
        atom_to_list(DbName) ++ "_" ++ atom_to_list(EntityType)
    ).

%% @private
%% Provision shards `0 .. Count-1` with rollback. `ProvisionFun(Shard)`
%% returns `{ok, ValA, ValB}` — the per-shard result pair, folded into two
%% accumulator maps keyed by `Shard` — or `{error, _}`. On any failure
%% every shard already built (`0 .. Shard-1`) is handed to
%% `TeardownFun(S, AccA, AccB)` (best-effort) and the error is returned.
%% Shared by the primary-shard and secondary-index-shard loops; the (A, B)
%% pair carries (instance-id, cache) for the primary and (cache, writer)
%% for an index, in provision-then-teardown order.
provision_seq(Count, ProvisionFun, TeardownFun) ->
    provision_seq(Count, ProvisionFun, TeardownFun, 0, #{}, #{}).

provision_seq(Count, _ProvisionFun, _TeardownFun, Count, AccA, AccB) ->
    {ok, AccA, AccB};
provision_seq(Count, ProvisionFun, TeardownFun, Shard, AccA, AccB) ->
    case ProvisionFun(Shard) of
        {ok, ValA, ValB} ->
            provision_seq(
                Count,
                ProvisionFun,
                TeardownFun,
                Shard + 1,
                AccA#{Shard => ValA},
                AccB#{Shard => ValB}
            );
        {error, _} = Err ->
            lists:foreach(
                fun(S) -> TeardownFun(S, AccA, AccB) end,
                lists:seq(0, Shard - 1)
            ),
            Err
    end.

%% @private
%% Provision every shard of a newly opened table. On any failure, roll
%% back partial provisioning so the caller does not inherit a half-built
%% table. `OplogOpts` is a map of extra options forwarded verbatim to
%% `bondy_oplog:start_instance/2` per shard — typically used to set
%% `backend` (e.g. `bondy_mst_pack_store`), `storage_path`, or
%% `fsync_mode`. Per-shard `fold_module`, `applier`, and `wal` opts
%% take precedence over keys with the same name in `OplogOpts`.
provision_shards(
    NS,
    DbName,
    EntityType,
    ShardCount,
    FoldModule,
    CrdtModule,
    OplogOpts,
    SecIndexes,
    Topology,
    TableState
) ->
    provision_seq(
        ShardCount,
        fun(Shard) ->
            provision_shard(
                NS,
                DbName,
                EntityType,
                ShardCount,
                FoldModule,
                CrdtModule,
                OplogOpts,
                SecIndexes,
                Topology,
                TableState,
                Shard
            )
        end,
        fun(S, Ids, Caches) ->
            teardown_shard(NS, S, Ids, Caches, Topology, TableState)
        end
    ).

%% @private
provision_shard(
    NS,
    DbName,
    EntityType,
    ShardCount,
    FoldModule,
    CrdtModule,
    OplogOpts,
    SecIndexes,
    Topology,
    TableState,
    Shard
) ->
    InstanceId = encode_instance_id(DbName, EntityType, Shard),
    case Topology:route(Shard, TableState) of
        {ok, ProjAdapter, ProjHandle} ->
            case acquire_cache(Topology, TableState, NS, ?INDEX, Shard) of
                {ok, Owner, CacheAdapter, CacheHandle} ->
                    Config = #{
                        shard_count => ShardCount,
                        cache_adapter => CacheAdapter,
                        cache_handle => CacheHandle,
                        projection_adapter => ProjAdapter,
                        projection_handle => ProjHandle,
                        fold_module => FoldModule,
                        %% Optional native CRDT for the cell projection;
                        %% `undefined` keeps the legacy fold path.
                        crdt_module => CrdtModule,
                        %% The CRDT's declared causal tier (default tier_0).
                        %% tier_2 provisions the per-cell DVV context stamp.
                        causal_tier => causal_tier_of(CrdtModule),
                        overlay => disabled,
                        %% Recorded so a secondary-index rebuild can find
                        %% this primary shard's applier from the registry.
                        instance_id => InstanceId,
                        %% Bind the registry monitor to the topology's
                        %% long-lived owner (the calling process when the
                        %% topology has none), so the row survives the
                        %% transient open_table caller exactly as the
                        %% projection + cache do.
                        owner => Owner
                    },
                    case
                        bondy_db_core_registry:register(
                            NS, ?INDEX, Shard, Config
                        )
                    of
                        ok ->
                            start_shard_instance(
                                NS,
                                InstanceId,
                                Shard,
                                FoldModule,
                                OplogOpts,
                                SecIndexes,
                                CacheHandle,
                                Topology,
                                TableState
                            );
                        {error, _} = Err ->
                            ok = release_cache(
                                Topology, TableState, CacheHandle
                            ),
                            Err
                    end;
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

%% @private
%% @private
%% A CRDT module's declared causal tier, or `tier_0` when no native CRDT
%% is configured (the legacy fold path). `tier_2` provisions the per-cell
%% DVV causal-context stamp for the table's writes.
causal_tier_of(undefined) ->
    tier_0;
causal_tier_of(CrdtModule) when is_atom(CrdtModule) ->
    %% `ensure_loaded` first: `function_exported/3` reports `false` for a
    %% not-yet-loaded module, which would silently mis-classify a tier_2
    %% CRDT as tier_0 (skipping both the DVV stamp and the safety
    %% assertion). `causal_tier/0` is a required `bondy_oplog_crdt`
    %% callback, so a loaded native CRDT always exports it.
    _ = code:ensure_loaded(CrdtModule),
    case erlang:function_exported(CrdtModule, causal_tier, 0) of
        true -> CrdtModule:causal_tier();
        false -> tier_0
    end.

%% @private
%% Fail fast at open: a `tier_2` CRDT MUST be `order_independent` — its
%% eager `apply_op` must equal the group `interpret_cog` (the DVV join is
%% commutative). A tier_2 type that is not order-independent would diverge
%% silently between the write and read paths.
assert_causal_tier_consistency(undefined) ->
    ok;
assert_causal_tier_consistency(CrdtModule) when is_atom(CrdtModule) ->
    case causal_tier_of(CrdtModule) of
        tier_2 ->
            IsOI =
                erlang:function_exported(CrdtModule, order_independent, 0) andalso
                    CrdtModule:order_independent(),
            case IsOI of
                true -> ok;
                false -> error({tier_2_requires_order_independent, CrdtModule})
            end;
        _ ->
            ok
    end.

%% @private
%% Provision the per-shard read cache. A topology that wants its
%% per-shard resources to outlive the transient open_table caller (an
%% ephemeral in-memory topology) exports `provision_cache/5` and hosts
%% the cache in a long-lived owner, returning that owner pid so the
%% registry monitor can bind to it too. Topologies that omit the callback
%% get the default: a `bondy_oplog_cache_ets` table owned by — and a
%% registry monitor on — the calling process (`self()`).
acquire_cache(Topology, TableState, NS, Index, Shard) ->
    case erlang:function_exported(Topology, provision_cache, 5) of
        true ->
            case Topology:provision_cache(NS, Index, Shard, #{}, TableState) of
                {ok, #{owner := Owner, adapter := Adapter, handle := Handle}} ->
                    {ok, Owner, Adapter, Handle};
                {error, _} = Err ->
                    Err
            end;
        false ->
            case bondy_oplog_cache_ets:init(NS, Index, Shard, #{}) of
                {ok, Handle} ->
                    {ok, self(), bondy_oplog_cache_ets, Handle};
                {error, _} = Err ->
                    Err
            end
    end.

%% @private
%% Release a cache acquired by `acquire_cache/5`. The owner-hosted path
%% runs the whole-table delete inside the owner (the facade caller cannot
%% — `ets:delete/1` is owner-only); the default path deletes the
%% caller-owned table directly.
release_cache(Topology, TableState, CacheHandle) ->
    case erlang:function_exported(Topology, release_cache, 2) of
        true ->
            _ = Topology:release_cache(CacheHandle, TableState),
            ok;
        false ->
            _ = bondy_oplog_cache_ets:close(CacheHandle),
            ok
    end.

%% @private
%% `OplogOpts` is merged into the per-shard instance opts. `fold_module`
%% and the applier's *routing* keys (`cell_apply_target`,
%% `secondary_indexes`) are pinned — they carry per-shard routing the
%% caller cannot meaningfully provide — and override any caller value.
%% Caller-provided applier *tuning* (e.g. `apply_batch_max_events`,
%% `oldstate_cache`) is merged in *under* the pinned routing keys, so it
%% reaches the applier instead of being dropped. Everything else
%% (`backend`, `storage_path`, `fsync_mode`, `max_install_in_flight`,
%% etc.) is forwarded verbatim.
%% @private
%% A3 — context-sensitive default for the applier's OldValue frame-cache:
%% ON for durable (leveled) projections, OFF for ephemeral (ets). A
%% caller-supplied value under `oplog_instance_opts.applier.oldstate_cache`
%% is preserved (it always wins). See the call site in
%% `open_table_provision/7` for why.
default_oldstate_cache_opt(OplogOpts, Backend) ->
    Applier = maps:get(applier, OplogOpts, #{}),
    case maps:is_key(oldstate_cache, Applier) of
        true ->
            OplogOpts;
        false ->
            OplogOpts#{
                applier => Applier#{oldstate_cache => Backend =:= leveled}
            }
    end.

%% @private
%% `OplogOpts` is merged into the per-shard instance opts. `fold_module`
%% and the applier's *routing* keys (`cell_apply_target`,
%% `secondary_indexes`) are pinned — they carry per-shard routing the
%% caller cannot meaningfully provide — and override any caller value.
%% Caller-provided applier *tuning* (e.g. `apply_batch_max_events`,
%% `oldstate_cache`) is merged in *under* the pinned routing keys, so it
%% reaches the applier instead of being dropped. Everything else
%% (`backend`, `storage_path`, `fsync_mode`, `max_install_in_flight`,
%% etc.) is forwarded verbatim.
start_shard_instance(
    NS,
    InstanceId,
    Shard,
    FoldModule,
    OplogOpts,
    SecIndexes,
    CacheHandle,
    Topology,
    TableState
) ->
    CallerApplier = maps:get(applier, OplogOpts, #{}),
    Pinned = #{
        fold_module => FoldModule,
        applier => CallerApplier#{
            cell_apply_target => {NS, ?INDEX, Shard},
            secondary_indexes => SecIndexes
        }
    },
    Opts = maps:merge(OplogOpts, Pinned),
    case bondy_oplog:start_instance(InstanceId, Opts) of
        {ok, _Sup} ->
            {ok, InstanceId, CacheHandle};
        {error, _} = Err ->
            ok = bondy_db_core_registry:unregister(NS, ?INDEX, Shard),
            ok = release_cache(Topology, TableState, CacheHandle),
            Err
    end.

%% @private
%% Three-step per-shard teardown shared by primary shards and index shards:
%% stop the shard's worker (`StopFun`, guarded — `undefined` when never
%% started), unregister the `(NS, Index, Shard)` row, release its cache
%% (guarded). Best-effort: a dead worker or stale handle never aborts the
%% teardown (it is also the rollback path for a half-built table).
teardown_shard_common(
    NS, Index, Shard, WorkerMap, StopFun, CacheHandles, Topology, TableState
) ->
    case maps:get(Shard, WorkerMap, undefined) of
        undefined ->
            ok;
        Worker ->
            _ = StopFun(Worker),
            ok
    end,
    _ = bondy_db_core_registry:unregister(NS, Index, Shard),
    case maps:get(Shard, CacheHandles, undefined) of
        undefined ->
            ok;
        CacheHandle ->
            _ = release_cache(Topology, TableState, CacheHandle),
            ok
    end,
    ok.

%% @private
teardown_shard(NS, Shard, InstanceIds, CacheHandles, Topology, TableState) ->
    teardown_shard_common(
        NS,
        ?INDEX,
        Shard,
        InstanceIds,
        fun bondy_oplog:stop_instance/1,
        CacheHandles,
        Topology,
        TableState
    ).

%% =============================================================================
%% PRIVATE — secondary index provisioning
%% =============================================================================

%% @private
%% Provision every secondary index declared in `indexes => [Spec]`. Each
%% index is an independent ETS shard-set under `(NS, IndexName, SecShard)`,
%% always memory-backed regardless of the table's projection backend (the
%% index is rebuilt from the primary on cold start, so it is never
%% persisted). No `bondy_oplog_instance` is started — the secondary writer
%% (a lightweight gen_server) drives these cells, not the primary applier
%% subtree. Specs are validated up front (fail before any table is
%% created); a mid-loop failure rolls back the indexes already built.
provision_indexes(Db, NS, Merged, DefaultShardCount) ->
    %% Specs were already validated in `open_table/7` before any shard was
    %% provisioned.
    Specs = maps:get(indexes, Merged, []),
    provision_indexes_loop(Db, NS, Specs, DefaultShardCount, #{}).

%% @private
validate_index_specs(Specs) when is_list(Specs) ->
    validate_index_specs(Specs, sets:new([{version, 2}]));
validate_index_specs(Other) ->
    {error, {invalid_indexes, Other}}.

validate_index_specs([], _Seen) ->
    ok;
validate_index_specs([Spec | Rest], Seen) ->
    case bondy_oplog_index_spec:validate(Spec) of
        ok ->
            Name = bondy_oplog_index_spec:name(Spec),
            case check_index_name(Name, Seen) of
                ok ->
                    case check_sec_shard_count(Spec) of
                        ok ->
                            validate_index_specs(
                                Rest, sets:add_element(Name, Seen)
                            );
                        {error, _} = Err ->
                            Err
                    end;
                {error, _} = Err ->
                    Err
            end;
        {error, Reason} ->
            {error, {invalid_index_spec, Reason}}
    end.

%% @private
check_index_name(?INDEX, _Seen) ->
    %% `primary` is the substrate's reserved index id.
    {error, {reserved_index_name, ?INDEX}};
check_index_name(Name, Seen) ->
    case sets:is_element(Name, Seen) of
        true -> {error, {duplicate_index_name, Name}};
        false -> ok
    end.

%% @private
check_sec_shard_count(Spec) ->
    case maps:get(sec_shard_count, Spec, default) of
        default -> ok;
        N when is_integer(N), N > 0 -> ok;
        Bad -> {error, {invalid_sec_shard_count, Bad}}
    end.

%% @private
provision_indexes_loop(_Db, _NS, [], _DefaultShardCount, Acc) ->
    {ok, Acc};
provision_indexes_loop(Db, NS, [Spec | Rest], DefaultShardCount, Acc) ->
    case provision_index(Db, NS, Spec, DefaultShardCount) of
        {ok, Name, Provision} ->
            provision_indexes_loop(
                Db, NS, Rest, DefaultShardCount, Acc#{Name => Provision}
            );
        {error, _} = Err ->
            teardown_indexes(NS, Acc),
            Err
    end.

%% @private
%% Provision one index: create its ETS shard-set in the DB's memory
%% provider (the effective ets topology), then register a secondary shard
%% per `SecShard` with the `index_entry` fold. The shard count defaults to
%% the primary's but can be overridden per index via `sec_shard_count`.
provision_index(Db, NS, Spec, DefaultShardCount) ->
    Name = bondy_oplog_index_spec:name(Spec),
    SecShardCount = maps:get(sec_shard_count, Spec, DefaultShardCount),
    CoalesceMs = bondy_oplog_index_spec:coalesce_ms(Spec),
    {Topology, EtsState} = effective_topology(ets, Db),
    case Topology:open_table(Name, SecShardCount, #{}, EtsState) of
        {ok, TableState, _NewState} ->
            case
                provision_index_shards(
                    NS, Name, SecShardCount, CoalesceMs, Topology, TableState
                )
            of
                {ok, CacheHandles, Writers} ->
                    {ok, Name, #{
                        spec => Spec,
                        sec_shard_count => SecShardCount,
                        topology => Topology,
                        table_state => TableState,
                        cache_handles => CacheHandles,
                        writer_pids => Writers
                    }};
                {error, _} = Err ->
                    _ = Topology:close_table(TableState, EtsState),
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

%% @private
provision_index_shards(
    NS, Name, SecShardCount, CoalesceMs, Topology, TableState
) ->
    provision_seq(
        SecShardCount,
        fun(Shard) ->
            provision_index_shard(
                NS, Name, SecShardCount, CoalesceMs, Topology, TableState, Shard
            )
        end,
        fun(S, Caches, Writers) ->
            teardown_index_shard(
                NS, Name, S, Caches, Writers, Topology, TableState
            )
        end
    ).

%% @private
%% A secondary shard is a projection table + cache + registry entry + a
%% `bondy_oplog_secondary_writer` — no oplog instance. The
%% `bondy_oplog_crdt_index_entry` CRDT gives the substrate's read/range path
%% the right decode; the writer (started after the row is registered so its
%% `set_writer_pid/4` stamp lands) drains dispatched index ops into the
%% projection.
provision_index_shard(
    NS, Name, SecShardCount, CoalesceMs, Topology, TableState, Shard
) ->
    case Topology:route(Shard, TableState) of
        {ok, ProjAdapter, ProjHandle} ->
            case acquire_cache(Topology, TableState, NS, Name, Shard) of
                {ok, Owner, CacheAdapter, CacheHandle} ->
                    Config = #{
                        shard_count => SecShardCount,
                        cache_adapter => CacheAdapter,
                        cache_handle => CacheHandle,
                        projection_adapter => ProjAdapter,
                        projection_handle => ProjHandle,
                        %% The index cell kernel is the native op-based CRDT
                        %% twin; `fold_module` is left unset (the read path
                        %% selects the crdt_module). Byte-identical encoding,
                        %% so existing durable index cells decode unchanged.
                        fold_module => undefined,
                        crdt_module => bondy_oplog_crdt_index_entry,
                        overlay => disabled,
                        %% IDX-4 back-pressure atomics (in-flight count +
                        %% needs_rebuild flag). Index shards only.
                        inflight_atomics => atomics:new(2, [{signed, true}]),
                        owner => Owner
                    },
                    case
                        bondy_db_core_registry:register(NS, Name, Shard, Config)
                    of
                        ok ->
                            case
                                start_index_writer(NS, Name, Shard, CoalesceMs)
                            of
                                {ok, WriterPid} ->
                                    {ok, CacheHandle, WriterPid};
                                {error, _} = Err ->
                                    _ = bondy_db_core_registry:unregister(
                                        NS, Name, Shard
                                    ),
                                    ok = release_cache(
                                        Topology, TableState, CacheHandle
                                    ),
                                    Err
                            end;
                        {error, _} = Err ->
                            ok = release_cache(
                                Topology, TableState, CacheHandle
                            ),
                            Err
                    end;
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

%% @private
start_index_writer(NS, Name, Shard, CoalesceMs) ->
    Args0 = #{ns => NS, index_name => Name, shard => Shard},
    Args =
        case CoalesceMs of
            undefined -> Args0;
            _ -> Args0#{coalesce_ms => CoalesceMs}
        end,
    bondy_oplog_secondary_sup:start_writer(Args).

%% @private
teardown_indexes(NS, IndexMap) ->
    maps:foreach(
        fun(Name, Provision) ->
            #{
                sec_shard_count := SecShardCount,
                topology := Topology,
                table_state := TableState,
                cache_handles := Caches,
                writer_pids := Writers
            } = Provision,
            lists:foreach(
                fun(Shard) ->
                    teardown_index_shard(
                        NS, Name, Shard, Caches, Writers, Topology, TableState
                    )
                end,
                lists:seq(0, SecShardCount - 1)
            ),
            _ = Topology:close_table(TableState, undefined),
            ok
        end,
        IndexMap
    ).

%% @private
teardown_index_shard(
    NS, Name, Shard, CacheHandles, Writers, Topology, TableState
) ->
    teardown_shard_common(
        NS,
        Name,
        Shard,
        Writers,
        fun bondy_oplog_secondary_sup:stop_writer/1,
        CacheHandles,
        Topology,
        TableState
    ).

%% @private
%% Build the static secondary-index descriptors handed to each primary
%% applier (term-diff + dispatch). `sec_shard_count` defaults to the
%% primary's shard count, matching `provision_index/4`.
index_descriptors(Specs, DefaultShardCount) ->
    [
        #{
            index_name => bondy_oplog_index_spec:name(Spec),
            spec => Spec,
            sec_shard_count => maps:get(
                sec_shard_count, Spec, DefaultShardCount
            ),
            %% IDX-4 back-pressure cap, read by the primary applier at
            %% dispatch to decide whether to drop a saturating batch.
            max_inflight => bondy_oplog_index_spec:max_inflight(Spec)
        }
     || Spec <- Specs
    ].

%% @private
%% Startup backfill (IDX-4): rebuild every declared index from the primary
%% once, after the writers are up. Best-effort — a failure leaves the
%% index marked for rebuild (reads refuse), recoverable by a later trigger
%% — so it never fails `open_table`.
backfill_indexes(NS, IndexMap) ->
    maps:foreach(
        fun(Name, _Provision) ->
            _ = bondy_oplog_index_rebuild:rebuild_sync(NS, Name)
        end,
        IndexMap
    ).

%% =============================================================================
%% PRIVATE — secondary index reads
%% =============================================================================

%% @private
%% Resolve an index by name and hand its spec + secondary shard count to
%% `Fun`. `{error, {unknown_index, _}}` when the table has no such index.
with_index(Table, IndexName, Fun) ->
    Indexes = maps:get(indexes, Table, #{}),
    case maps:find(IndexName, Indexes) of
        {ok, #{spec := Spec, sec_shard_count := SecShardCount}} ->
            Fun(Spec, SecShardCount);
        error ->
            {error, {unknown_index, IndexName}}
    end.

%% @private
index_bucket(
    #{db_topology := Topology, table_state := TableState, entity_type := ET},
    Realm,
    IndexName
) ->
    PrimaryBucket = Topology:bucket_for(ET, Realm, TableState),
    bondy_oplog_index_key:bucket(PrimaryBucket, IndexName).

%% @private
%% The `{max_lag, Ms}` gate, scoped to exactly the secondary shards the
%% read touches. An un-freshened (never-written) secondary shard reads as
%% maximally stale (the registry inits its `ae_atomics` to a sentinel), so
%% any finite bound refuses until the relevant writer has flushed (bumping
%% that shard's freshness).
%%
%% Granularity matches the read shape because the index is **term-sharded**
%% — a single write freshens only the one shard its term hashes to:
%%   - equality (`index_get`) touches one shard, so it checks one shard;
%%   - range (`index_range`) scatters, so it checks every shard.
%%
%% Deviation from `MST_DB_DESIGN.md` §13 / the IDX-2 sketch, which reused
%% the namespace-wide `bondy_db_core:ensure_fresh([NS], Ms)`. That conflates
%% the index's freshness with the *primary* shards' (and every sibling
%% index's): the primary applier never bumps its own freshness here, so a
%% namespace-wide finite `max_lag` would refuse forever even after the index
%% caught up — and a per-shard term-sharded write could never satisfy an
%% all-shards check. The freshness signal a reader actually wants is "are
%% the shard(s) I am about to read current".
%%
%% IDX-4 additions: the gate also returns the worst observed lag (so the
%% caller — and the `{stale_secondary, IndexName, Lag}` error — carries a
%% diagnostic), and a shard whose `needs_rebuild` flag is set (saturation
%% drop / writer crash) is unconditionally stale (`Lag = infinity`) until a
%% rebuild clears it, regardless of its AE timestamp.
ensure_shard_fresh(_NS, _IndexName, _Shard, infinity) ->
    ok;
ensure_shard_fresh(NS, IndexName, Shard, MaxLag) ->
    case shard_lag(NS, IndexName, Shard) of
        Lag when Lag =< MaxLag -> ok;
        Lag -> {stale, Lag}
    end.

%% @private
ensure_index_fresh(_NS, _IndexName, _SecShardCount, infinity) ->
    ok;
ensure_index_fresh(NS, IndexName, SecShardCount, MaxLag) ->
    WorstLag = lists:foldl(
        fun(Shard, Acc) -> max_lag(Acc, shard_lag(NS, IndexName, Shard)) end,
        0,
        lists:seq(0, SecShardCount - 1)
    ),
    case WorstLag =< MaxLag of
        true -> ok;
        false -> {stale, WorstLag}
    end.

%% @private
%% Per-shard lag: `infinity` for an unknown shard, a shard flagged
%% `needs_rebuild`, or a never-freshened shard; otherwise the wall-clock ms
%% since its last AE bump.
shard_lag(NS, IndexName, Shard) ->
    case bondy_db_core_registry:lookup(NS, IndexName, Shard) of
        not_found ->
            infinity;
        {ok, Entry} ->
            case bondy_db_core_registry:index_needs_rebuild(Entry) of
                true ->
                    infinity;
                false ->
                    case bondy_db_core_registry:entry_ever_freshened(Entry) of
                        false ->
                            infinity;
                        true ->
                            Now = erlang:monotonic_time(millisecond),
                            erlang:max(
                                0,
                                Now -
                                    bondy_db_core_registry:entry_last_ae(Entry)
                            )
                    end
            end
    end.

%% @private
%% Max of two lag values where `infinity` dominates any integer.
max_lag(infinity, _) -> infinity;
max_lag(_, infinity) -> infinity;
max_lag(A, B) when is_integer(A), is_integer(B) -> erlang:max(A, B).

%% @private
%% Diagnostic snapshot of one secondary shard's lag, in-flight backlog,
%% and rebuild flag (for `index_lag/2`).
shard_lag_info(NS, IndexName, Shard) ->
    Lag = shard_lag(NS, IndexName, Shard),
    {Inflight, NeedsRebuild} =
        case bondy_db_core_registry:lookup(NS, IndexName, Shard) of
            {ok, Entry} ->
                {
                    bondy_db_core_registry:index_inflight(Entry),
                    bondy_db_core_registry:index_needs_rebuild(Entry)
                };
            not_found ->
                {0, false}
        end,
    #{lag => Lag, inflight => Inflight, needs_rebuild => NeedsRebuild}.

%% @private
%% Forward only the scan-shaping opts to the substrate; `max_lag`/`shard`
%% are facade-level and must not leak into the adapter opts.
index_range_opts(Opts) ->
    maps:with([limit, direction], Opts).

%% @private
%% A stale index read either refuses with the lag diagnostic, or — when
%% the caller passes `fallback => primary` — runs the supplied
%% primary-scan thunk ("slow but correct", `MST_DB_DESIGN.md` §13.1).
stale_or_fallback(Opts, IndexName, Lag, FallbackFun) ->
    case maps:get(fallback, Opts, refuse) of
        primary -> FallbackFun();
        refuse -> {error, {stale_secondary, IndexName, Lag}}
    end.

%% @private
%% Run a stale-index fallback scan: enumerate the realm's primary cells and
%% hand them to `RowsFun` (which recomputes terms/columns and produces the
%% sorted, limited `[{Key, ColumnsMap}]`). Propagates a scan error verbatim.
primary_scan(Table, Realm, RowsFun) ->
    case primary_cells(Table, Realm) of
        {ok, Cells} -> {ok, RowsFun(Cells)};
        {error, _} = Err -> Err
    end.

%% @private
%% The distinct, deduplicated index terms a value contributes.
cell_terms(Spec, Value) ->
    lists:usort(bondy_oplog_index_spec:terms(Spec, Value)).

%% @private
%% Equality fallback: enumerate the realm's primary cells, recompute each
%% value's index terms, and keep the keys whose terms include `NormTerm`.
%% Returns the same `{Key, ColumnsMap}` shape as `index_get/5`.
primary_scan_eq(Table, Realm, Spec, NormTerm, Opts) ->
    Limit = maps:get(limit, Opts, ?DEFAULT_RANGE_LIMIT),
    primary_scan(Table, Realm, fun(Cells) ->
        Rows = [
            {Key, recompute_columns(Spec, Value)}
         || {Key, Value, _Hlc} <- Cells,
            lists:member(NormTerm, cell_terms(Spec, Value))
        ],
        lists:sublist(lists:keysort(1, Rows), Limit)
    end).

%% @private
%% Range fallback: emit one `{Key, ColumnsMap}` per (matching term, key)
%% in `[Lo, Hi)`, globally ordered by `(term, key)` to match
%% `index_range/6`.
primary_scan_range(Table, Realm, Spec, Lo, Hi, Opts) ->
    Limit = maps:get(limit, Opts, ?DEFAULT_RANGE_LIMIT),
    primary_scan(Table, Realm, fun(Cells) ->
        Rows = [
            {Term, Key, recompute_columns(Spec, Value)}
         || {Key, Value, _Hlc} <- Cells,
            Term <- cell_terms(Spec, Value),
            Term >= Lo,
            Term < Hi
        ],
        Sorted = lists:sublist(lists:sort(Rows), Limit),
        [{K, C} || {_T, K, C} <- Sorted]
    end).

%% @private
recompute_columns(Spec, Value) ->
    bondy_oplog_index_spec:decode_projection(
        bondy_oplog_index_spec:project(Spec, Value)
    ).

%% @private
%% Enumerate every primary cell in `Realm` (materialised values), across
%% all primary shards, with overlay disabled. Uses an open-ended
%% (`infinity` high) scan — no finite binary exceeds every key. Bounded by
%% `?PRIMARY_SCAN_LIMIT`; a scan that fills it is logged as potentially
%% incomplete.
primary_cells(#{namespace := NS} = Table, Realm) ->
    PrimaryBucket = primary_bucket(Table, Realm),
    case
        bondy_db_core:range_all(
            NS,
            ?INDEX,
            PrimaryBucket,
            {<<>>, infinity},
            #{limit => ?PRIMARY_SCAN_LIMIT, include_overlay => false}
        )
    of
        {ok, Cells} ->
            case length(Cells) >= ?PRIMARY_SCAN_LIMIT of
                true ->
                    ?LOG_WARNING(#{
                        description =>
                            "bondy_db primary-scan fallback hit its cell "
                            "cap; the stale-index fallback result may be "
                            "incomplete.",
                        namespace => NS,
                        realm => Realm,
                        cap => ?PRIMARY_SCAN_LIMIT
                    });
                false ->
                    ok
            end,
            {ok, Cells};
        {error, _} = Err ->
            Err
    end.

%% @private
primary_bucket(
    #{db_topology := Topology, table_state := TableState, entity_type := ET},
    Realm
) ->
    Topology:bucket_for(ET, Realm, TableState).

%% @private
read_index(NS, IndexName, SecBucket, Low, High, RangeOpts) ->
    case
        bondy_db_core:range(NS, IndexName, SecBucket, {Low, High}, RangeOpts)
    of
        {ok, Rows} -> {ok, index_rows(Rows)};
        {error, _} = Err -> Err
    end.

%% @private
%% A range row is `{SecKey, Columns, _Hlc}` where `SecKey` is the
%% `(Term, PrimaryKey)` composite and `Columns` is the index entry's
%% `to_value/1` (the denormalised columns binary, `<<>>` for pointer-only).
%% Recover the primary key from the composite and decode the columns.
index_rows(Rows) ->
    [
        {
            bondy_oplog_index_key:decode_pk(SecKey),
            bondy_oplog_index_spec:decode_projection(Columns)
        }
     || {SecKey, Columns, _Hlc} <- Rows
    ].

%% @private
%% Shard derivation matches `bondy_db_core`: `phash2({Bucket, Key}, N)`.
%% That same composite is used to pick the instance_id so an `apply/4`
%% and the subsequent `read/3` for the same `(Bucket, Key)` always hit
%% the same shard's oplog instance and projection.
instance_id_for(#{instance_ids := Ids, shard_count := SC}, Bucket, Key) ->
    Shard = erlang:phash2({Bucket, Key}, SC),
    maps:get(Shard, Ids).

%% @private
encode_instance_id(DbName, EntityType, Shard) ->
    iolist_to_binary([
        atom_to_binary(DbName, utf8),
        $/,
        atom_to_binary(EntityType, utf8),
        $/,
        integer_to_binary(Shard)
    ]).

%% @private
await(InstanceId) ->
    case bondy_oplog:await_apply(InstanceId) of
        ok -> ok;
        {error, timeout} = Err -> Err
    end.

merge_opts(DbOpts, TableOpts) ->
    %% Per-table opts win over DB defaults. `topology` and
    %% `topology_opts` are DB-level only and intentionally dropped from
    %% the cascade — a per-table override of those would be incoherent.
    Cascadable = maps:without([topology, topology_opts], DbOpts),
    maps:merge(Cascadable, TableOpts).
