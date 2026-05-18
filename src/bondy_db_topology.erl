%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_db_topology).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Behaviour for `bondy_db` **physical topologies**.

A topology maps logical addresses `(EntityType, Shard, Realm)` onto
physical `bondy_oplog_projection_adapter` handles. The facade
(`bondy_db`) is topology-agnostic: it tells the topology *what* it needs
(open this table with N shards, route this `(Shard, Realm)` lookup) and
the topology decides *how* to satisfy that — how many Bookies to run,
how to assign shards to them, how to map realms to buckets, where on
disk each Bookie lives.

The two reference implementations bundled with the test profile are:

- `bondy_db_topology_per_entity` — one Bookie per `(EntityType, Shard)`
  shared across realms; bucket = Realm. Suitable when sharding's goal is
  write-concurrency: each shard owns its own Bookie writer pipeline.
- `bondy_db_topology_single_bookie` — one Bookie for the whole DB;
  bucket = `(Realm, EntityType)` composite. Suitable for tests and tiny
  deployments where the per-Bookie write serialiser is not a bottleneck.

Other layouts (per-realm physical isolation, per-realm multi-table)
plug in by implementing this behaviour and supplying a different module
to `bondy_db:open/2` via `Opts#{topology => Mod}`.

## State separation

The behaviour distinguishes two pieces of state:

- `State` — the topology's process-wide bookkeeping, owned by the `Db`
  handle. Typically holds the supervisor pid that owns the Bookies and
  any cross-table state.
- `TableState` — per-table view derived from `State` at `open_table/4`
  time. Typically carries the per-shard `{Bookie, BookieOpts}` map that
  `route/3` resolves against.

`open_table/4` returns both the new global `State` and the per-table
`TableState`; the facade hands `TableState` to subsequent `route/3`
calls and `close_table/2` calls. This separation lets a topology pool
or share Bookies across tables (e.g., single_bookie reuses one Bookie
for every entity type) while still giving each table a stable handle.

## Adapter contract

`route/3` returns `{Adapter, Handle}` where `Adapter` is a module
implementing `bondy_oplog_projection_adapter` and `Handle` is whatever
that adapter expects from its own `open/4`. The facade does not call
the adapter's `open/4` directly — the topology has already done that
inside `open_table/4` and is handing back ready-to-use handles.

## What the behaviour does NOT cover

- WAL, replication, applier, overlay, or cache wiring — those are
  substrate concerns (`bondy_mst_db`, `bondy_oplog_*`). PR9's facade
  uses the projection adapter directly; substrate integration lands
  in a later PR.
- Realm lifecycle (creation, retirement, migration). Topology routes
  realms it is asked about; coordinating which realms exist is the
  caller's concern.
- Telemetry or metrics — left to the adapter.
""").

-export_type([
    state/0,
    table_state/0,
    entity_type/0,
    realm/0,
    shard/0
]).

-type state()       :: term().
-type table_state() :: term().
-type entity_type() :: atom().
-type realm()       :: binary().
-type shard()       :: non_neg_integer().

%% =============================================================================
%% CALLBACKS
%% =============================================================================

-doc("""
Initialise the topology for a DB named `DbName`. Returns the topology's
process-wide state (often a supervisor pid plus bookkeeping). The
returned `State` is opaque to `bondy_db`.

`Opts` is the `topology_opts` map from the DB's `Opts`. Topology
implementations document their own required keys.
""").
-callback init(DbName :: atom(), Opts :: map()) ->
    {ok, state()} | {error, term()}.


-doc("""
Provision the physical resources for `EntityType` with `ShardCount`
shards. Returns the per-table view `TableState` and the updated
process-wide `State`. The facade stashes `TableState` in the `Table`
handle and threads `State` back through the DB handle.

`Opts` is the table's effective opts (DB defaults cascaded with the
caller's per-table opts).
""").
-callback open_table(
    EntityType :: entity_type(),
    ShardCount :: pos_integer(),
    Opts :: map(),
    State :: state()
) -> {ok, table_state(), state()} | {error, term()}.


-doc("""
Resolve `(Shard, Realm)` inside the table represented by `TableState`.
Returns the projection adapter module and the handle to call it with.

The handle is the same shape the adapter expects from its `open/4` —
the topology has already opened it at `open_table/4` time and is
handing back the ready handle.
""").
-callback route(
    Shard :: shard(),
    Realm :: realm(),
    TableState :: table_state()
) -> {ok, Adapter :: module(), Handle :: term()} | {error, term()}.


-doc("""
Release the resources owned by `TableState`. Returns the updated
process-wide `State`.

A topology MAY skip releasing resources that are shared with other
tables (e.g., a single_bookie topology keeps its Bookie alive until
`shutdown/1` even after every `close_table/2` is invoked).
""").
-callback close_table(
    TableState :: table_state(),
    State :: state()
) -> {ok, state()}.


-doc("""
Tear down the topology: stop every Bookie, release every resource,
unlink supervisors. Called from `bondy_db:close/1`.
""").
-callback shutdown(State :: state()) -> ok.
