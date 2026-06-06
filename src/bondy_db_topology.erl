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

`route/2` returns `{Adapter, Handle}` where `Adapter` is a module
implementing `bondy_oplog_projection_adapter` and `Handle` is whatever
that adapter expects from its own `open/4`. The facade does not call
the adapter's `open/4` directly — the topology has already done that
inside `open_table/4` and is handing back ready-to-use handles.

The handle is **per-shard**, not per-`(shard, realm)`. The substrate
(`bondy_db_core`) is keyed by `(Namespace, Index, Shard)` with no realm
dimension; the facade folds `Realm` into the cell key
(`<<Realm/binary, "/", UserKey/binary>>`) so a single per-shard handle
serves every realm.

## What the behaviour does NOT cover

- WAL, replication, applier, or overlay wiring — those are substrate
  concerns (`bondy_db_core`, `bondy_oplog_*`). The facade wires them
  directly. The one exception is the optional cache-hosting hook
  (`provision_cache/5` + `release_cache/2`): a topology whose per-shard
  resources must outlive the transient `open_table/3` caller (an
  ephemeral in-memory topology) implements it to host the read cache in
  a long-lived owner; topologies that omit it get the default
  caller-owned cache.
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
    bucket/0,
    shard/0
]).

-type state() :: term().
-type table_state() :: term().
-type entity_type() :: atom().
-type realm() :: binary().
-type bucket() :: term().
-type shard() :: non_neg_integer().

%% =============================================================================
%% CALLBACKS
%% =============================================================================

-doc """
Initialise the topology for a DB named `DbName`. Returns the topology's
process-wide state (often a supervisor pid plus bookkeeping). The
returned `State` is opaque to `bondy_db`.

`Opts` is the `topology_opts` map from the DB's `Opts`. Topology
implementations document their own required keys.
""".
-callback init(DbName :: atom(), Opts :: map()) ->
    {ok, state()} | {error, term()}.

-doc """
Provision the physical resources for `EntityType` with `ShardCount`
shards. Returns the per-table view `TableState` and the updated
process-wide `State`. The facade stashes `TableState` in the `Table`
handle and threads `State` back through the DB handle.

`Opts` is the table's effective opts (DB defaults cascaded with the
caller's per-table opts).
""".
-callback open_table(
    EntityType :: entity_type(),
    ShardCount :: pos_integer(),
    Opts :: map(),
    State :: state()
) -> {ok, table_state(), state()} | {error, term()}.

-doc """
Resolve `Shard` inside the table represented by `TableState`.
Returns the projection adapter module and the handle to call it with.

The handle spans every realm inside the shard — realm separation is
done above the topology, by the facade (`bondy_db`) folding `Realm`
into the cell key before invoking the adapter. Topologies therefore
do not see realms at all; their job is purely shard placement.

The handle is the same shape the adapter expects from its `open/4` —
the topology has already opened it at `open_table/4` time and is
handing back the ready handle.
""".
-callback route(
    Shard :: shard(),
    TableState :: table_state()
) -> {ok, Adapter :: module(), Handle :: term()} | {error, term()}.

-doc """
Compose the storage-layer **Bucket** for `(EntityType, Realm)` inside
the table represented by `TableState`. Bucket is the leveled/Riak-style
partition tag that travels with every projection-adapter call; the
topology owns the composition rule because it knows what its Bookie
layout disambiguates by Bucket vs by NS.

Examples:

- **per_entity** topology (one Bookie per `(EntityType, Shard)`, shared
  across realms): `bucket_for(_, Realm, _) -> Realm` — EntityType is
  already implicit in the Bookie, Bucket isolates realms inside it.
- **single_bookie** topology (one Bookie for everything):
  `bucket_for(EntityType, Realm, _) -> <<Realm, "/", EntityType>>` —
  Bucket has to disambiguate both EntityType and Realm.
""".
-callback bucket_for(
    EntityType :: entity_type(),
    Realm :: realm(),
    TableState :: table_state()
) -> bucket().

-doc """
Release the resources owned by `TableState`. Returns the updated
process-wide `State`.

A topology MAY skip releasing resources that are shared with other
tables (e.g., a single_bookie topology keeps its Bookie alive until
`shutdown/1` even after every `close_table/2` is invoked).
""".
-callback close_table(
    TableState :: table_state(),
    State :: state()
) -> {ok, state()}.

-doc """
Tear down the topology: stop every Bookie, release every resource,
unlink supervisors. Called from `bondy_db:close/1`.
""".
-callback shutdown(State :: state()) -> ok.

-doc """
**Optional.** Provision the per-shard read cache for `(NS, Index, Shard)`
and name the long-lived process that owns it.

A topology implements this when its per-shard substrate resources must
outlive the transient process that calls `bondy_db:open_table/3` — the
motivating case is an ephemeral in-memory topology whose ETS tables
must survive the caller so the node-global appliers keep writing them.
The returned `owner` is the process the facade attributes BOTH the
cache table AND the `bondy_db_core_registry` monitor to: when it dies,
the registration is torn down and (for an ETS cache) the table is
reclaimed by the VM.

Topologies that omit this callback get the default **long-lived caller**
contract — the facade creates a `bondy_oplog_cache_ets` table owned by,
and registers a registry monitor on, the calling process. A topology
that exports `provision_cache/5` MUST also export `release_cache/2`.

`Opts` mirrors the 4th argument of `bondy_oplog_cache_adapter:init/4`.
Returns `#{owner := pid(), adapter := module(), handle := term()}` —
the adapter/handle pair is registered verbatim and used on the read
path exactly as a caller-owned cache would be.
""".
-callback provision_cache(
    NS :: atom(),
    Index :: atom(),
    Shard :: shard(),
    Opts :: map(),
    TableState :: table_state()
) ->
    {ok, #{owner := pid(), adapter := module(), handle := term()}}
    | {error, term()}.

-doc """
**Optional.** Release a cache provisioned by `provision_cache/5`.

Runs the whole-table delete inside the owning process — for an ETS
cache the facade cannot do it itself (`ets:delete/1` is owner-only).
Paired with `provision_cache/5`; a topology that exports one MUST
export the other.
""".
-callback release_cache(
    Handle :: term(),
    TableState :: table_state()
) -> ok.

-optional_callbacks([provision_cache/5, release_cache/2]).
