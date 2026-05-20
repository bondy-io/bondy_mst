%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_db).

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
2. A per-shard ETS cache via `bondy_oplog_cache_ets`.
3. A registry entry in `bondy_db_core_registry` mapping
   `(Namespace, primary, Shard)` to the
   `{cache_adapter, cache_handle, projection_adapter, projection_handle,
   fold_module}` tuple.
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
`apply_event/2`, encodes the new state, and writes it back through the
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
    fold_module => orset
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
-export([read/3]).
-export([range/5]).
-export([info/1]).

-export_type([db/0, table/0, realm/0]).

-define(DEFAULT_SHARD_COUNT, 8).
-define(DEFAULT_FOLD,        lww_register).
-define(INDEX,               primary).

-type realm() :: binary().

-type db() :: #{
    name           := atom(),
    topology       := module(),
    topology_state := bondy_db_topology:state(),
    opts           := map(),
    hlc            := bondy_oplog_hlc:t()
}.

-type table() :: #{
    db_name        := atom(),
    db_topology    := module(),
    db_hlc         := bondy_oplog_hlc:t(),
    entity_type    := atom(),
    namespace      := atom(),
    shard_count    := pos_integer(),
    fold_module    := module() | atom(),
    table_state    := bondy_db_topology:table_state(),
    instance_ids   := #{non_neg_integer() := binary()},
    cache_handles  := #{non_neg_integer() := term()}
}.

%% =============================================================================
%% API
%% =============================================================================

-doc("""
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
""").
-spec open(Name :: atom(), Opts :: map()) -> {ok, db()} | {error, term()}.

open(Name, Opts) when is_atom(Name), is_map(Opts) ->
    case maps:find(topology, Opts) of
        {ok, Topology} when is_atom(Topology) ->
            TopologyOpts = maps:get(topology_opts, Opts, #{}),
            case Topology:init(Name, TopologyOpts) of
                {ok, State} ->
                    Db = #{
                        name           => Name,
                        topology       => Topology,
                        topology_state => State,
                        opts           => Opts,
                        hlc            => bondy_oplog_hlc:new()
                    },
                    {ok, Db};
                {error, _} = Err ->
                    Err
            end;
        error ->
            {error, {missing_required_opt, topology}}
    end.


-doc("""
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
""").
-spec open_table(
    Db :: db(),
    EntityType :: atom(),
    Opts :: map()
) -> {ok, table()} | {error, term()}.

open_table(#{topology := Topology, topology_state := State} = Db,
           EntityType, Opts)
        when is_atom(EntityType), is_map(Opts) ->
    Merged = merge_opts(maps:get(opts, Db), Opts),
    case maps:find(fold_module, Merged) of
        {ok, FoldModule} when is_atom(FoldModule) ->
            ShardCount = maps:get(shard_count, Merged, ?DEFAULT_SHARD_COUNT),
            DbName = maps:get(name, Db),
            NS = namespace_atom(DbName, EntityType),
            OplogOpts = maps:get(oplog_instance_opts, Merged, #{}),
            case Topology:open_table(EntityType, ShardCount, Merged, State) of
                {ok, TableState, _NewState} ->
                    case provision_shards(
                            NS, DbName, EntityType, ShardCount,
                            FoldModule, OplogOpts,
                            Topology, TableState) of
                        {ok, InstanceIds, CacheHandles} ->
                            {ok, #{
                                db_name       => DbName,
                                db_topology   => Topology,
                                db_hlc        => maps:get(hlc, Db),
                                entity_type   => EntityType,
                                namespace     => NS,
                                shard_count   => ShardCount,
                                fold_module   => FoldModule,
                                table_state   => TableState,
                                instance_ids  => InstanceIds,
                                cache_handles => CacheHandles
                            }};
                        {error, _} = Err ->
                            %% Topology's open_table already provisioned
                            %% adapter handles for this table — tear them
                            %% down so a failed provisioning does not leak
                            %% Bookies.
                            _ = Topology:close_table(TableState, State),
                            Err
                    end;
                {error, _} = Err ->
                    Err
            end;
        error ->
            {error, {missing_required_opt, fold_module}}
    end.


-doc("""
Release the resources owned by `Table`. Stops every per-shard oplog
instance, unregisters every shard from `bondy_db_core_registry`,
deletes every per-shard cache table, then asks the topology to release
its physical resources (Bookies, etc.).

Whether physical resources are actually freed is still the topology's
call — single_bookie keeps its Bookie alive across `close_table/1` and
only stops it on `close/1`.
""").
-spec close_table(Table :: table()) -> ok.

close_table(#{db_topology := Topology, table_state := TableState,
              namespace := NS, shard_count := ShardCount,
              instance_ids := InstanceIds,
              cache_handles := CacheHandles}) ->
    lists:foreach(
        fun(Shard) ->
            teardown_shard(NS, Shard, InstanceIds, CacheHandles)
        end,
        lists:seq(0, ShardCount - 1)
    ),
    _ = Topology:close_table(TableState, undefined),
    ok.


-doc("""
Tear down `Db`: stop every Bookie, release every resource. Calls the
topology's `shutdown/1`.

Callers SHOULD `close_table/1` each open table first. `close/1` does
not chase open tables — it only walks the topology.
""").
-spec close(Db :: db()) -> ok.

close(#{topology := Topology, topology_state := State}) ->
    Topology:shutdown(State).


-doc("""
Generate a fresh HLC from the DB's clock. Callers inject this HLC into
fold-specific events before calling `apply/4`.

Strictly greater than the previous value returned by `tick/1` on the
same DB.
""").
-spec tick(Table :: table()) -> bondy_oplog_hlc:hlc().

tick(#{db_hlc := Hlc}) ->
    bondy_oplog_hlc:now(Hlc).


-doc("""
Apply a fold-specific event to `(Realm, Key)` inside `Table`.

Builds `{cell_apply, Bucket, Key, FoldEvent}` (Bucket composed via
`Topology:bucket_for/3`) and appends it through the shard's oplog
instance. Once the WAL append returns, blocks on
`bondy_oplog:await_apply/1` so the projection write is visible to a
subsequent `read/3` from the same caller (read-your-writes).

The event shape is whatever the table's `fold_module:apply_event/2`
accepts. Idempotency and conflict resolution are inherited from the
fold's contract; the facade does not validate event shapes.

Returns `ok` on successful WAL durability + applier commit, or
`{error, _}` if the WAL refuses the append or the applier's drain
times out.
""").
-spec apply(
    Table :: table(),
    Realm :: realm(),
    Key :: binary(),
    Event :: term()
) -> ok | {error, term()}.

apply(#{db_topology := Topology, table_state := TableState,
        entity_type := EntityType} = Table,
      Realm, Key, Event)
        when is_binary(Realm), is_binary(Key) ->
    Bucket = Topology:bucket_for(EntityType, Realm, TableState),
    InstanceId = instance_id_for(Table, Bucket, Key),
    Op = {cell_apply, Bucket, Key, Event},
    try bondy_oplog:append(InstanceId, Op) of
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


-doc("""
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
""").
-spec read(
    Table :: table(),
    Realm :: realm(),
    Key :: binary()
) -> {ok, State :: term(), Hlc :: bondy_oplog_hlc:hlc()}
   | not_found
   | {error, term()}.

read(#{namespace := NS, db_topology := Topology,
       table_state := TableState, entity_type := EntityType},
     Realm, Key)
        when is_binary(Realm), is_binary(Key) ->
    Bucket = Topology:bucket_for(EntityType, Realm, TableState),
    case bondy_db_core:read(NS, ?INDEX, Bucket, Key) of
        {Value, Hlc} when Value =/= undefined ->
            {ok, Value, Hlc};
        undefined ->
            not_found;
        {error, _} = Err ->
            Err
    end.


-doc("""
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
""").
-spec range(
    Table :: table(),
    Realm :: realm(),
    Low :: binary(),
    High :: binary(),
    Opts :: map()
) -> {ok, [{Key :: binary(), State :: term(),
            Hlc :: bondy_oplog_hlc:hlc()}]}
   | {error, term()}.

range(#{namespace := NS, shard_count := ShardCount,
        db_topology := Topology, table_state := TableState,
        entity_type := EntityType},
      Realm, Low, High, Opts)
        when is_binary(Realm), is_binary(Low), is_binary(High),
             is_map(Opts) ->
    Bucket = Topology:bucket_for(EntityType, Realm, TableState),
    Shard = maps:get(shard, Opts, erlang:phash2({Bucket, Low}, ShardCount)),
    AdapterOpts = (maps:without([shard], Opts))#{shard => Shard},
    bondy_db_core:range(NS, ?INDEX, Bucket, {Low, High}, AdapterOpts).


-doc("""
Return an informational map about `Db` or `Table`. Intended for
operator introspection and tests; the shape is not stable across
versions.
""").
-spec info(db() | table()) -> map().

info(#{name := Name, topology := Topology, opts := Opts}) ->
    #{
        kind     => db,
        name     => Name,
        topology => Topology,
        opts     => Opts
    };
info(#{entity_type := ET, shard_count := SC, fold_module := Fold,
       db_name := DbName, db_topology := Topology,
       namespace := NS}) ->
    #{
        kind        => table,
        db_name     => DbName,
        topology    => Topology,
        entity_type => ET,
        namespace   => NS,
        shard_count => SC,
        fold_module => Fold
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
%% Provision every shard of a newly opened table. On any failure, roll
%% back partial provisioning so the caller does not inherit a half-built
%% table. `OplogOpts` is a map of extra options forwarded verbatim to
%% `bondy_oplog:start_instance/2` per shard — typically used to set
%% `backend` (e.g. `bondy_mst_pack_store`), `storage_path`, or
%% `fsync_mode`. Per-shard `fold_module`, `applier`, and `wal` opts
%% take precedence over keys with the same name in `OplogOpts`.
provision_shards(NS, DbName, EntityType, ShardCount, FoldModule, OplogOpts,
                 Topology, TableState) ->
    provision_shards(NS, DbName, EntityType, ShardCount, FoldModule, OplogOpts,
                     Topology, TableState, 0, #{}, #{}).

provision_shards(_NS, _DbName, _EntityType, ShardCount, _FoldModule, _OplogOpts,
                 _Topology, _TableState, ShardCount, Ids, Caches) ->
    {ok, Ids, Caches};
provision_shards(NS, DbName, EntityType, ShardCount, FoldModule, OplogOpts,
                 Topology, TableState, Shard, Ids, Caches) ->
    case provision_shard(NS, DbName, EntityType, ShardCount, FoldModule,
                         OplogOpts, Topology, TableState, Shard) of
        {ok, InstanceId, CacheHandle} ->
            provision_shards(NS, DbName, EntityType, ShardCount,
                             FoldModule, OplogOpts, Topology, TableState,
                             Shard + 1,
                             Ids#{Shard => InstanceId},
                             Caches#{Shard => CacheHandle});
        {error, _} = Err ->
            lists:foreach(
                fun(S) -> teardown_shard(NS, S, Ids, Caches) end,
                lists:seq(0, Shard - 1)
            ),
            Err
    end.


%% @private
provision_shard(NS, DbName, EntityType, ShardCount, FoldModule, OplogOpts,
                Topology, TableState, Shard) ->
    case Topology:route(Shard, TableState) of
        {ok, ProjAdapter, ProjHandle} ->
            case bondy_oplog_cache_ets:init(NS, ?INDEX, Shard, #{}) of
                {ok, CacheHandle} ->
                    Config = #{
                        shard_count        => ShardCount,
                        cache_adapter      => bondy_oplog_cache_ets,
                        cache_handle       => CacheHandle,
                        projection_adapter => ProjAdapter,
                        projection_handle  => ProjHandle,
                        fold_module        => FoldModule,
                        overlay            => disabled
                    },
                    case bondy_db_core_registry:register(
                            NS, ?INDEX, Shard, Config) of
                        ok ->
                            start_shard_instance(
                                NS, DbName, EntityType, Shard,
                                FoldModule, OplogOpts, CacheHandle);
                        {error, _} = Err ->
                            ok = bondy_oplog_cache_ets:close(CacheHandle),
                            Err
                    end;
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.


%% @private
%% `OplogOpts` is merged into the per-shard instance opts. The pinned
%% keys (`fold_module`, `applier`) override any caller-provided values
%% — those carry per-shard routing the caller cannot meaningfully
%% provide. Everything else (`backend`, `storage_path`, `fsync_mode`,
%% `max_install_in_flight`, etc.) is forwarded verbatim.
start_shard_instance(NS, DbName, EntityType, Shard, FoldModule, OplogOpts,
                     CacheHandle) ->
    InstanceId = encode_instance_id(DbName, EntityType, Shard),
    Pinned = #{
        fold_module => FoldModule,
        applier => #{
            cell_apply_target => {NS, ?INDEX, Shard}
        }
    },
    Opts = maps:merge(OplogOpts, Pinned),
    case bondy_oplog:start_instance(InstanceId, Opts) of
        {ok, _Sup} ->
            {ok, InstanceId, CacheHandle};
        {error, _} = Err ->
            ok = bondy_db_core_registry:unregister(NS, ?INDEX, Shard),
            ok = bondy_oplog_cache_ets:close(CacheHandle),
            Err
    end.


%% @private
teardown_shard(NS, Shard, InstanceIds, CacheHandles) ->
    case maps:get(Shard, InstanceIds, undefined) of
        undefined -> ok;
        InstanceId ->
            _ = bondy_oplog:stop_instance(InstanceId),
            ok
    end,
    _ = bondy_db_core_registry:unregister(NS, ?INDEX, Shard),
    case maps:get(Shard, CacheHandles, undefined) of
        undefined -> ok;
        CacheHandle ->
            _ = bondy_oplog_cache_ets:close(CacheHandle),
            ok
    end,
    ok.


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
        atom_to_binary(DbName, utf8), $/,
        atom_to_binary(EntityType, utf8), $/,
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
