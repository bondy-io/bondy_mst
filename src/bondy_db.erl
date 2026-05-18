%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_db).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Consumer-facing **cell-mechanics facade** backed by a pluggable
`bondy_db_topology` (`MST_DB_DESIGN.md` §18 — PR9).

`bondy_db` decouples the user-visible model (DB → tables → cells keyed
by `(Realm, Key)`) from the physical layout (which Bookie owns which
shard, which bucket holds which realm). Callers see one API; topology
modules decide everything below it.

## What this facade does

- HLC management (per-DB clock; callers ask for fresh HLCs via
  `tick/1`).
- Topology-aware routing (`(Realm, Key) → projection-adapter handle`).
- Cell frame encoding / decoding (`bondy_oplog_cell_frame`).
- Read-modify-write of the cell state through the fold's
  `apply_event/2` callback.

## What this facade does NOT do

- Define CRDT operations. The **fold module** owns the cell's state
  representation and the event shape. `bondy_db:apply/4` takes a
  fold-shaped event (`{set, H, V}` for LWW, `{add, H, Elem}` for
  ORSWOT, `{incr, H, N}` for counters, …) and hands it to the fold's
  `apply_event/2`. `bondy_db:read/3` returns the raw decoded state and
  the caller pattern-matches per their CRDT. The facade never inspects
  event or state shapes.
- WAL, overlay, applier, replication, cache. PR9 writes directly
  through the topology-routed projection adapter; substrate integration
  is a separate concern.

## Lifecycle

```erlang
{ok, Db} = bondy_db:open(my_db, #{
    topology      => bondy_db_topology_per_entity,
    topology_opts => #{sup => MySup, dir => <<"/var/lib/bondy_db">>},
    shard_count   => 8,
    fold_module   => bondy_oplog_fold_lww_register
}),

{ok, Users}  = bondy_db:open_table(Db, users,  #{}),
{ok, Tags}   = bondy_db:open_table(Db, tags,   #{
    fold_module => bondy_oplog_fold_orset
}),

%% Register operations (LWW):
H = bondy_db:tick(Users),
ok = bondy_db:apply(Users, <<"r1">>, <<"alice">>, {set, H, <<"value">>}),
{ok, {set, <<"value">>, H}, H} = bondy_db:read(Users, <<"r1">>, <<"alice">>),

%% Set operations (ORSWOT):
H2 = bondy_db:tick(Tags),
ok = bondy_db:apply(Tags, <<"r1">>, <<"alice">>, {add, H2, <<"erlang">>}),
{ok, OrsetState, _} = bondy_db:read(Tags, <<"r1">>, <<"alice">>),

ok = bondy_db:close_table(Users),
ok = bondy_db:close_table(Tags),
ok = bondy_db:close(Db).
```

## Read-modify-write

`apply/4` reads the cell's current state, folds the supplied event
onto it via `fold_module:apply_event/2`, and writes the result back.
This is the same pipeline used by the substrate's applier — the only
difference is that PR9 does not yet replicate the event or route it
through the WAL. Idempotency, HLC monotonicity, and conflict
resolution are inherited from the fold's contract.

## Option cascading (K1)

DB-level `Opts` cascade as table defaults; per-table `Opts` override.
`fold_module` may be set at the DB level (and inherited) or at the
table level (and overridden) — but every table MUST end up with a
`fold_module`; `open_table/3` rejects calls otherwise.

`shard_count` defaults to **8**. `topology` and `topology_opts` are
DB-level only and do not cascade (topology decisions are made once
per DB).

## Concurrency

`apply/4`, `read/3`, `range/5`, and `tick/1` are wait-free at the
facade level — all state lives in the `Db` / `Table` handles. The
topology decides whether per-shard or per-Bookie serialisation
applies underneath. For the read-modify-write in `apply/4`, two
concurrent writers targeting the same `(EntityType, Shard, Realm,
Key)` cell race in the projection adapter; the fold's HLC-monotonic
`apply_event/2` guarantees the later-HLC event wins regardless of
arrival order, so the race is consistent — just not serialised. A
follow-on PR will add per-shard ownership via the substrate's
applier to make this serialised.
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
-define(DEFAULT_FOLD,        bondy_oplog_fold_lww_register).

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
    shard_count    := pos_integer(),
    fold_module    := module(),
    table_state    := bondy_db_topology:table_state()
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
| `fold_module` | `bondy_oplog_fold_lww_register` | Default fold strategy |

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
Open a logical table for `EntityType` inside `Db`. The topology
provisions the physical resources (e.g., one Bookie per shard for the
per-entity topology) and returns a per-table state stashed in the
`Table` handle.

Per-table `Opts` override DB-level defaults. The merged `Opts` MUST
include `fold_module`. The chosen fold module determines:

- the cell state representation (`encode_state/1` / `decode_state/1`),
- the event shape accepted by `apply/4` (whatever
  `apply_event/2` accepts),
- the conflict-resolution rules used during `apply/4`'s
  read-modify-write.

The facade itself is fold-agnostic — choose a fold whose state and
event shapes match the CRDT semantics the table needs.
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
            case Topology:open_table(EntityType, ShardCount, Merged, State) of
                {ok, TableState, _NewState} ->
                    Table = #{
                        db_name     => maps:get(name, Db),
                        db_topology => Topology,
                        db_hlc      => maps:get(hlc, Db),
                        entity_type => EntityType,
                        shard_count => ShardCount,
                        fold_module => FoldModule,
                        table_state => TableState
                    },
                    {ok, Table};
                {error, _} = Err ->
                    Err
            end;
        error ->
            {error, {missing_required_opt, fold_module}}
    end.


-doc("""
Release the resources owned by `Table`. Whether physical resources are
actually freed is the topology's call — single_bookie keeps its
Bookie alive across `close_table/1` and only stops it on `close/1`.
""").
-spec close_table(Table :: table()) -> ok.

close_table(#{db_topology := Topology, table_state := TableState}) ->
    _ = Topology:close_table(TableState, undefined),
    ok.


-doc("""
Tear down `Db`: stop every Bookie, release every resource. Calls the
topology's `shutdown/1`.
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

The event shape is whatever the table's `fold_module:apply_event/2`
accepts:

- `bondy_oplog_fold_lww_register` — `{set, H, V} | {clear, H}`
- `bondy_oplog_fold_orset` — `{add, H, E} | {remove, H, E}`
- and so on for any user-supplied fold module.

The facade reads the cell's current state, folds the event onto it,
and writes the result back. Idempotency and conflict resolution are
inherited from the fold's contract — `apply_event/2` is responsible
for handling out-of-order events, ties, and resurrection semantics.

Returns `ok` on success or `{error, _}` on adapter failure. The fold
module is responsible for crashing on malformed events; the facade
does not validate event shapes.
""").
-spec apply(
    Table :: table(),
    Realm :: realm(),
    Key :: binary(),
    Event :: term()
) -> ok | {error, term()}.

apply(Table, Realm, Key, Event)
        when is_binary(Realm), is_binary(Key) ->
    case route(Table, Realm, Key) of
        {ok, Adapter, Handle} ->
            Fold = maps:get(fold_module, Table),
            OldState = read_state(Adapter, Handle, Key, Fold),
            NewState = Fold:apply_event(OldState, Event),
            write_state(Adapter, Handle, Key, Fold, NewState);
        {error, _} = Err ->
            Err
    end.


-doc("""
Read the decoded fold state for `(Realm, Key)` from `Table`.

Returns:

- `{ok, State, Hlc}` — the cell's current fold state and the HLC
  recorded in the cell frame. `State` shape is fold-specific; the
  caller pattern-matches per their CRDT.
- `not_found` — no cell exists for `(Realm, Key)`.
- `{error, _}` — adapter failure.

A cell whose state is the fold's `initial_value/0` (e.g., `undefined`
for LWW, an empty set for ORSWOT) is **NOT** filtered out at this
layer — the facade returns whatever the fold gives it. If a CRDT's
"empty" state should be invisible to callers, the convenience wrapper
above the facade applies that policy.
""").
-spec read(
    Table :: table(),
    Realm :: realm(),
    Key :: binary()
) -> {ok, State :: term(), Hlc :: bondy_oplog_hlc:hlc()}
   | not_found
   | {error, term()}.

read(Table, Realm, Key) when is_binary(Realm), is_binary(Key) ->
    case route(Table, Realm, Key) of
        {ok, Adapter, Handle} ->
            case Adapter:get(Handle, Key) of
                {ok, Frame} ->
                    Fold = maps:get(fold_module, Table),
                    {Hlc, Body} = bondy_oplog_cell_frame:decode(Frame),
                    {ok, Fold:decode_state(Body), Hlc};
                not_found ->
                    not_found
            end;
        {error, _} = Err ->
            Err
    end.


-doc("""
Single-shard range scan over `(Realm, [Low, High))`.

The shard is selected by `phash2(Low, ShardCount)` unless the caller
passes `Opts#{shard => N}`. Callers whose `[Low, High)` spans more than
one shard MUST scatter across shards themselves and merge the results;
the facade does not do scatter-merge in v1.

Returns `{ok, [{Key, State, Hlc}]}` — one row per cell present in the
range, in ascending key order (or descending with
`Opts#{direction => desc}`). `State` is the fold's decoded state,
exactly as returned by `read/3`.

`Opts` are passed through to the adapter's `range/4`; supported keys
include `limit` (default 1000) and `direction`.
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

range(Table, Realm, Low, High, Opts)
        when is_binary(Realm), is_binary(Low), is_binary(High),
             is_map(Opts) ->
    ShardCount = maps:get(shard_count, Table),
    Shard = maps:get(shard, Opts, erlang:phash2(Low, ShardCount)),
    TableState = maps:get(table_state, Table),
    Topology = maps:get(db_topology, Table),
    case Topology:route(Shard, Realm, TableState) of
        {ok, Adapter, Handle} ->
            AdapterOpts = maps:without([shard], Opts),
            case Adapter:range(Handle, Low, High, AdapterOpts) of
                {ok, Entries} ->
                    Fold = maps:get(fold_module, Table),
                    {ok, [decode_row(K, F, Fold) || {K, F} <- Entries]};
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.


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
       db_name := DbName, db_topology := Topology}) ->
    #{
        kind        => table,
        db_name     => DbName,
        topology    => Topology,
        entity_type => ET,
        shard_count => SC,
        fold_module => Fold
    }.


%% =============================================================================
%% PRIVATE
%% =============================================================================

route(#{db_topology := Topology, table_state := TableState,
        shard_count := ShardCount}, Realm, Key) ->
    Shard = erlang:phash2(Key, ShardCount),
    Topology:route(Shard, Realm, TableState).


merge_opts(DbOpts, TableOpts) ->
    %% Per-table opts win over DB defaults. `topology` and
    %% `topology_opts` are DB-level only and intentionally dropped from
    %% the cascade — a per-table override of those would be incoherent.
    Cascadable = maps:without([topology, topology_opts], DbOpts),
    maps:merge(Cascadable, TableOpts).


read_state(Adapter, Handle, Key, Fold) ->
    case Adapter:get(Handle, Key) of
        not_found ->
            Fold:initial_value();
        {ok, Frame} ->
            {_Hlc, Body} = bondy_oplog_cell_frame:decode(Frame),
            Fold:decode_state(Body)
    end.


write_state(Adapter, Handle, Key, Fold, State) ->
    Hlc = Fold:hlc(State),
    Body = Fold:encode_state(State),
    Frame = bondy_oplog_cell_frame:encode(Hlc, Body),
    Adapter:put_batch(Handle, [{Key, Frame}]).


decode_row(Key, Frame, Fold) ->
    {Hlc, Body} = bondy_oplog_cell_frame:decode(Frame),
    {Key, Fold:decode_state(Body), Hlc}.
