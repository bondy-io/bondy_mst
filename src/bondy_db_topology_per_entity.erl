%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_db_topology_per_entity).
-behaviour(bondy_db_topology).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
**T2** reference topology for `bondy_db`: one leveled Bookie per
`(EntityType, Shard)` shared across realms; bucket = EntityType binary.

```
DB
├── users
│   ├── Bookie(users, 0)
│   ├── Bookie(users, 1)
│   └── ...
└── tokens
    ├── Bookie(tokens, 0)
    └── ...
```

Inside each Bookie a single bucket holds every cell for the
`(EntityType, Shard)`; realm is encoded into the key by the facade
as `<<Realm/binary, "/", UserKey/binary>>` so realms are logically
separated inside the bucket without requiring multiple physical
buckets:

```
Bookie(users, 0)
└── bucket=<<"users">>
    ├── <<"realm-1/alice">>   → frame
    ├── <<"realm-1/bob">>     → frame
    ├── <<"realm-2/carol">>   → frame
    └── <<"realm-2/dave">>    → frame
```

## When to use it

This is the "sharding for write concurrency" layout. Each shard owns
its own Bookie writer pipeline; concurrent puts targeting different
shards run in parallel. Realms inside the same shard are
write-serialised because they share a Bookie, but Bondy realms are
expected to be many-and-small — distributing them across shards by key
gives even concurrency without per-realm Bookies.

For per-realm physical isolation (one Bookie per Realm), use a
different topology — that layout shows up later in the design as T1.

## Required `topology_opts`

| Key | Type | Meaning |
|---|---|---|
| `sup` | `pid()` | The `bondy_db_leveled_sup` supervisor pid the topology spawns Bookies under |
| `dir` | `binary() \\| string()` | Root directory; each Bookie lives at `<Dir>/<entity>/<shard>` |

Optional:

| Key | Default | Meaning |
|---|---|---|
| `book_opts_fun` | `default_book_opts/1` | `fun((Dir) -> proplists:proplist())` builder for leveled's `book_start/1` opts; called once per Bookie |

## TableState shape

```erlang
#{
    entity_type := atom(),
    shard_count := pos_integer(),
    bucket      := binary(),
    shards      := #{Shard :: non_neg_integer() := pid()}
}
```

## Bucket

Bucket is the UTF-8 binary form of the entity type atom. All cells for
the `(EntityType, Shard)` live inside this one bucket — realm
isolation happens above the topology, in the facade's cell-key
encoding.
""").

-export([init/2]).
-export([open_table/4]).
-export([route/2]).
-export([bucket_for/3]).
-export([close_table/2]).
-export([shutdown/1]).

-define(PROJECTION_ADAPTER, bondy_oplog_projection_leveled).

%% =============================================================================
%% bondy_db_topology callbacks
%% =============================================================================

init(DbName, Opts) when is_atom(DbName), is_map(Opts) ->
    case maps:find(sup, Opts) of
        {ok, Sup} when is_pid(Sup) ->
            case maps:find(dir, Opts) of
                {ok, Dir} ->
                    BookOpts = maps:get(book_opts_fun, Opts,
                                        fun default_book_opts/1),
                    State = #{
                        db_name       => DbName,
                        sup           => Sup,
                        dir           => normalise_dir(Dir),
                        book_opts_fun => BookOpts
                    },
                    {ok, State};
                error ->
                    {error, {missing_required_opt, dir}}
            end;
        _ ->
            {error, {missing_required_opt, sup}}
    end.


open_table(EntityType, ShardCount, _TableOpts, State)
        when is_atom(EntityType), is_integer(ShardCount), ShardCount > 0 ->
    case start_shards(EntityType, ShardCount, State) of
        {ok, Shards} ->
            TableState = #{
                entity_type => EntityType,
                shard_count => ShardCount,
                shards      => Shards
            },
            {ok, TableState, State};
        {error, _} = Err ->
            Err
    end.


route(Shard, #{shards := Shards}) when is_integer(Shard) ->
    case maps:find(Shard, Shards) of
        {ok, Bookie} ->
            %% Bookie-only handle: Bucket is per-call, supplied by the
            %% facade via `bucket_for/3` on every adapter invocation.
            Handle = #{bookie => Bookie},
            {ok, ?PROJECTION_ADAPTER, Handle};
        error ->
            {error, {unknown_shard, Shard}}
    end.


-doc("""
Per-entity topology disambiguates EntityType by the Bookie itself —
each `(EntityType, Shard)` has its own Bookie. The Bucket only needs
to isolate realms inside that Bookie, so it is just the Realm verbatim.
""").
bucket_for(_EntityType, Realm, _TableState) when is_binary(Realm) ->
    Realm.


close_table(#{shards := Shards}, State) ->
    %% T2 owns one Bookie per (EntityType, Shard); close_table stops
    %% them all. State is unchanged — this topology keeps no per-table
    %% bookkeeping at the DB level.
    lists:foreach(
        fun({_Shard, Bookie}) -> stop_bookie_safe(Bookie) end,
        maps:to_list(Shards)
    ),
    {ok, State}.


shutdown(#{sup := Sup}) ->
    bondy_db_leveled_sup:stop(Sup).


%% =============================================================================
%% PRIVATE
%% =============================================================================

start_shards(EntityType, ShardCount, State) ->
    start_shards(EntityType, 0, ShardCount, State, #{}).

start_shards(_EntityType, N, N, _State, Acc) ->
    {ok, Acc};
start_shards(EntityType, I, N, #{sup := Sup, dir := Dir,
                                 book_opts_fun := BookOptsFun} = State, Acc) ->
    ShardDir = shard_dir(Dir, EntityType, I),
    case ensure_dir(ShardDir) of
        ok ->
            BookOpts = BookOptsFun(ShardDir),
            case bondy_db_leveled_sup:start_bookie(Sup, BookOpts) of
                {ok, Bookie} ->
                    start_shards(EntityType, I + 1, N, State,
                                 Acc#{I => Bookie});
                {error, _} = Err ->
                    %% Best-effort: stop already-started shards so the
                    %% caller is not left with a partial table.
                    [stop_bookie_safe(B) || B <- maps:values(Acc)],
                    Err
            end;
        {error, _} = Err ->
            [stop_bookie_safe(B) || B <- maps:values(Acc)],
            Err
    end.


shard_dir(Dir, EntityType, Shard) ->
    filename:join([
        Dir,
        atom_to_list(EntityType),
        integer_to_list(Shard)
    ]).


ensure_dir(Dir) ->
    filelib:ensure_dir(filename:join(Dir, ".keep")).


normalise_dir(Dir) when is_binary(Dir) -> binary_to_list(Dir);
normalise_dir(Dir) when is_list(Dir)   -> Dir.


default_book_opts(Dir) ->
    %% Mirrors the parameters the adapter eunit + PropEr suites use; the
    %% defaults are tuned for fast tests, not for production. Production
    %% deployments should override `book_opts_fun` in `topology_opts`.
    [{root_path, Dir},
     {cache_size, 2000},
     {max_journalsize, 100_000_000},
     {sync_strategy, none}].


stop_bookie_safe(Bookie) when is_pid(Bookie) ->
    case is_process_alive(Bookie) of
        true ->
            %% Tell leveled to flush + close. The supervisor will reap
            %% the now-dead child without restarting it (`temporary`).
            _ = catch leveled_bookie:book_close(Bookie),
            ok;
        false ->
            ok
    end;
stop_bookie_safe(_) ->
    ok.
