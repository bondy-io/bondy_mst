%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_db_topology_single_bookie).
-behaviour(bondy_db_topology).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Degenerate reference topology: one leveled Bookie for the whole DB
(`MST_DB_DESIGN.md` §18 — PR9).

```
DB
└── Bookie(single)
    ├── bucket=<<"R1/users">>  alice → frame
    ├── bucket=<<"R1/users">>  bob   → frame
    ├── bucket=<<"R1/tokens">> tok-1 → frame
    └── bucket=<<"R2/users">>  carol → frame
```

The Bookie is owned by the topology and shared across every table and
shard. Bucket disambiguation collapses `(Realm, EntityType)` into a
single binary so the keyspaces stay disjoint inside the one Bookie.

## When to use it

- **Tests** — one filesystem location, one process to clean up.
- **Tiny deployments** — entire DB fits inside one Bookie's
  write-serialised journal pipeline.
- **Bootstrap measurements** — establishes a single-Bookie baseline
  against which sharded topologies can be empirically compared.

NOT suitable when write concurrency matters: every write across every
shard, table, and realm serialises through the single Bookie's
gen_server.

## Required `topology_opts`

| Key | Type | Meaning |
|---|---|---|
| `sup` | `pid()` | The `bondy_db_leveled_sup` the Bookie is spawned under |
| `dir` | `binary() \\| string()` | Where leveled lays out its journal + ledger |

Optional:

| Key | Default | Meaning |
|---|---|---|
| `book_opts_fun` | `default_book_opts/1` | `fun((Dir) -> proplists:proplist())` builder for leveled's `book_start/1` opts |

## State + TableState

This topology starts the Bookie eagerly inside `init/2` so every table
shares it. `TableState` is `#{bookie := Pid, entity_type := atom()}`;
`route/3` builds the bucket from `(Realm, EntityType)` and returns a
projection-adapter handle pointing at the shared Bookie.

`close_table/2` is a no-op (the Bookie stays up until `shutdown/1`),
so opening and closing tables is cheap.

## Bucket format

```
<<Realm/binary, "/", EntityType/binary>>
```

`EntityType` is the atom's UTF-8 binary form. A `/` separator is used
because realms in Bondy are limited to `[A-Za-z0-9_\\-]` so the
separator cannot collide with realm content.
""").

-export([init/2]).
-export([open_table/4]).
-export([route/3]).
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
                {ok, Dir0} ->
                    Dir = normalise_dir(Dir0),
                    BookOptsFun = maps:get(book_opts_fun, Opts,
                                           fun default_book_opts/1),
                    case ensure_dir(Dir) of
                        ok ->
                            case bondy_db_leveled_sup:start_bookie(
                                    Sup, BookOptsFun(Dir)) of
                                {ok, Bookie} ->
                                    {ok, #{
                                        db_name => DbName,
                                        sup     => Sup,
                                        dir     => Dir,
                                        bookie  => Bookie
                                    }};
                                {error, _} = Err ->
                                    Err
                            end;
                        {error, _} = Err ->
                            Err
                    end;
                error ->
                    {error, {missing_required_opt, dir}}
            end;
        _ ->
            {error, {missing_required_opt, sup}}
    end.


open_table(EntityType, _ShardCount, _TableOpts,
           #{bookie := Bookie} = State)
        when is_atom(EntityType) ->
    %% Single_bookie ignores ShardCount at the physical level (there is
    %% only one Bookie); the facade still hashes keys into `shard_count`
    %% slots, but every shard for this topology routes to the same
    %% Bookie. The hash distribution remains useful as a uniform spread
    %% across the Bookie's internal hot keys.
    TableState = #{
        bookie      => Bookie,
        entity_type => EntityType
    },
    {ok, TableState, State}.


route(_Shard, Realm, #{bookie := Bookie, entity_type := EntityType})
        when is_binary(Realm) ->
    Bucket = <<Realm/binary, "/", (atom_to_binary(EntityType, utf8))/binary>>,
    Handle = #{bookie => Bookie, bucket => Bucket},
    {ok, ?PROJECTION_ADAPTER, Handle}.


close_table(_TableState, State) ->
    %% No-op: the Bookie is shared and outlives table close. Stopping
    %% the Bookie here would break every other table that has not yet
    %% been closed.
    {ok, State}.


shutdown(#{sup := Sup, bookie := Bookie}) ->
    %% Tell leveled to flush + close before bringing the supervisor
    %% down. `book_close` is a synchronous call; if the Bookie is
    %% already dead it raises, which we discard — `stop/1` will reap
    %% whatever supervisor children remain.
    _ = catch leveled_bookie:book_close(Bookie),
    bondy_db_leveled_sup:stop(Sup).


%% =============================================================================
%% PRIVATE
%% =============================================================================

ensure_dir(Dir) ->
    filelib:ensure_dir(filename:join(Dir, ".keep")).


normalise_dir(Dir) when is_binary(Dir) -> binary_to_list(Dir);
normalise_dir(Dir) when is_list(Dir)   -> Dir.


default_book_opts(Dir) ->
    [{root_path, Dir},
     {cache_size, 2000},
     {max_journalsize, 100_000_000},
     {sync_strategy, none}].
