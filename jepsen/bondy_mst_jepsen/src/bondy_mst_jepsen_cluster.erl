%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_mst_jepsen_cluster).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

%% =============================================================================
%% Public API
%% =============================================================================

-export([start_link/0]).
-export([db_name/0]).
-export([tables/0]).
-export([table/1]).
-export([peers/0]).
-export([hlc/0]).
-export([info/0]).

%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).

-define(SERVER, ?MODULE).
-define(PT_KEY(K), {?MODULE, K}).

-record(state, {
    db          :: map(),
    tables      :: #{atom() := map()},
    peers       :: [node()],
    leveled_sup :: pid()
}).

%% =============================================================================
%% API
%% =============================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec db_name() -> atom().
db_name() ->
    persistent_term:get(?PT_KEY(db_name)).

-spec tables() -> [atom()].
tables() ->
    persistent_term:get(?PT_KEY(table_names)).

-spec table(atom()) -> {ok, map()} | error.
table(Name) when is_atom(Name) ->
    case persistent_term:get(?PT_KEY({table, Name}), undefined) of
        undefined -> error;
        T        -> {ok, T}
    end.

-spec peers() -> [node()].
peers() ->
    persistent_term:get(?PT_KEY(peers), []).

-spec hlc() -> bondy_oplog_hlc:hlc().
hlc() ->
    bondy_db:tick(any_table()).

-spec info() -> map().
info() ->
    #{
        db    => db_name(),
        peers => peers(),
        tables => tables(),
        node  => node()
    }.

%% =============================================================================
%% gen_server CALLBACKS
%% =============================================================================

init([]) ->
    process_flag(trap_exit, true),
    DbName = env(db_name, jepsen),
    TableNames = env(tables, [t0, t1, t2, t3, t4, t5, t6, t7, t8, t9]),
    ShardCount = env(shard_count, 16),
    FoldModule = env(fold_module, lww_register),
    Peers = lists:filter(fun(N) -> N =/= node() end, env(peers, [])),
    DataDir = env(data_dir, "/var/lib/bondy_mst_jepsen"),
    ok = filelib:ensure_dir(filename:join(DataDir, ".keep")),
    %% Start the leveled supervisor as a child of *this* gen_server.
    %% A sibling-under-the-umbrella layout would deadlock because
    %% `supervisor:which_children/1` on the still-initializing parent
    %% blocks. Linking it here means the cluster gen_server is the
    %% sole owner of the leveled Bookies' lifetime — when this
    %% gen_server terminates, `terminate/2` calls `bondy_db:close/1`
    %% which calls `bondy_db_leveled_sup:stop/1` (which unlinks
    %% before killing the sup, so no spurious exit signal flows
    %% back here).
    {ok, LeveledSup} = bondy_db_leveled_sup:start_link(),
    %% `shared_shards` topology: the 16 leveled Bookies are shared
    %% across all 10 tables (16 Bookies per node, not 10×16=160). Each
    %% table sees the same set of shards; the bucket per Bookie
    %% disambiguates entity types.
    {ok, Db} = bondy_db:open(DbName, #{
        topology      => bondy_db_topology_shared_shards,
        topology_opts => #{sup => LeveledSup, dir => DataDir},
        shard_count   => ShardCount,
        fold_module   => FoldModule
    }),
    Tables = lists:foldl(
        fun(Name, Acc) ->
            {ok, T} = bondy_db:open_table(Db, Name, #{
                shard_count => ShardCount,
                fold_module => FoldModule
            }),
            ok = persistent_term:put(?PT_KEY({table, Name}), T),
            Acc#{Name => T}
        end,
        #{},
        TableNames
    ),
    ok = persistent_term:put(?PT_KEY(db_name), DbName),
    ok = persistent_term:put(?PT_KEY(table_names), TableNames),
    ok = persistent_term:put(?PT_KEY(db), Db),
    ok = persistent_term:put(?PT_KEY(peers), Peers),
    %% Wire the sync scheduler: static peer source + a dispatch that
    %% pulls from every connected peer via the disterl transport.
    ok = bondy_oplog_sync_scheduler:set_peer_source(
        bondy_mst_jepsen_peer_source, #{}
    ),
    ok = bondy_oplog_sync_scheduler:set_dispatch(
        fun bondy_mst_jepsen_dispatch:dispatch/2
    ),
    ?LOG_NOTICE(#{
        description => "bondy_mst_jepsen cluster ready",
        node => node(),
        peers => Peers,
        db => DbName,
        tables => TableNames,
        shard_count => ShardCount,
        fold_module => FoldModule
    }),
    {ok, #state{
        db          = Db,
        tables      = Tables,
        peers       = Peers,
        leveled_sup = LeveledSup
    }}.

handle_call(_Req, _From, State) ->
    {reply, {error, badcall}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{db = Db, tables = Tables}) ->
    %% Best-effort orderly shutdown: close every table, then the DB.
    maps:foreach(
        fun(_Name, T) ->
            _ = catch bondy_db:close_table(T)
        end,
        Tables
    ),
    _ = catch bondy_db:close(Db),
    lists:foreach(
        fun(Key) ->
            _ = persistent_term:erase(?PT_KEY(Key))
        end,
        [db, db_name, table_names, peers]
    ),
    lists:foreach(
        fun(Name) ->
            _ = persistent_term:erase(?PT_KEY({table, Name}))
        end,
        maps:keys(Tables)
    ),
    ok.

%% =============================================================================
%% PRIVATE
%% =============================================================================

env(Key, Default) ->
    application:get_env(bondy_mst_jepsen, Key, Default).

any_table() ->
    case tables() of
        [Name | _] ->
            {ok, T} = table(Name),
            T;
        [] ->
            error(no_tables_open)
    end.
