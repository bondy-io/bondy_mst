%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_db_demo_cluster).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-moduledoc """
Boot-time wiring for the `bondy_db` 3-node REPL demo.

On `init/1` this gen_server, driven entirely by app env
(`config/nodeN/sys.config`):

1. Starts a `bondy_db_leveled_sup` linked to itself (it owns the
   leveled Bookies' lifetime).
2. Opens DB `demo` against the `shared_shards` topology with
   `shard_count` shards, pinning the per-instance WAL/MST/checkpoint
   under `<data_dir>/oplog` with `seed => true` and a deterministic
   per-node `origin` so a restart recovers its own WAL.
3. Opens one logical table per `{Name, FoldModule}` entry in the
   `tables` env and stashes each handle in `persistent_term` for the
   `bondy_db_demo` API module to read on the REPL hot-path.
4. Wires the sync scheduler with the built-in static peer source
   (the configured `nodes` list minus `self`) and a disterl dispatch
   (`bondy_db_demo:dispatch/2`) so writes on one node replicate to the
   others on every scheduler tick.

`terminate/2` closes the tables and the DB and clears the
`persistent_term` entries.
""".

%% =============================================================================
%% API
%% =============================================================================

-export([start_link/0]).

%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).

-define(SERVER, ?MODULE).
-define(PT(K), {bondy_db_demo, K}).

-record(state, {
    db :: map(),
    tables :: #{atom() := map()},
    peers :: [node()],
    leveled_sup :: pid()
}).

%% =============================================================================
%% API
%% =============================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% =============================================================================
%% gen_server CALLBACKS
%% =============================================================================

init([]) ->
    process_flag(trap_exit, true),
    DbName = env(db_name, demo),
    TableSpecs = env(tables, [
        {users, lww_register},
        {counters, pn_counter},
        {tags, g_set}
    ]),
    ShardCount = env(shard_count, 8),
    DataDir = node_data_dir(),
    AllNodes = env(nodes, [node()]),
    Peers = [N || N <- AllNodes, N =/= node()],

    LeveledDir = filename:join(DataDir, "leveled"),
    OplogPath = unicode:characters_to_binary(
        filename:join(DataDir, "oplog")
    ),
    ok = filelib:ensure_dir(filename:join(LeveledDir, ".keep")),

    %% The leveled supervisor is linked to *this* gen_server so the
    %% Bookies live exactly as long as the demo wiring does. Closing
    %% the DB in `terminate/2` brings them down cleanly.
    {ok, LeveledSup} = bondy_db_leveled_sup:start_link(),

    {ok, Db} = bondy_db:open(DbName, #{
        topology => bondy_db_topology_shared_shards,
        topology_opts => #{sup => LeveledSup, dir => LeveledDir},
        shard_count => ShardCount,
        %% DB-level default fold; each table overrides it below.
        fold_module => lww_register,
        oplog_instance_opts => #{
            storage_path => OplogPath,
            %% Each demo node is a seed: it starts `live` immediately
            %% rather than holding appends in `pre_bootstrap` waiting
            %% for a catalogue snapshot from a peer.
            seed => true,
            %% Deterministic per-node origin so a restart of the same
            %% node recovers its own WAL instead of being rejected as
            %% an orphan segment.
            origin => stable_origin()
        }
    }),

    Tables = lists:foldl(
        fun({Name, Fold}, Acc) ->
            {ok, T} = bondy_db:open_table(Db, Name, #{
                shard_count => ShardCount,
                fold_module => Fold
            }),
            ok = persistent_term:put(?PT({table, Name}), T),
            Acc#{Name => T}
        end,
        #{},
        TableSpecs
    ),
    TableNames = [N || {N, _Fold} <- TableSpecs],

    ok = persistent_term:put(?PT(db_name), DbName),
    ok = persistent_term:put(?PT(table_names), TableNames),
    ok = persistent_term:put(?PT(db), Db),
    ok = persistent_term:put(?PT(peers), Peers),

    %% Static peer source returns the configured peers unconditionally;
    %% the disterl transport tolerates a down peer (the sync session
    %% absorbs the error), so nodes may start in any order.
    ok = bondy_oplog_sync_scheduler:set_peer_source(
        bondy_oplog_peer_source_static, #{peers => Peers}
    ),
    ok = bondy_oplog_sync_scheduler:set_dispatch(
        fun bondy_db_demo:dispatch/2
    ),

    ?LOG_NOTICE(#{
        description => "bondy_db_demo cluster ready",
        node => node(),
        peers => Peers,
        db => DbName,
        tables => TableSpecs,
        shard_count => ShardCount,
        data_dir => DataDir
    }),

    {ok, #state{
        db = Db,
        tables = Tables,
        peers = Peers,
        leveled_sup = LeveledSup
    }}.

handle_call(_Req, _From, State) ->
    {reply, {error, badcall}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{db = Db, tables = Tables}) ->
    maps:foreach(
        fun(_Name, T) ->
            _ = catch bondy_db:close_table(T)
        end,
        Tables
    ),
    _ = catch bondy_db:close(Db),
    lists:foreach(
        fun(Key) -> _ = persistent_term:erase(?PT(Key)) end,
        [db, db_name, table_names, peers]
    ),
    lists:foreach(
        fun(Name) -> _ = persistent_term:erase(?PT({table, Name})) end,
        maps:keys(Tables)
    ),
    ok.

%% =============================================================================
%% PRIVATE
%% =============================================================================

env(Key, Default) ->
    application:get_env(bondy_db_demo, Key, Default).

%% Per-node data directory: `<data_dir>/<node>` so three nodes on the
%% same host never clobber each other's leveled / WAL files.
node_data_dir() ->
    Base = env(data_dir, "/tmp/bondy_db_demo"),
    filename:join(Base, atom_to_list(node())).

%% First 16 bytes of `sha256(node())`. Deterministic per node name so
%% a restart of the same node recovers its own WAL segments.
stable_origin() ->
    Hash = crypto:hash(sha256, atom_to_binary(node(), utf8)),
    <<Origin:16/binary, _/binary>> = Hash,
    Origin.
