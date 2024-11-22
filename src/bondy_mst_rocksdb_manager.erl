%% =============================================================================
%%  plum_db_partition_manager.erl -
%%
%%  Copyright (c) 2016-2021 Leapsight. All rights reserved.
%%
%%  Licensed under the Apache License, Version 2.0 (the "License");
%%  you may not use this file except in compliance with the License.
%%  You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%%  Unless required by applicable law or agreed to in writing, software
%%  distributed under the License is distributed on an "AS IS" BASIS,
%%  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%  See the License for the specific language governing permissions and
%%  limitations under the License.
%% =============================================================================

%% -----------------------------------------------------------------------------
%% @doc 
%%
%% @end
%% -----------------------------------------------------------------------------
-module(bondy_mst_rocksdb_manager).
-behaviour(gen_server).
-include_lib("kernel/include/logger.hrl").

-record(state, {
    block_cache     :: rocksdb:cache_handle() | undefined,
    stats           :: rocksdb:statistics_handle() | undefined,
    write_buffer    :: rocksdb:write_buffer_manager() | undefined
}).

-type state()               ::  #state{}.


-export([start_link/1]).
-export([stop/0]).
-export([stats/1]).

%% gen_server callbacks
-export([init/1]).
-export([handle_continue/2]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Start plumtree_partitions_coordinator and link to calling process.
%% @end
%% -----------------------------------------------------------------------------
-spec start_link(list()) -> {ok, pid()} | ignore | {error, term()}.

start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Opts], []).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec stop() -> ok.

stop() ->
    gen_server:stop(?MODULE).


stats(Arg) ->
    Res = plum_db_partition_server:stats(Arg),
    resulto:map(Res, fun(Bin) -> parse(Bin) end).



%% =============================================================================
%% GEN_SERVER_ CALLBACKS
%% =============================================================================



%% @private
-spec init([]) ->
    {ok, state()}
    | {ok, state(), non_neg_integer() | infinity}
    | ignore
    | {stop, term()}.

init(Opts) ->
    State = #state{},
    {ok, State, {continue, {init_config, Opts}}}.


handle_continue({init_config, Opts0}, #state{} = State0) ->
    ?LOG_INFO("Initialising RocksDB Config"),

    N = key_value:get(partitions, Opts0, 1),

    %% Create a shared cache
    CacheSize = key_value:get(
        [block_based_table_options, block_cache_size],
        Opts0,
        memory:gibibytes(1)
    ),
    {ok, BlockCache} = rocksdb:new_cache(lru, CacheSize),


    WriteBufferSize = memory:mebibytes(10 * N),


    %% Create a shared buffer for partition hashtree instances
    %% This value is harcoded
    {ok, WriteBuffer} = rocksdb:new_write_buffer_manager(
        WriteBufferSize,
        BlockCache
    ),

    {ok, Stats} = rocksdb:new_statistics(),

    State = State0#state{
        block_cache = BlockCache,
        write_buffer = WriteBuffer,
        stats = Stats
    },

    Opts1 = key_value:put(create_if_missing, true, Opts0),
    Opts = key_value:put(create_missing_column_families, true, Opts1),


    OpenOpts =
        lists:foldl(
            fun ({K, V}, Acc) -> key_value:put(K, V, Acc) end,
            Opts,
            [
                {[block_based_table_options, block_cache], BlockCache},
                {write_buffer_manager, WriteBuffer},
                {max_write_buffer_number, 4},
                {statistics, State#state.stats}
            ]
        ),

    {noreply, State, {continue, {open, Opts}}};

handle_continue({open, Opts0}, State) ->
    DataDir = key_value:get(root_path, Opts0, "/tmp/bondy_mst/rocksdb"),
    Opts = key_value:remove(root_path, Opts0),
    {ok, Ref, _} = rocksdb:open_optimistic_transaction_db(DataDir, Opts),
    _ = persistent_term:put({bondy_mst, rocksdb}, Ref),
    {noreply, State};

handle_continue(_, State) ->
    {noreply, State}.


%% @private
-spec handle_call(term(), {pid(), term()}, state()) ->
    {reply, term(), state()}
    | {reply, term(), state(), non_neg_integer()}
    | {reply, term(), state(), {continue, term()}}
    | {noreply, state()}
    | {noreply, state(), non_neg_integer()}
    | {noreply, state(), {continue, term()}}
    | {stop, term(), term(), state()}
    | {stop, term(), state()}.

handle_call(_Message, _From, State) ->
    {reply, {error, unsupported_call}, State}.


%% @private
-spec handle_cast(term(), state()) ->
    {noreply, state()}
    | {noreply, state(), non_neg_integer()}
    | {noreply, state(), {continue, term()}}
    | {stop, term(), state()}.

handle_cast(_Msg, State) ->
    {noreply, State}.


%% @private
-spec handle_info(term(), state()) ->
    {noreply, state()}
    | {noreply, state(), non_neg_integer()}
    | {noreply, state(), {continue, term()}}
    | {stop, term(), state()}.

handle_info(Event, State) ->
    ?LOG_INFO(#{
        reason => unsupported_event,
        event => Event
    }),
    {noreply, State}.


%% @private
-spec terminate(term(), state()) -> term().

terminate(_Reason, State) ->
    rocksdb:release_cache(State#state.block_cache),
    rocksdb:release_write_buffer_manager(State#state.write_buffer),
    rocksdb:release_statistics(State#state.stats),

    ok.


%% @private
-spec code_change(term() | {down, term()}, state(), term()) -> {ok, state()}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% =============================================================================
%% PRIVATE
%% =============================================================================

%% TODO
parse(Bin) ->
  Bin.