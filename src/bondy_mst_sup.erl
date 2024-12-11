%% ===========================================================================
%%  bondy_mst_sup.erl -
%%
%%  Copyright (c) 2023-2024 Leapsight. All rights reserved.
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
%% ===========================================================================


%%%-------------------------------------------------------------------
%% @doc bondy_mst top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(bondy_mst_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).




%% =============================================================================
%% API
%% =============================================================================



start_link() ->
    case supervisor:start_link({local, ?SERVER}, ?MODULE, []) of
        {ok, _} = OK ->
            Children = supervisor:which_children(?MODULE),
            case lists:keyfind(leveled, 1, Children) of
                {leveled, Pid, worker, _} ->
                    _ = persistent_term:put({bondy_mst, leveled}, Pid),
                    OK;
                false ->
                    OK
            end;
        Error ->
            Error
    end.




%% =============================================================================
%% SUPERVISOR CALLBACKS
%% =============================================================================




init([]) ->
    ok = bondy_mst_config:init(),
    SupFlags = #{
        strategy => one_for_all,
        intensity => 0,
        period => 1
    },
    ChildSpecs = maybe_append_leveled(maybe_append_rocksdb_manager([])),

    {ok, {SupFlags, ChildSpecs}}.


maybe_append_leveled(Acc) ->
    case bondy_mst_config:get([store, leveled], []) of
            [] ->
                Acc;
            Opts ->
                [
                    #{
                        id => leveled,
                        start => {leveled_bookie, book_start, [Opts]},
                        restart => permanent,
                        shutdown => 5_000,
                        type => worker,
                        modules => [leveled_bookie]
                    } | Acc
                ]
    end.

maybe_append_rocksdb_manager(Acc) ->
    case bondy_mst_config:get([store, rocksdb], []) of
        [] ->
            Acc;
        Opts ->
            [
                #{
                    id => bondy_mst_rocksdb_manager,
                    start => {
                        bondy_mst_rocksdb_manager,
                        start_link,
                        [Opts]
                    },
                    restart => permanent,
                    shutdown => 5000,
                    type => worker,
                    modules => [bondy_mst_rocksdb_manager]
                } | Acc
            ]
    end.
