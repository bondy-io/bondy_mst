%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_db_demo_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

%% =============================================================================
%% API
%% =============================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% =============================================================================
%% SUPERVISOR CALLBACKS
%% =============================================================================

init([]) ->
    SupFlags = #{
        strategy  => one_for_one,
        intensity => 5,
        period    => 10
    },
    ChildSpecs = [
        #{
            id       => bondy_db_demo_cluster,
            start    => {bondy_db_demo_cluster, start_link, []},
            restart  => permanent,
            shutdown => 30_000,
            type     => worker,
            modules  => [bondy_db_demo_cluster]
        }
    ],
    {ok, {SupFlags, ChildSpecs}}.
