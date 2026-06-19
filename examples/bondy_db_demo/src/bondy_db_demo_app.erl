%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_db_demo_app).
-behaviour(application).

-include_lib("kernel/include/logger.hrl").

-export([start/2, stop/1]).

%% =============================================================================
%% application callbacks
%% =============================================================================

start(_StartType, _StartArgs) ->
    ?LOG_NOTICE(#{
        description => "bondy_db_demo starting",
        node => node(),
        nodes => application:get_env(bondy_db_demo, nodes, [])
    }),
    bondy_db_demo_sup:start_link().

stop(_State) ->
    ok.
