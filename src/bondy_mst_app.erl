%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_mst_app).

-behaviour(application).

-export([start/2, stop/1]).

%% =============================================================================
%% API
%% =============================================================================

start(_StartType, _StartArgs) ->
    bondy_mst_sup:start_link().

stop(_State) ->
    ok.
