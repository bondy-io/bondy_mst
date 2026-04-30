%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_validator_trust).
-behaviour(bondy_oplog_validator).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
No-op validator implementation for closed trusted clusters.

This is the default validator the library installs when an instance
is started without an explicit `validator` option. It carries no
state, never alters events, and never rejects them.
""").

-export([init/2]).
-export([sign_event/2]).
-export([verify_event/2]).
-export([detect_equivocation/2]).

%% =============================================================================
%% bondy_oplog_validator CALLBACKS
%% =============================================================================

init(_InstanceId, _Opts) ->
    {ok, undefined}.

sign_event(Event, State) ->
    {Event, State}.

verify_event(_Event, _State) ->
    ok.

detect_equivocation(_E1, _E2) ->
    ok.
