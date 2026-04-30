%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_mst_config).
-behaviour(app_config).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").

-define(APP, bondy_mst).
-define(ERROR, '$error_badarg').
-define(FUN_WITH_ARITY(N), fun
    ({Mod, Fun}) when is_atom(Mod); is_atom(Fun) ->
        erlang:function_exported(Mod, Fun, N);
    (_) ->
        false
end).

-export([get/1]).
-export([get/2]).
-export([set/2]).
-export([init/0]).
-export([on_set/2]).
-export([will_set/2]).

-compile({no_auto_import, [get/1]}).

%% =============================================================================
%% API
%% =============================================================================

?DOC("Initialises bondy_mst configuration").
init() ->
    ok = app_config:init(?APP, #{callback_mod => ?MODULE}),
    ?LOG_NOTICE(#{
        description => "bondy_mst configuration initialised"
    }),
    ok.

-spec get(Key :: list() | atom() | tuple()) -> term().

get(Key) ->
    app_config:get(?APP, Key).

-spec get(Key :: list() | atom() | tuple(), Default :: term()) -> term().

get(Key, Default) ->
    app_config:get(?APP, Key, Default).

-spec set(Key :: key_value:key() | tuple(), Value :: term()) -> ok.

set(Key, Value) ->
    app_config:set(?APP, Key, Value).

-spec will_set(Key :: key_value:key(), Value :: any()) ->
    ok | {ok, NewValue :: any()} | {error, Reason :: any()}.

will_set(_, _) ->
    ok.

-spec on_set(Key :: key_value:key(), Value :: any()) -> ok.

on_set(_, _) ->
    ok.

%% =============================================================================
%% PRIVATE
%% =============================================================================
