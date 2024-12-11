%% ===========================================================================
%%  bondy_mst_utils.erl -
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

-module(bondy_mst_utils).


-type hash()    ::  binary().

-export_type([hash/0]).

-export([behaviours/1]).
-export([ensure_loaded/1]).
-export([hash/1]).
-export([hash/2]).
-export([implementations/2]).
-export([implements_behaviour/2]).
-export([apply_lazy/3]).
-export([apply_lazy/5]).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec hash(Term :: term()) -> Digest :: binary().

hash(Term) ->
    hash(Term, sha256).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec hash(Term :: term(), Algo :: atom()) -> Digest :: binary().

hash(Term, Algo) ->
    crypto:hash(Algo, erlang:term_to_binary(Term, [{minor_version, 2}])).


%% -----------------------------------------------------------------------------
%% @doc Ensures a module is loaded.
%% @end
%% -----------------------------------------------------------------------------
ensure_loaded(Mod) ->
    erlang:function_exported(Mod, module_info, 0)
        orelse code:ensure_loaded(Mod),
    ok.


%% -----------------------------------------------------------------------------
%% @doc Lists the behaviours implemented by a module.
%% Raises an exception if the module is not loaded.
%% @end
%% -----------------------------------------------------------------------------
-spec behaviours(atom()) -> [atom()] | no_return().

behaviours(Module) ->
    ok = ensure_loaded(Module),
    Attributes = Module:module_info(attributes),
    lists:flatten(proplists:get_all_values(behaviour, Attributes)).


%% -----------------------------------------------------------------------------
%% @doc Returns `true' if module `module' implements behaviour `behaviour'.
%% Otherwise, it returns `false'.
%% @end
%% -----------------------------------------------------------------------------
-spec implements_behaviour(atom(), atom()) -> boolean().

implements_behaviour(Module, Behaviour) ->
    lists:member(Behaviour, behaviours(Module)).


%% -----------------------------------------------------------------------------
%% @doc Returns the list of modules implementing `Behaviour' in application
%% `Application'.
%% @end
%% -----------------------------------------------------------------------------
-spec implementations(atom(), atom()) -> [atom()].

implementations(Application, Behaviour) ->
    case application:get_key(Application, modules) of
        {ok, Mods} ->
            lists:filter(
                fun(Mod) -> implements_behaviour(Mod, Behaviour) end,
                Mods
            );

        _ ->
            []
    end.


-spec apply_lazy(module(), atom(), fun(() -> any())) -> any().

apply_lazy(Module, FunctionName, Fun) when is_function(Fun, 0) ->
    apply_lazy(Module, FunctionName, 0, [], Fun).


-spec apply_lazy(module(), atom(), integer(), list(), term()) -> term().

apply_lazy(Module, FunctionName, Arity, Args, Fun) when is_function(Fun, 0) ->
    ok = ensure_loaded(Module),

    case erlang:function_exported(Module, FunctionName, Arity) of
        true ->
            erlang:apply(Module, FunctionName, Args);

        false ->
            Fun()
    end.