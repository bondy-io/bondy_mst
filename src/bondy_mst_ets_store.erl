%% ===========================================================================
%%  bondy_mst_ets_store.erl -
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

%% -----------------------------------------------------------------------------
%% @doc Read-concurrent, MST backend using `ets'.
%% @end
%% -----------------------------------------------------------------------------
-module(bondy_mst_ets_store).

-behaviour(bondy_mst_store).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").

-ifdef(TEST).
    -define(ETS_ACCESS, public).
-else.
    -define(ETS_ACCESS, protected).
-endif.

-record(?MODULE, {
    name                ::  binary(),
    tab                 ::  ets:tid(),
    hashing_algorithm   ::  atom(),
    opts                ::  opts_map()
}).

-type t()           ::  #?MODULE{}.
-type opt()         ::  {name, binary()}
                        | {persistent, boolean()}.
-type opts()        ::  [opt()] | opts_map().
-type opts_map()    ::  #{
                            name := binary(),
                            persistent => boolean()
                        }.
-type page()        ::  bondy_mst_page:t().

-export_type([t/0]).
-export_type([page/0]).


%% API
-export([close/1]).
-export([copy/3]).
-export([delete/1]).
-export([free/3]).
-export([gc/2]).
-export([get/2]).
-export([get_root/1]).
-export([has/2]).
-export([list/1]).
-export([missing_set/2]).
-export([open/2]).
-export([page_refs/1]).
-export([put/2]).
-export([set_root/2]).



%% =============================================================================
%% BONDY_MST_STORE CALLBACKS
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec open(Algo :: atom(), Opts :: opts()) -> t() | no_return().

open(Algo, Opts) when is_atom(Algo), is_list(Opts) ->
    open(Algo, maps:from_list(Opts));

open(Algo, Opts0) when is_atom(Algo), is_map(Opts0) ->
    DefaultOpts = #{
        name => undefined,
        persistent => true
    },

    Opts = maps:merge(DefaultOpts, Opts0),

    ok = maps:foreach(
        fun
            (name, V) ->
                is_binary(V)
                orelse error({badarg, [{name, V}]});

            (persistent, V) ->
                is_boolean(V)
                orelse error({badarg, [{persistent, V}]})
        end,
        Opts
    ),

    Tab = ets:new(undefined, [set, ?ETS_ACCESS]),

    #?MODULE{
        name = maps:get(name, Opts),
        tab = Tab,
        hashing_algorithm = Algo,
        opts = Opts
    }.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec close(t()) -> ok.

close(#?MODULE{}) ->
    ok.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get_root(T :: t()) -> Root :: hash() | undefined.

get_root(#?MODULE{tab = Tab}) ->
    do_get(Tab, ?ROOT_KEY).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec set_root(T :: t(), Hash :: hash()) -> t().

set_root(#?MODULE{tab = Tab} = T, Hash) ->
    true = ets:insert(Tab, {?ROOT_KEY, Hash}),
    T.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get(T :: t(), Hash :: binary()) -> Page :: page() | undefined.

get(#?MODULE{tab = Tab}, Hash) ->
    do_get(Tab, Hash).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec has(T :: t(), Hash :: binary()) -> boolean().

has(#?MODULE{tab = Tab}, Hash) ->
    ets:member(Tab, Hash).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec put(T :: t(), Page :: page()) -> {Root :: binary(), T :: t()}.

put(#?MODULE{tab = Tab, hashing_algorithm = Algo} = T, Page) ->
    Hash = bondy_mst_page:hash(Page, Algo),
    true = ets:insert(Tab, {Hash, Page}),
    {Hash, T}.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec copy(t(), OtherStore :: bondy_mst_store:t(), Hash :: binary()) -> t().

copy(#?MODULE{tab = Tab} = T, OtherStore, Hash) ->
    case bondy_mst_store:get(OtherStore, Hash) of
        undefined ->
            T;

        Page ->
            Refs = bondy_mst_store:page_refs(OtherStore, Page),
            T = lists:foldl(
                fun(Ref, Acc) -> copy(Acc, OtherStore, Ref) end,
                T,
                Refs
            ),
            true = ets:insert(Tab, {Hash, Page}),
            T
    end.



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec list(t()) -> [page()].

list(#?MODULE{tab = Tab}) ->
    MS = [{{'$1', '$2'}, [{'=/=', '$1', ?ROOT_KEY}], ['$2']}],
    ets:select(Tab, MS).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec free(T :: t(), Hash :: binary(), Page :: page()) -> T :: t().

free(#?MODULE{tab = Tab, opts = #{persistent := true}} = T, Hash, Page0) ->
    %% We keep the hash and page, marking it free.
    %% gc/2 will actually delete it.
    Page = bondy_mst_page:set_freed_at(Page0, erlang:monotonic_time()),
    true = ets:insert(Tab, Hash, Page),
    T;

free(#?MODULE{tab = Tab, opts = #{persistent := false}} = T, Hash, _Page) ->
    %% We immediately delete
    true = ets:delete(Tab, Hash),
    T.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec gc(T :: t(), KeepRoots :: [list()] | epoch()) -> T :: t().

gc(#?MODULE{tab = Tab, opts = #{persistent := true}} = T, Epoch)
when is_integer(Epoch) ->
    Idx = bondy_mst_page:field_index(freed_at),
    Var = list_to_atom("$" ++ integer_to_list(Idx)),
    VPattern = setelement(Idx, bondy_mst_page:pattern(), Var),
    MatchSpec = [
        {
            {'_', VPattern},
            [{'=<', Var, {const, Epoch}}],
            [true]
        }
    ],
    W0 = ets:info(Tab, memory),
    Num = ets:select_delete(Tab, MatchSpec),
    W1 = ets:info(Tab, memory),
    Mem = memory:format(memory:words(W0 - W1), decimal),

    ?LOG_INFO(
        "Garbage Collection completed: ~p pages freed, ~p reclaimed.",
        [Num, Mem]
    ),
    T;

gc(#?MODULE{opts = #{persistent := true}} = T, KeepRoots)
when is_list(KeepRoots) ->
    %% Review: not supported yet
    T;

gc(#?MODULE{opts = #{persistent := false}} = T, _) ->
    %% No garbage as free/3 deletes immediately
    T.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec missing_set(T :: t(), Root :: binary()) -> sets:set(page()).

missing_set(T, Root) ->
    case get(T, Root) of
        undefined ->
            sets:from_list([Root], [{version, 2}]);

        Page ->
            lists:foldl(
                fun(Hash, Acc) -> sets:union(Acc, missing_set(T, Hash)) end,
                sets:new([{version, 2}]),
                page_refs(Page)
            )
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec page_refs(Page :: page()) -> [binary()].

page_refs(Page) ->
    bondy_mst_page:refs(Page).


-spec delete(t()) -> ok.

delete(#?MODULE{tab = Tab}) ->
    ets:delete(Tab),
    ok.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
do_get(Tab, Hash) ->
    case ets:lookup_element(Tab, Hash, 2, undefined) of
        undefined ->
            undefined;

        [Value] ->
            %% bag and duplicate bag tables
            Value;

        Value ->
            %%  set and ordered_set tables
            Value
    end.




