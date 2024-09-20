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


-module(bondy_mst_ets_store).
-moduledoc """
ETS-based store backend implementation.
""".

-behaviour(bondy_mst_store).

-record(?MODULE, {
    tab         :: ets:tid()
}).

-type t()       ::  #?MODULE{}.
-type page()    ::  bondy_mst_page:t().

-export_type([t/0]).
-export_type([page/0]).


%% API
-export([new/1]).
-export([get/2]).
-export([has/2]).
-export([put/2]).
-export([copy/3]).
-export([missing_set/2]).
-export([page_refs/1]).
-export([free/2]).
-export([gc/2]).



%% =============================================================================
%% BONDY_MST_STORE CALLBACKS
%% =============================================================================


-doc """
""".
-spec new(Opts :: map() | list()) -> t().

new(Opts) when is_list(Opts) ->
    new(maps:from_list(Opts));


new(#{name := Name} = Opts) ->
    Tab = ets:new(Name, [ordered_set, named_table, public]),
    #?MODULE{tab = Tab}.


-doc """
""".
-spec get(Store :: t(), Hash :: binary()) -> Page :: page() | no_return().

get(#?MODULE{tab = Tab}, Hash) ->
    do_get(Tab, Hash).


-doc """
""".
-spec has(Store :: t(), Hash :: binary()) -> boolean().

has(#?MODULE{tab = Tab}, Hash) ->
    MS = [ {{Hash, '_'}, [], [true]} ],
    1 == ets:select_count(Tab, MS).


-doc """
""".
-spec put(Store :: t(), Page :: page()) -> {Hash :: binary(), Store :: t()}.

put(#?MODULE{tab = Tab} = Store, Page) ->
    Hash = bondy_mst_utils:hash(Page),
    true = ets:insert(Tab, {Hash, Page}),
    {Hash, Store}.


-doc """
""".
-spec missing_set(Store :: t(), Root :: binary()) -> sets:set(page()).

missing_set(Store, Root) ->
    try
        Page = get(Store, Root),
        Refs = page_refs(Page),
        Fun = fun(P, Acc) -> sets:union(Acc, missing_set(Store, P)) end,
        Acc0 = sets:new([{version, 2}]),
        lists:foldl(Fun, Acc0, Refs)
    catch
        error:badarg ->
            sets:from_list([Root], [{version, 2}])
    end.


-doc """
""".
-spec page_refs(Page :: page()) -> [binary()].

page_refs(Page) ->
    bondy_mst_page:refs(Page).


-doc """
""".
-spec copy(Store :: t(), OtherStore :: bondy_mst_store:t(), Hash :: binary()) ->
    Store :: t().

copy(#?MODULE{tab = Tab} = Store, OtherStore, Hash) ->
    %% Store is always the same
    try
        case bondy_mst_store:get(OtherStore, Hash) of
            nil ->
                Store;

            Page ->
                Refs = bondy_mst_store:page_refs(OtherStore, Page),
                Store = lists:foldl(
                    fun(Ref, Acc) -> copy(Acc, OtherStore, Ref) end,
                    Store,
                    Refs
                ),
                true = ets:insert(Tab, {Hash, Page}),
                Store
        end
    catch
        error:badarg ->
            Store
    end.


-doc """
""".
-spec free(Store :: t(), Hash :: binary()) -> Store :: t().

free(Store, _Hash) ->
    %% Do nothing
    Store.


-doc """
""".
-spec gc(Store :: t(), KeepRoots :: [list()]) -> Store :: t().

gc(#?MODULE{tab = Tab} = Store, KeepRoots) ->
    Pages = lists:foldl(
        fun(X, Acc) -> gc_aux(Acc, Tab, X) end,
        #{},
        KeepRoots
    ),
    Store.




%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
do_get(Tab, Hash) ->
    ets:lookup_element(Tab, Hash, 1).


%% @private
gc_aux(Acc0, Tab, Root) when not is_map_key(Root, Acc0) ->
    try
        Page = do_get(Tab, Root),
        Acc1 = maps:put(Root, Page, Acc0),
        Refs = page_refs(Page),

        lists:foldl(
            fun(Ref, IAcc) -> gc_aux(IAcc, Tab, Ref) end,
            Acc1,
            Refs
        )

    catch
        error:badarg ->
            Acc0
    end;

gc_aux(Acc, _, _) ->
    Acc.



