%% ===========================================================================
%%  bondy_mst_local_store.erl -
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


-module(bondy_mst_local_store).
-moduledoc """
MST backend using `maps`.
""".

-behaviour(bondy_mst_store).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").

-record(?MODULE, {
    root        ::  hash() | undefined,
    pages = #{} ::  map()
}).

-type t()       ::  #?MODULE{}.
-type page()    ::  bondy_mst_page:t().

-export_type([t/0]).
-export_type([page/0]).


-export([copy/3]).
-export([delete/1]).
-export([free/2]).
-export([gc/2]).
-export([get/2]).
-export([get_root/1]).
-export([has/2]).
-export([missing_set/2]).
-export([new/1]).
-export([page_refs/1]).
-export([put/2]).
-export([set_root/2]).



%% =============================================================================
%% BONDY_MST_STORE CALLBACKS
%% =============================================================================



-doc """
""".
-spec new(Opts :: map() | list()) -> t().

new(Opts) when is_list(Opts) ->
    new(maps:from_list(Opts));

new(Opts) when is_map(Opts) ->
    #?MODULE{}.


-doc """
""".
-spec get_root(T :: t()) -> Root :: hash() | undefined.

get_root(#?MODULE{root = Root}) ->
    Root.


-doc """
""".
-spec set_root(T :: t(), Hash :: hash()) -> t().

set_root(#?MODULE{} = T, Hash) ->
    T#?MODULE{root = Hash}.


-doc """
""".
-spec get(t(), page()) -> page() | undefined.

get(#?MODULE{pages = Pages}, Hash) ->
    maps:get(Hash, Pages, undefined).


-doc """
""".
-spec has(t(), page()) -> boolean().

has(#?MODULE{pages = Pages}, Hash) ->
    maps:is_key(Hash, Pages).


-doc """
""".
-spec put(t(), page()) -> {Hash :: binary(), t()}.

put(#?MODULE{pages = Pages} = T0, Page) ->
    Hash = bondy_mst_utils:hash(Page),
    T = T0#?MODULE{pages = maps:put(Hash, Page, Pages)},
    {Hash, set_root(T, Hash)}.


-doc """
""".
-spec copy(t(), OtherStore :: bondy_mst_store:t(), Hash :: binary()) -> t().

copy(#?MODULE{pages = Pages} = T0, OtherStore, Hash) ->
    case bondy_mst_store:get(OtherStore, Hash) of
        undefined ->
            T0;
        Page ->
            Refs = bondy_mst_store:page_refs(OtherStore, Page),
            T = lists:foldl(
                fun(X, Acc) -> copy(Acc, OtherStore, X) end,
                T0,
                Refs
            ),
            T#?MODULE{pages = maps:put(Hash, Page, Pages)}
    end.


-doc """
""".
-spec free(t(), Hash :: binary()) -> t().

free(#?MODULE{pages = Pages0} = T, Hash) ->
    Pages = maps:remove(Hash, Pages0),
    T#?MODULE{pages = Pages}.


-doc """
""".
-spec gc(t(), KeepRoots :: [list()]) -> t().

gc(#?MODULE{pages = Pages0} = T, KeepRoots) ->
    Pages =
        lists:foldl(
            fun(X, Acc) -> gc_aux(Acc, Pages0, X) end,
            #{},
            KeepRoots
        ),
    T#?MODULE{pages = Pages}.


-doc """
""".
-spec missing_set(t(), Root :: binary()) -> [Pages :: list()].

missing_set(#?MODULE{pages = Pages} = T, Root) ->
    case maps:get(Root, Pages, undefined) of
        undefined ->
            sets:from_list([Root], [{version, 2}]);

        Page ->
            lists:foldl(
                fun(X, Acc) -> sets:union(Acc, missing_set(T, X)) end,
                sets:new([{version, 2}]),
                page_refs(Page)
            )
    end.


-spec page_refs(Page :: page()) -> Refs :: [binary()].

page_refs(Page) ->
    bondy_mst_page:refs(Page).


-spec delete(t()) -> ok.

delete(#?MODULE{}) ->
    %% We cannot delete, this is an in-memory map, we do nothing.
    ok.



%% =============================================================================
%% PRIVATE
%% =============================================================================



gc_aux(Acc0, FromPages, Root)
when is_map(Acc0) andalso not is_map_key(Root, Acc0) ->
    case maps:get(Root, FromPages, undefined) of
        undefined ->
            Acc0;

        Page ->
            case maps:is_key(Root, Acc0) of
                true ->
                    Acc0;
                false ->
                    Acc = maps:put(Root, Page, Acc0),
                    lists:foldl(
                        fun(X, IAcc) -> gc_aux(IAcc, FromPages, X) end,
                        Acc,
                        page_refs(Page)
                    )
            end
    end;

gc_aux(Acc, _, _) ->
    Acc.
