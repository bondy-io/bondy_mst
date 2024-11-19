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


-module(bondy_mst_leveled_store).
-moduledoc """
Non-concurrent, MST backend using `ets`.
""".

-behaviour(bondy_mst_store).

-include_lib("leveled/include/leveled.hrl").

-record(?MODULE, {
    pid         :: pid(),
    name        :: atom() | binary()
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
-export([delete/1]).



%% =============================================================================
%% BONDY_MST_STORE CALLBACKS
%% =============================================================================



-doc """
""".
-spec new(Opts :: map() | list()) -> t().

new(Opts) when is_list(Opts) ->
    new(maps:from_list(Opts));

new(#{name := Name} = _Opts) ->
    Pid = persistent_term:get({bondy_mst, leveled}),
    #?MODULE{pid = Pid, name = Name}.


-doc """
""".
-spec get(T :: t(), Hash :: binary()) -> Page :: page() | undefined.

get(#?MODULE{pid = Pid, name = Name}, Hash) ->
    do_get(Pid, Name, Hash).


-doc """
""".
-spec has(T :: t(), Hash :: binary()) -> boolean().

has(#?MODULE{pid = Pid, name = Name}, Hash) ->
    leveled_bookie:book_head(Pid, Name, Hash) /= not_found.


-doc """
""".
-spec put(T :: t(), Page :: page()) -> {Hash :: binary(), T :: t()}.

put(#?MODULE{pid = Pid, name = Name} = T, Page) ->
    Hash = bondy_mst_utils:hash(Page),
    ok = leveled_bookie:book_put(Pid, Name, Hash, Page, []),
    {Hash, T}.


-doc """
""".
-spec copy(t(), OtherStore :: bondy_mst_store:t(), Hash :: binary()) -> t().

copy(#?MODULE{pid = Pid, name = Name} = T, OtherStore, Hash) ->

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
            ok = leveled_bookie:book_put(Pid, Name, Hash, Page, []),
            T
    end.


-doc """
""".
-spec free(T :: t(), Hash :: binary()) -> T :: t().

free(#?MODULE{pid = Pid, name = Name} = T, Hash) ->
    ok = leveled_bookie:book_delete(Pid, Name, Hash, []),
    T.


-doc """
""".
-spec gc(T :: t(), KeepRoots :: [list()]) -> T :: t().

gc(#?MODULE{pid = Pid, name = Name} = T, KeepRoots) ->
    lists:foldl(
        fun(X, Acc) -> gc_aux(Acc, Pid, Name, X) end,
        ok,
        KeepRoots
    ),
    T.


-doc """
""".
-spec missing_set(T :: t(), Root :: binary()) -> sets:set(page()).

missing_set(T, Root) ->
    case get(T, Root) of
        undefined ->
            sets:from_list([Root], [{version, 2}]);

        Page ->
            lists:foldl(
                fun(P, Acc) -> sets:union(Acc, missing_set(T, P)) end,
                sets:new([{version, 2}]),
                page_refs(Page)
            )
    end.


-doc """
""".
-spec page_refs(Page :: page()) -> [binary()].

page_refs(Page) ->
    bondy_mst_page:refs(Page).


-spec delete(t()) -> ok.

delete(#?MODULE{pid = Pid, name = Name}) ->
    %% TODO fold over bucket (name) elements and delete them
    ok.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
do_get(Pid, Name, Hash) ->
    case leveled_bookie:book_get(Pid, Name, Hash) of
        {ok, [Page]} ->
            %% bag and duplicate bag tables
            Page;

        {ok, Page} ->
            %%  set and ordered_set tables
            Page;

        not_found ->
            undefined
    end.


%% @private
gc_aux(Acc0, Pid, Name, Root) when not is_map_key(Root, Acc0) ->
    case do_get(Pid, Name, Root) of
        undefined ->
            ok;
        Page ->
            Acc = maps:put(Root, Page, Acc0),
            lists:foldl(
                fun(X, IAcc) -> gc_aux(IAcc, Pid, Name, X) end,
                ok,
                page_refs(Page)
            )
    end;

gc_aux(Acc, _, _, _) ->
    Acc.



