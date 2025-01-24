%% ===========================================================================
%%  bondy_mst_leveled_store.erl -
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
%% @doc Non-concurrent, MST backend using `leveled`.
%% @end
%% -----------------------------------------------------------------------------
-module(bondy_mst_leveled_store).
-behaviour(bondy_mst_store).

-include_lib("kernel/include/logger.hrl").
-include_lib("leveled/include/leveled.hrl").
-include("bondy_mst.hrl").

-record(?MODULE, {
    pid         :: pid(),
    name        :: atom() | binary()
}).

-type t()       ::  #?MODULE{}.
-type page()    ::  bondy_mst_page:t().

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
-export([missing_set/2]).
-export([open/1]).
-export([page_refs/1]).
-export([put/2]).
-export([set_root/2]).



%% =============================================================================
%% BONDY_MST_STORE CALLBACKS
%% =============================================================================



-spec open(Opts :: map() | list()) -> t().

open(Opts) when is_list(Opts) ->
    open(maps:from_list(Opts));

open(#{name := Name} = _Opts) ->
    %% TODO at the moment we use a global instance, we should give the option
    %% to create a dedicated instance or have shared store
    Pid = persistent_term:get({bondy_mst, leveled}),
    #?MODULE{pid = Pid, name = Name}.


-spec close(t()) -> ok.

close(#?MODULE{}) ->
    %% TODO at the moment we use a global instance, we should give the option
    %% to create a dedicated instance or have shared store
    ok.


-spec get_root(T :: t()) -> Root :: hash() | undefined.

get_root(#?MODULE{pid = Pid, name = Name}) ->
    do_get(Pid, Name, ?ROOT_KEY).


-spec set_root(T :: t(), Hash :: hash()) -> t().

set_root(#?MODULE{pid = Pid, name = Name} = T, Hash) ->
    ok = leveled_bookie:book_put(Pid, Name, ?ROOT_KEY, Hash, []),
    T.


-spec get(T :: t(), Hash :: binary()) -> Page :: page() | undefined.

get(#?MODULE{pid = Pid, name = Name}, Hash) ->
    do_get(Pid, Name, Hash).


-spec has(T :: t(), Hash :: binary()) -> boolean().

has(#?MODULE{pid = Pid, name = Name}, Hash) ->
    leveled_bookie:book_head(Pid, Name, Hash) /= not_found.


-spec put(T :: t(), Page :: page()) -> {Hash :: binary(), T :: t()}.

put(#?MODULE{pid = Pid, name = Name} = T, Page) ->
    Hash = bondy_mst_utils:hash(Page),
    ok = leveled_bookie:book_put(Pid, Name, Hash, Page, []),
    ok = leveled_bookie:book_put(Pid, Name, ?ROOT_KEY, Hash, []),
    {Hash, T}.


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


-spec free(T :: t(), Hash :: binary(), Page :: page()) -> T :: t().

free(#?MODULE{pid = Pid, name = Name} = T, Hash, _Page) ->
    ok = leveled_bookie:book_delete(Pid, Name, Hash, []),
    T.


-spec gc(T :: t(), KeepRoots :: [list()]) -> T :: t().

gc(#?MODULE{} = T, _KeepRoots) ->
    %% Do nothing, we free instead
    T.


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


-spec page_refs(Page :: page()) -> [binary()].

page_refs(Page) ->
    bondy_mst_page:refs(Page).


-spec delete(t()) -> ok.

delete(#?MODULE{pid = _Pid, name = _Name}) ->
    %% TODO fold over bucket (name) elements and delete them
    ok.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
do_get(Pid, Name, Hash) when is_binary(Hash) orelse Hash =:= ?ROOT_KEY ->
    case leveled_bookie:book_get(Pid, Name, Hash) of
        {ok, Page} ->
            Page;

        not_found ->
            undefined
    end.


