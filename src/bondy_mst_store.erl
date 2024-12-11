%% ===========================================================================
%%  bondy_mst_store.erl -
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
%%
%%  This module contains a port the code written in Elixir for the
%%  simulations shown in the paper: Merkle Search Trees: Efficient State-Based
%%  CRDTs in Open Networks by Alex Auvolat, François Taïani
%% ===========================================================================


%% -----------------------------------------------------------------------------
%% @doc Behaviour to be implemented for page stores to allow their manipulation.
%% This behaviour may also be implemented by store proxies that track operations
%% and implement different synchronization or caching mechanisms.
%% @end
%% -----------------------------------------------------------------------------
-module(bondy_mst_store).


-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").


-record(?MODULE, {
    mod                 ::  module(),
    state               ::  backend(),
    transactions        ::  boolean()
}).

-type t()               ::  #?MODULE{}.
-type page()            ::  any().
-type backend()         ::  any().
%% -type iterator_action() ::  first
%%                             | last
%%                             | next
%%                             | prev
%%                             | binary()
%%                             | {seek, binary()}
%%                             | {seek_for_prev, binary()}.

-export_type([t/0]).
-export_type([backend/0]).
-export_type([page/0]).

%% API
-export([copy/3]).
-export([delete/1]).
-export([free/3]).
-export([gc/2]).
-export([get/2]).
-export([get_root/1]).
-export([has/2]).
-export([is_type/1]).
-export([missing_set/2]).
-export([new/2]).
-export([page_refs/2]).
-export([put/2]).
-export([set_root/2]).
-export([transaction/2]).


%% =============================================================================
%% CALLBACKS
%% =============================================================================


-callback new(Opts :: map()) -> backend().

-callback get_root(backend()) -> hash() | undefined.

-callback set_root(backend(), hash()) -> backend().

-callback get(backend(), page()) -> page() | undefined.

-callback has(backend(), page()) -> boolean().

-callback put(backend(), page()) -> {Hash :: hash(), backend()}.

-callback copy(backend(), OtherStore :: t(), Hash :: hash()) -> backend().

-callback free(backend(), hash(), page()) -> backend().

-callback gc(backend(), KeepRoots :: [list()] | Epoch :: epoch()) -> backend().

-callback missing_set(backend(), Root :: binary()) -> sets:set(hash()).

-callback page_refs(Page :: page()) -> Refs :: [binary()].

-callback delete(backend()) -> ok.

-callback transaction(backend(), Fun :: fun(() -> any())) ->
    any() | no_return().

-optional_callbacks([transaction/2]).



%% =============================================================================
%% API
%% =============================================================================



-spec new(Mod :: module(), Opts :: map()) -> t() | no_return().

new(Mod, Opts) when is_atom(Mod) andalso (is_map(Opts) orelse is_list(Opts)) ->
    #?MODULE{
        mod = Mod,
        state = Mod:new(Opts),
        transactions = supports_transactions(Mod)
    }.


-spec is_type(any()) -> boolean().

is_type(#?MODULE{}) -> true;
is_type(_) -> false.


%% -----------------------------------------------------------------------------
%% @doc Get the root hash.
%% Returns hash or `undefined'.
%% @end
%% -----------------------------------------------------------------------------
-spec get_root(Store :: t()) -> Root :: hash() | undefined.

get_root(#?MODULE{mod = Mod, state = State}) ->
    Mod:get_root(State).



%% -----------------------------------------------------------------------------
%% @doc Get the root hash.
%% Returns hash or `undefined'.
%% WARNING: You should never call this function. It is used internally.
%% @end
%% -----------------------------------------------------------------------------
-spec set_root(Store :: t(), Hash :: hash()) -> t().

set_root(#?MODULE{mod = Mod, state = State0} = T, Hash) when is_binary(Hash) ->
    State = Mod:set_root(State0, Hash),
    T#?MODULE{state = State}.


%% -----------------------------------------------------------------------------
%% @doc Get a page referenced by its hash.
%% Returns page or `undefined'.
%% @end
%% -----------------------------------------------------------------------------
-spec get(Store :: t(), Hash :: hash()) -> Page :: page() | undefined.

get(#?MODULE{mod = Mod, state = State}, Hash) ->
    Mod:get(State, Hash).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec has(Store :: t(), Hash :: hash()) -> boolean().

has(#?MODULE{mod = Mod, state = State}, Hash) ->
    Mod:has(State, Hash).


%% -----------------------------------------------------------------------------
%% @doc Put a page. Argument is the content of the page, returns the
%% hash that the store has associated to it.
%% @end
%% -----------------------------------------------------------------------------
-spec put(Store :: t(), Page :: page()) -> {Hash :: hash(), Store :: t()}.

put(#?MODULE{mod = Mod, state = State0} = T, Page) ->
    {Hash, State} = Mod:put(State0, Page),
    {Hash, T#?MODULE{state = State}}.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec copy(Store :: t(), OtherStore :: t(), Hash :: hash()) -> Store :: t().

copy(#?MODULE{mod = Mod, state = State0} = T, OtherStore, Hash) ->
    T#?MODULE{state = Mod:copy(State0, OtherStore, Hash)}.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec free(Store :: t(), Hash :: hash(), Page :: page()) -> Store :: t().

free(#?MODULE{mod = Mod, state = State0} = T, Hash, Page) ->
    T#?MODULE{state = Mod:free(State0, Hash, Page)}.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec gc(Store :: t(), KeepRoots :: [list()]) -> Store :: t().

gc(#?MODULE{mod = Mod, state = State0} = T, KeepRoots) ->
    T#?MODULE{state = Mod:gc(State0, KeepRoots)}.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec page_refs(Store :: t(), Page :: page()) -> Refs :: [binary()].

page_refs(#?MODULE{mod = Mod}, Page) ->
    Mod:page_refs(Page).


%% -----------------------------------------------------------------------------
%% @doc Returns the hashes of the pages identified by root hash that are missing
%% from the store.
%% @end
%% -----------------------------------------------------------------------------
-spec missing_set(Store :: t(), Root :: binary()) -> [hash()].

missing_set(#?MODULE{mod = Mod, state = State}, Root) ->
    Mod:missing_set(State, Root).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec delete(Store :: t()) -> ok.

delete(#?MODULE{mod = Mod, state = State}) ->
    Mod:delete(State).




%% =============================================================================
%% TRANSACTION API
%% =============================================================================


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec transaction(Store :: t(), Fun :: fun(() -> any())) ->
    any() | {error, Reason :: any()}.

transaction(#?MODULE{transactions = true, mod = Mod, state = State}, Fun) ->
    Mod:transaction(State, Fun);

transaction(#?MODULE{transactions = false}, Fun) ->
    Fun().



%% =============================================================================
%% PRIVATE
%% =============================================================================



supports_transactions(Mod) ->
    ok = bondy_mst_utils:ensure_loaded(Mod),
    erlang:function_exported(Mod, transaction, 2).

