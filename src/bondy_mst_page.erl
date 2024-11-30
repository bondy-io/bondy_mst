%% ===========================================================================
%%  bondy_mst_page.erl -
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



-module(bondy_mst_page).
-moduledoc"""
Module that represents objects that are used as data pages in a pagestore and
that may reference other data pages by their hash.
""".

-include("bondy_mst.hrl").

-record(?MODULE, {
    level           ::  level(),
    low             ::  hash() | undefined,
    list            ::  [entry()],
    freed_at        ::  epoch() | undefined
}).

-type t()           ::  #?MODULE{}.
-type entry()       ::  {key(), value(), hash() | undefined}.

-export_type([t/0]).
-export_type([entry/0]).
%% Defined in bondy_mst.hrl
-export_type([level/0]).
-export_type([key/0]).
-export_type([value/0]).
-export_type([hash/0]).

-export([freed_at/1]).
-export([is_referenced_at/2]).
-export([level/1]).
-export([list/1]).
-export([low/1]).
-export([new/3]).
-export([pattern/0]).
-export([refs/1]).
-export([set_freed_at/2]).



%% =============================================================================
%% API
%% =============================================================================


-doc "Creates a new page".
-spec new(level(), hash(), [entry()]) -> t().

new(Level, Low, List) when is_integer(Level), is_list(List) ->
    #?MODULE{
        level = Level,
        low = Low,
        list = List,
        freed_at = undefined
    }.


-doc "Creates a new page".
-spec pattern() -> t().

pattern() ->
    #?MODULE{
        level = '$1',
        low = '$2',
        list = '$3',
        freed_at = '$4'
    }.


-doc """
Returns the level of this page in the tree i.e. the logical height.
""".
-spec level(t()) -> level().

level(#?MODULE{level = Val}) -> Val.


-doc "".
-spec low(t()) -> hash().

low(#?MODULE{low = Val}) -> Val.


-doc """
Returns the epoch number at which this page has been freed or `undefined` if
it hasn't i.e. it is still active.
""".
-spec freed_at(t()) -> epoch() | undefined.

freed_at(#?MODULE{freed_at = Val}) -> Val.


-doc """
Sets the version number at which this page has been freed.
""".
-spec set_freed_at(t(), epoch()) -> t().

set_freed_at(#?MODULE{} = T, Epoch) when is_integer(Epoch) ->
    T#?MODULE{freed_at = Epoch}.


-doc """
Returns `true` if the page is referenced at `Epoch`. Otherwise, returns `false`.
""".
-spec is_referenced_at(t(), epoch()) -> boolean().

is_referenced_at(#?MODULE{freed_at = undefined}, _) ->
    true;

is_referenced_at(#?MODULE{freed_at = LastEpoch}, Epoch) ->
    LastEpoch >= Epoch.


-doc "Returns the list of entries in this page.".
-spec list(t()) -> [entry()].

list(#?MODULE{list = Val}) -> Val.


-doc "Returns the hashes of all pages referenced by this page.".
-spec refs(t()) -> [hash()].

refs(#?MODULE{list = List, low = Low}) ->
    Refs = [H || {_, _, H} <- List, H =/= undefined],

    case Low =/= undefined of
        true ->
            [Low | Refs];
        false ->
            Refs
    end.


