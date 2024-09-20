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

-record(?MODULE, {
    level,
    low,
    list
}).

-type t() ::  #?MODULE{}.

-export_type([t/0]).

-export([new/3]).
-export([refs/1]).
-export([level/1]).
-export([low/1]).
-export([list/1]).


%% =============================================================================
%% API
%% =============================================================================



new(Level, Low, List) ->
    #?MODULE{
        level = Level,
        low = Low,
        list = List
    }.


level(#?MODULE{level = Val}) -> Val.


low(#?MODULE{low = Val}) -> Val.


list(#?MODULE{list = Val}) -> Val.


-doc "Get hashes of all pages referenced by this page.".

refs(#?MODULE{list = List, low = Low}) ->
    Refs = [H || {_, _, H} <- List, H =/= undefined],
    case Low =/= undefined of
        true ->
            [Low | Refs];
        false ->
            Refs
    end.