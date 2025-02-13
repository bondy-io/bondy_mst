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


%% -----------------------------------------------------------------------------
%% @doc Module that represents objects that are used as data pages in a
%% pagestore and that may reference other data pages by their hash.
%% @end
%% -----------------------------------------------------------------------------
-module(bondy_mst_page).


-include("bondy_mst.hrl").

-record(?MODULE, {
    level               ::  level(),
    low                 ::  hash() | undefined,
    list                ::  [entry()],
    source = undefined  ::  source(),
    freed_at            ::  epoch() | undefined
}).

-type t()               ::  #?MODULE{}.
-type entry()           ::  {key(), value(), hash() | undefined}.
-type source()          ::  undefined | node().

-export_type([t/0]).
-export_type([entry/0]).

%% Defined in bondy_mst.hrl
-export_type([level/0]).
-export_type([key/0]).
-export_type([value/0]).
-export_type([hash/0]).
-export_type([source/0]).

-export([field_index/1]).
-export([fold/3]).
-export([foreach/2]).
-export([freed_at/1]).
-export([hash/2]).
-export([is_referenced_at/2]).
-export([is_type/1]).
-export([level/1]).
-export([list/1]).
-export([low/1]).
-export([new/3]).
-export([pattern/0]).
-export([refs/1]).
-export([set_freed_at/2]).
-export([set_source/2]).
-export([source/1]).



%% =============================================================================
%% API
%% =============================================================================


%% -----------------------------------------------------------------------------
%% @doc Creates a new page
%% @end
%% -----------------------------------------------------------------------------
-spec new(level(), hash() | undefined, [entry()]) -> t().

new(Level, Low, List) when is_integer(Level), is_list(List) ->
    #?MODULE{
        level = Level,
        low = Low,
        list = List,
        freed_at = undefined
    }.


%% -----------------------------------------------------------------------------
%% @doc Creates a new page
%% @end
%% -----------------------------------------------------------------------------
-spec pattern() -> t().

pattern() ->
    {
        ?MODULE,
        '$1', % level
        '$2', % low
        '$3', % list
        '$4', % source
        '$5'  % freed_at
    }.


%% -----------------------------------------------------------------------------
%% @doc Returns true if `Arg' is a page.
%% @end
%% -----------------------------------------------------------------------------
-spec is_type(Arg :: any()) -> boolean().

is_type(#?MODULE{}) -> true;
is_type(_) -> false.


-spec field_index(atom()) -> pos_integer().

field_index(level) -> #?MODULE.level;
field_index(low) -> #?MODULE.low;
field_index(list) -> #?MODULE.list;
field_index(source) -> #?MODULE.source;
field_index(freed_at) -> #?MODULE.freed_at.


%% -----------------------------------------------------------------------------
%% @doc Returns the level of this page in the tree i.e. the logical height.
%% @end
%% -----------------------------------------------------------------------------
-spec level(t()) -> level().

level(#?MODULE{level = Val}) -> Val.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec low(t()) -> hash().

low(#?MODULE{low = Val}) -> Val.


%% -----------------------------------------------------------------------------
%% @doc Returns the epoch number at which this page has been freed or
%% `undefined' if it hasn't i.e. it is still active.
%% @end
%% -----------------------------------------------------------------------------
-spec freed_at(t()) -> epoch() | undefined.

freed_at(#?MODULE{freed_at = Val}) -> Val.


%% -----------------------------------------------------------------------------
%% @doc Sets the version number at which this page has been freed.
%% @end
%% -----------------------------------------------------------------------------
-spec set_freed_at(t(), epoch()) -> t().

set_freed_at(#?MODULE{} = T, Epoch) when is_integer(Epoch) ->
    T#?MODULE{freed_at = Epoch}.



%% -----------------------------------------------------------------------------
%% @doc Returns the source from which we obtained this page
%% @end
%% -----------------------------------------------------------------------------
-spec source(t()) -> source().

source(#?MODULE{source = Val}) -> Val.


%% -----------------------------------------------------------------------------
%% @doc Sets the source from which we obtained this page
%% @end
%% -----------------------------------------------------------------------------
-spec set_source(t(), source()) -> t().

set_source(#?MODULE{} = T, undefined) ->
        T#?MODULE{source = undefined};

set_source(#?MODULE{} = T, Src) when is_atom(Src) ->
    set_source(T, atom_to_binary(Src));

set_source(#?MODULE{} = T, Src) when is_binary(Src) ->
    T#?MODULE{source = Src}.


%% -----------------------------------------------------------------------------
%% @doc Returns `true' if the page is referenced at `Epoch'.
%% Otherwise, returns `false'.
%% @end
%% -----------------------------------------------------------------------------
-spec is_referenced_at(t(), epoch()) -> boolean().

is_referenced_at(#?MODULE{freed_at = undefined}, _) ->
    true;

is_referenced_at(#?MODULE{freed_at = LastEpoch}, Epoch) ->
    LastEpoch >= Epoch.


%% -----------------------------------------------------------------------------
%% @doc %% @doc Computes the hash of the page using algorithm `Algo'.
%% This function must be used to obtain a hash as it ignores certain fields that
%% will diverge between replicas and are used for operational and/or efficiency
%% purposes.
%% @end
%% -----------------------------------------------------------------------------
hash(#?MODULE{} = T, Algo) when is_atom(Algo) ->
    #?MODULE{level = Level, low = Low, list = List} = T,
    bondy_mst_utils:hash({Level, Low, List}, Algo).


%% -----------------------------------------------------------------------------
%% @doc Returns the list of entries in this page.
%% @end
%% -----------------------------------------------------------------------------
-spec list(t()) -> [entry()].

list(#?MODULE{list = Val}) -> Val.



%% -----------------------------------------------------------------------------
%% @doc Calls `Fun(Entry, AccIn)' on successive entries of the page, starting
%% with `AccIn == Acc0'. `Fun/2' must return a new accumulator, which is passed
%% to the next call. The function returns the final value of the accumulator.
%% `Acc0' is returned if the tree is empty.
%% @end
%% -----------------------------------------------------------------------------
-spec fold(t(), fun((entry(), any()) -> any()), any()) -> any().

fold(#?MODULE{list = List}, Fun, Acc) ->
    lists:foldl(Fun, Acc, List).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec foreach(t(), fun((entry()) -> any())) -> ok.

foreach(#?MODULE{list = List}, Fun) ->
    lists:foreach(Fun, List).


%% -----------------------------------------------------------------------------
%% @doc Returns the hashes of all pages referenced by this page.
%% @end
%% -----------------------------------------------------------------------------
-spec refs(t()) -> [hash()].

refs(#?MODULE{list = List, low = Low}) ->
    Refs = [H || {_, _, H} <- List, H =/= undefined],

    case Low =/= undefined of
        true ->
            [Low | Refs];

        false ->
            Refs
    end.


