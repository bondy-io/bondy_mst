%% ===========================================================================
%%  bondy_mst.erl -
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
%% @doc
%%  This module implements a Merkle Search Tree (MST), a probabilistic data
%%  structure optimized for efficient storage and retrieval of key-value pairs.
%%
%%  The MST is designed for distributed systems where efficient merging and
%%  verification of large datasets are required.
%%
%%  It supports:
%%    - Efficient key-value insertion and retrieval.
%%    - Merkle-based verification for integrity checks.
%%    - Custom comparators and mergers.
%% An MST is a search tree, similar to a B-tree in the sense that internal tree
%% nodes contain several values that define a partition of the keys in which the
%% children values are separated.
%%
%% The tree is divided in layers which are numbered starting at layer 0 which
%% corresponds to the layer of the leaf nodes.
%%
%% The tree nodes in layer `L` are blocks of consecutive items whose boundaries
%% corresponds to items of layers `L' > L`.
%%
%% Deterministic randomness obtained by hashind the values is used to determine
%% the tree shape. Values stored in the MST are asigned a layer by computing
%% their hash and writing that value in base `B`. The layer to which an item is
%% assigned is the layer whose numner is the length of the longest prefix of the
%% hash.
%% @end
%% -----------------------------------------------------------------------------
-module(bondy_mst).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").

-record(bondy_mst, {
    store               ::  bondy_mst_store:t(),
    %% `comparator` is a compare function for keys
    comparator          ::  comparator(),
    %% `merger` is a function for merging two items that have the same key
    merger              ::  merger(),
    hash_algorithm      ::  atom()
}).

-type t()               ::  #?MODULE{}.
-type opts()            ::  [opt()] | opts_map().
-type opt()             ::  {store, bondy_mst_store:t()}
                            | {hash_algorithm, atom()}
                            | {merger, merger()}
                            | {comparator, comparator()}.
-type opts_map()        ::  #{
                                store => bondy_mst_store:t(),
                                hash_algorithm => atom(),
                                merger => merger(),
                                comparator => comparator()
                            }.
-type comparator()      ::  fun((key(), key()) -> eq | lt | gt).
-type merger()          ::  fun((key(), value(), value()) -> value()).
-type key_range()       ::  {key(), key()}
                            | {first, key()}
                            | {key(), undefined}.
%% Fold
-type fold_fun()        ::  fun(({key(), value()}, any()) -> any()).
-type fold_opts()       ::  [fold_opt()].
-type fold_opt()        ::  {root, hash()}
                            | {first, key()}
                            | {match_spec, term()}
                            | {stop, key()}
                            | {keys_only, boolean()}
                            | {limit, pos_integer() | infinity}.
-type fold_pages_fun()  ::  fun(({hash(), bondy_mst_page:t()}, any()) -> any()).

-export_type([t/0]).
-export_type([opt/0]).
-export_type([opts/0]).
-export_type([opts_map/0]).
-export_type([comparator/0]).
-export_type([merger/0]).

%% Defined in bondy_mst.hrl
-export_type([level/0]).
-export_type([key/0]).
-export_type([value/0]).
-export_type([hash/0]).


-export([delete/1]).
-export([diff_to_list/2]).
-export([dump/1]).
-export([first/1]).
-export([fold/3]).
-export([fold/4]).
-export([fold_pages/4]).
-export([foreach/2]).
-export([foreach/3]).
-export([gc/1]).
-export([gc/2]).
-export([get/2]).
-export([get_range/2]).
-export([keys/1]).
-export([last/1]).
-export([last_n/3]).
-export([merge/2]).
-export([merge/3]).
-export([missing_set/2]).
-export([new/0]).
-export([new/1]).
-export([put/2]).
-export([put/3]).
-export([put_page/2]).
-export([root/1]).
-export([store/1]).
-export([to_list/1]).
-export([to_list/2]).

-export([format_error/2]).


%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Create a new Merkle Search Tree using the default store.
%% The same as calling `new(#{})'.
%% @end
%% -----------------------------------------------------------------------------
-spec new() -> t().

new() ->
    new(#{}).


%% -----------------------------------------------------------------------------
%% @doc Creates a new MST instance with configurable options.
%%
%% This structure can be used as a CRDT set with only true keys (default)
%% or as a CRDT map if a proper merger function is given.
%% == Options ==
%%
%% <ul>
%% <li>`store => bondy_mst_store:t()' - Defaults to an instance of
%% `bondy_mst_map_store'.</li>
%% <li>`merger => merger()' - a merger function.
%% Defaults to the grow-only set merger function `merger/3'.</li>
%% <li>`comparator => comparator()' - a key comparator function.
%% Defaults to `comparator/2'.</li>
%% </ul>
%% @return A new MST instance.
%% @end
%% -----------------------------------------------------------------------------
-spec new(Opts :: opts()) -> t().

new(Opts) when is_list(Opts) ->
    new(maps:from_list(Opts));

new(Opts) when is_map(Opts) ->
    Algo = maps:get(hash_algorithm, Opts, sha256),
    DefaultStore = fun() ->
        bondy_mst_store:open(bondy_mst_map_store, Algo, Opts)
    end,
    Store = get_option(store, Opts, DefaultStore),
    Comparator = get_option(comparator, Opts, fun comparator/2),
    Merger = get_option(merger, Opts, fun merger/3),

    #?MODULE{
        store = Store,
        comparator = Comparator,
        merger = Merger,
        hash_algorithm = Algo
    }.


%% -----------------------------------------------------------------------------
%% @doc Deletes the tree (by deleting its backend store).
%% @end
%% -----------------------------------------------------------------------------
-spec delete(t()) -> ok.

delete(#?MODULE{store = Val}) ->
    bondy_mst_store:delete(Val).


%% -----------------------------------------------------------------------------
%% @doc Returns the tree's root hash.
%% @end
%% -----------------------------------------------------------------------------
-spec root(Tree :: t()) -> hash() | undefined.

root(#?MODULE{store = Store}) ->
    bondy_mst_store:get_root(Store).


%% -----------------------------------------------------------------------------
%% @doc Returns the tree's store.
%% @end
%% -----------------------------------------------------------------------------
-spec store(t()) -> bondy_mst_store:t().

store(#?MODULE{store = Val}) ->
    Val.


%% -----------------------------------------------------------------------------
%% @doc Returns the value associated with key `Key'.
%% @end
%% -----------------------------------------------------------------------------
-spec get(T :: t(), Key :: key()) -> Value :: any().

get(#?MODULE{} = T, Key) ->
    get(T, Key, root(T)).


%% -----------------------------------------------------------------------------
%% @doc Returns the first key-value pair in the MST or `undefined' if empty.
%% @end
%% -----------------------------------------------------------------------------
-spec first(T :: t()) -> Value :: {key(), value()} | undefined.

first(#?MODULE{} = T) ->
    first(T, root(T)).


%% -----------------------------------------------------------------------------
%% @doc Returns the last key-value pair in the MST or `undefined' if empty.
%% @end
%% -----------------------------------------------------------------------------
-spec last(T :: t()) -> Value :: {key(), value()} | undefined.

last(#?MODULE{} = T) ->
    last(T, root(T)).



-spec get_range(T :: t(), Range :: key_range()) -> Value :: any().

get_range(#?MODULE{} = _T, {_From, _To}) ->
    error(not_implemented).
    %% Opts = [{first, From}, {stop, To}],
    %% fold(
    %%     T,
    %%     fun(K, V, Acc) -> [{K, V} | Acc] end,
    %%     [],
    %%     Opts
    %% ).


%% -----------------------------------------------------------------------------
%% @doc List all items.
%% @end
%% -----------------------------------------------------------------------------
-spec to_list(t()) -> [{key(), value()}].

to_list(#?MODULE{} = T) ->
    to_list(T, root(T)).


%% -----------------------------------------------------------------------------
%% @doc List all items.
%% @end
%% -----------------------------------------------------------------------------
-spec to_list(t(), hash() | undefined) -> [{key(), value()}].

to_list(#?MODULE{}, undefined) ->
    [];

to_list(#?MODULE{} = T, Root) when is_binary(Root) ->
    lists:reverse(
        fold(T, fun(E, Acc) -> [E | Acc] end, [], [{root, Root}])
    ).


%% -----------------------------------------------------------------------------
%% @doc Calls `Fun(Elem)' for each element `Elem' in the tree, starting from its
%% current root.
%% This function is used for its side effects and the evaluation order is
%% defined to be the same as the order of the elements in the tree.
%%
%% The same as calling `foreach(T, root(T))'.
%% @end
%% -----------------------------------------------------------------------------
-spec foreach(t(), fun(({key(), value()}) -> ok)) -> ok.

foreach(#?MODULE{} = T, Fun) ->
    foreach(T, Fun, []).



%% -----------------------------------------------------------------------------
%% @doc Calls `Fun(Elem)' for each element `Elem' in the tree, starting from
%% `Root'.
%% This function is used for its side effects and the evaluation order is
%% defined to be the same as the order of the elements in the tree.
%% @end
%% -----------------------------------------------------------------------------
-spec foreach(t(),fun(({key(), value()}) -> ok), Opts :: list()) -> ok.

foreach(#?MODULE{store = Store} = T, Fun, Opts) ->
    Root = key_value:get_lazy(root, Opts, fun() -> root(T) end),
    do_foreach(Store, Fun, Opts, Root).


%% -----------------------------------------------------------------------------
%% @doc Calls `Fun(Elem, AccIn)'' on successive elements of tree `T', starting
%% from the current root with `AccIn == Acc0'. `Fun/2' must return a new
%% accumulator, which is passed to the next call. The function returns the final
%% value of the accumulator. `Acc0' is returned if the tree is empty.
%% @end
%% -----------------------------------------------------------------------------
-spec fold(t(), Fun :: fold_fun(), AccIn :: any()) -> AccOut :: any().

fold(T, Fun, AccIn) ->
    fold(T, Fun, AccIn, []).


%% -----------------------------------------------------------------------------
%% @doc Calls `Fun(Elem, AccIn)'' on successive elements of tree `T', starting
%% from the current root with `AccIn == Acc0'. `Fun/2' must return a new
%% accumulator, which is passed to the next call. The function returns the final
%% value of the accumulator. `Acc0' is returned if the tree is empty.
%% @end
%% -----------------------------------------------------------------------------
-spec fold(t(), Fun :: fold_fun(), AccIn :: any(), Opts :: fold_opts()) ->
    AccOut :: any().

fold(#?MODULE{store = Store} = T, Fun, AccIn, Opts) ->
    Root = key_value:get_lazy(root, Opts, fun() -> root(T) end),
    do_fold(Store, Fun, AccIn, Opts, Root).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec fold_pages(
    t(), Fun :: fold_pages_fun(), AccIn :: any(), Opts :: fold_opts()) ->
    AccOut :: any().

fold_pages(#?MODULE{store = Store} = T, Fun, AccIn, Opts)
when is_function(Fun, 2), is_map(Opts) ->
    Root = key_value:get_lazy(root, Opts, fun() -> root(T) end),
    do_fold_pages(Store, Fun, AccIn, Opts, Root).


%% -----------------------------------------------------------------------------
%% @doc List all items.
%% @end
%% -----------------------------------------------------------------------------
-spec keys(t()) -> [{key(), value()}].

keys(#?MODULE{} = T) ->
    lists:reverse(fold(T, fun({K, _}, Acc) -> [K | Acc] end, [])).


%% -----------------------------------------------------------------------------
%% @doc Computes the difference between two MSTs and returns it as a list.
%% @end
%% -----------------------------------------------------------------------------
-spec diff_to_list(t(), t()) -> list().

diff_to_list(#?MODULE{} = T1, #?MODULE{} = T2) ->
    diff_to_list(
        T1,
        T1#?MODULE.store,
        root(T1),
        T2#?MODULE.store,
        root(T2)
    ).


%% -----------------------------------------------------------------------------
%% @doc Get the last `N' items of the tree, or the last `N' items strictly
%% before given upper bound `TopBound' if non `undefined'.
%% @end
%% -----------------------------------------------------------------------------
last_n(#?MODULE{} = T, TopBound, N) ->
    last_n(T, TopBound, N, root(T)).


%% -----------------------------------------------------------------------------
%% @doc Inserts a key in the tree. The same as calling `put(T, Key, true)'.
%% @end
%% -----------------------------------------------------------------------------
-spec put(t(), key()) -> t().

put(#?MODULE{} = T, Key) ->
    put(T, Key, true).


%% -----------------------------------------------------------------------------
%% @doc Inserts a key-value pair into the MST.
%%
%% If key `Key' already exists in tree `Tree1', the old associated value is
%% merged with `Value' by calling the configured `merger' function. The function
%% returns a new map `Tree2' containing the new association and the old
%% associations in `Tree1'.
%%
%% The call fails with an exception if the tree has not been initialised with a
%% `merger' function supporting the type of `Value'.
%% @end
%% -----------------------------------------------------------------------------
-spec put(Tree1 :: t(), Key :: key(), Value :: value()) -> Tree2 :: t().

put(#?MODULE{store = Store0} = T, Key, Value) ->
    Fun = fun() ->
        Level = calc_level(T, Key),
        {Root, Store1} = put_at(T, Key, Value, Level),
        Store = bondy_mst_store:set_root(Store1, Root),
        T#?MODULE{store = Store}
    end,
    bondy_mst_store:transaction(Store0, Fun).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec put_page(t(), bondy_mst_page:t()) -> {Hash :: hash(), t()}.

put_page(#?MODULE{store = Store0} = T, Page) ->
    Fun = fun() ->
        {Hash, Store} = bondy_mst_store:put(Store0, Page),
        {Hash, T#?MODULE{store = Store}}
    end,
    bondy_mst_store:transaction(Store0, Fun).


%% -----------------------------------------------------------------------------
%% @doc Merges two MSTs into a single tree.
%% @end
%% -----------------------------------------------------------------------------
-spec merge(T1 :: t(), T2 :: t()) -> NewT1 :: t().

merge(#?MODULE{} = T1, #?MODULE{} = T2) ->
    merge(T1, T2, root(T2)).


%% -----------------------------------------------------------------------------
%% @doc Merges two MSTs into a single tree.
%% @end
%% -----------------------------------------------------------------------------
-spec merge(T1 :: t(), T2 :: t(), Root :: hash()) -> NewT1 :: t().

merge(#?MODULE{store = Store0} = T1, #?MODULE{} = T2, Root)
when is_binary(Root) ->
    Fun = fun() ->
        {NewRoot, Store1} = merge_aux(T1, T2, Store0, root(T1), Root),
        Store = bondy_mst_store:set_root(Store1, NewRoot),
        T1#?MODULE{store = Store}
    end,
    bondy_mst_store:transaction(Store0, Fun).


%% -----------------------------------------------------------------------------
%% @doc Returns the hashes of the pages identified by root hash that are missing
%% from the store.
%% @end
%% -----------------------------------------------------------------------------
-spec missing_set(t(), Root :: binary()) -> [hash()].

missing_set(#?MODULE{store = Store}, Root) ->
    bondy_mst_store:missing_set(Store, Root).


%% -----------------------------------------------------------------------------
%% @doc Dumps the structure of the MST for debugging purposes.
%% @end
%% -----------------------------------------------------------------------------
-spec dump(t()) -> ok.

dump(#?MODULE{store = Store} = T) ->
    dump(Store, root(T)).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec gc(t()) -> t().

gc(#?MODULE{} = T) ->
    gc(T, [root(T)]).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec gc(t(), KeepRoots :: [binary()] | Epoch :: non_neg_integer()) -> t().

gc(#?MODULE{store = Store0} = T, Arg) when is_list(Arg) orelse is_integer(Arg) ->
    Fun = fun() ->
        Store = bondy_mst_store:gc(Store0, Arg),
        T#?MODULE{store = Store}
    end,
    bondy_mst_store:transaction(Store0, Fun).



format_error(Reason, [{_M, _F, _As, Info} | _]) ->
    ErrorInfo = proplists:get_value(error_info, Info, #{}),
    ErrorMap = maps:get(cause, ErrorInfo),
    ErrorMap#{
        %% general => "optional general information",
        reason => io_lib:format("~p: ~p", [?MODULE, Reason])
    }.


%% =============================================================================
%% PRIVATE: OPTIONS VALIDATION & DEFAULTS
%% =============================================================================

%% priv
get_option(store, #{store := Store} = Opts, _) ->
    bondy_mst_store:is_type(Store)
        orelse erlang:error(
            badarg,
            [Opts],
            [{error_info, #{
                module => ?MODULE,
                cause => #{
                    1 =>
                        "value for option 'store' "
                        "should be a valid bondy_mst_store:t()"
                }
            }}]
        ),

    Store;

get_option(store, _, Default) ->
    apply_default(Default);

get_option(comparator, #{comparator := Fun}, _) when is_function(Fun, 2) ->
    Fun;

get_option(comparator, _, Default) ->
    apply_default(Default);

get_option(merger, #{merger := Fun}, _) when is_function(Fun, 3) ->
    Fun;

get_option(merger, _, Default) ->
    apply_default(Default).


apply_default(Default) when is_function(Default, 0) ->
    Default();

apply_default(Default) ->
    Default.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @private
%% @doc Returns the default comparator function used in the MST.
%% @end
%% -----------------------------------------------------------------------------
comparator(A, B) when A < B -> lt;
comparator(A, B) when A == B -> eq;
comparator(A, B) when A > B -> gt.


%% -----------------------------------------------------------------------------
%% @private
%% @doc Returns the default merger function, assuming MST as a set. (where all
%% values are `true').
%% @end
%% -----------------------------------------------------------------------------
merger(_Key, true, true) ->
    true.


%% @private
%% Computes the level of a key by hashing and counting leading zeroes.
calc_level(#?MODULE{hash_algorithm = Algo}, Key) ->
    Hash = binary:encode_hex(bondy_mst_utils:hash(Key, Algo)),
    count_leading_zeroes(Hash, 0).


%% @private
%% Counts leading zeroes in a binary hash.
count_leading_zeroes(<<"0", Rest/binary>>, Acc) ->
    count_leading_zeroes(Rest, Acc + 1);

count_leading_zeroes(_, Acc) ->
    Acc.


%% @private
%% Compares two keys using the MST’s configured comparator.
compare(#?MODULE{comparator = Fun}, A, B) ->
    Fun(A, B).


%% @private
%% Merges two values using the MST’s configured merger function.
merge_values(#?MODULE{merger = Fun}, Key, A, B) ->
    Fun(Key, A, B).


%% @private
-spec get(T :: t(), Key :: key(), Root :: binary() | undefined) ->
    Value :: any().

get(#?MODULE{}, _, undefined) ->
    undefined;

get(#?MODULE{} = T, Key, Root) ->
    case bondy_mst_store:get(T#?MODULE.store, Root) of
        undefined ->
            undefined;

        Page ->
            Low = bondy_mst_page:low(Page),
            List = bondy_mst_page:list(Page),
            do_get(T, Key, Low, List)
    end.


%% @private
%% Recursively retrieves a value from the MST.
do_get(T, Key, Low, []) ->
    get(T, Key, Low);

do_get(T, Key, Low, [{K, V, Low2} | Rest]) ->
    case compare(T, Key, K) of
        eq ->
            V;
        lt ->
            get(T, Key, Low);
        gt ->
            do_get(T, Key, Low2, Rest)
    end.


first(#?MODULE{}, undefined) ->
    undefined;

first(#?MODULE{} = T, Root) ->
    Page = bondy_mst_store:get(T#?MODULE.store, Root),
    case bondy_mst_page:low(Page) of
        undefined ->
            case bondy_mst_page:list(Page) of
                [] ->
                    undefined;

                [{K, V, undefined} | _] ->
                    {K, V}
            end;

        Low ->
            first(T, Low)
    end.


last(#?MODULE{}, undefined) ->
    undefined;

last(#?MODULE{} = T, Root) ->
    Page = bondy_mst_store:get(T#?MODULE.store, Root),
    case Page == undefined orelse bondy_mst_page:list(Page) of
        true ->
            undefined;

        [] ->
            undefined;

        L when is_list(L) ->
            case lists:last(L) of
                {K, V, undefined} ->
                    {K, V};

                {_, _, Next} ->
                    last(T, Next)
            end
    end.


%% @private
last_n(#?MODULE{}, _, _, undefined) ->
    [];

last_n(#?MODULE{} = T, TopBound, N, Root) ->
    case bondy_mst_store:get(T#?MODULE.store, Root) of
        undefined ->
            [];

        Page ->
            Low = bondy_mst_page:low(Page),
            List = bondy_mst_page:list(Page),
            do_last_n(T, TopBound, N, Low, List)
    end.


%% @private
do_last_n(T, TopBound, N, Low, []) ->
    last_n(T, TopBound, N, Low);

do_last_n(T, TopBound, N, Low, [{K, V, Low2} | Rest]) ->
    case TopBound == undefined orelse compare(T, TopBound, K) == gt of
        true ->
            Items0 = do_last_n(T, TopBound, N, Low2, Rest),
            Items =
                case length(Items0) < N of
                    true ->
                        [{K, V} | Items0];
                    false ->
                        Items0
                end,
            Cnt = length(Items),
            case Cnt < N of
                true ->
                    last_n(T, TopBound, N - Cnt, Low) ++ Items;
                false ->
                    Items
            end;
        false ->
            last_n(T, TopBound, N, Low)
    end.


%% @private
put_at(T, Key, Value, Level) ->
    put_at(T, Key, Value, Level, T#?MODULE.store, root(T)).


%% @private
put_at(_T, Key, Value, Level, Store, undefined) ->
    NewPage = bondy_mst_page:new(Level, undefined, [{Key, Value, undefined}]),
    bondy_mst_store:put(Store, NewPage);

put_at(T, Key, Value, Level, Store0, Root) ->
    Page = bondy_mst_store:get(Store0, Root),
    [First | _] = bondy_mst_page:list(Page),

    case bondy_mst_page:level(Page) of
        PageLevel when PageLevel < Level ->
            put_at_higher_level(T, Key, Value, Level, Root, Store0);

        PageLevel when PageLevel == Level ->
            Store = bondy_mst_store:free(Store0, Root, Page),
            put_at_same_level(T, Key, Value, Level, First, Store, Page);

        PageLevel when PageLevel > Level ->
            Store = bondy_mst_store:free(Store0, Root, Page),
            put_at_lower_level(T, Key, Value, Level, First, Store, Page)
    end.


%% @private
put_at_higher_level(T, Key, Value, Level, Root, Store0) ->
    {Low, High, Store} = split(T, Store0, Root, Key),
    NewPage = bondy_mst_page:new(Level, Low, [{Key, Value, High}]),
    bondy_mst_store:put(Store, NewPage).


%% @private
put_at_same_level(T, Key, Value, Level, {K0, _, _}, Store0, Page) ->
    List0 = bondy_mst_page:list(Page),
    Low = bondy_mst_page:low(Page),
    PageLevel = bondy_mst_page:level(Page),

    case compare(T, Key, K0) of
        lt ->
            {LowA, LowB, Store} = split(T, Store0, Low, Key),
            List = [{Key, Value, LowB} | List0],
            NewPage = bondy_mst_page:new(Level, LowA, List),
            bondy_mst_store:put(Store, NewPage);

        Other when Other == gt orelse Other == eq ->
            {List1, Store} = put_after_first(T, Key, Value, Store0, List0),
            NewPage = bondy_mst_page:new(PageLevel, Low, List1),
            bondy_mst_store:put(Store, NewPage)
    end.


%% @private
put_at_lower_level(T, Key, Value, Level, {K0, _, _}, Store0, Page) ->
    List0 = bondy_mst_page:list(Page),
    Low0 = bondy_mst_page:low(Page),
    PageLevel = bondy_mst_page:level(Page),

    case compare(T, Key, K0) of
        lt ->
            {Low, Store} = put_at(T, Key, Value, Level, Store0, Low0),
            NewPage = bondy_mst_page:new(PageLevel, Low, List0),
            bondy_mst_store:put(Store, NewPage);

        gt ->
            {List, Store} = put_sub_after_first(
                T, Key, Value, Store0, Level, List0
            ),
            NewPage = bondy_mst_page:new(PageLevel, Low0, List),
            bondy_mst_store:put(Store, NewPage)
    end.


%% @private
put_after_first(T, Key, Value, Store0, [{K1, V1, R1}]) ->
    case compare(T, K1, Key) of
        eq ->
            List = [{K1, merge_values(T, Key, V1, Value), R1}],
            {List, Store0};

        lt ->
            {R1A, R1B, Store} = split(T, Store0, R1, Key),
            List = [{K1, V1, R1A}, {Key, Value, R1B}],
            {List, Store}
    end;

put_after_first(T, Key, Value, Store0, [First, Second | Rest0]) ->
    {K1, V1, R1} = First,
    {K2, _, _} = Second,

    case compare(T, K1, Key) of
        eq ->
            List = [{K1, merge_values(T, K1, V1, Value), R1}, Second | Rest0],
            {List, Store0};

        lt ->
            case compare(T, K2, Key) of
                gt ->
                    {R1A, R1B, Store} = split(T, Store0, R1, Key),
                    List = [{K1, V1, R1A}, {Key, Value, R1B}, Second | Rest0],
                    {List, Store};

                _ ->
                    {Rest, Store} = put_after_first(
                        T, Key, Value, Store0, [Second | Rest0]
                    ),
                    List = [First | Rest],
                    {List, Store}

            end
    end.


%% @private
put_sub_after_first(T, Key, Value, Store0, Level, [{K1, V1, R1}]) ->
    case compare(T, K1, Key) of
        eq ->
            error(inconsistency);

        _ ->
            {R, Store} = put_at(T, Key, Value, Level, Store0, R1),
            List = [{K1, V1, R}],
            {List, Store}
    end;


put_sub_after_first(T, Key, Value, Store0, Level, [First, Second | Rest0]) ->
    {K1, V1, R1} = First,
    {K2, _, _} = Second,

    case compare(T, K1, Key) of
        eq ->
            error(inconsistency);

        _ ->
            case compare(T, Key, K2) of
                lt ->
                    {R, Store} = put_at(T, Key, Value, Level, Store0, R1),
                    List = [{K1, V1, R}, Second | Rest0],
                    {List, Store};
                _ ->
                    {Rest, Store} = put_sub_after_first(
                        T, Key, Value, Store0, Level, [Second | Rest0]
                    ),
                    List = [First | Rest],
                    {List, Store}
            end
    end.


%% @private
split(_, Store, undefined, _) ->
    {undefined, undefined, Store};

split(T, Store0, Hash, Key) ->
    Page = get_page(T, Store0, Hash),
    Level = bondy_mst_page:level(Page),
    Low = bondy_mst_page:low(Page),
    [{K0, _, _} | _] = List0 = bondy_mst_page:list(Page),

    Store1 = bondy_mst_store:free(Store0, Hash, Page),

    case compare(T, Key, K0) of
        lt ->
            {LowLow, LowHi, Store2} = split(T, Store1, Low, Key),
            NewPage = bondy_mst_page:new(Level, LowHi, List0),
            {NewPageHash, Store} = bondy_mst_store:put(Store2, NewPage),
            {LowLow, NewPageHash, Store};

        gt ->
            {List, P2, Store2} = split_aux(T, Store1, Key, Level, List0),
            NewPage = bondy_mst_page:new(Level, Low, List),
            {NewPageHash, Store} = bondy_mst_store:put(Store2, NewPage),
            {NewPageHash, P2, Store}
    end.


%% @private
split_aux(T, Store0, Key, _, [{K1, V1, R1}]) ->
    case compare(T, K1, Key) of
        eq ->
            error(inconsistency);

        _ ->
            {R1L, R1H, Store} = split(T, Store0, R1, Key),
            {[{K1, V1, R1L}], R1H, Store}
    end;


split_aux(T, Store0, Key, Level, [First, Second | Rest0]) ->
    {K1, V1, R1} = First,
    {K2, _, _} = Second,

    case compare(T, Key, K2) of
        eq ->
            error(inconsistency);

        lt ->
            {R1L, R1H, Store1} = split(T, Store0, R1, Key),
            NewPage = bondy_mst_page:new(Level, R1H, [Second | Rest0]),
            {NewPageHash, Store} = bondy_mst_store:put(Store1, NewPage),
            {[{K1, V1, R1L}], NewPageHash, Store};

        gt ->
            {Rest, Hi, Store} = split_aux(
                T, Store0, Key, Level, [Second | Rest0]
            ),
            {[First | Rest], Hi, Store}
    end.


%% @private
get_page(T, Store, Hash) ->
    case bondy_mst_store:get(Store, Hash) of
        undefined ->
            bondy_mst_store:get(T#?MODULE.store, Hash);

        Page ->
            Page
    end.


%% @private
-spec merge_aux(
    A :: t(),
    B :: t(),
    Store :: bondy_mst_store:t(),
    ARoot :: hash(),
    BRoot :: hash()) ->
    {NewRootHash :: hash(), NewStore :: bondy_mst_store:t()}.

merge_aux(_, _, Store0, Root, Root) ->
    {Root, Store0};

merge_aux(_, _, Store0, ARoot, undefined) ->
    {ARoot, Store0};

merge_aux(_, B, Store0, undefined, BRoot) ->
    Store = bondy_mst_store:copy(Store0, B#?MODULE.store, BRoot),
    {BRoot, Store};

merge_aux(A, B, Store0, ARoot, BRoot) ->
    APage = bondy_mst_store:get(Store0, ARoot),
    ALevel = bondy_mst_page:level(APage),
    ALow = bondy_mst_page:low(APage),
    AEntries = bondy_mst_page:list(APage),

    BPage = get_page(B, Store0, BRoot),
    BLevel = bondy_mst_page:level(BPage),
    BLow = bondy_mst_page:low(BPage),
    BEntries = bondy_mst_page:list(BPage),

    {Level, {Low, List, Store}} =
        case BLevel of
            ALevel ->
                {
                    ALevel,
                    merge_aux_rec(A, B, Store0, ALow, AEntries, BLow, BEntries)
                };
            BLevel when ALevel > BLevel ->
                {
                    ALevel,
                    merge_aux_rec(A, B, Store0, ALow, AEntries, BRoot, [])
                };
            BLevel when ALevel < BLevel ->
                {
                    BLevel,
                    merge_aux_rec(A, B, Store0, ARoot, [], BLow, BEntries)
                }
        end,
    NewPage = bondy_mst_page:new(Level, Low, List),
    bondy_mst_store:put(Store, NewPage).


%% @private
merge_aux_rec(A, B, Store0, ALow, [], BLow, []) ->
    {Hash, Store} = merge_aux(A, B, Store0, ALow, BLow),
    {Hash, [], Store};

merge_aux_rec(A, B, Store0, ALow, [], BLow, [{K, V, R} | BRest]) ->
    {ALowL, ALowH, Store1} = split(A, Store0, ALow, K),
    {NewLow, Store2} = merge_aux(A, B, Store1, ALowL, BLow),
    {NewR, NewRest, Store} = merge_aux_rec(A, B, Store2, ALowH, [], R, BRest),
    {NewLow, [{K, V, NewR} | NewRest], Store};

merge_aux_rec(A, B, Store0, ALow, [{K, V, R} | ARest], BLow, []) ->
    {BLowL, BLowH, Store1} = split(B, Store0, BLow, K),
    {NewLow, Store2} = merge_aux(A, B, Store1, ALow, BLowL),
    {NewR, NewRest, Store} = merge_aux_rec(A, B, Store2, R, ARest, BLowH, []),
    {NewLow, [{K, V, NewR} | NewRest], Store};

merge_aux_rec(
    A,
    B,
    Store0,
    ALow,
    [{AKey, AValue, ARoot} | ARest] = AEntries,
    BLow,
    [{BKey, BValue, BRoot} | BRest] = BEntries) ->
    case compare(A, AKey, BKey) of
        lt ->
            {BLowL, BLowH, Store1} = split(B, Store0, BLow, AKey),
            {NewLow, Store2} = merge_aux(A, B, Store1, ALow, BLowL),
            {NewR, NewRest, Store} = merge_aux_rec(
                A, B, Store2, ARoot, ARest, BLowH, BEntries
            ),
            {NewLow, [{AKey, AValue, NewR} | NewRest], Store};

        gt ->
            {ALowL, ALowH, Store1} = split(A, Store0, ALow, BKey),
            {NewLow, Store2} = merge_aux(A, B, Store1, ALowL, BLow),
            {NewR, NewRest, Store} = merge_aux_rec(
                A, B, Store2, ALowH, AEntries, BRoot, BRest
            ),
            {NewLow, [{BKey, BValue, NewR} | NewRest], Store};

        eq ->
            {NewLow, Store1} = merge_aux(A, B, Store0, ALow, BLow),
            NewV = merge_values(A, AKey, AValue, BValue),
            {NewR, NewRest, Store} = merge_aux_rec(
                A, B, Store1, ARoot, ARest, BRoot, BRest
            ),
            {NewLow, [{AKey, NewV, NewR} | NewRest], Store}
    end.


%% @private
%% Iterates over the MST and applies a function to each element.
do_fold(_, _, AccIn, _, undefined) ->
    AccIn;

do_fold(Store, Fun, AccIn, Opts, Root) ->
    case bondy_mst_store:get(Store, Root) of
        undefined ->
            AccIn;

        Page ->
            Low = bondy_mst_page:low(Page),
            AccOut = do_fold(Store, Fun, AccIn, Opts, Low),
            bondy_mst_page:fold(
                Page,
                fun({K, V, Hash}, Acc0) ->
                    Acc1 = Fun({K, V}, Acc0),
                    do_fold(Store, Fun, Acc1, Opts, Hash)
                end,
                AccOut
            )
    end.


%% @private
%% Iterates over the MST and applies a function to each element.
do_fold_pages(_, _, Acc, _, undefined) ->
    Acc;

do_fold_pages(Store, Fun, AccIn, Opts, Root) ->
    case bondy_mst_store:get(Store, Root) of
        undefined ->
            AccIn;

        Page ->
            Low = bondy_mst_page:low(Page),
            AccOut = do_fold_pages(Store, Fun, AccIn, Opts, Low),
            bondy_mst_page:fold(
                Page,
                fun({_, _, Hash}, Acc0) ->
                    do_fold_pages(Store, Fun, Acc0, Opts, Hash)
                end,
                Fun({Root, Page}, AccOut)
            )
    end.


%% @private
do_foreach(_, _, _, undefined) ->
    ok;

do_foreach(Store, Fun, Opts, Root) ->
    case bondy_mst_store:get(Store, Root) of
        undefined ->
            ok;

        Page ->
            Low = bondy_mst_page:low(Page),
            ok = do_foreach(Store, Fun, Opts, Low),
            bondy_mst_page:foreach(
                Page,
                fun({K, V, Hash}) ->
                    ok = Fun({K, V}),
                    do_foreach(Store, Fun, Opts, Hash)
                end
            )
    end.



%% @private
diff_to_list(_, _, R, _, R) ->
    [];

diff_to_list(_, _, undefined, _, _) ->
    [];

diff_to_list(_, Store1, ARoot, _, undefined) ->
    lists:reverse(
        do_fold(Store1, fun(E, Acc) -> [E | Acc] end, [], [], ARoot)
    );

diff_to_list(T, Store1, ARoot, Store2, BRoot) ->
    APage = bondy_mst_store:get(Store1, ARoot),
    ALow = bondy_mst_page:low(APage),
    AEntries = bondy_mst_page:list(APage),
    ALevel = bondy_mst_page:level(APage),

    BPage = bondy_mst_store:get(Store2, BRoot),
    BEntries = bondy_mst_page:list(BPage),
    BLow = bondy_mst_page:low(BPage),
    BLevel = bondy_mst_page:level(BPage),

    case BLevel of
        ALevel ->
            diff_to_list_rec(T, Store1, ALow, AEntries, Store2, BLow, BEntries);

        BLevel when ALevel > BLevel ->
            diff_to_list_rec(T, Store1, ALow, AEntries, Store2, BRoot, []);

        BLevel when ALevel < BLevel ->
            diff_to_list_rec(T, Store1, ARoot, [], Store2, BLow, BEntries)
    end.



%% @private
diff_to_list_rec(T, Store1, ALow, [], Store2, BLow, []) ->
    diff_to_list(T, Store1, ALow, Store2, BLow);

diff_to_list_rec(T, Store1_0, ALow, [], Store2, BLow, [{K, _, R} | Rest2]) ->
    {ALowL, ALowH, Store1} = split(T, Store1_0, ALow, K),
    diff_to_list(T, Store1, ALowL, Store2, BLow) ++
    diff_to_list_rec(T, Store1, ALowH, [], Store2, R, Rest2);

diff_to_list_rec(T, Store1, ALow, [{K, V, R} | Rest1], Store2_0, BLow, []) ->
    {BLowL, BLowH, Store2} = split(T, Store2_0, BLow, K),
    diff_to_list(T, Store1, ALow, Store2, BLowL) ++
    [{K, V} | diff_to_list_rec(T, Store1, R, Rest1, Store2, BLowH, [])];

diff_to_list_rec(T, Store1_0, ALow, AEntries, Store2_0, BLow, BEntries) ->
    [{K1, V1, ARoot} | Rest1] = AEntries,
    [{K2, V2, BRoot} | Rest2] = BEntries,

    case compare(T, K1, K2) of
        lt ->
            {BLowL, BLowH, Store2} = split(T, Store2_0, BLow, K1),
            diff_to_list(T, Store1_0, ALow, Store2, BLowL)
            ++ [
                {K1, V1}
                | diff_to_list_rec(
                    T, Store1_0, ARoot, Rest1, Store2, BLowH, BEntries
                )
            ];

        gt ->
            {ALowL, ALowH, Store1} = split(T, Store1_0, ALow, K2),
            diff_to_list(T, Store1, ALowL, Store2_0, BLow)
            ++ diff_to_list_rec(T, Store1, ALowH, AEntries, Store2_0, BRoot, Rest2);

        eq ->
            L0 = diff_to_list_rec(T, Store1_0, ARoot, Rest1, Store2_0, BRoot, Rest2),

            case V1 == V2 of
                true ->
                    diff_to_list(T, Store1_0, ALow, Store2_0, BLow) ++ L0;

                false ->
                    L = [{K1, V1} | L0],
                    diff_to_list(T, Store1_0, ALow, Store2_0, BLow) ++ L

            end
    end.


%% @private
dump(Store, R) ->
    dump(Store, R, "").


%% @private
dump(_, undefined, _) ->
    ok;

dump(Store, Root, Space) ->
    Page = bondy_mst_store:get(Store, Root),
    Low = bondy_mst_page:low(Page),
    List = bondy_mst_page:list(Page),
    Level = bondy_mst_page:level(Page),

    io:format("~s~s (~p)~n", [Space, binary:encode_hex(Root), Level]),
    dump(Store, Low, Space ++ [$\s, $\s]),
    [
        begin
            io:format("~s- ~p => ~p~n", [Space, K, V]),
            dump(Store, R, Space ++ [$\s, $\s])
        end
        || {K, V, R} <- List
    ].



