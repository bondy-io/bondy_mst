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
%% @doc A Merkle search tree.
%% @end
%% -----------------------------------------------------------------------------
-module(bondy_mst).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").

-record(bondy_mst, {
    store           ::  bondy_mst_store:t(),
    %% `comparator` is a compare function for keys
    comparator      ::  comparator(),
    %% `merger` is a function for merging two items that have the same key
    merger          ::  merger()
}).

-type t()           ::  #?MODULE{}.
-type opts()        ::  [opt()] | opts_map().
-type opt()         ::  {store, bondy_mst_store:t()}
                        | {merger, merger()}
                        | {comparator, comparator()}.
-type opts_map()    ::  #{
                            store => bondy_mst_store:t(),
                            merger => merger(),
                            comparator => comparator()
                        }.
-type comparator()  ::  fun((key(), key()) -> eq | lt | gt).
-type merger()      ::  fun((key(), value(), value()) -> value()).
-type key_range()   ::  {key(), key()}
                        | {first, key()}
                        | {key(), undefined}.
%% Fold
-type fold_fun()    ::  fun(({key(), value()}, any()) -> any()).
-type fold_opts()   ::  [fold_opt()].
-type fold_opt()    ::  {root, hash()}
                        | {first, key()}
                        | {match_spec, term()}
                        | {stop, key()}
                        | {keys_only, boolean()}
                        | {limit, pos_integer() | infinity}.


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
%% This structure can be used as a ser with only true keys or as a map if a
%% merge function is given.
%% @end
%% -----------------------------------------------------------------------------
-spec new() -> t().

new() ->
    new([]).


%% -----------------------------------------------------------------------------
%% @doc Create a new Merkle Search Tree.

%% This structure can be used as a ser with only true keys (default) or as a map
%% if a proper merger function is given.
%% == Options ==
%% * {store, bondy_mst_store:t()} - a stores.
%% Defaults to an instance of `bondy_mst_map_store'.
%% * {merger, merger()} - a merger function.
%% Defaults to the grow-only set merger function `merger/3'.
%% * {comparator, comparator()} - a key comparator function.
%% Defaults to `comparator/2'.
%% @end
%% -----------------------------------------------------------------------------
-spec new(opts()) -> t().

new(Opts) when is_list(Opts) ->
    new(maps:from_list(Opts));

new(Opts) when is_map(Opts) ->
    DefaultStore = fun() -> bondy_mst_store:new(bondy_mst_map_store, []) end,
    Store = get_option(store, Opts, DefaultStore),
    Comparator = get_option(comparator, Opts, fun comparator/2),
    Merger = get_option(merger, Opts, fun merger/3),

    #?MODULE{
        store = Store,
        comparator = Comparator,
        merger = Merger
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
-spec root(Tree :: t()) -> binary().

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
%% @doc Returns the value for key `Key'.
%% @end
%% -----------------------------------------------------------------------------
-spec get(T :: t(), Key :: key()) -> Value :: any().

get(#?MODULE{} = T, Key) ->
    get(T, Key, root(T)).


%% -----------------------------------------------------------------------------
%% @doc Returns the first entry.
%% @end
%% -----------------------------------------------------------------------------
-spec first(T :: t()) -> Value :: {key(), value()} | undefined.

first(#?MODULE{} = T) ->
    first(T, root(T)).


%% -----------------------------------------------------------------------------
%% @doc Returns the last entry.
%% @end
%% -----------------------------------------------------------------------------
-spec last(T :: t()) -> Value :: {key(), value()} | undefined.

last(#?MODULE{} = T) ->
    last(T, root(T)).



-spec get_range(T :: t(), Range :: key_range()) -> Value :: any().

get_range(#?MODULE{} = T, {From, To}) ->
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
-spec to_list(t(), hash()) -> [{key(), value()}].

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
%% @doc List all items.
%% @end
%% -----------------------------------------------------------------------------
-spec keys(t()) -> [{key(), value()}].

keys(#?MODULE{} = T) ->
    lists:reverse(fold(T, fun({K, _}, Acc) -> [K | Acc] end, [])).


%% -----------------------------------------------------------------------------
%% @doc
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
%% @doc Associates `Key' with value `Value' and inserts the association into
%% tree `Tree1'.
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
        Level = calc_level(Key),
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
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec merge(T1 :: t(), T2 :: t()) -> NewT1 :: t().

merge(#?MODULE{} = T1, #?MODULE{} = T2) ->
    merge(T1, T2, root(T2)).


%% -----------------------------------------------------------------------------
%% @doc
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
%% @doc Dump Merkle search tree structure.
%% @end
%% -----------------------------------------------------------------------------
-spec dump(t()) -> undefined.

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
%% @doc Returns the default comparator
%% @end
%% -----------------------------------------------------------------------------
comparator(A, B) when A < B -> lt;
comparator(A, B) when A > B -> gt;
comparator(A, B) when A == B -> eq.


%% -----------------------------------------------------------------------------
%% @private
%% @doc Returns the default merger, which assumes the MST is a Set (where all
%% values are `true').
%% @end
%% -----------------------------------------------------------------------------
merger(_Key, true, true) ->
    true.


%% @private
calc_level(Key) ->
    Hash = binary:encode_hex(bondy_mst_utils:hash(Key)),
    count_leading_zeroes(Hash, 0).

%% @private
count_leading_zeroes(<<"0", Rest/binary>>, Acc) ->
    count_leading_zeroes(Rest, Acc + 1);

count_leading_zeroes(_, Acc) ->
    Acc.


%% @private
compare(#?MODULE{comparator = Fun}, A, B) ->
    Fun(A, B).


%% @private
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
    case bondy_mst_page:list(Page) of
        [] ->
            undefined;

        L ->
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
    Page = get_page(Store0, Hash, T),
    [{K0, _, _} | _] = List0 = bondy_mst_page:list(Page),
    Level = bondy_mst_page:level(Page),
    Low = bondy_mst_page:low(Page),

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
get_page(Store, Hash, T) ->
    case bondy_mst_store:get(Store, Hash) of
        undefined ->
            bondy_mst_store:get(T#?MODULE.store, Hash);

        Page ->
            Page
    end.


%% @private
-spec merge_aux(
    T1 :: t(),
    T2 :: t(),
    T1Store :: bondy_mst_store:t(),
    T1Root :: hash(),
    T2Root :: hash()) -> NewT1 :: t().

merge_aux(_, _, Store0, R, R) ->
    {R, Store0};

merge_aux(_, _, Store0, R1, undefined) ->
    {R1, Store0};

merge_aux(_, T2, Store0, undefined, R2) ->
    Store = bondy_mst_store:copy(Store0, T2#?MODULE.store, R2),
    {R2, Store};

merge_aux(T1, T2, Store0, R1, R2) ->
    Page1 = bondy_mst_store:get(Store0, R1),
    Level1 = bondy_mst_page:level(Page1),
    Low1 = bondy_mst_page:low(Page1),
    List1 = bondy_mst_page:list(Page1),

    Page2 = get_page(Store0, R2, T2),
    Low2 = bondy_mst_page:low(Page2),
    List2 = bondy_mst_page:list(Page2),

    {Level, {Low, List, Store}} =
        case bondy_mst_page:level(Page2) of
            Level1 ->
                {
                    Level1,
                    merge_aux_rec(T1, T2, Store0, Low1, List1, Low2, List2)
                };
            Level2 when Level1 > Level2 ->
                {
                    Level1,
                    merge_aux_rec(T1, T2, Store0, Low1, List1, R2, [])
                };
            Level2 when Level1 < Level2 ->
                {
                    Level2,
                    merge_aux_rec(T1, T2, Store0, R1, [], Low2, List2)
                }
        end,
    NewPage = bondy_mst_page:new(Level, Low, List),
    bondy_mst_store:put(Store, NewPage).


%% @private
merge_aux_rec(T1, T2, Store0, Low1, [], Low2, []) ->
    {Hash, Store} = merge_aux(T1, T2, Store0, Low1, Low2),
    {Hash, [], Store};

merge_aux_rec(T1, T2, Store0, Low1, [], Low2, [{K, V, R} | Rest]) ->
    {Low1L, Low1H, Store1} = split(T1, Store0, Low1, K),
    {NewLow, Store2} = merge_aux(T1, T2, Store1, Low1L, Low2),
    {NewR, NewRest, Store} = merge_aux_rec(T1, T2, Store2, Low1H, [], R, Rest),
    {NewLow, [{K, V, NewR} | NewRest], Store};

merge_aux_rec(T1, T2, Store0, Low1, [{K, V, R} | Rest], Low2, []) ->
    {Low2L, Low2H, Store1} = split(T2, Store0, Low2, K),
    {NewLow, Store2} = merge_aux(T1, T2, Store1, Low1, Low2L),
    {NewR, NewRest, Store} = merge_aux_rec(
        T1, T2, Store2, R, Rest, Low2H, []
    ),
    {NewLow, [{K, V, NewR} | NewRest], Store};

merge_aux_rec(
    T1,
    T2,
    Store0,
    Low1,
    [{K1, V1, R1} | Rest1] = List1,
    Low2,
    [{K2, V2, R2} | Rest2] = List2) ->
    case compare(T1, K1, K2) of
        lt ->
            {Low2L, Low2H, Store1} = split(T2, Store0, Low2, K1),
            {NewLow, Store2} = merge_aux(T1, T2, Store1, Low1, Low2L),
            {NewR, NewRest, Store} = merge_aux_rec(
                T1, T2, Store2, R1, Rest1, Low2H, List2
            ),
            {NewLow, [{K1, V1, NewR} | NewRest], Store};
        gt ->
            {Low1L, Low1H, Store1} = split(T1, Store0, Low1, K2),
            {NewLow, Store2} = merge_aux(T1, T2, Store1, Low1L, Low2),
            {NewR, NewRest, Store} = merge_aux_rec(
                T1, T2, Store2, Low1H, List1, R2, Rest2
            ),
            {NewLow, [{K2, V2, NewR} | NewRest], Store};

        eq ->
            {NewLow, Store1} = merge_aux(T1, T2, Store0, Low1, Low2),
            NewV = merge_values(T1, K1, V1, V2),
            {NewR, NewRest, Store} = merge_aux_rec(
                T1, T2, Store1, R1, Rest1, R2, Rest2
            ),
            {NewLow, [{K1, NewV, NewR} | NewRest], Store}
    end.


%% @private
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

diff_to_list(_, Store1, R1, _, undefined) ->
    lists:reverse(
        do_fold(Store1, fun(E, Acc) -> [E | Acc] end, [], [], R1)
    );

diff_to_list(T, Store1, R1, Store2, R2) ->
    Page1 = bondy_mst_store:get(Store1, R1),
    Low1 = bondy_mst_page:low(Page1),
    List1 = bondy_mst_page:list(Page1),
    Level1 = bondy_mst_page:level(Page1),

    Page2 = bondy_mst_store:get(Store2, R2),
    List2 = bondy_mst_page:list(Page2),
    Low2 = bondy_mst_page:low(Page2),

    case bondy_mst_page:level(Page2) of
        Level1 ->
            diff_to_list_rec(T, Store1, Low1, List1, Store2, Low2, List2);

        Level2 when Level1 > Level2 ->
            diff_to_list_rec(T, Store1, Low1, List1, Store2, R2, []);

        Level2 when Level1 < Level2 ->
            diff_to_list_rec(T, Store1, R1, [], Store2, Low2, List2)
    end.



%% @private
diff_to_list_rec(T, Store1, Low1, [], Store2, Low2, []) ->
    diff_to_list(T, Store1, Low1, Store2, Low2);

diff_to_list_rec(T, Store1_0, Low1, [], Store2, Low2, [{K, _, R} | Rest2]) ->
    {Low1L, Low1H, Store1} = split(T, Store1_0, Low1, K),
    diff_to_list(T, Store1, Low1L, Store2, Low2) ++
    diff_to_list_rec(T, Store1, Low1H, [], Store2, R, Rest2);

diff_to_list_rec(T, Store1, Low1, [{K, V, R} | Rest1], Store2_0, Low2, []) ->
    {Low2L, Low2H, Store2} = split(T, Store2_0, Low2, K),
    diff_to_list(T, Store1, Low1, Store2, Low2L) ++
    [{K, V} | diff_to_list_rec(T, Store1, R, Rest1, Store2, Low2H, [])];

diff_to_list_rec(T, Store1_0, Low1, List1, Store2_0, Low2, List2) ->
    [{K1, V1, R1} | Rest1] = List1,
    [{K2, V2, R2} | Rest2] = List2,

    case compare(T, K1, K2) of
        lt ->
            {Low2L, Low2H, Store2} = split(T, Store2_0, Low2, K1),
            diff_to_list(T, Store1_0, Low1, Store2, Low2L)
            ++ [
                {K1, V1}
                | diff_to_list_rec(T, Store1_0, R1, Rest1, Store2, Low2H, List2)
            ];

        gt ->
            {Low1L, Low1H, Store1} = split(T, Store1_0, Low1, K2),
            diff_to_list(T, Store1, Low1L, Store2_0, Low2)
            ++ diff_to_list_rec(T, Store1, Low1H, List1, Store2_0, R2, Rest2);

        eq ->
            L0 = diff_to_list_rec(T, Store1_0, R1, Rest1, Store2_0, R2, Rest2),

            case V1 == V2 of
                true ->
                    diff_to_list(T, Store1_0, Low1, Store2_0, Low2) ++ L0;

                false ->
                    L = [{K1, V1} | L0],
                    diff_to_list(T, Store1_0, Low1, Store2_0, Low2) ++ L

            end
    end.


%% @private
dump(Store, R) ->
    dump(Store, R, "").


%% @private
dump(_, undefined, _) ->
    undefined;

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



