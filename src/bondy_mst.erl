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
%% ===========================================================================



-module(bondy_mst).
-moduledoc """
A Merkle search tree.

A node of the tree is:

```
{
  Level,
  NodeHash | undefined,
  [
    { ItemLowBound, NodeHash | undefined },
    { ItemLowBound, NodeHash | undefined },
    ...
  }
}
```
""".

-include("bondy_mst.hrl").

-record(bondy_mst, {
    store           ::  bondy_mst_store:t(),
    root            ::  optional(any()),
    %% `comparator` is a compare function for keys
    comparator      ::  comparator(),
    %% `merger` is a function for merging two items that have the same key
    merger          ::  merger()
}).

-type t()           ::  #bondy_mst{}.
-type opts()        ::  list() | map().
-type key()         ::  any().
-type value()       ::  any().
-type comparator()  ::  fun((key(), key()) -> eq | lt | gt).
-type merger()      ::  fun((value(), value()) -> value()).

-export_type([t/0]).
-export_type([opts/0]).
-export_type([key/0]).
-export_type([value/0]).
-export_type([comparator/0]).
-export_type([merger/0]).

-export([new/0]).
-export([new/1]).
-export([root/1]).
-export([store/1]).
-export([insert/2]).
-export([merge/2]).
-export([get/2]).
-export([last/3]).
-export([to_list/1]).
-export([diff_to_list/2]).
-export([dump/1]).



%% =============================================================================
%% API
%% =============================================================================



-doc """
Create a new Merkle Search Tree.

This structure can be used as a ser with only true keys or as a map if a
merge function is given.
""".
-spec new() -> t().

new() ->
    new([]).


-doc """
Create a new Merkle Search Tree.

This structure can be used as a ser with only true keys or as a map if a
merge function is given.
""".
-spec new(opts()) -> t().

new(Opts) when is_list(Opts) ->
    new(maps:from_list(Opts));

new(Opts) when is_map(Opts) ->
    DefaultStore = fun() -> bondy_mst_store:new(bondy_mst_local_store, []) end,
    Store = get_option(store, Opts, DefaultStore),
    Root = get_option(root, Opts, undefined),
    Comparator = get_option(comparator, Opts, fun comparator/2),
    Merger = get_option(merger, Opts, fun merger/2),

    #?MODULE{
        store = Store,
        root = Root,
        comparator = Comparator,
        merger = Merger
    }.


-doc """
Returns the tree's root hash.
""".
-spec root(t()) -> binary().

root(#?MODULE{root = Val}) ->
    Val.


-doc """
Returns the tree's store.
""".
-spec store(t()) -> bondy_mst_store:t().

store(#?MODULE{store = Val}) ->
    Val.


-doc """
Returns the value for key `Key`.
""".
-spec get(T :: t(), Key :: key()) -> Value :: any().

get(#?MODULE{root = R} = T, Key) ->
    get(T, Key, R).


-doc """
Get the last `N` items of the tree, or the last `N` items strictly before given
upper bound `TopBound` if non `undefined`.
""".
last(#?MODULE{root = R} = T, TopBound, N) ->
    last(T, TopBound, N, R).


-doc """
""".
-spec insert(t(), key()) -> t().

insert(#?MODULE{} = T, Key) ->
    insert(T, Key, true).


-doc """
""".
-spec insert(t(), key(), value()) -> t().

insert(#?MODULE{} = T, Key, Value) ->
    Level = calc_level(Key),
    {Hash, Store} = insert_at(T, Key, Value, Level),
    T#?MODULE{root = Hash, store = Store}.


-doc """
""".
-spec merge(T1 :: t(), T2 :: t()) -> NewT1 :: t().

merge(#?MODULE{} = T1, #?MODULE{} = T2) ->
    {Root, Store} = merge_aux(
        T1, T2, T1#?MODULE.store, T1#?MODULE.root, T2#?MODULE.root
    ),
    T1#?MODULE{store = Store, root = Root}.


-doc """
List all items.
""".
-spec to_list(t()) -> list().

to_list(#?MODULE{store = Store, root = R}) ->
    to_list(Store, R).


-doc """

""".
-spec diff_to_list(t(), t()) -> list().

diff_to_list(#?MODULE{} = T1, #?MODULE{} = T2) ->
    diff_to_list(
        T1,
        T1#?MODULE.store,
        T1#?MODULE.root,
        T2#?MODULE.store,
        T2#?MODULE.root
    ).


-doc """
Dump Merkle search tree structure.
""".
-spec dump(t()) -> undefined.

dump(#?MODULE{store = Store, root = R}) ->
    dump(Store, R, <<>>).


%% =============================================================================
%% PRIVATE: OPTIONS VALIDATION & DEFAULTS
%% =============================================================================

%% priv
get_option(store, #{store := Store}, _) ->
    bondy_mst_store:is_type(Store) orelse error(badarg),
    Store;

get_option(store, _, Default) ->
    apply_default(Default);

get_option(root, #{root := Root}, _) ->
    Root;

get_option(root, _, Default) ->
    apply_default(Default);

get_option(comparator, #{comparator := Fun}, _) when is_function(Fun, 2) ->
    Fun;

get_option(comparator, _, Default) ->
    apply_default(Default);

get_option(merger, #{merger := Fun}, _) when is_function(Fun, 2) ->
    Fun;

get_option(merger, _, Default) ->
    apply_default(Default).


apply_default(Default) when is_function(Default, 0) ->
    Default();

apply_default(Default) ->
    Default.


-doc "Returns the default comparator".
comparator(A, B) when A < B -> lt;
comparator(A, B) when A > B -> gt;
comparator(A, B) when A == B -> eq.

-doc """
Returns the default merger, which assumes the MST is a Set (where all values
are `true`).
""".
merger(true, true) -> true.



%% =============================================================================
%% PRIVATE
%% =============================================================================



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
merge(#?MODULE{merger = Fun}, A, B) ->
    Fun(A, B).


%% @private
-spec get(T :: t(), Key :: key(), Root :: binary() | undefined) ->
    Value :: any().

get(#?MODULE{}, _, undefined) ->
    undefined;

get(#?MODULE{} = T, Key, Root) ->
    Page = bondy_mst_store:get(T#?MODULE.store, Root),
    Low = bondy_mst_page:low(Page),
    List = bondy_mst_page:list(Page),
    do_get(T, Key, Low, List).


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


%% @private
last(#?MODULE{}, _, _, undefined) ->
    [];

last(#?MODULE{} = T, TopBound, N, Root) ->
    case bondy_mst_store:get(T#?MODULE.store, Root) of
        undefined ->
            [];
        Page ->
            Low = bondy_mst_page:low(Page),
            List = bondy_mst_page:list(Page),
            do_last(T, TopBound, N, Low, List)
    end.


%% @private
do_last(T, TopBound, N, Low, []) ->
    last(T, TopBound, N, Low);

do_last(T, TopBound, N, Low, [{K, V, Low2} | Rest]) ->
    case TopBound == undefined orelse compare(T, TopBound, K) == gt of
        true ->
            Items0 = do_last(T, TopBound, N, Low2, Rest),
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
                    last(T, TopBound, N - Cnt, Low) ++ Items;
                false ->
                    Items
            end;
        false ->
            last(T, TopBound, N, Low)
    end.


%% @private
insert_at(T, Key, Value, Level) ->
    insert_at(T, Key, Value, Level, T#?MODULE.store, T#?MODULE.root).


%% @private
insert_at(_T, Key, Value, Level, Store, undefined) ->
    NewPage = bondy_mst_page:new(Level, undefined, [{Key, Value, undefined}]),
    bondy_mst_store:put(Store, NewPage);


insert_at(T, Key, Value, Level, Store0, Root) ->
    Page = bondy_mst_store:get(Store0, Root),
    [First | _] = bondy_mst_page:list(Page),

    case bondy_mst_page:level(Page) of
        PageLevel when PageLevel < Level ->
            insert_at_higher_level(T, Key, Value, Level, Root, Store0);

        PageLevel when PageLevel == Level ->
            Store = bondy_mst_store:free(Store0, Root),
            insert_at_same_level(T, Key, Value, Level, First, Store, Page);

        PageLevel when PageLevel > Level ->
            Store = bondy_mst_store:free(Store0, Root),
            insert_at_lower_level(T, Key, Value, Level, First, Store, Page)
    end.


%% @private
insert_at_higher_level(T, Key, Value, Level, Root, Store0) ->
    {Low, High, Store} = split(T, Store0, Root, Key),
    NewPage = bondy_mst_page:new(Level, Low, [{Key, Value, High}]),
    bondy_mst_store:put(Store, NewPage).


%% @private
insert_at_same_level(T, Key, Value, Level, {K0, _, _}, Store0, Page) ->
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
            {List1, Store} = insert_after_first(T, Key, Value, Store0, List0),
            NewPage = bondy_mst_page:new(PageLevel, Low, List1),
            bondy_mst_store:put(Store, NewPage)
    end.


%% @private
insert_at_lower_level(T, Key, Value, Level, {K0, _, _}, Store0, Page) ->
    List0 = bondy_mst_page:list(Page),
    Low0 = bondy_mst_page:low(Page),
    PageLevel = bondy_mst_page:level(Page),

    case compare(T, Key, K0) of
        lt ->
            {Low, Store} = insert_at(T, Key, Value, Level, Store0, Low0),
            NewPage = bondy_mst_page:new(PageLevel, Low, List0),
            bondy_mst_store:put(Store, NewPage);

        gt ->
            {List, Store} = insert_sub_after_first(
                T, Key, Value, Store0, Level, List0
            ),
            NewPage = bondy_mst_page:new(PageLevel, Low0, List),
            bondy_mst_store:put(Store, NewPage)
    end.


%% @private
insert_after_first(T, Key, Value, Store0, [{K1, V1, R1}]) ->
    case compare(T, K1, Key) of
        eq ->
            List = [{K1, merge(T, V1, Value), R1}],
            {List, Store0};

        lt ->
            {R1A, R1B, Store} = split(T, Store0, R1, Key),
            List = [{K1, V1, R1A}, {Key, Value, R1B}],
            {List, Store}
    end;

insert_after_first(T, Key, Value, Store0, [First, Second | Rest0]) ->
    {K1, V1, R1} = First,
    {K2, _, _} = Second,

    case compare(T, K1, Key) of
        eq ->
            List = [{K1, merge(T, V1, Value), R1}, Second | Rest0],
            {List, Store0};

        lt ->
            case compare(T, K2, Key) of
                gt ->
                    {R1A, R1B, Store} = split(T, Store0, R1, Key),
                    List = [{K1, V1, R1A}, {Key, Value, R1B}, Second | Rest0],
                    {List, Store};

                _ ->
                    {Rest, Store} = insert_after_first(
                        T, Key, Value, Store0, [Second | Rest0]
                    ),
                    List = [First | Rest],
                    {List, Store}

            end
    end.


%% @private
insert_sub_after_first(T, Key, Value, Store0, Level, [{K1, V1, R1}]) ->
    case compare(T, K1, Key) of
        eq ->
            error(inconsistency);

        _ ->
            {R, Store} = insert_at(T, Key, Value, Level, Store0, R1),
            List = [{K1, V1, R}],
            {List, Store}
    end;


insert_sub_after_first(T, Key, Value, Store0, Level, [First, Second | Rest0]) ->
    {K1, V1, R1} = First,
    {K2, _, _} = Second,

    case compare(T, K1, Key) of
        eq ->
            error(inconsistency);

        _ ->
            case compare(T, Key, K2) of
                lt ->
                    {R, Store} = insert_at(T, Key, Value, Level, Store0, R1),
                    List = [{K1, V1, R}, Second | Rest0],
                    {List, Store};
                _ ->
                    {Rest, Store} = insert_sub_after_first(
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

    Store1 = bondy_mst_store:free(Store0, Hash),

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
            NewV = merge(T1, V1, V2),
            {NewR, NewRest, Store} = merge_aux_rec(
                T1, T2, Store1, R1, Rest1, R2, Rest2
            ),
            {NewLow, [{K1, NewV, NewR} | NewRest], Store}
    end.


%% @private
to_list(_, undefined) ->
    [];

to_list(Store, Root) ->
    Page = bondy_mst_store:get(Store, Root),
    Low = bondy_mst_page:low(Page),
    List = bondy_mst_page:list(Page),
    L1 = to_list(Store, Low),
    Acc = lists:flatmap(
        fun({K, V, R}) -> [{K, V} | to_list(Store, R)] end,
        List
    ),
    L1 ++ Acc.


%% @private
diff_to_list(_, _, R, _, R) ->
    [];

diff_to_list(_, _, undefined, _, _) ->
    [];

diff_to_list(_, Store1, R1, _, undefined) ->
    to_list(Store1, R1);

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



dump(_, undefined, _) ->
    undefined;

dump(Store, Root, Bin) ->
    Page = bondy_mst_store:get(Store, Root),
    Low = bondy_mst_page:low(Page),
    List = bondy_mst_page:list(Page),
    Level = bondy_mst_page:level(Page),

    IOList = [Bin, binary:encode_hex(Root), $(, integer_to_binary(Level)],
    io:format("~s~n", [IOList]),
    dump(Store, Low, [Bin, "  "]),
    [
        begin
            io:format("~s- ~p => ~p~n", [Bin, K, V]),
            dump(Store, R, [Bin, "  "])
        end
        || {K, V, R} <- List
    ].



