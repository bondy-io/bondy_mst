-module(bondy_mst_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(MST, bondy_mst).

-define(ISET(L), interval_sets:from_list(L)).

-compile(export_all).

%% All test cases to be run

all() ->
    [
        {group, local_store, []},
        {group, ets_store, []},
        {group, leveled_store, []},
        {group, rocksdb_store, []}
    ].

groups() ->
    [
        {local_store, [], [
            small_test,
            first_last_test,
            large_test
        ]},
        {ets_store, [], [
            small_test,
            first_last_test,
            large_test
        ]},
        {leveled_store, [], [
            small_test,
            first_last_test,
            large_test
        ]},
        {rocksdb_store, [], [
            small_test,
            first_last_test,
            large_test
        ]}
    ].


init_per_group(local_store, Config) ->
    Fun = fun(_) -> bondy_mst_store:new(bondy_mst_map_store, []) end,
    [{store_fun, Fun}] ++ Config;

init_per_group(ets_store, Config) ->
    Fun = fun(Name) ->
        bondy_mst_store:new(bondy_mst_ets_store, [{name, Name}])
    end,
    [{store_fun, Fun}] ++ Config;


init_per_group(leveled_store, Config) ->
    _ = file:delete("/tmp/bondy_mst/leveled"),
    {ok, _} = application:ensure_all_started(bondy_mst),

    Fun = fun(Name) ->
        bondy_mst_store:new(bondy_mst_leveled_store, [{name, Name}])
    end,
    [{store_fun, Fun}] ++ Config;

init_per_group(rocksdb_store, Config) ->
    _ = file:delete("/tmp/bondy_mst/rocksdb"),
    {ok, _} = application:ensure_all_started(bondy_mst),

    Fun = fun(Name) ->
        bondy_mst_store:new(bondy_mst_rocksdb_store, [{name, Name}])
    end,
    [{store_fun, Fun}] ++ Config.


end_per_group(_, _Config) ->
    ok.


%% Setup and teardown functions

%% Called once before any test cases are run
init_per_suite(Config) ->
    %% You can add any setup you need here
    Config.

%% Called once after all test cases have been run
end_per_suite(_Config) ->
    ok.

%% Called before each test case
init_per_testcase(_TestCase, Config) ->
    Config.

%% Called after each test case
end_per_testcase(_TestCase, _Config) ->
    ok.



small_test(Config) ->
    Fun = ?config(store_fun, Config),

    %% Test for basic MST operations
    A = lists:foldl(
        fun(N, Acc) -> ?MST:insert(Acc, N) end,
        ?MST:new(#{store => Fun(bondy_mst_a)}),
        lists:seq(1, 10)
    ),
    B = lists:foldl(
        fun(N, Acc) -> ?MST:insert(Acc, N) end,
        ?MST:new(#{store => Fun(bondy_mst_b)}),
        lists:seq(5, 15)
    ),
    Z = lists:foldl(
        fun(N, Acc) -> ?MST:insert(Acc, N) end,
        ?MST:new(#{store => Fun(bondy_mst_z)}),
        lists:seq(1, 15)
    ),
    C = ?MST:merge(A, B),
    D = ?MST:merge(B, A),

    ?assertEqual(?MST:root(C), ?MST:root(D)),
    ?assertEqual(?MST:root(C), ?MST:root(Z)),

    ?MST:dump(C),

    DA = [K || {K, _} <- ?MST:diff_to_list(C, A)],
    ?assertEqual(lists:sort(DA), lists:sort(lists:seq(11, 15))),
    DB = [K || {K, _} <- ?MST:diff_to_list(C, B)],
    ?assertEqual(lists:sort(DB), lists:sort(lists:seq(1, 4))),

    ?assertEqual([], ?MST:diff_to_list(A, C)),
    ?assertEqual([], ?MST:diff_to_list(B, C)),

    DBA = [K || {K, _} <- ?MST:diff_to_list(B, A)],
    ?assertEqual(lists:seq(11, 15), lists:sort(DBA)),
    DAB = [K || {K, _} <- ?MST:diff_to_list(A, B)],
    ?assertEqual(lists:seq(1, 4), lists:sort(DAB)),

    ok = bondy_mst:delete(A),
    ok = bondy_mst:delete(B),
    ok = bondy_mst:delete(Z).


large_test(Config) ->
    Fun = ?config(store_fun, Config),

    %% Test for large MST operations
    ShuffledA = list_shuffle(lists:seq(1, 1000)),
    ShuffledB = list_shuffle(lists:seq(550, 15_000)),
     A = lists:foldl(
        fun(N, Acc) -> ?MST:insert(Acc, N) end,
        ?MST:new(#{store => Fun(bondy_mst_a)}),
        ShuffledA
    ),
    B = lists:foldl(
        fun(N, Acc) -> ?MST:insert(Acc, N) end,
        ?MST:new(#{store => Fun(bondy_mst_b)}),
        ShuffledB
    ),
    Z = lists:foldl(
        fun(N, Acc) -> ?MST:insert(Acc, N) end,
        ?MST:new(#{store => Fun(bondy_mst_z)}),
        lists:seq(1, 15_000)
    ),
    C = ?MST:merge(A, B),
    D = ?MST:merge(B, A),

    ?assertEqual(?MST:root(C), ?MST:root(D)),
    ?assertEqual(?MST:root(C), ?MST:root(Z)),

    FullList = [K || {K, _} <- ?MST:to_list(C)],
    ?assertEqual(?ISET(lists:seq(1, 15_000)), ?ISET(lists:sort(FullList))),

    DCA = [K || {K, _} <- ?MST:diff_to_list(C, A)],
    ?assertEqual(?ISET(lists:seq(1001, 15_000)), ?ISET(DCA)),
    DCB = [K || {K, _} <- ?MST:diff_to_list(C, B)],
    ?assertEqual(?ISET(lists:seq(1, 549)), ?ISET(DCB)),

    ?assertEqual([], ?MST:diff_to_list(A, C)),
    ?assertEqual([], ?MST:diff_to_list(B, C)),

    DBA = [K || {K, _} <- ?MST:diff_to_list(B, A)],
    ?assertEqual(?ISET(lists:seq(1001, 15_000)), ?ISET(DBA)),
    DAB = [K || {K, _} <- ?MST:diff_to_list(A, B)],
    ?assertEqual(?ISET(lists:seq(1, 549)), ?ISET(DAB)),

    ok = bondy_mst:delete(A),
    ok = bondy_mst:delete(B),
    ok = bondy_mst:delete(Z).


first_last_test(Config) ->
    Fun = ?config(store_fun, Config),

    %% Test for basic MST operations
    A = lists:foldl(
        fun(N, Acc) -> ?MST:insert(Acc, N) end,
        ?MST:new(#{store => Fun(bondy_mst_a)}),
        lists:seq(1, 10)
    ),
    ?assertEqual({1, true}, bondy_mst:first(A)),
    ?assertEqual({10, true}, bondy_mst:last(A)).


concurrent_test(Config) ->
    Fun = ?config(store_fun, Config),

    %% Test for large MST operations
    ShuffledA = list_shuffle(lists:seq(1, 1000)),
    ShuffledB = list_shuffle(lists:seq(550, 15_000)),
     A = lists:foldl(
        fun(N, Acc) ->
            spawn(fun() -> ?MST:insert(Acc, N) end),
            Acc
        end,
        ?MST:new(#{store => Fun(bondy_mst_a)}),
        ShuffledA
    ),
    B = lists:foldl(
        fun(N, Acc) ->
            spawn(fun() -> ?MST:insert(Acc, N) end),
            Acc
        end,
        ?MST:new(#{store => Fun(bondy_mst_b)}),
        ShuffledB
    ),
    Z = lists:foldl(
        fun(N, Acc) ->
            spawn(fun() -> ?MST:insert(Acc, N) end),
            Acc
        end,
        ?MST:new(#{store => Fun(bondy_mst_z)}),
        lists:seq(1, 15_000)
    ),
    C = ?MST:merge(A, B),
    D = ?MST:merge(B, A),

    ?assertEqual(?MST:root(C), ?MST:root(D)),
    ?assertEqual(?MST:root(C), ?MST:root(Z)),

    FullList = [K || {K, _} <- ?MST:to_list(C)],
    ?assertEqual(?ISET(lists:seq(1, 15_000)), ?ISET(lists:sort(FullList))),

    DCA = [K || {K, _} <- ?MST:diff_to_list(C, A)],
    ?assertEqual(?ISET(lists:seq(1001, 15_000)), ?ISET(DCA)),
    DCB = [K || {K, _} <- ?MST:diff_to_list(C, B)],
    ?assertEqual(?ISET(lists:seq(1, 549)), ?ISET(DCB)),

    ?assertEqual([], ?MST:diff_to_list(A, C)),
    ?assertEqual([], ?MST:diff_to_list(B, C)),

    DBA = [K || {K, _} <- ?MST:diff_to_list(B, A)],
    ?assertEqual(?ISET(lists:seq(1001, 15_000)), ?ISET(DBA)),
    DAB = [K || {K, _} <- ?MST:diff_to_list(A, B)],
    ?assertEqual(?ISET(lists:seq(1, 549)), ?ISET(DAB)),

    ok = bondy_mst:delete(A),
    ok = bondy_mst:delete(B),
    ok = bondy_mst:delete(Z).


%% =============================================================================
%% PRIVATE
%% =============================================================================


%% @private
randomize(1, List) ->
    randomize(List);

randomize(T, List) ->
    lists:foldl(
        fun(_E, Acc) -> randomize(Acc) end,
        randomize(List),
        lists:seq(1, (T - 1))).


%% @private
randomize(List) ->
    D = lists:map(fun(A) -> {rand:uniform(), A} end, List),
    {_, D1} = lists:unzip(lists:keysort(1, D)),
    D1.

%% -----------------------------------------------------------------------------
%% @doc
%% From https://erlangcentral.org/wiki/index.php/RandomShuffle
%% @end
%% -----------------------------------------------------------------------------
list_shuffle([]) ->
    [];

list_shuffle(List) ->
    %% Determine the log n portion then randomize the list.
    randomize(round(math:log(length(List)) + 0.5), List).

