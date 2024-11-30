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
            persistent_test,
            large_test
        ]},
        {rocksdb_store, [], [
            small_test,
            first_last_test,
            persistent_test,
            large_test
        ]},
        {leveled_store, [], [
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
    {ok, _} = application:ensure_all_started(bondy_mst),

    Fun = fun(Name) ->
        bondy_mst_store:new(bondy_mst_leveled_store, [{name, Name}])
    end,
    [{store_fun, Fun}] ++ Config;

init_per_group(rocksdb_store, Config) ->
    {ok, _} = application:ensure_all_started(bondy_mst),

    Fun = fun(Name) ->
        bondy_mst_store:new(bondy_mst_rocksdb_store, [{name, Name}])
    end,
    [{store_fun, Fun}] ++ Config.


end_per_group(_, _Config) ->
    catch file:del_dir_r("/tmp/bondy_mst/"),
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
    catch file:del_dir_r("/tmp/bondy_mst/"),
    ok.



small_test(Config) ->
    Fun = ?config(store_fun, Config),

    %% Test for basic MST operations
    A = lists:foldl(
        fun(N, Acc) -> ?MST:insert(Acc, N) end,
        ?MST:new(#{store => Fun(~"bondy_mst_a")}),
        lists:seq(1, 10)
    ),
    ?assertEqual([{1, 10}], ?ISET([K || {K, true} <- ?MST:to_list(A)])),

    B = lists:foldl(
        fun(N, Acc) -> ?MST:insert(Acc, N) end,
        ?MST:new(#{store => Fun(~"bondy_mst_b")}),
        lists:seq(5, 15)
    ),
    ?assertEqual([{5, 15}], ?ISET([K || {K, true} <- ?MST:to_list(B)])),

    Z = lists:foldl(
        fun(N, Acc) -> ?MST:insert(Acc, N) end,
        ?MST:new(#{store => Fun(~"bondy_mst_z")}),
        lists:seq(1, 15)
    ),
    ?assertEqual([{1, 15}], ?ISET([K || {K, true} <- ?MST:to_list(Z)])),

    C = ?MST:merge(A, B),
    D = ?MST:merge(B, A),

    ?assertNotEqual(undefined, ?MST:root(C)),
    ?assertNotEqual(undefined, ?MST:root(D)),

    ?assertEqual(?MST:root(C), ?MST:root(D)),
    ?assertEqual(?MST:root(C), ?MST:root(Z)),
    ?assertEqual(?MST:to_list(C), ?MST:to_list(Z)),

    case C =/= A of
        true ->
            %% Only true for map store
            DA = [K || {K, true} <- ?MST:diff_to_list(C, A)],
            ?assertEqual(
                ?ISET(lists:sort(lists:seq(11, 15))),
                ?ISET(lists:sort(DA))
            ),
            ?assertEqual(
                [],
                ?MST:diff_to_list(A, C)
            ),

            DB = [K || {K, true} <- ?MST:diff_to_list(C, B)],
            ?assertEqual(
                ?ISET(lists:sort(lists:seq(1, 4))),
                ?ISET(lists:sort(DB))
            ),

            ?assertEqual(
                [],
                ?MST:diff_to_list(B, C)
            ),

            DBA = [K || {K, _} <- ?MST:diff_to_list(B, A)],
            ?assertEqual(
                ?ISET(lists:seq(11, 15)),
                ?ISET(lists:sort(DBA))
            ),

            DAB = [K || {K, _} <- ?MST:diff_to_list(A, B)],
            ?assertEqual(
                ?ISET(lists:seq(1, 4)),
                ?ISET(lists:sort(DAB))
            );

        false ->
            ?assertEqual(A, C),
            ?assertEqual(D, B)
    end,


    ok = bondy_mst:delete(A),
    ok = bondy_mst:delete(B),
    ok = bondy_mst:delete(Z).


large_test(Config) ->
    Fun = ?config(store_fun, Config),

    %% Test for large MST operations
    ShuffledA = list_shuffle(lists:seq(1, 1000)),
    ShuffledB = list_shuffle(lists:seq(550, 1500)),
     A = lists:foldl(
        fun(N, Acc) -> ?MST:insert(Acc, N) end,
        ?MST:new(#{store => Fun(~"bondy_mst_a")}),
        ShuffledA
    ),
    B = lists:foldl(
        fun(N, Acc) -> ?MST:insert(Acc, N) end,
        ?MST:new(#{store => Fun(~"bondy_mst_b")}),
        ShuffledB
    ),
    Z = lists:foldl(
        fun(N, Acc) -> ?MST:insert(Acc, N) end,
        ?MST:new(#{store => Fun(~"bondy_mst_z")}),
        lists:seq(1, 1500)
    ),
    C = ?MST:merge(A, B),
    D = ?MST:merge(B, A),

    case C =/= A of
        true ->

            ?assertEqual(?MST:root(C), ?MST:root(D)),
            ?assertEqual(?MST:root(C), ?MST:root(Z)),

            FullList = [K || {K, _} <- ?MST:to_list(C)],
            ?assertEqual(?ISET(lists:seq(1, 1500)), ?ISET(lists:sort(FullList))),

            DCA = [K || {K, _} <- ?MST:diff_to_list(C, A)],
            ?assertEqual(?ISET(lists:seq(1001, 1500)), ?ISET(DCA)),
            DCB = [K || {K, _} <- ?MST:diff_to_list(C, B)],
            ?assertEqual(?ISET(lists:seq(1, 549)), ?ISET(DCB)),

            ?assertEqual([], ?MST:diff_to_list(A, C)),
            ?assertEqual([], ?MST:diff_to_list(B, C)),

            DBA = [K || {K, _} <- ?MST:diff_to_list(B, A)],
            ?assertEqual(?ISET(lists:seq(1001, 1500)), ?ISET(DBA)),
            DAB = [K || {K, _} <- ?MST:diff_to_list(A, B)],
            ?assertEqual(?ISET(lists:seq(1, 549)), ?ISET(DAB));

        false ->
            ?assertEqual(A, C),
            ?assertEqual(D, B)
    end,

    ok = bondy_mst:delete(A),
    ok = bondy_mst:delete(B),
    ok = bondy_mst:delete(Z).


first_last_test(Config) ->
    Fun = ?config(store_fun, Config),

    %% Test for basic MST operations
    A = lists:foldl(
        fun(N, Acc) -> ?MST:insert(Acc, N) end,
        ?MST:new(#{store => Fun(~"first_last_test")}),
        lists:seq(1, 10)
    ),
    ?assertEqual({1, true}, bondy_mst:first(A)),
    ?assertEqual({10, true}, bondy_mst:last(A)).

persistent_test(Config) ->
    Fun = ?config(store_fun, Config),

    T0 = ?MST:new(#{store => Fun(~"persistent_test")}),

    T1 = ?MST:insert(T0, 1),
    R1 = ?MST:root(T1),

    T2 = ?MST:insert(T1, 2),
    E2 = erlang:monotonic_time(),
    R2 = ?MST:root(T2),

    T3 = ?MST:insert(T2, 3),
    E3 = erlang:monotonic_time(),
    R3 = ?MST:root(T3),


    ?assertEqual([{1, true}], bondy_mst:to_list(T1, R1)),
    ?assertEqual([{1, true}, {2, true}], bondy_mst:to_list(T2, R2)),
    ?assertEqual([{1, true}, {2, true}, {3, true}], bondy_mst:to_list(T3, R3)),
    ?assertEqual(bondy_mst:to_list(T3, R3), bondy_mst:to_list(T3)),


    %% GC
    T4 = bondy_mst:gc(T3, E2),
    ?assertEqual([], bondy_mst:to_list(T4, R1)),
    ?assertEqual([{1, true}, {2, true}], bondy_mst:to_list(T4, R2)),

    T5 = bondy_mst:gc(T4, E3),
    ?assertEqual([], bondy_mst:to_list(T5, R2)),

    ?assertEqual([{1, true}, {2, true}, {3, true}], bondy_mst:to_list(T5, R3)),
    ?assertEqual(bondy_mst:to_list(T5, R3), bondy_mst:to_list(T5)).



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

