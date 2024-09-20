-module(bondy_mst_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(MST, bondy_mst).

-compile(export_all).

%% All test cases to be run
all() -> 
    [
        basic_test,
        large_test
    ].

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


basic_test(_Config) ->
    %% Test for basic MST operations
    A = lists:foldl(fun(N, Acc) -> ?MST:insert(Acc, N) end, ?MST:new(), lists:seq(1, 10)),
    B = lists:foldl(fun(N, Acc) -> ?MST:insert(Acc, N) end, ?MST:new(), lists:seq(5, 15)),
    Z = lists:foldl(fun(N, Acc) -> ?MST:insert(Acc, N) end, ?MST:new(), lists:seq(1, 15)),
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
    ?assertEqual(lists:seq(1, 4), lists:sort(DAB)).

large_test(_Config) ->
    %% Test for large MST operations
    ShuffledA = list_shuffle(lists:seq(1, 1000)),
    ShuffledB = list_shuffle(lists:seq(550, 1500)),
    A = lists:foldl(fun(N, Acc) -> ?MST:insert(Acc, N) end, ?MST:new(), ShuffledA),
    B = lists:foldl(fun(N, Acc) -> ?MST:insert(Acc, N) end, ?MST:new(), ShuffledB),
    Z = lists:foldl(fun(N, Acc) -> ?MST:insert(Acc, N) end, ?MST:new(), lists:seq(1, 1500)),
    C = ?MST:merge(A, B),
    D = ?MST:merge(B, A),

    ?assertEqual(?MST:root(C), ?MST:root(D)),
    ?assertEqual(?MST:root(C), ?MST:root(Z)),

    FullList = [K || {K, _} <- ?MST:to_list(C)],
    ?assertEqual(lists:seq(1, 1500), lists:sort(FullList)),

    DCA = [K || {K, _} <- ?MST:diff_to_list(C, A)],
    ?assertEqual(lists:seq(1001, 1500), DCA),
    DCB = [K || {K, _} <- ?MST:diff_to_list(C, B)],
    ?assertEqual(lists:seq(1, 549), DCB),

    ?assertEqual([], ?MST:diff_to_list(A, C)),
    ?assertEqual([], ?MST:diff_to_list(B, C)),

    DBA = [K || {K, _} <- ?MST:diff_to_list(B, A)],
    ?assertEqual(lists:seq(1001, 1500), DBA),
    DAB = [K || {K, _} <- ?MST:diff_to_list(A, B)],
    ?assertEqual(lists:seq(1, 549), DAB).


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

