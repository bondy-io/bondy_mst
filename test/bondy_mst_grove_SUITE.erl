-module(bondy_mst_grove_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

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
            online_sync,
            anti_entropy_fwd,
            online_sync_complex
        ]},
        {ets_store, [], [
            online_sync,
            anti_entropy_fwd,
            online_sync_complex
        ]},
        {rocksdb_store, [], [
            online_sync,
            anti_entropy_fwd,
            online_sync_complex
        ]},
        {leveled_store, [], [
            online_sync,
            anti_entropy_fwd,
            online_sync_complex
        ]}
    ].


init_per_group(local_store, Config) ->
    {ok, _} = application:ensure_all_started(bondy_mst),
    [{store_type, bondy_mst_map_store}] ++ Config;

init_per_group(ets_store, Config) ->
    {ok, _} = application:ensure_all_started(bondy_mst),
    [{store_type, bondy_mst_map_store}] ++ Config;


init_per_group(leveled_store, Config) ->
    {ok, _} = application:ensure_all_started(bondy_mst),
    [{store_type, bondy_mst_leveled_store}] ++ Config;

init_per_group(rocksdb_store, Config) ->
    {ok, _} = application:ensure_all_started(bondy_mst),
    [{store_type, bondy_mst_rocksdb_store}] ++ Config.


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
    _ = [catch gen_server:stop(Peer) || Peer <- bondy_mst_test_grove:peers()],
    catch file:del_dir_r("/tmp/bondy_mst/"),
    ok.



online_sync(Config) ->
    StoreType = ?config(store_type, Config),
    {ok, Peers} = bondy_mst_test_grove:start_all(bondy_mst_map_store),

    [Peer1, Peer2, Peer3] = Peers,

    ?assert(
        lists:all(
            fun(Peer) ->
                erlang:is_process_alive(erlang:whereis(Peer))
                andalso pong == gen_server:call(Peer, ping)
            end,
            Peers
        )
    ),

    _ = [gen_server:call(Peer1, {put, X}) || X <- suffled_seq(1, 1000)],
    E1 = ?ISET([{1, 1000}]),
    ?assertEqual(
        E1,
        ?ISET([K || {K, true} <- gen_server:call(Peer1, list)]),
        "Peer1 should have all the elements we put"
    ),
     ?assertEqual(
        E1,
        ?ISET([K || {K, true} <- gen_server:call(Peer2, list)]),
        "Peer2 should have all the elements via replication"
    ),
    ?assertEqual(
        E1,
        ?ISET([K || {K, true} <- gen_server:call(Peer3, list)]),
        "Peer3 should have all the elements via replication"
    ),

    _ = [gen_server:call(Peer2, {put, X}) || X <- suffled_seq(1001, 2000)],
    _ = [gen_server:call(Peer3, {put, X}) || X <- suffled_seq(2001, 3000)],

    E2 = ?ISET([{1, 3000}]),
    ?assertEqual(
        E2,
        ?ISET([K || {K, true} <- gen_server:call(Peer1, list)]),
        "Peer1 should have all the elements via replication"
    ),
     ?assertEqual(
        E2,
        ?ISET([K || {K, true} <- gen_server:call(Peer2, list)]),
        "Peer2 should have all the elements via replication"
    ),
    ?assertEqual(
        E2,
        ?ISET([K || {K, true} <- gen_server:call(Peer3, list)]),
        "Peer3 should have all the elements via replication"
    ),

    _ = [gen_server:call(Peer1, {put, X}) || X <- suffled_seq(1001, 2000)],
    _ = [gen_server:call(Peer2, {put, X}) || X <- suffled_seq(2001, 3000)],
    _ = [gen_server:call(Peer3, {put, X}) || X <- suffled_seq(1, 1000)],
    ?assertEqual(
        E2,
        ?ISET([K || {K, true} <- gen_server:call(Peer1, list)]),
        "Peer1 should have all the elements via replication"
    ),
     ?assertEqual(
        E2,
        ?ISET([K || {K, true} <- gen_server:call(Peer2, list)]),
        "Peer2 should have all the elements via replication"
    ),
    ?assertEqual(
        E2,
        ?ISET([K || {K, true} <- gen_server:call(Peer3, list)]),
        "Peer3 should have all the elements via replication"
    ),

    ?assertEqual(
        gen_server:call(Peer1, root),
        gen_server:call(Peer2, root)
    ),

    ?assertEqual(
        gen_server:call(Peer2, root),
        gen_server:call(Peer3, root)
    ),

    ok.

online_sync_complex(Config) ->
    StoreType = ?config(store_type, Config),
    {ok, Peers} = bondy_mst_test_grove:start_all(bondy_mst_map_store),
    [Peer1, Peer2, Peer3] = Peers,

    L = [
        #{id => X, pid => self(), timestamp => erlang:monotonic_time()}
        || X <- lists:seq(1, 1000)
    ],

    _ = [gen_server:call(Peer1, {put, X}) || X <- list_shuffle(L)],

    L1 = [K || {K, true} <- gen_server:call(Peer1, list)],
    L2 = [K || {K, true} <- gen_server:call(Peer2, list)],
    L3 = [K || {K, true} <- gen_server:call(Peer3, list)],

    ?assertEqual(L, L1),
    ?assertEqual(L1, L2),
    ?assertEqual(L2, L3),
    ok.


anti_entropy_fwd(Config) ->
    StoreType = ?config(store_type, Config),
    [Peer1 | RestPeers] = bondy_mst_test_grove:peers(),
    {ok, [Peer1]} = bondy_mst_test_grove:start(bondy_mst_map_store, [Peer1]),

    L = [
        #{id => X, pid => self(), timestamp => erlang:monotonic_time()}
        || X <- lists:seq(1, 1000)
    ],

    _ = [gen_server:call(Peer1, {put, X}) || X <- list_shuffle(L)],

    L1 = [K || {K, true} <- gen_server:call(Peer1, list)],
    ?assertEqual(L, L1),

    %% Start peers 2 and 3
    {ok, RestPeers} = bondy_mst_test_grove:start(bondy_mst_map_store, RestPeers),
    [Peer2, Peer3] = RestPeers,

    %% Trigger sync Peer1 -> [Peer2, Peer3]
    ok = gen_server:call(Peer1, {trigger, Peer2}),
    ok = gen_server:call(Peer1, {trigger, Peer3}),

    timer:sleep(5000),

    L2 = [K || {K, true} <- gen_server:call(Peer2, list)],
    L3 = [K || {K, true} <- gen_server:call(Peer3, list)],

    ?assertEqual(L1, L2),
    ?assertEqual(L2, L3),
    ok.



%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
suffled_seq(N, M) ->
    list_shuffle(lists:seq(N, M)).


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

