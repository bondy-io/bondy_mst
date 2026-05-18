%% =============================================================================
%% Integration tests for the `bondy_db` facade.
%%
%% The facade is fold-agnostic; the tests drive it with explicit
%% fold-shaped events so the verification covers the actual contract
%% (read-modify-write, HLC monotonicity, range over decoded states).
%% The same scenarios run twice — once against per_entity (T2) and once
%% against single_bookie — confirming the facade is topology-agnostic.
%% =============================================================================

-module(bondy_db_test).

-include_lib("eunit/include/eunit.hrl").

-define(FOLD, bondy_oplog_fold_lww_register).

%% =============================================================================
%% Test generators
%% =============================================================================

per_entity_test_() ->
    topology_suite(bondy_db_topology_per_entity).


single_bookie_test_() ->
    topology_suite(bondy_db_topology_single_bookie).


topology_suite(Topology) ->
    Tag = atom_to_list(Topology),
    {foreach,
        fun() -> setup(Topology) end,
        fun cleanup/1,
        [
            test("apply_then_read/" ++ Tag,        fun apply_then_read/1),
            test("read_missing/" ++ Tag,           fun read_missing/1),
            test("later_hlc_wins/" ++ Tag,         fun later_hlc_wins/1),
            test("earlier_hlc_is_rejected/" ++ Tag,
                 fun earlier_hlc_is_rejected/1),
            test("clear_then_read/" ++ Tag,        fun clear_then_read/1),
            test("clear_then_resurrect/" ++ Tag,   fun clear_then_resurrect/1),
            test("realm_isolation/" ++ Tag,        fun realm_isolation/1),
            test("range_returns_states/" ++ Tag,   fun range_returns_states/1),
            test("tick_is_monotonic/" ++ Tag,      fun tick_is_monotonic/1),
            test("open_table_requires_fold/" ++ Tag,
                 fun open_table_requires_fold_module/1),
            test("info/" ++ Tag,                   fun info_db_and_table/1)
        ]}.


test(Title, Fn) ->
    fun(Ctx) -> {Title, fun() -> Fn(Ctx) end} end.

%% =============================================================================
%% Setup / teardown
%% =============================================================================

setup(Topology) ->
    process_flag(trap_exit, true),
    %% The substrate's per-shard `bondy_db_core_registry` lives inside the
    %% `bondy_mst` application; without this the facade's `open_table/3`
    %% cannot register a shard.
    {ok, _} = application:ensure_all_started(bondy_mst),
    Dir = make_tempdir(),
    {ok, Sup} = bondy_db_leveled_sup:start_link(),
    {ok, Db} = bondy_db:open(my_db, #{
        topology      => Topology,
        topology_opts => #{sup => Sup, dir => Dir},
        shard_count   => 4,
        fold_module   => ?FOLD
    }),
    {Db, Sup, Dir}.


cleanup({Db, Sup, Dir}) ->
    _ = catch bondy_db:close(Db),
    case is_process_alive(Sup) of
        true  -> bondy_db_leveled_sup:stop(Sup);
        false -> ok
    end,
    rmrf(Dir),
    ok.

%% =============================================================================
%% Tests — every mutating test drives apply/4 with explicit fold events
%% so the facade's fold-agnosticism is visible in the test code.
%% =============================================================================

apply_then_read({Db, _Sup, _Dir}) ->
    {ok, T} = bondy_db:open_table(Db, users, #{}),
    H = bondy_db:tick(T),
    ok = bondy_db:apply(T, <<"r1">>, <<"alice">>, {set, H, <<"v1">>}),
    ?assertEqual({ok, {set, <<"v1">>, H}, H},
                 bondy_db:read(T, <<"r1">>, <<"alice">>)),
    ok = bondy_db:close_table(T).


read_missing({Db, _Sup, _Dir}) ->
    {ok, T} = bondy_db:open_table(Db, users, #{}),
    ?assertEqual(not_found, bondy_db:read(T, <<"r1">>, <<"nobody">>)),
    ok = bondy_db:close_table(T).


later_hlc_wins({Db, _Sup, _Dir}) ->
    {ok, T} = bondy_db:open_table(Db, users, #{}),
    H1 = bondy_db:tick(T),
    ok = bondy_db:apply(T, <<"r1">>, <<"alice">>, {set, H1, <<"first">>}),
    H2 = bondy_db:tick(T),
    ?assert(H2 > H1),
    ok = bondy_db:apply(T, <<"r1">>, <<"alice">>, {set, H2, <<"second">>}),
    ?assertEqual({ok, {set, <<"second">>, H2}, H2},
                 bondy_db:read(T, <<"r1">>, <<"alice">>)),
    ok = bondy_db:close_table(T).


earlier_hlc_is_rejected({Db, _Sup, _Dir}) ->
    %% LWW: an event with an HLC older than the current cell's HLC must
    %% leave the cell unchanged. Tests the read-modify-write contract
    %% routes events through fold:apply_event/2 (not blind overwrite).
    {ok, T} = bondy_db:open_table(Db, users, #{}),
    H2 = bondy_db:tick(T),
    ok = bondy_db:apply(T, <<"r1">>, <<"alice">>, {set, H2, <<"newer">>}),
    %% Replay a fabricated older event.
    H1 = H2 - 1,
    ok = bondy_db:apply(T, <<"r1">>, <<"alice">>, {set, H1, <<"older">>}),
    ?assertEqual({ok, {set, <<"newer">>, H2}, H2},
                 bondy_db:read(T, <<"r1">>, <<"alice">>)),
    ok = bondy_db:close_table(T).


clear_then_read({Db, _Sup, _Dir}) ->
    {ok, T} = bondy_db:open_table(Db, users, #{}),
    H1 = bondy_db:tick(T),
    ok = bondy_db:apply(T, <<"r1">>, <<"alice">>, {set, H1, <<"v">>}),
    H2 = bondy_db:tick(T),
    ok = bondy_db:apply(T, <<"r1">>, <<"alice">>, {clear, H2}),
    ?assertEqual({ok, {cleared, H2}, H2},
                 bondy_db:read(T, <<"r1">>, <<"alice">>)),
    ok = bondy_db:close_table(T).


clear_then_resurrect({Db, _Sup, _Dir}) ->
    %% LWW: a higher-HLC `set` after a `clear` re-populates the register.
    {ok, T} = bondy_db:open_table(Db, users, #{}),
    H1 = bondy_db:tick(T),
    ok = bondy_db:apply(T, <<"r1">>, <<"alice">>, {set, H1, <<"v1">>}),
    H2 = bondy_db:tick(T),
    ok = bondy_db:apply(T, <<"r1">>, <<"alice">>, {clear, H2}),
    H3 = bondy_db:tick(T),
    ok = bondy_db:apply(T, <<"r1">>, <<"alice">>, {set, H3, <<"v2">>}),
    ?assertEqual({ok, {set, <<"v2">>, H3}, H3},
                 bondy_db:read(T, <<"r1">>, <<"alice">>)),
    ok = bondy_db:close_table(T).


realm_isolation({Db, _Sup, _Dir}) ->
    {ok, T} = bondy_db:open_table(Db, users, #{}),
    H1 = bondy_db:tick(T),
    ok = bondy_db:apply(T, <<"r1">>, <<"alice">>, {set, H1, <<"v1">>}),
    H2 = bondy_db:tick(T),
    ok = bondy_db:apply(T, <<"r2">>, <<"alice">>, {set, H2, <<"v2">>}),
    ?assertEqual({ok, {set, <<"v1">>, H1}, H1},
                 bondy_db:read(T, <<"r1">>, <<"alice">>)),
    ?assertEqual({ok, {set, <<"v2">>, H2}, H2},
                 bondy_db:read(T, <<"r2">>, <<"alice">>)),
    ?assertEqual(not_found, bondy_db:read(T, <<"r3">>, <<"alice">>)),
    ok = bondy_db:close_table(T).


range_returns_states({Db, _Sup, _Dir}) ->
    %% range/5 must return decoded fold states (not user-level values).
    {ok, T} = bondy_db:open_table(Db, users, #{}),
    Keys = [list_to_binary("k" ++ integer_to_list(I))
            || I <- lists:seq(1, 20)],
    Written = lists:sort(Keys),
    lists:foreach(
        fun(K) ->
            H = bondy_db:tick(T),
            ok = bondy_db:apply(T, <<"r1">>, K,
                                {set, H, <<K/binary, "v">>})
        end,
        Keys
    ),
    Shard = erlang:phash2(hd(Keys), 4),
    {ok, Rows} = bondy_db:range(T, <<"r1">>, <<"k">>, <<"l">>,
                                #{shard => Shard, limit => 100}),
    Got = [K || {K, _State, _Hlc} <- Rows],
    %% Sorted ascending.
    ?assertEqual(lists:sort(Got), Got),
    %% Every returned key lies in [<<"k">>, <<"l">>).
    ?assert(lists:all(fun(K) -> K >= <<"k">> andalso K < <<"l">> end, Got)),
    %% Every returned key was written.
    ?assert(lists:all(fun(K) -> lists:member(K, Written) end, Got)),
    %% Every returned state is a {set, V, H} with V = <<K, "v">>.
    ?assert(lists:all(
        fun({K, {set, V, H}, Hlc}) ->
            V =:= <<K/binary, "v">>
                andalso is_integer(H)
                andalso Hlc =:= H
        end,
        Rows
    )),
    %% At least one row came back.
    ?assert(length(Got) >= 1),
    ok = bondy_db:close_table(T).


tick_is_monotonic({Db, _Sup, _Dir}) ->
    {ok, T} = bondy_db:open_table(Db, users, #{}),
    Hs = [bondy_db:tick(T) || _ <- lists:seq(1, 50)],
    ?assertEqual(lists:sort(Hs), Hs),
    %% No duplicates.
    ?assertEqual(length(Hs), sets:size(sets:from_list(Hs))),
    ok = bondy_db:close_table(T).


open_table_requires_fold_module({_Db, Sup, Dir}) ->
    {ok, Db2} = bondy_db:open(my_db2, #{
        topology      => bondy_db_topology_single_bookie,
        topology_opts => #{sup => Sup,
                           dir => filename:join(Dir, "no_fold")},
        shard_count   => 2
    }),
    ?assertMatch(
        {error, {missing_required_opt, fold_module}},
        bondy_db:open_table(Db2, users, #{})
    ),
    ok = bondy_db:close(Db2).


info_db_and_table({Db, _Sup, _Dir}) ->
    DbInfo = bondy_db:info(Db),
    ?assertMatch(#{kind := db, name := my_db}, DbInfo),
    {ok, T} = bondy_db:open_table(Db, users, #{}),
    TInfo = bondy_db:info(T),
    ?assertMatch(
        #{kind := table, db_name := my_db, entity_type := users,
          shard_count := 4, fold_module := ?FOLD},
        TInfo
    ),
    ok = bondy_db:close_table(T).


%% =============================================================================
%% Helpers
%% =============================================================================

make_tempdir() ->
    Base = filename:join([
        "/tmp",
        "bondy_db_test",
        integer_to_list(erlang:unique_integer([positive, monotonic]))
    ]),
    ok = filelib:ensure_dir(filename:join(Base, ".keep")),
    Base.


rmrf(Dir) ->
    case file:del_dir_r(Dir) of
        ok              -> ok;
        {error, enoent} -> ok;
        {error, _}      -> ok
    end.
