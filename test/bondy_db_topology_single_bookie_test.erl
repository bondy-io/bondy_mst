%% =============================================================================
%% Unit tests for the single_bookie topology
%% (`bondy_db_topology_single_bookie`).
%%
%% Verified in isolation against the topology behaviour — no `bondy_db`
%% facade. Focuses on Bookie sharing, bucket composition, and the no-op
%% close_table contract.
%% =============================================================================

-module(bondy_db_topology_single_bookie_test).

-include_lib("eunit/include/eunit.hrl").

-define(MOD, bondy_db_topology_single_bookie).

%% =============================================================================
%% Test list
%% =============================================================================

topology_test_() ->
    {foreach,
        fun setup/0,
        fun cleanup/1,
        [
            fun init_with_valid_opts_starts_bookie/1,
            fun init_rejects_missing_sup/1,
            fun init_rejects_missing_dir/1,
            fun open_table_does_not_start_new_bookie/1,
            fun open_two_tables_share_one_bookie/1,
            fun route_bucket_is_realm_slash_entity/1,
            fun route_distinct_realms_get_distinct_buckets/1,
            fun route_distinct_entities_get_distinct_buckets/1,
            fun close_table_is_a_noop/1,
            fun shutdown_stops_bookie_and_supervisor/1,
            fun end_to_end_put_get_through_topology/1,
            fun bucket_isolation_across_realms/1
        ]}.

%% =============================================================================
%% Setup / teardown
%% =============================================================================

setup() ->
    process_flag(trap_exit, true),
    Dir = make_tempdir(),
    {ok, Sup} = bondy_db_leveled_sup:start_link(),
    {Sup, Dir}.

cleanup({Sup, Dir}) ->
    case is_process_alive(Sup) of
        true  -> bondy_db_leveled_sup:stop(Sup);
        false -> ok
    end,
    rmrf(Dir),
    ok.

%% =============================================================================
%% Tests
%% =============================================================================

init_with_valid_opts_starts_bookie({Sup, Dir}) ->
    fun() ->
        {ok, State} = ?MOD:init(my_db, #{sup => Sup, dir => Dir}),
        #{bookie := Bookie, sup := Sup, db_name := my_db} = State,
        ?assert(is_pid(Bookie)),
        ?assert(is_process_alive(Bookie))
    end.


init_rejects_missing_sup({_Sup, Dir}) ->
    fun() ->
        ?assertMatch(
            {error, {missing_required_opt, sup}},
            ?MOD:init(my_db, #{dir => Dir})
        )
    end.


init_rejects_missing_dir({Sup, _Dir}) ->
    fun() ->
        ?assertMatch(
            {error, {missing_required_opt, dir}},
            ?MOD:init(my_db, #{sup => Sup})
        )
    end.


open_table_does_not_start_new_bookie({Sup, Dir}) ->
    fun() ->
        {ok, S0} = ?MOD:init(my_db, #{sup => Sup, dir => Dir}),
        BookieBefore = maps:get(bookie, S0),
        {ok, T, _S1} = ?MOD:open_table(users, 8, #{}, S0),
        ?assertEqual(BookieBefore, maps:get(bookie, T))
    end.


open_two_tables_share_one_bookie({Sup, Dir}) ->
    fun() ->
        {ok, S0} = ?MOD:init(my_db, #{sup => Sup, dir => Dir}),
        {ok, T1, _} = ?MOD:open_table(users,  8, #{}, S0),
        {ok, T2, _} = ?MOD:open_table(tokens, 8, #{}, S0),
        ?assertEqual(maps:get(bookie, T1), maps:get(bookie, T2))
    end.


route_bucket_is_realm_slash_entity({Sup, Dir}) ->
    fun() ->
        {ok, S0} = ?MOD:init(my_db, #{sup => Sup, dir => Dir}),
        {ok, T,  _} = ?MOD:open_table(users, 8, #{}, S0),
        {ok, Adapter, #{bucket := Bucket}} =
            ?MOD:route(0, <<"realm-1">>, T),
        ?assertEqual(bondy_oplog_projection_leveled, Adapter),
        ?assertEqual(<<"realm-1/users">>, Bucket)
    end.


route_distinct_realms_get_distinct_buckets({Sup, Dir}) ->
    fun() ->
        {ok, S0} = ?MOD:init(my_db, #{sup => Sup, dir => Dir}),
        {ok, T,  _} = ?MOD:open_table(users, 8, #{}, S0),
        {ok, _, #{bucket := B1}} = ?MOD:route(0, <<"r1">>, T),
        {ok, _, #{bucket := B2}} = ?MOD:route(0, <<"r2">>, T),
        ?assertNotEqual(B1, B2)
    end.


route_distinct_entities_get_distinct_buckets({Sup, Dir}) ->
    fun() ->
        {ok, S0} = ?MOD:init(my_db, #{sup => Sup, dir => Dir}),
        {ok, Users,  _} = ?MOD:open_table(users,  8, #{}, S0),
        {ok, Tokens, _} = ?MOD:open_table(tokens, 8, #{}, S0),
        {ok, _, #{bucket := UB}} = ?MOD:route(0, <<"r1">>, Users),
        {ok, _, #{bucket := TB}} = ?MOD:route(0, <<"r1">>, Tokens),
        ?assertNotEqual(UB, TB)
    end.


close_table_is_a_noop({Sup, Dir}) ->
    fun() ->
        {ok, S0} = ?MOD:init(my_db, #{sup => Sup, dir => Dir}),
        Bookie = maps:get(bookie, S0),
        {ok, T, _} = ?MOD:open_table(users, 8, #{}, S0),
        {ok, _} = ?MOD:close_table(T, S0),
        %% The shared Bookie must survive close_table.
        ?assert(is_process_alive(Bookie))
    end.


shutdown_stops_bookie_and_supervisor({Sup, Dir}) ->
    fun() ->
        {ok, S0} = ?MOD:init(my_db, #{sup => Sup, dir => Dir}),
        Bookie = maps:get(bookie, S0),
        ok = ?MOD:shutdown(S0),
        wait_until_dead([Sup, Bookie], 5_000)
    end.


end_to_end_put_get_through_topology({Sup, Dir}) ->
    fun() ->
        {ok, S0} = ?MOD:init(my_db, #{sup => Sup, dir => Dir}),
        {ok, T,  _} = ?MOD:open_table(users, 8, #{}, S0),
        {ok, Adapter, Handle} = ?MOD:route(0, <<"realm-1">>, T),
        ok = Adapter:put_batch(Handle, [{<<"alice">>, <<"frame">>}]),
        ?assertEqual({ok, <<"frame">>}, Adapter:get(Handle, <<"alice">>))
    end.


bucket_isolation_across_realms({Sup, Dir}) ->
    fun() ->
        {ok, S0} = ?MOD:init(my_db, #{sup => Sup, dir => Dir}),
        {ok, T, _} = ?MOD:open_table(users, 8, #{}, S0),
        {ok, Adapter, H1} = ?MOD:route(0, <<"realm-1">>, T),
        {ok, _,       H2} = ?MOD:route(0, <<"realm-2">>, T),
        ok = Adapter:put_batch(H1, [{<<"alice">>, <<"v1">>}]),
        ?assertEqual({ok, <<"v1">>}, Adapter:get(H1, <<"alice">>)),
        %% realm-2 cannot see realm-1's alice — buckets isolate them.
        ?assertEqual(not_found, Adapter:get(H2, <<"alice">>))
    end.


%% =============================================================================
%% Helpers
%% =============================================================================

make_tempdir() ->
    Base = filename:join([
        "/tmp",
        "bondy_db_single_bookie_test",
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


wait_until_dead([], _Deadline) ->
    ok;
wait_until_dead([Pid | Rest], Deadline) when Deadline > 0 ->
    case is_process_alive(Pid) of
        false ->
            wait_until_dead(Rest, Deadline);
        true ->
            timer:sleep(50),
            wait_until_dead([Pid | Rest], Deadline - 50)
    end;
wait_until_dead([Pid | _], _) ->
    error({still_alive, Pid}).
