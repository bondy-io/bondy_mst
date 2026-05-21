%% =============================================================================
%% End-to-end test for AW-Map's logical-event resolution path:
%%
%%   bondy_db:apply(T, R, K, {remove_aw_key, MapKey})
%%     ↓
%%   maybe_resolve/4 detects logical event, calls applier
%%     ↓
%%   bondy_oplog_applier:resolve_logical_event/4
%%     ↓
%%   bondy_oplog_fold:resolve_event(aw_map, State, Event)
%%     ↓
%%   bondy_oplog_fold_aw_map:resolve_event/2 → {remove, MapKey, Dots}
%%     ↓
%%   WAL append of the resolved physical event
%%     ↓
%%   applier's drain loop projects the remove
%%
%% Confirms cross-replica convergence: the WAL stores the resolved
%% form, not the logical form, so replays / sync from peers see the
%% canonical event.
%% =============================================================================

-module(bondy_db_aw_map_e2e_test).

-include_lib("eunit/include/eunit.hrl").

-define(FOLD, bondy_oplog_fold_aw_map).

%% =============================================================================
%% Test generators
%% =============================================================================

aw_map_e2e_test_() ->
    {foreach,
        fun setup/0, fun cleanup/1,
        [
            test("put_apply_read_round_trip",
                 fun put_apply_read_round_trip/1),
            test("remove_aw_key_resolves_and_removes",
                 fun remove_aw_key_resolves_and_removes/1),
            test("remove_aw_key_on_absent_key_is_passthrough",
                 fun remove_aw_key_on_absent_key_is_passthrough/1),
            test("remove_aw_key_on_tombstoned_key_is_idempotent",
                 fun remove_aw_key_on_tombstoned_key_is_idempotent/1)
        ]
    }.


test(Title, Fn) ->
    fun(Ctx) -> {Title, {timeout, 30, fun() -> Fn(Ctx) end}} end.

%% =============================================================================
%% Setup / teardown
%% =============================================================================

setup() ->
    process_flag(trap_exit, true),
    {ok, _} = application:ensure_all_started(bondy_mst),
    Dir = make_tempdir(),
    DbName = list_to_atom("aw_map_e2e_" ++
                          integer_to_list(erlang:unique_integer([positive]))),
    {ok, Sup} = bondy_db_leveled_sup:start_link(),
    {ok, Db} = bondy_db:open(DbName, #{
        topology      => bondy_db_topology_single_bookie,
        topology_opts => #{sup => Sup, dir => Dir},
        shard_count   => 1,
        fold_module   => ?FOLD
    }),
    {Db, Sup, Dir}.


cleanup({Db, Sup, Dir}) ->
    _ = catch bondy_db:close(Db),
    _ = [catch bondy_oplog:stop_instance(I)
         || I <- bondy_oplog:list_instances()],
    case is_process_alive(Sup) of
        true  -> bondy_db_leveled_sup:stop(Sup);
        false -> ok
    end,
    rmrf(Dir),
    ok.

%% =============================================================================
%% Tests
%% =============================================================================

put_apply_read_round_trip({Db, _Sup, _Dir}) ->
    {ok, T} = bondy_db:open_table(Db, users, #{}),
    Realm = <<"r1">>,
    Key = <<"alice">>,
    H = bondy_db:tick(T),
    %% Put a single AW-Map entry mapped to an LWW-Register sub-state.
    ok = bondy_db:aw_put(T, Realm, Key, <<"email">>,
                         {lww_register, {set, <<"alice@example.com">>, H}}),
    {ok, Value, _Hlc} = bondy_db:read(T, Realm, Key),
    ?assertEqual(#{<<"email">> => <<"alice@example.com">>}, Value),
    ok = bondy_db:close_table(T).


remove_aw_key_resolves_and_removes({Db, _Sup, _Dir}) ->
    {ok, T} = bondy_db:open_table(Db, users, #{}),
    Realm = <<"r1">>,
    Key = <<"alice">>,
    H = bondy_db:tick(T),
    %% Two distinct AW-Map keys.
    ok = bondy_db:aw_put(T, Realm, Key, <<"email">>,
                         {lww_register, {set, <<"alice@example.com">>, H}}),
    H2 = bondy_db:tick(T),
    ok = bondy_db:aw_put(T, Realm, Key, <<"phone">>,
                         {lww_register, {set, <<"+1-555-1234">>, H2}}),
    %% Verify both present.
    {ok, V0, _} = bondy_db:read(T, Realm, Key),
    ?assertEqual(2, map_size(V0)),
    %% Logical remove: server-side resolution looks up dots.
    ok = bondy_db:aw_remove(T, Realm, Key, <<"email">>),
    %% Read shows only the remaining key.
    {ok, V1, _} = bondy_db:read(T, Realm, Key),
    ?assertEqual(#{<<"phone">> => <<"+1-555-1234">>}, V1),
    ok = bondy_db:close_table(T).


remove_aw_key_on_absent_key_is_passthrough({Db, _Sup, _Dir}) ->
    {ok, T} = bondy_db:open_table(Db, users, #{}),
    Realm = <<"r1">>,
    Key = <<"alice">>,
    %% Cell doesn't exist yet. Removing an AW-Map key in a never-
    %% created cell must be a no-op (passthrough), not a crash.
    ok = bondy_db:aw_remove(T, Realm, Key, <<"email">>),
    ?assertEqual(not_found, bondy_db:read(T, Realm, Key)),

    %% Create the cell with one AW-Map key, then try to remove a
    %% different (absent) key — also passthrough.
    H = bondy_db:tick(T),
    ok = bondy_db:aw_put(T, Realm, Key, <<"email">>,
                         {lww_register, {set, <<"e@x">>, H}}),
    ok = bondy_db:aw_remove(T, Realm, Key, <<"phone">>),
    {ok, V, _} = bondy_db:read(T, Realm, Key),
    ?assertEqual(#{<<"email">> => <<"e@x">>}, V),
    ok = bondy_db:close_table(T).


remove_aw_key_on_tombstoned_key_is_idempotent({Db, _Sup, _Dir}) ->
    {ok, T} = bondy_db:open_table(Db, users, #{}),
    Realm = <<"r1">>,
    Key = <<"alice">>,
    H = bondy_db:tick(T),
    ok = bondy_db:aw_put(T, Realm, Key, <<"email">>,
                         {lww_register, {set, <<"e@x">>, H}}),
    %% First remove tombstones the AW-Map key.
    ok = bondy_db:aw_remove(T, Realm, Key, <<"email">>),
    {ok, V0, _} = bondy_db:read(T, Realm, Key),
    ?assertEqual(#{}, V0),
    %% Second remove on the now-tombstoned key is passthrough — must
    %% not crash and must leave state unchanged.
    ok = bondy_db:aw_remove(T, Realm, Key, <<"email">>),
    {ok, V1, _} = bondy_db:read(T, Realm, Key),
    ?assertEqual(#{}, V1),
    ok = bondy_db:close_table(T).

%% =============================================================================
%% Helpers
%% =============================================================================

make_tempdir() ->
    Base = filename:join("/tmp",
        "bondy_mst_aw_map_e2e_" ++ integer_to_list(erlang:unique_integer([positive]))),
    ok = filelib:ensure_dir(filename:join(Base, "x")),
    Base.


rmrf(Dir) ->
    _ = os:cmd("rm -rf " ++ Dir),
    ok.
