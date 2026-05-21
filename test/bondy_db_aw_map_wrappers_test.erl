%% =============================================================================
%% Unit tests for bondy_db's AW-Map convenience wrappers
%% (aw_put/5, aw_apply/5, aw_remove/4).
%%
%% Pins:
%% - wrappers produce identical effects to the raw apply/4 calls;
%% - aw_apply revives a tombstoned key (start fresh from
%%   initial_value + sub-event);
%% - aw_remove passthrough returns ok (absent / wrong map key).
%%
%% SubFold mismatch crashes are pinned at the fold-smoke level
%% (bondy_oplog_fold_aw_map_test); asserting them through the bondy_db
%% facade is brittle because the applier crashes asynchronously while
%% the WAL append (and thus apply/4's return) completes synchronously
%% beforehand.
%% =============================================================================

-module(bondy_db_aw_map_wrappers_test).

-include_lib("eunit/include/eunit.hrl").

-define(FOLD, bondy_oplog_fold_aw_map).

%% =============================================================================
%% Test generators
%% =============================================================================

aw_map_wrappers_test_() ->
    {foreach,
        fun setup/0, fun cleanup/1,
        [
            test("aw_put_matches_raw_apply",
                 fun aw_put_matches_raw_apply/1),
            test("aw_apply_revives_after_remove",
                 fun aw_apply_revives_after_remove/1),
            test("aw_remove_passthrough_returns_ok",
                 fun aw_remove_passthrough_returns_ok/1)
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
    DbName = list_to_atom("aw_map_wrap_" ++
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

aw_put_matches_raw_apply({Db, _Sup, _Dir}) ->
    {ok, T} = bondy_db:open_table(Db, users, #{}),
    Realm = <<"r">>,
    KeyA = <<"raw">>,
    KeyB = <<"wrap">>,
    H = bondy_db:tick(T),
    %% Raw path
    ok = bondy_db:apply(T, Realm, KeyA,
                        {put, <<"email">>, lww_register,
                         {set, <<"e@x">>, H}}),
    %% Wrapper path on a different cell
    H2 = bondy_db:tick(T),
    ok = bondy_db:aw_put(T, Realm, KeyB, <<"email">>,
                         {lww_register, {set, <<"e@x">>, H2}}),
    {ok, VA, _} = bondy_db:read(T, Realm, KeyA),
    {ok, VB, _} = bondy_db:read(T, Realm, KeyB),
    ?assertEqual(#{<<"email">> => <<"e@x">>}, VA),
    ?assertEqual(#{<<"email">> => <<"e@x">>}, VB),
    ok = bondy_db:close_table(T).


aw_apply_revives_after_remove({Db, _Sup, _Dir}) ->
    {ok, T} = bondy_db:open_table(Db, users, #{}),
    Realm = <<"r">>,
    Key = <<"alice">>,
    H1 = bondy_db:tick(T),
    ok = bondy_db:aw_put(T, Realm, Key, <<"email">>,
                         {lww_register, {set, <<"a@x">>, H1}}),
    ok = bondy_db:aw_remove(T, Realm, Key, <<"email">>),
    {ok, V0, _} = bondy_db:read(T, Realm, Key),
    ?assertEqual(#{}, V0),
    %% aw_apply onto a tombstoned key revives with initial_value +
    %% sub-event (LWW absorbs `set` regardless of init).
    H2 = bondy_db:tick(T),
    %% lww_register event shape is {set, H, V} (Hlc first), distinct
    %% from state shape {set, V, H}.
    ok = bondy_db:aw_apply(T, Realm, Key, <<"email">>,
                           {lww_register, {set, H2, <<"b@x">>}}),
    {ok, V1, _} = bondy_db:read(T, Realm, Key),
    ?assertEqual(#{<<"email">> => <<"b@x">>}, V1),
    ok = bondy_db:close_table(T).


aw_remove_passthrough_returns_ok({Db, _Sup, _Dir}) ->
    {ok, T} = bondy_db:open_table(Db, users, #{}),
    Realm = <<"r">>,
    Key = <<"none">>,
    %% Cell entirely absent — passthrough.
    ?assertEqual(ok, bondy_db:aw_remove(T, Realm, Key, <<"x">>)),
    %% Cell exists with one key; remove a different (absent) map key.
    H = bondy_db:tick(T),
    ok = bondy_db:aw_put(T, Realm, Key, <<"a">>,
                         {lww_register, {set, <<"v">>, H}}),
    ?assertEqual(ok, bondy_db:aw_remove(T, Realm, Key, <<"b">>)),
    {ok, V, _} = bondy_db:read(T, Realm, Key),
    ?assertEqual(#{<<"a">> => <<"v">>}, V),
    ok = bondy_db:close_table(T).


%% =============================================================================
%% Helpers
%% =============================================================================

make_tempdir() ->
    Base = filename:join("/tmp",
        "bondy_mst_aw_map_wrap_" ++ integer_to_list(erlang:unique_integer([positive]))),
    ok = filelib:ensure_dir(filename:join(Base, "x")),
    Base.


rmrf(Dir) ->
    _ = os:cmd("rm -rf " ++ Dir),
    ok.
