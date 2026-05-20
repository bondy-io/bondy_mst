%% =============================================================================
%% End-to-end validation for the production wiring:
%%
%%     bondy_db (facade)
%%       ↓
%%     per-shard bondy_oplog instance
%%       ├─ WAL (bondy_oplog_wal)
%%       ├─ MST snapshot store  → bondy_mst_pack_store (persistent)
%%       └─ projection adapter   → bondy_oplog_projection_leveled
%%
%% Mirrors the scenarios in `bondy_db_multi_shard_e2e_test` so any
%% drift between the ETS-MST baseline and the pack-MST + leveled
%% wiring surfaces side-by-side. The exhaustive multi-shard / multi-
%% realm / concurrent-writer coverage already lives there; this suite
%% concentrates on the things the pack-store path actually changes:
%% reopen-recovery of MST state, integration with the leveled
%% projection, and that the new shape compiles cleanly through
%% `bondy_db`'s `oplog_instance_opts` plumbing.
%% =============================================================================

-module(bondy_db_pack_leveled_e2e_test).

-include_lib("eunit/include/eunit.hrl").

-define(FOLD, bondy_oplog_fold_lww_register).
-define(SHARDS, 4).
-define(KEYS, 32).
-define(DB, mst_pack_leveled_e2e_db).

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
            test("put_read_round_trip/" ++ Tag,
                 fun put_read_round_trip/1),
            test("multi_shard_fanout/" ++ Tag,
                 fun multi_shard_fanout/1),
            test("concurrent_writers/" ++ Tag,
                 fun concurrent_writers/1),
            test("mst_state_persists_across_close_reopen/" ++ Tag,
                 fun mst_state_persists_across_close_reopen/1)
        ]}.


test(Title, Fn) ->
    fun(Ctx) -> {Title, {timeout, 60, fun() -> Fn(Ctx) end}} end.

%% =============================================================================
%% Setup / teardown
%% =============================================================================

setup(Topology) ->
    process_flag(trap_exit, true),
    {ok, _} = application:ensure_all_started(bondy_mst),
    LeveledDir = make_tempdir("leveled"),
    PackDir = make_tempdir("pack"),
    {ok, Sup} = bondy_db_leveled_sup:start_link(),
    {ok, Db} = bondy_db:open(?DB, #{
        topology      => Topology,
        topology_opts => #{sup => Sup, dir => LeveledDir},
        shard_count   => ?SHARDS,
        fold_module   => ?FOLD,
        %% This is the production wiring under test: route every per-
        %% shard `bondy_oplog` instance to the MST pack-store backend,
        %% rooted under `PackDir`. The leveled projection store is
        %% provisioned via the topology above.
        oplog_instance_opts => #{
            backend      => bondy_mst_pack_store,
            storage_path => unicode:characters_to_binary(PackDir)
        }
    }),
    {Topology, Db, Sup, LeveledDir, PackDir}.


cleanup({_T, Db, Sup, LeveledDir, PackDir}) ->
    _ = catch bondy_db:close(Db),
    _ = [catch bondy_oplog:stop_instance(I)
         || I <- bondy_oplog:list_instances()],
    case is_process_alive(Sup) of
        true  -> bondy_db_leveled_sup:stop(Sup);
        false -> ok
    end,
    rmrf(LeveledDir),
    rmrf(PackDir),
    rmrf(wal_dir_for_this_db()),
    ok.

%% =============================================================================
%% Tests
%% =============================================================================

put_read_round_trip({_Topo, Db, _Sup, _LDir, _PDir}) ->
    %% Smoke test: one apply, one read, every layer of the stack
    %% touched at least once.
    {ok, T} = bondy_db:open_table(Db, users, #{}),
    Realm = <<"r1">>,
    Key = <<"alice">>,
    H = bondy_db:tick(T),
    V = <<"alice@example.com">>,
    ok = bondy_db:apply(T, Realm, Key, {set, H, V}),
    ?assertEqual({ok, {set, V, H}, H}, bondy_db:read(T, Realm, Key)),
    ok = bondy_db:close_table(T).


multi_shard_fanout({Topology, Db, _Sup, _LDir, _PDir}) ->
    %% Apply ?KEYS keys and confirm they fan out across all shards and
    %% all round-trip correctly. Mirrors the canonical multi-shard
    %% scenario but on the pack-MST + leveled wiring.
    {ok, T} = bondy_db:open_table(Db, users, #{}),
    Realm = <<"r1">>,
    Keys = test_keys(?KEYS),
    Writes = lists:map(
        fun(K) ->
            H = bondy_db:tick(T),
            V = <<K/binary, "-v">>,
            ok = bondy_db:apply(T, Realm, K, {set, H, V}),
            {K, V, H}
        end,
        Keys
    ),
    lists:foreach(
        fun({K, V, H}) ->
            ?assertEqual({ok, {set, V, H}, H}, bondy_db:read(T, Realm, K))
        end,
        Writes
    ),
    Bucket = bucket_for(Topology, users, Realm),
    Used = lists:foldl(
        fun(K, Acc) ->
            sets:add_element(erlang:phash2({Bucket, K}, ?SHARDS), Acc)
        end,
        sets:new([{version, 2}]),
        Keys
    ),
    ?assertEqual(?SHARDS, sets:size(Used)),
    ok = bondy_db:close_table(T).


concurrent_writers({_Topo, Db, _Sup, _LDir, _PDir}) ->
    %% 4 writers × 16 keys each, disjoint key prefixes (so no LWW
    %% interference). After every writer has returned, every key must
    %% be readable — proves WAL+applier serialisation under load with
    %% the pack-MST + leveled wiring.
    {ok, T} = bondy_db:open_table(Db, users, #{}),
    Realm = <<"r1">>,
    Writers = 4,
    PerWriter = 16,
    Self = self(),
    _ = [spawn_link(fun() ->
            Writes = [begin
                K = <<"w", (integer_to_binary(W))/binary,
                      "-k", (integer_to_binary(I))/binary>>,
                H = bondy_db:tick(T),
                V = <<K/binary, "-v">>,
                ok = bondy_db:apply(T, Realm, K, {set, H, V}),
                {K, V, H}
            end || I <- lists:seq(1, PerWriter)],
            Self ! {done, W, Writes}
        end) || W <- lists:seq(1, Writers)],
    All = collect_writers(Writers, []),
    ?assertEqual(Writers * PerWriter, length(All)),
    lists:foreach(
        fun({K, V, H}) ->
            ?assertEqual({ok, {set, V, H}, H}, bondy_db:read(T, Realm, K))
        end,
        All
    ),
    ok = bondy_db:close_table(T).


mst_state_persists_across_close_reopen({Topology, Db, _Sup, LDir, PDir}) ->
    %% Pack-store-specific: the MST snapshot store is persistent, so
    %% closing the oplog instance and reopening must surface the
    %% prior root + every reachable page. The leveled projection
    %% provides the user-visible KV state; this test additionally
    %% verifies that the underlying MST recovers from disk by
    %% reading the same keys back via the same fold module.
    {ok, T0} = bondy_db:open_table(Db, users, #{}),
    Realm = <<"r1">>,
    Keys = test_keys(?KEYS),
    Writes = lists:map(
        fun(K) ->
            H = bondy_db:tick(T0),
            V = <<K/binary, "-pre-reopen">>,
            ok = bondy_db:apply(T0, Realm, K, {set, H, V}),
            {K, V, H}
        end,
        Keys
    ),
    %% Tear down the table + DB. `bondy_db:close/1` stops the leveled
    %% supervisor (and every Bookie under it), so we also stop every
    %% running oplog instance — they cache projection-adapter handles
    %% pointing at the just-killed Bookies. The on-disk state (the
    %% leveled journal/ledger and the pack-store manifests + sealed
    %% packs) survives, which is what the reopen below depends on.
    ok = bondy_db:close_table(T0),
    ok = bondy_db:close(Db),
    _ = [catch bondy_oplog:stop_instance(I)
         || I <- bondy_oplog:list_instances()],

    %% Reopen with a fresh leveled supervisor over the same on-disk
    %% dirs. Each Bookie is restarted against its prior journal +
    %% ledger; the pack store reopens its manifest + sealed packs.
    {ok, Sup1} = bondy_db_leveled_sup:start_link(),
    {ok, Db1} = bondy_db:open(?DB, #{
        topology      => Topology,
        topology_opts => #{sup => Sup1, dir => LDir},
        shard_count   => ?SHARDS,
        fold_module   => ?FOLD,
        oplog_instance_opts => #{
            backend      => bondy_mst_pack_store,
            storage_path => unicode:characters_to_binary(PDir)
        }
    }),
    {ok, T1} = bondy_db:open_table(Db1, users, #{}),

    try
        lists:foreach(
            fun({K, V, H}) ->
                ?assertEqual({ok, {set, V, H}, H},
                             bondy_db:read(T1, Realm, K))
            end,
            Writes
        )
    after
        _ = catch bondy_db:close_table(T1),
        _ = catch bondy_db:close(Db1),
        _ = [catch bondy_oplog:stop_instance(I)
             || I <- bondy_oplog:list_instances()],
        case is_process_alive(Sup1) of
            true  -> bondy_db_leveled_sup:stop(Sup1);
            false -> ok
        end
    end.

%% =============================================================================
%% Helpers
%% =============================================================================

collect_writers(0, Acc) ->
    Acc;
collect_writers(N, Acc) ->
    receive
        {done, _W, Writes} ->
            collect_writers(N - 1, Writes ++ Acc)
    after 60000 ->
        error({timeout_waiting_for_writers, N})
    end.


test_keys(N) ->
    [<<"key-", (integer_to_binary(I))/binary>> || I <- lists:seq(1, N)].


bucket_for(bondy_db_topology_per_entity, _ET, Realm) ->
    Realm;
bucket_for(bondy_db_topology_single_bookie, ET, Realm) ->
    <<Realm/binary, "/", (atom_to_binary(ET, utf8))/binary>>.


make_tempdir(Prefix) ->
    Base = filename:join([
        "/tmp",
        "bondy_db_pack_leveled_e2e",
        Prefix,
        integer_to_list(erlang:unique_integer([positive, monotonic]))
    ]),
    ok = filelib:ensure_dir(filename:join(Base, ".keep")),
    Base.


wal_dir_for_this_db() ->
    filename:join([
        "/tmp", "bondy_oplog_wal", os:getpid(), atom_to_list(?DB)
    ]).


rmrf(Dir) ->
    case file:del_dir_r(Dir) of
        ok              -> ok;
        {error, enoent} -> ok;
        {error, _}      -> ok
    end.
