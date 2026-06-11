%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

%% Ephemeral ETS WAL (task #50). A fused ephemeral instance can opt into an
%% in-memory WAL backend (`wal_backend => mem`, `bondy_oplog_wal_mem`) that
%% drops the fsync from the ack path: events live in an ETS `ordered_set`, the
%% fused drain reads them via `bondy_oplog_wal_mem_reader` the instant they are
%% inserted (no durable-position gate). These tests prove the mem WAL is
%% actually wired (not silently falling back to disk), that single + bulk writes
%% round-trip through the producer → mem reader → fused drain → projection, and
%% that two mem-backed replicas still converge under `sync` exactly like the
%% disk-backed fused path — i.e. the reader swap changed throughput mechanics,
%% not semantics.

-module(bondy_db_fused_mem_wal_test).

-include_lib("eunit/include/eunit.hrl").

fused_mem_wal_test_() ->
    {setup, fun setup/0, fun cleanup/1, [
        {"mem backend is actually wired (not a disk fallback)",
            {timeout, 30, fun mem_backend_is_wired/0}},
        {"single + bulk writes round-trip through the mem WAL",
            {timeout, 30, fun bulk_writes_round_trip/0}},
        {"two mem-backed replicas converge via sync",
            {timeout, 30, fun mem_replicas_converge/0}}
    ]}.

setup() ->
    {ok, _} = application:ensure_all_started(bondy_mst),
    bondy_oplog_sync_scheduler:set_dispatch(undefined),
    bondy_oplog_gc_scheduler:set_trigger(undefined),
    ok.

cleanup(_) ->
    [bondy_oplog:stop_instance(I) || I <- bondy_oplog:list_instances()],
    ok.

%% =============================================================================
%% Tests
%% =============================================================================

%% The WAL child must be the in-memory writer. Assert via its diagnostic
%% `info/1` so a silent disk fallback (e.g. a broken gate) fails loudly here
%% rather than passing the functional tests on the disk path.
mem_backend_is_wired() ->
    {Db, T, Id} = open_fused_mem(fmw_wired),
    WalPid = bondy_oplog_registry:wal_pid(Id),
    ?assert(is_pid(WalPid)),
    Info = bondy_oplog_wal_mem:info(WalPid),
    ?assertEqual(mem, maps:get(backend, Info)),
    %% A single write advances the head Seq and is readable.
    H = bondy_db:tick(T),
    ok = bondy_db:apply(T, <<"r">>, <<"k">>, {set, H, <<"v">>}),
    ?assertEqual(<<"v">>, val(bondy_db:read(T, <<"r">>, <<"k">>))),
    ?assert(maps:get(head_seq, bondy_oplog_wal_mem:info(WalPid)) >= 1),
    ok = bondy_db:close(Db).

%% Many writes must all round-trip: producer → ETS → chunked mem reader →
%% collect aggregation → fused install → projection. Exercises the
%% multi-chunk reader path (N > one ets:select chunk) and the byte accounting.
bulk_writes_round_trip() ->
    {Db, T, Id} = open_fused_mem(fmw_bulk),
    N = 1500,
    [
        begin
            H = bondy_db:tick(T),
            ok = bondy_db:apply(
                T, <<"r">>, key(I), {set, H, val_for(I)}
            )
        end
     || I <- lists:seq(1, N)
    ],
    %% Every key reads back its value.
    [
        ?assertEqual(val_for(I), val(bondy_db:read(T, <<"r">>, key(I))))
     || I <- lists:seq(1, N)
    ],
    Info = bondy_oplog_wal_mem:info(bondy_oplog_registry:wal_pid(Id)),
    ?assert(maps:get(head_seq, Info) >= N),
    ?assert(maps:get(append_count, Info) >= N),
    ok = bondy_db:close(Db).

%% A writes k1, B writes k2; after a bidirectional sync both replicas answer
%% reads for both keys with identical MST roots — the mem reader feeds the same
%% inline replay the disk reader does, so convergence is unchanged.
mem_replicas_converge() ->
    {DbA, Ta, Ia} = open_fused_mem(fmw_conv_a),
    {DbB, Tb, Ib} = open_fused_mem(fmw_conv_b),
    Ha = bondy_db:tick(Ta),
    ok = bondy_db:apply(Ta, <<"r">>, <<"k1">>, {set, Ha, <<"va">>}),
    Hb = bondy_db:tick(Tb),
    ok = bondy_db:apply(Tb, <<"r">>, <<"k2">>, {set, Hb, <<"vb">>}),
    ok = wait_live(Ia, 1),
    ok = wait_live(Ib, 1),
    {ok, _} = bondy_oplog:sync(Ia, Ib),
    {ok, _} = bondy_oplog:sync(Ib, Ia),
    ?assertEqual(<<"va">>, val(bondy_db:read(Ta, <<"r">>, <<"k1">>))),
    ?assertEqual(<<"vb">>, val(bondy_db:read(Ta, <<"r">>, <<"k2">>))),
    ?assertEqual(<<"va">>, val(bondy_db:read(Tb, <<"r">>, <<"k1">>))),
    ?assertEqual(<<"vb">>, val(bondy_db:read(Tb, <<"r">>, <<"k2">>))),
    ?assertEqual(bondy_oplog:root_hash(Ia), bondy_oplog:root_hash(Ib)),
    ok = bondy_db:close(DbA),
    ok = bondy_db:close(DbB).

%% =============================================================================
%% Helpers
%% =============================================================================

open_fused_mem(Name) ->
    Origin = bondy_oplog_origin:new(),
    {ok, Db} = bondy_db:open(Name, #{
        topology => bondy_db_topology_memory,
        shard_count => 1,
        fold_module => lww_register,
        %% `wal_backend => mem` rides `oplog_instance_opts` through to both the
        %% supervisor (which swaps the WAL child) and the instance (which
        %% dispatches the drain reader). Gated on `fused` by the supervisor.
        oplog_instance_opts => #{origin => Origin, wal_backend => mem}
    }),
    {ok, T} = bondy_db:open_table(Db, items, #{fused => true}),
    {Db, T, instance_of(T)}.

instance_of(Table) ->
    #{0 := InstanceId} = maps:get(instance_ids, Table),
    InstanceId.

key(I) ->
    list_to_binary("k" ++ integer_to_list(I)).

val_for(I) ->
    list_to_binary("v" ++ integer_to_list(I)).

val({ok, V, _Hlc}) -> V.

wait_live(Id, N) ->
    wait_until(fun() -> live_size(Id) >= N end, 5000).

live_size(Id) ->
    case bondy_oplog_registry:live_size(Id) of
        undefined -> 0;
        N -> N
    end.

wait_until(_Pred, Remaining) when Remaining =< 0 ->
    error(timeout);
wait_until(Pred, Remaining) ->
    case Pred() of
        true ->
            ok;
        false ->
            timer:sleep(20),
            wait_until(Pred, Remaining - 20)
    end.
