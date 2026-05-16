%% Tests for the per-instance MST owner (Stage 2 after second course
%% correction). Instance ids are binaries; lifecycle goes through the
%% library façade.

-module(bondy_oplog_instance_test).

-include_lib("eunit/include/eunit.hrl").
-include("bondy_oplog.hrl").

setup() ->
    {ok, _} = application:ensure_all_started(bondy_mst),
    ok.

cleanup(_) ->
    [
        bondy_oplog:stop_instance(I)
     || I <- bondy_oplog:list_instances()
    ],
    ok.

instance_test_() ->
    {setup, fun setup/0, fun cleanup/1, [
        fun empty_root_is_undefined/0,
        fun append_changes_root/0,
        fun append_round_trip/0,
        fun append_orders_keys/0,
        fun append_meta_round_trip/0,
        fun idempotent_append_remote/0,
        fun deterministic_root_across_replicas/0,
        fun fold_range_inclusive/0,
        fun range_returns_events_in_key_order/0,
        fun truncate_prefix/0,
        fun truncate_prefix_advances_watermark/0,
        fun size_tracks_inserts_and_truncations/0,
        fun concurrent_appends_unique_and_ordered/0,
        fun append_many_atomic/0,
        fun first_and_latest_keys/0,
        fun rejects_remote_event_with_local_origin/0,
        fun info_returns_diagnostic/0,
        fun divergent_remote_events_are_quarantined/0,
        fun custom_validator_can_reject_remote/0,
        fun list_instances_reports_running/0,
        fun start_instance_idempotent/0
    ]}.

empty_root_is_undefined() ->
    Id = mk_id(),
    {ok, _} = bondy_oplog:start_instance(Id),
    ?assertEqual(undefined, bondy_oplog:root_hash(Id)),
    ?assertEqual(0, bondy_oplog:size(Id)),
    ?assertEqual(empty, bondy_oplog:first_key(Id)),
    ?assertEqual(empty, bondy_oplog:latest_key(Id)),
    ok = bondy_oplog:stop_instance(Id).

append_changes_root() ->
    Id = mk_id(),
    {ok, _} = bondy_oplog:start_instance(Id),
    _ = bondy_oplog:append(Id, a),
    ok = bondy_oplog:await_apply(Id),
    R1 = bondy_oplog:root_hash(Id),
    _ = bondy_oplog:append(Id, b),
    ok = bondy_oplog:await_apply(Id),
    R2 = bondy_oplog:root_hash(Id),
    ?assert(is_binary(R1) andalso is_binary(R2)),
    ?assertNotEqual(R1, R2),
    ok = bondy_oplog:stop_instance(Id).

append_round_trip() ->
    Id = mk_id(),
    {ok, _} = bondy_oplog:start_instance(Id),
    K = bondy_oplog:append(Id, {payload, hello}),
    {ok, E} = bondy_oplog:get(Id, K),
    ?assertEqual({payload, hello}, bondy_oplog_event:op(E)),
    ?assertEqual(undefined, bondy_oplog_event:meta(E)),
    Missing = bondy_oplog_event:key(0, <<0>>, 0),
    ?assertEqual(not_found, bondy_oplog:get(Id, Missing)),
    ok = bondy_oplog:stop_instance(Id).

append_orders_keys() ->
    Id = mk_id(),
    {ok, _} = bondy_oplog:start_instance(Id),
    Keys = [
        bondy_oplog:append(Id, {n, N})
     || N <- lists:seq(1, 50)
    ],
    ?assertEqual(Keys, lists:sort(Keys)),
    ?assertEqual(length(Keys), length(lists:usort(Keys))),
    ok = bondy_oplog:stop_instance(Id).

append_meta_round_trip() ->
    Id = mk_id(),
    {ok, _} = bondy_oplog:start_instance(Id),
    Meta = {dots, [bondy_oplog_event:key(1, <<"x">>, 1)]},
    K = bondy_oplog:append(Id, op, Meta),
    {ok, E} = bondy_oplog:get(Id, K),
    ?assertEqual(Meta, bondy_oplog_event:meta(E)),
    ok = bondy_oplog:stop_instance(Id).

idempotent_append_remote() ->
    Id = mk_id(),
    {ok, _} = bondy_oplog:start_instance(Id),
    PeerKey = bondy_oplog_event:key(
        bondy_oplog_hlc:encode(erlang:system_time(millisecond) + 1000, 0),
        <<"peer-origin-aaaa">>,
        1
    ),
    PeerEvent = bondy_oplog_event:new(PeerKey, {peer_op, 1}, undefined),
    ok = bondy_oplog:append_remote(Id, PeerEvent),
    R1 = bondy_oplog:root_hash(Id),
    ok = bondy_oplog:append_remote(Id, PeerEvent),
    R2 = bondy_oplog:root_hash(Id),
    ?assertEqual(R1, R2),
    ?assertEqual(1, bondy_oplog:size(Id)),
    ok = bondy_oplog:stop_instance(Id).

%% Two replicas, distinct origins, fed the same event set in opposite
%% orders → identical root hashes. Core convergence invariant.
deterministic_root_across_replicas() ->
    Events = [
        bondy_oplog_event:new(
            bondy_oplog_event:key(
                bondy_oplog_hlc:encode(1_000_000_000 + N, 0),
                <<"peer-origin-fixed">>,
                N
            ),
            {op, N},
            undefined
        )
     || N <- lists:seq(1, 100)
    ],
    IdA = mk_id(),
    IdB = mk_id(),
    {ok, _} = bondy_oplog:start_instance(IdA, #{
        origin => bondy_oplog_origin:new()
    }),
    {ok, _} = bondy_oplog:start_instance(IdB, #{
        origin => bondy_oplog_origin:new()
    }),
    [bondy_oplog:append_remote(IdA, E) || E <- Events],
    [bondy_oplog:append_remote(IdB, E) || E <- lists:reverse(Events)],
    RA = bondy_oplog:root_hash(IdA),
    RB = bondy_oplog:root_hash(IdB),
    ?assertEqual(RA, RB),
    ok = bondy_oplog:stop_instance(IdA),
    ok = bondy_oplog:stop_instance(IdB).

fold_range_inclusive() ->
    Id = mk_id(),
    {ok, _} = bondy_oplog:start_instance(Id),
    Keys = [bondy_oplog:append(Id, N) || N <- lists:seq(1, 10)],
    [_, _, K3 | _] = Keys,
    K8 = lists:nth(8, Keys),
    Got = bondy_oplog:fold_range(
        Id,
        K3,
        K8,
        fun(E, Acc) -> [bondy_oplog_event:key(E) | Acc] end,
        []
    ),
    ?assertEqual(lists:sublist(Keys, 3, 6), lists:reverse(Got)),
    ok = bondy_oplog:stop_instance(Id).

range_returns_events_in_key_order() ->
    Id = mk_id(),
    {ok, _} = bondy_oplog:start_instance(Id),
    _ = [bondy_oplog:append(Id, N) || N <- lists:seq(1, 20)],
    Min = bondy_oplog_event:min_key(),
    Max = bondy_oplog_event:max_key_for_hlc(16#FFFFFFFFFFFFFFFF),
    Es = bondy_oplog:range(Id, Min, Max),
    Keys = [bondy_oplog_event:key(E) || E <- Es],
    ?assertEqual(20, length(Keys)),
    ?assertEqual(Keys, lists:sort(Keys)),
    ok = bondy_oplog:stop_instance(Id).

truncate_prefix() ->
    Id = mk_id(),
    {ok, _} = bondy_oplog:start_instance(Id),
    Keys = [bondy_oplog:append(Id, N) || N <- lists:seq(1, 20)],
    Watermark = lists:nth(10, Keys),
    Removed = bondy_oplog:truncate_prefix(Id, Watermark),
    ?assertEqual(10, Removed),
    ?assertEqual(10, bondy_oplog:size(Id)),
    {ok, First} = bondy_oplog:first_key(Id),
    ?assert(First > Watermark),
    ok = bondy_oplog:stop_instance(Id).

%% After truncate_prefix, peer events with HLC =< Watermark must be
%% rejected by the receive-side filter — otherwise a peer that has not
%% yet seen the truncate would keep re-shipping the events we just
%% dropped.
truncate_prefix_advances_watermark() ->
    Id = mk_id(),
    {ok, _} = bondy_oplog:start_instance(Id),
    Base = erlang:system_time(millisecond) + 1_000_000,
    MkEvent = fun(N) ->
        Key = bondy_oplog_event:key(
            bondy_oplog_hlc:encode(Base + N, 0),
            <<"peer-twwm-aaaa">>,
            N
        ),
        bondy_oplog_event:new(Key, {n, N}, undefined)
    end,
    Events = [MkEvent(N) || N <- lists:seq(1, 5)],
    [ok = bondy_oplog:append_remote(Id, E) || E <- Events],
    ok = bondy_oplog:await_apply(Id),
    ?assertEqual(5, bondy_oplog:size(Id)),
    ?assertEqual(undefined, bondy_oplog:current_watermark(Id)),
    K3 = bondy_oplog_event:key(lists:nth(3, Events)),
    Removed = bondy_oplog:truncate_prefix(Id, K3),
    ?assertEqual(3, Removed),
    ?assertEqual(2, bondy_oplog:size(Id)),
    ?assertEqual(K3, bondy_oplog:current_watermark(Id)),
    %% Re-shipped peer event with HLC =< Watermark: filtered, no install.
    ok = bondy_oplog:append_remote(Id, MkEvent(2)),
    ok = bondy_oplog:await_apply(Id),
    ?assertEqual(2, bondy_oplog:size(Id)),
    %% Fresh peer event past the watermark: installs normally.
    FreshKey = bondy_oplog_event:key(
        bondy_oplog_hlc:encode(Base + 100, 0),
        <<"peer-twwm-aaaa">>,
        100
    ),
    ok = bondy_oplog:append_remote(
        Id, bondy_oplog_event:new(FreshKey, {n, 100}, undefined)
    ),
    ok = bondy_oplog:await_apply(Id),
    ?assertEqual(3, bondy_oplog:size(Id)),
    %% Calling truncate_prefix with a lower watermark must NOT regress.
    K1 = bondy_oplog_event:key(lists:nth(1, Events)),
    _ = bondy_oplog:truncate_prefix(Id, K1),
    ?assertEqual(K3, bondy_oplog:current_watermark(Id)),
    ok = bondy_oplog:stop_instance(Id).

size_tracks_inserts_and_truncations() ->
    Id = mk_id(),
    {ok, _} = bondy_oplog:start_instance(Id),
    ?assertEqual(0, bondy_oplog:size(Id)),
    _ = [bondy_oplog:append(Id, N) || N <- lists:seq(1, 5)],
    ?assertEqual(5, bondy_oplog:size(Id)),
    {ok, K3} = pick_nth_key(Id, 3),
    _ = bondy_oplog:truncate_prefix(Id, K3),
    ?assertEqual(2, bondy_oplog:size(Id)),
    ok = bondy_oplog:stop_instance(Id).

concurrent_appends_unique_and_ordered() ->
    Id = mk_id(),
    {ok, _} = bondy_oplog:start_instance(Id),
    Parent = self(),
    NWorkers = 8,
    NPerWorker = 100,
    Pids = [
        spawn_link(fun() ->
            Keys = [
                bondy_oplog:append(Id, {worker, W, N})
             || N <- lists:seq(1, NPerWorker)
            ],
            Parent ! {self(), Keys}
        end)
     || W <- lists:seq(1, NWorkers)
    ],
    All = lists:flatten([
        receive
            {Wp, Ks} -> Ks
        end
     || Wp <- Pids
    ]),
    ?assertEqual(NWorkers * NPerWorker, length(All)),
    ?assertEqual(NWorkers * NPerWorker, length(lists:usort(All))),
    ?assertEqual(NWorkers * NPerWorker, bondy_oplog:size(Id)),
    ok = bondy_oplog:stop_instance(Id).

append_many_atomic() ->
    Id = mk_id(),
    {ok, _} = bondy_oplog:start_instance(Id),
    Items = [{op_a, undefined}, {op_b, undefined}, {op_c, undefined}],
    Keys = bondy_oplog:append_many(Id, Items),
    ?assertEqual(3, length(Keys)),
    ?assertEqual(Keys, lists:sort(Keys)),
    ?assertEqual(3, bondy_oplog:size(Id)),
    ok = bondy_oplog:stop_instance(Id).

first_and_latest_keys() ->
    Id = mk_id(),
    {ok, _} = bondy_oplog:start_instance(Id),
    K1 = bondy_oplog:append(Id, a),
    K2 = bondy_oplog:append(Id, b),
    K3 = bondy_oplog:append(Id, c),
    ?assertEqual({ok, K1}, bondy_oplog:first_key(Id)),
    ?assertEqual({ok, K3}, bondy_oplog:latest_key(Id)),
    ?assert(K2 > K1 andalso K3 > K2),
    ok = bondy_oplog:stop_instance(Id).

rejects_remote_event_with_local_origin() ->
    Id = mk_id(),
    Origin = bondy_oplog_origin:new(),
    {ok, _} = bondy_oplog:start_instance(Id, #{origin => Origin}),
    Bogus = bondy_oplog_event:new(
        bondy_oplog_event:key(1, Origin, 1),
        op,
        undefined
    ),
    ?assertError(
        {remote_event_with_local_origin, _},
        bondy_oplog:append_remote(Id, Bogus)
    ),
    ?assertEqual(0, bondy_oplog:size(Id)),
    ok = bondy_oplog:stop_instance(Id).

info_returns_diagnostic() ->
    Id = mk_id(),
    {ok, _} = bondy_oplog:start_instance(Id),
    Info = bondy_oplog:info(Id),
    ?assertMatch(#{instance_id := Id}, Info),
    ?assertMatch(#{backend := ets}, Info),
    ?assertMatch(
        #{validator := bondy_oplog_validator_trust},
        Info
    ),
    ?assertMatch(
        #{merge_strategy := bondy_oplog_merge_strict_uniqueness},
        Info
    ),
    ok = bondy_oplog:stop_instance(Id).

%% Two remote events with the same `{HLC, Origin, Seq}` but different
%% payloads are equivocation. The instance must NOT crash — it must
%% reject the second event with `{error, equivocation_detected}`,
%% record the proof in the quarantine table, and keep the first event
%% as the canonical value.
divergent_remote_events_are_quarantined() ->
    Id = mk_id(),
    {ok, _} = bondy_oplog:start_instance(Id),
    Key = bondy_oplog_event:key(
        bondy_oplog_hlc:encode(erlang:system_time(millisecond) + 1000, 0),
        <<"peer-origin-zzzz">>,
        1
    ),
    E1 = bondy_oplog_event:new(Key, value_a, undefined),
    E2 = bondy_oplog_event:new(Key, value_b, undefined),
    ok = bondy_oplog:append_remote(Id, E1),
    ?assertEqual(
        {error, equivocation_detected},
        bondy_oplog:append_remote(Id, E2)
    ),
    %% E1 is preserved; E2 is rejected.
    {ok, Stored} = bondy_oplog:get(Id, Key),
    ?assertEqual(value_a, bondy_oplog_event:op(Stored)),
    ?assertEqual(1, bondy_oplog:size(Id)),
    %% Quarantine row was recorded.
    ok = bondy_oplog_peer_state:sync(),
    {ok, Q} = wait_until_value(
        fun() -> bondy_oplog_quarantine:lookup(Id, Key) end,
        2000
    ),
    ?assertEqual(value_a, bondy_oplog_event:op(maps:get(event_one, Q))),
    ?assertEqual(value_b, bondy_oplog_event:op(maps:get(event_two, Q))),
    ok = bondy_oplog:stop_instance(Id).

%% A custom validator can reject peer events. We provide one that
%% returns `{error, refused}` and check the API surfaces the error.
custom_validator_can_reject_remote() ->
    Id = mk_id(),
    {ok, _} = bondy_oplog:start_instance(Id, #{
        validator => bondy_oplog_test_reject_validator
    }),
    Peer = bondy_oplog_event:new(
        bondy_oplog_event:key(1, <<"peer-origin-bbbb">>, 1),
        op,
        undefined
    ),
    ?assertEqual(
        {error, refused},
        bondy_oplog:append_remote(Id, Peer)
    ),
    ?assertEqual(0, bondy_oplog:size(Id)),
    ok = bondy_oplog:stop_instance(Id).

list_instances_reports_running() ->
    A = mk_id(),
    B = mk_id(),
    {ok, _} = bondy_oplog:start_instance(A),
    {ok, _} = bondy_oplog:start_instance(B),
    Inst = bondy_oplog:list_instances(),
    ?assert(lists:member(A, Inst)),
    ?assert(lists:member(B, Inst)),
    ok = bondy_oplog:stop_instance(A),
    ok = bondy_oplog:stop_instance(B).

start_instance_idempotent() ->
    Id = mk_id(),
    {ok, Pid1} = bondy_oplog:start_instance(Id),
    {ok, Pid2} = bondy_oplog:start_instance(Id),
    ?assertEqual(Pid1, Pid2),
    ok = bondy_oplog:stop_instance(Id).

%% Helpers

mk_id() ->
    list_to_binary(
        "inst_" ++ integer_to_list(erlang:unique_integer([positive, monotonic]))
    ).

pick_nth_key(Id, N) ->
    Es = bondy_oplog:range(
        Id,
        bondy_oplog_event:min_key(),
        bondy_oplog_event:max_key_for_hlc(16#FFFFFFFFFFFFFFFF)
    ),
    case length(Es) >= N of
        true -> {ok, bondy_oplog_event:key(lists:nth(N, Es))};
        false -> error
    end.

wait_until_value(_F, T) when T =< 0 -> error(timeout);
wait_until_value(F, T) ->
    case F() of
        not_found ->
            timer:sleep(20),
            wait_until_value(F, T - 20);
        {ok, _} = OK ->
            OK
    end.

%% =============================================================================
%% Stage 6: backpressure
%% =============================================================================

backpressure_test_() ->
    {setup, fun setup/0, fun cleanup/1, [
        fun rejects_append_when_full/0,
        fun infinity_disables_backpressure/0,
        fun append_many_atomic_under_cap/0
    ]}.

rejects_append_when_full() ->
    Id = mk_id(),
    {ok, _} = bondy_oplog:start_instance(Id, #{
        max_working_set => 3,
        origin => bondy_oplog_origin:new()
    }),
    [bondy_oplog:append(Id, X) || X <- lists:seq(1, 3)],
    %% Cap reached.
    ?assertEqual(
        {error, working_set_full},
        bondy_oplog:append(Id, x)
    ),
    ?assertEqual(3, bondy_oplog:size(Id)),
    ok = bondy_oplog:stop_instance(Id).

infinity_disables_backpressure() ->
    Id = mk_id(),
    {ok, _} = bondy_oplog:start_instance(Id, #{
        max_working_set => infinity,
        origin => bondy_oplog_origin:new()
    }),
    [bondy_oplog:append(Id, X) || X <- lists:seq(1, 100)],
    ?assertEqual(100, bondy_oplog:size(Id)),
    ok = bondy_oplog:stop_instance(Id).

%% append_many is atomic: either all events fit under the cap or none.
append_many_atomic_under_cap() ->
    Id = mk_id(),
    {ok, _} = bondy_oplog:start_instance(Id, #{
        max_working_set => 5,
        origin => bondy_oplog_origin:new()
    }),
    %% Fits.
    Items3 = [{op_a, undefined}, {op_b, undefined}, {op_c, undefined}],
    Keys = bondy_oplog:append_many(Id, Items3),
    ?assertEqual(3, length(Keys)),
    %% Doesn't fit (3 + 5 > cap=5).
    Items5 = [{op_x, undefined} || _ <- lists:seq(1, 5)],
    ?assertEqual(
        {error, working_set_full},
        bondy_oplog:append_many(Id, Items5)
    ),
    %% Atomic — no partial insert.
    ?assertEqual(3, bondy_oplog:size(Id)),
    ok = bondy_oplog:stop_instance(Id).
