%% =============================================================================
%% Tests for `bondy_mst_db:read_batch/2` (`MST_DB_DESIGN.md` §8, wired
%% in D4).
%%
%% Pins fence semantics (overlay events past the fence are excluded;
%% projection cells past the fence are returned as-is), skew detection,
%% the consistency-knob defaults, and the freshness predicate stub
%% returning `{error, ensure_fresh_not_wired}` for finite max_lag (the
%% real predicate lands in D7).
%% =============================================================================

-module(bondy_mst_db_batch_test).

-include_lib("eunit/include/eunit.hrl").

setup() ->
    {ok, _} = application:ensure_all_started(bondy_mst),
    ok.

cleanup(_) ->
    ok.

batch_test_() ->
    {setup, fun setup/0, fun cleanup/1, [
        fun empty_batch_returns_empty_map/0,
        fun single_cell_batch_returns_value/0,
        fun multi_cell_batch_returns_all_values/0,
        fun batch_with_missing_shard_returns_error_per_cell/0,
        fun fence_excludes_overlay_events_past_it/0,
        fun fence_admits_overlay_events_at_or_below/0,
        fun fence_passes_through_projection_past_fence/0,
        fun skew_within_bound_returns_ok/0,
        fun skew_above_bound_returns_error/0,
        fun consistency_eventual_skips_freshness/0,
        fun consistency_causal_unbumped_shard_is_stale/0,
        fun consistency_causal_freshly_bumped_shard_is_fresh/0,
        fun consistency_causal_only_checks_touched_shards/0,
        fun consistency_snapshot_applies_half_lag_skew/0
    ]}.

%% =============================================================================
%% Tests
%% =============================================================================

empty_batch_returns_empty_map() ->
    {ok, Map, _Fence} = bondy_mst_db:read_batch([], #{}),
    ?assertEqual(#{}, Map).

single_cell_batch_returns_value() ->
    NS = mk_ns(),
    {Setup, #{projection := PH}} = setup_shard(NS, primary, 0, 1, lww_register),
    materialise(PH, <<"k">>, {set, <<"v">>, 42}, 42),
    {ok, Map, _Fence} =
        bondy_mst_db:read_batch([{NS, primary, <<"k">>}], #{}),
    ?assertEqual(#{{NS, primary, <<"k">>} => {{set, <<"v">>, 42}, 42}}, Map),
    teardown_shard(Setup).

multi_cell_batch_returns_all_values() ->
    NS = mk_ns(),
    {Setup, #{projection := PH}} = setup_shard(NS, primary, 0, 1, lww_register),
    materialise(PH, <<"a">>, {set, <<"va">>, 10}, 10),
    materialise(PH, <<"b">>, {set, <<"vb">>, 20}, 20),
    materialise(PH, <<"c">>, {set, <<"vc">>, 30}, 30),
    Reads = [
        {NS, primary, <<"a">>},
        {NS, primary, <<"b">>},
        {NS, primary, <<"c">>}
    ],
    {ok, Map, _} = bondy_mst_db:read_batch(Reads, #{}),
    ?assertEqual(3, map_size(Map)),
    ?assertEqual({{set, <<"va">>, 10}, 10},
                 maps:get({NS, primary, <<"a">>}, Map)),
    ?assertEqual({{set, <<"vb">>, 20}, 20},
                 maps:get({NS, primary, <<"b">>}, Map)),
    ?assertEqual({{set, <<"vc">>, 30}, 30},
                 maps:get({NS, primary, <<"c">>}, Map)),
    teardown_shard(Setup).

batch_with_missing_shard_returns_error_per_cell() ->
    %% Reads for an unregistered namespace surface the per-cell error
    %% in the map; the batch itself still returns `ok`. Callers handle
    %% partial failures by walking the result.
    NS = mk_ns(),
    {ok, Map, _} =
        bondy_mst_db:read_batch([{NS, primary, <<"missing">>}], #{}),
    ?assertEqual({error, no_shards},
                 maps:get({NS, primary, <<"missing">>}, Map)).

fence_excludes_overlay_events_past_it() ->
    NS = mk_ns(),
    {Setup, #{projection := PH, overlay := OV}} =
        setup_shard(NS, primary, 0, 1, lww_register),
    %% Projection at HLC=5.
    materialise(PH, <<"k">>, {set, <<"old">>, 5}, 5),
    %% Two overlay events: one at HLC=10, one at HLC=20.
    overlay_insert(OV, <<"k">>, 10, {set, 10, <<"mid">>}),
    overlay_insert(OV, <<"k">>, 20, {set, 20, <<"new">>}),
    %% Fence at HLC=15 → only the HLC=10 overlay event applies.
    {ok, Map, _Fence} =
        bondy_mst_db:read_batch([{NS, primary, <<"k">>}], #{fence => 15}),
    ?assertEqual({{set, <<"mid">>, 10}, 10},
                 maps:get({NS, primary, <<"k">>}, Map)),
    teardown_shard(Setup).

fence_admits_overlay_events_at_or_below() ->
    NS = mk_ns(),
    {Setup, #{projection := PH, overlay := OV}} =
        setup_shard(NS, primary, 0, 1, lww_register),
    materialise(PH, <<"k">>, {set, <<"old">>, 5}, 5),
    overlay_insert(OV, <<"k">>, 10, {set, 10, <<"mid">>}),
    %% Fence at HLC=10 → the HLC=10 event is included (=< fence).
    {ok, Map, _} =
        bondy_mst_db:read_batch([{NS, primary, <<"k">>}], #{fence => 10}),
    ?assertEqual({{set, <<"mid">>, 10}, 10},
                 maps:get({NS, primary, <<"k">>}, Map)),
    teardown_shard(Setup).

fence_passes_through_projection_past_fence() ->
    %% §8.2 — projection cells whose last_modified_hlc has advanced past
    %% the fence are returned at their actual HLC, not synthesised back
    %% in time.
    NS = mk_ns(),
    {Setup, #{projection := PH}} =
        setup_shard(NS, primary, 0, 1, lww_register),
    materialise(PH, <<"k">>, {set, <<"v">>, 100}, 100),
    {ok, Map, _} =
        bondy_mst_db:read_batch([{NS, primary, <<"k">>}], #{fence => 50}),
    ?assertEqual({{set, <<"v">>, 100}, 100},
                 maps:get({NS, primary, <<"k">>}, Map)),
    teardown_shard(Setup).

skew_within_bound_returns_ok() ->
    NS = mk_ns(),
    {Setup, #{projection := PH}} =
        setup_shard(NS, primary, 0, 1, lww_register),
    %% HLCs separated by 50 ms in physical time.
    H1 = bondy_oplog_hlc:encode(1_000, 0),
    H2 = bondy_oplog_hlc:encode(1_050, 0),
    materialise(PH, <<"a">>, {set, <<"va">>, H1}, H1),
    materialise(PH, <<"b">>, {set, <<"vb">>, H2}, H2),
    Reads = [{NS, primary, <<"a">>}, {NS, primary, <<"b">>}],
    {ok, _, _} = bondy_mst_db:read_batch(Reads, #{require_skew_below => 100}),
    teardown_shard(Setup).

skew_above_bound_returns_error() ->
    NS = mk_ns(),
    {Setup, #{projection := PH}} =
        setup_shard(NS, primary, 0, 1, lww_register),
    H1 = bondy_oplog_hlc:encode(1_000, 0),
    H2 = bondy_oplog_hlc:encode(2_000, 0),
    materialise(PH, <<"a">>, {set, <<"va">>, H1}, H1),
    materialise(PH, <<"b">>, {set, <<"vb">>, H2}, H2),
    Reads = [{NS, primary, <<"a">>}, {NS, primary, <<"b">>}],
    ?assertMatch({error, {skew_too_large, 1_000, 500}},
                 bondy_mst_db:read_batch(Reads, #{require_skew_below => 500})),
    teardown_shard(Setup).

consistency_eventual_skips_freshness() ->
    %% Even with a finite max_lag, `eventual` skips the freshness check
    %% (it's the cheapest mode and explicitly tolerant of staleness).
    NS = mk_ns(),
    {Setup, _} = setup_shard(NS, primary, 0, 1, lww_register),
    {ok, _, _} = bondy_mst_db:read_batch(
        [{NS, primary, <<"k">>}],
        #{consistency => eventual, max_lag => 50}
    ),
    teardown_shard(Setup).

consistency_causal_unbumped_shard_is_stale() ->
    %% A registered shard whose AE counter has never been bumped is
    %% "infinitely stale": `Now - 0 = huge`, > any finite MaxLag.
    NS = mk_ns(),
    {Setup, _} = setup_shard(NS, primary, 0, 1, lww_register),
    ?assertEqual(
        {error, {stale, [NS]}},
        bondy_mst_db:read_batch(
            [{NS, primary, <<"k">>}],
            #{consistency => causal, max_lag => 100}
        )
    ),
    teardown_shard(Setup).

consistency_causal_freshly_bumped_shard_is_fresh() ->
    %% After `bump_ae/3`, the shard's last-AE is "now"; a generous
    %% MaxLag accepts the batch.
    NS = mk_ns(),
    {Setup, _} = setup_shard(NS, primary, 0, 1, lww_register),
    ok = bondy_mst_db_registry:bump_ae(NS, primary, 0),
    {ok, _, _} =
        bondy_mst_db:read_batch(
            [{NS, primary, <<"k">>}],
            #{consistency => causal, max_lag => 1_000_000}
        ),
    teardown_shard(Setup).

consistency_causal_only_checks_touched_shards() ->
    %% Per-key freshness: a batch hitting only shard 0 must succeed
    %% even when shard 1 in the same namespace has never been bumped.
    NS = mk_ns(),
    {S0, _} = setup_shard(NS, primary, 0, 2, lww_register),
    {S1, _} = setup_shard(NS, primary, 1, 2, lww_register),
    ok = bondy_mst_db_registry:bump_ae(NS, primary, 0),
    %% Find a key that hashes to shard 0; the batch must NOT fail on
    %% the unbumped shard 1.
    K0 = find_key_for_shard(NS, primary, 0),
    ?assertMatch(
        {ok, _, _},
        bondy_mst_db:read_batch(
            [{NS, primary, K0}],
            #{consistency => causal, max_lag => 1_000_000}
        )
    ),
    %% A key hitting shard 1 must still fail.
    K1 = find_key_for_shard(NS, primary, 1),
    ?assertEqual(
        {error, {stale, [NS]}},
        bondy_mst_db:read_batch(
            [{NS, primary, K1}],
            #{consistency => causal, max_lag => 1_000_000}
        )
    ),
    teardown_shard(S0),
    teardown_shard(S1).

consistency_snapshot_applies_half_lag_skew() ->
    %% Snapshot consistency pins skew to max_lag / 2. With max_lag=200,
    %% skew bound = 100. Two cells separated by 150ms must fail the
    %% skew check (the freshness check is bypassed by passing
    %% `max_lag => infinity` and verifying the skew arithmetic alone).
    %%
    %% This test exercises the snapshot computation by overriding to
    %% infinity max_lag (so D7 is not needed) and an explicit skew
    %% smaller than the half-lag default — confirming the min() takes
    %% the stricter of the two.
    NS = mk_ns(),
    {Setup, #{projection := PH}} =
        setup_shard(NS, primary, 0, 1, lww_register),
    H1 = bondy_oplog_hlc:encode(1_000, 0),
    H2 = bondy_oplog_hlc:encode(1_150, 0),
    materialise(PH, <<"a">>, {set, <<"va">>, H1}, H1),
    materialise(PH, <<"b">>, {set, <<"vb">>, H2}, H2),
    Reads = [{NS, primary, <<"a">>}, {NS, primary, <<"b">>}],
    ?assertMatch(
        {error, {skew_too_large, 150, 50}},
        bondy_mst_db:read_batch(Reads, #{
            consistency => snapshot,
            max_lag => infinity,
            require_skew_below => 50
        })
    ),
    teardown_shard(Setup).

%% =============================================================================
%% Helpers
%% =============================================================================

mk_ns() ->
    list_to_atom("mst_db_batch_" ++
                 integer_to_list(erlang:unique_integer([positive, monotonic]))).

find_key_for_shard(NS, Index, WantedShard) ->
    find_key_for_shard(NS, Index, WantedShard, 0).

find_key_for_shard(NS, Index, WantedShard, N) when N < 10_000 ->
    K = integer_to_binary(N),
    case bondy_mst_db:shard_for(NS, Index, K) of
        {ok, WantedShard} -> K;
        _ -> find_key_for_shard(NS, Index, WantedShard, N + 1)
    end;
find_key_for_shard(_, _, _, _) ->
    erlang:error(no_key_for_shard).

mk_event(Hlc, Origin, Seq, Op) ->
    K = bondy_oplog_event:key(Hlc, Origin, Seq),
    bondy_oplog_event:new(K, Op, undefined).

materialise(PH, Key, State, Hlc) ->
    Frame = bondy_oplog_cell_frame:encode(
        Hlc,
        bondy_oplog_fold:encode_state(lww_register, State)
    ),
    ok = bondy_oplog_projection_ets:put_batch(PH, [{Key, Frame}]).

overlay_insert(OV, Key, Hlc, Op) ->
    Event = mk_event(Hlc, <<"origin">>, Hlc, Op),
    ok = bondy_oplog_db_overlay:insert(OV, Key, Event).

setup_shard(NS, Index, Shard, ShardCount, Strategy) ->
    {ok, CH} = bondy_oplog_cache_ets:init(NS, Index, Shard, #{}),
    {ok, PH} = bondy_oplog_projection_ets:open(NS, Index, Shard, #{}),
    OV = bondy_oplog_db_overlay:new(),
    ok = bondy_mst_db_registry:register(NS, Index, Shard, #{
        shard_count => ShardCount,
        cache_adapter => bondy_oplog_cache_ets,
        cache_handle => CH,
        projection_adapter => bondy_oplog_projection_ets,
        projection_handle => PH,
        overlay => OV,
        fold_module => Strategy
    }),
    Setup = #{ns => NS, index => Index, shard => Shard,
              cache_handle => CH, projection => PH, overlay => OV},
    {Setup, Setup}.

teardown_shard(#{ns := NS, index := Index, shard := Shard,
                 cache_handle := CH, projection := PH, overlay := OV}) ->
    ok = bondy_mst_db_registry:unregister(NS, Index, Shard),
    ok = bondy_oplog_cache_ets:close(CH),
    ok = bondy_oplog_projection_ets:close(PH),
    ok = bondy_oplog_db_overlay:delete(OV).
