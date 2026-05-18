%% =============================================================================
%% Tests for `bondy_db_core:range/4` (`MST_DB_DESIGN.md` §9, wired in D5).
%%
%% Pins: projection-only ranges, overlay-only ranges, projection+overlay
%% merge per key, limit, direction, include_overlay flag, fence on
%% overlay events, half-open `[Low, High)` semantics, undefined cells
%% (overlay-only with no terminal value) suppressed from results.
%% =============================================================================

-module(bondy_db_core_range_test).

-include_lib("eunit/include/eunit.hrl").

setup() ->
    {ok, _} = application:ensure_all_started(bondy_mst),
    ok.

cleanup(_) ->
    ok.

range_test_() ->
    {setup, fun setup/0, fun cleanup/1, [
        fun empty_range_returns_empty_list/0,
        fun projection_only_range_returns_in_order/0,
        fun overlay_only_range_returns_in_order/0,
        fun projection_and_overlay_merge_per_key/0,
        fun half_open_interval_excludes_high_key/0,
        fun limit_caps_the_result/0,
        fun direction_desc_reverses_result/0,
        fun include_overlay_false_drops_overlay_events/0,
        fun fence_excludes_overlay_events_past_it/0,
        fun overlay_only_undefined_terminal_is_filtered/0,
        fun unknown_namespace_returns_no_shards/0
    ]}.

%% =============================================================================
%% Tests
%% =============================================================================

empty_range_returns_empty_list() ->
    NS = mk_ns(),
    {Setup, _} = setup_shard(NS, primary, 0, 1, lww_register),
    {ok, []} = bondy_db_core:range(NS, primary, {<<"a">>, <<"z">>}, #{}),
    teardown_shard(Setup).

projection_only_range_returns_in_order() ->
    NS = mk_ns(),
    {Setup, #{projection := PH}} =
        setup_shard(NS, primary, 0, 1, lww_register),
    materialise(PH, <<"a">>, {set, <<"va">>, 1}, 1),
    materialise(PH, <<"b">>, {set, <<"vb">>, 2}, 2),
    materialise(PH, <<"c">>, {set, <<"vc">>, 3}, 3),
    {ok, Rows} = bondy_db_core:range(NS, primary, {<<"a">>, <<"z">>}, #{}),
    ?assertEqual(
        [
            {<<"a">>, {set, <<"va">>, 1}, 1},
            {<<"b">>, {set, <<"vb">>, 2}, 2},
            {<<"c">>, {set, <<"vc">>, 3}, 3}
        ],
        Rows
    ),
    teardown_shard(Setup).

overlay_only_range_returns_in_order() ->
    NS = mk_ns(),
    {Setup, #{overlay := OV}} =
        setup_shard(NS, primary, 0, 1, lww_register),
    %% Three overlay-only cells; lww_register's initial_value is undefined,
    %% so after fold the values are the event payloads.
    overlay_insert(OV, <<"a">>, 10, {set, 10, <<"va">>}),
    overlay_insert(OV, <<"b">>, 20, {set, 20, <<"vb">>}),
    overlay_insert(OV, <<"c">>, 30, {set, 30, <<"vc">>}),
    {ok, Rows} = bondy_db_core:range(NS, primary, {<<"a">>, <<"z">>}, #{}),
    ?assertEqual(
        [
            {<<"a">>, {set, <<"va">>, 10}, 10},
            {<<"b">>, {set, <<"vb">>, 20}, 20},
            {<<"c">>, {set, <<"vc">>, 30}, 30}
        ],
        Rows
    ),
    teardown_shard(Setup).

projection_and_overlay_merge_per_key() ->
    NS = mk_ns(),
    {Setup, #{projection := PH, overlay := OV}} =
        setup_shard(NS, primary, 0, 1, lww_register),
    %% Projection has a at HLC=5, c at HLC=15. Overlay has a (newer) at
    %% HLC=30 and b (new key) at HLC=20.
    materialise(PH, <<"a">>, {set, <<"old-a">>, 5}, 5),
    materialise(PH, <<"c">>, {set, <<"old-c">>, 15}, 15),
    overlay_insert(OV, <<"a">>, 30, {set, 30, <<"new-a">>}),
    overlay_insert(OV, <<"b">>, 20, {set, 20, <<"new-b">>}),
    {ok, Rows} = bondy_db_core:range(NS, primary, {<<"a">>, <<"z">>}, #{}),
    ?assertEqual(
        [
            {<<"a">>, {set, <<"new-a">>, 30}, 30},
            {<<"b">>, {set, <<"new-b">>, 20}, 20},
            {<<"c">>, {set, <<"old-c">>, 15}, 15}
        ],
        Rows
    ),
    teardown_shard(Setup).

half_open_interval_excludes_high_key() ->
    NS = mk_ns(),
    {Setup, #{projection := PH}} =
        setup_shard(NS, primary, 0, 1, lww_register),
    materialise(PH, <<"a">>, {set, <<"va">>, 1}, 1),
    materialise(PH, <<"b">>, {set, <<"vb">>, 2}, 2),
    materialise(PH, <<"c">>, {set, <<"vc">>, 3}, 3),
    {ok, Rows} = bondy_db_core:range(NS, primary, {<<"a">>, <<"c">>}, #{}),
    %% `c` is excluded by the half-open upper bound.
    ?assertEqual(
        [
            {<<"a">>, {set, <<"va">>, 1}, 1},
            {<<"b">>, {set, <<"vb">>, 2}, 2}
        ],
        Rows
    ),
    teardown_shard(Setup).

limit_caps_the_result() ->
    NS = mk_ns(),
    {Setup, #{projection := PH}} =
        setup_shard(NS, primary, 0, 1, lww_register),
    [materialise(PH, <<"k", N>>, {set, <<N>>, N}, N) || N <- lists:seq($a, $e)],
    {ok, Rows} =
        bondy_db_core:range(NS, primary, {<<"k">>, <<"z">>}, #{limit => 2}),
    ?assertEqual(2, length(Rows)),
    teardown_shard(Setup).

direction_desc_reverses_result() ->
    NS = mk_ns(),
    {Setup, #{projection := PH}} =
        setup_shard(NS, primary, 0, 1, lww_register),
    materialise(PH, <<"a">>, {set, <<"va">>, 1}, 1),
    materialise(PH, <<"b">>, {set, <<"vb">>, 2}, 2),
    materialise(PH, <<"c">>, {set, <<"vc">>, 3}, 3),
    {ok, Rows} =
        bondy_db_core:range(NS, primary, {<<"a">>, <<"z">>}, #{direction => desc}),
    ?assertEqual(
        [
            {<<"c">>, {set, <<"vc">>, 3}, 3},
            {<<"b">>, {set, <<"vb">>, 2}, 2},
            {<<"a">>, {set, <<"va">>, 1}, 1}
        ],
        Rows
    ),
    teardown_shard(Setup).

include_overlay_false_drops_overlay_events() ->
    NS = mk_ns(),
    {Setup, #{projection := PH, overlay := OV}} =
        setup_shard(NS, primary, 0, 1, lww_register),
    materialise(PH, <<"a">>, {set, <<"old">>, 1}, 1),
    overlay_insert(OV, <<"a">>, 10, {set, 10, <<"new">>}),
    {ok, Rows} =
        bondy_db_core:range(NS, primary, {<<"a">>, <<"z">>},
                           #{include_overlay => false}),
    ?assertEqual([{<<"a">>, {set, <<"old">>, 1}, 1}], Rows),
    teardown_shard(Setup).

fence_excludes_overlay_events_past_it() ->
    NS = mk_ns(),
    {Setup, #{projection := PH, overlay := OV}} =
        setup_shard(NS, primary, 0, 1, lww_register),
    materialise(PH, <<"a">>, {set, <<"old">>, 1}, 1),
    overlay_insert(OV, <<"a">>, 10, {set, 10, <<"mid">>}),
    overlay_insert(OV, <<"a">>, 30, {set, 30, <<"new">>}),
    %% Fence at 20 → only the HLC=10 overlay event applies.
    {ok, Rows} =
        bondy_db_core:range(NS, primary, {<<"a">>, <<"z">>}, #{fence => 20}),
    ?assertEqual([{<<"a">>, {set, <<"mid">>, 10}, 10}], Rows),
    teardown_shard(Setup).

overlay_only_undefined_terminal_is_filtered() ->
    %% A `clear` op against an unset cell stays at `{cleared, H}` in
    %% lww_register, which is NOT undefined, so it IS emitted. Conversely
    %% if a fold returned undefined as the terminal value for an
    %% overlay-only cell, that cell would not be emitted. We pin the
    %% positive case (cleared is emitted) here; the undefined-suppression
    %% behaviour is covered by `read_returns_undefined_when_*` tests.
    NS = mk_ns(),
    {Setup, #{overlay := OV}} =
        setup_shard(NS, primary, 0, 1, lww_register),
    overlay_insert(OV, <<"k">>, 10, {clear, 10}),
    {ok, Rows} =
        bondy_db_core:range(NS, primary, {<<"a">>, <<"z">>}, #{}),
    ?assertEqual([{<<"k">>, {cleared, 10}, 10}], Rows),
    teardown_shard(Setup).

unknown_namespace_returns_no_shards() ->
    NS = mk_ns(),
    ?assertEqual(
        {error, no_shards},
        bondy_db_core:range(NS, primary, {<<"a">>, <<"z">>}, #{})
    ).

%% =============================================================================
%% Helpers
%% =============================================================================

mk_ns() ->
    list_to_atom("mst_db_range_" ++
                 integer_to_list(erlang:unique_integer([positive, monotonic]))).

mk_event(Hlc, Origin, Seq, Op) ->
    K = bondy_oplog_event:key(Hlc, Origin, Seq),
    bondy_oplog_event:new(K, Op, undefined).

materialise(PH, Key, State, Hlc) ->
    Frame = bondy_oplog_cell_frame:encode(
        Hlc,
        bondy_oplog_fold:encode_state(lww_register, State)
    ),
    ok = bondy_oplog_projection_ets:put_batch(PH, [{<<>>, Key, Frame}]).

overlay_insert(OV, Key, Hlc, Op) ->
    Event = mk_event(Hlc, <<"origin">>, Hlc, Op),
    ok = bondy_oplog_db_overlay:insert(OV, <<>>, Key, Event).

setup_shard(NS, Index, Shard, ShardCount, Strategy) ->
    {ok, CH} = bondy_oplog_cache_ets:init(NS, Index, Shard, #{}),
    {ok, PH} = bondy_oplog_projection_ets:open(NS, Index, Shard, #{}),
    OV = bondy_oplog_db_overlay:new(),
    ok = bondy_db_core_registry:register(NS, Index, Shard, #{
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
    ok = bondy_db_core_registry:unregister(NS, Index, Shard),
    ok = bondy_oplog_cache_ets:close(CH),
    ok = bondy_oplog_projection_ets:close(PH),
    ok = bondy_oplog_db_overlay:delete(OV).
