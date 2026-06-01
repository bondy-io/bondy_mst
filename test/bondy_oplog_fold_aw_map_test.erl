%% =============================================================================
%% EUnit smoke tests for `bondy_oplog_fold_aw_map`.
%%
%% Covers put / apply / remove semantics, dot-source-from-Meta,
%% per-key tombstones, revive on absent / tombstoned key, strategy-
%% mismatch crash, resolve_event, encode round-trip, merge by example.
%%
%% CAI invariants (commutative / associative / idempotent merge,
%% idempotent apply, encode round-trip generator) live in the proper
%% test.
%% =============================================================================

-module(bondy_oplog_fold_aw_map_test).

-include_lib("eunit/include/eunit.hrl").

-define(MOD, bondy_oplog_fold_aw_map).

%% =============================================================================
%% Helpers
%% =============================================================================

hlc(Phys, Log) ->
    bondy_oplog_hlc:encode(Phys, Log).

key(Hlc, Origin, Seq) ->
    bondy_oplog_event:key(Hlc, Origin, Seq).

initial() ->
    ?MOD:initial_value().

apply_ev(State, Event, Meta) ->
    {S1, _Delta} = ?MOD:apply_event(State, Event, Meta),
    S1.

apply_with_delta(State, Event, Meta) ->
    ?MOD:apply_event(State, Event, Meta).

%% =============================================================================
%% Initial value
%% =============================================================================

initial_value_test() ->
    ?assertEqual(#{entries => #{}, hlc => 0}, initial()),
    ?assertEqual(undefined, ?MOD:gc_threshold(initial())),
    ?assertEqual(#{}, ?MOD:to_value(initial())).

%% =============================================================================
%% put: dot, sub-state, value
%% =============================================================================

put_inserts_new_key_with_dot_from_meta_test() ->
    Meta = key(hlc(100, 0), <<"n1">>, 1),
    S = apply_ev(
        initial(),
        {put, <<"k">>, lww_register, {set, <<"v">>, hlc(100, 0)}},
        Meta
    ),
    #{entries := E} = S,
    ?assertMatch(
        #{<<"k">> := {lww_register, _, [{<<"n1">>, 1}], []}},
        E
    ),
    ?assertEqual(#{<<"k">> => <<"v">>}, ?MOD:to_value(S)).

put_emits_set_elem_delta_for_new_key_test() ->
    Meta = key(hlc(100, 0), <<"n1">>, 1),
    {_, Delta} = apply_with_delta(
        initial(),
        {put, <<"k">>, lww_register, {set, <<"v">>, hlc(100, 0)}},
        Meta
    ),
    ?assertEqual({set_elem, <<"k">>, <<"v">>}, Delta).

put_concurrent_dots_both_in_add_dots_test() ->
    %% Two replicas both put K with different dots. After both events
    %% land, K's AddDots contains both dots; sub-states merge.
    InitVal = fun(V) -> {set, V, hlc(100, 0)} end,
    M1 = key(hlc(100, 0), <<"n1">>, 1),
    M2 = key(hlc(100, 0), <<"n2">>, 1),
    S0 = apply_ev(
        initial(), {put, <<"k">>, lww_register, InitVal(<<"a">>)}, M1
    ),
    S1 = apply_ev(S0, {put, <<"k">>, lww_register, InitVal(<<"a">>)}, M2),
    #{entries := E} = S1,
    {lww_register, _SubState, AddDots, _Tombs} = maps:get(<<"k">>, E),
    ?assertEqual(
        ordsets:from_list([{<<"n1">>, 1}, {<<"n2">>, 1}]),
        AddDots
    ).

put_strategy_mismatch_crashes_test() ->
    M1 = key(hlc(100, 0), <<"n1">>, 1),
    M2 = key(hlc(101, 0), <<"n1">>, 2),
    S0 = apply_ev(
        initial(),
        {put, <<"k">>, lww_register, {set, <<"v">>, hlc(100, 0)}},
        M1
    ),
    ?assertError(
        {strategy_mismatch, <<"k">>, lww_register, pn_counter},
        ?MOD:apply_event(
            S0, {put, <<"k">>, pn_counter, #{counters => #{}, hlc => 0}}, M2
        )
    ).

%% =============================================================================
%% apply: dot, sub-state mutation, revive
%% =============================================================================

apply_on_absent_key_revives_via_sub_initial_test() ->
    %% First contact with K is an apply, not a put. AW-Map implicitly
    %% creates K from SubFold:initial_value() then applies the
    %% sub-event.
    Meta = key(hlc(100, 0), <<"n1">>, 1),
    S = apply_ev(
        initial(),
        {apply, <<"k">>, pn_counter, {inc, 5}},
        Meta
    ),
    #{entries := E} = S,
    {pn_counter, SubState, [{<<"n1">>, 1}], []} = maps:get(<<"k">>, E),
    %% Sub-state should reflect the inc, with Origin/Seq from Meta.
    ?assertEqual(5, bondy_oplog_fold:to_value(pn_counter, SubState)).

apply_on_live_key_mutates_sub_state_test() ->
    M1 = key(hlc(100, 0), <<"n1">>, 1),
    M2 = key(hlc(101, 0), <<"n1">>, 2),
    S0 = apply_ev(
        initial(),
        {put, <<"k">>, pn_counter, #{counters => #{}, hlc => 0}},
        M1
    ),
    S1 = apply_ev(S0, {apply, <<"k">>, pn_counter, {inc, 7}}, M2),
    ?assertEqual(#{<<"k">> => 7}, ?MOD:to_value(S1)).

apply_strategy_mismatch_on_live_key_crashes_test() ->
    M1 = key(hlc(100, 0), <<"n1">>, 1),
    M2 = key(hlc(101, 0), <<"n1">>, 2),
    S0 = apply_ev(
        initial(),
        {put, <<"k">>, lww_register, {set, <<"v">>, hlc(100, 0)}},
        M1
    ),
    ?assertError(
        {strategy_mismatch, <<"k">>, lww_register, pn_counter},
        ?MOD:apply_event(S0, {apply, <<"k">>, pn_counter, {inc, 1}}, M2)
    ).

%% =============================================================================
%% remove: tombstones, scrubs, transitions to tombstoned marker
%% =============================================================================

remove_tombstones_dots_and_transitions_to_marker_test() ->
    %% Put K with dot D, then remove K observing D. K transitions to
    %% {tombstoned, lww_register, [D]}.
    M1 = key(hlc(100, 0), <<"n1">>, 1),
    M2 = key(hlc(200, 0), <<"n1">>, 2),
    S0 = apply_ev(
        initial(),
        {put, <<"k">>, lww_register, {set, <<"v">>, hlc(100, 0)}},
        M1
    ),
    {S1, Delta} = apply_with_delta(S0, {remove, <<"k">>, [{<<"n1">>, 1}]}, M2),
    ?assertEqual({remove_elem, <<"k">>}, Delta),
    #{entries := E} = S1,
    ?assertEqual(
        {tombstoned, lww_register, [{<<"n1">>, 1}]},
        maps:get(<<"k">>, E)
    ),
    ?assertEqual(#{}, ?MOD:to_value(S1)).

remove_partial_keeps_surviving_dots_test() ->
    %% Put twice with two distinct dots; remove only one. K is still
    %% live; the other dot remains in AddDots.
    InitVal = fun(V) -> {set, V, hlc(100, 0)} end,
    M1 = key(hlc(100, 0), <<"n1">>, 1),
    M2 = key(hlc(100, 0), <<"n2">>, 1),
    M3 = key(hlc(200, 0), <<"n1">>, 2),
    S0 = apply_ev(
        initial(), {put, <<"k">>, lww_register, InitVal(<<"a">>)}, M1
    ),
    S1 = apply_ev(S0, {put, <<"k">>, lww_register, InitVal(<<"a">>)}, M2),
    S2 = apply_ev(S1, {remove, <<"k">>, [{<<"n1">>, 1}]}, M3),
    #{entries := E} = S2,
    {lww_register, _SubState, AddDots, Tombs} = maps:get(<<"k">>, E),
    ?assertEqual([{<<"n2">>, 1}], AddDots),
    ?assertEqual([{<<"n1">>, 1}], Tombs).

remove_of_absent_key_is_noop_test() ->
    %% Pre-emptive remove on a never-added K is dropped; HLC bumps.
    M1 = key(hlc(100, 0), <<"n1">>, 1),
    {S1, Delta} = apply_with_delta(
        initial(),
        {remove, <<"k">>, [{<<"n1">>, 1}]},
        M1
    ),
    ?assertEqual(none, Delta),
    ?assertEqual(#{}, maps:get(entries, S1)),
    ?assertEqual(hlc(100, 0), maps:get(hlc, S1)).

%% =============================================================================
%% Revive after tombstone: AW property
%% =============================================================================

put_after_full_tombstone_revives_test() ->
    %% Tombstone K via remove, then put K again. New put's dot is not
    %% in tombstones, so K revives with the new dot in AddDots.
    InitVal = fun(V) -> {set, V, hlc(100, 0)} end,
    M1 = key(hlc(100, 0), <<"n1">>, 1),
    M2 = key(hlc(200, 0), <<"n1">>, 2),
    M3 = key(hlc(300, 0), <<"n1">>, 3),
    S0 = apply_ev(
        initial(), {put, <<"k">>, lww_register, InitVal(<<"a">>)}, M1
    ),
    S1 = apply_ev(S0, {remove, <<"k">>, [{<<"n1">>, 1}]}, M2),
    %% K is tombstoned now.
    ?assertMatch(
        #{<<"k">> := {tombstoned, lww_register, _}},
        maps:get(entries, S1)
    ),
    S2 = apply_ev(S1, {put, <<"k">>, lww_register, InitVal(<<"b">>)}, M3),
    #{entries := E} = S2,
    {lww_register, _, AddDots, [{<<"n1">>, 1}]} = maps:get(<<"k">>, E),
    ?assertEqual([{<<"n1">>, 3}], AddDots),
    ?assertEqual(#{<<"k">> => <<"b">>}, ?MOD:to_value(S2)).

apply_after_full_tombstone_revives_test() ->
    %% Tombstone K via remove, then apply K with a sub-event. New
    %% apply's dot is not in tombstones, so K revives from
    %% sub_fold:initial_value() + sub-event.
    M1 = key(hlc(100, 0), <<"n1">>, 1),
    M2 = key(hlc(200, 0), <<"n1">>, 2),
    M3 = key(hlc(300, 0), <<"n1">>, 3),
    S0 = apply_ev(
        initial(),
        {put, <<"k">>, lww_register, {set, <<"a">>, hlc(100, 0)}},
        M1
    ),
    S1 = apply_ev(S0, {remove, <<"k">>, [{<<"n1">>, 1}]}, M2),
    ?assertMatch(
        #{<<"k">> := {tombstoned, lww_register, _}},
        maps:get(entries, S1)
    ),
    S2 = apply_ev(
        S1,
        %% NOTE: lww_register *event* shape is {set, H, V}
        %% (Hlc first), distinct from *state* shape {set, V, H}.
        {apply, <<"k">>, lww_register, {set, hlc(300, 0), <<"b">>}},
        M3
    ),
    #{entries := E} = S2,
    {lww_register, _, AddDots, [{<<"n1">>, 1}]} = maps:get(<<"k">>, E),
    ?assertEqual([{<<"n1">>, 3}], AddDots),
    ?assertEqual(#{<<"k">> => <<"b">>}, ?MOD:to_value(S2)).

apply_after_full_tombstone_emits_set_elem_delta_test() ->
    %% The revive must emit a {set_elem, K, V} delta so the projection's
    %% value layer rebuilds the entry. Without this, a downstream
    %% bondy_db:read after remove+apply would still return #{}.
    M1 = key(hlc(100, 0), <<"n1">>, 1),
    M2 = key(hlc(200, 0), <<"n1">>, 2),
    M3 = key(hlc(300, 0), <<"n1">>, 3),
    S0 = apply_ev(
        initial(),
        {put, <<"k">>, lww_register, {set, <<"a">>, hlc(100, 0)}},
        M1
    ),
    S1 = apply_ev(S0, {remove, <<"k">>, [{<<"n1">>, 1}]}, M2),
    {_, Delta} = apply_with_delta(
        S1,
        {apply, <<"k">>, lww_register, {set, hlc(300, 0), <<"b">>}},
        M3
    ),
    ?assertEqual({set_elem, <<"k">>, <<"b">>}, Delta).

put_strategy_mismatch_on_tombstoned_key_crashes_test() ->
    M1 = key(hlc(100, 0), <<"n1">>, 1),
    M2 = key(hlc(200, 0), <<"n1">>, 2),
    M3 = key(hlc(300, 0), <<"n1">>, 3),
    S0 = apply_ev(
        initial(),
        {put, <<"k">>, lww_register, {set, <<"v">>, hlc(100, 0)}},
        M1
    ),
    S1 = apply_ev(S0, {remove, <<"k">>, [{<<"n1">>, 1}]}, M2),
    ?assertError(
        {strategy_mismatch, <<"k">>, lww_register, pn_counter},
        ?MOD:apply_event(
            S1, {put, <<"k">>, pn_counter, #{counters => #{}, hlc => 0}}, M3
        )
    ).

%% =============================================================================
%% resolve_event: logical {remove_aw_key, K} → physical {remove, K, Dots}
%% =============================================================================

resolve_event_translates_remove_aw_key_test() ->
    InitVal = fun(V) -> {set, V, hlc(100, 0)} end,
    M1 = key(hlc(100, 0), <<"n1">>, 1),
    M2 = key(hlc(100, 0), <<"n2">>, 1),
    S0 = apply_ev(
        initial(), {put, <<"k">>, lww_register, InitVal(<<"a">>)}, M1
    ),
    S1 = apply_ev(S0, {put, <<"k">>, lww_register, InitVal(<<"a">>)}, M2),
    ?assertEqual(
        {remove, <<"k">>, ordsets:from_list([{<<"n1">>, 1}, {<<"n2">>, 1}])},
        ?MOD:resolve_event(S1, {remove_aw_key, <<"k">>})
    ).

resolve_event_on_absent_key_is_passthrough_test() ->
    ?assertEqual(
        passthrough,
        ?MOD:resolve_event(initial(), {remove_aw_key, <<"k">>})
    ).

resolve_event_on_tombstoned_key_is_passthrough_test() ->
    M1 = key(hlc(100, 0), <<"n1">>, 1),
    M2 = key(hlc(200, 0), <<"n1">>, 2),
    S0 = apply_ev(
        initial(),
        {put, <<"k">>, lww_register, {set, <<"v">>, hlc(100, 0)}},
        M1
    ),
    S1 = apply_ev(S0, {remove, <<"k">>, [{<<"n1">>, 1}]}, M2),
    ?assertEqual(
        passthrough,
        ?MOD:resolve_event(S1, {remove_aw_key, <<"k">>})
    ).

resolve_event_via_dispatcher_test() ->
    %% Confirm bondy_oplog_fold:resolve_event/3 routes through the
    %% AW-Map module when called with shorthand `aw_map`.
    InitVal = fun(V) -> {set, V, hlc(100, 0)} end,
    M1 = key(hlc(100, 0), <<"n1">>, 1),
    S0 = apply_ev(
        initial(), {put, <<"k">>, lww_register, InitVal(<<"a">>)}, M1
    ),
    ?assertEqual(
        {remove, <<"k">>, [{<<"n1">>, 1}]},
        bondy_oplog_fold:resolve_event(aw_map, S0, {remove_aw_key, <<"k">>})
    ).

resolve_event_dispatcher_passthrough_for_folds_without_callback_test() ->
    %% Folds that don't export resolve_event/2 get the original event
    %% back unchanged via the dispatcher.
    Event = {inc, 5},
    State = #{counters => #{}, hlc => 0},
    ?assertEqual(
        Event,
        bondy_oplog_fold:resolve_event(pn_counter, State, Event)
    ).

%% =============================================================================
%% to_value / apply_value_delta
%% =============================================================================

to_value_skips_tombstoned_entries_test() ->
    M1 = key(hlc(100, 0), <<"n1">>, 1),
    M2 = key(hlc(101, 0), <<"n1">>, 2),
    M3 = key(hlc(200, 0), <<"n1">>, 3),
    S0 = apply_ev(
        initial(),
        {put, <<"a">>, lww_register, {set, <<"x">>, hlc(100, 0)}},
        M1
    ),
    S1 = apply_ev(
        S0,
        {put, <<"b">>, lww_register, {set, <<"y">>, hlc(100, 0)}},
        M2
    ),
    S2 = apply_ev(S1, {remove, <<"a">>, [{<<"n1">>, 1}]}, M3),
    ?assertEqual(#{<<"b">> => <<"y">>}, ?MOD:to_value(S2)).

apply_value_delta_set_elem_test() ->
    ?assertEqual(
        #{<<"k">> => <<"v">>},
        ?MOD:apply_value_delta(#{}, {set_elem, <<"k">>, <<"v">>})
    ).

apply_value_delta_remove_elem_test() ->
    ?assertEqual(
        #{},
        ?MOD:apply_value_delta(#{<<"k">> => <<"v">>}, {remove_elem, <<"k">>})
    ).

%% =============================================================================
%% Merge by example
%% =============================================================================

merge_concurrent_puts_unions_dots_and_merges_sub_state_test() ->
    InitVal = fun(V) -> {set, V, hlc(100, 0)} end,
    M1 = key(hlc(100, 0), <<"n1">>, 1),
    M2 = key(hlc(100, 0), <<"n2">>, 1),
    A = apply_ev(initial(), {put, <<"k">>, lww_register, InitVal(<<"a">>)}, M1),
    B = apply_ev(initial(), {put, <<"k">>, lww_register, InitVal(<<"a">>)}, M2),
    M = ?MOD:merge_states(A, B),
    #{entries := E} = M,
    {lww_register, _SubState, AddDots, []} = maps:get(<<"k">>, E),
    ?assertEqual(
        ordsets:from_list([{<<"n1">>, 1}, {<<"n2">>, 1}]),
        AddDots
    ).

merge_remove_on_one_side_keeps_unobserved_add_test() ->
    %% Classic AW property — replica A removed K observing only its
    %% own dot. Replica B's concurrent put is not observed by A's
    %% remove; the merged state keeps B's add.
    InitVal = fun(V) -> {set, V, hlc(100, 0)} end,
    M1 = key(hlc(100, 0), <<"n1">>, 1),
    M2 = key(hlc(100, 0), <<"n2">>, 1),
    M3 = key(hlc(200, 0), <<"n1">>, 2),
    A0 = apply_ev(
        initial(), {put, <<"k">>, lww_register, InitVal(<<"a">>)}, M1
    ),
    A1 = apply_ev(A0, {remove, <<"k">>, [{<<"n1">>, 1}]}, M3),
    B = apply_ev(initial(), {put, <<"k">>, lww_register, InitVal(<<"b">>)}, M2),
    M = ?MOD:merge_states(A1, B),
    #{entries := E} = M,
    {lww_register, _SubState, AddDots, Tombs} = maps:get(<<"k">>, E),
    ?assertEqual([{<<"n2">>, 1}], AddDots),
    ?assertEqual([{<<"n1">>, 1}], Tombs).

merge_strategy_mismatch_crashes_test() ->
    M1 = key(hlc(100, 0), <<"n1">>, 1),
    M2 = key(hlc(100, 0), <<"n2">>, 1),
    A = apply_ev(
        initial(),
        {put, <<"k">>, lww_register, {set, <<"a">>, hlc(100, 0)}},
        M1
    ),
    B = apply_ev(
        initial(),
        {put, <<"k">>, pn_counter, #{counters => #{}, hlc => 0}},
        M2
    ),
    ?assertError(
        {strategy_mismatch, <<"k">>, lww_register, pn_counter},
        ?MOD:merge_states(A, B)
    ).

%% =============================================================================
%% hlc / gc_threshold
%% =============================================================================

hlc_tracks_max_observed_test() ->
    M = key(hlc(500, 0), <<"n1">>, 1),
    S = apply_ev(
        initial(),
        {put, <<"k">>, lww_register, {set, <<"v">>, hlc(100, 0)}},
        M
    ),
    ?assertEqual(hlc(500, 0), ?MOD:hlc(S)).

gc_threshold_includes_sub_fold_threshold_test() ->
    %% pn_counter's gc_threshold == hlc once populated. AW-Map's
    %% gc_threshold should be at least the max of its own HLC and
    %% the sub-fold's threshold.
    M = key(hlc(500, 0), <<"n1">>, 1),
    S = apply_ev(
        initial(),
        {apply, <<"k">>, pn_counter, {inc, 1}},
        M
    ),
    ?assertEqual(hlc(500, 0), ?MOD:gc_threshold(S)).

%% =============================================================================
%% Encode / decode round-trip
%% =============================================================================

encode_decode_initial_test() ->
    S = initial(),
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_live_entry_test() ->
    M = key(hlc(100, 0), <<"n1">>, 1),
    S = apply_ev(
        initial(),
        {put, <<"k">>, lww_register, {set, <<"v">>, hlc(100, 0)}},
        M
    ),
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_tombstoned_entry_test() ->
    M1 = key(hlc(100, 0), <<"n1">>, 1),
    M2 = key(hlc(200, 0), <<"n1">>, 2),
    S0 = apply_ev(
        initial(),
        {put, <<"k">>, lww_register, {set, <<"v">>, hlc(100, 0)}},
        M1
    ),
    S1 = apply_ev(S0, {remove, <<"k">>, [{<<"n1">>, 1}]}, M2),
    ?assertEqual(S1, ?MOD:decode_state(?MOD:encode_state(S1))).

encode_decode_event_put_test() ->
    E = {put, <<"k">>, lww_register, {set, <<"v">>, hlc(100, 0)}},
    ?assertEqual(E, ?MOD:decode_event(?MOD:encode_event(E))).

encode_decode_event_apply_test() ->
    E = {apply, <<"k">>, pn_counter, {inc, 7}},
    ?assertEqual(E, ?MOD:decode_event(?MOD:encode_event(E))).

encode_decode_event_remove_test() ->
    E = {remove, <<"k">>, [{<<"n1">>, 1}, {<<"n2">>, 2}]},
    ?assertEqual(E, ?MOD:decode_event(?MOD:encode_event(E))).

encode_is_canonical_for_equal_states_test() ->
    InitVal = fun(V) -> {set, V, hlc(100, 0)} end,
    M1 = key(hlc(100, 0), <<"n1">>, 1),
    M2 = key(hlc(101, 0), <<"n1">>, 2),
    P1 = apply_ev(
        initial(), {put, <<"a">>, lww_register, InitVal(<<"x">>)}, M1
    ),
    P1b = apply_ev(P1, {put, <<"b">>, lww_register, InitVal(<<"y">>)}, M2),
    P2 = apply_ev(
        initial(), {put, <<"b">>, lww_register, InitVal(<<"y">>)}, M2
    ),
    P2b = apply_ev(P2, {put, <<"a">>, lww_register, InitVal(<<"x">>)}, M1),
    ?assertEqual(?MOD:encode_state(P1b), ?MOD:encode_state(P2b)).

%% =============================================================================
%% Dispatcher passthrough
%% =============================================================================

dispatcher_resolves_shorthand_test() ->
    ?assertEqual(?MOD, bondy_oplog_fold:mod_of(aw_map)).

dispatcher_is_known_test() ->
    ?assertEqual(true, bondy_oplog_fold:is_known(aw_map)).

dispatcher_tag_round_trip_test() ->
    ?assertEqual(11, bondy_oplog_fold:tag_of(aw_map)),
    ?assertEqual(aw_map, bondy_oplog_fold:mod_of_tag(11)).
