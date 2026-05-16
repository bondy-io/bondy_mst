%% =============================================================================
%% EUnit smoke tests for `bondy_oplog_fold_orset`.
%%
%% Covers OR-Set semantics per `FOLD_STRATEGY_DESIGN.md` §4.5: add,
%% remove, observed-remove dot semantics, tombstone retention across
%% out-of-order delivery, merge by example, encode round-trip.
%%
%% Invariants (idempotency, monotonicity, encode round-trip, GC safety,
%% merge commutativity/associativity/idempotency) live in
%% `bondy_oplog_fold_orset_proper_test.erl`.
%% =============================================================================

-module(bondy_oplog_fold_orset_test).

-include_lib("eunit/include/eunit.hrl").

-define(MOD, bondy_oplog_fold_orset).

%% =============================================================================
%% Helpers
%% =============================================================================

hlc(Phys, Log) ->
    bondy_oplog_hlc:encode(Phys, Log).

dot(Node, Counter) ->
    {Node, Counter}.

initial() ->
    ?MOD:initial_value().

%% =============================================================================
%% Initial value
%% =============================================================================

initial_value_test() ->
    ?assertEqual(
        #{live => #{}, tombstones => [], hlc => 0},
        ?MOD:initial_value()
    ).

%% =============================================================================
%% Add semantics
%% =============================================================================

add_to_empty_inserts_element_test() ->
    H = hlc(100, 0),
    D = dot(<<"n1">>, 1),
    S = ?MOD:apply_event(initial(), {add, H, <<"e1">>, D}),
    ?assertEqual(
        #{live => #{<<"e1">> => [D]}, tombstones => [], hlc => H},
        S
    ).

add_same_dot_idempotent_test() ->
    H = hlc(100, 0),
    D = dot(<<"n1">>, 1),
    E = <<"e1">>,
    S1 = ?MOD:apply_event(initial(), {add, H, E, D}),
    S2 = ?MOD:apply_event(S1, {add, H, E, D}),
    ?assertEqual(S1, S2).

add_two_dots_same_element_keeps_both_test() ->
    H = hlc(100, 0),
    D1 = dot(<<"n1">>, 1),
    D2 = dot(<<"n2">>, 1),
    E = <<"e1">>,
    S = lists:foldl(fun(Ev, Acc) -> ?MOD:apply_event(Acc, Ev) end,
                    initial(),
                    [{add, H, E, D1}, {add, H, E, D2}]),
    ?assertEqual(lists:sort([D1, D2]),
                 maps:get(E, maps:get(live, S))).

add_after_tombstone_is_noop_test() ->
    %% A dot already in tombstones cannot be re-added; the application
    %% must allocate a fresh dot to re-add the element.
    H = hlc(100, 0),
    D = dot(<<"n1">>, 1),
    E = <<"e1">>,
    S0 = ?MOD:apply_event(initial(), {remove, H, E, [D]}),
    S1 = ?MOD:apply_event(S0, {add, H, E, D}),
    ?assertEqual(S0, S1).

%% =============================================================================
%% Remove semantics
%% =============================================================================

remove_drops_element_when_all_dots_gone_test() ->
    H = hlc(100, 0),
    D = dot(<<"n1">>, 1),
    E = <<"e1">>,
    S0 = ?MOD:apply_event(initial(), {add, H, E, D}),
    S1 = ?MOD:apply_event(S0, {remove, hlc(200, 0), E, [D]}),
    ?assertEqual(#{}, maps:get(live, S1)),
    ?assertEqual([D], maps:get(tombstones, S1)).

remove_partial_keeps_surviving_dots_test() ->
    H = hlc(100, 0),
    D1 = dot(<<"n1">>, 1),
    D2 = dot(<<"n2">>, 1),
    E = <<"e1">>,
    S0 = lists:foldl(fun(Ev, Acc) -> ?MOD:apply_event(Acc, Ev) end,
                     initial(),
                     [{add, H, E, D1}, {add, H, E, D2}]),
    S1 = ?MOD:apply_event(S0, {remove, hlc(200, 0), E, [D1]}),
    ?assertEqual(#{E => [D2]}, maps:get(live, S1)),
    ?assertEqual([D1], maps:get(tombstones, S1)).

remove_before_add_tombstones_dot_test() ->
    %% Out-of-order: remove arrives before the corresponding add.
    %% Tombstone keeps the dot, so the late-arriving add is rejected.
    H = hlc(100, 0),
    D = dot(<<"n1">>, 1),
    E = <<"e1">>,
    S0 = ?MOD:apply_event(initial(), {remove, H, E, [D]}),
    ?assertEqual([D], maps:get(tombstones, S0)),
    ?assertEqual(#{}, maps:get(live, S0)),
    S1 = ?MOD:apply_event(S0, {add, H, E, D}),
    ?assertEqual([D], maps:get(tombstones, S1)),
    ?assertEqual(#{}, maps:get(live, S1)).

remove_empty_dot_list_only_bumps_hlc_test() ->
    H1 = hlc(100, 0),
    H2 = hlc(200, 0),
    E = <<"e1">>,
    D = dot(<<"n1">>, 1),
    S0 = ?MOD:apply_event(initial(), {add, H1, E, D}),
    S1 = ?MOD:apply_event(S0, {remove, H2, E, []}),
    ?assertEqual(maps:get(live, S0), maps:get(live, S1)),
    ?assertEqual([], maps:get(tombstones, S1)),
    ?assertEqual(H2, maps:get(hlc, S1)).

%% =============================================================================
%% Convergence: concurrent add+remove keeps the unobserved add
%% =============================================================================

concurrent_add_survives_remove_test() ->
    %% Replica A added Dot1; replica B added Dot2 concurrently. Replica
    %% A removes Elem observing only Dot1. Dot2 survives because A
    %% never saw it (classical OR-Set behaviour).
    H = hlc(100, 0),
    D1 = dot(<<"n1">>, 1),
    D2 = dot(<<"n2">>, 1),
    E = <<"e1">>,
    S = lists:foldl(fun(Ev, Acc) -> ?MOD:apply_event(Acc, Ev) end,
                    initial(),
                    [{add, H, E, D1},
                     {add, H, E, D2},
                     {remove, hlc(200, 0), E, [D1]}]),
    ?assertEqual(#{E => [D2]}, maps:get(live, S)),
    ?assertEqual([D1], maps:get(tombstones, S)).

%% =============================================================================
%% hlc/1 and gc_threshold/1
%% =============================================================================

hlc_of_initial_is_zero_test() ->
    ?assertEqual(0, ?MOD:hlc(initial())).

hlc_tracks_max_event_hlc_test() ->
    S = lists:foldl(fun(Ev, Acc) -> ?MOD:apply_event(Acc, Ev) end,
                    initial(),
                    [{add, hlc(100, 0), <<"e">>, dot(<<"n">>, 1)},
                     {add, hlc(50, 0),  <<"e">>, dot(<<"n">>, 2)}]),
    ?assertEqual(hlc(100, 0), ?MOD:hlc(S)).

gc_threshold_of_initial_is_undefined_test() ->
    ?assertEqual(undefined, ?MOD:gc_threshold(initial())).

gc_threshold_after_event_is_hlc_test() ->
    H = hlc(100, 0),
    S = ?MOD:apply_event(initial(), {add, H, <<"e">>, dot(<<"n">>, 1)}),
    ?assertEqual(H, ?MOD:gc_threshold(S)).

%% =============================================================================
%% merge_states/2 — by example
%% =============================================================================

merge_initial_left_returns_right_test() ->
    H = hlc(100, 0),
    B = ?MOD:apply_event(initial(), {add, H, <<"e">>, dot(<<"n">>, 1)}),
    ?assertEqual(B, ?MOD:merge_states(initial(), B)).

merge_initial_right_returns_left_test() ->
    H = hlc(100, 0),
    A = ?MOD:apply_event(initial(), {add, H, <<"e">>, dot(<<"n">>, 1)}),
    ?assertEqual(A, ?MOD:merge_states(A, initial())).

merge_disjoint_adds_unions_live_test() ->
    H = hlc(100, 0),
    A = ?MOD:apply_event(initial(), {add, H, <<"e1">>, dot(<<"n1">>, 1)}),
    B = ?MOD:apply_event(initial(), {add, H, <<"e2">>, dot(<<"n2">>, 1)}),
    AB = ?MOD:merge_states(A, B),
    BA = ?MOD:merge_states(B, A),
    ?assertEqual(AB, BA),
    ?assertEqual(2, map_size(maps:get(live, AB))).

merge_same_element_distinct_dots_unions_dots_test() ->
    H = hlc(100, 0),
    A = ?MOD:apply_event(initial(), {add, H, <<"e">>, dot(<<"n1">>, 1)}),
    B = ?MOD:apply_event(initial(), {add, H, <<"e">>, dot(<<"n2">>, 1)}),
    AB = ?MOD:merge_states(A, B),
    ?assertEqual(
        lists:sort([dot(<<"n1">>, 1), dot(<<"n2">>, 1)]),
        maps:get(<<"e">>, maps:get(live, AB))
    ).

merge_tombstone_drops_dot_from_other_side_test() ->
    %% Replica A has dot1 live; replica B tombstoned dot1 via a remove.
    %% After merge, dot1 is gone from live (tombstone wins).
    H = hlc(100, 0),
    D1 = dot(<<"n1">>, 1),
    E = <<"e">>,
    A = ?MOD:apply_event(initial(), {add, H, E, D1}),
    B = ?MOD:apply_event(initial(), {remove, H, E, [D1]}),
    AB = ?MOD:merge_states(A, B),
    ?assertEqual(#{}, maps:get(live, AB)),
    ?assertEqual([D1], maps:get(tombstones, AB)).

merge_hlc_takes_max_test() ->
    A = ?MOD:apply_event(initial(),
                         {add, hlc(100, 0), <<"e1">>, dot(<<"n1">>, 1)}),
    B = ?MOD:apply_event(initial(),
                         {add, hlc(200, 0), <<"e2">>, dot(<<"n2">>, 1)}),
    AB = ?MOD:merge_states(A, B),
    ?assertEqual(hlc(200, 0), maps:get(hlc, AB)).

%% =============================================================================
%% encode / decode round-trip
%% =============================================================================

encode_decode_state_initial_test() ->
    S = initial(),
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_state_one_element_test() ->
    S = ?MOD:apply_event(initial(),
                         {add, hlc(100, 0), <<"e">>, dot(<<"n">>, 1)}),
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_state_with_tombstones_test() ->
    H = hlc(100, 0),
    D = dot(<<"n">>, 1),
    S0 = ?MOD:apply_event(initial(), {add, H, <<"e">>, D}),
    S1 = ?MOD:apply_event(S0, {remove, hlc(200, 0), <<"e">>, [D]}),
    ?assertEqual(S1, ?MOD:decode_state(?MOD:encode_state(S1))).

encode_decode_state_multi_element_multi_dot_test() ->
    H = hlc(100, 0),
    Events = [
        {add, H, <<"a">>, dot(<<"n1">>, 1)},
        {add, H, <<"a">>, dot(<<"n2">>, 1)},
        {add, H, <<"b">>, dot(<<"n1">>, 2)},
        {remove, hlc(200, 0), <<"b">>, [dot(<<"n1">>, 2)]}
    ],
    S = lists:foldl(fun(E, Acc) -> ?MOD:apply_event(Acc, E) end,
                    initial(), Events),
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_state_is_canonical_test() ->
    %% Two semantically equal states encode to the same bytes — required
    %% for content-addressable storage if we ever hash encoded states.
    H = hlc(100, 0),
    A = lists:foldl(fun(E, Acc) -> ?MOD:apply_event(Acc, E) end, initial(),
                    [{add, H, <<"a">>, dot(<<"n1">>, 1)},
                     {add, H, <<"b">>, dot(<<"n2">>, 1)}]),
    B = lists:foldl(fun(E, Acc) -> ?MOD:apply_event(Acc, E) end, initial(),
                    [{add, H, <<"b">>, dot(<<"n2">>, 1)},
                     {add, H, <<"a">>, dot(<<"n1">>, 1)}]),
    ?assertEqual(A, B),
    ?assertEqual(?MOD:encode_state(A), ?MOD:encode_state(B)).

encode_decode_event_add_test() ->
    E = {add, hlc(100, 0), <<"e">>, dot(<<"n">>, 1)},
    ?assertEqual(E, ?MOD:decode_event(?MOD:encode_event(E))).

encode_decode_event_remove_test() ->
    E = {remove, hlc(100, 0), <<"e">>, [dot(<<"n1">>, 1), dot(<<"n2">>, 2)]},
    ?assertEqual(E, ?MOD:decode_event(?MOD:encode_event(E))).

encode_decode_event_remove_empty_dots_test() ->
    E = {remove, hlc(100, 0), <<"e">>, []},
    ?assertEqual(E, ?MOD:decode_event(?MOD:encode_event(E))).

%% =============================================================================
%% Dispatcher passthrough
%% =============================================================================

dispatcher_resolves_shorthand_test() ->
    ?assertEqual(?MOD, bondy_oplog_fold:mod_of(orset)).

dispatcher_apply_event_via_shorthand_test() ->
    H = hlc(100, 0),
    S = bondy_oplog_fold:apply_event(orset, initial(),
                                     {add, H, <<"e">>, dot(<<"n">>, 1)}),
    ?assertEqual(#{<<"e">> => [dot(<<"n">>, 1)]}, maps:get(live, S)).

dispatcher_merge_states_via_shorthand_test() ->
    H = hlc(100, 0),
    A = ?MOD:apply_event(initial(), {add, H, <<"e1">>, dot(<<"n1">>, 1)}),
    B = ?MOD:apply_event(initial(), {add, H, <<"e2">>, dot(<<"n2">>, 1)}),
    AB = bondy_oplog_fold:merge_states(orset, A, B),
    ?assertEqual(2, map_size(maps:get(live, AB))).
