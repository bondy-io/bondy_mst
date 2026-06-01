%% =============================================================================
%% EUnit smoke tests for `bondy_oplog_fold_lww_register`.
%%
%% Covers the state-machine transitions in `FOLD_STRATEGY_DESIGN.md`
%% §4.2: set semantics, clear semantics, LWW conflict resolution,
%% same-HLC tie-break, merge_states by example, encode round-trip.
%%
%% Invariants (idempotency, monotonicity, encode round-trip, GC safety,
%% merge commutativity/associativity/idempotency) live in
%% `bondy_oplog_fold_lww_register_proper_test.erl`.
%% =============================================================================

-module(bondy_oplog_fold_lww_register_test).

-include_lib("eunit/include/eunit.hrl").

-define(MOD, bondy_oplog_fold_lww_register).

%% =============================================================================
%% Helpers
%% =============================================================================

mk_value(N) ->
    <<"v-", (integer_to_binary(N))/binary>>.

hlc(Phys, Log) ->
    bondy_oplog_hlc:encode(Phys, Log).

%% =============================================================================
%% Initial value
%% =============================================================================

initial_value_is_undefined_test() ->
    ?assertEqual(undefined, ?MOD:initial_value()).

%% =============================================================================
%% Set semantics
%% =============================================================================

undefined_plus_set_becomes_set_test() ->
    H = hlc(100, 0),
    V = mk_value(1),
    ?assertEqual({set, V, H}, apply_ev(undefined, {set, H, V})).

set_plus_newer_set_supersedes_test() ->
    H1 = hlc(100, 0),
    H2 = hlc(200, 0),
    S0 = {set, mk_value(1), H1},
    ?assertEqual(
        {set, mk_value(2), H2},
        apply_ev(S0, {set, H2, mk_value(2)})
    ).

set_plus_older_set_rejected_test() ->
    H1 = hlc(200, 0),
    H2 = hlc(100, 0),
    S0 = {set, mk_value(1), H1},
    ?assertEqual(S0, apply_ev(S0, {set, H2, mk_value(2)})).

set_plus_same_hlc_same_value_idempotent_test() ->
    H = hlc(100, 0),
    V = mk_value(1),
    S0 = {set, V, H},
    ?assertEqual(S0, apply_ev(S0, {set, H, V})).

set_plus_same_hlc_lex_tie_break_test() ->
    %% Tie-break is deterministic: larger lex payload wins, regardless
    %% of arrival order.
    H = hlc(100, 0),
    Vsmall = <<"a">>,
    Vlarge = <<"z">>,
    ?assertEqual(
        {set, Vlarge, H},
        apply_ev({set, Vsmall, H}, {set, H, Vlarge})
    ),
    ?assertEqual(
        {set, Vlarge, H},
        apply_ev({set, Vlarge, H}, {set, H, Vsmall})
    ).

%% =============================================================================
%% Clear semantics
%% =============================================================================

undefined_plus_clear_becomes_cleared_test() ->
    H = hlc(100, 0),
    ?assertEqual({cleared, H}, apply_ev(undefined, {clear, H})).

set_plus_newer_clear_becomes_cleared_test() ->
    H1 = hlc(100, 0),
    H2 = hlc(200, 0),
    S0 = {set, mk_value(1), H1},
    ?assertEqual({cleared, H2}, apply_ev(S0, {clear, H2})).

set_plus_older_clear_rejected_test() ->
    H1 = hlc(200, 0),
    H2 = hlc(100, 0),
    S0 = {set, mk_value(1), H1},
    ?assertEqual(S0, apply_ev(S0, {clear, H2})).

set_plus_same_hlc_clear_clear_wins_test() ->
    %% Tie at same HLC — cleared deterministically wins.
    H = hlc(100, 0),
    ?assertEqual(
        {cleared, H},
        apply_ev({set, mk_value(1), H}, {clear, H})
    ).

cleared_plus_newer_set_resurrects_register_test() ->
    %% LWW: a later-HLC set re-populates the register; cleared is NOT
    %% terminal (this is the key difference from presence_basic).
    H1 = hlc(100, 0),
    H2 = hlc(200, 0),
    V = mk_value(1),
    ?assertEqual(
        {set, V, H2},
        apply_ev({cleared, H1}, {set, H2, V})
    ).

cleared_plus_older_set_rejected_test() ->
    H1 = hlc(200, 0),
    H2 = hlc(100, 0),
    S0 = {cleared, H1},
    ?assertEqual(S0, apply_ev(S0, {set, H2, mk_value(1)})).

cleared_plus_same_hlc_set_cleared_wins_test() ->
    H = hlc(100, 0),
    S0 = {cleared, H},
    ?assertEqual(S0, apply_ev(S0, {set, H, mk_value(1)})).

cleared_plus_clear_bumps_hlc_test() ->
    %% Repeated clear: bump HLC so the cell's last-modified reflects
    %% everything observed.
    H1 = hlc(100, 0),
    H2 = hlc(200, 0),
    ?assertEqual(
        {cleared, H2},
        apply_ev({cleared, H1}, {clear, H2})
    ).

%% =============================================================================
%% hlc/1
%% =============================================================================

hlc_of_undefined_is_zero_test() ->
    ?assertEqual(0, ?MOD:hlc(undefined)).

hlc_of_set_is_state_hlc_test() ->
    H = hlc(100, 0),
    ?assertEqual(H, ?MOD:hlc({set, mk_value(1), H})).

hlc_of_cleared_is_state_hlc_test() ->
    H = hlc(100, 0),
    ?assertEqual(H, ?MOD:hlc({cleared, H})).

%% =============================================================================
%% gc_threshold/1
%% =============================================================================

gc_threshold_of_undefined_is_undefined_test() ->
    ?assertEqual(undefined, ?MOD:gc_threshold(undefined)).

gc_threshold_of_set_is_state_hlc_test() ->
    H = hlc(100, 0),
    ?assertEqual(H, ?MOD:gc_threshold({set, mk_value(1), H})).

gc_threshold_of_cleared_is_state_hlc_test() ->
    H = hlc(100, 0),
    ?assertEqual(H, ?MOD:gc_threshold({cleared, H})).

%% =============================================================================
%% merge_states/2 — by example
%% =============================================================================

merge_undefined_left_returns_right_test() ->
    H = hlc(100, 0),
    B = {set, mk_value(1), H},
    ?assertEqual(B, ?MOD:merge_states(undefined, B)).

merge_undefined_right_returns_left_test() ->
    H = hlc(100, 0),
    A = {set, mk_value(1), H},
    ?assertEqual(A, ?MOD:merge_states(A, undefined)).

merge_two_sets_higher_hlc_wins_test() ->
    A = {set, mk_value(1), hlc(100, 0)},
    B = {set, mk_value(2), hlc(200, 0)},
    ?assertEqual(B, ?MOD:merge_states(A, B)),
    ?assertEqual(B, ?MOD:merge_states(B, A)).

merge_two_sets_same_hlc_lex_tie_break_test() ->
    H = hlc(100, 0),
    A = {set, <<"a">>, H},
    B = {set, <<"z">>, H},
    ?assertEqual(B, ?MOD:merge_states(A, B)),
    ?assertEqual(B, ?MOD:merge_states(B, A)).

merge_set_and_newer_cleared_test() ->
    Set = {set, mk_value(1), hlc(100, 0)},
    Cleared = {cleared, hlc(200, 0)},
    ?assertEqual(Cleared, ?MOD:merge_states(Set, Cleared)),
    ?assertEqual(Cleared, ?MOD:merge_states(Cleared, Set)).

merge_set_and_older_cleared_test() ->
    Set = {set, mk_value(1), hlc(200, 0)},
    Cleared = {cleared, hlc(100, 0)},
    ?assertEqual(Set, ?MOD:merge_states(Set, Cleared)),
    ?assertEqual(Set, ?MOD:merge_states(Cleared, Set)).

merge_set_and_same_hlc_cleared_cleared_wins_test() ->
    H = hlc(100, 0),
    Set = {set, mk_value(1), H},
    Cleared = {cleared, H},
    ?assertEqual(Cleared, ?MOD:merge_states(Set, Cleared)),
    ?assertEqual(Cleared, ?MOD:merge_states(Cleared, Set)).

merge_two_cleared_keeps_max_hlc_test() ->
    A = {cleared, hlc(100, 0)},
    B = {cleared, hlc(200, 0)},
    ?assertEqual(B, ?MOD:merge_states(A, B)),
    ?assertEqual(B, ?MOD:merge_states(B, A)).

%% =============================================================================
%% encode / decode round-trip
%% =============================================================================

encode_decode_state_undefined_test() ->
    ?assertEqual(undefined, ?MOD:decode_state(?MOD:encode_state(undefined))).

encode_decode_state_set_test() ->
    S = {set, mk_value(1), hlc(100, 0)},
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_state_set_empty_value_test() ->
    S = {set, <<>>, hlc(100, 0)},
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_state_cleared_test() ->
    S = {cleared, hlc(200, 0)},
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_event_set_test() ->
    E = {set, hlc(100, 0), mk_value(1)},
    ?assertEqual(E, ?MOD:decode_event(?MOD:encode_event(E))).

encode_decode_event_clear_test() ->
    E = {clear, hlc(200, 0)},
    ?assertEqual(E, ?MOD:decode_event(?MOD:encode_event(E))).

%% =============================================================================
%% Dispatcher passthrough
%% =============================================================================

dispatcher_resolves_shorthand_test() ->
    ?assertEqual(?MOD, bondy_oplog_fold:mod_of(lww_register)).

dispatcher_merge_states_via_shorthand_test() ->
    A = {set, mk_value(1), hlc(100, 0)},
    B = {set, mk_value(2), hlc(200, 0)},
    ?assertEqual(B, bondy_oplog_fold:merge_states(lww_register, A, B)).

%% Wrapper over %`apply_event/3`%; existing folds ignore Meta.
apply_ev(S, E) ->
    {NewState, _Delta} = ?MOD:apply_event(S, E, undefined),
    NewState.
