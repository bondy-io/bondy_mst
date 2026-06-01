%% =============================================================================
%% EUnit smoke tests for `bondy_oplog_fold_min_register`.
%%
%% Mirror of Max-Register suite with min-merge on the value field. HLC
%% is still merged by max (last-modified metadata).
%% =============================================================================

-module(bondy_oplog_fold_min_register_test).

-include_lib("eunit/include/eunit.hrl").

-define(MOD, bondy_oplog_fold_min_register).

hlc(Phys, Log) ->
    bondy_oplog_hlc:encode(Phys, Log).

key(Hlc) ->
    bondy_oplog_event:key(Hlc, <<"o">>, 0).

apply_ev(State, Event, Meta) ->
    {NewState, _Delta} =
        bondy_oplog_fold_min_register:apply_event(State, Event, Meta),
    NewState.

%% =============================================================================
%% Initial value
%% =============================================================================

initial_value_is_undefined_test() ->
    ?assertEqual(undefined, ?MOD:initial_value()).

%% =============================================================================
%% apply_event/3
%% =============================================================================

first_set_records_value_and_hlc_test() ->
    H = hlc(100, 0),
    ?assertEqual(
        {5, H},
        apply_ev(undefined, {set, 5}, key(H))
    ).

lower_value_supersedes_test() ->
    H1 = hlc(100, 0),
    H2 = hlc(200, 0),
    S0 = {5, H1},
    ?assertEqual({1, H2}, apply_ev(S0, {set, 1}, key(H2))).

higher_value_is_min_noop_but_hlc_advances_test() ->
    H1 = hlc(100, 0),
    H2 = hlc(200, 0),
    S0 = {1, H1},
    ?assertEqual({1, H2}, apply_ev(S0, {set, 9}, key(H2))).

equal_value_idempotent_on_value_test() ->
    H = hlc(100, 0),
    S0 = {5, H},
    ?assertEqual(S0, apply_ev(S0, {set, 5}, key(H))).

negative_values_accepted_test() ->
    H = hlc(100, 0),
    ?assertEqual(
        {-9, H},
        apply_ev({-1, H}, {set, -9}, key(H))
    ).

%% =============================================================================
%% to_value/1
%% =============================================================================

to_value_undefined_is_undefined_test() ->
    ?assertEqual(undefined, ?MOD:to_value(undefined)).

to_value_strips_hlc_test() ->
    ?assertEqual(7, ?MOD:to_value({7, hlc(100, 0)})).

%% =============================================================================
%% merge_states/2
%% =============================================================================

merge_undefined_returns_other_test() ->
    A = {5, hlc(100, 0)},
    ?assertEqual(A, ?MOD:merge_states(undefined, A)),
    ?assertEqual(A, ?MOD:merge_states(A, undefined)).

merge_takes_min_value_and_max_hlc_test() ->
    A = {5, hlc(200, 0)},
    B = {9, hlc(100, 0)},
    %% Value min = 5 (from A); HLC max = 200 (from A).
    ?assertEqual({5, hlc(200, 0)}, ?MOD:merge_states(A, B)),
    ?assertEqual({5, hlc(200, 0)}, ?MOD:merge_states(B, A)).

%% =============================================================================
%% hlc/gc_threshold
%% =============================================================================

hlc_of_undefined_is_zero_test() ->
    ?assertEqual(0, ?MOD:hlc(undefined)).

hlc_of_populated_test() ->
    H = hlc(100, 0),
    ?assertEqual(H, ?MOD:hlc({5, H})).

gc_threshold_of_undefined_is_undefined_test() ->
    ?assertEqual(undefined, ?MOD:gc_threshold(undefined)).

gc_threshold_of_populated_is_hlc_test() ->
    H = hlc(100, 0),
    ?assertEqual(H, ?MOD:gc_threshold({5, H})).

%% =============================================================================
%% encode / decode round-trip
%% =============================================================================

encode_decode_undefined_test() ->
    ?assertEqual(undefined, ?MOD:decode_state(?MOD:encode_state(undefined))).

encode_decode_positive_test() ->
    S = {1234, hlc(100, 0)},
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_negative_test() ->
    S = {-1234, hlc(100, 0)},
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_event_test() ->
    E1 = {set, 7},
    E2 = {set, -3},
    ?assertEqual(E1, ?MOD:decode_event(?MOD:encode_event(E1))),
    ?assertEqual(E2, ?MOD:decode_event(?MOD:encode_event(E2))).

%% =============================================================================
%% Dispatcher
%% =============================================================================

dispatcher_resolves_shorthand_test() ->
    ?assertEqual(?MOD, bondy_oplog_fold:mod_of(min_register)).

dispatcher_is_known_test() ->
    ?assertEqual(true, bondy_oplog_fold:is_known(min_register)).
