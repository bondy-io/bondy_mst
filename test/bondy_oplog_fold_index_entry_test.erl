%% =============================================================================
%% EUnit smoke tests for `bondy_oplog_fold_index_entry`.
%%
%% Covers put/remove transitions, HLC-conditional LWW with the
%% equal-HLC tie-break, idempotent replay, out-of-order convergence,
%% to_value/value_equals_state, hlc/gc_threshold, and encode round-trips.
%% =============================================================================

-module(bondy_oplog_fold_index_entry_test).

-include_lib("eunit/include/eunit.hrl").

-define(MOD, bondy_oplog_fold_index_entry).

%% index_entry reads the HLC from the event payload, not Meta, so Meta is
%% irrelevant here.
-define(META, undefined).

apply_ev(State, Event) ->
    {NewState, none} = ?MOD:apply_event(State, Event, ?META),
    NewState.

%% =============================================================================
%% Initial value
%% =============================================================================

initial_value_test() ->
    ?assertEqual({dead, <<>>, 0}, ?MOD:initial_value()).

%% =============================================================================
%% put / remove transitions
%% =============================================================================

put_goes_live_test() ->
    S = apply_ev(?MOD:initial_value(), {put, <<"cols">>, 5}),
    ?assertEqual({live, <<"cols">>, 5}, S).

pointer_only_put_is_live_with_empty_cols_test() ->
    S = apply_ev(?MOD:initial_value(), {put, <<>>, 5}),
    ?assertEqual({live, <<>>, 5}, S).

remove_after_put_goes_dead_test() ->
    S0 = apply_ev(?MOD:initial_value(), {put, <<"cols">>, 5}),
    S1 = apply_ev(S0, {remove, 9}),
    ?assertEqual({dead, <<>>, 9}, S1).

put_after_remove_revives_test() ->
    S0 = apply_ev(?MOD:initial_value(), {remove, 5}),
    S1 = apply_ev(S0, {put, <<"cols">>, 9}),
    ?assertEqual({live, <<"cols">>, 9}, S1).

higher_hlc_columns_replace_test() ->
    S0 = apply_ev(?MOD:initial_value(), {put, <<"old">>, 5}),
    S1 = apply_ev(S0, {put, <<"new">>, 9}),
    ?assertEqual({live, <<"new">>, 9}, S1).

%% =============================================================================
%% HLC-conditional rejection
%% =============================================================================

older_put_is_rejected_test() ->
    S0 = apply_ev(?MOD:initial_value(), {put, <<"new">>, 9}),
    S1 = apply_ev(S0, {put, <<"old">>, 5}),
    ?assertEqual({live, <<"new">>, 9}, S1).

older_remove_is_rejected_test() ->
    S0 = apply_ev(?MOD:initial_value(), {put, <<"v">>, 9}),
    S1 = apply_ev(S0, {remove, 5}),
    ?assertEqual({live, <<"v">>, 9}, S1).

%% =============================================================================
%% Equal-HLC tie-break (live wins) and convergence
%% =============================================================================

equal_hlc_live_beats_dead_regardless_of_order_test() ->
    Put = {put, <<"v">>, 5},
    Rem = {remove, 5},
    S0 = ?MOD:initial_value(),
    PutThenRemove = apply_ev(apply_ev(S0, Put), Rem),
    RemoveThenPut = apply_ev(apply_ev(S0, Rem), Put),
    ?assertEqual(PutThenRemove, RemoveThenPut),
    ?assertEqual({live, <<"v">>, 5}, PutThenRemove).

out_of_order_delivery_converges_test() ->
    Put = {put, <<"v">>, 5},
    Rem = {remove, 9},
    S0 = ?MOD:initial_value(),
    Forward = apply_ev(apply_ev(S0, Put), Rem),
    Reverse = apply_ev(apply_ev(S0, Rem), Put),
    ?assertEqual(Forward, Reverse),
    ?assertEqual({dead, <<>>, 9}, Forward).

%% =============================================================================
%% Idempotency
%% =============================================================================

replay_put_is_noop_test() ->
    E = {put, <<"v">>, 5},
    S0 = apply_ev(?MOD:initial_value(), E),
    S1 = apply_ev(S0, E),
    ?assertEqual(S0, S1).

replay_remove_is_noop_test() ->
    E = {remove, 5},
    S0 = apply_ev(?MOD:initial_value(), E),
    S1 = apply_ev(S0, E),
    ?assertEqual(S0, S1).

%% =============================================================================
%% to_value / value_equals_state
%% =============================================================================

to_value_live_returns_columns_test() ->
    ?assertEqual(<<"cols">>, ?MOD:to_value({live, <<"cols">>, 5})).

to_value_dead_is_undefined_test() ->
    ?assertEqual(undefined, ?MOD:to_value({dead, <<>>, 5})),
    ?assertEqual(undefined, ?MOD:to_value(?MOD:initial_value())).

value_equals_state_is_true_test() ->
    ?assertEqual(true, ?MOD:value_equals_state()).

%% =============================================================================
%% hlc / gc_threshold
%% =============================================================================

hlc_of_initial_is_zero_test() ->
    ?assertEqual(0, ?MOD:hlc(?MOD:initial_value())).

hlc_tracks_state_test() ->
    ?assertEqual(7, ?MOD:hlc({live, <<"v">>, 7})).

gc_threshold_of_initial_is_undefined_test() ->
    ?assertEqual(undefined, ?MOD:gc_threshold(?MOD:initial_value())).

gc_threshold_of_populated_is_hlc_test() ->
    ?assertEqual(7, ?MOD:gc_threshold({live, <<"v">>, 7})),
    ?assertEqual(9, ?MOD:gc_threshold({dead, <<>>, 9})).

%% =============================================================================
%% merge_states
%% =============================================================================

merge_takes_higher_hlc_test() ->
    A = {live, <<"a">>, 9},
    B = {dead, <<>>, 5},
    ?assertEqual(A, ?MOD:merge_states(A, B)),
    ?assertEqual(A, ?MOD:merge_states(B, A)).

merge_is_idempotent_test() ->
    S = {live, <<"v">>, 5},
    ?assertEqual(S, ?MOD:merge_states(S, S)).

merge_equal_hlc_live_wins_test() ->
    A = {live, <<"v">>, 5},
    B = {dead, <<>>, 5},
    ?assertEqual(A, ?MOD:merge_states(A, B)),
    ?assertEqual(A, ?MOD:merge_states(B, A)).

%% =============================================================================
%% Encode / decode round-trip
%% =============================================================================

encode_decode_state_live_test() ->
    S = {live, <<"cols">>, 5},
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_state_dead_test() ->
    S = {dead, <<>>, 9},
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_state_initial_test() ->
    S = ?MOD:initial_value(),
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_event_put_test() ->
    E = {put, <<"cols">>, 5},
    ?assertEqual(E, ?MOD:decode_event(?MOD:encode_event(E))).

encode_decode_event_remove_test() ->
    E = {remove, 9},
    ?assertEqual(E, ?MOD:decode_event(?MOD:encode_event(E))).

%% =============================================================================
%% Dispatcher registration
%% =============================================================================

dispatcher_resolves_shorthand_test() ->
    ?assertEqual(?MOD, bondy_oplog_fold:mod_of(index_entry)).

dispatcher_is_known_test() ->
    ?assertEqual(true, bondy_oplog_fold:is_known(index_entry)).

dispatcher_value_equals_state_true_test() ->
    ?assertEqual(true, bondy_oplog_fold:value_equals_state(index_entry)).
