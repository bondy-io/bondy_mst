%% =============================================================================
%% EUnit smoke tests for `bondy_oplog_fold_g_set`.
%%
%% Covers `apply_event/3` ordset insertion + HLC tracking, idempotency
%% on repeated adds, `to_value/1`, `merge_states/2` via ordsets:union,
%% `value_equals_state/0 -> true`, encode round-trip.
%% =============================================================================

-module(bondy_oplog_fold_g_set_test).

-include_lib("eunit/include/eunit.hrl").

-define(MOD, bondy_oplog_fold_g_set).

hlc(Phys, Log) ->
    bondy_oplog_hlc:encode(Phys, Log).

key(Hlc) ->
    bondy_oplog_event:key(Hlc, <<"o">>, 0).

%% Wrapper that discards the value-delta (G-Set declares
%% `value_equals_state/0 -> true` and never emits a delta).
apply_ev(State, Event, Meta) ->
    {NewState, none} = bondy_oplog_fold_g_set:apply_event(State, Event, Meta),
    NewState.

%% =============================================================================
%% Initial value
%% =============================================================================

initial_value_test() ->
    ?assertEqual({[], 0}, ?MOD:initial_value()).

%% =============================================================================
%% apply_event/3
%% =============================================================================

add_single_element_test() ->
    H = hlc(100, 0),
    S = apply_ev(?MOD:initial_value(), {add, <<"a">>}, key(H)),
    ?assertEqual({[<<"a">>], H}, S).

add_multiple_elements_preserves_ordset_order_test() ->
    H1 = hlc(100, 0),
    H2 = hlc(101, 0),
    H3 = hlc(102, 0),
    S0 = ?MOD:initial_value(),
    S1 = apply_ev(S0, {add, <<"c">>}, key(H1)),
    S2 = apply_ev(S1, {add, <<"a">>}, key(H2)),
    S3 = apply_ev(S2, {add, <<"b">>}, key(H3)),
    ?assertEqual({[<<"a">>, <<"b">>, <<"c">>], H3}, S3).

duplicate_add_is_idempotent_on_set_test() ->
    H1 = hlc(100, 0),
    H2 = hlc(200, 0),
    S0 = apply_ev(?MOD:initial_value(), {add, <<"a">>}, key(H1)),
    %% Re-add the same element at a higher HLC: set unchanged, HLC bumps.
    S1 = apply_ev(S0, {add, <<"a">>}, key(H2)),
    ?assertEqual({[<<"a">>], H2}, S1).

older_hlc_event_keeps_max_hlc_test() ->
    H1 = hlc(200, 0),
    H2 = hlc(100, 0),
    S0 = apply_ev(?MOD:initial_value(), {add, <<"a">>}, key(H1)),
    S1 = apply_ev(S0, {add, <<"b">>}, key(H2)),
    ?assertEqual({[<<"a">>, <<"b">>], H1}, S1).

%% =============================================================================
%% to_value/1, value_equals_state/0
%% =============================================================================

to_value_returns_ordset_test() ->
    ?assertEqual([], ?MOD:to_value(?MOD:initial_value())),
    ?assertEqual(
        [<<"a">>, <<"b">>],
        ?MOD:to_value({[<<"a">>, <<"b">>], hlc(100, 0)})
    ).

value_equals_state_is_true_test() ->
    ?assertEqual(true, ?MOD:value_equals_state()).

%% =============================================================================
%% merge_states/2 — ordsets:union
%% =============================================================================

merge_unions_sets_and_takes_max_hlc_test() ->
    A = {[<<"a">>, <<"b">>], hlc(200, 0)},
    B = {[<<"b">>, <<"c">>], hlc(100, 0)},
    ?assertEqual(
        {[<<"a">>, <<"b">>, <<"c">>], hlc(200, 0)},
        ?MOD:merge_states(A, B)
    ),
    ?assertEqual(
        {[<<"a">>, <<"b">>, <<"c">>], hlc(200, 0)},
        ?MOD:merge_states(B, A)
    ).

merge_disjoint_sets_test() ->
    A = {[<<"a">>], hlc(100, 0)},
    B = {[<<"z">>], hlc(101, 0)},
    ?assertEqual(
        {[<<"a">>, <<"z">>], hlc(101, 0)},
        ?MOD:merge_states(A, B)
    ).

merge_same_state_is_idempotent_test() ->
    S = {[<<"a">>, <<"b">>], hlc(100, 0)},
    ?assertEqual(S, ?MOD:merge_states(S, S)).

%% =============================================================================
%% hlc/gc_threshold
%% =============================================================================

hlc_of_initial_is_zero_test() ->
    ?assertEqual(0, ?MOD:hlc(?MOD:initial_value())).

hlc_of_populated_test() ->
    H = hlc(100, 0),
    ?assertEqual(H, ?MOD:hlc({[<<"a">>], H})).

gc_threshold_of_initial_is_undefined_test() ->
    ?assertEqual(undefined, ?MOD:gc_threshold(?MOD:initial_value())).

gc_threshold_of_populated_is_hlc_test() ->
    H = hlc(100, 0),
    ?assertEqual(H, ?MOD:gc_threshold({[<<"a">>], H})).

%% =============================================================================
%% Encode / decode round-trip
%% =============================================================================

encode_decode_initial_test() ->
    S = ?MOD:initial_value(),
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_single_element_test() ->
    S = {[<<"a">>], hlc(100, 0)},
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_many_elements_test() ->
    Elems = [<<"e-", (integer_to_binary(N))/binary>> || N <- lists:seq(1, 32)],
    Set = ordsets:from_list(Elems),
    S = {Set, hlc(500, 0)},
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_empty_element_test() ->
    S = {[<<>>], hlc(100, 0)},
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_event_test() ->
    E = {add, <<"hello">>},
    ?assertEqual(E, ?MOD:decode_event(?MOD:encode_event(E))).

%% =============================================================================
%% Dispatcher
%% =============================================================================

dispatcher_resolves_shorthand_test() ->
    ?assertEqual(?MOD, bondy_oplog_fold:mod_of(g_set)).

dispatcher_is_known_test() ->
    ?assertEqual(true, bondy_oplog_fold:is_known(g_set)).

dispatcher_value_equals_state_true_test() ->
    ?assertEqual(true, bondy_oplog_fold:value_equals_state(g_set)).
