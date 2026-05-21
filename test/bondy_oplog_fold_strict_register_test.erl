%% =============================================================================
%% EUnit smoke tests for `bondy_oplog_fold_strict_register`.
%%
%% Covers the state-machine transitions in `FOLD_STRATEGY_DESIGN.md`
%% §4.3: set semantics, conflict surfacing on same-HLC distinct value,
%% revoke (terminal), resolve (admin escape from conflict), merge by
%% example, encode round-trip.
%%
%% Invariants (idempotency, HLC monotonicity, encode round-trip, GC
%% safety, merge commutativity/associativity/idempotency) live in
%% `bondy_oplog_fold_strict_register_proper_test.erl`.
%% =============================================================================

-module(bondy_oplog_fold_strict_register_test).

-include_lib("eunit/include/eunit.hrl").

-define(MOD, bondy_oplog_fold_strict_register).

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

set_plus_same_hlc_distinct_value_surfaces_conflict_test() ->
    %% Strict-register's defining behaviour: do NOT silently pick one.
    H = hlc(100, 0),
    V1 = <<"a">>,
    V2 = <<"z">>,
    S0 = {set, V1, H},
    ?assertEqual(
        {conflict, lists:usort([{V1, H}, {V2, H}])},
        apply_ev(S0, {set, H, V2})
    ).

%% =============================================================================
%% Revoke semantics (terminal)
%% =============================================================================

undefined_plus_revoke_tombstones_test() ->
    H = hlc(100, 0),
    ?assertEqual({revoked, H}, apply_ev(undefined, {revoke, H})).

set_plus_newer_revoke_becomes_revoked_test() ->
    H1 = hlc(100, 0),
    H2 = hlc(200, 0),
    S0 = {set, mk_value(1), H1},
    ?assertEqual({revoked, H2}, apply_ev(S0, {revoke, H2})).

set_plus_same_hlc_revoke_wins_tie_test() ->
    %% Tie at same HLC — revoke (security-critical) deterministically wins.
    H = hlc(100, 0),
    S0 = {set, mk_value(1), H},
    ?assertEqual({revoked, H}, apply_ev(S0, {revoke, H})).

set_plus_older_revoke_rejected_test() ->
    H1 = hlc(200, 0),
    H2 = hlc(100, 0),
    S0 = {set, mk_value(1), H1},
    ?assertEqual(S0, apply_ev(S0, {revoke, H2})).

revoked_plus_newer_set_stays_revoked_test() ->
    %% Strict-register's revoke is TERMINAL: later sets don't resurrect,
    %% they only bump HLC. This is required for CRDT-safe merge.
    H1 = hlc(100, 0),
    H2 = hlc(200, 0),
    S0 = {revoked, H1},
    ?assertEqual({revoked, H2}, apply_ev(S0, {set, H2, mk_value(1)})).

revoked_plus_older_set_idempotent_test() ->
    H1 = hlc(200, 0),
    H2 = hlc(100, 0),
    S0 = {revoked, H1},
    ?assertEqual(S0, apply_ev(S0, {set, H2, mk_value(1)})).

revoked_plus_revoke_bumps_hlc_test() ->
    H1 = hlc(100, 0),
    H2 = hlc(200, 0),
    ?assertEqual({revoked, H2},
                 apply_ev({revoked, H1}, {revoke, H2})).

revoked_plus_resolve_stays_revoked_test() ->
    %% Even admin resolve cannot escape revoked (terminal).
    H1 = hlc(100, 0),
    H2 = hlc(200, 0),
    S0 = {revoked, H1},
    ?assertEqual({revoked, H2},
                 apply_ev(S0, {resolve, H2, mk_value(1)})).

%% =============================================================================
%% Resolve semantics
%% =============================================================================

undefined_plus_resolve_becomes_set_test() ->
    H = hlc(100, 0),
    V = mk_value(1),
    ?assertEqual({set, V, H}, apply_ev(undefined, {resolve, H, V})).

set_plus_newer_resolve_overrides_test() ->
    H1 = hlc(100, 0),
    H2 = hlc(200, 0),
    S0 = {set, mk_value(1), H1},
    ?assertEqual({set, mk_value(2), H2},
                 apply_ev(S0, {resolve, H2, mk_value(2)})).

set_plus_older_resolve_rejected_test() ->
    H1 = hlc(200, 0),
    H2 = hlc(100, 0),
    S0 = {set, mk_value(1), H1},
    ?assertEqual(S0, apply_ev(S0, {resolve, H2, mk_value(2)})).

%% =============================================================================
%% Conflict transitions
%% =============================================================================

conflict_plus_existing_entry_idempotent_test() ->
    H = hlc(100, 0),
    V1 = <<"a">>,
    V2 = <<"z">>,
    S0 = {conflict, lists:usort([{V1, H}, {V2, H}])},
    ?assertEqual(S0, apply_ev(S0, {set, H, V1})).

conflict_plus_new_entry_grows_conflict_test() ->
    H = hlc(100, 0),
    V1 = <<"a">>,
    V2 = <<"m">>,
    V3 = <<"z">>,
    S0 = {conflict, lists:usort([{V1, H}, {V2, H}])},
    ?assertEqual(
        {conflict, lists:usort([{V1, H}, {V2, H}, {V3, H}])},
        apply_ev(S0, {set, H, V3})
    ).

conflict_plus_resolve_at_or_above_max_collapses_test() ->
    H1 = hlc(100, 0),
    H2 = hlc(200, 0),
    S0 = {conflict, [{<<"a">>, H1}, {<<"z">>, H1}]},
    ?assertEqual({set, mk_value(7), H2},
                 apply_ev(S0, {resolve, H2, mk_value(7)})),
    %% Tied HLC: resolve still accepted at >= max.
    ?assertEqual({set, mk_value(7), H1},
                 apply_ev(S0, {resolve, H1, mk_value(7)})).

conflict_plus_resolve_below_max_rejected_test() ->
    H_max = hlc(200, 0),
    H_old = hlc(100, 0),
    S0 = {conflict, [{<<"a">>, H_max}, {<<"z">>, H_max}]},
    ?assertEqual(S0,
                 apply_ev(S0, {resolve, H_old, mk_value(7)})).

conflict_plus_revoke_at_or_above_max_terminates_test() ->
    H1 = hlc(100, 0),
    H2 = hlc(200, 0),
    S0 = {conflict, [{<<"a">>, H1}, {<<"z">>, H1}]},
    ?assertEqual({revoked, H2}, apply_ev(S0, {revoke, H2})),
    ?assertEqual({revoked, H1}, apply_ev(S0, {revoke, H1})).

conflict_plus_revoke_below_max_rejected_test() ->
    H_max = hlc(200, 0),
    H_old = hlc(100, 0),
    S0 = {conflict, [{<<"a">>, H_max}, {<<"z">>, H_max}]},
    ?assertEqual(S0, apply_ev(S0, {revoke, H_old})).

%% =============================================================================
%% hlc/1
%% =============================================================================

hlc_of_undefined_is_zero_test() ->
    ?assertEqual(0, ?MOD:hlc(undefined)).

hlc_of_set_is_state_hlc_test() ->
    H = hlc(100, 0),
    ?assertEqual(H, ?MOD:hlc({set, mk_value(1), H})).

hlc_of_revoked_is_state_hlc_test() ->
    H = hlc(100, 0),
    ?assertEqual(H, ?MOD:hlc({revoked, H})).

hlc_of_conflict_is_max_entry_hlc_test() ->
    H1 = hlc(100, 0),
    H2 = hlc(200, 0),
    ?assertEqual(H2,
                 ?MOD:hlc({conflict, [{<<"a">>, H1}, {<<"b">>, H2}]})).

%% =============================================================================
%% gc_threshold/1
%% =============================================================================

gc_threshold_of_undefined_is_undefined_test() ->
    ?assertEqual(undefined, ?MOD:gc_threshold(undefined)).

gc_threshold_of_set_is_state_hlc_test() ->
    H = hlc(100, 0),
    ?assertEqual(H, ?MOD:gc_threshold({set, mk_value(1), H})).

gc_threshold_of_revoked_is_state_hlc_test() ->
    H = hlc(100, 0),
    ?assertEqual(H, ?MOD:gc_threshold({revoked, H})).

gc_threshold_of_conflict_is_max_entry_hlc_test() ->
    H1 = hlc(100, 0),
    H2 = hlc(200, 0),
    ?assertEqual(
        H2,
        ?MOD:gc_threshold({conflict, [{<<"a">>, H1}, {<<"b">>, H2}]})
    ).

%% =============================================================================
%% merge_states/2 — by example
%% =============================================================================

merge_undefined_returns_other_test() ->
    H = hlc(100, 0),
    A = {set, mk_value(1), H},
    ?assertEqual(A, ?MOD:merge_states(undefined, A)),
    ?assertEqual(A, ?MOD:merge_states(A, undefined)).

merge_two_sets_higher_hlc_wins_test() ->
    A = {set, mk_value(1), hlc(100, 0)},
    B = {set, mk_value(2), hlc(200, 0)},
    ?assertEqual(B, ?MOD:merge_states(A, B)),
    ?assertEqual(B, ?MOD:merge_states(B, A)).

merge_two_sets_same_hlc_same_value_idempotent_test() ->
    H = hlc(100, 0),
    A = {set, mk_value(1), H},
    ?assertEqual(A, ?MOD:merge_states(A, A)).

merge_two_sets_same_hlc_distinct_value_surfaces_conflict_test() ->
    H = hlc(100, 0),
    A = {set, <<"a">>, H},
    B = {set, <<"z">>, H},
    Expected = {conflict, lists:usort([{<<"a">>, H}, {<<"z">>, H}])},
    ?assertEqual(Expected, ?MOD:merge_states(A, B)),
    ?assertEqual(Expected, ?MOD:merge_states(B, A)).

merge_set_and_conflict_folds_into_conflict_test() ->
    H = hlc(100, 0),
    Set = {set, <<"x">>, H},
    Conf = {conflict, [{<<"a">>, H}, {<<"z">>, H}]},
    Expected = {conflict, lists:usort([{<<"a">>, H}, {<<"x">>, H}, {<<"z">>, H}])},
    ?assertEqual(Expected, ?MOD:merge_states(Set, Conf)),
    ?assertEqual(Expected, ?MOD:merge_states(Conf, Set)).

merge_two_conflicts_unions_entries_test() ->
    H = hlc(100, 0),
    A = {conflict, [{<<"a">>, H}, {<<"b">>, H}]},
    B = {conflict, [{<<"b">>, H}, {<<"c">>, H}]},
    Expected = {conflict, lists:usort([{<<"a">>, H}, {<<"b">>, H}, {<<"c">>, H}])},
    ?assertEqual(Expected, ?MOD:merge_states(A, B)),
    ?assertEqual(Expected, ?MOD:merge_states(B, A)).

merge_revoked_dominates_set_test() ->
    Set = {set, mk_value(1), hlc(200, 0)},
    Rev = {revoked, hlc(100, 0)},
    %% Revoke dominates regardless of which side has higher HLC; HLC of
    %% the result is the max so the invariant `hlc(merge) >= max(hlc A,
    %% hlc B)` holds.
    ?assertEqual({revoked, hlc(200, 0)}, ?MOD:merge_states(Set, Rev)),
    ?assertEqual({revoked, hlc(200, 0)}, ?MOD:merge_states(Rev, Set)).

merge_revoked_dominates_conflict_test() ->
    Conf = {conflict, [{<<"a">>, hlc(100, 0)}, {<<"b">>, hlc(200, 0)}]},
    Rev = {revoked, hlc(50, 0)},
    ?assertEqual({revoked, hlc(200, 0)}, ?MOD:merge_states(Conf, Rev)),
    ?assertEqual({revoked, hlc(200, 0)}, ?MOD:merge_states(Rev, Conf)).

merge_two_revoked_takes_max_hlc_test() ->
    A = {revoked, hlc(100, 0)},
    B = {revoked, hlc(200, 0)},
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

encode_decode_state_conflict_test() ->
    H = hlc(100, 0),
    S = {conflict, lists:usort([{<<"a">>, H}, {<<"z">>, H}])},
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_state_revoked_test() ->
    S = {revoked, hlc(200, 0)},
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_event_set_test() ->
    E = {set, hlc(100, 0), mk_value(1)},
    ?assertEqual(E, ?MOD:decode_event(?MOD:encode_event(E))).

encode_decode_event_revoke_test() ->
    E = {revoke, hlc(200, 0)},
    ?assertEqual(E, ?MOD:decode_event(?MOD:encode_event(E))).

encode_decode_event_resolve_test() ->
    E = {resolve, hlc(200, 0), mk_value(1)},
    ?assertEqual(E, ?MOD:decode_event(?MOD:encode_event(E))).

%% =============================================================================
%% Dispatcher passthrough
%% =============================================================================

dispatcher_resolves_shorthand_test() ->
    ?assertEqual(?MOD, bondy_oplog_fold:mod_of(strict_register)).

dispatcher_apply_event_via_shorthand_test() ->
    H = hlc(100, 0),
    V = mk_value(1),
    ?assertEqual(
        {{set, V, H}, V},
        bondy_oplog_fold:apply_event(strict_register, undefined,
                                     {set, H, V}, undefined)
    ).

dispatcher_merge_states_via_shorthand_test() ->
    A = {set, mk_value(1), hlc(100, 0)},
    B = {set, mk_value(2), hlc(200, 0)},
    ?assertEqual(B, bondy_oplog_fold:merge_states(strict_register, A, B)).

%% Wrapper over %`apply_event/3`%; existing folds ignore Meta.
apply_ev(S, E) ->
    {NewState, _Delta} = ?MOD:apply_event(S, E, undefined),
    NewState.
