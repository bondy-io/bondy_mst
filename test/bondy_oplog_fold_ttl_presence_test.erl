%% =============================================================================
%% EUnit smoke tests for `bondy_oplog_fold_ttl_presence`.
%%
%% Covers the state-machine transitions in `FOLD_STRATEGY_DESIGN.md`
%% §4.6: issue, revoke, re-issue after revoke (LWW), is_currently_valid
%% against an external "now" HLC, merge by example, encode round-trip.
%%
%% Invariants (idempotency, monotonicity, encode round-trip, GC safety,
%% merge commutativity/associativity/idempotency) live in
%% `bondy_oplog_fold_ttl_presence_proper_test.erl`.
%% =============================================================================

-module(bondy_oplog_fold_ttl_presence_test).

-include_lib("eunit/include/eunit.hrl").

-define(MOD, bondy_oplog_fold_ttl_presence).

%% =============================================================================
%% Helpers
%% =============================================================================

mk_payload(N) ->
    <<"p-", (integer_to_binary(N))/binary>>.

hlc(Phys, Log) ->
    bondy_oplog_hlc:encode(Phys, Log).

%% =============================================================================
%% Initial value
%% =============================================================================

initial_value_is_undefined_test() ->
    ?assertEqual(undefined, ?MOD:initial_value()).

%% =============================================================================
%% Issue semantics
%% =============================================================================

undefined_plus_issue_becomes_issued_test() ->
    H = hlc(100, 0),
    E = hlc(1000, 0),
    P = mk_payload(1),
    ?assertEqual({issued, H, E, P},
                 apply_ev(undefined, {issue, H, E, P})).

issued_plus_newer_issue_supersedes_test() ->
    H1 = hlc(100, 0),
    H2 = hlc(200, 0),
    E1 = hlc(1000, 0),
    E2 = hlc(2000, 0),
    S0 = {issued, H1, E1, mk_payload(1)},
    ?assertEqual({issued, H2, E2, mk_payload(2)},
                 apply_ev(S0, {issue, H2, E2, mk_payload(2)})).

issued_plus_older_issue_rejected_test() ->
    H1 = hlc(200, 0),
    H2 = hlc(100, 0),
    S0 = {issued, H1, hlc(1000, 0), mk_payload(1)},
    ?assertEqual(S0,
                 apply_ev(S0, {issue, H2, hlc(2000, 0), mk_payload(2)})).

issued_plus_same_hlc_same_data_idempotent_test() ->
    H = hlc(100, 0),
    E = hlc(1000, 0),
    P = mk_payload(1),
    S0 = {issued, H, E, P},
    ?assertEqual(S0, apply_ev(S0, {issue, H, E, P})).

issued_plus_same_hlc_larger_payload_resolves_test() ->
    H = hlc(100, 0),
    E = hlc(1000, 0),
    S0 = {issued, H, E, <<"a">>},
    ?assertEqual({issued, H, E, <<"z">>},
                 apply_ev(S0, {issue, H, E, <<"z">>})).

%% =============================================================================
%% Revoke semantics
%% =============================================================================

undefined_plus_revoke_tombstones_test() ->
    H = hlc(100, 0),
    ?assertEqual({revoked, H}, apply_ev(undefined, {revoke, H})).

issued_plus_newer_revoke_terminates_test() ->
    H1 = hlc(100, 0),
    H2 = hlc(200, 0),
    S0 = {issued, H1, hlc(1000, 0), mk_payload(1)},
    ?assertEqual({revoked, H2}, apply_ev(S0, {revoke, H2})).

issued_plus_same_hlc_revoke_wins_tie_test() ->
    H = hlc(100, 0),
    S0 = {issued, H, hlc(1000, 0), mk_payload(1)},
    ?assertEqual({revoked, H}, apply_ev(S0, {revoke, H})).

issued_plus_older_revoke_rejected_test() ->
    H1 = hlc(200, 0),
    H2 = hlc(100, 0),
    S0 = {issued, H1, hlc(1000, 0), mk_payload(1)},
    ?assertEqual(S0, apply_ev(S0, {revoke, H2})).

revoked_plus_newer_revoke_bumps_hlc_test() ->
    H1 = hlc(100, 0),
    H2 = hlc(200, 0),
    ?assertEqual({revoked, H2},
                 apply_ev({revoked, H1}, {revoke, H2})).

revoked_plus_older_revoke_idempotent_test() ->
    H1 = hlc(200, 0),
    H2 = hlc(100, 0),
    S0 = {revoked, H1},
    ?assertEqual(S0, apply_ev(S0, {revoke, H2})).

%% =============================================================================
%% Re-issue after revoke (deviation from doc §4.6)
%% =============================================================================

revoked_plus_newer_issue_reanimates_test() ->
    %% LWW: a later-HLC `issue` after revoke reanimates the cell. This
    %% supports lease re-grant workflows.
    H1 = hlc(100, 0),
    H2 = hlc(200, 0),
    E = hlc(2000, 0),
    P2 = mk_payload(2),
    S0 = {revoked, H1},
    ?assertEqual({issued, H2, E, P2},
                 apply_ev(S0, {issue, H2, E, P2})).

revoked_plus_older_issue_rejected_test() ->
    H1 = hlc(200, 0),
    H2 = hlc(100, 0),
    S0 = {revoked, H1},
    ?assertEqual(S0, apply_ev(S0, {issue, H2, hlc(2000, 0), mk_payload(2)})).

revoked_plus_same_hlc_issue_revoke_wins_test() ->
    H = hlc(100, 0),
    S0 = {revoked, H},
    ?assertEqual(S0,
                 apply_ev(S0, {issue, H, hlc(2000, 0), mk_payload(2)})).

%% =============================================================================
%% is_currently_valid/2
%% =============================================================================

is_valid_undefined_is_false_test() ->
    ?assertNot(?MOD:is_currently_valid(undefined, hlc(100, 0))).

is_valid_issued_before_expiry_is_true_test() ->
    S = {issued, hlc(100, 0), hlc(1000, 0), mk_payload(1)},
    ?assert(?MOD:is_currently_valid(S, hlc(500, 0))).

is_valid_issued_at_expiry_is_false_test() ->
    E = hlc(1000, 0),
    S = {issued, hlc(100, 0), E, mk_payload(1)},
    ?assertNot(?MOD:is_currently_valid(S, E)).

is_valid_issued_after_expiry_is_false_test() ->
    S = {issued, hlc(100, 0), hlc(1000, 0), mk_payload(1)},
    ?assertNot(?MOD:is_currently_valid(S, hlc(2000, 0))).

is_valid_revoked_is_false_test() ->
    S = {revoked, hlc(100, 0)},
    ?assertNot(?MOD:is_currently_valid(S, hlc(50, 0))).

%% =============================================================================
%% hlc/1 and gc_threshold/1
%% =============================================================================

hlc_of_undefined_is_zero_test() ->
    ?assertEqual(0, ?MOD:hlc(undefined)).

hlc_of_issued_is_state_hlc_test() ->
    H = hlc(100, 0),
    ?assertEqual(H, ?MOD:hlc({issued, H, hlc(1000, 0), mk_payload(1)})).

hlc_of_revoked_is_state_hlc_test() ->
    H = hlc(200, 0),
    ?assertEqual(H, ?MOD:hlc({revoked, H})).

gc_threshold_of_undefined_is_undefined_test() ->
    ?assertEqual(undefined, ?MOD:gc_threshold(undefined)).

gc_threshold_of_issued_is_expiry_test() ->
    %% Doc §4.6: gc_threshold for issued is the expiry, not the issue's
    %% HLC. Lets us drop historic events past the cell's deadline.
    E = hlc(1000, 0),
    ?assertEqual(E,
                 ?MOD:gc_threshold({issued, hlc(100, 0), E, mk_payload(1)})).

gc_threshold_of_revoked_is_state_hlc_test() ->
    H = hlc(200, 0),
    ?assertEqual(H, ?MOD:gc_threshold({revoked, H})).

%% =============================================================================
%% merge_states/2 — by example
%% =============================================================================

merge_undefined_returns_other_test() ->
    H = hlc(100, 0),
    A = {issued, H, hlc(1000, 0), mk_payload(1)},
    ?assertEqual(A, ?MOD:merge_states(undefined, A)),
    ?assertEqual(A, ?MOD:merge_states(A, undefined)).

merge_two_issued_higher_hlc_wins_test() ->
    A = {issued, hlc(100, 0), hlc(1000, 0), mk_payload(1)},
    B = {issued, hlc(200, 0), hlc(2000, 0), mk_payload(2)},
    ?assertEqual(B, ?MOD:merge_states(A, B)),
    ?assertEqual(B, ?MOD:merge_states(B, A)).

merge_two_issued_same_hlc_lex_tie_break_test() ->
    H = hlc(100, 0),
    E = hlc(1000, 0),
    A = {issued, H, E, <<"a">>},
    B = {issued, H, E, <<"z">>},
    ?assertEqual(B, ?MOD:merge_states(A, B)),
    ?assertEqual(B, ?MOD:merge_states(B, A)).

merge_issued_and_newer_revoke_test() ->
    Issued = {issued, hlc(100, 0), hlc(1000, 0), mk_payload(1)},
    Revoked = {revoked, hlc(200, 0)},
    ?assertEqual(Revoked, ?MOD:merge_states(Issued, Revoked)),
    ?assertEqual(Revoked, ?MOD:merge_states(Revoked, Issued)).

merge_issued_and_older_revoke_re_issued_dominates_test() ->
    Issued = {issued, hlc(200, 0), hlc(2000, 0), mk_payload(2)},
    Revoked = {revoked, hlc(100, 0)},
    ?assertEqual(Issued, ?MOD:merge_states(Issued, Revoked)),
    ?assertEqual(Issued, ?MOD:merge_states(Revoked, Issued)).

merge_issued_and_same_hlc_revoke_revoke_wins_test() ->
    H = hlc(100, 0),
    Issued = {issued, H, hlc(1000, 0), mk_payload(1)},
    Revoked = {revoked, H},
    ?assertEqual(Revoked, ?MOD:merge_states(Issued, Revoked)),
    ?assertEqual(Revoked, ?MOD:merge_states(Revoked, Issued)).

merge_two_revoked_higher_hlc_wins_test() ->
    A = {revoked, hlc(100, 0)},
    B = {revoked, hlc(200, 0)},
    ?assertEqual(B, ?MOD:merge_states(A, B)),
    ?assertEqual(B, ?MOD:merge_states(B, A)).

%% =============================================================================
%% encode / decode round-trip
%% =============================================================================

encode_decode_state_undefined_test() ->
    ?assertEqual(undefined, ?MOD:decode_state(?MOD:encode_state(undefined))).

encode_decode_state_issued_test() ->
    S = {issued, hlc(100, 0), hlc(1000, 0), mk_payload(1)},
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_state_issued_empty_payload_test() ->
    S = {issued, hlc(100, 0), hlc(1000, 0), <<>>},
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_state_revoked_test() ->
    S = {revoked, hlc(200, 0)},
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_event_issue_test() ->
    E = {issue, hlc(100, 0), hlc(1000, 0), mk_payload(1)},
    ?assertEqual(E, ?MOD:decode_event(?MOD:encode_event(E))).

encode_decode_event_revoke_test() ->
    E = {revoke, hlc(200, 0)},
    ?assertEqual(E, ?MOD:decode_event(?MOD:encode_event(E))).

%% =============================================================================
%% Dispatcher passthrough
%% =============================================================================

dispatcher_resolves_shorthand_test() ->
    ?assertEqual(?MOD, bondy_oplog_fold:mod_of(ttl_presence)).

dispatcher_apply_event_via_shorthand_test() ->
    H = hlc(100, 0),
    E = hlc(1000, 0),
    P = mk_payload(1),
    ?assertEqual(
        {{issued, H, E, P}, P},
        bondy_oplog_fold:apply_event(ttl_presence, undefined,
                                     {issue, H, E, P}, undefined)
    ).

dispatcher_merge_states_via_shorthand_test() ->
    A = {issued, hlc(100, 0), hlc(1000, 0), mk_payload(1)},
    B = {issued, hlc(200, 0), hlc(2000, 0), mk_payload(2)},
    ?assertEqual(B, bondy_oplog_fold:merge_states(ttl_presence, A, B)).

%% Wrapper over %`apply_event/3`%; existing folds ignore Meta.
apply_ev(S, E) ->
    {NewState, _Delta} = ?MOD:apply_event(S, E, undefined),
    NewState.
