%% =============================================================================
%% EUnit smoke tests for `bondy_oplog_fold_presence_basic`.
%%
%% Covers the state-machine transitions in `FOLD_STRATEGY_DESIGN.md` §4.1:
%% empty -> live, live -> dead, idempotent re-create, out-of-order create
%% rejection, terminal dead, encode round-trip, gc_threshold semantics.
%%
%% PropEr-driven invariants (idempotency, HLC monotonicity, GC safety)
%% live in `bondy_oplog_fold_presence_basic_proper_test.erl`.
%% =============================================================================

-module(bondy_oplog_fold_presence_basic_test).

-include_lib("eunit/include/eunit.hrl").

-define(MOD, bondy_oplog_fold_presence_basic).

%% =============================================================================
%% Helpers
%% =============================================================================

mk_payload(N) ->
    <<"payload-", (integer_to_binary(N))/binary>>.

hlc(Phys, Log) ->
    bondy_oplog_hlc:encode(Phys, Log).

%% =============================================================================
%% State machine
%% =============================================================================

initial_value_is_empty_test() ->
    ?assertEqual(empty, ?MOD:initial_value()).

empty_plus_create_becomes_live_test() ->
    H = hlc(100, 0),
    P = mk_payload(1),
    ?assertEqual({live, H, P}, apply_ev(empty, {create, H, P})).

empty_plus_delete_tombstones_test() ->
    %% Delete arriving before create produces a tombstone — a smaller-
    %% HLC create arriving later must not silently resurrect the cell.
    H = hlc(50, 0),
    ?assertEqual({dead, H}, apply_ev(empty, {delete, H})).

live_plus_create_supersedes_with_higher_hlc_test() ->
    H1 = hlc(100, 0),
    H2 = hlc(200, 0),
    S0 = {live, H1, mk_payload(1)},
    ?assertEqual(
        {live, H2, mk_payload(2)},
        apply_ev(S0, {create, H2, mk_payload(2)})
    ).

live_plus_create_idempotent_at_same_hlc_test() ->
    H = hlc(100, 0),
    P = mk_payload(1),
    S0 = {live, H, P},
    ?assertEqual(S0, apply_ev(S0, {create, H, P})).

live_plus_older_create_rejected_test() ->
    H1 = hlc(200, 0),
    H2 = hlc(100, 0),
    S0 = {live, H1, mk_payload(1)},
    ?assertEqual(S0, apply_ev(S0, {create, H2, mk_payload(2)})).

live_plus_delete_becomes_dead_test() ->
    H1 = hlc(100, 0),
    H2 = hlc(200, 0),
    S0 = {live, H1, mk_payload(1)},
    ?assertEqual({dead, H2}, apply_ev(S0, {delete, H2})).

live_plus_older_delete_preserves_monotonic_hlc_test() ->
    %% Delete arrives with a causally older HLC than live's; dead's HLC
    %% must be max(live, delete) to preserve monotonicity.
    H_live = hlc(200, 0),
    H_del  = hlc(100, 0),
    S0 = {live, H_live, mk_payload(1)},
    ?assertEqual({dead, H_live}, apply_ev(S0, {delete, H_del})).

dead_plus_newer_create_stays_dead_but_bumps_hlc_test() ->
    %% Dead is terminal — never resurrects. The cell HLC bumps to the
    %% newer event's HLC so `last_modified_hlc` reflects everything
    %% the cell has observed.
    H1 = hlc(100, 0),
    H2 = hlc(300, 0),
    ?assertEqual(
        {dead, H2},
        apply_ev({dead, H1}, {create, H2, mk_payload(1)})
    ).

dead_plus_older_create_idempotent_test() ->
    %% Older-HLC create on dead: terminal, HLC unchanged.
    H1 = hlc(300, 0),
    H2 = hlc(100, 0),
    S0 = {dead, H1},
    ?assertEqual(S0, apply_ev(S0, {create, H2, mk_payload(1)})).

dead_plus_newer_delete_bumps_hlc_test() ->
    H1 = hlc(100, 0),
    H2 = hlc(300, 0),
    ?assertEqual(
        {dead, H2},
        apply_ev({dead, H1}, {delete, H2})
    ).

dead_plus_older_delete_idempotent_test() ->
    H1 = hlc(300, 0),
    H2 = hlc(100, 0),
    S0 = {dead, H1},
    ?assertEqual(S0, apply_ev(S0, {delete, H2})).

%% =============================================================================
%% hlc/1
%% =============================================================================

hlc_of_empty_is_zero_test() ->
    ?assertEqual(0, ?MOD:hlc(empty)).

hlc_of_live_is_state_hlc_test() ->
    H = hlc(100, 0),
    ?assertEqual(H, ?MOD:hlc({live, H, mk_payload(1)})).

hlc_of_dead_is_state_hlc_test() ->
    H = hlc(100, 0),
    ?assertEqual(H, ?MOD:hlc({dead, H})).

%% =============================================================================
%% gc_threshold/1
%% =============================================================================

gc_threshold_of_empty_is_undefined_test() ->
    ?assertEqual(undefined, ?MOD:gc_threshold(empty)).

gc_threshold_of_live_is_state_hlc_test() ->
    H = hlc(100, 0),
    ?assertEqual(H, ?MOD:gc_threshold({live, H, mk_payload(1)})).

gc_threshold_of_dead_is_state_hlc_test() ->
    H = hlc(100, 0),
    ?assertEqual(H, ?MOD:gc_threshold({dead, H})).

%% =============================================================================
%% encode / decode round-trip
%% =============================================================================

encode_decode_state_empty_test() ->
    ?assertEqual(empty, ?MOD:decode_state(?MOD:encode_state(empty))).

encode_decode_state_live_test() ->
    S = {live, hlc(100, 0), mk_payload(1)},
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_state_live_with_empty_payload_test() ->
    S = {live, hlc(100, 0), <<>>},
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_state_dead_test() ->
    S = {dead, hlc(200, 0)},
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_event_create_test() ->
    E = {create, hlc(100, 0), mk_payload(1)},
    ?assertEqual(E, ?MOD:decode_event(?MOD:encode_event(E))).

encode_decode_event_delete_test() ->
    E = {delete, hlc(200, 0)},
    ?assertEqual(E, ?MOD:decode_event(?MOD:encode_event(E))).

%% =============================================================================
%% Dispatcher passthrough
%% =============================================================================

dispatcher_resolves_shorthand_test() ->
    ?assertEqual(?MOD, bondy_oplog_fold:mod_of(presence_basic)).

dispatcher_apply_event_via_shorthand_test() ->
    H = hlc(100, 0),
    P = mk_payload(1),
    ?assertEqual(
        {{live, H, P}, P},
        bondy_oplog_fold:apply_event(presence_basic, empty,
                                     {create, H, P}, undefined)
    ).

dispatcher_initial_value_via_shorthand_test() ->
    ?assertEqual(empty, bondy_oplog_fold:initial_value(presence_basic)).

dispatcher_merge_states_unsupported_test() ->
    %% presence_basic intentionally omits merge_states/2.
    ?assertError(
        {merge_states_not_supported, ?MOD},
        bondy_oplog_fold:merge_states(presence_basic, empty, empty)
    ).

dispatcher_page_refs_defaults_to_empty_test() ->
    %% presence_basic omits page_refs/1; dispatcher returns [].
    H = hlc(100, 0),
    ?assertEqual(
        [],
        bondy_oplog_fold:page_refs(presence_basic, {create, H, mk_payload(1)})
    ).

%% Wrapper over %`apply_event/3`%; existing folds ignore Meta.
apply_ev(S, E) ->
    {NewState, _Delta} = ?MOD:apply_event(S, E, undefined),
    NewState.
