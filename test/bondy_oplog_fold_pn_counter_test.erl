%% =============================================================================
%% EUnit smoke tests for `bondy_oplog_fold_pn_counter`.
%%
%% Covers `apply_event/3` per-Origin accumulation, MaxSeq dedup,
%% `to_value/1` sum-difference projection, `to_value_delta/3` +
%% `apply_value_delta/2`, `merge_states/2`, encode round-trip, hlc and
%% gc_threshold corners.
%%
%% Idempotency and merge CAI invariants live in the proper test.
%% =============================================================================

-module(bondy_oplog_fold_pn_counter_test).

-include_lib("eunit/include/eunit.hrl").

-define(MOD, bondy_oplog_fold_pn_counter).

%% =============================================================================
%% Helpers
%% =============================================================================

hlc(Phys, Log) ->
    bondy_oplog_hlc:encode(Phys, Log).

key(Hlc, Origin, Seq) ->
    bondy_oplog_event:key(Hlc, Origin, Seq).

apply_inc(State, Delta, Origin, Seq, Hlc) ->
    {NewState, _ValueDelta} =
        ?MOD:apply_event(State, {inc, Delta}, key(Hlc, Origin, Seq)),
    NewState.

%% Return only the value-delta emitted by `apply_event/3`.
apply_inc_delta(State, Delta, Origin, Seq, Hlc) ->
    {_NewState, ValueDelta} =
        ?MOD:apply_event(State, {inc, Delta}, key(Hlc, Origin, Seq)),
    ValueDelta.

%% =============================================================================
%% Initial value
%% =============================================================================

initial_value_is_empty_test() ->
    ?assertEqual(#{counters => #{}, hlc => 0}, ?MOD:initial_value()).

%% =============================================================================
%% apply_event/3 — single Origin
%% =============================================================================

single_inc_records_pos_test() ->
    H = hlc(100, 0),
    S = apply_inc(?MOD:initial_value(), 5, <<"a">>, 1, H),
    ?assertEqual(#{<<"a">> => {5, 0, 1}}, maps:get(counters, S)),
    ?assertEqual(H, maps:get(hlc, S)).

single_dec_records_neg_test() ->
    H = hlc(100, 0),
    S = apply_inc(?MOD:initial_value(), -3, <<"a">>, 1, H),
    ?assertEqual(#{<<"a">> => {0, 3, 1}}, maps:get(counters, S)).

mixed_inc_dec_same_origin_accumulates_test() ->
    S0 = ?MOD:initial_value(),
    S1 = apply_inc(S0, 5, <<"a">>, 1, hlc(100, 0)),
    S2 = apply_inc(S1, -3, <<"a">>, 2, hlc(101, 0)),
    S3 = apply_inc(S2, 7, <<"a">>, 3, hlc(102, 0)),
    ?assertEqual(#{<<"a">> => {12, 3, 3}}, maps:get(counters, S3)),
    ?assertEqual(9, ?MOD:to_value(S3)).

%% =============================================================================
%% apply_event/3 — duplicate Seq is a no-op on counters; HLC may bump
%% =============================================================================

duplicate_seq_is_value_noop_test() ->
    S0 = apply_inc(?MOD:initial_value(), 5, <<"a">>, 1, hlc(100, 0)),
    %% Replay the same Seq at a higher HLC (e.g. crash recovery sees the
    %% same event with a fresh receive-time HLC). Counters unchanged;
    %% cell HLC bumps to track last-observed.
    S1 = apply_inc(S0, 5, <<"a">>, 1, hlc(200, 0)),
    ?assertEqual(#{<<"a">> => {5, 0, 1}}, maps:get(counters, S1)),
    ?assertEqual(hlc(200, 0), maps:get(hlc, S1)).

older_seq_is_rejected_test() ->
    S0 = apply_inc(?MOD:initial_value(), 5, <<"a">>, 2, hlc(100, 0)),
    %% Out-of-order delivery of Seq=1 after Seq=2 — rejected.
    S1 = apply_inc(S0, 9, <<"a">>, 1, hlc(99, 0)),
    ?assertEqual(#{<<"a">> => {5, 0, 2}}, maps:get(counters, S1)).

%% =============================================================================
%% apply_event/3 — multi-Origin
%% =============================================================================

two_origins_independent_test() ->
    S0 = ?MOD:initial_value(),
    S1 = apply_inc(S0, 5, <<"a">>, 1, hlc(100, 0)),
    S2 = apply_inc(S1, 7, <<"b">>, 1, hlc(101, 0)),
    ?assertEqual({5, 0, 1}, maps:get(<<"a">>, maps:get(counters, S2))),
    ?assertEqual({7, 0, 1}, maps:get(<<"b">>, maps:get(counters, S2))),
    ?assertEqual(12, ?MOD:to_value(S2)).

%% =============================================================================
%% to_value/1
%% =============================================================================

to_value_empty_is_zero_test() ->
    ?assertEqual(0, ?MOD:to_value(?MOD:initial_value())).

to_value_sums_pos_neg_across_origins_test() ->
    S = #{
        counters => #{<<"a">> => {10, 3, 5}, <<"b">> => {5, 2, 3}},
        hlc => hlc(100, 0)
    },
    ?assertEqual(10, ?MOD:to_value(S)).

%% =============================================================================
%% apply_event/3 value-delta + apply_value_delta/2
%% =============================================================================

delta_for_fresh_event_equals_event_delta_test() ->
    %% First inc on a fresh Origin: value moves 0 → 5, delta is +5.
    ?assertEqual(
        5,
        apply_inc_delta(?MOD:initial_value(), 5, <<"a">>, 1, hlc(100, 0))
    ).

delta_for_duplicate_event_is_none_test() ->
    %% Replay of the same (Origin, Seq) is dedup'd: state HLC bumps but
    %% the value column does not move — `apply_event/3` returns `none`.
    S0 = apply_inc(?MOD:initial_value(), 5, <<"a">>, 1, hlc(100, 0)),
    ?assertEqual(
        none,
        apply_inc_delta(S0, 5, <<"a">>, 1, hlc(200, 0))
    ).

apply_value_delta_adds_test() ->
    ?assertEqual(7, ?MOD:apply_value_delta(3, 4)),
    ?assertEqual(-1, ?MOD:apply_value_delta(3, -4)).

%% =============================================================================
%% merge_states/2
%% =============================================================================

merge_empty_left_returns_right_test() ->
    A = ?MOD:initial_value(),
    B = apply_inc(?MOD:initial_value(), 5, <<"a">>, 1, hlc(100, 0)),
    ?assertEqual(B, ?MOD:merge_states(A, B)),
    ?assertEqual(B, ?MOD:merge_states(B, A)).

merge_distinct_origins_unions_test() ->
    A = apply_inc(?MOD:initial_value(), 5, <<"a">>, 1, hlc(100, 0)),
    B = apply_inc(?MOD:initial_value(), 7, <<"b">>, 1, hlc(101, 0)),
    M = ?MOD:merge_states(A, B),
    ?assertEqual({5, 0, 1}, maps:get(<<"a">>, maps:get(counters, M))),
    ?assertEqual({7, 0, 1}, maps:get(<<"b">>, maps:get(counters, M))).

merge_same_origin_max_per_field_test() ->
    %% Per-Origin contiguous prefix: A has 5+3 (Pos=8), B has 5+3+2
    %% (Pos=10). Max-merge keeps the longer prefix.
    A = apply_inc(
        apply_inc(?MOD:initial_value(), 5, <<"a">>, 1, hlc(100, 0)),
        3, <<"a">>, 2, hlc(101, 0)),
    B = apply_inc(
        apply_inc(
            apply_inc(?MOD:initial_value(), 5, <<"a">>, 1, hlc(100, 0)),
            3, <<"a">>, 2, hlc(101, 0)),
        2, <<"a">>, 3, hlc(102, 0)),
    M = ?MOD:merge_states(A, B),
    ?assertEqual({10, 0, 3}, maps:get(<<"a">>, maps:get(counters, M))).

%% =============================================================================
%% hlc/1, gc_threshold/1
%% =============================================================================

hlc_of_initial_is_zero_test() ->
    ?assertEqual(0, ?MOD:hlc(?MOD:initial_value())).

hlc_tracks_max_observed_test() ->
    S = apply_inc(?MOD:initial_value(), 5, <<"a">>, 1, hlc(500, 0)),
    ?assertEqual(hlc(500, 0), ?MOD:hlc(S)).

gc_threshold_of_initial_is_undefined_test() ->
    ?assertEqual(undefined, ?MOD:gc_threshold(?MOD:initial_value())).

gc_threshold_of_populated_is_hlc_test() ->
    H = hlc(500, 0),
    S = apply_inc(?MOD:initial_value(), 5, <<"a">>, 1, H),
    ?assertEqual(H, ?MOD:gc_threshold(S)).

%% =============================================================================
%% Encode / decode round-trip
%% =============================================================================

encode_decode_initial_test() ->
    S = ?MOD:initial_value(),
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_single_origin_test() ->
    S = apply_inc(?MOD:initial_value(), 5, <<"a">>, 1, hlc(100, 0)),
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_multi_origin_test() ->
    S0 = ?MOD:initial_value(),
    S1 = apply_inc(S0, 5, <<"a">>, 1, hlc(100, 0)),
    S2 = apply_inc(S1, -3, <<"b">>, 1, hlc(101, 0)),
    S3 = apply_inc(S2, 11, <<"c">>, 1, hlc(102, 0)),
    ?assertEqual(S3, ?MOD:decode_state(?MOD:encode_state(S3))).

encode_is_canonical_for_equal_states_test() ->
    %% Two paths to the same state should produce byte-identical
    %% encodings — sort key is Origin, not insertion order.
    S0 = ?MOD:initial_value(),
    Path1 = apply_inc(
        apply_inc(S0, 5, <<"a">>, 1, hlc(100, 0)),
        7, <<"b">>, 1, hlc(101, 0)),
    Path2 = apply_inc(
        apply_inc(S0, 7, <<"b">>, 1, hlc(101, 0)),
        5, <<"a">>, 1, hlc(100, 0)),
    ?assertEqual(?MOD:encode_state(Path1), ?MOD:encode_state(Path2)).

encode_decode_event_test() ->
    E1 = {inc, 5},
    E2 = {inc, -3},
    E3 = {inc, 0},
    ?assertEqual(E1, ?MOD:decode_event(?MOD:encode_event(E1))),
    ?assertEqual(E2, ?MOD:decode_event(?MOD:encode_event(E2))),
    ?assertEqual(E3, ?MOD:decode_event(?MOD:encode_event(E3))).

%% =============================================================================
%% Dispatcher passthrough
%% =============================================================================

dispatcher_resolves_shorthand_test() ->
    ?assertEqual(?MOD, bondy_oplog_fold:mod_of(pn_counter)).

dispatcher_is_known_test() ->
    ?assertEqual(true, bondy_oplog_fold:is_known(pn_counter)).

dispatcher_value_equals_state_false_test() ->
    ?assertEqual(false, bondy_oplog_fold:value_equals_state(pn_counter)).

dispatcher_apply_value_delta_dispatch_test() ->
    ?assertEqual(9, bondy_oplog_fold:apply_value_delta(pn_counter, 5, 4)).
