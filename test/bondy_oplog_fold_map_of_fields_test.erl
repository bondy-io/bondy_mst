%% =============================================================================
%% EUnit suite for `bondy_oplog_fold_map_of_fields`.
%%
%% Covers FOLD_STRATEGY_DESIGN §4.4 and the §2.1/§5 contract.
%% =============================================================================

-module(bondy_oplog_fold_map_of_fields_test).

-include_lib("eunit/include/eunit.hrl").

-define(MOD, bondy_oplog_fold_map_of_fields).

%% =============================================================================
%% initial_value/0
%% =============================================================================

initial_value_is_empty_map_test() ->
    ?assertEqual(#{}, ?MOD:initial_value()).

%% =============================================================================
%% apply_event — field_event (insert)
%% =============================================================================

field_event_inserts_lww_register_test() ->
    H = hlc(1, 0),
    S = ?MOD:apply_event(?MOD:initial_value(),
                         {field_event, <<"name">>, lww_register, {set, H, <<"alice">>}}),
    ?assertEqual(#{<<"name">> => {lww_register, {set, <<"alice">>, H}}}, S).

field_event_inserts_strict_register_test() ->
    H = hlc(1, 0),
    S = ?MOD:apply_event(?MOD:initial_value(),
                         {field_event, <<"role">>, strict_register, {set, H, <<"admin">>}}),
    ?assertEqual(#{<<"role">> => {strict_register, {set, <<"admin">>, H}}}, S).

field_event_inserts_ttl_presence_test() ->
    H = hlc(1, 0),
    E = hlc(10, 0),
    S = ?MOD:apply_event(?MOD:initial_value(),
                         {field_event, <<"lease">>, ttl_presence,
                          {issue, H, E, <<"p">>}}),
    ?assertMatch(#{<<"lease">> := {ttl_presence, {issued, H, E, <<"p">>}}}, S).

%% =============================================================================
%% apply_event — field_event (update existing)
%% =============================================================================

field_event_updates_existing_test() ->
    H1 = hlc(1, 0),
    H2 = hlc(2, 0),
    S0 = ?MOD:apply_event(?MOD:initial_value(),
                          {field_event, <<"x">>, lww_register, {set, H1, <<"a">>}}),
    S1 = ?MOD:apply_event(S0,
                          {field_event, <<"x">>, lww_register, {set, H2, <<"b">>}}),
    ?assertEqual(#{<<"x">> => {lww_register, {set, <<"b">>, H2}}}, S1).

field_event_independent_fields_test() ->
    H = hlc(1, 0),
    S0 = ?MOD:apply_event(?MOD:initial_value(),
                          {field_event, <<"a">>, lww_register, {set, H, <<"av">>}}),
    S1 = ?MOD:apply_event(S0,
                          {field_event, <<"b">>, strict_register, {set, H, <<"bv">>}}),
    ?assertMatch(#{<<"a">> := {lww_register, _},
                   <<"b">> := {strict_register, _}}, S1).

field_event_strategy_mismatch_crashes_test() ->
    H = hlc(1, 0),
    S0 = ?MOD:apply_event(?MOD:initial_value(),
                          {field_event, <<"f">>, lww_register, {set, H, <<"v">>}}),
    ?assertError({strategy_mismatch, <<"f">>, lww_register, strict_register},
                 ?MOD:apply_event(S0,
                                  {field_event, <<"f">>, strict_register,
                                   {set, H, <<"v">>}})).

field_event_unsupported_strategy_crashes_test() ->
    H = hlc(1, 0),
    ?assertError({unsupported_field_strategy, presence_basic},
                 ?MOD:apply_event(?MOD:initial_value(),
                                  {field_event, <<"f">>, presence_basic,
                                   {create, H, <<"v">>}})).

orset_strategy_rejected_test() ->
    H = hlc(1, 0),
    ?assertError({unsupported_field_strategy, orset},
                 ?MOD:apply_event(?MOD:initial_value(),
                                  {field_event, <<"tags">>, orset,
                                   {add, H, <<"red">>, {<<"n1">>, 1}}})).

%% =============================================================================
%% apply_event — remove_field (dispatched to sub-fold purge)
%% =============================================================================

remove_field_lww_on_empty_creates_cleared_test() ->
    H = hlc(5, 0),
    S = ?MOD:apply_event(?MOD:initial_value(),
                         {remove_field, H, <<"x">>, lww_register}),
    ?assertEqual(#{<<"x">> => {lww_register, {cleared, H}}}, S).

remove_field_strict_on_empty_creates_revoked_test() ->
    H = hlc(5, 0),
    S = ?MOD:apply_event(?MOD:initial_value(),
                         {remove_field, H, <<"x">>, strict_register}),
    ?assertEqual(#{<<"x">> => {strict_register, {revoked, H}}}, S).

remove_field_ttl_on_empty_creates_revoked_test() ->
    H = hlc(5, 0),
    S = ?MOD:apply_event(?MOD:initial_value(),
                         {remove_field, H, <<"x">>, ttl_presence}),
    ?assertEqual(#{<<"x">> => {ttl_presence, {revoked, H}}}, S).

remove_field_on_live_clears_test() ->
    H1 = hlc(1, 0),
    H2 = hlc(2, 0),
    S0 = ?MOD:apply_event(?MOD:initial_value(),
                          {field_event, <<"x">>, lww_register, {set, H1, <<"v">>}}),
    S1 = ?MOD:apply_event(S0, {remove_field, H2, <<"x">>, lww_register}),
    ?assertEqual(#{<<"x">> => {lww_register, {cleared, H2}}}, S1).

remove_field_strict_on_live_revokes_test() ->
    H1 = hlc(1, 0),
    H2 = hlc(2, 0),
    S0 = ?MOD:apply_event(?MOD:initial_value(),
                          {field_event, <<"x">>, strict_register, {set, H1, <<"v">>}}),
    S1 = ?MOD:apply_event(S0, {remove_field, H2, <<"x">>, strict_register}),
    ?assertEqual(#{<<"x">> => {strict_register, {revoked, H2}}}, S1).

remove_field_at_same_hlc_clears_test() ->
    %% Clear wins ties per lww_register semantics.
    H = hlc(1, 0),
    S0 = ?MOD:apply_event(?MOD:initial_value(),
                          {field_event, <<"x">>, lww_register, {set, H, <<"v">>}}),
    S1 = ?MOD:apply_event(S0, {remove_field, H, <<"x">>, lww_register}),
    ?assertEqual(#{<<"x">> => {lww_register, {cleared, H}}}, S1).

remove_field_with_older_hlc_is_noop_test() ->
    H1 = hlc(5, 0),
    H2 = hlc(2, 0),
    S0 = ?MOD:apply_event(?MOD:initial_value(),
                          {field_event, <<"x">>, lww_register, {set, H1, <<"v">>}}),
    S1 = ?MOD:apply_event(S0, {remove_field, H2, <<"x">>, lww_register}),
    %% LWW rejects older clear — state unchanged.
    ?assertEqual(S0, S1).

remove_field_strategy_mismatch_crashes_test() ->
    H = hlc(1, 0),
    S0 = ?MOD:apply_event(?MOD:initial_value(),
                          {field_event, <<"f">>, lww_register, {set, H, <<"v">>}}),
    ?assertError({strategy_mismatch, <<"f">>, lww_register, strict_register},
                 ?MOD:apply_event(S0, {remove_field, H, <<"f">>, strict_register})).

remove_field_unsupported_strategy_crashes_test() ->
    H = hlc(1, 0),
    ?assertError({unsupported_field_strategy, orset},
                 ?MOD:apply_event(?MOD:initial_value(),
                                  {remove_field, H, <<"x">>, orset})).

%% =============================================================================
%% apply_event — re-introduction after cleared (LWW only — strict and ttl
%% behave per their own terminal-state rules)
%% =============================================================================

field_event_after_cleared_lww_reintroduces_test() ->
    H1 = hlc(1, 0),
    H2 = hlc(2, 0),
    S0 = ?MOD:apply_event(?MOD:initial_value(),
                          {remove_field, H1, <<"x">>, lww_register}),
    S1 = ?MOD:apply_event(S0,
                          {field_event, <<"x">>, lww_register, {set, H2, <<"new">>}}),
    ?assertEqual(#{<<"x">> => {lww_register, {set, <<"new">>, H2}}}, S1).

field_event_after_revoked_strict_stays_revoked_test() ->
    %% strict_register revoke is terminal.
    H1 = hlc(1, 0),
    H2 = hlc(2, 0),
    S0 = ?MOD:apply_event(?MOD:initial_value(),
                          {remove_field, H1, <<"x">>, strict_register}),
    S1 = ?MOD:apply_event(S0,
                          {field_event, <<"x">>, strict_register, {set, H2, <<"new">>}}),
    ?assertEqual(#{<<"x">> => {strict_register, {revoked, H2}}}, S1).

%% =============================================================================
%% apply_event — idempotency
%% =============================================================================

field_event_is_idempotent_test() ->
    H = hlc(1, 0),
    E = {field_event, <<"x">>, lww_register, {set, H, <<"v">>}},
    S1 = ?MOD:apply_event(?MOD:initial_value(), E),
    S2 = ?MOD:apply_event(S1, E),
    ?assertEqual(S1, S2).

remove_field_is_idempotent_test() ->
    H = hlc(1, 0),
    E = {remove_field, H, <<"x">>, lww_register},
    S1 = ?MOD:apply_event(?MOD:initial_value(), E),
    S2 = ?MOD:apply_event(S1, E),
    ?assertEqual(S1, S2).

%% =============================================================================
%% hlc/1
%% =============================================================================

hlc_empty_test() ->
    ?assertEqual(0, ?MOD:hlc(?MOD:initial_value())).

hlc_returns_max_across_fields_test() ->
    H1 = hlc(1, 0),
    H2 = hlc(5, 0),
    S = lists:foldl(
        fun(Ev, Acc) -> ?MOD:apply_event(Acc, Ev) end,
        ?MOD:initial_value(),
        [{field_event, <<"a">>, lww_register, {set, H1, <<"av">>}},
         {field_event, <<"b">>, lww_register, {set, H2, <<"bv">>}}]),
    ?assertEqual(H2, ?MOD:hlc(S)).

hlc_includes_cleared_fields_test() ->
    H1 = hlc(1, 0),
    H2 = hlc(10, 0),
    S = lists:foldl(
        fun(Ev, Acc) -> ?MOD:apply_event(Acc, Ev) end,
        ?MOD:initial_value(),
        [{field_event, <<"a">>, lww_register, {set, H1, <<"av">>}},
         {remove_field, H2, <<"b">>, lww_register}]),
    ?assertEqual(H2, ?MOD:hlc(S)).

%% =============================================================================
%% gc_threshold/1
%% =============================================================================

gc_threshold_empty_test() ->
    ?assertEqual(undefined, ?MOD:gc_threshold(?MOD:initial_value())).

gc_threshold_max_across_fields_test() ->
    H1 = hlc(1, 0),
    H2 = hlc(5, 0),
    S = lists:foldl(
        fun(Ev, Acc) -> ?MOD:apply_event(Acc, Ev) end,
        ?MOD:initial_value(),
        [{field_event, <<"a">>, lww_register, {set, H1, <<"av">>}},
         {field_event, <<"b">>, lww_register, {set, H2, <<"bv">>}}]),
    ?assertEqual(H2, ?MOD:gc_threshold(S)).

%% =============================================================================
%% merge_states/2
%% =============================================================================

merge_empty_with_empty_test() ->
    ?assertEqual(#{}, ?MOD:merge_states(#{}, #{})).

merge_empty_with_populated_test() ->
    H = hlc(1, 0),
    S = ?MOD:apply_event(?MOD:initial_value(),
                         {field_event, <<"x">>, lww_register, {set, H, <<"v">>}}),
    ?assertEqual(S, ?MOD:merge_states(#{}, S)),
    ?assertEqual(S, ?MOD:merge_states(S, #{})).

merge_disjoint_field_sets_test() ->
    H = hlc(1, 0),
    A = ?MOD:apply_event(#{}, {field_event, <<"a">>, lww_register, {set, H, <<"av">>}}),
    B = ?MOD:apply_event(#{}, {field_event, <<"b">>, lww_register, {set, H, <<"bv">>}}),
    Merged = ?MOD:merge_states(A, B),
    ?assertMatch(#{<<"a">> := {lww_register, _},
                   <<"b">> := {lww_register, _}}, Merged).

merge_same_field_same_strategy_test() ->
    H1 = hlc(1, 0),
    H2 = hlc(2, 0),
    A = ?MOD:apply_event(#{}, {field_event, <<"x">>, lww_register, {set, H1, <<"a">>}}),
    B = ?MOD:apply_event(#{}, {field_event, <<"x">>, lww_register, {set, H2, <<"b">>}}),
    Merged = ?MOD:merge_states(A, B),
    ?assertEqual(#{<<"x">> => {lww_register, {set, <<"b">>, H2}}}, Merged).

merge_cleared_vs_live_lww_test() ->
    %% Higher HLC wins (lww semantics).
    H1 = hlc(1, 0),
    H2 = hlc(2, 0),
    A = ?MOD:apply_event(#{}, {field_event, <<"x">>, lww_register, {set, H1, <<"a">>}}),
    B = ?MOD:apply_event(#{}, {remove_field, H2, <<"x">>, lww_register}),
    ?assertEqual(#{<<"x">> => {lww_register, {cleared, H2}}}, ?MOD:merge_states(A, B)),
    ?assertEqual(#{<<"x">> => {lww_register, {cleared, H2}}}, ?MOD:merge_states(B, A)).

merge_revoked_dominates_strict_register_test() ->
    %% strict_register's revoke is a dominant absorber regardless of HLC.
    H1 = hlc(5, 0),
    H2 = hlc(1, 0),
    A = ?MOD:apply_event(#{}, {field_event, <<"x">>, strict_register, {set, H1, <<"a">>}}),
    B = ?MOD:apply_event(#{}, {remove_field, H2, <<"x">>, strict_register}),
    %% Revoke dominates; max HLC.
    ?assertEqual(#{<<"x">> => {strict_register, {revoked, H1}}}, ?MOD:merge_states(A, B)),
    ?assertEqual(#{<<"x">> => {strict_register, {revoked, H1}}}, ?MOD:merge_states(B, A)).

merge_strategy_mismatch_crashes_test() ->
    H = hlc(1, 0),
    A = ?MOD:apply_event(#{}, {field_event, <<"x">>, lww_register, {set, H, <<"v">>}}),
    B = ?MOD:apply_event(#{}, {field_event, <<"x">>, strict_register, {set, H, <<"v">>}}),
    ?assertError({strategy_mismatch, <<"x">>, lww_register, strict_register},
                 ?MOD:merge_states(A, B)).

merge_is_idempotent_test() ->
    H1 = hlc(1, 0),
    H2 = hlc(2, 0),
    S = lists:foldl(
        fun(Ev, Acc) -> ?MOD:apply_event(Acc, Ev) end,
        ?MOD:initial_value(),
        [{field_event, <<"a">>, lww_register, {set, H1, <<"a">>}},
         {field_event, <<"b">>, strict_register, {set, H2, <<"b">>}},
         {remove_field, H1, <<"c">>, lww_register}]),
    ?assertEqual(S, ?MOD:merge_states(S, S)).

%% =============================================================================
%% encode_state / decode_state — round-trip
%% =============================================================================

encode_decode_empty_test() ->
    S = #{},
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_single_lww_field_test() ->
    H = hlc(1, 0),
    S = #{<<"x">> => {lww_register, {set, <<"v">>, H}}},
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_single_cleared_field_test() ->
    H = hlc(1, 0),
    S = #{<<"x">> => {lww_register, {cleared, H}}},
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_decode_all_strategies_test() ->
    H = hlc(1, 0),
    E = hlc(5, 0),
    S = #{<<"a">> => {lww_register,    {set, <<"av">>, H}},
          <<"b">> => {strict_register, {set, <<"bv">>, H}},
          <<"c">> => {ttl_presence,    {issued, H, E, <<"p">>}},
          <<"d">> => {lww_register,    {cleared, H}}},
    ?assertEqual(S, ?MOD:decode_state(?MOD:encode_state(S))).

encode_is_canonical_test() ->
    %% Same state with different insertion orders must encode to identical bytes.
    H = hlc(1, 0),
    S1 = #{<<"a">> => {lww_register, {set, <<"av">>, H}},
           <<"b">> => {lww_register, {cleared, H}}},
    S2 = #{<<"b">> => {lww_register, {cleared, H}},
           <<"a">> => {lww_register, {set, <<"av">>, H}}},
    ?assertEqual(?MOD:encode_state(S1), ?MOD:encode_state(S2)).

%% =============================================================================
%% encode_event / decode_event — round-trip
%% =============================================================================

encode_decode_field_event_lww_test() ->
    H = hlc(1, 0),
    E = {field_event, <<"x">>, lww_register, {set, H, <<"v">>}},
    ?assertEqual(E, ?MOD:decode_event(?MOD:encode_event(E))).

encode_decode_field_event_strict_test() ->
    H = hlc(1, 0),
    E = {field_event, <<"x">>, strict_register, {revoke, H}},
    ?assertEqual(E, ?MOD:decode_event(?MOD:encode_event(E))).

encode_decode_field_event_ttl_presence_test() ->
    H = hlc(1, 0),
    Exp = hlc(5, 0),
    E = {field_event, <<"lease">>, ttl_presence, {issue, H, Exp, <<"p">>}},
    ?assertEqual(E, ?MOD:decode_event(?MOD:encode_event(E))).

encode_decode_remove_field_event_test() ->
    H = hlc(1, 0),
    E = {remove_field, H, <<"x">>, lww_register},
    ?assertEqual(E, ?MOD:decode_event(?MOD:encode_event(E))).

encode_decode_remove_field_strict_test() ->
    H = hlc(1, 0),
    E = {remove_field, H, <<"x">>, strict_register},
    ?assertEqual(E, ?MOD:decode_event(?MOD:encode_event(E))).

encode_event_unsupported_strategy_crashes_test() ->
    H = hlc(1, 0),
    ?assertError({unsupported_field_strategy, presence_basic},
                 ?MOD:encode_event({field_event, <<"x">>, presence_basic,
                                    {create, H, <<"v">>}})).

%% =============================================================================
%% dispatcher integration
%% =============================================================================

dispatcher_resolves_map_of_fields_test() ->
    ?assertEqual(?MOD, bondy_oplog_fold:mod_of(map_of_fields)).

dispatcher_initial_value_via_shorthand_test() ->
    ?assertEqual(#{}, bondy_oplog_fold:initial_value(map_of_fields)).

dispatcher_apply_event_via_shorthand_test() ->
    H = hlc(1, 0),
    E = {field_event, <<"x">>, lww_register, {set, H, <<"v">>}},
    S = bondy_oplog_fold:apply_event(map_of_fields, #{}, E),
    ?assertMatch(#{<<"x">> := {lww_register, _}}, S).

%% =============================================================================
%% Helpers
%% =============================================================================

hlc(Phys, Log) ->
    bondy_oplog_hlc:encode(Phys, Log).
