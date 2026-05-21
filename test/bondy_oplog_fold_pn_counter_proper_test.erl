%% =============================================================================
%% PropEr properties for `bondy_oplog_fold_pn_counter`.
%%
%% Validates the patterns in `_design/catalogue_expansion_plan.md` §4.5:
%%
%%   - prop_apply_event_idempotent/0
%%   - prop_apply_event_hlc_monotonic/0
%%   - prop_merge_states_commutative/0
%%   - prop_merge_states_associative/0
%%   - prop_merge_states_idempotent/0
%%   - prop_encode_state_roundtrip/0
%%   - prop_encode_event_roundtrip/0
%%   - prop_gc_safe/0
%%   - prop_delta_consistency/0
%% =============================================================================

-module(bondy_oplog_fold_pn_counter_proper_test).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(MOD, bondy_oplog_fold_pn_counter).
-define(DEFAULT_NUMTESTS, 200).

-export([prop_apply_event_idempotent/0]).
-export([prop_apply_event_hlc_monotonic/0]).
-export([prop_merge_states_commutative/0]).
-export([prop_merge_states_associative/0]).
-export([prop_merge_states_idempotent/0]).
-export([prop_encode_state_roundtrip/0]).
-export([prop_encode_event_roundtrip/0]).
-export([prop_gc_safe/0]).
-export([prop_delta_consistency/0]).

%% =============================================================================
%% Generators
%% =============================================================================

hlc_gen() ->
    ?LET({Phys, Log}, {integer(0, 1000), integer(0, 1023)},
         bondy_oplog_hlc:encode(Phys, Log)).

origin_gen() ->
    elements([<<"a">>, <<"b">>, <<"c">>]).

delta_gen() ->
    integer(-100, 100).

%% A "rich event" carries its own meta — (hlc, origin, delta). Per-Origin
%% Seq is assigned in arrival order by `renumber_seqs/1` so the generated
%% sequence respects the contiguous-prefix invariant.
rich_event_gen() ->
    {hlc_gen(), origin_gen(), delta_gen()}.

rich_events_gen() ->
    ?LET(Events, list(rich_event_gen()),
         renumber_seqs(Events)).

renumber_seqs(Events) ->
    {Out, _} = lists:mapfoldl(
        fun({H, O, D}, Seqs) ->
            S = maps:get(O, Seqs, 0) + 1,
            {{H, O, S, D}, Seqs#{O => S}}
        end, #{}, Events),
    Out.

apply_events(Events) ->
    lists:foldl(
        fun({H, O, S, D}, Acc) ->
            Meta = bondy_oplog_event:key(H, O, S),
            {NewState, _Delta} = ?MOD:apply_event(Acc, {inc, D}, Meta),
            NewState
        end,
        ?MOD:initial_value(),
        Events).

state_gen() ->
    ?LET(Events, rich_events_gen(), apply_events(Events)).

event_with_meta_gen() ->
    ?LET({H, O, S, D},
         {hlc_gen(), origin_gen(), integer(1, 50), delta_gen()},
         {bondy_oplog_event:key(H, O, S), {inc, D}}).

%% =============================================================================
%% Properties
%% =============================================================================

prop_apply_event_idempotent() ->
    ?FORALL({State, {Meta, Event}},
            {state_gen(), event_with_meta_gen()},
        begin
            {S1, _} = ?MOD:apply_event(State, Event, Meta),
            {S2, _} = ?MOD:apply_event(S1, Event, Meta),
            S1 =:= S2
        end).

prop_apply_event_hlc_monotonic() ->
    ?FORALL({State, {Meta, Event}},
            {state_gen(), event_with_meta_gen()},
        begin
            H0 = ?MOD:hlc(State),
            {NewState, _} = ?MOD:apply_event(State, Event, Meta),
            H1 = ?MOD:hlc(NewState),
            H1 >= H0
        end).

prop_merge_states_commutative() ->
    ?FORALL({A, B}, {state_gen(), state_gen()},
        ?MOD:merge_states(A, B) =:= ?MOD:merge_states(B, A)).

prop_merge_states_associative() ->
    ?FORALL({A, B, C}, {state_gen(), state_gen(), state_gen()},
        begin
            L = ?MOD:merge_states(?MOD:merge_states(A, B), C),
            R = ?MOD:merge_states(A, ?MOD:merge_states(B, C)),
            L =:= R
        end).

prop_merge_states_idempotent() ->
    ?FORALL(A, state_gen(),
        ?MOD:merge_states(A, A) =:= A).

prop_encode_state_roundtrip() ->
    ?FORALL(State, state_gen(),
        ?MOD:decode_state(?MOD:encode_state(State)) =:= State).

prop_encode_event_roundtrip() ->
    ?FORALL(D, delta_gen(),
        ?MOD:decode_event(?MOD:encode_event({inc, D})) =:= {inc, D}).

%% Replaying events older than gc_threshold is a no-op — they are
%% already absorbed (per-Origin Seq dedup catches duplicates).
prop_gc_safe() ->
    ?FORALL(Events, rich_events_gen(),
        begin
            S = apply_events(Events),
            Threshold = ?MOD:gc_threshold(S),
            Tail = [E || {H, _, _, _} = E <- Events,
                         H > as_int(Threshold)],
            S2 = apply_events_on(S, Tail),
            S =:= S2
        end).

%% Delta-consistency property (Contract C): the value-delta returned
%% by `apply_event/3` is consistent with the projection of the new
%% state — combining `OldValue` and the delta yields `to_value(NewState)`.
%% A `none` delta means the value did not change.
prop_delta_consistency() ->
    ?FORALL({State, {Meta, Event}},
            {state_gen(), event_with_meta_gen()},
        begin
            {NewState, Delta} = ?MOD:apply_event(State, Event, Meta),
            OldValue = ?MOD:to_value(State),
            NewValue = case Delta of
                none -> OldValue;
                D    -> ?MOD:apply_value_delta(OldValue, D)
            end,
            NewValue =:= ?MOD:to_value(NewState)
        end).

%% =============================================================================
%% EUnit wrapper
%% =============================================================================

properties_test_() ->
    {timeout, 180,
     fun() ->
        Opts = [{to_file, user}, {numtests, ?DEFAULT_NUMTESTS}],
        Props = [
            prop_apply_event_idempotent(),
            prop_apply_event_hlc_monotonic(),
            prop_merge_states_commutative(),
            prop_merge_states_associative(),
            prop_merge_states_idempotent(),
            prop_encode_state_roundtrip(),
            prop_encode_event_roundtrip(),
            prop_gc_safe(),
            prop_delta_consistency()
        ],
        lists:foreach(
            fun(Prop) -> ?assert(proper:quickcheck(Prop, Opts)) end,
            Props
        )
     end}.

%% =============================================================================
%% Helpers
%% =============================================================================

apply_events_on(State, Events) ->
    lists:foldl(
        fun({H, O, S, D}, Acc) ->
            Meta = bondy_oplog_event:key(H, O, S),
            {NewState, _Delta} = ?MOD:apply_event(Acc, {inc, D}, Meta),
            NewState
        end,
        State,
        Events).

as_int(undefined) -> -1;
as_int(N) when is_integer(N) -> N.
