%% =============================================================================
%% PropEr properties for `bondy_oplog_fold_index_entry`.
%%
%% The index-entry fold is an LWW register over presence whose
%% `apply_event/3` is defined as a merge against a total order. These
%% properties pin the design's mandatory convergence guarantee (risk 3:
%% out-of-order cross-shard put/remove must commute):
%%
%%   - `apply_event` equals `merge_states(State, state_of(Event))`;
%%   - merge is commutative, associative, idempotent;
%%   - apply is idempotent and HLC-monotone;
%%   - encode round-trips; value_equals_state byte-equality.
%% =============================================================================

-module(bondy_oplog_fold_index_entry_proper_test).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(MOD, bondy_oplog_fold_index_entry).
-define(DEFAULT_NUMTESTS, 200).
-define(META, undefined).

-export([prop_apply_equals_merge/0]).
-export([prop_apply_event_idempotent/0]).
-export([prop_apply_event_hlc_monotonic/0]).
-export([prop_fold_order_independent/0]).
-export([prop_merge_states_commutative/0]).
-export([prop_merge_states_associative/0]).
-export([prop_merge_states_idempotent/0]).
-export([prop_encode_state_roundtrip/0]).
-export([prop_encode_event_roundtrip/0]).
-export([prop_value_equals_state/0]).

%% =============================================================================
%% Generators
%% =============================================================================

hlc_gen() ->
    integer(0, 1000).

cols_gen() ->
    ?SIZED(N, resize(min(N, 16), binary())).

event_gen() ->
    oneof([
        ?LET({C, H}, {cols_gen(), hlc_gen()}, {put, C, H}),
        ?LET(H, hlc_gen(), {remove, H})
    ]).

events_gen() ->
    list(event_gen()).

apply_events(Events) ->
    lists:foldl(
        fun(Ev, Acc) ->
            {NewState, none} = ?MOD:apply_event(Acc, Ev, ?META),
            NewState
        end,
        ?MOD:initial_value(),
        Events
    ).

state_gen() ->
    ?LET(Events, events_gen(), apply_events(Events)).

%% The state an event represents, used by prop_apply_equals_merge.
state_of({put, C, H}) -> {live, C, H};
state_of({remove, H}) -> {dead, <<>>, H}.

%% =============================================================================
%% Properties
%% =============================================================================

prop_apply_equals_merge() ->
    ?FORALL(
        {State, Event},
        {state_gen(), event_gen()},
        begin
            {Applied, none} = ?MOD:apply_event(State, Event, ?META),
            Applied =:= ?MOD:merge_states(State, state_of(Event))
        end
    ).

prop_apply_event_idempotent() ->
    ?FORALL(
        {State, Event},
        {state_gen(), event_gen()},
        begin
            {S1, _} = ?MOD:apply_event(State, Event, ?META),
            {S2, _} = ?MOD:apply_event(S1, Event, ?META),
            S1 =:= S2
        end
    ).

prop_apply_event_hlc_monotonic() ->
    ?FORALL(
        {State, Event},
        {state_gen(), event_gen()},
        begin
            H0 = ?MOD:hlc(State),
            {NewState, _} = ?MOD:apply_event(State, Event, ?META),
            ?MOD:hlc(NewState) >= H0
        end
    ).

%% Applying the same multiset of events in two different orders converges
%% to the same state — the core out-of-order-delivery guarantee.
prop_fold_order_independent() ->
    ?FORALL(
        Events,
        events_gen(),
        apply_events(Events) =:= apply_events(lists:reverse(Events))
    ).

prop_merge_states_commutative() ->
    ?FORALL(
        {A, B},
        {state_gen(), state_gen()},
        ?MOD:merge_states(A, B) =:= ?MOD:merge_states(B, A)
    ).

prop_merge_states_associative() ->
    ?FORALL(
        {A, B, C},
        {state_gen(), state_gen(), state_gen()},
        ?MOD:merge_states(?MOD:merge_states(A, B), C) =:=
            ?MOD:merge_states(A, ?MOD:merge_states(B, C))
    ).

prop_merge_states_idempotent() ->
    ?FORALL(A, state_gen(), ?MOD:merge_states(A, A) =:= A).

prop_encode_state_roundtrip() ->
    ?FORALL(
        State,
        state_gen(),
        ?MOD:decode_state(?MOD:encode_state(State)) =:= State
    ).

prop_encode_event_roundtrip() ->
    ?FORALL(
        Event,
        event_gen(),
        ?MOD:decode_event(?MOD:encode_event(Event)) =:= Event
    ).

prop_value_equals_state() ->
    ?FORALL(
        State,
        state_gen(),
        ?MOD:to_value(?MOD:decode_state(?MOD:encode_state(State))) =:=
            ?MOD:to_value(State)
    ).

%% =============================================================================
%% Runner
%% =============================================================================

properties_test_() ->
    {timeout, 180, fun() ->
        Opts = [{to_file, user}, {numtests, ?DEFAULT_NUMTESTS}],
        Props = [
            prop_apply_equals_merge(),
            prop_apply_event_idempotent(),
            prop_apply_event_hlc_monotonic(),
            prop_fold_order_independent(),
            prop_merge_states_commutative(),
            prop_merge_states_associative(),
            prop_merge_states_idempotent(),
            prop_encode_state_roundtrip(),
            prop_encode_event_roundtrip(),
            prop_value_equals_state()
        ],
        lists:foreach(
            fun(Prop) -> ?assert(proper:quickcheck(Prop, Opts)) end,
            Props
        )
    end}.
