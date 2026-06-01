%% =============================================================================
%% PropEr properties for `bondy_oplog_fold_min_register`.
%%
%% Mirror of `bondy_oplog_fold_max_register_proper_test` with min on the
%% value lattice (HLC is still merged by max).
%% =============================================================================

-module(bondy_oplog_fold_min_register_proper_test).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(MOD, bondy_oplog_fold_min_register).
-define(DEFAULT_NUMTESTS, 200).

-export([prop_apply_event_idempotent/0]).
-export([prop_apply_event_hlc_monotonic/0]).
-export([prop_merge_states_commutative/0]).
-export([prop_merge_states_associative/0]).
-export([prop_merge_states_idempotent/0]).
-export([prop_encode_state_roundtrip/0]).
-export([prop_encode_event_roundtrip/0]).
-export([prop_gc_safe/0]).

hlc_gen() ->
    ?LET(
        {Phys, Log},
        {integer(0, 1000), integer(0, 1023)},
        bondy_oplog_hlc:encode(Phys, Log)
    ).

origin_gen() ->
    elements([<<"a">>, <<"b">>, <<"c">>]).

value_gen() ->
    integer(-1000, 1000).

meta_gen() ->
    ?LET(
        {H, O, S},
        {hlc_gen(), origin_gen(), integer(0, 100)},
        bondy_oplog_event:key(H, O, S)
    ).

event_gen() ->
    ?LET(V, value_gen(), {set, V}).

apply_events(Events) ->
    lists:foldl(
        fun({Meta, E}, Acc) -> apply_ev(Acc, E, Meta) end,
        ?MOD:initial_value(),
        Events
    ).

apply_ev(State, Event, Meta) ->
    {NewState, _Delta} = ?MOD:apply_event(State, Event, Meta),
    NewState.

events_gen() ->
    list({meta_gen(), event_gen()}).

state_gen() ->
    ?LET(Events, events_gen(), apply_events(Events)).

prop_apply_event_idempotent() ->
    ?FORALL(
        {State, Meta, Event},
        {state_gen(), meta_gen(), event_gen()},
        begin
            S1 = apply_ev(State, Event, Meta),
            S2 = apply_ev(S1, Event, Meta),
            S1 =:= S2
        end
    ).

prop_apply_event_hlc_monotonic() ->
    ?FORALL(
        {State, Meta, Event},
        {state_gen(), meta_gen(), event_gen()},
        begin
            H0 = ?MOD:hlc(State),
            H1 = ?MOD:hlc(apply_ev(State, Event, Meta)),
            H1 >= H0
        end
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
        begin
            L = ?MOD:merge_states(?MOD:merge_states(A, B), C),
            R = ?MOD:merge_states(A, ?MOD:merge_states(B, C)),
            L =:= R
        end
    ).

prop_merge_states_idempotent() ->
    ?FORALL(
        A,
        state_gen(),
        ?MOD:merge_states(A, A) =:= A
    ).

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

prop_gc_safe() ->
    ?FORALL(
        Events,
        events_gen(),
        begin
            S = apply_events(Events),
            Threshold = ?MOD:gc_threshold(S),
            Tail = [
                {M, E}
             || {M, E} <- Events,
                bondy_oplog_event:key_hlc(M) > as_int(Threshold)
            ],
            S2 = lists:foldl(
                fun({M, E}, Acc) -> apply_ev(Acc, E, M) end, S, Tail
            ),
            S =:= S2
        end
    ).

properties_test_() ->
    {timeout, 180, fun() ->
        Opts = [{to_file, user}, {numtests, ?DEFAULT_NUMTESTS}],
        Props = [
            prop_apply_event_idempotent(),
            prop_apply_event_hlc_monotonic(),
            prop_merge_states_commutative(),
            prop_merge_states_associative(),
            prop_merge_states_idempotent(),
            prop_encode_state_roundtrip(),
            prop_encode_event_roundtrip(),
            prop_gc_safe()
        ],
        lists:foreach(
            fun(Prop) -> ?assert(proper:quickcheck(Prop, Opts)) end,
            Props
        )
    end}.

as_int(undefined) -> -1;
as_int(N) when is_integer(N) -> N.
