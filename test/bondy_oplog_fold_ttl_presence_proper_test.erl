%% =============================================================================
%% PropEr properties for `bondy_oplog_fold_ttl_presence`.
%%
%% Implements the validation patterns in `FOLD_STRATEGY_DESIGN.md` §5:
%%
%%   - prop_apply_event_idempotent/0                (§5.1)
%%   - prop_apply_event_hlc_monotonic/0             (§5.2)
%%   - prop_merge_states_commutative/0              (§5.3)
%%   - prop_merge_states_associative/0              (§5.3)
%%   - prop_merge_states_idempotent/0               (§5.3)
%%   - prop_encode_state_roundtrip/0                (§5.4)
%%   - prop_encode_event_roundtrip/0                (§5.4)
%%   - prop_gc_safe/0                               (§5.5)
%% =============================================================================

-module(bondy_oplog_fold_ttl_presence_proper_test).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(MOD, bondy_oplog_fold_ttl_presence).
-define(DEFAULT_NUMTESTS, 300).

-export([prop_apply_event_idempotent/0]).
-export([prop_apply_event_hlc_monotonic/0]).
-export([prop_merge_states_commutative/0]).
-export([prop_merge_states_associative/0]).
-export([prop_merge_states_idempotent/0]).
-export([prop_encode_state_roundtrip/0]).
-export([prop_encode_event_roundtrip/0]).
-export([prop_gc_safe/0]).

%% =============================================================================
%% Generators
%% =============================================================================

hlc_gen() ->
    ?LET(
        {Phys, Log},
        {integer(0, 1000), integer(0, 1023)},
        bondy_oplog_hlc:encode(Phys, Log)
    ).

%% Expiry must satisfy E > H in normal use; we don't enforce that
%% structurally — the fold is robust against any (H, E) combination.
expiry_gen() ->
    ?LET(
        {Phys, Log},
        {integer(0, 2000), integer(0, 1023)},
        bondy_oplog_hlc:encode(Phys, Log)
    ).

payload_gen() ->
    ?SIZED(N, resize(min(N, 8), binary())).

event_gen() ->
    oneof([
        {issue, hlc_gen(), expiry_gen(), payload_gen()},
        {revoke, hlc_gen()}
    ]).

events_gen() ->
    list(event_gen()).

state_gen() ->
    ?LET(
        Events,
        events_gen(),
        lists:foldl(
            fun(E, S) -> apply_ev(S, E) end,
            ?MOD:initial_value(),
            Events
        )
    ).

%% =============================================================================
%% Properties
%% =============================================================================

prop_apply_event_idempotent() ->
    ?FORALL(
        {State, Event},
        {state_gen(), event_gen()},
        begin
            S1 = apply_ev(State, Event),
            S2 = apply_ev(S1, Event),
            S1 =:= S2
        end
    ).

prop_apply_event_hlc_monotonic() ->
    ?FORALL(
        {State, Event},
        {state_gen(), event_gen()},
        begin
            H0 = ?MOD:hlc(State),
            H1 = ?MOD:hlc(apply_ev(State, Event)),
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
            S = lists:foldl(
                fun(E, Acc) -> apply_ev(Acc, E) end,
                ?MOD:initial_value(),
                Events
            ),
            Threshold = ?MOD:gc_threshold(S),
            Remaining = [E || E <- Events, event_hlc(E) > as_int(Threshold)],
            S2 = lists:foldl(
                fun(E, Acc) -> apply_ev(Acc, E) end,
                S,
                Remaining
            ),
            S =:= S2
        end
    ).

%% =============================================================================
%% EUnit wrapper
%% =============================================================================

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

%% =============================================================================
%% Helpers
%% =============================================================================

event_hlc({issue, H, _, _}) -> H;
event_hlc({revoke, H}) -> H.

as_int(undefined) -> -1;
as_int(N) when is_integer(N) -> N.

%% Wrapper over %`apply_event/3`%; existing folds ignore Meta.
apply_ev(S, E) ->
    {NewState, _Delta} = ?MOD:apply_event(S, E, undefined),
    NewState.
