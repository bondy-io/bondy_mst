%% =============================================================================
%% PropEr properties for `bondy_oplog_fold_strict_register`.
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

-module(bondy_oplog_fold_strict_register_proper_test).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(MOD, bondy_oplog_fold_strict_register).
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
    ?LET({Phys, Log}, {integer(0, 1000), integer(0, 1023)},
         bondy_oplog_hlc:encode(Phys, Log)).

%% Small payload so shrunk counter-examples are readable.
value_gen() ->
    ?SIZED(N, resize(min(N, 8), binary())).

event_gen() ->
    oneof([
        {set, hlc_gen(), value_gen()},
        {revoke, hlc_gen()},
        {resolve, hlc_gen(), value_gen()}
    ]).

%% Unconstrained — strict_register's apply_event/2 is total over the
%% (state, event) cross product, so every input is in contract. The
%% fold itself handles concurrent writers (the conflict path) and
%% admin overrides (resolve / revoke).
events_gen() ->
    list(event_gen()).

state_gen() ->
    ?LET(Events, events_gen(),
         lists:foldl(fun(E, S) -> ?MOD:apply_event(S, E) end,
                     ?MOD:initial_value(),
                     Events)).

%% =============================================================================
%% Properties
%% =============================================================================

prop_apply_event_idempotent() ->
    ?FORALL({State, Event}, {state_gen(), event_gen()},
        begin
            S1 = ?MOD:apply_event(State, Event),
            S2 = ?MOD:apply_event(S1, Event),
            S1 =:= S2
        end).

prop_apply_event_hlc_monotonic() ->
    ?FORALL({State, Event}, {state_gen(), event_gen()},
        begin
            H0 = ?MOD:hlc(State),
            H1 = ?MOD:hlc(?MOD:apply_event(State, Event)),
            H1 >= H0
        end).

%% §5.3 — merge_states is commutative: merge(A, B) == merge(B, A).
prop_merge_states_commutative() ->
    ?FORALL({A, B}, {state_gen(), state_gen()},
        ?MOD:merge_states(A, B) =:= ?MOD:merge_states(B, A)).

%% §5.3 — merge_states is associative.
prop_merge_states_associative() ->
    ?FORALL({A, B, C}, {state_gen(), state_gen(), state_gen()},
        begin
            L = ?MOD:merge_states(?MOD:merge_states(A, B), C),
            R = ?MOD:merge_states(A, ?MOD:merge_states(B, C)),
            L =:= R
        end).

%% §5.3 — merge_states is idempotent.
prop_merge_states_idempotent() ->
    ?FORALL(A, state_gen(),
        ?MOD:merge_states(A, A) =:= A).

prop_encode_state_roundtrip() ->
    ?FORALL(State, state_gen(),
        ?MOD:decode_state(?MOD:encode_state(State)) =:= State).

prop_encode_event_roundtrip() ->
    ?FORALL(Event, event_gen(),
        ?MOD:decode_event(?MOD:encode_event(Event)) =:= Event).

prop_gc_safe() ->
    ?FORALL(Events, events_gen(),
        begin
            S = lists:foldl(fun(E, Acc) -> ?MOD:apply_event(Acc, E) end,
                            ?MOD:initial_value(), Events),
            Threshold = ?MOD:gc_threshold(S),
            Remaining = [E || E <- Events, event_hlc(E) > as_int(Threshold)],
            S2 = lists:foldl(fun(E, Acc) -> ?MOD:apply_event(Acc, E) end,
                             S, Remaining),
            S =:= S2
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

event_hlc({set, H, _})     -> H;
event_hlc({revoke, H})     -> H;
event_hlc({resolve, H, _}) -> H.

as_int(undefined) -> -1;
as_int(N) when is_integer(N) -> N.
