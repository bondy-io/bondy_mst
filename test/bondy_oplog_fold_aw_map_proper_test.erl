%% =============================================================================
%% PropEr properties for `bondy_oplog_fold_aw_map`.
%%
%% Implements the validation patterns in
%% `_design/catalogue_expansion_plan.md` §4.5 plus the AW-Map-specific
%% encoding/state CAI properties:
%%
%%   - prop_apply_event_idempotent/0
%%   - prop_apply_event_hlc_monotonic/0
%%   - prop_merge_states_commutative/0
%%   - prop_merge_states_associative/0
%%   - prop_merge_states_idempotent/0
%%   - prop_encode_state_roundtrip/0
%%   - prop_encode_event_roundtrip/0
%%
%% All keys in a single generated state use the **same** SubFold
%% (`lww_register`). Heterogeneous sub-folds across the same key would
%% trigger `{strategy_mismatch, ...}` crashes which are out of scope
%% for these convergence properties — strategy mismatch is exercised
%% in the smoke tests.
%% =============================================================================

-module(bondy_oplog_fold_aw_map_proper_test).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(MOD, bondy_oplog_fold_aw_map).
-define(SUB_FOLD, lww_register).
-define(DEFAULT_NUMTESTS, 200).

-export([prop_apply_event_idempotent/0]).
-export([prop_apply_event_hlc_monotonic/0]).
-export([prop_merge_states_commutative/0]).
-export([prop_merge_states_associative/0]).
-export([prop_merge_states_idempotent/0]).
-export([prop_encode_state_roundtrip/0]).
-export([prop_encode_event_roundtrip/0]).

%% =============================================================================
%% Generators
%% =============================================================================

hlc_gen() ->
    ?LET({Phys, Log}, {integer(0, 1000), integer(0, 1023)},
         bondy_oplog_hlc:encode(Phys, Log)).

origin_gen() ->
    oneof([<<"n1">>, <<"n2">>, <<"n3">>]).

key_gen() ->
    oneof([<<"a">>, <<"b">>, <<"c">>]).

value_gen() ->
    oneof([<<"x">>, <<"y">>, <<"z">>]).

dot_gen() ->
    {origin_gen(), integer(1, 20)}.

%% A "rich event" carries its own meta — (hlc, origin, key, payload).
%% Per-Origin Seq is assigned in arrival order by `renumber_seqs/1` so
%% the substrate Seq invariant holds across the generated sequence.
rich_event_gen() ->
    {hlc_gen(), origin_gen(),
     oneof([
        {put, key_gen(), value_gen()},
        {apply_set, key_gen(), value_gen()},
        ?LET(N, integer(0, 3),
             {remove, key_gen(), vector(N, dot_gen())})
     ])}.

rich_events_gen() ->
    ?LET(Events, list(rich_event_gen()),
         renumber_seqs(Events)).

renumber_seqs(Events) ->
    {Out, _} = lists:mapfoldl(
        fun({H, O, P}, Seqs) ->
            S = maps:get(O, Seqs, 0) + 1,
            {{H, O, S, P}, Seqs#{O => S}}
        end, #{}, Events),
    Out.

apply_events(Events) ->
    lists:foldl(
        fun({H, O, S, P}, Acc) ->
            Meta = bondy_oplog_event:key(H, O, S),
            Event = to_physical_event(H, P),
            {NewState, _Delta} = ?MOD:apply_event(Acc, Event, Meta),
            NewState
        end,
        ?MOD:initial_value(),
        Events).

to_physical_event(H, {put, K, V}) ->
    {put, K, ?SUB_FOLD, {set, V, H}};
to_physical_event(H, {apply_set, K, V}) ->
    {apply, K, ?SUB_FOLD, {set, H, V}};
to_physical_event(_H, {remove, K, Dots}) ->
    {remove, K, lists:usort(Dots)}.

state_gen() ->
    ?LET(Events, rich_events_gen(), apply_events(Events)).

%% A standalone event with valid Meta for property testing.
event_with_meta_gen() ->
    ?LET({H, O, S, P},
         {hlc_gen(), origin_gen(), integer(1, 50),
          oneof([
             {put, key_gen(), value_gen()},
             {apply_set, key_gen(), value_gen()},
             ?LET(N, integer(0, 3),
                  {remove, key_gen(), vector(N, dot_gen())})
          ])},
        {bondy_oplog_event:key(H, O, S), to_physical_event(H, P)}).

encodeable_event_gen() ->
    ?LET({_Meta, Event}, event_with_meta_gen(), Event).

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
            ?MOD:hlc(NewState) >= H0
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
    ?FORALL(Event, encodeable_event_gen(),
        ?MOD:decode_event(?MOD:encode_event(Event)) =:= Event).

%% =============================================================================
%% EUnit wrapper
%% =============================================================================

properties_test_() ->
    {timeout, 240,
     fun() ->
        Opts = [{to_file, user}, {numtests, ?DEFAULT_NUMTESTS}],
        Props = [
            prop_apply_event_idempotent(),
            prop_apply_event_hlc_monotonic(),
            prop_merge_states_commutative(),
            prop_merge_states_associative(),
            prop_merge_states_idempotent(),
            prop_encode_state_roundtrip(),
            prop_encode_event_roundtrip()
        ],
        lists:foreach(
            fun(Prop) -> ?assert(proper:quickcheck(Prop, Opts)) end,
            Props
        )
     end}.
