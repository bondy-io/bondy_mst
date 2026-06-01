%% =============================================================================
%% PropEr properties for `bondy_oplog_fold_presence_basic`.
%%
%% Implements the validation patterns in `FOLD_STRATEGY_DESIGN.md` §5:
%%
%%   - prop_apply_event_idempotent/0       (§5.1)
%%   - prop_apply_event_hlc_monotonic/0    (§5.2)
%%   - prop_encode_state_roundtrip/0       (§5.4)
%%   - prop_encode_event_roundtrip/0       (§5.4)
%%   - prop_gc_safe/0                      (§5.5)
%%
%% The `merge_states/2` callback is intentionally not implemented for
%% this fold (no concurrent writers per key); the commutativity property
%% (§5.3) is therefore not exercised here.
%%
%% Run with:
%%   rebar3 as test eunit --module=bondy_oplog_fold_presence_basic_proper_test
%% =============================================================================

-module(bondy_oplog_fold_presence_basic_proper_test).

%% PropEr defines `LET` and friends; include it before EUnit so EUnit's
%% `LET` doesn't shadow PropEr's.
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(MOD, bondy_oplog_fold_presence_basic).
-define(DEFAULT_NUMTESTS, 300).

-export([prop_apply_event_idempotent/0]).
-export([prop_apply_event_hlc_monotonic/0]).
-export([prop_encode_state_roundtrip/0]).
-export([prop_encode_event_roundtrip/0]).
-export([prop_gc_safe/0]).

%% =============================================================================
%% Generators
%% =============================================================================

%% A non-negative 64-bit integer matching `bondy_oplog_hlc:hlc()`. We
%% don't go up to 2^64-1 because PropEr's `integer/2` shrinks better with
%% bounded ranges and the fold's behaviour does not depend on absolute
%% magnitude.
hlc_gen() ->
    ?LET(
        {Phys, Log},
        {integer(0, 1000), integer(0, 1023)},
        bondy_oplog_hlc:encode(Phys, Log)
    ).

payload_gen() ->
    %% Opaque binary; small to keep shrinks readable.
    ?SIZED(N, resize(min(N, 16), binary())).

event_gen() ->
    oneof([
        {create, hlc_gen(), payload_gen()},
        {delete, hlc_gen()}
    ]).

%% Well-formed per-cell event sequences, matching the fold's input
%% contract (unique-by-construction key: at most one create, at most
%% one delete). When both events are present, the delete is causally
%% after the create (`H_d > H_c`), but arrival order is arbitrary.
%%
%% PropEr can otherwise generate inputs the fold has no contract for —
%% e.g. two concurrent creates at the same HLC with different payloads,
%% which is undefined behaviour for a unique-by-construction key.
events_gen() ->
    oneof([
        [],
        ?LET(
            {H, P},
            {hlc_gen(), payload_gen()},
            [{create, H, P}]
        ),
        ?LET(
            H,
            hlc_gen(),
            [{delete, H}]
        ),
        ?LET(
            {HC, P, Diff},
            {hlc_gen(), payload_gen(), integer(1, 100)},
            [{create, HC, P}, {delete, HC + Diff}]
        ),
        ?LET(
            {HC, P, Diff},
            {hlc_gen(), payload_gen(), integer(1, 100)},
            [{delete, HC + Diff}, {create, HC, P}]
        )
    ]).

%% A fold state produced by folding a fresh event sequence. Reaches
%% every reachable state (empty, live, dead) with realistic distribution.
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

%% §5.1 — apply_event is idempotent: applying the same event twice
%% produces the same state as applying it once.
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

%% §5.2 — hlc/1 is non-decreasing under apply_event/3.
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

%% §5.4 — encode_state/decode_state is a bijection on reachable states.
prop_encode_state_roundtrip() ->
    ?FORALL(
        State,
        state_gen(),
        ?MOD:decode_state(?MOD:encode_state(State)) =:= State
    ).

%% §5.4 — encode_event/decode_event is a bijection on events.
prop_encode_event_roundtrip() ->
    ?FORALL(
        Event,
        event_gen(),
        ?MOD:decode_event(?MOD:encode_event(Event)) =:= Event
    ).

%% §5.5 — GC safety: after folding events, dropping events with HLC
%% at-or-below gc_threshold and replaying the remainder on top of the
%% folded state must give the same state.
%%
%% For terminal-state folds like presence_basic, gc_threshold = hlc(S),
%% so the set of events with hlc > gc_threshold is empty after folding
%% the full sequence — but the property holds for partial folds too:
%% the replayed-from-S sub-fold is idempotent on already-absorbed
%% events.
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
%% EUnit wrapper — keeps the property suite in CI.
%% =============================================================================

properties_test_() ->
    {timeout, 120, fun() ->
        Opts = [{to_file, user}, {numtests, ?DEFAULT_NUMTESTS}],
        Props = [
            prop_apply_event_idempotent(),
            prop_apply_event_hlc_monotonic(),
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
%% Internal helpers
%% =============================================================================

event_hlc({create, H, _}) -> H;
event_hlc({delete, H}) -> H.

%% gc_threshold returns `undefined` on `empty`; treat that as -1 so the
%% "remaining" filter keeps everything (any non-negative HLC is > -1).
as_int(undefined) -> -1;
as_int(N) when is_integer(N) -> N.

%% Wrapper over %`apply_event/3`%; existing folds ignore Meta.
apply_ev(S, E) ->
    {NewState, _Delta} = ?MOD:apply_event(S, E, undefined),
    NewState.
