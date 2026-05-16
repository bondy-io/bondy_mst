%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_fold).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Behaviour for per-namespace **event folds**.

A fold defines how WAL events accumulate into the projection cell value
for a given namespace. The substrate is fold-agnostic — it appends and
replicates events — and each namespace plugs in a fold module that gives
those events meaning.

This module is the **behaviour contract** and the **dispatch surface**:
consumer modules call `apply_event/3`, `initial_value/1`, etc. with a
strategy identifier (an atom shorthand or a module name).

## Behaviour requirements

A fold module MUST implement these callbacks:

- `initial_value/0` — the zero state.
- `apply_event/2` — fold one event; idempotent, HLC-monotonic.
- `hlc/1` — the maximum HLC absorbed by the state.
- `gc_threshold/1` — highest HLC whose events can be GC'd from the WAL.
- `encode_state/1`, `decode_state/1` — canonical state serialisation.
- `encode_event/1`, `decode_event/1` — canonical event serialisation.

It MAY also implement:

- `merge_states/2` — required for replica-side state reconciliation
  (commutative, associative, idempotent). Folds whose state is terminal
  in a single event sequence (e.g. presence machines) can skip this.
- `page_refs/1` — page hashes referenced by an event. Returns `[]` for
  most folds; non-empty for folds that embed MST page references.

## Required properties

- **Idempotency**: `apply_event(apply_event(S, E), E) == apply_event(S, E)`.
- **HLC monotonicity**: for any state `S` and event `E`,
  `hlc(apply_event(S, E)) >= hlc(S)`.
- **Encode round-trip**: `decode_state(encode_state(S)) == S` and the
  same for events.
- **Merge commutativity** (when implemented):
  `merge_states(A, B) == merge_states(B, A)`.

See `FOLD_STRATEGY_DESIGN.md` §2.1 for the full contract and §5 for the
validation patterns every fold MUST satisfy.

## Strategy resolution

Callers pass either a shorthand atom (`presence_basic`, `lww_register`,
etc.) or a module name directly. `mod_of/1` resolves the shorthand;
unknown atoms are assumed to be application-defined modules.
""").

-export_type([
    state/0,
    event/0,
    hlc/0,
    strategy/0
]).

-type state()    :: any().
-type event()    :: any().
-type hlc()      :: bondy_oplog_hlc:hlc().
-type strategy() :: module() | atom().

%% Public type aliases for fold authors; intentionally permissive.

-export([
    initial_value/1,
    apply_event/3,
    hlc/2,
    gc_threshold/2,
    encode_event/2,
    decode_event/2,
    encode_state/2,
    decode_state/2,
    merge_states/3,
    page_refs/2,
    mod_of/1,
    is_known/1,
    validate/1
]).


%% =============================================================================
%% BEHAVIOUR CALLBACKS
%% =============================================================================

-callback initial_value() -> state().

-callback apply_event(state(), event()) ->
    state() | {conflict, [state()]}.

-callback hlc(state()) -> hlc().

-callback gc_threshold(state()) -> hlc() | undefined.

-callback encode_event(event()) -> binary().

-callback decode_event(binary()) -> event().

-callback encode_state(state()) -> binary().

-callback decode_state(binary()) -> state().

-callback merge_states(state(), state()) ->
    state() | {conflict, [state()]}.

-callback page_refs(event()) -> [hash()].

-optional_callbacks([merge_states/2, page_refs/1]).

%% =============================================================================
%% DISPATCHER API
%% =============================================================================


-spec initial_value(strategy()) -> state().

initial_value(Strategy) ->
    (mod_of(Strategy)):initial_value().


-spec apply_event(strategy(), state(), event()) ->
    state() | {conflict, [state()]}.

apply_event(Strategy, State, Event) ->
    (mod_of(Strategy)):apply_event(State, Event).


-spec hlc(strategy(), state()) -> hlc().

hlc(Strategy, State) ->
    (mod_of(Strategy)):hlc(State).


-spec gc_threshold(strategy(), state()) -> hlc() | undefined.

gc_threshold(Strategy, State) ->
    (mod_of(Strategy)):gc_threshold(State).


-spec encode_event(strategy(), event()) -> binary().

encode_event(Strategy, Event) ->
    (mod_of(Strategy)):encode_event(Event).


-spec decode_event(strategy(), binary()) -> event().

decode_event(Strategy, Bin) ->
    (mod_of(Strategy)):decode_event(Bin).


-spec encode_state(strategy(), state()) -> binary().

encode_state(Strategy, State) ->
    (mod_of(Strategy)):encode_state(State).


-spec decode_state(strategy(), binary()) -> state().

decode_state(Strategy, Bin) ->
    (mod_of(Strategy)):decode_state(Bin).


-spec merge_states(strategy(), state(), state()) ->
    state() | {conflict, [state()]}.

merge_states(Strategy, A, B) ->
    Mod = mod_of(Strategy),
    case erlang:function_exported(Mod, merge_states, 2) of
        true ->
            Mod:merge_states(A, B);
        false ->
            erlang:error({merge_states_not_supported, Mod})
    end.


-spec page_refs(strategy(), event()) -> [hash()].

page_refs(Strategy, Event) ->
    Mod = mod_of(Strategy),
    case erlang:function_exported(Mod, page_refs, 1) of
        true  -> Mod:page_refs(Event);
        false -> []
    end.


-spec mod_of(strategy()) -> module().

mod_of(presence_basic)  -> bondy_oplog_fold_presence_basic;
mod_of(lww_register)    -> bondy_oplog_fold_lww_register;
mod_of(strict_register) -> bondy_oplog_fold_strict_register;
mod_of(orset)           -> bondy_oplog_fold_orset;
mod_of(ttl_presence)    -> bondy_oplog_fold_ttl_presence;
mod_of(map_of_fields)   -> bondy_oplog_fold_map_of_fields;
mod_of(Mod) when is_atom(Mod) -> Mod.


-doc """
Returns `true` if `Strategy` is a built-in shorthand atom (e.g.
`lww_register`, `map_of_fields`). Returns `false` for any other atom —
including loadable application-defined fold modules. Use `validate/1`
when you want the full "is this loadable and does it implement the
behaviour" check.
""".
-spec is_known(strategy()) -> boolean().

is_known(presence_basic)  -> true;
is_known(lww_register)    -> true;
is_known(strict_register) -> true;
is_known(orset)           -> true;
is_known(ttl_presence)    -> true;
is_known(map_of_fields)   -> true;
is_known(Atom) when is_atom(Atom) -> false;
is_known(_) -> false.


-doc """
Validate that `Strategy` resolves to a module that implements the
mandatory `bondy_oplog_fold` callbacks: `initial_value/0`,
`apply_event/2`, `hlc/1`, `gc_threshold/1`, `encode_state/1`,
`decode_state/1`, `encode_event/1`, `decode_event/1`.

Returns `ok` when the module is loadable and exports every mandatory
callback. Returns `{error, {unknown_strategy, Term}}` when `Strategy`
isn't an atom, `{error, {module_not_loadable, Mod, LoadError}}` when
the module can't be loaded, and `{error, {missing_callbacks, Mod,
[{Name, Arity}]}}` when one or more mandatory callbacks are absent.

`merge_states/2` and `page_refs/1` are optional callbacks and not
checked here.
""".
-spec validate(strategy()) -> ok | {error, term()}.

validate(Strategy) when is_atom(Strategy) ->
    Mod = mod_of(Strategy),
    case code:ensure_loaded(Mod) of
        {module, Mod} ->
            Required = [
                {initial_value, 0},
                {apply_event, 2},
                {hlc, 1},
                {gc_threshold, 1},
                {encode_state, 1},
                {decode_state, 1},
                {encode_event, 1},
                {decode_event, 1}
            ],
            Missing = [
                FA
                || {F, A} = FA <- Required,
                   not erlang:function_exported(Mod, F, A)
            ],
            case Missing of
                []      -> ok;
                Missing -> {error, {missing_callbacks, Mod, Missing}}
            end;
        {error, Err} ->
            {error, {module_not_loadable, Mod, Err}}
    end;
validate(Other) ->
    {error, {unknown_strategy, Other}}.
