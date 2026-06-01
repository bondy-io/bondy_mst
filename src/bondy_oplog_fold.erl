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
- `apply_event/3` — fold one event; idempotent, HLC-monotonic. Returns
  `{NewState, ValueDelta | none}`. `NewState` is the new fold
  accumulator. `ValueDelta` is the change the event contributed to the
  user-facing value; `none` signals that the event made no value change
  (dedup, no-op, or a `value_equals_state` fold). The third argument is
  the WAL event key (`bondy_oplog_event:event_key()`) so folds can
  extract `key_origin`, `key_seq`, `key_hlc` without embedding them in
  the event payload. Folds that ignore meta just bind it to `_Meta`.
- `to_value/1` — projection from state to the user-facing value. Used
  by the substrate to seed the cell's value column on the *first*
  write to a key (cold-start); subsequent writes maintain the value
  incrementally via `apply_value_delta/2`.
- `hlc/1` — the maximum HLC absorbed by the state.
- `gc_threshold/1` — highest HLC whose events can be GC'd from the WAL.
- `encode_state/1`, `decode_state/1` — canonical state serialisation.
- `encode_event/1`, `decode_event/1` — canonical event serialisation.

It MAY also implement:

- `merge_states/2` — required for replica-side state reconciliation
  (commutative, associative, idempotent). Currently unused by the live
  substrate paths (sync ships ops, not states) but reserved for a
  future state-based bootstrap path.
- `page_refs/1` — page hashes referenced by an event. Returns `[]` for
  most folds; non-empty for folds that embed MST page references.
- `apply_value_delta/2` — combines an `OldValue` and a `ValueDelta`
  (as returned by `apply_event/3`) into a `NewValue`. Required for any
  fold whose `apply_event/3` may return a non-`none` delta. Folds with
  replacement semantics (LWW, Max, Min) implement it as
  `apply_value_delta(_, NewVal) -> NewVal`; folds with arithmetic
  deltas (PN-Counter) implement it as the appropriate combine
  (`apply_value_delta(V, D) -> V + D`).
- `value_equals_state/0` — returns `true` when `to_value(State)` is the
  identity over the encoded state bytes. The substrate then omits the
  value column from the cell frame (HasValueColumn=0) and treats the
  state bytes as the value bytes on HEAD reads. Such folds return
  `none` for the delta from every `apply_event/3` clause (the
  substrate does not maintain a separate value column for them).
  Defaults to `false`.

## Op-based, not state-based

Sync ships **events** (operations), not states. Each replica's
projection cell holds a *materialised fold-cache* over the locally
applied op sequence — a per-key accumulator the fold's `apply_event/3`
maintains. The "state" name reflects historical Erlang fold-vocabulary;
the contract is purely op-based. `merge_states/2` is reserved for a
future state-based bootstrap path that the live substrate does not
exercise today.

## Required properties

- **Idempotency**: replay of the same event is a no-op
  (`apply_event(apply_event(S, E, M), E, M)` produces the same state
  and `none` delta on the second call).
- **HLC monotonicity**: for any state `S` and event `E`,
  `hlc(NewState) >= hlc(S)` where `{NewState, _} = apply_event(S, E, M)`.
- **Encode round-trip**: `decode_state(encode_state(S)) == S` and the
  same for events.
- **Delta consistency**: when the delta is non-`none`,
  `apply_value_delta(to_value(S), Delta) == to_value(NewState)`.
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
    fold_value/0,
    value_delta/0,
    apply_result/0,
    meta/0,
    hlc/0,
    strategy/0
]).

-type state() :: any().
-type event() :: any().
-type fold_value() :: any().
-type value_delta() :: any().
-type meta() :: bondy_oplog_event:event_key() | undefined.
-type hlc() :: bondy_oplog_hlc:hlc().
-type strategy() :: module() | atom().

-type apply_result() ::
    {state() | {conflict, [state()]}, value_delta() | none}.

%% Public type aliases for fold authors; intentionally permissive.

-export([
    initial_value/1,
    apply_event/4,
    to_value/2,
    apply_value_delta/3,
    value_equals_state/1,
    hlc/2,
    gc_threshold/2,
    encode_event/2,
    decode_event/2,
    encode_state/2,
    decode_state/2,
    merge_states/3,
    page_refs/2,
    resolve_event/3,
    mod_of/1,
    is_known/1,
    tag_of/1,
    mod_of_tag/1,
    validate/1
]).

%% =============================================================================
%% BEHAVIOUR CALLBACKS
%% =============================================================================

-callback initial_value() -> state().

-callback apply_event(state(), event(), meta()) -> apply_result().

-callback to_value(state()) -> fold_value().

-callback hlc(state()) -> hlc().

-callback gc_threshold(state()) -> hlc() | undefined.

-callback encode_event(event()) -> binary().

-callback decode_event(binary()) -> event().

-callback encode_state(state()) -> binary().

-callback decode_state(binary()) -> state().

-callback merge_states(state(), state()) ->
    state() | {conflict, [state()]}.

-callback page_refs(event()) -> [hash()].

-callback apply_value_delta(fold_value(), value_delta()) -> fold_value().

-callback value_equals_state() -> boolean().

-doc """
Translate a logical event into a physical event by inspecting the
fold's current state. Called by the substrate **before** WAL append,
atomically within the cell's single-applier scope. Folds that don't
need server-side resolution don't export this callback (the substrate
checks via `erlang:function_exported/3`).

Return `passthrough` to indicate the logical event has no observable
effect against the current state (e.g. remove of an absent or
already-tombstoned key) — the substrate may elect to skip the WAL
append entirely.

Used by `aw_map` to convert `{remove_aw_key, K}` into the physical
`{remove, K, ObservedDots}` form whose dot list is captured atomically
from current state.
""".
-callback resolve_event(state(), event()) -> event() | passthrough.

-optional_callbacks([
    merge_states/2,
    page_refs/1,
    apply_value_delta/2,
    value_equals_state/0,
    resolve_event/2
]).

%% =============================================================================
%% DISPATCHER API
%% =============================================================================

-spec initial_value(strategy()) -> state().

initial_value(Strategy) ->
    (mod_of(Strategy)):initial_value().

-spec apply_event(strategy(), state(), event(), meta()) -> apply_result().

apply_event(Strategy, State, Event, Meta) ->
    (mod_of(Strategy)):apply_event(State, Event, Meta).

-spec to_value(strategy(), state()) -> fold_value().

to_value(Strategy, State) ->
    (mod_of(Strategy)):to_value(State).

-doc """
Combine an `OldValue` with a `ValueDelta` (as returned by
`apply_event/3`) into a `NewValue`. The substrate calls this whenever
`apply_event/3` returns a non-`none` delta on a non-`value_equals_state`
fold.

Required for any fold whose `apply_event/3` may emit a non-`none` delta.
Crashes with `{apply_value_delta_not_supported, Mod}` if the callback
is missing — that indicates a contract violation, not a runtime
condition.
""".
-spec apply_value_delta(strategy(), fold_value(), value_delta()) ->
    fold_value().

apply_value_delta(Strategy, Value, Delta) ->
    Mod = mod_of(Strategy),
    case erlang:function_exported(Mod, apply_value_delta, 2) of
        true -> Mod:apply_value_delta(Value, Delta);
        false -> erlang:error({apply_value_delta_not_supported, Mod})
    end.

-doc """
Returns the fold's declared `value_equals_state/0` value, defaulting to
`false` when the callback is not exported. When `true`, the substrate
omits the value column from the cell frame and treats the state bytes
as the value bytes on HEAD reads.
""".
-spec value_equals_state(strategy()) -> boolean().

value_equals_state(Strategy) ->
    Mod = mod_of(Strategy),
    case erlang:function_exported(Mod, value_equals_state, 0) of
        true -> Mod:value_equals_state();
        false -> false
    end.

-doc """
Translate a logical event into a physical event against the current
fold state. Returns the resolved event for substrate-side WAL append,
`passthrough` when the logical event has no effect (idempotent / target
absent), or the original event unchanged when the fold doesn't export
`resolve_event/2`.

Callers are responsible for the read-then-resolve being atomic against
the cell's single-applier scope.
""".
-spec resolve_event(strategy(), state(), event()) -> event() | passthrough.

resolve_event(Strategy, State, Event) ->
    Mod = mod_of(Strategy),
    case erlang:function_exported(Mod, resolve_event, 2) of
        true -> Mod:resolve_event(State, Event);
        false -> Event
    end.

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
        true -> Mod:page_refs(Event);
        false -> []
    end.

-spec mod_of(strategy()) -> module().

mod_of(presence_basic) -> bondy_oplog_fold_presence_basic;
mod_of(lww_register) -> bondy_oplog_fold_lww_register;
mod_of(strict_register) -> bondy_oplog_fold_strict_register;
mod_of(orset) -> bondy_oplog_fold_orset;
mod_of(ttl_presence) -> bondy_oplog_fold_ttl_presence;
mod_of(map_of_fields) -> bondy_oplog_fold_map_of_fields;
mod_of(aw_map) -> bondy_oplog_fold_aw_map;
mod_of(pn_counter) -> bondy_oplog_fold_pn_counter;
mod_of(g_counter) -> bondy_oplog_fold_g_counter;
mod_of(max_register) -> bondy_oplog_fold_max_register;
mod_of(min_register) -> bondy_oplog_fold_min_register;
mod_of(g_set) -> bondy_oplog_fold_g_set;
mod_of(Mod) when is_atom(Mod) -> Mod.

-doc """
Returns `true` if `Strategy` is a built-in shorthand atom (e.g.
`lww_register`, `map_of_fields`). Returns `false` for any other atom —
including loadable application-defined fold modules. Use `validate/1`
when you want the full "is this loadable and does it implement the
behaviour" check.
""".
-spec is_known(strategy()) -> boolean().

is_known(presence_basic) -> true;
is_known(lww_register) -> true;
is_known(strict_register) -> true;
is_known(orset) -> true;
is_known(ttl_presence) -> true;
is_known(map_of_fields) -> true;
is_known(aw_map) -> true;
is_known(pn_counter) -> true;
is_known(g_counter) -> true;
is_known(max_register) -> true;
is_known(min_register) -> true;
is_known(g_set) -> true;
is_known(Atom) when is_atom(Atom) -> false;
is_known(_) -> false.

-doc """
Canonical byte tag for a built-in fold shorthand. Used as the
sub-strategy identifier in composite encodings (`map_of_fields`,
`aw_map`).

Tag values are stable across releases — changing a mapping breaks
on-disk encoding. Tag 3 is intentionally reserved (gap left by the
original `map_of_fields` allocation). Tags 1, 2, 4 keep their
historical meanings to preserve `map_of_fields` wire compatibility.

Crashes with `function_clause` on unknown atoms — composite encoders
treat that as a contract violation, not a runtime condition.
""".
-spec tag_of(strategy()) -> non_neg_integer().

tag_of(lww_register) -> 1;
tag_of(strict_register) -> 2;
%% 3 reserved
tag_of(ttl_presence) -> 4;
tag_of(g_set) -> 5;
tag_of(pn_counter) -> 6;
tag_of(g_counter) -> 7;
tag_of(max_register) -> 8;
tag_of(min_register) -> 9;
tag_of(orset) -> 10;
tag_of(aw_map) -> 11;
tag_of(map_of_fields) -> 12.

-doc """
Inverse of `tag_of/1`. Decode a sub-strategy byte tag back into its
shorthand atom. Crashes with `function_clause` on unknown tags —
indicates either an on-disk corruption or an encoding from a newer
release.
""".
-spec mod_of_tag(non_neg_integer()) -> strategy().

mod_of_tag(1) -> lww_register;
mod_of_tag(2) -> strict_register;
mod_of_tag(4) -> ttl_presence;
mod_of_tag(5) -> g_set;
mod_of_tag(6) -> pn_counter;
mod_of_tag(7) -> g_counter;
mod_of_tag(8) -> max_register;
mod_of_tag(9) -> min_register;
mod_of_tag(10) -> orset;
mod_of_tag(11) -> aw_map;
mod_of_tag(12) -> map_of_fields.

-doc """
Validate that `Strategy` resolves to a module that implements the
mandatory `bondy_oplog_fold` callbacks: `initial_value/0`,
`apply_event/3`, `to_value/1`, `hlc/1`, `gc_threshold/1`,
`encode_state/1`, `decode_state/1`, `encode_event/1`, `decode_event/1`.

Returns `ok` when the module is loadable and exports every mandatory
callback. Returns `{error, {unknown_strategy, Term}}` when `Strategy`
isn't an atom, `{error, {module_not_loadable, Mod, LoadError}}` when
the module can't be loaded, and `{error, {missing_callbacks, Mod,
[{Name, Arity}]}}` when one or more mandatory callbacks are absent.

`merge_states/2`, `page_refs/1`, `apply_value_delta/2`, and
`value_equals_state/0` are optional callbacks and not checked here.
""".
-spec validate(strategy()) -> ok | {error, term()}.

validate(Strategy) when is_atom(Strategy) ->
    Mod = mod_of(Strategy),
    case code:ensure_loaded(Mod) of
        {module, Mod} ->
            Required = [
                {initial_value, 0},
                {apply_event, 3},
                {to_value, 1},
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
                [] -> ok;
                Missing -> {error, {missing_callbacks, Mod, Missing}}
            end;
        {error, Err} ->
            {error, {module_not_loadable, Mod, Err}}
    end;
validate(Other) ->
    {error, {unknown_strategy, Other}}.
