%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_crdt_commutative).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Helper + behaviour for **commutative** operation-based CRDTs.

It carries the per-cell COG-Interpreter machinery (sort a cell's events by
the canonical key, fold a single-op step over them) for the majority of
CRDTs whose concurrent operations **commute** — registers, sets, counters,
presence.

It depends only on `bondy_oplog_event`; there is no longer any state-based
fold family to depend on (the re-grounding removed it in PR-Z), so building
on it cannot re-introduce the state-based drift this re-grounding exists to
remove.

## The contract

A commutative CRDT module implements the small per-operation step
`apply_op/3` plus the projection-seam callbacks, and delegates its
`bondy_oplog_crdt:interpret_cog/2` to `interpret_cog/3` here:

```erlang
-behaviour(bondy_oplog_crdt).
-behaviour(bondy_oplog_crdt_commutative).

interpret_cog(Events, State) ->
    bondy_oplog_crdt_commutative:interpret_cog(?MODULE, Events, State).

apply_op(State, Op, Key) -> ...   %% one operation, in key order
order_independent() -> true.
```

## Why one step covers two paths

For a commutative CRDT, the order in which operations are applied cannot
change the result. Therefore:

- **Authoritative batch path** (`interpret_cog/3`): sort the cell's events
  into canonical key order `{hlc, origin, seq}` and fold `apply_op/3`. The
  sort makes the result a deterministic function of the event *set*,
  independent of arrival order — the Strong Eventual Consistency
  invariant.

- **Eager O(1) write path** (`apply_op/5`): apply a single new operation
  onto the materialised state as it arrives. Because the CRDT commutes,
  this converges to exactly the batch result without re-folding the
  cell's history — the cheap projection-maintenance path
  (`architecture_regrounding_plan.md` §6, Option B).

## tier_0 vs tier_2 step

A module implements **exactly one** per-operation step:

- `apply_op/3(State, Op, Key)` — tier_0 (commutative, context-free). The
  scalar dot `Key` is all the causality the step needs.
- `apply_op/4(State, Op, Key, Context)` — tier_2 (DVV). `Context` is the
  write's observed causal context (the event `meta`), so the step can
  join via `bondy_dvvset`. Both forms are optional callbacks; the helper
  routes to whichever the module exports.

Non-commutative CRDTs (an observed-remove map, a bounded counter) do
**not** use this module: they must re-interpret the cell's live group on
write, so they implement `bondy_oplog_crdt:interpret_cog/2` directly and
declare `order_independent() -> false`.

## Operation extraction

Both paths read the operation from a `bondy_oplog_event`. A catalogue
cell wraps the CRDT operation as `{cell_apply, Bucket, Key, Op}`; a
monolithic single-CRDT instance carries the operation directly. `op_of/1`
unwraps the former and passes the latter through, so the same helper
serves both shapes.
""").

-export([interpret_cog/3]).
-export([apply_op/4]).
-export([apply_op/5]).
-export([op_of/1]).
-export([context_of_event/1]).

%% A commutative CRDT module is ALSO a `bondy_oplog_crdt` (it inherits
%% `init/0`, `to_value/1`, `hlc/1`, `encode_state/1`, `decode_state/1`
%% and the optional refinements from there). This behaviour adds the
%% single-operation step the helper calls back into — in **one** of two
%% forms (the helper routes to whichever the module exports):
%%   - `apply_op/3` — tier_0, context-free.
%%   - `apply_op/4` — tier_2, with the write's causal context.

-callback apply_op(
    State :: term(),
    Op :: term(),
    Key :: bondy_oplog_event:event_key()
) -> NewState :: term().

-callback apply_op(
    State :: term(),
    Op :: term(),
    Key :: bondy_oplog_event:event_key(),
    Context :: term()
) -> NewState :: term().

-optional_callbacks([apply_op/3, apply_op/4]).

%% =============================================================================
%% API
%% =============================================================================

-doc """
Interpret a Concurrent Operation Group — a batch of a cell's events — on
top of `State`, in canonical key order, by folding the module's
`apply_op/3`. Deterministic in the event *set*: any permutation of the
same events yields the same state.
""".
-spec interpret_cog(
    Mod :: module(),
    Events :: [bondy_oplog_event:t()],
    State :: term()
) -> NewState :: term().

interpret_cog(Mod, Events, State) when is_list(Events) ->
    lists:foldl(
        fun(E, S) ->
            apply_op(
                Mod,
                S,
                op_of(E),
                bondy_oplog_event:key(E),
                context_of_event(E)
            )
        end,
        State,
        sort_by_key(Events)
    ).

-doc """
Apply a single operation onto the materialised state — the O(1)
incremental write step. Safe for a commutative CRDT regardless of arrival
order; converges to the same state as `interpret_cog/3`. Context-free
form; equivalent to `apply_op/5` with `undefined` context (tier_0).
""".
-spec apply_op(
    Mod :: module(),
    State :: term(),
    Op :: term(),
    Key :: bondy_oplog_event:event_key()
) -> NewState :: term().

apply_op(Mod, State, Op, Key) ->
    apply_op(Mod, State, Op, Key, undefined).

-doc """
Apply a single operation with the write's causal `Context` (the event
`meta` — `undefined` for tier_0). Routes to the module's `apply_op/4`
(tier_2) when exported, else its `apply_op/3` (tier_0, context ignored).
""".
-spec apply_op(
    Mod :: module(),
    State :: term(),
    Op :: term(),
    Key :: bondy_oplog_event:event_key(),
    Context :: term()
) -> NewState :: term().

apply_op(Mod, State, Op, Key, Context) ->
    case erlang:function_exported(Mod, apply_op, 4) of
        true -> Mod:apply_op(State, Op, Key, Context);
        false -> Mod:apply_op(State, Op, Key)
    end.

-doc """
Extract the CRDT operation from an event, unwrapping a catalogue
`{cell_apply, Bucket, Key, Op}` wrapper; any other op shape passes
through unchanged.
""".
-spec op_of(bondy_oplog_event:t()) -> term().

op_of(Event) ->
    case bondy_oplog_event:op(Event) of
        {cell_apply, _Bucket, _Key, Op} -> Op;
        Op -> Op
    end.

-doc """
The write's causal context, carried in the event `meta` field. `undefined`
for tier_0 events (no context was stamped). tier_2 events carry the
observed version vector the substrate stamped at the origin.
""".
-spec context_of_event(bondy_oplog_event:t()) -> term().

context_of_event(Event) ->
    bondy_oplog_event:meta(Event).

%% =============================================================================
%% INTERNAL
%% =============================================================================

%% @private
%% Canonical key order: `{hlc, origin, seq}` via the substrate comparator.
%% A stable sort preserves input order on equal keys (which cannot occur for
%% distinct operations — the dot is unique — but keeps the fold total).
sort_by_key(Events) ->
    lists:sort(
        fun(A, B) ->
            bondy_oplog_event:compare_keys(
                bondy_oplog_event:key(A), bondy_oplog_event:key(B)
            ) =/= gt
        end,
        Events
    ).
