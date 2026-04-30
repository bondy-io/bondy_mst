%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_crdt).

-include("bondy_mst.hrl").
-include("bondy_oplog.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Behaviour for consumer-defined CRDTs (`_design/10_new_design.md` §9).

A *CRDT module* binds an oplog instance to a domain semantics. The
library is otherwise agnostic to event payload meaning; it is the
CRDT module — through its `interpret_cog/2` callback — that decides
what an event "means" and how concurrent operations are resolved.

The Concurrent Operation Group (COG) abstraction and the
`interpret_cog` interface come from Preston McCrary's *Canteen*
(UC Berkeley, 2022 — EECS-2022-160). The library carries the COG
machinery; the consumer supplies the interpretation function.

## Required callbacks

- `causal_tier/0` declares which Tier of metadata events of this CRDT
  carry (`tier_0` = none, `tier_1` = dot sets, `tier_2` = version
  vectors). The library does not enforce the declaration; it is a
  hint for tooling and documentation.

- `init/0` returns the bottom state — what the CRDT looks like when
  no events have ever been applied. Pure; called at first compaction
  on a fresh instance.

- `interpret_cog(Events, State) -> NewState` is the *workhorse*. It
  receives a batch of events (a Concurrent Operation Group) in key
  order and returns the updated state. It MUST be deterministic: same
  inputs ⇒ same output, on every replica. The library calls this on
  compaction (folding stable prefixes into snapshots) and on hot
  queries (folding live events on top of the latest snapshot).

- `query(Query, State) -> Result` projects the CRDT state for a
  client query. Pure.

## Optional callbacks

- `state_to_ops(OldState, NewState) -> [op()]` — for consumers whose
  external interface accepts state diffs (PUT-style APIs) instead of
  operations. Stage-5+ feature; not yet wired.

- `merge_values(Key, V1, V2) -> Merged` — for CRDT-valued events
  where the same MST key may be written with CRDT values that need
  merging (vs. the strict-uniqueness default). Stage-5+ feature.

## Determinism invariant

`interpret_cog/2`'s determinism is the foundation of the system's
*Strong Eventual Consistency* guarantee. A non-deterministic
implementation will break convergence: replicas that received the
same events will produce different snapshots and the system will
silently diverge.
""").

-callback causal_tier() -> tier_0 | tier_1 | tier_2.

-callback init() -> State :: term().

-callback interpret_cog(
    Events :: [bondy_oplog_event:t()],
    State :: term()
) -> NewState :: term().

-callback query(Query :: term(), State :: term()) -> Result :: term().

-callback state_to_ops(OldState :: term(), NewState :: term()) ->
    [bondy_oplog_event:op()].

-callback merge_values(
    Key :: bondy_oplog_event:event_key(),
    V1 :: term(),
    V2 :: term()
) -> Merged :: term().

-optional_callbacks([state_to_ops/2, merge_values/3]).
