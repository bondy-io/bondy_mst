%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_validator).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Behaviour for the pluggable event validator (`_design/10_new_design.md`
§10).

The library wires `sign_event/2` on local appends and `verify_event/2`
on remote receipts; consumers configure the validator per instance
(see `bondy_oplog_instance:start_link/2` opts).

Two implementations ship with the library:

- `bondy_oplog_validator_trust` — no-op; suitable for closed
  trusted clusters.
- `bondy_oplog_validator_crypto` — Ed25519 signing with
  per-Origin hash chain; suitable for Byzantine-tolerant deployments.

`detect_equivocation/2` is invoked when a peer-received event collides
with an already-known event under the same `{HLC, Origin, Seq}`. The
trust implementation returns `ok`; the crypto implementation returns
`{equivocation, Proof}` when the two events constitute proof that the
Origin signed contradictory statements.

## Verifier state lifetime

The per-instance applier process captures a read-only snapshot of the
validator state at its `init/1` (`bondy_oplog_applier:init/1`) and
reuses that snapshot for the lifetime of the process to verify every
peer-received event. `verify_event/2` is therefore called *off* the
instance gen_server, with a state value that may be older than the
state currently held by any other consumer.

**Contract for implementations:** `verify_event/2` MUST be safe to run
with a snapshot of `State` that is stale relative to wall-clock — i.e.
all data that affects the accept/reject decision must be derived from
the event itself plus values present in `State` at applier-start time.
There is no mechanism for the applier to observe later state
mutations.

If a future implementation needs runtime rotation/revocation (e.g.
adding a peer's public key without restarting the subtree), the
behaviour will need to grow a `refresh/1` callback and the applier a
matching `gen_server:cast` to swap its snapshot. Until then, treat
the validator state as configuration: changes require a subtree
restart.
""").

-callback init(InstanceId :: binary(), Opts :: map()) ->
    {ok, State :: term()} | {error, Reason :: term()}.

-callback sign_event(Event :: bondy_oplog_event:t(), State :: term()) ->
    {SignedEvent :: bondy_oplog_event:t(), NewState :: term()}.

-callback verify_event(Event :: bondy_oplog_event:t(), State :: term()) ->
    ok | {error, Reason :: term()}.

-callback detect_equivocation(
    E1 :: bondy_oplog_event:t(),
    E2 :: bondy_oplog_event:t()
) -> ok | {equivocation, Proof :: term()}.

-optional_callbacks([detect_equivocation/2]).
