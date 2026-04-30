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
