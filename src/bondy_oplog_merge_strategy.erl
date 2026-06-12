%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_merge_strategy).

-include("bondy_doc.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Behaviour for resolving the (rare) case where the MST encounters two
values for the same event key.

Event keys are globally unique by construction (`{HLC, Origin, Seq}`),
so the only legitimate caller of `merge/3` is an idempotent peer
re-receive — the values must be equal. A divergent merge is a system
invariant violation and the default strategy
(`bondy_oplog_merge_strict_uniqueness`) crashes loudly.

When CRDT-valued events ship in Stage 5, a CRDT-aware strategy will
delegate to the value's CRDT merge. The hook is provided here so the
Stage 2 instance owner can configure it per instance without code
changes.
""").

-callback merge(
    Key :: bondy_oplog_event:event_key(),
    V1 :: term(),
    V2 :: term()
) -> Merged :: term().
