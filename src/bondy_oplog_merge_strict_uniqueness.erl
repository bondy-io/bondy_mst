%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_merge_strict_uniqueness).
-behaviour(bondy_oplog_merge_strategy).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Default merge strategy for opaque events.

Identical values are returned unchanged (the idempotent peer re-receive
case). Divergent values for the same `{HLC, Origin, Seq}` key violate
a system invariant and raise a `divergent_value` error so the failure
is surfaced rather than silently absorbed.

Replace with a CRDT-aware strategy when the value type is itself a
CRDT (Stage 5).
""").

-export([merge/3]).

%% =============================================================================
%% bondy_oplog_merge_strategy CALLBACKS
%% =============================================================================

merge(_Key, V, V) ->
    V;
merge(Key, V1, V2) ->
    ?LOG_ERROR(#{
        description =>
            "MST merger invoked with divergent values; "
            "system invariant violated",
        key => Key,
        v1 => V1,
        v2 => V2
    }),
    erlang:error({divergent_value, Key, V1, V2}).
