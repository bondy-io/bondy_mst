%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_origin).

-include("bondy_mst.hrl").
-include("bondy_oplog.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Origin identity for the MST event-store replication layer.

An *Origin* identifies a replica — the node-instance that creates events.
The replication layer treats `t/0` as an opaque binary; the only invariant
is that two distinct replicas must never share the same Origin (see
`_design/4_event_key_uniqueness.md`).

## Default behaviour

`default/0` returns a stable, per-VM 128-bit random identifier. It is
generated on first call and cached in `persistent_term`, so subsequent
calls within the same VM lifetime return the same value. **It is NOT
persisted across VM restarts** — callers that need a stable identity
across restarts must persist the value externally and pass it via the
`origin` option to
`bondy_oplog:start_instance/2`.

## Validation

`validate/1` enforces the only structural invariant: Origin is a
non-empty binary. Uniqueness is the operator's responsibility (see
`_design/4_event_key_uniqueness.md` §3).
""").

-type t() :: binary().

-export_type([t/0]).

-export([default/0]).
-export([new/0]).
-export([validate/1]).

%% =============================================================================
%% API
%% =============================================================================

?DOC("""
Returns the per-VM default Origin, generating it lazily on first call.

The value is cached in `persistent_term` under the key `{?MODULE, default}`.
It is intentionally **not persisted across VM restarts**: each restart is
treated as a new replica identity, which is conservative — peers that have
seen events from the previous identity will treat the restarted node as a
new participant. Production deployments that need identity continuity should
generate the id externally and pass it via the `origin` start_instance
option.
""").
-spec default() -> t().

default() ->
    Key = {?MODULE, default},
    case persistent_term:get(Key, undefined) of
        undefined ->
            Id = new(),
            ok = persistent_term:put(Key, Id),
            Id;
        Id when is_binary(Id) ->
            Id
    end.

?DOC("""
Generates a fresh 128-bit random Origin identifier.
""").
-spec new() -> t().

new() ->
    crypto:strong_rand_bytes(?BONDY_OPLOG_ORIGIN_BYTES).

?DOC("""
Validates an origin value. Returns `ok` if valid, `{error, Reason}` otherwise.
""").
-spec validate(term()) -> ok | {error, term()}.

validate(Bin) when is_binary(Bin), byte_size(Bin) > 0 ->
    ok;
validate(_) ->
    {error, invalid_origin}.
