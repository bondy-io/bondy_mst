%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_transport).

-include("bondy_mst.hrl").
-include("bondy_oplog.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Behaviour for the wire transport used by the sync protocol
(`_design/10_new_design.md` §7.5).

The library defines the protocol; the transport plugs in the actual
network. The library ships:

- `bondy_oplog_transport_inline` — for in-VM tests; routes
  requests to the local `bondy_oplog_instance` for the
  `peer_id` (treated as a local instance id).
- `bondy_oplog_transport_disterl` — Distributed Erlang
  transport; `peer_id` is a node atom.

Consumers using Partisan or another networking layer implement this
behaviour themselves (the library does not take Partisan as a hard
dependency).

## Request types

The library issues these requests, encoded as opaque terms; the
transport delivers them to the peer's responder which calls back into
`bondy_oplog_instance` and returns the reply.

| Request               | Reply                                       |
|---|---|
| `get_root`            | `{ok, hash() \| undefined}`                 |
| `{get_pages, Set}`    | `{ok, #{hash() => page()}}`                 |
| `get_snapshot`        | `{ok, event_key(), term()}` \| `{ok, no_snapshot}` |

The transport itself is stateless. Per-call options are passed through
the `Opts` argument.
""").

-type request() ::
    get_root
    | {get_pages, [bondy_mst:hash()] | sets:set(bondy_mst:hash())}
    | get_snapshot.

-type response() ::
    {ok, bondy_mst:hash() | undefined}
    | {ok, #{bondy_mst:hash() => bondy_mst_page:t()}}
    | {ok, no_snapshot}
    | {ok, bondy_oplog_event:event_key(), term()}.

-export_type([request/0]).
-export_type([response/0]).

-callback request(
    Peer :: peer_id(),
    InstanceId :: instance_id(),
    Request :: request(),
    Opts :: map()
) -> {ok, term()} | {error, term()}.
