%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_transport_inline).
-behaviour(bondy_oplog_transport).

-include("bondy_mst.hrl").
-include("bondy_oplog.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
In-VM transport for tests and single-node deployments.

`peer_id` is interpreted as an `instance_id()` — the request is
dispatched to the local `bondy_oplog_instance` registered
under that name. This makes it trivial to wire up two replicas of the
same logical CRDT in a single VM for unit tests.

For real distributed sync, replace with a network-aware transport
(e.g. Distributed Erlang, Partisan, gRPC) implementing the same
behaviour.
""").

-export([request/4]).

-spec request(
    peer_id(),
    instance_id(),
    bondy_oplog_transport:request(),
    map()
) -> {ok, term()} | {error, term()}.

%% peer_id is treated as a local instance id.
request(PeerInstanceId, _InstanceId, Request, _Opts) when
    is_binary(PeerInstanceId)
->
    case bondy_oplog_instance:whereis(PeerInstanceId) of
        undefined ->
            {error, {peer_not_running, PeerInstanceId}};
        _Pid ->
            do_request(PeerInstanceId, Request)
    end;
request(Peer, _InstanceId, _Request, _Opts) ->
    {error, {invalid_peer_for_inline_transport, Peer}}.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
do_request(PeerInstance, get_root) ->
    %% Drain the peer's applier so the returned root reflects every
    %% WAL-fsynced event. Without this, a freshly-appended event that
    %% is still in the peer's overlay would not be in the returned
    %% root, and the local sync session would conclude
    %% prematurely-equal or miss pages.
    _ = bondy_oplog_instance:await_apply(PeerInstance),
    {ok, bondy_oplog_instance:root_hash(PeerInstance)};
do_request(PeerInstance, {get_pages, Hashes}) ->
    HashList =
        case is_list(Hashes) of
            true -> Hashes;
            false -> sets:to_list(Hashes)
        end,
    {ok, bondy_oplog_instance:get_pages(PeerInstance, HashList)};
do_request(PeerInstance, get_snapshot) ->
    case bondy_oplog_instance:snapshot(PeerInstance) of
        not_found -> {ok, no_snapshot};
        {ok, W, S} -> {ok, W, S}
    end.
