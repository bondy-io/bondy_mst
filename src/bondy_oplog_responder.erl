%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_responder).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").
-include("bondy_oplog.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Node-level sync responder for distributed transports.

A single `gen_server` per node, registered locally under the atom
`?MODULE`. Distributed transport implementations
(`bondy_oplog_transport_disterl`, partisan-based, gRPC-based, ...)
deliver incoming sync requests *here*, and the responder dispatches
them to the right local `bondy_oplog_instance`.

## Why a single responder

Instance ids are arbitrary binaries chosen by the consumer, and we
support millions of instances per node. We can't register each
instance's gen_server under a distinct atom name (atom-table growth
is unbounded). A single fixed-atom responder per node is the
addressing primitive distributed transports can rely on.

## Concurrency model

Every incoming `{sync_protocol, InstanceId, Request}` call is handled
by a *short-lived worker process* — the responder's `handle_call`
spawns the worker, defers the reply (`{noreply, State}`) and the
worker calls `gen_server:reply/2` once the dispatch completes. The
responder's mailbox is therefore freed almost immediately and many
peers can drive sync requests in parallel.

This matters because all sync traffic from all peers across all
instances funnels through this one process. A serial design would
serialise the entire node's incoming sync rate.

## Wire shape

A remote transport issues:

```erlang
gen_server:call(
    {bondy_oplog_responder, PeerNode},
    {sync_protocol, InstanceId, Request},
    Timeout
).
```

| Request               | Reply                                              |
|---|---|
| `get_root`            | `{ok, hash() \| undefined}`                        |
| `{get_pages, Set}`    | `{ok, #{hash() => page()}}`                        |
| `get_snapshot`        | `{ok, no_snapshot}` \| `{ok, event_key(), term()}` |

Errors propagate as `{error, Reason}` (e.g. `{instance_not_running, Id}`).
""").

-export([start_link/0]).
-export([dispatch/2]).
-export([child_spec/0]).

%% gen_server
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).

%% =============================================================================
%% LIFECYCLE
%% =============================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec child_spec() -> supervisor:child_spec().

child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
    }.

%% =============================================================================
%% API
%% =============================================================================

?DOC("""
Locally dispatches a sync `Request` to the instance identified by
`InstanceId`. Read-only requests use the lock-free instance API and
do not round-trip the instance gen_server.
""").
-spec dispatch(instance_id(), bondy_oplog_transport:request()) ->
    {ok, term()} | {error, term()}.

dispatch(InstanceId, get_root) when is_binary(InstanceId) ->
    case bondy_oplog_instance:whereis(InstanceId) of
        undefined ->
            {error, {instance_not_running, InstanceId}};
        _Pid ->
            {ok, bondy_oplog_instance:root_hash(InstanceId)}
    end;
dispatch(InstanceId, {get_pages, Hashes}) when is_binary(InstanceId) ->
    case bondy_oplog_instance:whereis(InstanceId) of
        undefined ->
            {error, {instance_not_running, InstanceId}};
        _Pid ->
            HashList =
                case is_list(Hashes) of
                    true -> Hashes;
                    false -> sets:to_list(Hashes)
                end,
            {ok, bondy_oplog_instance:get_pages(InstanceId, HashList)}
    end;
dispatch(InstanceId, get_snapshot) when is_binary(InstanceId) ->
    case bondy_oplog_instance:whereis(InstanceId) of
        undefined ->
            {error, {instance_not_running, InstanceId}};
        _Pid ->
            case bondy_oplog_instance:snapshot(InstanceId) of
                not_found -> {ok, no_snapshot};
                {ok, W, S} -> {ok, W, S}
            end
    end.

%% =============================================================================
%% gen_server CALLBACKS
%% =============================================================================

init([]) ->
    process_flag(trap_exit, true),
    {ok, #{}}.

handle_call({sync_protocol, InstanceId, Request}, From, State) ->
    %% Spawn-and-go: free the responder's mailbox immediately. The
    %% worker dispatches and uses gen_server:reply/2 to answer.
    _ = spawn(fun() ->
        Reply =
            try
                dispatch(InstanceId, Request)
            catch
                C:R:S ->
                    ?LOG_WARNING(#{
                        description => "responder dispatch raised",
                        instance_id => InstanceId,
                        request => Request,
                        class => C,
                        reason => R,
                        stacktrace => S
                    }),
                    {error, {dispatch_failed, R}}
            end,
        gen_server:reply(From, Reply)
    end),
    {noreply, State};
handle_call(_Req, _From, State) ->
    {reply, {error, badcall}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
