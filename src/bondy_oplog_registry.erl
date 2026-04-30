%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_registry).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").
-include("bondy_oplog.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Node-shared per-instance read-snapshot registry.

A single ETS `set` table per node, keyed by `instance_id()`, holding
the latest read-relevant state of every running instance:

| Field         | Refreshed on |
|---|---|
| `pid`         | `init/terminate` of the instance gen_server |
| `origin`      | `init` (immutable thereafter) |
| `mst`         | every state-mutating handle_call |
| `watermark`   | compact / load_snapshot |
| `snapshot`    | compact / load_snapshot |
| `crdt_module` | `init` (immutable thereafter) |
| `live_size`   | every state-mutating handle_call |

## Why ETS, not persistent_term

`persistent_term:put/2` triggers a *global GC scan of every process*
on the node. With many instances doing many writes, that's a
non-starter. ETS `insert` is constant-time, no global side effects,
and `read_concurrency: true` lets readers run in parallel with
writes.

## Concurrency model

The table is `public` so each instance gen_server writes its own row
directly — no roundtrip through this module's gen_server on the hot
write path. The contract: **only the owning instance gen_server
writes its row**. Other processes are read-only.

This module's gen_server exists only to own the table (so the table
survives any single instance gen_server crash) and to keep the
table's lifecycle tied to a supervisor child.
""").

-define(TABLE, bondy_oplog_registry_tab).

-record(entry, {
    instance_id :: instance_id(),
    pid :: pid(),
    origin :: bondy_oplog_origin:t(),
    mst :: bondy_mst:t(),
    watermark :: undefined | bondy_oplog_event:event_key(),
    snapshot :: undefined | {bondy_oplog_event:event_key(), term()},
    crdt_module :: module() | undefined,
    live_size :: non_neg_integer()
}).

-record(state, {}).

-type entry() :: #{
    instance_id := instance_id(),
    pid := pid(),
    origin := bondy_oplog_origin:t(),
    mst := bondy_mst:t(),
    watermark := undefined | bondy_oplog_event:event_key(),
    snapshot := undefined | {bondy_oplog_event:event_key(), term()},
    crdt_module := module() | undefined,
    live_size := non_neg_integer()
}.

-export_type([entry/0]).

%% Lifecycle
-export([start_link/0]).
-export([child_spec/0]).

%% Per-instance gen_server hooks
-export([register/1]).
-export([unregister/1]).
-export([publish/1]).

%% Reads
-export([lookup/1]).
-export([pid/1]).
-export([origin/1]).
-export([mst/1]).
-export([watermark/1]).
-export([snapshot/1]).
-export([crdt_module/1]).
-export([live_size/1]).

%% gen_server callbacks
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
%% INSTANCE-FACING WRITES
%% =============================================================================

?DOC("""
Inserts (or replaces) the row for an instance. Called by the instance
gen_server in `init/1` once the initial MST handle and snapshot state
are computed. Direct ETS write — no roundtrip through this module's
gen_server.
""").
-spec register(entry()) -> ok.

register(Entry) when is_map(Entry) ->
    true = ets:insert(?TABLE, to_record(Entry)),
    ok.

?DOC("""
Removes the row for an instance. Called by the instance gen_server in
`terminate/2`.
""").
-spec unregister(instance_id()) -> ok.

unregister(InstanceId) when is_binary(InstanceId) ->
    true = ets:delete(?TABLE, InstanceId),
    ok.

?DOC("""
Republishes the row for an instance. Same shape as `register/1`;
named differently for readability at call sites — `publish` is the
hot-path operation done after every state-mutating handle_call.
""").
-spec publish(entry()) -> ok.

publish(Entry) ->
    register(Entry).

%% =============================================================================
%% READS
%% =============================================================================

?DOC("""
Returns the full registry row for an instance, or `not_found`.
A single ETS lookup; safe to call from any process.
""").
-spec lookup(instance_id()) -> {ok, entry()} | not_found.

lookup(InstanceId) when is_binary(InstanceId) ->
    case ets:lookup(?TABLE, InstanceId) of
        [Entry] -> {ok, to_map(Entry)};
        [] -> not_found
    end.

-spec pid(instance_id()) -> pid() | undefined.

pid(InstanceId) ->
    field(InstanceId, #entry.pid).

-spec origin(instance_id()) -> bondy_oplog_origin:t() | undefined.

origin(InstanceId) ->
    field(InstanceId, #entry.origin).

-spec mst(instance_id()) -> bondy_mst:t() | undefined.

mst(InstanceId) ->
    field(InstanceId, #entry.mst).

-spec watermark(instance_id()) ->
    undefined | bondy_oplog_event:event_key().

watermark(InstanceId) ->
    field(InstanceId, #entry.watermark).

-spec snapshot(instance_id()) ->
    undefined | {bondy_oplog_event:event_key(), term()}.

snapshot(InstanceId) ->
    field(InstanceId, #entry.snapshot).

-spec crdt_module(instance_id()) -> module() | undefined.

crdt_module(InstanceId) ->
    field(InstanceId, #entry.crdt_module).

-spec live_size(instance_id()) -> non_neg_integer() | undefined.

live_size(InstanceId) ->
    field(InstanceId, #entry.live_size).

%% =============================================================================
%% gen_server CALLBACKS
%% =============================================================================

init([]) ->
    process_flag(trap_exit, true),
    _Tab = ets:new(?TABLE, [
        named_table,
        set,
        public,
        {keypos, #entry.instance_id},
        {read_concurrency, true},
        {write_concurrency, true}
    ]),
    {ok, #state{}}.

handle_call(_Req, _From, State) ->
    {reply, {error, badcall}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
field(InstanceId, FieldPos) when is_binary(InstanceId) ->
    try
        ets:lookup_element(?TABLE, InstanceId, FieldPos)
    catch
        error:badarg -> undefined
    end.

%% @private
to_record(#{
    instance_id := Id,
    pid := Pid,
    origin := Origin,
    mst := MST,
    watermark := W,
    snapshot := S,
    crdt_module := C,
    live_size := L
}) ->
    #entry{
        instance_id = Id,
        pid = Pid,
        origin = Origin,
        mst = MST,
        watermark = W,
        snapshot = S,
        crdt_module = C,
        live_size = L
    }.

%% @private
to_map(#entry{
    instance_id = Id,
    pid = Pid,
    origin = O,
    mst = M,
    watermark = W,
    snapshot = S,
    crdt_module = C,
    live_size = L
}) ->
    #{
        instance_id => Id,
        pid => Pid,
        origin => O,
        mst => M,
        watermark => W,
        snapshot => S,
        crdt_module => C,
        live_size => L
    }.
