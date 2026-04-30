%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_instance_dyn_sup).

-behaviour(supervisor).

-include("bondy_mst.hrl").
-include("bondy_oplog.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Dynamic supervisor that hosts one `bondy_oplog_instance` worker per
running instance (`_design/10_new_design.md` §11.1).

`simple_one_for_one` of *workers* — instances are spawned at runtime
and their identity is the binary instance id, not a static child id.
A previous design wrapped each worker in a per-instance one_for_all
supervisor; that level was dropped because no per-instance auxiliary
processes were ever added and it doubled the per-instance process
count.
""").

-export([start_link/0]).
-export([start_instance/2]).
-export([stop_instance/1]).
-export([init/1]).

-define(SERVER, ?MODULE).

%% =============================================================================
%% API
%% =============================================================================

-spec start_link() -> supervisor:startlink_ret().

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

?DOC("""
Spawns a per-instance worker. Idempotent: if an instance with the
same id is already running, returns its current pid.
""").
-spec start_instance(instance_id(), bondy_oplog_instance:opts()) ->
    {ok, pid()} | {error, term()}.

start_instance(InstanceId, Opts) when
    is_binary(InstanceId), is_map(Opts)
->
    case bondy_oplog_instance:whereis(InstanceId) of
        undefined ->
            supervisor:start_child(?SERVER, [InstanceId, Opts]);
        Pid ->
            {ok, Pid}
    end.

-spec stop_instance(instance_id() | pid()) -> ok | {error, not_found}.

stop_instance(InstanceId) when is_binary(InstanceId) ->
    case bondy_oplog_instance:whereis(InstanceId) of
        undefined ->
            {error, not_found};
        Pid ->
            stop_instance(Pid)
    end;
stop_instance(WorkerPid) when is_pid(WorkerPid) ->
    case supervisor:terminate_child(?SERVER, WorkerPid) of
        ok -> ok;
        {error, not_found} -> {error, not_found}
    end.

%% =============================================================================
%% supervisor CALLBACKS
%% =============================================================================

init([]) ->
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 10,
        period => 10
    },
    ChildSpec = #{
        id => bondy_oplog_instance,
        start => {bondy_oplog_instance, start_link, []},
        restart => transient,
        shutdown => 5000,
        type => worker,
        modules => [bondy_oplog_instance]
    },
    {ok, {SupFlags, [ChildSpec]}}.
