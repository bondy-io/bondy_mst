%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_wal_sup).

-behaviour(supervisor).

-include("bondy_mst.hrl").
-include("bondy_oplog.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Dynamic supervisor that hosts one `bondy_oplog_wal` writer per running
instance.

`simple_one_for_one` of workers — instances are spawned at runtime
keyed by `InstanceId`, matching the convention established by
`bondy_oplog_instance_dyn_sup`.

This supervisor will eventually be folded into a per-instance
`bondy_oplog_instance_sup` (`one_for_all` over the WAL writer, the
applier, and the instance API gen_server). It currently stands on its
own so the WAL can be exercised in isolation while the applier
integration is still being built out.
""").

-export([start_link/0]).
-export([start_wal/2]).
-export([stop_wal/1]).
-export([init/1]).

-define(SERVER, ?MODULE).

%% =============================================================================
%% API
%% =============================================================================

-spec start_link() -> supervisor:startlink_ret().

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

?DOC("""
Spawns a WAL writer for `InstanceId`. The caller is responsible for
ensuring the same `InstanceId` is not already running — the per-
instance sub-supervisor (to land alongside the applier) will own
that uniqueness invariant.
""").
-spec start_wal(instance_id(), bondy_oplog_wal:opts()) ->
    {ok, pid()} | {error, term()}.

start_wal(InstanceId, Opts) when is_binary(InstanceId), is_map(Opts) ->
    supervisor:start_child(?SERVER, [InstanceId, Opts]).

-spec stop_wal(pid()) -> ok | {error, not_found}.

stop_wal(Pid) when is_pid(Pid) ->
    case supervisor:terminate_child(?SERVER, Pid) of
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
        id => bondy_oplog_wal,
        start => {bondy_oplog_wal, start_link, []},
        restart => transient,
        shutdown => 30000,
        type => worker,
        modules => [bondy_oplog_wal]
    },
    {ok, {SupFlags, [ChildSpec]}}.
