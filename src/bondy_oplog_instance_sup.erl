%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_instance_sup).

-behaviour(supervisor).

-include("bondy_mst.hrl").
-include("bondy_oplog.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Per-instance one_for_all supervisor.

Holds the three processes that together implement one running
instance:

| Order | Child | Role |
|---|---|---|
| 1 | `bondy_oplog_instance` | MST owner, validator, public API entry point |
| 2 | `bondy_oplog_wal`      | Per-instance write-ahead log writer |
| 3 | `bondy_oplog_applier`  | Reads the WAL and feeds the instance |

`one_for_all` because the three are interdependent: an instance with
a dead WAL cannot serve writes, and a WAL with no applier accumulates
unconsumed events the retention sweep cannot clear. A crash anywhere
in the subtree restarts the whole subtree, and recovery on reopen
reconciles the on-disk state.

Start order matters: the instance creates its registry row before the
WAL writes `wal_pid` and before the applier writes `applier_pid`. The
WAL is up before the applier opens its reader; the applier resolves
the WAL pid and instance pid from the registry at init time.
""").

-export([start_link/2]).
-export([init/1]).

-export([wal_pid/1]).
-export([instance_pid/1]).
-export([applier_pid/1]).

%% =============================================================================
%% API
%% =============================================================================

-spec start_link(instance_id(), bondy_oplog_instance:opts()) ->
    supervisor:startlink_ret().

start_link(InstanceId, Opts) when
    is_binary(InstanceId), byte_size(InstanceId) > 0, is_map(Opts)
->
    supervisor:start_link(?MODULE, {InstanceId, Opts}).

?DOC("""
Returns the pid of the per-instance `bondy_oplog_instance` child for
the given subtree supervisor pid.
""").
-spec instance_pid(pid()) -> pid() | undefined.

instance_pid(SupPid) when is_pid(SupPid) ->
    find_child(SupPid, bondy_oplog_instance).

?DOC("""
Returns the pid of the per-instance `bondy_oplog_wal` child.
""").
-spec wal_pid(pid()) -> pid() | undefined.

wal_pid(SupPid) when is_pid(SupPid) ->
    find_child(SupPid, bondy_oplog_wal).

?DOC("""
Returns the pid of the per-instance `bondy_oplog_applier` child.
""").
-spec applier_pid(pid()) -> pid() | undefined.

applier_pid(SupPid) when is_pid(SupPid) ->
    find_child(SupPid, bondy_oplog_applier).

%% =============================================================================
%% supervisor CALLBACKS
%% =============================================================================

init({InstanceId, Opts}) ->
    SupFlags = #{
        strategy => one_for_all,
        intensity => 5,
        period => 10
    },
    InstanceSpec = #{
        id => bondy_oplog_instance,
        start => {bondy_oplog_instance, start_link, [InstanceId, Opts]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [bondy_oplog_instance]
    },
    WalOpts = wal_opts(InstanceId, Opts),
    WalSpec = #{
        id => bondy_oplog_wal,
        start => {bondy_oplog_wal, start_link, [InstanceId, WalOpts]},
        restart => permanent,
        shutdown => 30000,
        type => worker,
        modules => [bondy_oplog_wal]
    },
    ApplierOpts = applier_opts(InstanceId, Opts),
    ApplierSpec = #{
        id => bondy_oplog_applier,
        start => {bondy_oplog_applier, start_link, [ApplierOpts]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [bondy_oplog_applier]
    },
    {ok, {SupFlags, [InstanceSpec, WalSpec, ApplierSpec]}}.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
find_child(SupPid, Id) ->
    try supervisor:which_children(SupPid) of
        Children ->
            case lists:keyfind(Id, 1, Children) of
                {Id, Pid, _Type, _Mods} when is_pid(Pid) -> Pid;
                _ -> undefined
            end
    catch
        exit:_ -> undefined
    end.

%% @private
%% Extract the WAL-relevant options from the instance options map. The
%% caller-facing `bondy_oplog_instance:opts()` is a superset; we filter
%% the keys the WAL recognises so the WAL can validate them itself.
wal_opts(InstanceId, Opts) ->
    Base0 = maps:with(
        [
            max_segment_bytes,
            max_batch_bytes,
            retention,
            idx_interval_bytes,
            fsync_mode,
            batched_fsync_interval,
            batched_fsync_bytes,
            min_live_segments,
            retention_sweep_interval,
            max_total_wal_size,
            max_live_segments
        ],
        Opts
    ),
    Origin = maps:get(origin, Opts, bondy_oplog_origin:default()),
    Dir = wal_base_dir(InstanceId, Opts),
    Base0#{dir => Dir, origin => Origin}.

%% @private
%% The WAL stores its segments under `Dir/<InstanceId>` — the writer's
%% `open/2` appends `InstanceId` to the configured base directory.
%% Resolve a writable base from explicit `wal_dir`, falling back to
%% `storage_path`, and finally to a per-id tmp directory when neither
%% is set. The tmp default mirrors what the instance gen_server already
%% does for its MST backend so a caller with no on-disk configuration
%% still gets a working subtree.
wal_base_dir(InstanceId, Opts) ->
    case maps:find(wal_dir, Opts) of
        {ok, D} ->
            D;
        error ->
            case maps:find(storage_path, Opts) of
                {ok, BaseDir} ->
                    Strategy = maps:get(
                        path_strategy, Opts, bondy_oplog_path_sharded
                    ),
                    Base = Strategy:storage_path(InstanceId, BaseDir),
                    filename:join(
                        unicode:characters_to_binary(Base), <<"wal">>
                    );
                error ->
                    %% Default tmp dir is namespaced by OS pid so a
                    %% fresh BEAM run does not inherit segments from
                    %% a prior run sharing the same `InstanceId`. The
                    %% WAL writer then appends `InstanceId` itself, so
                    %% the final directory is
                    %% `/tmp/bondy_oplog_wal/<os_pid>/<InstanceId>`.
                    Pid = list_to_binary(os:getpid()),
                    Tmp = filename:join(
                        ["/tmp", "bondy_oplog_wal", Pid]
                    ),
                    unicode:characters_to_binary(Tmp)
            end
    end.

%% @private
%% The applier needs the on-disk WAL directory. Pid lookup is deferred
%% to the applier's own `init/1` because the WAL writer (started after
%% the instance, before the applier) only publishes `wal_pid` once its
%% own init has returned — by which time the applier is already being
%% started by the supervisor.
applier_opts(InstanceId, Opts) ->
    Base = wal_base_dir(InstanceId, Opts),
    %% The WAL writer's `open/2` appends `InstanceId` to the configured
    %% base directory; the applier needs the same fully-qualified path
    %% to locate the on-disk `consumer.offset`.
    WalDir = iolist_to_binary(filename:join(Base, InstanceId)),
    Applier0 = maps:get(applier, Opts, #{}),
    Applier0#{
        instance_id => InstanceId,
        wal_dir => WalDir
    }.
