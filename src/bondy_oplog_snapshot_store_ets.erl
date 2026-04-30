%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_snapshot_store_ets).
-behaviour(bondy_oplog_snapshot_store).

-include("bondy_mst.hrl").
-include("bondy_oplog.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
In-memory snapshot store backed by a per-instance ETS table.

Default implementation. Suitable for tests and ephemeral instances.
For durable snapshots, consumers should plug a different module that
implements the `bondy_oplog_snapshot_store` behaviour
(DETS, leveled, RocksDB, S3, etc.).
""").

-record(state, {
    instance_id :: instance_id(),
    tab :: ets:tid()
}).

-export([init/2]).
-export([put_snapshot/3]).
-export([get_snapshot/1]).
-export([current_watermark/1]).
-export([close/1]).

%% =============================================================================
%% bondy_oplog_snapshot_store CALLBACKS
%% =============================================================================

init(InstanceId, _Opts) when is_binary(InstanceId) ->
    Tab = ets:new(undefined, [
        set, public, {read_concurrency, true}
    ]),
    {ok, #state{instance_id = InstanceId, tab = Tab}}.

put_snapshot(#state{tab = Tab}, Watermark, Snapshot) ->
    %% Single-row policy: overwrite any prior snapshot.
    true = ets:insert(Tab, {snapshot, Watermark, Snapshot}),
    ok.

get_snapshot(#state{tab = Tab}) ->
    case ets:lookup(Tab, snapshot) of
        [{snapshot, W, S}] -> {ok, W, S};
        [] -> not_found
    end.

current_watermark(#state{tab = Tab}) ->
    case ets:lookup(Tab, snapshot) of
        [{snapshot, W, _}] -> W;
        [] -> undefined
    end.

close(#state{tab = Tab}) ->
    _ = catch ets:delete(Tab),
    ok.
