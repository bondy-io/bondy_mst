%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_snapshot_store_file).
-behaviour(bondy_oplog_snapshot_store).

-include("bondy_mst.hrl").
-include("bondy_oplog.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
File-backed snapshot store. Durable, single-snapshot, atomic.

One file per instance, holding the latest `{Watermark, Snapshot}` as
an Erlang External Term Format binary. Writes use the standard
write-tmp + atomic-rename idiom: a partial write produced by a VM
crash leaves the previous good file in place; readers see either the
old snapshot or the new one, never a partial mix.

## Opts

| Key    | Required | Meaning |
|---|---|---|
| `path` | yes      | Base directory. The instance's snapshot is stored at `<path>/<InstanceId>/snapshot.etf`. |

## Why not DETS

The library used to ship a DETS-backed implementation. DETS earned
its keep on none of the dimensions that matter for a single-row
snapshot store: it has no real transactional guarantees, requires
atom-named tables (atom-table footgun for many instances), and runs
a slow repair pass on dirty restart. `file:rename/2` is atomic on
POSIX by specification — strictly better than DETS for our use case.
""").

-record(state, {
    instance_id :: instance_id(),
    path :: file:filename_all()
}).

-export([init/2]).
-export([put_snapshot/3]).
-export([get_snapshot/1]).
-export([current_watermark/1]).
-export([close/1]).

%% =============================================================================
%% bondy_oplog_snapshot_store CALLBACKS
%% =============================================================================

init(InstanceId, Opts) when is_binary(InstanceId), is_map(Opts) ->
    case maps:find(path, Opts) of
        error ->
            {error, {missing_option, path}};
        {ok, BaseDir} ->
            Dir = filename:join(BaseDir, InstanceId),
            File = filename:join(Dir, "snapshot.etf"),
            ok = filelib:ensure_dir(File),
            {ok, #state{instance_id = InstanceId, path = File}}
    end.

put_snapshot(#state{path = Path}, Watermark, Snapshot) ->
    Bin = erlang:term_to_binary(
        {snapshot_v1, Watermark, Snapshot},
        [{minor_version, 2}]
    ),
    Tmp = tmp_path(Path),
    case file:write_file(Tmp, Bin, [raw]) of
        ok ->
            %% POSIX atomic rename: readers see either the old file or
            %% the new one, never a partial write.
            file:rename(Tmp, Path);
        {error, _} = E ->
            E
    end.

get_snapshot(#state{path = Path}) ->
    case file:read_file(Path) of
        {ok, Bin} ->
            {snapshot_v1, W, S} = erlang:binary_to_term(Bin),
            {ok, W, S};
        {error, enoent} ->
            not_found
    end.

current_watermark(#state{path = Path}) ->
    case file:read_file(Path) of
        {ok, Bin} ->
            {snapshot_v1, W, _} = erlang:binary_to_term(Bin),
            W;
        {error, enoent} ->
            undefined
    end.

close(#state{}) ->
    ok.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
tmp_path(Path) when is_list(Path) ->
    Path ++ ".tmp";
tmp_path(Path) when is_binary(Path) ->
    <<Path/binary, ".tmp">>.
