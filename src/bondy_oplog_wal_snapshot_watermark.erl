%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_wal_snapshot_watermark).

-include("bondy_mst.hrl").
-include("bondy_oplog.hrl").
-include("bondy_oplog_wal.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
On-disk snapshot watermark for a per-instance WAL.

The watermark is the highest HLC that has been covered by a compaction
snapshot. It bounds retention: a segment is only eligible for deletion
once **all** of its events are HLC-covered by the watermark (see
`_design/WAL_DESIGN.md` §10).

File format is a single-term, `file:consult/1`-readable Erlang file
(matching the manifest's style):

```erlang
{snapshot_watermark_version, 1}.
{hlc, 17155200001230000}.
```

Writes follow the tmp-then-rename pattern (see the manifest's
docstring for the four-step sequence). An interrupted rename leaves
either the old or the new watermark, never a partial mix.

The watermark is the slowest-evolving piece of WAL state — a few
writes per minute at most — so the per-rewrite fsync cost is
negligible.
""").

-export([read/1]).
-export([write/2]).

%% =============================================================================
%% API
%% =============================================================================

?DOC("""
Reads the snapshot watermark from `Dir`.

Returns:
- `{ok, Hlc}` — the persisted watermark.
- `{ok, undefined}` — no watermark file exists yet (fresh WAL).
- `{error, Reason}` — the file exists but cannot be parsed (wrong
  version, missing field, etc.).
""").
-spec read(Dir :: file:filename_all()) ->
    {ok, bondy_oplog_hlc:hlc() | undefined} | {error, term()}.

read(Dir) ->
    Path = filename:join(
        Dir, ?BONDY_OPLOG_WAL_SNAPSHOT_WATERMARK_FILENAME
    ),
    case filelib:is_regular(Path) of
        false ->
            {ok, undefined};
        true ->
            case file:consult(Path) of
                {ok, Terms} -> parse_terms(Terms);
                {error, _} = E -> E
            end
    end.

?DOC("""
Atomically writes `Hlc` as the new watermark.

Uses the same tmp+datasync+rename+dir-fsync sequence as the manifest.
Errors at any step short-circuit and leave the prior on-disk
watermark intact.
""").
-spec write(Dir :: file:filename_all(), bondy_oplog_hlc:hlc()) ->
    ok | {error, term()}.

write(Dir, Hlc) when is_integer(Hlc), Hlc >= 0 ->
    TmpPath = filename:join(
        Dir, ?BONDY_OPLOG_WAL_SNAPSHOT_WATERMARK_TMP_FILENAME
    ),
    FinalPath = filename:join(
        Dir, ?BONDY_OPLOG_WAL_SNAPSHOT_WATERMARK_FILENAME
    ),
    Bin = format(Hlc),
    case write_and_sync(TmpPath, Bin) of
        ok ->
            case bondy_oplog_wal_io:rename(TmpPath, FinalPath) of
                ok ->
                    bondy_oplog_wal_io:fsync_dir(Dir);
                {error, _} = E ->
                    _ = prim_file:delete(TmpPath),
                    E
            end;
        {error, _} = E ->
            _ = prim_file:delete(TmpPath),
            E
    end.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
parse_terms(Terms) ->
    Map = lists:foldl(
        fun
            ({K, V}, Acc) -> Acc#{K => V};
            (_, Acc) -> Acc
        end,
        #{},
        Terms
    ),
    try
        Version = required(snapshot_watermark_version, Map),
        validate_version(Version),
        Hlc = required(hlc, Map),
        validate_hlc(Hlc),
        {ok, Hlc}
    catch
        throw:{missing_field, F} -> {error, {missing_field, F}};
        throw:{invalid, R} -> {error, R}
    end.

%% @private
required(K, M) ->
    case maps:find(K, M) of
        {ok, V} -> V;
        error -> throw({missing_field, K})
    end.

%% @private
validate_version(?BONDY_OPLOG_WAL_SNAPSHOT_WATERMARK_VERSION) -> ok;
validate_version(V) ->
    throw({invalid, {unsupported_snapshot_watermark_version, V}}).

%% @private
validate_hlc(H) when is_integer(H), H >= 0 -> ok;
validate_hlc(V) ->
    throw({invalid, {invalid_hlc, V}}).

%% @private
format(Hlc) ->
    iolist_to_binary([
        io_lib:format("~tw.~n", [{
            snapshot_watermark_version,
            ?BONDY_OPLOG_WAL_SNAPSHOT_WATERMARK_VERSION
        }]),
        io_lib:format("~tw.~n", [{hlc, Hlc}])
    ]).

%% @private
write_and_sync(TmpPath, Bin) ->
    case prim_file:open(TmpPath, [write, raw, binary]) of
        {ok, Fd} ->
            Res =
                case prim_file:write(Fd, Bin) of
                    ok ->
                        case bondy_oplog_wal_io:datasync(Fd) of
                            ok -> ok;
                            {error, _} = E1 -> E1
                        end;
                    {error, _} = E2 ->
                        E2
                end,
            ok = prim_file:close(Fd),
            Res;
        {error, _} = E ->
            E
    end.
