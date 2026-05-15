%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_wal_consumer_offset).

-include("bondy_mst.hrl").
-include("bondy_oplog.hrl").
-include("bondy_oplog_wal.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Per-instance consumer offset (`consumer.offset`).

See `_design/WAL_DESIGN.md` §6. The applier writes this file to commit
the position up to which events have been durably applied to the page
store. The WAL reads it on recovery to resume the applier from a
known-good frame boundary.

Format is a sequence of `file:consult/1`-readable Erlang terms, one
per line, mirroring the manifest pattern for debuggability:

```erlang
{committed_segment, 42}.
{committed_frame_offset, 1048576}.
{committed_hlc, 1715521234567890}.
{commit_count, 1234567}.
{schema_version, 1}.
```

Writes follow the tmp-then-rename pattern (§6.1):

1. Write `consumer.offset.tmp` with the new content.
2. `datasync` the temp file.
3. `rename(consumer.offset.tmp, consumer.offset)` — atomic on POSIX.
4. `datasync` the enclosing directory — required on ext4/xfs.

A missing `consumer.offset` is **not** an error — it means nothing has
ever been committed. `read/1` returns `{ok, new()}` in that case so
the writer's recovery treats a fresh WAL identically to a never-
committed-against WAL.
""").

-record(?MODULE, {
    %% Initially 0 for a fresh WAL; clamped to the first live segment on
    %% recovery if the previously committed segment has been swept.
    committed_segment :: non_neg_integer(),
    %% Byte offset of the START of the next frame to apply. Always a
    %% frame boundary — the applier never commits mid-frame. On
    %% recovery, clamped to the largest frame-start offset ≤ the file
    %% value, with `≤ last_valid_offset_of(committed_segment)` enforced.
    committed_frame_offset :: non_neg_integer(),
    %% HLC of the last applied event. `undefined` for a never-committed
    %% WAL.
    committed_hlc :: bondy_oplog_hlc:hlc() | undefined,
    %% Monotonic counter incremented on every commit. Diagnostic only.
    commit_count :: non_neg_integer(),
    schema_version = ?BONDY_OPLOG_WAL_CONSUMER_OFFSET_VERSION :: pos_integer()
}).

-type t() :: #?MODULE{}.

-export_type([t/0]).

-export([new/0]).
-export([read/1]).
-export([write/2]).
-export([committed_segment/1]).
-export([committed_frame_offset/1]).
-export([committed_hlc/1]).
-export([commit_count/1]).
-export([with_position/3]).
-export([with_hlc/2]).
-export([with_commit_count/2]).

-define(SEG_HEADER_BYTES, ?BONDY_OPLOG_WAL_SEGMENT_HEADER_BYTES).

%% =============================================================================
%% API
%% =============================================================================

?DOC("""
Returns a fresh consumer offset: segment 0, offset at the segment
header boundary, no HLC, count zero. This is the "nothing committed
yet" state and is what `read/1` returns for a missing file.
""").
-spec new() -> t().

new() ->
    #?MODULE{
        committed_segment = 0,
        committed_frame_offset = ?SEG_HEADER_BYTES,
        committed_hlc = undefined,
        commit_count = 0
    }.

?DOC("""
Reads and parses `consumer.offset` from `Dir`.

Returns:
- `{ok, t()}` on success.
- `{ok, new()}` when the file is missing — a fresh / never-committed
  WAL is indistinguishable from one whose applier has never run.
- `{error, Reason}` for malformed content / unsupported version /
  missing required field.
""").
-spec read(file:filename_all()) -> {ok, t()} | {error, term()}.

read(Dir) ->
    Path = filename:join(Dir, ?BONDY_OPLOG_WAL_CONSUMER_OFFSET_FILENAME),
    case file:consult(Path) of
        {ok, Terms} ->
            parse_terms(Terms);
        {error, enoent} ->
            {ok, new()};
        {error, _} = E ->
            E
    end.

?DOC("""
Atomically writes `t()` to `Dir`. Mirrors the manifest's four-step
durability sequence (write tmp → datasync → rename → fsync dir).
""").
-spec write(file:filename_all(), t()) -> ok | {error, term()}.

write(Dir, #?MODULE{} = CO) ->
    TmpPath = filename:join(
        Dir, ?BONDY_OPLOG_WAL_CONSUMER_OFFSET_TMP_FILENAME
    ),
    FinalPath = filename:join(
        Dir, ?BONDY_OPLOG_WAL_CONSUMER_OFFSET_FILENAME
    ),
    Bin = format(CO),
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

?DOC("Returns the committed segment id.").
-spec committed_segment(t()) -> non_neg_integer().
committed_segment(#?MODULE{committed_segment = S}) -> S.

?DOC("Returns the committed frame-start byte offset within the segment.").
-spec committed_frame_offset(t()) -> non_neg_integer().
committed_frame_offset(#?MODULE{committed_frame_offset = O}) -> O.

?DOC("Returns the committed HLC, or `undefined` if nothing was ever committed.").
-spec committed_hlc(t()) -> bondy_oplog_hlc:hlc() | undefined.
committed_hlc(#?MODULE{committed_hlc = H}) -> H.

?DOC("Returns the monotonic commit count.").
-spec commit_count(t()) -> non_neg_integer().
commit_count(#?MODULE{commit_count = N}) -> N.

?DOC("""
Replaces the `committed_segment` and `committed_frame_offset` fields.
""").
-spec with_position(t(), non_neg_integer(), non_neg_integer()) -> t().
with_position(#?MODULE{} = CO, Seg, Off) when
    is_integer(Seg), Seg >= 0,
    is_integer(Off), Off >= ?SEG_HEADER_BYTES
->
    CO#?MODULE{committed_segment = Seg, committed_frame_offset = Off}.

?DOC("Replaces the `committed_hlc` field.").
-spec with_hlc(t(), bondy_oplog_hlc:hlc() | undefined) -> t().
with_hlc(#?MODULE{} = CO, Hlc) when is_integer(Hlc), Hlc >= 0 ->
    CO#?MODULE{committed_hlc = Hlc};
with_hlc(#?MODULE{} = CO, undefined) ->
    CO#?MODULE{committed_hlc = undefined}.

?DOC("Replaces the `commit_count` field.").
-spec with_commit_count(t(), non_neg_integer()) -> t().
with_commit_count(#?MODULE{} = CO, N) when is_integer(N), N >= 0 ->
    CO#?MODULE{commit_count = N}.

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
        Seg = required(committed_segment, Map),
        validate_non_neg_integer(committed_segment, Seg),
        Off = required(committed_frame_offset, Map),
        validate_non_neg_integer(committed_frame_offset, Off),
        Hlc = maps:get(committed_hlc, Map, undefined),
        validate_hlc_or_undefined(Hlc),
        Count = maps:get(commit_count, Map, 0),
        validate_non_neg_integer(commit_count, Count),
        Version = maps:get(
            schema_version, Map, ?BONDY_OPLOG_WAL_CONSUMER_OFFSET_VERSION
        ),
        validate_schema_version(Version),
        {ok, #?MODULE{
            committed_segment = Seg,
            committed_frame_offset = Off,
            committed_hlc = Hlc,
            commit_count = Count,
            schema_version = Version
        }}
    catch
        throw:{missing_field, F} ->
            {error, {missing_field, F}};
        throw:{invalid, R} ->
            {error, R}
    end.

%% @private
required(K, M) ->
    case maps:find(K, M) of
        {ok, V} -> V;
        error -> throw({missing_field, K})
    end.

%% @private
validate_non_neg_integer(_K, V) when is_integer(V), V >= 0 -> ok;
validate_non_neg_integer(K, V) -> throw({invalid, {invalid_field, K, V}}).

%% @private
validate_hlc_or_undefined(undefined) -> ok;
validate_hlc_or_undefined(V) when is_integer(V), V >= 0 -> ok;
validate_hlc_or_undefined(V) ->
    throw({invalid, {invalid_field, committed_hlc, V}}).

%% @private
validate_schema_version(?BONDY_OPLOG_WAL_CONSUMER_OFFSET_VERSION) -> ok;
validate_schema_version(V) ->
    throw({invalid, {unsupported_schema_version, V}}).

%% @private
format(#?MODULE{
    committed_segment = Seg,
    committed_frame_offset = Off,
    committed_hlc = Hlc,
    commit_count = Count,
    schema_version = Version
}) ->
    iolist_to_binary([
        format_term({committed_segment, Seg}),
        format_term({committed_frame_offset, Off}),
        format_term({committed_hlc, Hlc}),
        format_term({commit_count, Count}),
        format_term({schema_version, Version})
    ]).

%% @private
format_term(T) ->
    io_lib:format("~tw.~n", [T]).

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
                    {error, _} = E2 -> E2
                end,
            ok = prim_file:close(Fd),
            Res;
        {error, _} = E ->
            E
    end.
