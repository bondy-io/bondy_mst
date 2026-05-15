%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_wal_recovery).

-include_lib("kernel/include/logger.hrl").
-include_lib("kernel/include/file.hrl").
-include("bondy_mst.hrl").
-include("bondy_oplog.hrl").
-include("bondy_oplog_wal.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Recovery sequencing for a per-instance WAL directory.

See `_design/WAL_DESIGN.md` §12. Recovery is the procedure the writer
runs on open when a manifest exists. It produces a usable, consistent
in-memory state from on-disk artifacts, applying break-and-truncate to
the head segment if needed and rebuilding lost / stale `.qidx` files.

The recovery contract: **the WAL is the source of truth.** Any frame
that survives recovery is durable; anything beyond the last valid
frame in the head segment is truncated. The applier resumes from a
clamped `committed_frame_offset` that is guaranteed to be a real
frame boundary.

### Steps

1. **Manifest read & validate.** Refuse to open on instance_id mismatch
   or unsupported schema version.
2. **Orphan cleanup.** Remove `.tmp` files; remove `.qdata` / `.qidx`
   for segment ids outside `live_segments`. Log every deletion.
3. **Per sealed segment** (`live_segments ∖ {current_segment}`):
   open `.qdata` and validate the header against `InstanceId` / `Origin`
   (refuse on mismatch). Open `.qidx`; if it is missing or fails to
   parse, rebuild it by walking the segment's frame stream — body-
   decoding only those frames that the writer's accumulator would have
   indexed.
4. **Head segment** (`current_segment`): open RW, validate header,
   scan forward from offset 48 frame-by-frame. The first invalid
   frame (CRC mismatch, bad magic, length out of range, body decode
   failure) marks the end of the durable tail; truncate the file to
   that offset. The accumulator built during the scan becomes the
   writer's `idx_acc`. Position the fd past the last valid frame so
   the writer's next `prim_file:write/2` appends correctly.
5. **Consumer offset.** Read `consumer.offset` (missing = fresh). Clamp:
   the segment must be in `live_segments`; the offset must be ≤ the
   last valid offset of the committed segment; the offset must be at
   a real frame boundary (use the `.qidx` to find the nearest
   preceding entry, then forward-scan).
6. **Return.** The writer installs the recovered state and resumes
   normal operation.
""").

-define(SEG_HEADER_BYTES, ?BONDY_OPLOG_WAL_SEGMENT_HEADER_BYTES).
-define(FRAME_HEADER_BYTES, ?BONDY_OPLOG_WAL_FRAME_HEADER_BYTES).

-type recovery_result() :: #{
    manifest := bondy_oplog_wal_manifest:t(),
    head_fd := file:fd(),
    head_segment_id := non_neg_integer(),
    head_offset := non_neg_integer(),
    first_hlc := bondy_oplog_hlc:hlc() | undefined,
    last_hlc := bondy_oplog_hlc:hlc() | undefined,
    append_count := non_neg_integer(),
    idx_acc := bondy_oplog_wal_idx:accumulator(),
    consumer_offset := bondy_oplog_wal_consumer_offset:t(),
    truncated_bytes := non_neg_integer(),
    cleaned_orphans := [file:filename_all()]
}.

-export_type([recovery_result/0]).

-export([recover/4]).

%% =============================================================================
%% API
%% =============================================================================

?DOC("""
Runs recovery for the WAL directory `Dir` belonging to `InstanceId` /
`Origin`. `IdxIntervalBytes` is the sparse-index emit interval the
writer uses; recovery threads the same value through the head-segment
scan so the rebuilt accumulator matches what a from-scratch writer
would have produced.

Returns `{ok, recovery_result()}` on success or `{error, Reason}` for:

- `{manifest, _}` — manifest is missing, unreadable, or fails validation.
- `{instance_id_mismatch, Expected, Found}` — WAL directory was created
  for a different instance.
- `{orphan_segment, _}` — a sealed segment's header doesn't match this
  instance/origin (e.g., backup restored onto the wrong node).
- `{head_segment, _}` — head segment header is corrupt or unreadable.
- `{consumer_offset, _}` — `consumer.offset` file is malformed.

The caller (typically `bondy_oplog_wal:init/1`) is responsible for
installing the returned state and publishing the head atomics. The
recovery procedure itself does no atomics work.
""").
-spec recover(
    Dir :: file:filename_all(),
    InstanceId :: instance_id(),
    Origin :: bondy_oplog_origin:t(),
    IdxIntervalBytes :: pos_integer()
) -> {ok, recovery_result()} | {error, term()}.

recover(Dir, InstanceId, Origin, IdxIntervalBytes) ->
    case bondy_oplog_wal_manifest:read(Dir) of
        {ok, Manifest} ->
            run_pipeline(Dir, InstanceId, Origin, IdxIntervalBytes, Manifest);
        {error, Reason} ->
            {error, {manifest, Reason}}
    end.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
%% Top-level orchestration. Each step `case`s on the previous step's
%% return so a single failure short-circuits cleanly without leaving
%% partially-recovered state visible.
run_pipeline(Dir, InstanceId, Origin, IdxIntervalBytes, Manifest) ->
    case validate_manifest(Manifest, InstanceId) of
        ok ->
            CleanedOrphans = cleanup_orphans(Dir, Manifest),
            case verify_sealed_segments(Dir, InstanceId, Origin,
                                         IdxIntervalBytes, Manifest) of
                ok ->
                    case recover_head_segment(
                        Dir, InstanceId, Origin, IdxIntervalBytes, Manifest
                    ) of
                        {ok, HeadInfo} ->
                            finalize(Dir, Manifest, HeadInfo, CleanedOrphans);
                        {error, _} = E ->
                            E
                    end;
                {error, _} = E ->
                    E
            end;
        {error, _} = E ->
            E
    end.

%% @private
%% Validates the manifest **before** any destructive operation. Two
%% invariants:
%%
%% 1. `instance_id` matches the caller — refuses orphan WAL directories.
%% 2. `current_segment` is a member of `live_segments`. If this is
%%    violated, the subsequent `cleanup_orphans/2` would delete the
%%    head segment's `.qdata` (because it appears to be orphaned),
%%    leaving recovery in an unrecoverable state. A hand-edited or
%%    crash-corrupted manifest with an empty / inconsistent
%%    `live_segments` must abort recovery *before* anything is
%%    deleted.
validate_manifest(Manifest, InstanceId) ->
    case bondy_oplog_wal_manifest:instance_id(Manifest) of
        InstanceId ->
            validate_current_in_live(Manifest);
        Other ->
            {error, {instance_id_mismatch, InstanceId, Other}}
    end.

%% @private
validate_current_in_live(Manifest) ->
    Current = bondy_oplog_wal_manifest:current_segment(Manifest),
    LiveIds = [
        Id || {Id, _} <- bondy_oplog_wal_manifest:live_segments(Manifest)
    ],
    case lists:member(Current, LiveIds) of
        true ->
            ok;
        false ->
            {error, {manifest, {current_not_in_live, Current, LiveIds}}}
    end.

%% @private
finalize(Dir, Manifest, HeadInfo, CleanedOrphans) ->
    case bondy_oplog_wal_consumer_offset:read(Dir) of
        {ok, CO0} ->
            CO = clamp_consumer_offset(CO0, Manifest, HeadInfo, Dir),
            ok = persist_clamped_offset_if_changed(Dir, CO0, CO),
            {ok, build_result(Manifest, HeadInfo, CO, CleanedOrphans)};
        {error, Reason} ->
            _ = close_head_fd(HeadInfo),
            {error, {consumer_offset, Reason}}
    end.

%% @private
%% Persists the clamped consumer offset back to disk so recovery is
%% idempotent: a second crash-and-recover cycle observes the clamped
%% value directly, not the original (potentially past-EOF) one. We
%% skip the write when the clamp was a no-op so unused WALs don't pay
%% a per-open fsync.
persist_clamped_offset_if_changed(_Dir, Same, Same) ->
    ok;
persist_clamped_offset_if_changed(Dir, _Before, After) ->
    case bondy_oplog_wal_consumer_offset:write(Dir, After) of
        ok ->
            ok;
        {error, Reason} ->
            ?LOG_WARNING(#{
                description =>
                    "Failed to persist clamped consumer offset during "
                    "recovery; in-memory state is correct but next "
                    "recovery will re-clamp from the stale on-disk file",
                reason => Reason
            }),
            ok
    end.

%% @private
close_head_fd(#{head_fd := Fd}) ->
    _ = prim_file:close(Fd),
    ok.

%% @private
build_result(Manifest, HeadInfo, CO, CleanedOrphans) ->
    #{
        manifest => Manifest,
        head_fd => maps:get(head_fd, HeadInfo),
        head_segment_id => maps:get(segment_id, HeadInfo),
        head_offset => maps:get(last_valid_offset, HeadInfo),
        first_hlc => maps:get(first_hlc, HeadInfo),
        last_hlc => maps:get(last_hlc, HeadInfo),
        append_count => maps:get(frame_count, HeadInfo),
        idx_acc => maps:get(idx_acc, HeadInfo),
        consumer_offset => CO,
        truncated_bytes => maps:get(truncated_bytes, HeadInfo),
        cleaned_orphans => CleanedOrphans
    }.

%% -----------------------------------------------------------------------------
%% Orphan cleanup
%% -----------------------------------------------------------------------------

%% @private
%% Walks the WAL directory and removes:
%%
%% - `*.tmp` files (left behind by a crash mid-rename).
%% - `.qdata` files whose segment id is not in `live_segments`. These
%%   are typically the failed-rotation residue: a new segment was
%%   created but `commit_rotation` never landed.
%% - `.qidx` files whose segment id is not in `live_segments`. Same
%%   cause, plus stale indexes from segments since deleted via
%%   retention sweep.
%%
%% Files that are not WAL artifacts (anything that doesn't match the
%% expected naming patterns) are ignored — this is a defensive choice;
%% an operator who left an unrelated file in the WAL directory might
%% be unhappy to find it deleted on every restart.
cleanup_orphans(Dir, Manifest) ->
    LiveIds = [Id || {Id, _} <- bondy_oplog_wal_manifest:live_segments(Manifest)],
    case file:list_dir(Dir) of
        {ok, Names} ->
            lists:filtermap(
                fun(Name) ->
                    maybe_delete(Dir, Name, LiveIds)
                end,
                Names
            );
        {error, Reason} ->
            ?LOG_WARNING(#{
                description =>
                    "Failed to list WAL directory during orphan cleanup; "
                    "continuing without cleanup",
                dir => Dir,
                reason => Reason
            }),
            []
    end.

%% @private
maybe_delete(Dir, Name, LiveIds) ->
    case classify_file(Name, LiveIds) of
        keep ->
            false;
        {drop, Reason} ->
            Path = filename:join(Dir, Name),
            case prim_file:delete(Path) of
                ok ->
                    ?LOG_INFO(#{
                        description => "Removed orphan WAL artifact",
                        path => Path,
                        reason => Reason
                    }),
                    {true, Path};
                {error, DErr} ->
                    ?LOG_WARNING(#{
                        description =>
                            "Failed to remove orphan WAL artifact",
                        path => Path,
                        reason => DErr
                    }),
                    false
            end
    end.

%% @private
classify_file(Name, LiveIds) when is_binary(Name) ->
    classify_file(binary_to_list(Name), LiveIds);
classify_file(Name, LiveIds) ->
    case Name of
        ?BONDY_OPLOG_WAL_MANIFEST_FILENAME -> keep;
        ?BONDY_OPLOG_WAL_MANIFEST_TMP_FILENAME -> {drop, manifest_tmp};
        ?BONDY_OPLOG_WAL_CONSUMER_OFFSET_FILENAME -> keep;
        ?BONDY_OPLOG_WAL_CONSUMER_OFFSET_TMP_FILENAME ->
            {drop, consumer_offset_tmp};
        ?BONDY_OPLOG_WAL_SNAPSHOT_WATERMARK_FILENAME -> keep;
        ?BONDY_OPLOG_WAL_SNAPSHOT_WATERMARK_TMP_FILENAME ->
            {drop, snapshot_watermark_tmp};
        _ ->
            case lists:suffix(".tmp", Name) of
                true -> {drop, generic_tmp};
                false -> classify_data_file(Name, LiveIds)
            end
    end.

%% @private
classify_data_file(Name, LiveIds) ->
    case parse_segment_id(Name, ".qdata") of
        {ok, Id} ->
            classify_by_membership(Id, LiveIds, orphan_qdata);
        not_a_match ->
            case parse_segment_id(Name, ".qidx") of
                {ok, Id} ->
                    classify_by_membership(Id, LiveIds, orphan_qidx);
                not_a_match ->
                    %% Unknown extension — leave alone.
                    keep
            end
    end.

%% @private
classify_by_membership(Id, LiveIds, Reason) ->
    case lists:member(Id, LiveIds) of
        true -> keep;
        false -> {drop, {Reason, Id}}
    end.

%% @private
%% Parse "000000042.qdata" → {ok, 42}; "000000042.qidx" → {ok, 42};
%% anything else → not_a_match.
parse_segment_id(Name, Suffix) when is_list(Name) ->
    case lists:suffix(Suffix, Name) of
        false ->
            not_a_match;
        true ->
            Prefix = lists:sublist(Name, length(Name) - length(Suffix)),
            try list_to_integer(Prefix) of
                Id when Id >= 0 -> {ok, Id};
                _ -> not_a_match
            catch
                _:_ -> not_a_match
            end
    end.

%% -----------------------------------------------------------------------------
%% Sealed segments
%% -----------------------------------------------------------------------------

%% @private
%% Validates and (if necessary) rebuilds the `.qidx` for every sealed
%% segment in the manifest. The head segment is handled separately by
%% `recover_head_segment/5`.
verify_sealed_segments(Dir, InstanceId, Origin, IdxIntervalBytes, Manifest) ->
    Current = bondy_oplog_wal_manifest:current_segment(Manifest),
    Live = bondy_oplog_wal_manifest:live_segments(Manifest),
    Sealed = [{Id, FH} || {Id, FH} <- Live, Id =/= Current],
    verify_sealed_loop(Sealed, Dir, InstanceId, Origin, IdxIntervalBytes).

%% @private
verify_sealed_loop([], _Dir, _InstanceId, _Origin, _Interval) ->
    ok;
verify_sealed_loop([{SegId, _FH} | Rest], Dir, InstanceId, Origin, Interval) ->
    case verify_sealed_segment(Dir, SegId, InstanceId, Origin, Interval) of
        ok ->
            verify_sealed_loop(Rest, Dir, InstanceId, Origin, Interval);
        {error, _} = E ->
            E
    end.

%% @private
verify_sealed_segment(Dir, SegId, InstanceId, Origin, Interval) ->
    SegPath = filename:join(Dir, bondy_oplog_wal_segment:filename(SegId)),
    case bondy_oplog_wal_segment:open(SegPath) of
        {ok, Fd, Header} ->
            Res =
                case bondy_oplog_wal_segment:verify(
                    Header, InstanceId, Origin
                ) of
                    ok ->
                        ensure_sealed_idx(Dir, SegId, Fd, Interval);
                    {error, _} = E ->
                        E
                end,
            _ = prim_file:close(Fd),
            Res;
        {error, Reason} ->
            {error, {sealed_segment, SegId, Reason}}
    end.

%% @private
%% Returns `ok` if the on-disk `.qidx` is loadable. Otherwise rebuilds
%% by scanning the segment's frame stream and writes the new file.
ensure_sealed_idx(Dir, SegId, Fd, Interval) ->
    IdxPath = filename:join(Dir, bondy_oplog_wal_idx:filename(SegId)),
    case bondy_oplog_wal_idx:read_file(IdxPath) of
        {ok, _Entries} ->
            ok;
        {error, Reason} ->
            ?LOG_INFO(#{
                description =>
                    "Sealed segment .qidx unavailable; rebuilding",
                segment => SegId,
                reason => Reason
            }),
            rebuild_sealed_idx(IdxPath, Fd, SegId, Interval)
    end.

%% @private
rebuild_sealed_idx(IdxPath, Fd, SegId, Interval) ->
    case scan_segment_for_index(Fd, Interval) of
        {ok, Acc} ->
            Entries = bondy_oplog_wal_idx:entries(Acc),
            case bondy_oplog_wal_idx:write_file(IdxPath, Entries) of
                ok ->
                    ?LOG_INFO(#{
                        description =>
                            "Sealed segment .qidx rebuilt",
                        segment => SegId,
                        entries => length(Entries)
                    }),
                    ok;
                {error, Reason} ->
                    {error, {idx_rebuild_write, SegId, Reason}}
            end;
        {error, Reason} ->
            {error, {idx_rebuild_scan, SegId, Reason}}
    end.

%% @private
%% Scans the segment from offset 48 to EOF. For each frame the
%% accumulator decides via `would_index/2` whether the body must be
%% decoded; non-indexed frames are skipped header-only (a single pread
%% of the 16-byte frame header per frame). Per design §12 sealed
%% segments are trusted (only their segment header is validated on
%% recovery), so skipping CRC verification for non-indexed frames is
%% consistent with the documented contract.
scan_segment_for_index(Fd, Interval) ->
    Acc0 = bondy_oplog_wal_idx:new(Interval),
    scan_loop_for_index(Fd, ?SEG_HEADER_BYTES, Acc0).

%% @private
scan_loop_for_index(Fd, Off, Acc) ->
    case peek_frame_header(Fd, Off) of
        {ok, FrameLen} ->
            case bondy_oplog_wal_idx:would_index(Acc, FrameLen) of
                true ->
                    case read_and_decode_frame_body(Fd, Off, FrameLen) of
                        {ok, Body} ->
                            case decode_first_hlc(Body) of
                                {ok, Hlc} ->
                                    Acc1 =
                                        bondy_oplog_wal_idx:note_indexed_frame(
                                            Acc, Hlc, Off
                                        ),
                                    scan_loop_for_index(
                                        Fd, Off + FrameLen, Acc1
                                    );
                                {error, _} = E -> E
                            end;
                        {truncate, Reason} ->
                            %% Sealed segment body corruption is a real
                            %% recovery error — surface so the operator
                            %% sees it.
                            {error, {sealed_body, Reason}};
                        {error, _} = E -> E
                    end;
                false ->
                    Acc1 = bondy_oplog_wal_idx:note_skipped_frame(
                        Acc, FrameLen
                    ),
                    scan_loop_for_index(Fd, Off + FrameLen, Acc1)
            end;
        eof ->
            {ok, Acc};
        {truncate, Reason} ->
            {error, {sealed_header, Reason}};
        {error, _} = E ->
            E
    end.

%% -----------------------------------------------------------------------------
%% Head segment
%% -----------------------------------------------------------------------------

%% @private
%% Opens the head segment R/W, validates its header, scans forward
%% break-and-truncate-style, and returns the recovered state needed
%% to install in the writer.
recover_head_segment(Dir, InstanceId, Origin, IdxIntervalBytes, Manifest) ->
    SegId = bondy_oplog_wal_manifest:current_segment(Manifest),
    SegPath = filename:join(Dir, bondy_oplog_wal_segment:filename(SegId)),
    case bondy_oplog_wal_segment:open(SegPath) of
        {ok, Fd, Header} ->
            case bondy_oplog_wal_segment:verify(Header, InstanceId, Origin) of
                ok ->
                    finalize_head(
                        Fd, SegId, IdxIntervalBytes
                    );
                {error, Reason} ->
                    _ = prim_file:close(Fd),
                    {error, {head_segment, SegId, Reason}}
            end;
        {error, Reason} ->
            {error, {head_segment, SegId, Reason}}
    end.

%% @private
finalize_head(Fd, SegId, IdxIntervalBytes) ->
    Acc0 = bondy_oplog_wal_idx:new(IdxIntervalBytes),
    case scan_head_loop(Fd, ?SEG_HEADER_BYTES,
                        undefined, undefined, 0, Acc0) of
        {ok, ScanResult} ->
            #{last_valid_offset := LastValid} = ScanResult,
            case truncate_head_if_needed(Fd, LastValid) of
                {ok, TruncatedBytes} ->
                    {ok, ScanResult#{
                        segment_id => SegId,
                        head_fd => Fd,
                        truncated_bytes => TruncatedBytes
                    }};
                {error, Reason} ->
                    _ = prim_file:close(Fd),
                    {error, {head_segment, SegId, {truncate, Reason}}}
            end;
        {error, Reason} ->
            _ = prim_file:close(Fd),
            {error, {head_segment, SegId, Reason}}
    end.

%% @private
%% Forward scan of the head segment. Every successfully-decoded frame
%% extends `last_valid_offset`. The first failure (CRC, magic, length,
%% body-decode) is the truncation point — we stop and return the state
%% as of the previous frame.
%%
%% The head segment **must** be CRC-verified (a frame with a valid
%% header but a torn body is the most common crash signature); we
%% therefore call `read_and_decode_frame_body/3` rather than skipping
%% to `peek_frame_header`. The first HLC of the batch is extracted
%% from the already-decoded body and fed to the accumulator.
scan_head_loop(Fd, Off, FirstHlc, LastHlc, Count, Acc) ->
    case peek_frame_header(Fd, Off) of
        {ok, FrameLen} ->
            case read_and_decode_frame_body(Fd, Off, FrameLen) of
                {ok, Body} ->
                    case decode_first_hlc(Body) of
                        {ok, Hlc} ->
                            Acc1 = bondy_oplog_wal_idx:note_frame(
                                Acc, Hlc, Off, FrameLen
                            ),
                            scan_head_loop(
                                Fd, Off + FrameLen,
                                pick_first_hlc(FirstHlc, Hlc), Hlc,
                                Count + 1, Acc1
                            );
                        {error, _} ->
                            %% Frame's CRC is valid but its body isn't a
                            %% well-formed batch list (malformed
                            %% `term_to_binary` content on a CRC-clean
                            %% frame). Treat as truncation.
                            {ok, head_scan_result(
                                Off, FirstHlc, LastHlc, Count, Acc
                            )}
                    end;
                {truncate, _Reason} ->
                    %% Frame header looks fine but body CRC failed or
                    %% the file ends mid-body — break-and-truncate.
                    {ok, head_scan_result(
                        Off, FirstHlc, LastHlc, Count, Acc
                    )};
                {error, _} = E ->
                    E
            end;
        eof ->
            {ok, head_scan_result(Off, FirstHlc, LastHlc, Count, Acc)};
        {truncate, _Reason} ->
            %% Bad magic / invalid frame length at the header — break.
            {ok, head_scan_result(Off, FirstHlc, LastHlc, Count, Acc)};
        {error, _} = E ->
            E
    end.

%% @private
head_scan_result(LastValid, FirstHlc, LastHlc, Count, Acc) ->
    #{
        last_valid_offset => LastValid,
        first_hlc => FirstHlc,
        last_hlc => LastHlc,
        frame_count => Count,
        idx_acc => Acc
    }.

%% @private
%% Returns the number of bytes trimmed; 0 if the file was already at
%% `LastValid`. After truncation, the file position is at the new EOF,
%% so subsequent `prim_file:write/2` on the writer's fd lands at the
%% right offset.
truncate_head_if_needed(Fd, LastValid) ->
    case prim_file:position(Fd, eof) of
        {ok, Size} when Size =:= LastValid ->
            %% File already ends at the last valid offset. Seek back
            %% there so subsequent writes append in the right place.
            {ok, _} = prim_file:position(Fd, LastValid),
            {ok, 0};
        {ok, Size} when Size > LastValid ->
            {ok, _} = prim_file:position(Fd, LastValid),
            case prim_file:truncate(Fd) of
                ok ->
                    case bondy_oplog_wal_io:datasync(Fd) of
                        ok -> {ok, Size - LastValid};
                        {error, _} = E -> E
                    end;
                {error, _} = E -> E
            end;
        {ok, Size} when Size < LastValid ->
            %% Shouldn't happen — the scan can't progress past the
            %% physical EOF. Crash loudly if it does.
            {error, {scan_past_eof, Size, LastValid}};
        {error, _} = E -> E
    end.

%% -----------------------------------------------------------------------------
%% Consumer offset clamping
%% -----------------------------------------------------------------------------

%% @private
%% Clamps the consumer offset to a position that is:
%% 1. In a segment that's still in `live_segments`.
%% 2. ≤ `last_valid_offset` of that segment.
%% 3. At a real frame boundary.
%%
%% On any invariant violation, the offset is moved down (never up).
clamp_consumer_offset(CO, Manifest, HeadInfo, Dir) ->
    Seg = bondy_oplog_wal_consumer_offset:committed_segment(CO),
    Off = bondy_oplog_wal_consumer_offset:committed_frame_offset(CO),
    Live = bondy_oplog_wal_manifest:live_segments(Manifest),
    LiveIds = [Id || {Id, _} <- Live],
    case lists:member(Seg, LiveIds) of
        false ->
            %% Committed segment has been swept. Clamp to the start of
            %% the earliest live segment.
            FirstLive = lists:min(LiveIds),
            bondy_oplog_wal_consumer_offset:with_position(
                CO, FirstLive, ?SEG_HEADER_BYTES
            );
        true ->
            clamp_offset_within_segment(CO, Seg, Off, HeadInfo, Dir)
    end.

%% @private
clamp_offset_within_segment(CO, Seg, Off, HeadInfo, Dir) ->
    HeadSeg = maps:get(segment_id, HeadInfo),
    Bound = case Seg of
        HeadSeg ->
            maps:get(last_valid_offset, HeadInfo);
        _ ->
            sealed_segment_size(Dir, Seg)
    end,
    %% Clamp magnitude to ≤ Bound; then clamp to a frame boundary.
    ClampedToBound = min(Off, Bound),
    Aligned = align_to_frame_boundary(
        Dir, Seg, ClampedToBound, HeadInfo
    ),
    bondy_oplog_wal_consumer_offset:with_position(CO, Seg, Aligned).

%% @private
sealed_segment_size(Dir, Seg) ->
    Path = filename:join(Dir, bondy_oplog_wal_segment:filename(Seg)),
    case prim_file:read_file_info(Path) of
        {ok, #file_info{size = Size}} ->
            Size;
        _ ->
            %% Defensive: if we can't size the file we conservatively
            %% return the segment header boundary so the clamp lands at
            %% "nothing committed".
            ?SEG_HEADER_BYTES
    end.

%% @private
%% Returns the largest frame-start offset `≤ Target` within the
%% segment. Uses the `.qidx` (or the head segment's in-memory acc) to
%% find a nearby anchor, then forward-scans to find the exact boundary.
%% Returns `?SEG_HEADER_BYTES` if no anchor / scan reaches `Target`.
%%
%% The `.qidx` is keyed by HLC, but the clamp target is a byte offset.
%% We sweep entries linearly to find the largest entry with
%% `ByteOffset ≤ Target`. The list is small (sub-1k entries), so the
%% linear sweep is fast enough.
align_to_frame_boundary(_Dir, _Seg, Target, _HeadInfo) when
    Target =< ?SEG_HEADER_BYTES
->
    ?SEG_HEADER_BYTES;
align_to_frame_boundary(Dir, Seg, Target, HeadInfo) ->
    HeadSeg = maps:get(segment_id, HeadInfo),
    Entries =
        case Seg of
            HeadSeg ->
                bondy_oplog_wal_idx:entries(
                    maps:get(idx_acc, HeadInfo)
                );
            _ ->
                sealed_idx_entries(Dir, Seg)
        end,
    Anchor = seek_byte_offset(Entries, Target),
    forward_scan_to_boundary(Dir, Seg, Anchor, Target).

%% @private
seek_byte_offset(Entries, Target) ->
    lists:foldl(
        fun
            ({_H, Off}, Best) when Off =< Target, Off > Best -> Off;
            (_, Best) -> Best
        end,
        ?SEG_HEADER_BYTES,
        Entries
    ).

%% @private
sealed_idx_entries(Dir, Seg) ->
    Path = filename:join(Dir, bondy_oplog_wal_idx:filename(Seg)),
    case bondy_oplog_wal_idx:read_file(Path) of
        {ok, Entries} -> Entries;
        {error, _} -> []
    end.

%% @private
%% Walks frames starting at `Anchor` looking for the largest frame-
%% start offset `≤ Target`. Uses header-only peeks; no CRC verification
%% needed (we just want a boundary; the applier will re-CRC on apply).
forward_scan_to_boundary(Dir, Seg, Anchor, Target) ->
    Path = filename:join(Dir, bondy_oplog_wal_segment:filename(Seg)),
    case prim_file:open(Path, [read, raw, binary]) of
        {ok, Fd} ->
            try
                walk_to_boundary(Fd, Anchor, Target, Anchor)
            after
                _ = prim_file:close(Fd)
            end;
        {error, _} ->
            Anchor
    end.

%% @private
walk_to_boundary(Fd, Off, Target, Best) when Off =< Target ->
    case peek_frame_header(Fd, Off) of
        {ok, FrameLen} ->
            Next = Off + FrameLen,
            if
                Next =< Target ->
                    walk_to_boundary(Fd, Next, Target, Next);
                true ->
                    %% The next frame would overshoot Target; current
                    %% frame's start is the largest boundary ≤ Target.
                    Off
            end;
        _ ->
            Best
    end;
walk_to_boundary(_Fd, _Off, _Target, Best) ->
    Best.

%% -----------------------------------------------------------------------------
%% Frame-level read helpers
%% -----------------------------------------------------------------------------

%% @private
%% Reads just the 16-byte frame header at `Off` and returns the frame
%% length. Does **not** CRC-verify the body — that's
%% `read_and_decode_frame_body/3`'s job. Callers that don't need the
%% body (sealed-segment rebuild for non-indexed frames, the consumer-
%% offset clamp walk) save the body pread + decode.
%%
%% Returns:
%%
%% - `{ok, FrameLen}`: header parsed, magic OK, FrameLen ≥ header size.
%% - `eof`: file ends before a full header is available.
%% - `{truncate, Reason}`: header-level integrity failure (bad magic,
%%   length out of range). The head-segment scan treats this as the
%%   truncation point.
%% - `{error, Reason}`: I/O error (surfaced to the caller).
peek_frame_header(Fd, Off) ->
    case prim_file:pread(Fd, Off, ?FRAME_HEADER_BYTES) of
        {ok, HeaderBin} when byte_size(HeaderBin) =:= ?FRAME_HEADER_BYTES ->
            case bondy_oplog_wal_frame:decode_header(HeaderBin) of
                {ok, #{frame_len := FrameLen}} -> {ok, FrameLen};
                {error, Reason} -> {truncate, Reason}
            end;
        {ok, Short} when byte_size(Short) < ?FRAME_HEADER_BYTES ->
            eof;
        eof ->
            eof;
        {error, Reason} ->
            {error, Reason}
    end.

%% @private
%% Reads the full frame at `Off` and CRC-verifies it. Returns:
%%
%% - `{ok, Body}`: frame decoded successfully; `Body` is the inner
%%   bytes (the encoded `[Event_1, ..., Event_N]` list).
%% - `{truncate, Reason}`: CRC mismatch, body short, or body decode
%%   failure. The head-segment scan treats this as the truncation
%%   point; the sealed-segment rebuild surfaces it as corruption.
%% - `{error, Reason}`: I/O error.
read_and_decode_frame_body(Fd, Off, FrameLen) ->
    case prim_file:pread(Fd, Off, FrameLen) of
        {ok, Bin} when byte_size(Bin) =:= FrameLen ->
            case bondy_oplog_wal_frame:decode(Bin) of
                {ok, Body, _Meta} -> {ok, Body};
                {error, Reason} -> {truncate, Reason}
            end;
        {ok, _Short} ->
            {truncate, truncated_body};
        eof ->
            {truncate, truncated_body};
        {error, Reason} ->
            {error, Reason}
    end.

%% @private
%% Decodes the first event's HLC out of an already-CRC-verified body.
%% `[safe]` blocks atom-table-exhaustion attacks via crafted terms.
decode_first_hlc(Body) ->
    try binary_to_term(Body, [safe]) of
        [Event | _] ->
            try
                {ok, bondy_oplog_event:key_hlc(
                    bondy_oplog_event:key(Event)
                )}
            catch
                _:R -> {error, {bad_event, R}}
            end;
        [] ->
            {error, empty_batch};
        Other ->
            {error, {not_a_batch_list, Other}}
    catch
        error:badarg -> {error, badarg}
    end.

%% @private
pick_first_hlc(undefined, Hlc) -> Hlc;
pick_first_hlc(Existing, _) -> Existing.
