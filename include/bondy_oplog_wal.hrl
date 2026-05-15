%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% Constants and record definitions shared by the bondy_oplog_wal modules.
%% See `_design/WAL_DESIGN.md`.
%% -----------------------------------------------------------------------------

-ifndef(BONDY_OPLOG_WAL_HRL).
-define(BONDY_OPLOG_WAL_HRL, true).

%% -----------------------------------------------------------------------------
%% Frame format (§3)
%% -----------------------------------------------------------------------------

%% "BDOP" in ASCII — Bondy OPlog frame magic.
-define(BONDY_OPLOG_WAL_FRAME_MAGIC, 16#42444F50).

%% Header bytes for a frame: Magic(4) + FrameLen(4) + CRC32(4) +
%% FrameVersion(1) + Flags(3) = 16.
-define(BONDY_OPLOG_WAL_FRAME_HEADER_BYTES, 16).

-define(BONDY_OPLOG_WAL_FRAME_VERSION, 1).

-define(BONDY_OPLOG_WAL_FRAME_FLAG_COMPRESSED, 16#000001).
-define(BONDY_OPLOG_WAL_FRAME_FLAG_ENCRYPTED, 16#000002).

%% Bitmask of flag bits the v1 reader understands. Bits outside this
%% mask are rejected (encode-side `badarg`, decode-side `unknown_flag`).
%% v1 implements neither compression nor encryption, so the mask is
%% zero; future versions widen it as they land support.
-define(BONDY_OPLOG_WAL_FRAME_KNOWN_FLAGS_V1, 16#000000).

%% -----------------------------------------------------------------------------
%% Segment format (§4)
%% -----------------------------------------------------------------------------

%% "BDSG" in ASCII — segment header magic.
-define(BONDY_OPLOG_WAL_SEGMENT_MAGIC, 16#42445347).

%% Segment header: Magic(4) + Version(1) + Flags(3) + SegmentId(8) +
%% InstanceIdHash(8) + CreatedAt(8) + Origin(16) = 48.
-define(BONDY_OPLOG_WAL_SEGMENT_HEADER_BYTES, 48).

-define(BONDY_OPLOG_WAL_SEGMENT_VERSION, 1).

-define(BONDY_OPLOG_WAL_INSTANCE_ID_HASH_BYTES, 8).

%% -----------------------------------------------------------------------------
%% Manifest (§5)
%% -----------------------------------------------------------------------------

-define(BONDY_OPLOG_WAL_MANIFEST_VERSION, 1).
-define(BONDY_OPLOG_WAL_MANIFEST_FILENAME, "manifest").
-define(BONDY_OPLOG_WAL_MANIFEST_TMP_FILENAME, "manifest.tmp").

%% -----------------------------------------------------------------------------
%% Consumer offset (§6)
%% -----------------------------------------------------------------------------

-define(BONDY_OPLOG_WAL_CONSUMER_OFFSET_FILENAME, "consumer.offset").
-define(BONDY_OPLOG_WAL_CONSUMER_OFFSET_TMP_FILENAME, "consumer.offset.tmp").
-define(BONDY_OPLOG_WAL_CONSUMER_OFFSET_VERSION, 1).

%% -----------------------------------------------------------------------------
%% Sparse index `.qidx` (§7)
%% -----------------------------------------------------------------------------

%% "BDIX" in ASCII — sparse index file magic.
-define(BONDY_OPLOG_WAL_IDX_MAGIC, 16#42444958).

%% Index header: Magic(4) + Version(1) + Flags(3) + EntryCount(4) +
%% Reserved(4) = 16.
-define(BONDY_OPLOG_WAL_IDX_HEADER_BYTES, 16).

%% Each entry: HLC(8) + ByteOffset(8) = 16.
-define(BONDY_OPLOG_WAL_IDX_ENTRY_BYTES, 16).

-define(BONDY_OPLOG_WAL_IDX_VERSION, 1).

%% Default index interval in bytes — the writer emits one index entry per
%% ~64 KB of frames written. See `_design/WAL_DESIGN.md` §7.
-define(BONDY_OPLOG_WAL_IDX_DEFAULT_INTERVAL_BYTES, (64 * 1024)).

%% -----------------------------------------------------------------------------
%% Fsync modes + batched-mode defaults (§8.1)
%% -----------------------------------------------------------------------------

%% Default fsync mode for instances that do not specify one. Per-write is
%% the conservative choice — security-class namespaces rely on it
%% (`grants`, `tickets`, `users`). High-churn namespaces (`registry`)
%% override to `batched` in their per-instance config.
-define(BONDY_OPLOG_WAL_FSYNC_MODE_DEFAULT, per_write).

%% In `batched` mode the writer fsyncs at most every
%% `batched_fsync_interval` ms. 50 ms gives a 20 Hz fsync cadence which
%% is fast enough that `await_durable/3` callers rarely block more than
%% one tick, and slow enough that 1000-event bursts amortise to ~50
%% fsyncs.
-define(BONDY_OPLOG_WAL_BATCHED_FSYNC_INTERVAL_DEFAULT_MS, 50).

%% Size-trigger: fsync if `pending_fsync` bytes exceed this threshold,
%% even before the interval elapses. 1 MB is the WAL_DESIGN default — it
%% bounds tail-of-log loss on crash to ~1 MB of buffered writes per
%% writer.
-define(BONDY_OPLOG_WAL_BATCHED_FSYNC_BYTES_DEFAULT, (1 * 1024 * 1024)).

%% -----------------------------------------------------------------------------
%% Atomic batches (§3, §8.3 — Q9)
%% -----------------------------------------------------------------------------

%% Hard upper bound on the encoded body of a single atomic batch frame.
%% 4 MiB is large enough to hold tens of thousands of small events in one
%% atomic write, while keeping a single frame well below the default
%% `max_segment_bytes` (64 MiB) so pre-rotation always has room.
-define(BONDY_OPLOG_WAL_MAX_BATCH_BYTES_DEFAULT, (4 * 1024 * 1024)).

%% -----------------------------------------------------------------------------
%% Retention + snapshot watermark (§10)
%% -----------------------------------------------------------------------------

-define(BONDY_OPLOG_WAL_SNAPSHOT_WATERMARK_FILENAME, "snapshot.watermark").
-define(BONDY_OPLOG_WAL_SNAPSHOT_WATERMARK_TMP_FILENAME,
        "snapshot.watermark.tmp").
-define(BONDY_OPLOG_WAL_SNAPSHOT_WATERMARK_VERSION, 1).

%% Minimum number of live segments to keep after a retention sweep, even
%% if all segments are otherwise eligible for deletion. Provides a
%% recent-history safety net for ad-hoc inspection / replay.
-define(BONDY_OPLOG_WAL_MIN_LIVE_SEGMENTS_DEFAULT, 2).

%% Default cadence of the periodic retention sweep (ms). 5 minutes per
%% WAL_DESIGN §10.4 — a safety net behind the event-driven triggers
%% (applier commit advance, watermark advance).
-define(BONDY_OPLOG_WAL_RETENTION_SWEEP_INTERVAL_DEFAULT_MS, (5 * 60 * 1000)).

%% -----------------------------------------------------------------------------
%% Backpressure (§14, §15)
%% -----------------------------------------------------------------------------

%% Hard cap on the sum of `.qdata` sizes across all live segments. Once
%% crossed, `append`/`append_batch` return `{error, wal_full}` until
%% retention frees space. 8 GiB matches the WAL_DESIGN §14 default.
-define(BONDY_OPLOG_WAL_MAX_TOTAL_WAL_SIZE_DEFAULT, (8 * 1024 * 1024 * 1024)).

%% Hard cap on `length(live_segments)`. Once reached, the writer refuses
%% the rotation that would create segment N+1 — the in-flight append is
%% rejected with `{error, wal_full}`. 256 matches WAL_DESIGN §14.
-define(BONDY_OPLOG_WAL_MAX_LIVE_SEGMENTS_DEFAULT, 256).

%% Minimum interval between `wal_full` telemetry events (ms). A backpressured
%% client typically retries on a tight loop; without debouncing the WAL
%% would emit one event per retry. 30 s matches the WAL_DESIGN §15
%% recommendation.
-define(BONDY_OPLOG_WAL_WAL_FULL_TELEMETRY_DEBOUNCE_MS, (30 * 1000)).

-endif.
