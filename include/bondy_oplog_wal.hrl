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

-endif.
