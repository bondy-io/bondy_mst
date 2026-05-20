%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% Shared definitions for the MST page-store packfile backend
%% (`bondy_mst_pack_*` modules). See `_design/latest/MST_PAGE_STORE_DESIGN.md`
%% §3 (pack file format) and §4 (index file format).
%%
%% Contents:
%%
%% - Wire-format constants — stable on-disk values; any change is a
%%   format break and must bump the corresponding `*_VERSION` macro.
%% - Shared in-memory records used by more than one module in the
%%   `bondy_mst_pack_*` family.

%% -----------------------------------------------------------------------------
%% Pack file (`*.pack`)
%% -----------------------------------------------------------------------------

%% "BDPG" — Bondy paGe.
-define(BONDY_MST_PACK_MAGIC,           16#42445047).
-define(BONDY_MST_PACK_VERSION,         1).
-define(BONDY_MST_PACK_HEADER_BYTES,    48).
-define(BONDY_MST_PACK_RECORD_HEADER_BYTES, 40).
-define(BONDY_MST_PACK_HASH_BYTES,      32).
-define(BONDY_MST_PACK_TRAILER_BYTES,   32).

%% Hash algorithm ids carried in the pack header (§3.1 byte 20).
-define(BONDY_MST_PACK_HASH_ALGO_SHA256, 1).

%% Pack header flag bits (§3.1 bytes 5..7 — currently reserved).
%% Reserved for future use; all bits must be 0 in v1.
-define(BONDY_MST_PACK_FLAGS_RESERVED_MASK, 16#FFFFFF).

%% -----------------------------------------------------------------------------
%% Index file (`*.idx`)
%% -----------------------------------------------------------------------------

%% "BDIN" — bonDy INdex.
-define(BONDY_MST_PACK_IDX_MAGIC,         16#4244494E).
-define(BONDY_MST_PACK_IDX_VERSION,       1).
-define(BONDY_MST_PACK_IDX_HEADER_BYTES,  16).
-define(BONDY_MST_PACK_IDX_FANOUT_BYTES,  1024).
-define(BONDY_MST_PACK_IDX_FANOUT_ENTRIES, 256).
-define(BONDY_MST_PACK_IDX_OFFSET_BYTES,  8).

%% Trailing sha256 over the rest of the file. Symmetric to
%% `BONDY_MST_PACK_TRAILER_BYTES`; placed at the absolute end so
%% pre-trailer readers (which size sections off the header) ignore
%% it and keep working.
-define(BONDY_MST_PACK_IDX_TRAILER_BYTES, 32).

%% Index header flag bits (byte 5, §4.1).
%% Bit 0 — bloom section present after the header.
-define(BONDY_MST_PACK_IDX_FLAG_BLOOM,    1).

%% -----------------------------------------------------------------------------
%% Bloom section (§4 extension; see `bondy_mst_pack_index` docstring)
%% -----------------------------------------------------------------------------

-define(BONDY_MST_PACK_BLOOM_HEADER_BYTES, 16).
-define(BONDY_MST_PACK_BLOOM_DEFAULT_P,    0.01).

%% -----------------------------------------------------------------------------
%% Manifest (`manifest`)
%% -----------------------------------------------------------------------------

-define(BONDY_MST_PACK_MANIFEST_VERSION,  1).
-define(BONDY_MST_PACK_MANIFEST_FILENAME, "manifest").
-define(BONDY_MST_PACK_MANIFEST_TMP_FILENAME, "manifest.tmp").

%% -----------------------------------------------------------------------------
%% Per-instance directory layout filenames (§2 of MST_PAGE_STORE_DESIGN.md)
%% -----------------------------------------------------------------------------

-define(BONDY_MST_PACK_INCOMING_PACK_FILENAME, "incoming.pack").
-define(BONDY_MST_PACK_INCOMING_IDX_FILENAME,  "incoming.idx").
-define(BONDY_MST_PACK_ROOT_FILENAME,          "root").

%% -----------------------------------------------------------------------------
%% Tombstones file (`tombstones`)
%% -----------------------------------------------------------------------------

%% "BDTS" — bonDy TombStones.
-define(BONDY_MST_PACK_TOMBSTONES_MAGIC,         16#42445453).
-define(BONDY_MST_PACK_TOMBSTONES_VERSION,       1).
-define(BONDY_MST_PACK_TOMBSTONES_HEADER_BYTES,  16).
-define(BONDY_MST_PACK_TOMBSTONES_TRAILER_BYTES, 32).
-define(BONDY_MST_PACK_TOMBSTONES_FILENAME,      "tombstones").
-define(BONDY_MST_PACK_TOMBSTONES_TMP_FILENAME,  "tombstones.tmp").

%% -----------------------------------------------------------------------------
%% Production defaults for open-time options
%% -----------------------------------------------------------------------------
%%
%% Centralised so the values are visible at one read. Each is the
%% out-of-the-box default applied when `bondy_mst_pack_store:open/2`
%% is called without the corresponding key. Tests and benches that
%% want a specific policy pass the key explicitly.
%%
%% `sync_every_records` — datasync the incoming-pack fd after this
%% many appends. The pack store sits beneath a WAL that is the
%% authoritative source of truth (`STORAGE_ARCHITECTURE.md` §4),
%% so a crash with unsynced pack-store writes is recoverable by
%% the applier replaying the WAL — per-record fsync would be
%% over-conservative. 32 trades a modest visibility lag (bounded
%% by `sync_every_ms` below) for ~32× fewer fsync syscalls at
%% steady throughput.
-define(BONDY_MST_PACK_DEFAULT_SYNC_EVERY_RECORDS, 32).
%%
%% `sync_every_ms` — opportunistic wall-clock floor. Datasync any
%% pending records that have been unsynced for this many ms when
%% the next append happens. Caps visibility lag at low throughput
%% so a quiet incoming pack doesn't sit unflushed indefinitely.
%% The timer is opportunistic (only checked on append); callers
%% needing strict wall-clock semantics drive `flush/1`.
-define(BONDY_MST_PACK_DEFAULT_SYNC_EVERY_MS, 200).
%%
%% `auto_seal_records` / `auto_seal_bytes` — bound the incoming
%% pack so the resume-scan cost on reopen is bounded by the
%% threshold, not by the lifetime put volume (design §3.3).
%% Defaults sized to keep the resume scan under ~100 ms on a
%% cold-cache SSD:
%%   - records: 10 000 → ~10 000 preads.
%%   - bytes:   16 MB  → matches design §3.3's `max_incoming_size`.
%% Whichever fires first triggers the seal; either can be
%% overridden (incl. to `infinity` for caller-driven seal only).
-define(BONDY_MST_PACK_DEFAULT_AUTO_SEAL_RECORDS, 10_000).
-define(BONDY_MST_PACK_DEFAULT_AUTO_SEAL_BYTES,   16_000_000).

%% -----------------------------------------------------------------------------
%% Shared in-memory records
%% -----------------------------------------------------------------------------

%% Per-sealed-pack handle shared by `bondy_mst_pack_reader` and
%% `bondy_mst_pack_store`. Carries the pack's numeric id, its parsed
%% `.idx` (in-memory), and an open read fd against the `.pack` file.
%% The owning module decides ordering (the reader keeps them newest-
%% first for short-circuit reads; the store mirrors that).
-record(sealed_view, {
    pack_id :: non_neg_integer(),
    idx     :: bondy_mst_pack_index:t(),
    pack_fd :: file:fd()
}).
