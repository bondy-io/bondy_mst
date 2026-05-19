%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% Wire-format constants for the MST page-store packfile backend
%% (`bondy_mst_pack_*` modules). See `_design/latest/MST_PAGE_STORE_DESIGN.md`
%% §3 (pack file format) and §4 (index file format).
%%
%% These constants are stable on-disk values; any change is a format
%% break and must bump the corresponding `*_VERSION` macro.

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

%% Index header flag bits (byte 5, §4.1).
%% Bit 0 — bloom section present after the header.
-define(BONDY_MST_PACK_IDX_FLAG_BLOOM,    1).

%% -----------------------------------------------------------------------------
%% Bloom section (§4 extension; see `bondy_mst_pack_index` docstring)
%% -----------------------------------------------------------------------------

-define(BONDY_MST_PACK_BLOOM_HEADER_BYTES, 16).
-define(BONDY_MST_PACK_BLOOM_DEFAULT_P,    0.01).
