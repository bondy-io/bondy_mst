%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_mst_pack_paths).

-include("bondy_mst.hrl").
-include("bondy_mst_pack.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Filename conventions for the MST page-store pack backend.

See `_design/latest/MST_PAGE_STORE_DESIGN.md` §2. Sealed packs are
named `pack-NNNN.pack` (with `NNNN` a 4-digit zero-padded decimal
pack id) and accompanied by `pack-NNNN.idx`. The incoming pack is
`incoming.pack` (no separate on-disk index — the writer keeps an
in-memory `pending` map and rebuilds it on resume by scanning the
incoming-pack file). Tmp-rename files share the `.tmp` suffix.

Pure path arithmetic; no I/O.
""").

-export([sealed_pack_path/2]).
-export([sealed_pack_tmp_path/2]).
-export([sealed_idx_path/2]).
-export([sealed_idx_tmp_path/2]).
-export([incoming_pack_path/1]).
-export([sealed_pack_basename/1]).
-export([sealed_idx_basename/1]).

-define(PACK_DIGITS, 4).

%% =============================================================================
%% API
%% =============================================================================

?DOC("""
`Dir/pack-NNNN.pack` for sealed pack `PackId`. `PackId` is the
integer id stored in the manifest; the filename pads it to 4
digits for lexicographic-equals-numeric ordering up to 9999
packs per instance. Beyond that, the format must widen and the
recovery scanner adjust.
""").
-spec sealed_pack_path(file:filename_all(), non_neg_integer()) ->
    file:filename_all().

sealed_pack_path(Dir, PackId) ->
    filename:join(Dir, sealed_pack_basename(PackId)).

-spec sealed_pack_tmp_path(file:filename_all(), non_neg_integer()) ->
    file:filename_all().

sealed_pack_tmp_path(Dir, PackId) ->
    filename:join(Dir, sealed_pack_basename(PackId) ++ ".tmp").

-spec sealed_idx_path(file:filename_all(), non_neg_integer()) ->
    file:filename_all().

sealed_idx_path(Dir, PackId) ->
    filename:join(Dir, sealed_idx_basename(PackId)).

-spec sealed_idx_tmp_path(file:filename_all(), non_neg_integer()) ->
    file:filename_all().

sealed_idx_tmp_path(Dir, PackId) ->
    filename:join(Dir, sealed_idx_basename(PackId) ++ ".tmp").

-spec incoming_pack_path(file:filename_all()) -> file:filename_all().

incoming_pack_path(Dir) ->
    filename:join(Dir, ?BONDY_MST_PACK_INCOMING_PACK_FILENAME).

?DOC("""
Returns the basename `pack-NNNN.pack` for a pack id.
""").
-spec sealed_pack_basename(non_neg_integer()) -> string().

sealed_pack_basename(PackId) when is_integer(PackId), PackId >= 0 ->
    "pack-" ++ pad(PackId) ++ ".pack".

?DOC("""
Returns the basename `pack-NNNN.idx` for a pack id.
""").
-spec sealed_idx_basename(non_neg_integer()) -> string().

sealed_idx_basename(PackId) when is_integer(PackId), PackId >= 0 ->
    "pack-" ++ pad(PackId) ++ ".idx".

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
pad(N) ->
    S = integer_to_list(N),
    case ?PACK_DIGITS - length(S) of
        Pad when Pad > 0 -> lists:duplicate(Pad, $0) ++ S;
        _ -> S
    end.
