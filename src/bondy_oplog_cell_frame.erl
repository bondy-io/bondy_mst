%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_cell_frame).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Codec for **projection cell value frames** (`MST_DB_DESIGN.md` §3).

A projection adapter stores each cell as a binary frame:

```
<<HlcLen:16, Hlc:HlcLen/binary, FoldedValueBytes/binary>>
```

The HLC is encoded as a fixed-width 8-byte big-endian unsigned integer
(8 bytes is sufficient for `bondy_oplog_hlc:hlc()` which is a 64-bit
non-negative integer). The leading `HlcLen:16` byte-length prefix
preserves forward compatibility — future formats can carry larger HLC
representations (HLC vectors, version vectors) without a frame-format
migration.

The body is opaque to this module; it is the output of the namespace's
`bondy_oplog_fold:encode_state/2`. `bondy_db_core` strips the frame on
read and calls `decode_state/2` on the body.

## Invariants

- `decode(encode(Hlc, Body)) == {Hlc, Body}` for any valid Hlc / Body.
- `encode/2` produces a binary of length `2 + HlcLen + byte_size(Body)`.
- `decode/1` is total over well-formed frames; malformed input raises
  `error:function_clause`.
""").

-define(HLC_BYTES, 8).

-export([encode/2]).
-export([decode/1]).
-export([encoded_size/1]).

%% =============================================================================
%% API
%% =============================================================================

-spec encode(Hlc :: bondy_oplog_hlc:hlc(), Body :: binary()) -> binary().

encode(Hlc, Body) when
    is_integer(Hlc),
    Hlc >= 0,
    is_binary(Body)
->
    <<?HLC_BYTES:16/big-unsigned, Hlc:64/big-unsigned, Body/binary>>.


-spec decode(Frame :: binary()) -> {bondy_oplog_hlc:hlc(), Body :: binary()}.

decode(<<HlcLen:16/big-unsigned, HlcBin:HlcLen/binary, Body/binary>>) ->
    Hlc = binary:decode_unsigned(HlcBin, big),
    {Hlc, Body}.


-spec encoded_size(BodySize :: non_neg_integer()) -> non_neg_integer().

encoded_size(BodySize) when is_integer(BodySize), BodySize >= 0 ->
    2 + ?HLC_BYTES + BodySize.
