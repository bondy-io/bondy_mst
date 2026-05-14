%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_wal_frame).

-include("bondy_mst.hrl").
-include("bondy_oplog_wal.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Pure encode/decode of WAL frame headers and bodies.

See `_design/WAL_DESIGN.md` §3. Every batch is written as one frame; a
single-event append is a one-element batch. The frame layout is:

```
Offset  Size  Field           Description
------  ----  -----           -----------
   0     4    Magic           0x42444F50  ("BDOP")
   4     4    FrameLen        total frame length in bytes, including header
   8     4    CRC32           over bytes [4 .. FrameLen)
  12     1    FrameVersion    schema version; starts at 1
  13     3    Flags           bit 0: compressed body
                              bit 1: encrypted body
                              bits 2..23: reserved, zero
  16   var    Body            encoded list of bondy_oplog_event
```

CRC32 is `erlang:crc32/1` (zlib CRC32). The CRC covers
`FrameLen || FrameVersion || Flags || Body` — i.e. bytes
`[4 .. FrameLen)`. It does **not** cover Magic or CRC itself; Magic and
CRC are validated as separate sniff checks during recovery, so a
corrupted Magic produces a distinct error type from a CRC mismatch.

`encode/1,2` returns the frame as iodata so callers (the writer, tests)
that hand the result to `prim_file:write/2` avoid an extra binary copy
of the body. Wrap with `iolist_to_binary/1` if a contiguous binary is
needed.

This module is pure: no I/O. It is the building block used by the
writer (`bondy_oplog_wal`), the reader (`bondy_oplog_wal_reader`), and
the recovery scanner (`bondy_oplog_wal_recovery`).
""").

-define(MAGIC, ?BONDY_OPLOG_WAL_FRAME_MAGIC).
-define(HEADER_BYTES, ?BONDY_OPLOG_WAL_FRAME_HEADER_BYTES).
-define(VERSION, ?BONDY_OPLOG_WAL_FRAME_VERSION).
-define(KNOWN_FLAGS, ?BONDY_OPLOG_WAL_FRAME_KNOWN_FLAGS_V1).

-type body() :: iodata().
-type frame() :: iodata().
-type flags() :: 0..16#FFFFFF.
-type frame_version() :: 0..16#FF.
-type decode_error() ::
    bad_magic
    | crc_mismatch
    | length_invalid
    | truncated_header
    | truncated_body
    | trailing_bytes
    | unsupported_version
    | unknown_flag.

-export_type([body/0]).
-export_type([frame/0]).
-export_type([flags/0]).
-export_type([frame_version/0]).
-export_type([decode_error/0]).

-export([encode/1]).
-export([encode/2]).
-export([decode/1]).
-export([decode_header/1]).
-export([header_bytes/0]).

%% =============================================================================
%% API
%% =============================================================================

?DOC("Returns the fixed frame header size in bytes (16).").
-spec header_bytes() -> pos_integer().

header_bytes() ->
    ?HEADER_BYTES.

?DOC("""
Encodes `Body` into a frame with default version and zero flags.

Equivalent to `encode(Body, [])`.
""").
-spec encode(body()) -> frame().

encode(Body) ->
    encode(Body, []).

?DOC("""
Encodes `Body` into a frame. `Opts` may contain:

- `{version, FrameVersion}` — defaults to `?BONDY_OPLOG_WAL_FRAME_VERSION`.
- `{flags, Flags}` — only bits set in
  `?BONDY_OPLOG_WAL_FRAME_KNOWN_FLAGS_V1` are accepted; v1 rejects any
  other bit with `badarg` so the on-disk format stays clean of forward
  contamination. Defaults to `0`.

`Body` may be any `iodata()`. The returned frame is also `iodata()` —
callers that pass it to `prim_file:write/2` avoid an extra body copy.
Wrap with `iolist_to_binary/1` if a contiguous binary is needed.
""").
-spec encode(body(), [{version, frame_version()} | {flags, flags()}]) ->
    frame().

encode(Body, Opts) when is_list(Opts) ->
    Version = proplists:get_value(version, Opts, ?VERSION),
    Flags = proplists:get_value(flags, Opts, 0),
    valid_version(Version) orelse error({badarg, {version, Version}}),
    valid_flags(Flags) orelse error({badarg, {flags, Flags}}),
    BodySize = iolist_size(Body),
    FrameLen = ?HEADER_BYTES + BodySize,
    %% Layout: Magic(4) | FrameLen(4) | Crc(4) | Version(1) | Flags(3) | Body.
    %% CRC scope: FrameLen | Version | Flags | Body — i.e. bytes [4..FrameLen).
    %% Reusing `VerFlags` between the CRC input and the on-disk frame
    %% avoids the second body copy that a single contiguous binary
    %% would require.
    VerFlags = <<Version:8/unsigned, Flags:24/big-unsigned>>,
    Crc = erlang:crc32([<<FrameLen:32/big-unsigned>>, VerFlags, Body]),
    [<<?MAGIC:32/big-unsigned, FrameLen:32/big-unsigned,
        Crc:32/big-unsigned>>, VerFlags, Body].

?DOC("""
Decodes a single frame from `Binary`. The binary must contain **exactly
one complete frame**; both too-few and too-many bytes are errors.

Returns `{ok, Body, Meta}` where `Meta = #{version => V, flags => F}`,
or `{error, Reason}`. See `decode_error/0` for the reason space.

Errors used by the recovery scanner to drive break-and-truncate:
- `truncated_header` — fewer than 16 bytes available.
- `truncated_body` — declared FrameLen exceeds the available bytes.
- `trailing_bytes` — declared FrameLen is shorter than the available
  bytes; caller has read past the frame end.
- `bad_magic` — Magic field is not `BDOP`.
- `crc_mismatch` — CRC32 over `[4..FrameLen)` does not match.
- `length_invalid` — FrameLen is smaller than the header itself.
- `unsupported_version` — FrameVersion is not v1.
- `unknown_flag` — a flag bit outside the known mask is set.

For streaming decode (multiple frames in a stream), use
`decode_header/1` to read the header from the first 16 bytes, then read
`FrameLen - 16` more bytes and pass the complete frame here.
""").
-spec decode(binary()) ->
    {ok, binary(), #{version := frame_version(), flags := flags()}}
    | {error, decode_error()}.

decode(Bin) when is_binary(Bin), byte_size(Bin) < ?HEADER_BYTES ->
    %% Size check is first so a too-short input is reported as
    %% truncated regardless of whatever bytes it happens to contain.
    {error, truncated_header};
decode(<<?MAGIC:32/big-unsigned, FrameLen:32/big-unsigned,
        Crc:32/big-unsigned, Version:8/unsigned, Flags:24/big-unsigned,
        Rest/binary>>) ->
    decode_validated(FrameLen, Crc, Version, Flags, Rest);
decode(<<Magic:32/big-unsigned, _/binary>>) when Magic =/= ?MAGIC ->
    {error, bad_magic}.

?DOC("""
Decodes only the 16-byte frame header. Used by the streaming recovery
scanner that reads the body separately after sizing.

Returns `{ok, Header}` where `Header = #{frame_len => integer(),
crc => integer(), version => frame_version(), flags => flags()}`, or
`{error, Reason}`.

`unknown_flag`, `unsupported_version` and `crc_mismatch` are **not**
reported here; this is a sniff function. The caller must read the body
and call `decode/1` for full validation.
""").
-spec decode_header(binary()) ->
    {ok, #{
        frame_len := pos_integer(),
        crc := non_neg_integer(),
        version := frame_version(),
        flags := flags()
    }}
    | {error, bad_magic | length_invalid | truncated_header}.

decode_header(Bin) when is_binary(Bin), byte_size(Bin) < ?HEADER_BYTES ->
    {error, truncated_header};
decode_header(
    <<?MAGIC:32/big-unsigned, FrameLen:32/big-unsigned,
        Crc:32/big-unsigned, Version:8/unsigned, Flags:24/big-unsigned,
        _/binary>>
) when FrameLen >= ?HEADER_BYTES ->
    {ok, #{
        frame_len => FrameLen,
        crc => Crc,
        version => Version,
        flags => Flags
    }};
decode_header(<<?MAGIC:32/big-unsigned, FrameLen:32/big-unsigned, _/binary>>)
    when FrameLen < ?HEADER_BYTES ->
    {error, length_invalid};
decode_header(<<Magic:32/big-unsigned, _/binary>>) when Magic =/= ?MAGIC ->
    {error, bad_magic}.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
decode_validated(FrameLen, _Crc, _Version, _Flags, _Rest) when
    FrameLen < ?HEADER_BYTES
->
    {error, length_invalid};
decode_validated(FrameLen, Crc, Version, Flags, Rest) ->
    BodyLen = FrameLen - ?HEADER_BYTES,
    RestLen = byte_size(Rest),
    if
        RestLen < BodyLen ->
            {error, truncated_body};
        RestLen > BodyLen ->
            {error, trailing_bytes};
        true ->
            verify_crc_and_decode(Crc, Version, Flags, FrameLen, Rest)
    end.

%% @private
verify_crc_and_decode(Crc, Version, Flags, FrameLen, Body) ->
    LenVerFlags = <<FrameLen:32/big-unsigned, Version:8/unsigned,
                    Flags:24/big-unsigned>>,
    case erlang:crc32([LenVerFlags, Body]) of
        Crc ->
            case valid_version(Version) of
                false ->
                    {error, unsupported_version};
                true ->
                    case valid_flags(Flags) of
                        false -> {error, unknown_flag};
                        true ->
                            {ok, Body,
                                #{version => Version, flags => Flags}}
                    end
            end;
        _ ->
            {error, crc_mismatch}
    end.

%% @private
valid_version(V) when is_integer(V), V >= 0, V =< 16#FF ->
    %% v1 only accepts its own schema version. Future versions widen
    %% this predicate.
    V =:= ?VERSION;
valid_version(_) ->
    false.

%% @private
valid_flags(F) when is_integer(F), F >= 0, F =< 16#FFFFFF ->
    F band (bnot ?KNOWN_FLAGS band 16#FFFFFF) =:= 0;
valid_flags(_) ->
    false.
