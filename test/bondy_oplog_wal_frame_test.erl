%% =============================================================================
%% Unit tests for `bondy_oplog_wal_frame` (frame encode/decode).
%%
%% Property-based tests live in `bondy_oplog_wal_proper_test`.
%% =============================================================================

-module(bondy_oplog_wal_frame_test).

-include_lib("eunit/include/eunit.hrl").
-include("bondy_oplog_wal.hrl").

-define(MAGIC, ?BONDY_OPLOG_WAL_FRAME_MAGIC).
-define(HEADER, ?BONDY_OPLOG_WAL_FRAME_HEADER_BYTES).

%% Encode and immediately flatten to a binary for the EUnit tests that
%% want to inspect bytes. The writer passes the iodata straight to
%% `prim_file:write/2`.
encode(Body) ->
    iolist_to_binary(bondy_oplog_wal_frame:encode(Body)).

encode(Body, Opts) ->
    iolist_to_binary(bondy_oplog_wal_frame:encode(Body, Opts)).

%% =============================================================================
%% Basic encode/decode round-trip
%% =============================================================================

empty_body_roundtrip_test() ->
    Frame = encode(<<>>),
    ?assertEqual(?HEADER, byte_size(Frame)),
    ?assertMatch({ok, <<>>, #{version := 1, flags := 0}},
                 bondy_oplog_wal_frame:decode(Frame)).

small_body_roundtrip_test() ->
    Body = <<"hello, wal">>,
    Frame = encode(Body),
    ?assertEqual(?HEADER + byte_size(Body), byte_size(Frame)),
    ?assertMatch({ok, Body, _},
                 bondy_oplog_wal_frame:decode(Frame)).

large_body_roundtrip_test() ->
    Body = crypto:strong_rand_bytes(64 * 1024),
    Frame = encode(Body),
    {ok, Decoded, _Meta} = bondy_oplog_wal_frame:decode(Frame),
    ?assertEqual(Body, Decoded).

iodata_body_is_flattened_on_decode_test() ->
    %% Passing iodata in must produce the same on-wire body bytes
    %% as the contiguous binary equivalent.
    Iodata = [<<"hello, ">>, <<"wal">>],
    Frame = encode(Iodata),
    {ok, Decoded, _} = bondy_oplog_wal_frame:decode(Frame),
    ?assertEqual(<<"hello, wal">>, Decoded).

term_to_binary_body_roundtrip_test() ->
    Events = [
        {event, 1, <<"alpha">>},
        {event, 2, <<"beta">>},
        {event, 3, <<"gamma">>}
    ],
    Body = term_to_binary(Events, [{minor_version, 2}, deterministic]),
    Frame = encode(Body),
    {ok, Decoded, _} = bondy_oplog_wal_frame:decode(Frame),
    ?assertEqual(Events, binary_to_term(Decoded)).

zero_flags_roundtrip_test() ->
    Frame = encode(<<"x">>, [{flags, 0}]),
    ?assertMatch({ok, <<"x">>, #{flags := 0}},
                 bondy_oplog_wal_frame:decode(Frame)).

encode_returns_iodata_test() ->
    %% Sanity: the public encode/1 returns iolist that flattens to a
    %% well-formed frame without needing iolist_to_binary in the hot path.
    Iodata = bondy_oplog_wal_frame:encode(<<"x">>),
    ?assert(is_list(Iodata)),
    ?assertEqual(?HEADER + 1, iolist_size(Iodata)),
    ?assertMatch({ok, <<"x">>, _},
                 bondy_oplog_wal_frame:decode(iolist_to_binary(Iodata))).

header_bytes_constant_test() ->
    ?assertEqual(?HEADER, bondy_oplog_wal_frame:header_bytes()).

%% =============================================================================
%% Decode error paths
%% =============================================================================

truncated_header_test() ->
    [
        ?assertMatch({error, truncated_header},
                     bondy_oplog_wal_frame:decode(crypto:strong_rand_bytes(N)))
     || N <- [0, 1, 5, 15]
    ].

bad_magic_test() ->
    Frame = encode(<<"x">>),
    Corrupt = <<16#DEADBEEF:32,
                (binary:part(Frame, 4, byte_size(Frame) - 4))/binary>>,
    ?assertEqual({error, bad_magic}, bondy_oplog_wal_frame:decode(Corrupt)).

crc_mismatch_test() ->
    Frame0 = encode(<<"hello">>),
    Before = binary:part(Frame0, 0, ?HEADER),
    Body0 = binary:part(Frame0, ?HEADER, byte_size(Frame0) - ?HEADER),
    <<H, T/binary>> = Body0,
    Body1 = <<(H bxor 1):8, T/binary>>,
    Frame1 = <<Before/binary, Body1/binary>>,
    ?assertEqual({error, crc_mismatch}, bondy_oplog_wal_frame:decode(Frame1)).

length_invalid_test() ->
    Bin = <<?MAGIC:32, 8:32, 0:32, 1:8, 0:24>>,
    ?assertEqual({error, length_invalid}, bondy_oplog_wal_frame:decode(Bin)).

truncated_body_test() ->
    %% FrameLen says 32 but only 16 bytes are present.
    Bin = <<?MAGIC:32, 32:32, 0:32, 1:8, 0:24>>,
    ?assertEqual({error, truncated_body}, bondy_oplog_wal_frame:decode(Bin)).

trailing_bytes_test() ->
    %% A complete frame followed by extra bytes the caller forgot to trim.
    Frame = encode(<<"x">>),
    WithGarbage = <<Frame/binary, "extra">>,
    ?assertEqual({error, trailing_bytes},
                 bondy_oplog_wal_frame:decode(WithGarbage)).

unsupported_version_test() ->
    Body = <<"body">>,
    FrameLen = ?HEADER + byte_size(Body),
    Version = 99,
    Flags = 0,
    CrcInput = <<FrameLen:32, Version:8, Flags:24, Body/binary>>,
    Crc = erlang:crc32(CrcInput),
    Frame = <<?MAGIC:32, FrameLen:32, Crc:32, Version:8, Flags:24, Body/binary>>,
    ?assertEqual({error, unsupported_version},
                 bondy_oplog_wal_frame:decode(Frame)).

unknown_flag_test() ->
    %% v1 known-flags mask is 0; any set flag bit is unknown_flag at decode.
    Body = <<"body">>,
    FrameLen = ?HEADER + byte_size(Body),
    Version = 1,
    Flags = 16#000004,
    CrcInput = <<FrameLen:32, Version:8, Flags:24, Body/binary>>,
    Crc = erlang:crc32(CrcInput),
    Frame = <<?MAGIC:32, FrameLen:32, Crc:32, Version:8, Flags:24, Body/binary>>,
    ?assertEqual({error, unknown_flag},
                 bondy_oplog_wal_frame:decode(Frame)).

%% =============================================================================
%% decode_header/1
%% =============================================================================

decode_header_ok_test() ->
    Frame = encode(<<"abc">>),
    {ok, Header} = bondy_oplog_wal_frame:decode_header(Frame),
    ?assertEqual(?HEADER + 3, maps:get(frame_len, Header)),
    ?assertEqual(1, maps:get(version, Header)),
    ?assertEqual(0, maps:get(flags, Header)).

decode_header_only_header_bytes_test() ->
    Frame = encode(<<"abc">>),
    HeaderOnly = binary:part(Frame, 0, ?HEADER),
    ?assertMatch({ok, _}, bondy_oplog_wal_frame:decode_header(HeaderOnly)).

decode_header_bad_magic_test() ->
    Bin = <<0:32, 32:32, 0:32, 1:8, 0:24>>,
    ?assertEqual({error, bad_magic},
                 bondy_oplog_wal_frame:decode_header(Bin)).

decode_header_length_invalid_test() ->
    Bin = <<?MAGIC:32, 8:32, 0:32, 1:8, 0:24>>,
    ?assertEqual({error, length_invalid},
                 bondy_oplog_wal_frame:decode_header(Bin)).

decode_header_truncated_test() ->
    [
        ?assertMatch({error, truncated_header},
                     bondy_oplog_wal_frame:decode_header(<<>>)),
        ?assertMatch({error, truncated_header},
                     bondy_oplog_wal_frame:decode_header(<<?MAGIC:32>>)),
        ?assertMatch({error, truncated_header},
                     bondy_oplog_wal_frame:decode_header(<<?MAGIC:32, 0:64>>))
    ].

%% =============================================================================
%% Validation guards
%% =============================================================================

invalid_version_rejected_test() ->
    ?assertError({badarg, _},
                 bondy_oplog_wal_frame:encode(<<>>, [{version, 99}])).

invalid_flag_bit_rejected_at_encode_test() ->
    %% v1 known-flags mask is zero; any non-zero flag is `badarg` on encode.
    ?assertError({badarg, _},
                 bondy_oplog_wal_frame:encode(<<>>, [{flags, 16#1}])),
    ?assertError({badarg, _},
                 bondy_oplog_wal_frame:encode(<<>>, [{flags, 16#FFFFFFFF}])).
