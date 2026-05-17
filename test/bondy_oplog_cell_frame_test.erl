%% =============================================================================
%% Tests for the projection cell value frame codec
%% (`MST_DB_DESIGN.md` §3, wired in D2).
%%
%% Pins the encode/decode round-trip, the wire format
%% (`<<HlcLen:16, Hlc:64, Body/binary>>`), and edge cases.
%% =============================================================================

-module(bondy_oplog_cell_frame_test).

-include_lib("eunit/include/eunit.hrl").

%% =============================================================================
%% encode/2 + decode/1 round-trip
%% =============================================================================

roundtrip_zero_hlc_empty_body_test() ->
    Frame = bondy_oplog_cell_frame:encode(0, <<>>),
    ?assertEqual({0, <<>>}, bondy_oplog_cell_frame:decode(Frame)).

roundtrip_small_hlc_test() ->
    Frame = bondy_oplog_cell_frame:encode(42, <<"payload">>),
    ?assertEqual({42, <<"payload">>}, bondy_oplog_cell_frame:decode(Frame)).

roundtrip_max_hlc_test() ->
    MaxHlc = 16#FFFFFFFFFFFFFFFF,
    Frame = bondy_oplog_cell_frame:encode(MaxHlc, <<1, 2, 3>>),
    ?assertEqual({MaxHlc, <<1, 2, 3>>}, bondy_oplog_cell_frame:decode(Frame)).

roundtrip_large_body_test() ->
    Body = binary:copy(<<"x">>, 65_536),
    Frame = bondy_oplog_cell_frame:encode(1234, Body),
    ?assertEqual({1234, Body}, bondy_oplog_cell_frame:decode(Frame)).

%% =============================================================================
%% Wire-format checks
%% =============================================================================

encoded_frame_carries_hlc_length_prefix_test() ->
    %% Default impl uses 8-byte fixed-width HLC.
    Frame = bondy_oplog_cell_frame:encode(42, <<"abc">>),
    <<HlcLen:16, _Hlc:HlcLen/binary, Body/binary>> = Frame,
    ?assertEqual(8, HlcLen),
    ?assertEqual(<<"abc">>, Body).

encoded_size_matches_helper_test() ->
    Body = <<"some body bytes">>,
    Frame = bondy_oplog_cell_frame:encode(1, Body),
    ?assertEqual(
        bondy_oplog_cell_frame:encoded_size(byte_size(Body)),
        byte_size(Frame)
    ).

%% =============================================================================
%% Malformed input
%% =============================================================================

decode_truncated_frame_raises_test() ->
    ?assertError(function_clause, bondy_oplog_cell_frame:decode(<<>>)),
    ?assertError(function_clause, bondy_oplog_cell_frame:decode(<<0:8>>)).

%% =============================================================================
%% Negative-HLC guard
%% =============================================================================

encode_rejects_negative_hlc_test() ->
    ?assertError(function_clause, bondy_oplog_cell_frame:encode(-1, <<>>)).
