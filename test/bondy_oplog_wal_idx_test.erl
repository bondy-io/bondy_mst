%% =============================================================================
%% Unit tests for `bondy_oplog_wal_idx` (sparse HLC index `.qidx`).
%%
%% Tests cover the three concerns of the index module:
%%
%% 1. Accumulator semantics — first frame always indexed, subsequent
%%    frames indexed only when bytes-since-last crosses the interval,
%%    interval reset on emit, entries returned in HLC-ascending order.
%% 2. File I/O — header/entry codec round-trip via write_file/read_file;
%%    empty index is valid; tmp+rename atomicity (a partial tmp does not
%%    overwrite the live file); error paths for truncated and bad-magic
%%    files.
%% 3. Seek — binary search returns the largest entry with HLC <= T;
%%    none for T below first entry; correct on edge cases (single entry,
%%    exact-match HLC, T at first/last entry's HLC).
%% =============================================================================

-module(bondy_oplog_wal_idx_test).

-include_lib("eunit/include/eunit.hrl").
-include("bondy_oplog_wal.hrl").

-define(MAGIC, ?BONDY_OPLOG_WAL_IDX_MAGIC).
-define(HEADER, ?BONDY_OPLOG_WAL_IDX_HEADER_BYTES).
-define(ENTRY, ?BONDY_OPLOG_WAL_IDX_ENTRY_BYTES).

%% =============================================================================
%% Fixture helpers
%% =============================================================================

mktemp_dir() ->
    Base = filename:join(
        ["/tmp", io_lib:format("bondy_oplog_wal_idx_test_~p_~p",
                              [erlang:system_time(microsecond),
                               erlang:unique_integer([positive])])]
    ),
    Dir = lists:flatten(Base),
    ok = filelib:ensure_path(Dir),
    Dir.

rmrf(Dir) ->
    _ = file:del_dir_r(Dir),
    ok.

with_tmp_dir(Fun) ->
    Dir = mktemp_dir(),
    try Fun(Dir)
    after rmrf(Dir)
    end.

%% =============================================================================
%% Constants
%% =============================================================================

filename_renders_zero_padded_9_digit_decimal_test() ->
    ?assertEqual(<<"000000000.qidx">>, bondy_oplog_wal_idx:filename(0)),
    ?assertEqual(<<"000000042.qidx">>, bondy_oplog_wal_idx:filename(42)),
    ?assertEqual(<<"123456789.qidx">>, bondy_oplog_wal_idx:filename(123456789)).

header_bytes_is_16_test() ->
    ?assertEqual(16, bondy_oplog_wal_idx:header_bytes()),
    ?assertEqual(16, ?HEADER).

entry_bytes_is_16_test() ->
    ?assertEqual(16, bondy_oplog_wal_idx:entry_bytes()),
    ?assertEqual(16, ?ENTRY).

%% =============================================================================
%% Accumulator
%% =============================================================================

new_returns_empty_accumulator_test() ->
    Acc = bondy_oplog_wal_idx:new(),
    ?assertEqual([], bondy_oplog_wal_idx:entries(Acc)),
    ?assertEqual(0, bondy_oplog_wal_idx:entry_count(Acc)),
    ?assertEqual(
        ?BONDY_OPLOG_WAL_IDX_DEFAULT_INTERVAL_BYTES,
        bondy_oplog_wal_idx:interval_bytes(Acc)
    ).

new_with_custom_interval_test() ->
    Acc = bondy_oplog_wal_idx:new(1024),
    ?assertEqual(1024, bondy_oplog_wal_idx:interval_bytes(Acc)).

first_frame_is_always_indexed_test() ->
    %% Even when frame size < interval, the very first frame of a
    %% segment must produce an entry so seek always finds at least one
    %% anchor point.
    Acc0 = bondy_oplog_wal_idx:new(1_000_000),
    Acc1 = bondy_oplog_wal_idx:note_frame(Acc0, 100, 48, 80),
    ?assertEqual([{100, 48}], bondy_oplog_wal_idx:entries(Acc1)),
    ?assertEqual(1, bondy_oplog_wal_idx:entry_count(Acc1)).

subsequent_frames_indexed_only_after_interval_test() ->
    %% Interval = 1000 bytes; frames are 100 bytes each. Frame 0 indexed
    %% (always); frames 1..9 accumulate without emitting; frame 10
    %% crosses interval and emits.
    Acc0 = bondy_oplog_wal_idx:new(1000),
    %% First frame at offset 48.
    Acc1 = bondy_oplog_wal_idx:note_frame(Acc0, 100, 48, 100),
    %% 9 more frames at offsets 148, 248, ..., 948. None should emit
    %% because the bytes_since_last counter resets to 0 after the first
    %% emit, then accumulates 100 per frame: after frame 9 it's 900.
    Acc10 = lists:foldl(
        fun(I, A) ->
            Off = 48 + I * 100,
            Hlc = 100 + I,
            bondy_oplog_wal_idx:note_frame(A, Hlc, Off, 100)
        end,
        Acc1,
        lists:seq(1, 9)
    ),
    ?assertEqual(1, bondy_oplog_wal_idx:entry_count(Acc10)),
    %% Frame 10 at offset 1048; bytes_since_last + 100 = 900 + 100 = 1000
    %% which crosses the threshold → emit.
    Acc11 = bondy_oplog_wal_idx:note_frame(Acc10, 110, 1048, 100),
    ?assertEqual(2, bondy_oplog_wal_idx:entry_count(Acc11)),
    ?assertEqual([{100, 48}, {110, 1048}], bondy_oplog_wal_idx:entries(Acc11)).

entries_are_hlc_ascending_test() ->
    Acc0 = bondy_oplog_wal_idx:new(100),
    %% Three entries: small interval forces emit on every frame.
    Acc1 = bondy_oplog_wal_idx:note_frame(Acc0, 100, 48, 200),
    Acc2 = bondy_oplog_wal_idx:note_frame(Acc1, 110, 248, 200),
    Acc3 = bondy_oplog_wal_idx:note_frame(Acc2, 120, 448, 200),
    ?assertEqual(
        [{100, 48}, {110, 248}, {120, 448}],
        bondy_oplog_wal_idx:entries(Acc3)
    ).

interval_resets_on_emit_test() ->
    Acc0 = bondy_oplog_wal_idx:new(500),
    %% Frame 0 (300 bytes) indexed (first). bytes_since_last = 0.
    Acc1 = bondy_oplog_wal_idx:note_frame(Acc0, 1, 48, 300),
    %% Frame 1 (300 bytes). bytes_since_last + 300 = 0 + 300 = 300, < 500
    %% → no emit, bytes_since_last = 300.
    Acc2 = bondy_oplog_wal_idx:note_frame(Acc1, 2, 348, 300),
    ?assertEqual(1, bondy_oplog_wal_idx:entry_count(Acc2)),
    %% Frame 2 (300 bytes). 300 + 300 = 600, >= 500 → emit. Reset.
    Acc3 = bondy_oplog_wal_idx:note_frame(Acc2, 3, 648, 300),
    ?assertEqual(2, bondy_oplog_wal_idx:entry_count(Acc3)),
    %% Frame 3 (300 bytes). After reset bytes_since_last = 0; this
    %% becomes 300 < 500 → no emit.
    Acc4 = bondy_oplog_wal_idx:note_frame(Acc3, 4, 948, 300),
    ?assertEqual(2, bondy_oplog_wal_idx:entry_count(Acc4)).

%% =============================================================================
%% File I/O
%% =============================================================================

write_then_read_roundtrip_test() ->
    with_tmp_dir(fun(Dir) ->
        Path = filename:join(Dir, "000000000.qidx"),
        Entries = [{100, 48}, {200, 1024}, {300, 2048}],
        ok = bondy_oplog_wal_idx:write_file(Path, Entries),
        ?assertEqual({ok, Entries}, bondy_oplog_wal_idx:read_file(Path))
    end).

write_empty_index_is_valid_test() ->
    with_tmp_dir(fun(Dir) ->
        Path = filename:join(Dir, "000000000.qidx"),
        ok = bondy_oplog_wal_idx:write_file(Path, []),
        ?assertEqual({ok, []}, bondy_oplog_wal_idx:read_file(Path)),
        %% File should be exactly 16 bytes (header only).
        {ok, FileInfo} = file:read_file_info(Path),
        ?assertEqual(16, element(2, FileInfo))
    end).

read_nonexistent_file_returns_enoent_test() ->
    with_tmp_dir(fun(Dir) ->
        Path = filename:join(Dir, "does_not_exist.qidx"),
        ?assertEqual({error, enoent}, bondy_oplog_wal_idx:read_file(Path))
    end).

read_truncated_header_test() ->
    with_tmp_dir(fun(Dir) ->
        Path = filename:join(Dir, "000000000.qidx"),
        %% Write only 8 bytes — less than the 16-byte header.
        ok = file:write_file(Path, <<1, 2, 3, 4, 5, 6, 7, 8>>),
        ?assertEqual(
            {error, truncated_header},
            bondy_oplog_wal_idx:read_file(Path)
        )
    end).

read_bad_magic_test() ->
    with_tmp_dir(fun(Dir) ->
        Path = filename:join(Dir, "000000000.qidx"),
        %% 16 bytes but not the magic.
        ok = file:write_file(Path, <<0, 0, 0, 0,  0, 0, 0, 0,
                                     0, 0, 0, 0,  0, 0, 0, 0>>),
        ?assertEqual({error, bad_magic}, bondy_oplog_wal_idx:read_file(Path))
    end).

read_unsupported_version_test() ->
    with_tmp_dir(fun(Dir) ->
        Path = filename:join(Dir, "000000000.qidx"),
        %% Valid magic but version = 99.
        Bin = <<?MAGIC:32/big-unsigned, 99:8, 0:24, 0:32, 0:32>>,
        ok = file:write_file(Path, Bin),
        ?assertEqual(
            {error, unsupported_version},
            bondy_oplog_wal_idx:read_file(Path)
        )
    end).

read_truncated_entries_test() ->
    with_tmp_dir(fun(Dir) ->
        Path = filename:join(Dir, "000000000.qidx"),
        %% Header claims 2 entries but only 1 entry's worth of bytes
        %% follows.
        Header = <<?MAGIC:32/big-unsigned, 1:8, 0:24, 2:32, 0:32>>,
        Entry = <<100:64, 48:64>>,
        ok = file:write_file(Path, [Header, Entry]),
        ?assertEqual(
            {error, truncated_entries},
            bondy_oplog_wal_idx:read_file(Path)
        )
    end).

read_trailing_bytes_test() ->
    with_tmp_dir(fun(Dir) ->
        Path = filename:join(Dir, "000000000.qidx"),
        %% Header claims 0 entries but bytes follow.
        Header = <<?MAGIC:32/big-unsigned, 1:8, 0:24, 0:32, 0:32>>,
        ok = file:write_file(Path, [Header, <<"garbage">>]),
        ?assertEqual(
            {error, trailing_bytes},
            bondy_oplog_wal_idx:read_file(Path)
        )
    end).

write_is_atomic_rename_test() ->
    %% After a successful write, the tmp file is gone (rename consumed
    %% it) and the final file exists with the new content. We can't
    %% easily kill the writer mid-operation in unit tests, so we just
    %% verify the post-state.
    with_tmp_dir(fun(Dir) ->
        Path = filename:join(Dir, "000000000.qidx"),
        TmpPath = iolist_to_binary([Path, ".tmp"]),
        ok = bondy_oplog_wal_idx:write_file(Path, [{100, 48}]),
        ?assert(filelib:is_regular(Path)),
        ?assertNot(filelib:is_regular(TmpPath))
    end).

write_overwrites_existing_file_test() ->
    with_tmp_dir(fun(Dir) ->
        Path = filename:join(Dir, "000000000.qidx"),
        ok = bondy_oplog_wal_idx:write_file(Path, [{100, 48}]),
        ok = bondy_oplog_wal_idx:write_file(
            Path, [{100, 48}, {200, 1024}]
        ),
        ?assertEqual(
            {ok, [{100, 48}, {200, 1024}]},
            bondy_oplog_wal_idx:read_file(Path)
        )
    end).

write_accumulator_entries_round_trip_test() ->
    with_tmp_dir(fun(Dir) ->
        Acc0 = bondy_oplog_wal_idx:new(100),
        Acc1 = bondy_oplog_wal_idx:note_frame(Acc0, 1, 48, 100),
        Acc2 = bondy_oplog_wal_idx:note_frame(Acc1, 2, 148, 100),
        Acc3 = bondy_oplog_wal_idx:note_frame(Acc2, 3, 248, 100),
        Entries = bondy_oplog_wal_idx:entries(Acc3),
        Path = filename:join(Dir, "000000000.qidx"),
        ok = bondy_oplog_wal_idx:write_file(Path, Entries),
        ?assertEqual({ok, Entries}, bondy_oplog_wal_idx:read_file(Path))
    end).

%% =============================================================================
%% Reader handle / seek
%% =============================================================================

from_entries_empty_returns_handle_test() ->
    Handle = bondy_oplog_wal_idx:from_entries([]),
    ?assertEqual([], bondy_oplog_wal_idx:handle_entries(Handle)),
    ?assertEqual(none, bondy_oplog_wal_idx:seek(Handle, 100)).

seek_finds_exact_match_test() ->
    Handle = bondy_oplog_wal_idx:from_entries(
        [{100, 48}, {200, 1024}, {300, 2048}]
    ),
    ?assertEqual({ok, 1024}, bondy_oplog_wal_idx:seek(Handle, 200)).

seek_finds_largest_le_target_test() ->
    %% T = 150 → largest entry with HLC <= 150 is {100, 48}.
    Handle = bondy_oplog_wal_idx:from_entries(
        [{100, 48}, {200, 1024}, {300, 2048}]
    ),
    ?assertEqual({ok, 48}, bondy_oplog_wal_idx:seek(Handle, 150)),
    %% T = 250 → {200, 1024}.
    ?assertEqual({ok, 1024}, bondy_oplog_wal_idx:seek(Handle, 250)).

seek_returns_last_entry_for_t_above_all_test() ->
    Handle = bondy_oplog_wal_idx:from_entries(
        [{100, 48}, {200, 1024}, {300, 2048}]
    ),
    ?assertEqual({ok, 2048}, bondy_oplog_wal_idx:seek(Handle, 1000)).

seek_returns_none_for_t_below_first_test() ->
    Handle = bondy_oplog_wal_idx:from_entries(
        [{100, 48}, {200, 1024}, {300, 2048}]
    ),
    ?assertEqual(none, bondy_oplog_wal_idx:seek(Handle, 50)).

seek_single_entry_test() ->
    Handle = bondy_oplog_wal_idx:from_entries([{100, 48}]),
    ?assertEqual({ok, 48}, bondy_oplog_wal_idx:seek(Handle, 100)),
    ?assertEqual({ok, 48}, bondy_oplog_wal_idx:seek(Handle, 1000)),
    ?assertEqual(none, bondy_oplog_wal_idx:seek(Handle, 99)).

seek_at_t_equals_first_hlc_test() ->
    Handle = bondy_oplog_wal_idx:from_entries(
        [{100, 48}, {200, 1024}]
    ),
    ?assertEqual({ok, 48}, bondy_oplog_wal_idx:seek(Handle, 100)).

seek_large_random_index_test() ->
    %% Build an index with 1000 entries with HLCs at strides of 10:
    %%   {10, _}, {20, _}, ..., {10000, _}
    %% Then seek for a handful of T values and verify the result is the
    %% largest entry HLC <= T.
    Entries = [{H, H * 1000} || H <- lists:seq(10, 10000, 10)],
    Handle = bondy_oplog_wal_idx:from_entries(Entries),
    %% T = 9 → none (below first).
    ?assertEqual(none, bondy_oplog_wal_idx:seek(Handle, 9)),
    %% T = 10 → {10, 10000}.
    ?assertEqual({ok, 10000}, bondy_oplog_wal_idx:seek(Handle, 10)),
    %% T = 15 → {10, 10000} (largest HLC <= 15).
    ?assertEqual({ok, 10000}, bondy_oplog_wal_idx:seek(Handle, 15)),
    %% T = 1234 → largest HLC <= 1234 is 1230 → {1230, 1230000}.
    ?assertEqual({ok, 1230000}, bondy_oplog_wal_idx:seek(Handle, 1234)),
    %% T = 5000 → exact match {5000, 5000000}.
    ?assertEqual({ok, 5000000}, bondy_oplog_wal_idx:seek(Handle, 5000)),
    %% T = 10001 → largest HLC <= 10001 is 10000 → {10000, 10000000}.
    ?assertEqual({ok, 10000000}, bondy_oplog_wal_idx:seek(Handle, 10001)),
    %% T = 10000000 → same.
    ?assertEqual({ok, 10000000}, bondy_oplog_wal_idx:seek(Handle, 10000000)).

open_round_trips_via_file_test() ->
    with_tmp_dir(fun(Dir) ->
        Path = filename:join(Dir, "000000000.qidx"),
        Entries = [{100, 48}, {200, 1024}, {300, 2048}],
        ok = bondy_oplog_wal_idx:write_file(Path, Entries),
        {ok, Handle} = bondy_oplog_wal_idx:open(Path),
        ?assertEqual(Entries, bondy_oplog_wal_idx:handle_entries(Handle)),
        ?assertEqual({ok, 1024}, bondy_oplog_wal_idx:seek(Handle, 250))
    end).

open_propagates_file_errors_test() ->
    with_tmp_dir(fun(Dir) ->
        Path = filename:join(Dir, "missing.qidx"),
        ?assertEqual({error, enoent}, bondy_oplog_wal_idx:open(Path))
    end).
