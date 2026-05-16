#!/usr/bin/env escript
%% -*- erlang -*-
%%! +sbtu +A0 -hidden -noinput
%%
%% Reader-side mmap profiling for sealed WAL segments.
%%
%% Gates the mmap upgrade decision recorded in
%% _design/latest/WAL_DESIGN_V2.md §PR9: the mmap path ships only if a
%% realistic reader workload spends >= 5 % of its wall-clock time in
%% the pread() calls that mmap would replace.
%%
%% Phase A — build a sealed WAL: write enough events to roll over
%% several segments so the reader actually has sealed segments to
%% scan. The head segment is excluded from the mmap analysis per the
%% design (head reads stay on pread; mmap on a file under append is
%% non-trivial).
%%
%% Phase B — measure the canonical operator-debug workload: a full
%% beginning-to-end scan via `bondy_oplog_wal_reader:next/1`. This is
%% the wall-clock total.
%%
%% Phase C — measure the same byte volume in pread-only mode: open
%% each sealed segment with `prim_file:open([read, raw, binary])` and
%% issue the same per-frame pread pattern (header + body) without
%% decoding. This is the lower bound on the pread cost share —
%% anything mmap could save lives inside this number.
%%
%% Gate: pread_share = pread_only_us / scan_us. If pread_share < 5 %,
%% mmap cannot meaningfully improve reader throughput regardless of
%% how cheap it makes the syscall — the remaining 95 %+ is decode,
%% CRC, term-to-binary, and dispatch overhead that mmap does not
%% touch. Defer in that case.
%%
%% Usage:
%%   scripts/wal_mmap_bench.escript
%%   scripts/wal_mmap_bench.escript --frames 4000 --batch 16384
%%   scripts/wal_mmap_bench.escript --segment-bytes 8388608

-include_lib("kernel/include/file.hrl").

-define(FRAME_HEADER_BYTES, 16).
-define(SEG_HEADER_BYTES, 48).

main(Args) ->
    Cfg = parse_args(Args),
    ok = setup_paths(),
    ok = ensure_apps([crypto, telemetry]),

    io:format("~n=== WAL mmap-share profiling ===~n"),
    io:format("hardware: ~s ~s   schedulers: ~p~n"
              "config:   frames=~p  batch_bytes=~p  segment_bytes=~p~n",
              [erlang:system_info(system_architecture),
               erlang:system_info(otp_release),
               erlang:system_info(schedulers_online),
               maps:get(frames, Cfg), maps:get(batch_bytes, Cfg),
               maps:get(segment_bytes, Cfg)]),

    Dir = build_wal(Cfg),
    try
        Sealed = list_sealed_segments(Dir),
        io:format("~n--- Workload summary ---~n"),
        io:format("  sealed_segments: ~p~n", [length(Sealed)]),
        SealedBytes = total_bytes(Sealed),
        io:format("  total_sealed_bytes: ~p (~.1f MiB)~n",
                  [SealedBytes, SealedBytes / (1024 * 1024)]),

        %% --- Phase B: full reader scan ---------------------------
        io:format("~n--- Phase B: full reader scan ---~n"),
        {ScanUs, FramesScanned, EventCount} = run_reader(Dir, Cfg),
        io:format("  walked_frames:   ~p~n", [FramesScanned]),
        io:format("  events_emitted:  ~p~n", [EventCount]),
        io:format("  scan_wall_us:    ~p~n", [ScanUs]),

        %% --- Phase C: pread-only walk ----------------------------
        io:format("~n--- Phase C: pread-only walk (sealed, no mmap) ---~n"),
        {PreadUs, PreadFrames, PreadBytes} =
            run_walk(Sealed, [read, raw, binary]),
        io:format("  walked_frames:   ~p~n", [PreadFrames]),
        io:format("  walked_bytes:    ~p (~.1f MiB)~n",
                  [PreadBytes, PreadBytes / (1024 * 1024)]),
        io:format("  pread_wall_us:   ~p~n", [PreadUs]),

        %% --- Phase D: mmap-mode walk -----------------------------
        io:format("~n--- Phase D: mmap-mode walk (sealed, +mmap +read_ahead) ---~n"),
        {MmapUs, MmapFrames, MmapBytes} =
            run_walk(Sealed, [read, raw, binary, read_ahead, mmap]),
        io:format("  walked_frames:   ~p~n", [MmapFrames]),
        io:format("  walked_bytes:    ~p (~.1f MiB)~n",
                  [MmapBytes, MmapBytes / (1024 * 1024)]),
        io:format("  mmap_wall_us:    ~p~n", [MmapUs]),

        %% --- Decision -------------------------------------------
        Share = case ScanUs of
                    0 -> 0.0;
                    _ -> (PreadUs / ScanUs) * 100
                end,
        MmapDelta = case PreadUs of
                        0 -> 0.0;
                        _ -> ((PreadUs - MmapUs) / PreadUs) * 100
                    end,
        MmapShareOfScan = case ScanUs of
                              0 -> 0.0;
                              _ -> ((PreadUs - MmapUs) / ScanUs) * 100
                          end,
        io:format("~n--- Summary ---~n"),
        io:format("~-38s ~10s ~10s~n",
                  ["metric", "value", "gate"]),
        io:format("~-38s ~10s ~10s~n",
                  [string:copies("-", 38), string:copies("-", 10),
                   string:copies("-", 10)]),
        Gate1 = pf_gate(Share),
        io:format("~-38s ~9.2f %% ~10s~n",
                  ["pread share of scan time (upper bound)",
                   Share, Gate1]),
        Gate2 = pf_gate(MmapDelta),
        io:format("~-38s ~9.2f %% ~10s~n",
                  ["mmap improvement over pread (delta)",
                   MmapDelta, Gate2]),
        Gate3 = pf_gate(MmapShareOfScan),
        io:format("~-38s ~9.2f %% ~10s~n",
                  ["mmap projected save vs scan time",
                   MmapShareOfScan, Gate3]),

        io:format("~n--- Decision ---~n"),
        case MmapShareOfScan >= 5.0 of
            true ->
                io:format(
                    "mmap improves wall-clock by >=5%% of full scan "
                    "time on this workload. Proceed with the "
                    "implementation PR.~n"
                );
            false ->
                case Share >= 5.0 of
                    true ->
                        io:format(
                            "pread share of scan time is >=5%% but "
                            "measured mmap delta does not realise "
                            ">=5%% of the scan budget. Defer; the "
                            "syscall savings do not survive the rest "
                            "of the reader hot path.~n"
                        );
                    false ->
                        io:format(
                            "pread share of scan time is <5%%. Even "
                            "a free pread cannot meet the gate; the "
                            "remaining time is decode/CRC/term work "
                            "that mmap does not touch. Defer.~n"
                        )
                end
        end,
        erlang:halt(0)
    after
        rmrf(Dir)
    end.

%% =============================================================================
%% Phase A — build the sealed WAL
%% =============================================================================

build_wal(Cfg) ->
    Frames     = maps:get(frames, Cfg),
    BatchBytes = maps:get(batch_bytes, Cfg),
    SegBytes   = maps:get(segment_bytes, Cfg),
    Dir = mktemp_dir(),
    Opts = #{
        dir                    => Dir,
        origin                 =>
            <<1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16>>,
        fsync_mode             => batched,
        batched_fsync_bytes    => 32 * 1024 * 1024,
        batched_fsync_interval => 60000,
        max_segment_bytes      => SegBytes,
        max_batch_bytes        => max(4 * 1024 * 1024, BatchBytes * 2),
        max_total_wal_size     => 10 * 1024 * 1024 * 1024
    },
    io:format("~n--- Phase A: build sealed WAL ---~n"),
    io:format("  dir: ~s~n", [Dir]),
    {ok, Pid} = bondy_oplog_wal:start_link(<<"mmap-bench">>, Opts),
    try
        Hlc = bondy_oplog_hlc:new(),
        run_appends(Pid, Hlc, BatchBytes, Frames),
        ok = bondy_oplog_wal:sync(Pid)
    after
        catch bondy_oplog_wal:close(Pid)
    end,
    Dir.

run_appends(_Pid, _Hlc, _BatchBytes, 0) -> ok;
run_appends(Pid, Hlc, BatchBytes, N) ->
    append_one(Pid, Hlc, BatchBytes),
    run_appends(Pid, Hlc, BatchBytes, N - 1).

append_one(Pid, Hlc, BatchBytes) ->
    Now = bondy_oplog_hlc:now(Hlc),
    PayloadBytes = max(64, BatchBytes - 256),
    Payload = crypto:strong_rand_bytes(PayloadBytes),
    Key = bondy_oplog_event:key(
        Now,
        <<1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16>>,
        0
    ),
    Event = bondy_oplog_event:new(Key, {op, Payload}, undefined),
    {ok, _Hlc2, _Pos} = bondy_oplog_wal:append(Pid, Event),
    ok.

%% =============================================================================
%% Phase B — full reader scan
%% =============================================================================

run_reader(Dir, _Cfg) ->
    {ok, Pid} = bondy_oplog_wal:start_link(
        <<"mmap-bench">>,
        #{dir => Dir,
          origin => <<1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16>>}
    ),
    try
        {ok, Iter0} = bondy_oplog_wal_reader:open(Pid, beginning),
        T0 = erlang:monotonic_time(microsecond),
        {Frames, EventCount} = drain(Iter0, 0, 0),
        T1 = erlang:monotonic_time(microsecond),
        %% We can't get per-frame on-disk byte counts from the reader
        %% API; report event-count as the workload signal instead.
        {T1 - T0, Frames, EventCount}
    after
        catch bondy_oplog_wal:close(Pid)
    end.

drain(Iter, Frames, Events) ->
    case bondy_oplog_wal_reader:next(Iter) of
        {ok, Batch, _Hlcs, _Pos, NextIter} ->
            drain(NextIter, Frames + 1, Events + length(Batch));
        end_of_log ->
            ok = bondy_oplog_wal_reader:close(Iter),
            {Frames, Events};
        {error, Reason} ->
            ok = bondy_oplog_wal_reader:close(Iter),
            erlang:error({reader_drain_failed, Reason})
    end.

%% =============================================================================
%% Phase C — pread-only walk (no decode, no CRC, no term parse)
%% =============================================================================

run_walk(Sealed, OpenOpts) ->
    T0 = erlang:monotonic_time(microsecond),
    {Frames, Bytes} = lists:foldl(
        fun(Path, {FAcc, BAcc}) ->
            {F, B} = pread_walk_one(Path, OpenOpts),
            {FAcc + F, BAcc + B}
        end,
        {0, 0},
        Sealed
    ),
    T1 = erlang:monotonic_time(microsecond),
    {T1 - T0, Frames, Bytes}.

pread_walk_one(Path, OpenOpts) ->
    {ok, Fd} = prim_file:open(Path, OpenOpts),
    try
        Size = filelib:file_size(Path),
        pread_walk_loop(Fd, ?SEG_HEADER_BYTES, Size, 0, 0)
    after
        _ = prim_file:close(Fd)
    end.

pf_gate(N) when N >= 5.0 -> "PASS";
pf_gate(_) -> "FAIL".

pread_walk_loop(_Fd, Off, Size, Frames, Bytes) when Off >= Size ->
    {Frames, Bytes};
pread_walk_loop(Fd, Off, Size, Frames, Bytes) ->
    case prim_file:pread(Fd, Off, ?FRAME_HEADER_BYTES) of
        {ok, <<_Magic:32, FrameLen:32/unsigned-big,
               _Crc:32, _VerFlags:32>>} ->
            BodyLen = FrameLen - ?FRAME_HEADER_BYTES,
            {ok, _Body} = prim_file:pread(
                Fd, Off + ?FRAME_HEADER_BYTES, BodyLen
            ),
            pread_walk_loop(
                Fd, Off + FrameLen, Size,
                Frames + 1, Bytes + FrameLen
            );
        eof ->
            {Frames, Bytes};
        {ok, Short} when byte_size(Short) < ?FRAME_HEADER_BYTES ->
            {Frames, Bytes}
    end.

%% =============================================================================
%% Helpers
%% =============================================================================

list_sealed_segments(Dir) ->
    InstanceDir = filename:join(Dir, <<"mmap-bench">>),
    {ok, Terms} = file:consult(
        filename:join(InstanceDir, "manifest")
    ),
    Current = proplists:get_value(current_segment, Terms),
    Live = proplists:get_value(live_segments, Terms, []),
    Ids = [Id || {Id, _} <- Live, Id =/= Current],
    [filename:join(InstanceDir, bondy_oplog_wal_segment:filename(Id))
     || Id <- Ids].

total_bytes(Paths) ->
    lists:sum([filelib:file_size(P) || P <- Paths]).

parse_args(Args) ->
    parse_args(Args, #{frames => 2000,
                       batch_bytes => 16 * 1024,
                       segment_bytes => 8 * 1024 * 1024}).

parse_args([], Cfg) -> Cfg;
parse_args(["--frames", S | Rest], Cfg) ->
    parse_args(Rest, Cfg#{frames := list_to_integer(S)});
parse_args(["--batch", S | Rest], Cfg) ->
    parse_args(Rest, Cfg#{batch_bytes := list_to_integer(S)});
parse_args(["--segment-bytes", S | Rest], Cfg) ->
    parse_args(Rest, Cfg#{segment_bytes := list_to_integer(S)});
parse_args([Other | _], _Cfg) ->
    io:format(standard_error, "wal_mmap_bench: unknown arg ~p~n",
              [Other]),
    erlang:halt(2).

setup_paths() ->
    Root = filename:dirname(filename:dirname(escript:script_name())),
    EbinPattern = filename:join(
        [Root, "_build", "test", "lib", "*", "ebin"]
    ),
    Ebins = filelib:wildcard(EbinPattern),
    case Ebins of
        [] ->
            io:format(
                standard_error,
                "wal_mmap_bench: no test ebin dirs under ~s — run "
                "`rebar3 as test compile` first~n",
                [EbinPattern]
            ),
            erlang:halt(3);
        _ ->
            code:add_pathsa(Ebins)
    end,
    ok.

ensure_apps([]) -> ok;
ensure_apps([App | Rest]) ->
    case application:ensure_all_started(App) of
        {ok, _} -> ensure_apps(Rest);
        {error, Reason} ->
            io:format(standard_error,
                      "wal_mmap_bench: failed to start ~p: ~p~n",
                      [App, Reason]),
            erlang:halt(4)
    end.

mktemp_dir() ->
    Base = filename:join(
        ["/tmp",
         io_lib:format("wal_mmap_bench_~p_~p",
                       [erlang:system_time(microsecond),
                        erlang:unique_integer([positive])])]
    ),
    Dir = lists:flatten(Base),
    ok = filelib:ensure_path(Dir),
    Dir.

rmrf(Dir) ->
    _ = file:del_dir_r(Dir),
    ok.
