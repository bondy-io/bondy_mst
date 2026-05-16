#!/usr/bin/env escript
%% -*- erlang -*-
%%! +sbtu +A0 -hidden -noinput
%%
%% CRC-share profiling for the WAL writer hot path.
%%
%% Measures CRC32's share of total writer time across representative
%% configurations, gating the CRC32C upgrade decision recorded in
%% _design/latest/WAL_DESIGN_V2.md (the CRC32C option ships only if
%% the projected gain is ≥ 5 % of writer throughput).
%%
%% Phase A — isolated CRC32 throughput on this hardware (erlang:crc32/1
%% on random buffers of representative sizes).
%%
%% Phase B — end-to-end writer throughput in three configs:
%%   * plain         — no body codec
%%   * compressed    — zlib body compression
%%   * encrypted     — compressed + AES-256-GCM
%% For each config, divides isolated CRC cost into the realised cost
%% per frame to get CRC's share. An idealised CRC32C accelerator that
%% is 2× faster than zlib CRC32 (a generous upper bound on x86 with
%% SSE 4.2 / on ARMv8 with crc32cb) would save half of that share —
%% which is the figure compared against the 5 % gate.
%%
%% Usage:
%%   scripts/wal_crc_bench.escript
%%   scripts/wal_crc_bench.escript --batch 65536 --frames 2000

main(Args) ->
    {BatchBytes, NumFrames} = parse_args(Args),
    ok = setup_paths(),
    ok = ensure_apps([crypto, telemetry]),

    io:format("~n=== WAL CRC-share profiling ===~n"),
    io:format("hardware: ~s ~s   schedulers: ~p   "
              "batch_bytes: ~p   frames: ~p~n",
              [erlang:system_info(system_architecture),
               erlang:system_info(otp_release),
               erlang:system_info(schedulers_online),
               BatchBytes, NumFrames]),

    %% --- Phase A: isolated CRC32 throughput -----------------------
    io:format("~n--- Phase A: isolated erlang:crc32/1 throughput ---~n"),
    Sizes = [4096, 65536, 262144, BatchBytes],
    UniqSizes = lists:usort(Sizes),
    CrcResults = [crc_bench(Sz) || Sz <- UniqSizes],
    [io:format("  size=~p B  -> ~.2f MB/s  (~.3f us/buf, ~p iters)~n",
               [Sz, MBps, Us, Iters])
     || {Sz, Us, MBps, Iters} <- CrcResults],

    %% --- Phase B: end-to-end writer throughput --------------------
    io:format("~n--- Phase B: end-to-end writer throughput ---~n"),
    Configs = [
        {plain,      #{}},
        {compressed, #{body_compression => zlib,
                       body_compression_min_bytes => 1}},
        {encrypted,  #{body_compression => zlib,
                       body_compression_min_bytes => 1,
                       body_encryption  =>
                           {enabled, bondy_oplog_wal_codec_test}}}
    ],
    {_, _, CrcMBpsForBatch, _} =
        lists:keyfind(BatchBytes, 1, CrcResults),
    CrcUsPerBatch = (BatchBytes / (CrcMBpsForBatch * 1024 * 1024)) * 1.0e6,
    io:format("  isolated CRC32 cost for ~p-byte body: ~.3f us~n",
              [BatchBytes, CrcUsPerBatch]),

    Rows = [run_config(Cfg, BatchBytes, NumFrames) || Cfg <- Configs],

    io:format("~n--- Summary (per ~p-byte batch frame) ---~n",
              [BatchBytes]),
    io:format("~-12s  ~10s  ~10s  ~10s  ~12s  ~12s~n",
              ["config", "write_us", "throughput",
               "crc_share", "ideal_save", "gate"]),
    io:format("~-12s  ~10s  ~10s  ~10s  ~12s  ~12s~n",
              [string:copies("-", 12), string:copies("-", 10),
               string:copies("-", 10), string:copies("-", 10),
               string:copies("-", 12), string:copies("-", 12)]),
    lists:foreach(
        fun({Name, FrameUs, MBps}) ->
            Share = (CrcUsPerBatch / FrameUs) * 100,
            IdealSave = Share / 2,
            Gate = case IdealSave >= 5.0 of
                       true  -> "PASS";
                       false -> "FAIL"
                   end,
            io:format(
                "~-12s  ~10.2f  ~6.1f MB/s   ~7.2f %%   ~7.2f %%   ~12s~n",
                [atom_to_list(Name), FrameUs, MBps, Share, IdealSave, Gate]
            )
        end,
        Rows
    ),

    Decisive = lists:all(
        fun({_, FrameUs, _}) -> (CrcUsPerBatch / FrameUs) * 100 / 2 < 5.0 end,
        Rows
    ),
    io:format("~n--- Decision ---~n"),
    case Decisive of
        true ->
            io:format(
                "CRC32C upgrade does NOT meet the 5%% gate in any "
                "tested configuration. Recommendation: defer "
                "indefinitely; keep the on-disk seam (Flags bit 2 + "
                "compute_crc/2 dispatch) so a future PR can ship a "
                "NIF-backed implementation if hardware/workload mix "
                "changes.~n"
            ),
            erlang:halt(0);
        false ->
            io:format(
                "At least one configuration shows a >= 5%% projected "
                "win from CRC32C. Proceed with the implementation PR "
                "(wire compute_crc(crc32c, _) into the frame module, "
                "add Flags bit 2 plumbing, ship a CRC32C provider).~n"
            ),
            erlang:halt(0)
    end.

%% =============================================================================
%% Phase A — isolated CRC32 throughput
%% =============================================================================

crc_bench(Size) ->
    Buf = crypto:strong_rand_bytes(Size),
    %% Warm up — first call sometimes pays JIT / icache costs.
    _ = erlang:crc32(Buf),
    {Iters, ElapsedUs} = run_for_budget(
        fun() -> erlang:crc32(Buf) end, 100000
    ),
    UsPerIter = ElapsedUs / Iters,
    MBps = (Size * Iters / (ElapsedUs / 1.0e6)) / (1024 * 1024),
    {Size, UsPerIter, MBps, Iters}.

%% Runs `Fun` repeatedly until at least `BudgetUs` microseconds have
%% elapsed. Returns `{iterations, elapsed_us}`.
run_for_budget(Fun, BudgetUs) ->
    T0 = erlang:monotonic_time(microsecond),
    run_for_budget_loop(Fun, T0 + BudgetUs, 0).

run_for_budget_loop(Fun, Deadline, N) ->
    Fun(),
    case N rem 256 of
        0 ->
            Now = erlang:monotonic_time(microsecond),
            case Now >= Deadline of
                true ->
                    Start = Deadline - (Deadline - Now) - 100000,
                    {N + 1, erlang:monotonic_time(microsecond) - Start};
                false ->
                    run_for_budget_loop(Fun, Deadline, N + 1)
            end;
        _ ->
            run_for_budget_loop(Fun, Deadline, N + 1)
    end.

%% =============================================================================
%% Phase B — end-to-end writer benchmark
%% =============================================================================

run_config({Name, Extra}, BatchBytes, NumFrames) ->
    Dir = mktemp_dir(),
    BaseOpts = #{
        dir                     => Dir,
        origin                  => <<1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16>>,
        fsync_mode              => batched,
        batched_fsync_bytes     => 64 * 1024 * 1024,
        batched_fsync_interval  => 60000,
        max_segment_bytes       => 256 * 1024 * 1024,
        max_batch_bytes         => 4 * 1024 * 1024
    },
    Opts = maps:merge(BaseOpts, Extra),
    {ok, Pid} = bondy_oplog_wal:start_link(<<"crc-bench">>, Opts),
    try
        %% Body shape: a list of bondy_oplog_event records whose payload
        %% sums to roughly BatchBytes once term-encoded. We use one
        %% record per batch with a large binary payload — this keeps
        %% per-frame term_to_binary cost low so the measurement isolates
        %% the I/O / codec / CRC path.
        Hlc = bondy_oplog_hlc:new(),
        warm_up(Pid, Hlc, BatchBytes),
        T0 = erlang:monotonic_time(microsecond),
        run_appends(Pid, Hlc, BatchBytes, NumFrames),
        ok = bondy_oplog_wal:sync(Pid),
        T1 = erlang:monotonic_time(microsecond),
        ElapsedUs = T1 - T0,
        FrameUs = ElapsedUs / NumFrames,
        BytesTotal = BatchBytes * NumFrames,
        MBps = (BytesTotal / (ElapsedUs / 1.0e6)) / (1024 * 1024),
        {Name, FrameUs, MBps}
    after
        catch bondy_oplog_wal:close(Pid),
        rmrf(Dir)
    end.

warm_up(Pid, Hlc, BatchBytes) ->
    %% A handful of warm-up appends so file-system, JIT, and codec
    %% bookkeeping settle before the timed window opens.
    lists:foreach(
        fun(_) -> append_one(Pid, Hlc, BatchBytes) end,
        lists:seq(1, 8)
    ),
    ok.

run_appends(_Pid, _Hlc, _BatchBytes, 0) -> ok;
run_appends(Pid, Hlc, BatchBytes, N) ->
    append_one(Pid, Hlc, BatchBytes),
    run_appends(Pid, Hlc, BatchBytes, N - 1).

append_one(Pid, Hlc, BatchBytes) ->
    Now = bondy_oplog_hlc:now(Hlc),
    %% A binary payload roughly BatchBytes large; subtract a small
    %% allowance for the term_to_binary envelope around the record.
    PayloadBytes = max(64, BatchBytes - 256),
    Payload = crypto:strong_rand_bytes(PayloadBytes),
    Key = bondy_oplog_event:key(Now,
              <<1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16>>, 0),
    Event = bondy_oplog_event:new(Key, {op, Payload}, undefined),
    {ok, _Hlc2, _Pos} = bondy_oplog_wal:append(Pid, Event),
    ok.

%% =============================================================================
%% Helpers
%% =============================================================================

parse_args(Args) ->
    parse_args(Args, 65536, 1000).
parse_args([], B, N) -> {B, N};
parse_args(["--batch", S | Rest], _, N) -> parse_args(Rest, list_to_integer(S), N);
parse_args(["--frames", S | Rest], B, _) -> parse_args(Rest, B, list_to_integer(S));
parse_args([Other | _], _, _) ->
    io:format(standard_error, "wal_crc_bench: unknown arg ~p~n", [Other]),
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
                "wal_crc_bench: no test ebin dirs under ~s — run "
                "`rebar3 as test compile` first~n",
                [EbinPattern]
            ),
            erlang:halt(3);
        _ ->
            code:add_pathsa(Ebins)
    end,
    TestBeam = filename:join(
        [Root, "_build", "test", "lib", "bondy_mst", "test"]
    ),
    code:add_patha(TestBeam),
    ok.

ensure_apps([]) -> ok;
ensure_apps([App | Rest]) ->
    case application:ensure_all_started(App) of
        {ok, _} -> ensure_apps(Rest);
        {error, Reason} ->
            io:format(standard_error,
                      "wal_crc_bench: failed to start ~p: ~p~n",
                      [App, Reason]),
            erlang:halt(4)
    end.

mktemp_dir() ->
    Base = filename:join(
        ["/tmp",
         io_lib:format("wal_crc_bench_~p_~p",
                       [erlang:system_time(microsecond),
                        erlang:unique_integer([positive])])]
    ),
    Dir = lists:flatten(Base),
    ok = filelib:ensure_path(Dir),
    Dir.

rmrf(Dir) ->
    _ = file:del_dir_r(Dir),
    ok.
