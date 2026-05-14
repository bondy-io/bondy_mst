%% =============================================================================
%% PropEr properties for the bondy_oplog_wal stack.
%%
%% This file is the single home for WAL property tests. Each property
%% carries the P-number it implements from `_design/WAL_DESIGN.md` §16.
%%
%% Run with:
%%   rebar3 as test eunit --module=bondy_oplog_wal_proper_test
%% or:
%%   proper:quickcheck(bondy_oplog_wal_proper_test:prop_xxx(),
%%                     [{numtests, 1000}]).
%%
%% Property index (P_BatchAtomicity, P_WalFull, P1..P15 land here as
%% the implementation lands the behaviour each verifies):
%%
%%   Frame layer:
%%     - prop_frame_roundtrip/0          (P1 framing slice)
%%     - prop_frame_bit_flip_detection/0 (P3 framing slice)
%%
%%   Single-event writer:
%%     - prop_wal_single_event_roundtrip/0  (P1 single-event slice)
%%     - prop_wal_hlc_monotonicity/0        (P2 single-event slice)
%%
%%   Reader / iterator:
%%     - prop_wal_roundtrip/0            (P1 end-to-end via reader)
%%
%%   Sparse index `.qidx`:
%%     - prop_index_consistency/0        (P6)
%%
%%   Recovery:
%%     - prop_truncation_safety/0        (P5)
%%     - prop_manifest_atomicity/0       (P7)
%%     - prop_consumer_offset_clamping/0 (P10)
%%
%%   Batched fsync + durability:
%%     - prop_await_durable_correctness/0
%%
%%   Atomic batch frames (TODO):
%%     - prop_batch_atomicity/0          (P_BatchAtomicity)
%%
%%   Retention (TODO):
%%     - prop_retention_safety/0         (P9)
%%
%%   Backpressure (TODO):
%%     - prop_wal_full/0                 (P_WalFull)
%%
%%   Stateful + fault injection (TODO):
%%     - prop_concurrent_reader_safety/0 (P11)
%%     - prop_multiproc_convergence/0    (P12)
%%     - prop_partial_write/0            (P13)
%%     - prop_failed_fsync/0             (P14)
%%     - prop_rename_failure/0           (P15)
%%     - prop_rotation_atomicity/0       (P8)
%%     - prop_bit_flip_magic/0           (P4)
%%
%% =============================================================================

-module(bondy_oplog_wal_proper_test).

%% PropEr defines `LET` and friends; include it before EUnit so EUnit's
%% `LET` doesn't shadow PropEr's.
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("bondy_oplog.hrl").
-include("bondy_oplog_wal.hrl").

-define(HEADER, ?BONDY_OPLOG_WAL_FRAME_HEADER_BYTES).
-define(SEG_HEADER, ?BONDY_OPLOG_WAL_SEGMENT_HEADER_BYTES).
-define(KNOWN_FLAGS, ?BONDY_OPLOG_WAL_FRAME_KNOWN_FLAGS_V1).
-define(DEFAULT_NUMTESTS, 200).
-define(WAL_NUMTESTS, 50).

-export([prop_frame_roundtrip/0]).
-export([prop_frame_bit_flip_detection/0]).
-export([prop_wal_single_event_roundtrip/0]).
-export([prop_wal_hlc_monotonicity/0]).
-export([prop_wal_roundtrip/0]).
-export([prop_index_consistency/0]).
-export([prop_truncation_safety/0]).
-export([prop_manifest_atomicity/0]).
-export([prop_consumer_offset_clamping/0]).
-export([prop_await_durable_correctness/0]).

%% =============================================================================
%% Frame-layer properties
%% =============================================================================

%% P1 (framing slice).
%% Every body that we can encode decodes back to itself, with metadata
%% preserved. Flags are restricted to the v1 known mask — non-zero bits
%% outside the mask are intentionally rejected and tested elsewhere.
prop_frame_roundtrip() ->
    ?FORALL(
        {Body, Flags},
        {binary(), known_flags()},
        begin
            Frame = iolist_to_binary(
                bondy_oplog_wal_frame:encode(Body, [{flags, Flags}])
            ),
            case bondy_oplog_wal_frame:decode(Frame) of
                {ok, Decoded, #{flags := F}} ->
                    Decoded =:= Body andalso F =:= Flags;
                _ ->
                    false
            end
        end
    ).

%% P3 (framing slice).
%% Flipping any single bit inside the CRC-covered region of an encoded
%% frame produces a decode error (CRC mismatch / length / truncated /
%% unknown_flag / unsupported_version). A flip inside `Magic` itself is
%% covered by a separate sniff-priority test, because `bad_magic` is a
%% deliberately distinct error type. See P4 (todo).
prop_frame_bit_flip_detection() ->
    ?FORALL(
        {Body, BitIdx},
        ?LET(B, non_empty(binary()),
             {B, choose(32, (?HEADER + byte_size(B)) * 8 - 1)}),
        begin
            Frame = iolist_to_binary(bondy_oplog_wal_frame:encode(Body)),
            Corrupt = flip_bit(Frame, BitIdx),
            case bondy_oplog_wal_frame:decode(Corrupt) of
                {ok, _, _} -> false;
                {error, _} -> true
            end
        end
    ).

%% v1 only accepts flag bits inside the known mask. Today that's zero,
%% but expressing this as a generator means the property keeps testing
%% the full space when future versions widen the mask.
known_flags() ->
    case ?KNOWN_FLAGS of
        0 -> 0;
        Mask -> choose(0, Mask)
    end.

%% =============================================================================
%% WAL writer properties (single-event path)
%% =============================================================================

%% P1 (single-event slice).
%% Append N events through the WAL writer; raw-scan the segment files
%% and verify the recovered event list equals the appended sequence
%% with HLCs preserved. Rotation is exercised by choosing
%% `max_segment_bytes` small enough that ~1/3 of generated events
%% trigger a rotation.
prop_wal_single_event_roundtrip() ->
    ?FORALL(
        N,
        choose(1, 50),
        with_wal_dir(fun(Dir) ->
            HLC = bondy_oplog_hlc:new(),
            Events = generate_events(HLC, N),
            %% Pick a cap that yields ~3 events per segment on average.
            MaxBytes = ?SEG_HEADER + estimated_frame_size() * 3,
            {ok, Pid} = bondy_oplog_wal:start_link(
                instance_id(),
                #{
                    dir => Dir,
                    origin => origin(),
                    max_segment_bytes => MaxBytes
                }
            ),
            Results = [bondy_oplog_wal:append(Pid, E) || E <- Events],
            Info = bondy_oplog_wal:info(Pid),
            ok = bondy_oplog_wal:close(Pid),
            Recovered = scan_all_segments(
                Dir, instance_id(), maps:get(current_segment, Info)
            ),
            length(Results) =:= N
                andalso lists:all(fun({ok, _, _}) -> true; (_) -> false end,
                                  Results)
                andalso Recovered =:= Events
        end)
    ).

%% P2 (single-event slice).
%% Across all appended events in append order, the HLCs returned by
%% `append/2` are strictly increasing.
prop_wal_hlc_monotonicity() ->
    ?FORALL(
        N,
        choose(1, 50),
        with_wal_dir(fun(Dir) ->
            HLC = bondy_oplog_hlc:new(),
            Events = generate_events(HLC, N),
            {ok, Pid} = bondy_oplog_wal:start_link(
                instance_id(),
                #{dir => Dir, origin => origin()}
            ),
            Hlcs = [
                begin
                    {ok, H, _} = bondy_oplog_wal:append(Pid, E),
                    H
                end
             || E <- Events
            ],
            ok = bondy_oplog_wal:close(Pid),
            is_strictly_increasing(Hlcs)
        end)
    ).

%% =============================================================================
%% Reader / iterator end-to-end property
%% =============================================================================

%% P1 (end-to-end via reader).
%% Append N events through the WAL writer, then open a bounded reader
%% at `beginning` and drain it. Recovered event list must equal the
%% appended list in order. Rotation is exercised by choosing
%% `max_segment_bytes` small enough that ~1/3 of generated events
%% trigger a rotation. The reader walks across segments transparently.
prop_wal_roundtrip() ->
    ?FORALL(
        N,
        choose(1, 50),
        with_wal_dir(fun(Dir) ->
            HLC = bondy_oplog_hlc:new(),
            Events = generate_events(HLC, N),
            MaxBytes = ?SEG_HEADER + estimated_frame_size() * 3,
            {ok, Pid} = bondy_oplog_wal:start_link(
                instance_id(),
                #{
                    dir => Dir,
                    origin => origin(),
                    max_segment_bytes => MaxBytes
                }
            ),
            [{ok, _, _} = bondy_oplog_wal:append(Pid, E) || E <- Events],
            {ok, Iter} = bondy_oplog_wal_reader:open(Pid, beginning),
            Recovered = drain_reader(Iter, []),
            ok = bondy_oplog_wal:close(Pid),
            Recovered =:= Events
        end)
    ).

%% =============================================================================
%% Sparse-index consistency property (P6)
%% =============================================================================

%% P6.
%% For every entry the writer emits into a `.qidx`, the on-disk frame at
%% the entry's `ByteOffset` (within the segment file) must:
%%
%% - parse as a valid frame (Magic, CRC, version, flags all valid),
%% - decode to a non-empty event list,
%% - have its first event's HLC equal to the entry's HLC.
%%
%% This property checks both the sealed-segment path (`.qidx` flushed on
%% rotation) and the head-segment path (`.qidx` flushed on `close/1`).
%% Workload: random N appended events with a rotation-friendly cap so
%% multiple segments accumulate during the trial.
prop_index_consistency() ->
    ?FORALL(
        N,
        choose(1, 50),
        with_wal_dir(fun(Dir) ->
            HLC = bondy_oplog_hlc:new(),
            Events = generate_events(HLC, N),
            MaxBytes = ?SEG_HEADER + estimated_frame_size() * 3,
            %% Tighten the index interval so the workload reliably
            %% produces more than just the first-frame entry per
            %% segment. Default 64 KiB would gate most trials to the
            %% single mandatory entry.
            {ok, Pid} = bondy_oplog_wal:start_link(
                instance_id(),
                #{
                    dir => Dir,
                    origin => origin(),
                    max_segment_bytes => MaxBytes,
                    idx_interval_bytes => 200
                }
            ),
            Info0 = bondy_oplog_wal:info(Pid),
            [bondy_oplog_wal:append(Pid, E) || E <- Events],
            HeadSeg = maps:get(current_segment, bondy_oplog_wal:info(Pid)),
            _ = Info0,
            ok = bondy_oplog_wal:close(Pid),
            check_index_consistency(Dir, instance_id(), HeadSeg)
        end)
    ).

%% For every segment from 0..HeadSeg, load its `.qidx` (which exists for
%% all of them after `close/1`) and verify each entry points at a real
%% frame whose first event's HLC equals the indexed HLC.
check_index_consistency(Dir, InstanceId, HeadSeg) ->
    lists:all(
        fun(SegId) -> check_segment_index(Dir, InstanceId, SegId) end,
        lists:seq(0, HeadSeg)
    ).

check_segment_index(Dir, InstanceId, SegId) ->
    IdxPath = filename:join(
        [Dir, InstanceId, bondy_oplog_wal_idx:filename(SegId)]
    ),
    case bondy_oplog_wal_idx:read_file(IdxPath) of
        {ok, []} ->
            %% An empty index is only legal for an empty segment (e.g.,
            %% a head segment created by rotation but never appended
            %% to). Verify the segment has no frames past its header.
            seg_is_empty(Dir, InstanceId, SegId);
        {ok, Entries} ->
            lists:all(
                fun(E) -> check_entry(Dir, InstanceId, SegId, E) end,
                Entries
            );
        {error, enoent} ->
            %% A missing `.qidx` is allowed for an empty head segment
            %% (the writer skips the flush in that case). Verify the
            %% segment is indeed empty.
            seg_is_empty(Dir, InstanceId, SegId);
        {error, _} ->
            false
    end.

check_entry(Dir, InstanceId, SegId, {Hlc, Offset}) ->
    SegPath = filename:join(
        [Dir, InstanceId, bondy_oplog_wal_segment:filename(SegId)]
    ),
    case file:read_file(SegPath) of
        {ok, Bin} when byte_size(Bin) >= Offset + ?HEADER ->
            <<_:Offset/binary, Header:?HEADER/binary, _/binary>> = Bin,
            case bondy_oplog_wal_frame:decode_header(Header) of
                {ok, #{frame_len := FrameLen}} when
                    byte_size(Bin) >= Offset + FrameLen
                ->
                    <<_:Offset/binary, Frame:FrameLen/binary, _/binary>> = Bin,
                    case bondy_oplog_wal_frame:decode(Frame) of
                        {ok, Body, _Meta} ->
                            case binary_to_term(Body, [safe]) of
                                [Event | _] ->
                                    Key = bondy_oplog_event:key(Event),
                                    bondy_oplog_event:key_hlc(Key) =:= Hlc;
                                _ ->
                                    false
                            end;
                        _ ->
                            false
                    end;
                _ ->
                    false
            end;
        _ ->
            false
    end.

seg_is_empty(Dir, InstanceId, SegId) ->
    SegPath = filename:join(
        [Dir, InstanceId, bondy_oplog_wal_segment:filename(SegId)]
    ),
    case file:read_file_info(SegPath) of
        {ok, FI} ->
            element(2, FI) =:= ?SEG_HEADER;
        _ ->
            false
    end.

%% =============================================================================
%% Recovery properties (P5, P7, P10)
%% =============================================================================

%% P5 (truncation safety).
%% After appending N events and truncating the head segment's `.qdata`
%% at an arbitrary byte offset, reopening the WAL via recovery must
%% expose only frames whose end-offset ≤ TruncOffset, in append order,
%% and the file size after recovery must equal the writer's
%% `head_offset` (i.e., no partial frame is left dangling).
prop_truncation_safety() ->
    ?FORALL(
        {N, ChopBytes},
        ?LET(NN, choose(2, 20), {NN, choose(0, max(1, NN * 30))}),
        with_wal_dir(fun(Dir) ->
            HLC = bondy_oplog_hlc:new(),
            Events = generate_events(HLC, N),
            Opts = #{dir => Dir, origin => origin()},
            {ok, P1} = bondy_oplog_wal:start_link(instance_id(), Opts),
            [bondy_oplog_wal:append(P1, E) || E <- Events],
            ok = bondy_oplog_wal:close(P1),
            SegPath = filename:join(
                [Dir, instance_id(),
                 bondy_oplog_wal_segment:filename(0)]
            ),
            {ok, Size} = file_size(SegPath),
            %% Trim ChopBytes off the tail (but never below the segment
            %% header — the header isn't tested by P5).
            TruncTo = max(?SEG_HEADER, Size - ChopBytes),
            truncate_file(SegPath, TruncTo),
            {ok, P2} = bondy_oplog_wal:start_link(instance_id(), Opts),
            Read = read_all_events(P2),
            Info = bondy_oplog_wal:info(P2),
            {ok, NewSize} = file_size(SegPath),
            ok = bondy_oplog_wal:close(P2),
            %% Recovered events are a strict prefix of the original.
            IsPrefix =
                Read =:= lists:sublist(Events, length(Read)),
            %% File size equals head_offset (no dangling bytes).
            FileSizeMatches =
                NewSize =:= maps:get(head_offset, Info),
            %% All recovered frames end at or before the truncation
            %% point — i.e., recovery never resurrects bytes from
            %% beyond the trim.
            EndsBeforeTrunc =
                maps:get(head_offset, Info) =< TruncTo,
            IsPrefix andalso FileSizeMatches andalso EndsBeforeTrunc
        end)
    ).

%% P7 (manifest atomicity, observability slice).
%% The full crash-trace property (kill writer mid-rename with a partial
%% tmp file on disk) needs a fault-injection harness that is still
%% TODO. Here we exercise the in-process atomicity contract: a
%% malformed `manifest.tmp` left on disk after the rename has already
%% succeeded must not be observed by recovery — the orphan cleanup
%% removes it and the live `manifest` is what's read.
prop_manifest_atomicity() ->
    ?FORALL(
        N,
        choose(1, 10),
        with_wal_dir(fun(Dir) ->
            HLC = bondy_oplog_hlc:new(),
            Events = generate_events(HLC, N),
            Opts = #{dir => Dir, origin => origin()},
            {ok, P1} = bondy_oplog_wal:start_link(instance_id(), Opts),
            [bondy_oplog_wal:append(P1, E) || E <- Events],
            ok = bondy_oplog_wal:close(P1),
            InstDir = filename:join(Dir, instance_id()),
            %% Seed a junk manifest.tmp — must not affect recovery.
            TmpPath = filename:join(
                InstDir, ?BONDY_OPLOG_WAL_MANIFEST_TMP_FILENAME
            ),
            ok = file:write_file(TmpPath, <<"garbage manifest tmp">>),
            {ok, P2} = bondy_oplog_wal:start_link(instance_id(), Opts),
            Read = read_all_events(P2),
            ok = bondy_oplog_wal:close(P2),
            %% Recovery must (1) succeed (gen_server up), (2) surface
            %% the original event sequence, and (3) clean the orphan
            %% manifest.tmp.
            Read =:= Events
                andalso not filelib:is_regular(TmpPath)
        end)
    ).

%% P10 (consumer-offset clamping).
%% For any pre-seeded `consumer.offset` content (random segment +
%% random offset, possibly past EOF or mid-frame), after recovery the
%% on-disk consumer.offset:
%% - has a committed_segment that is in `live_segments`;
%% - has a committed_frame_offset on a real frame boundary;
%% - has committed_frame_offset ≤ `head_offset` (for the head segment)
%%   or ≤ segment file size (for a sealed segment).
prop_consumer_offset_clamping() ->
    ?FORALL(
        {N, BadSeg, BadOff},
        ?LET(NN, choose(1, 15),
             {NN, choose(0, 99), choose(0, 1_000_000)}),
        with_wal_dir(fun(Dir) ->
            HLC = bondy_oplog_hlc:new(),
            Events = generate_events(HLC, N),
            Opts = #{dir => Dir, origin => origin()},
            {ok, P1} = bondy_oplog_wal:start_link(instance_id(), Opts),
            Positions = [
                begin
                    {ok, _, Pos} = bondy_oplog_wal:append(P1, E),
                    Pos
                end
             || E <- Events
            ],
            HeadOff = maps:get(
                head_offset, bondy_oplog_wal:info(P1)
            ),
            HeadSeg = maps:get(
                current_segment, bondy_oplog_wal:info(P1)
            ),
            ok = bondy_oplog_wal:close(P1),
            InstDir = filename:join(Dir, instance_id()),
            %% Seed an aggressively-bad consumer.offset by writing
            %% the file content directly. The `with_position/3` setter
            %% guards against offsets < segment header, but recovery
            %% must still cope with such values when they appear on
            %% disk (file written by an older buggy applier, manual
            %% edit, etc.).
            ok = seed_raw_consumer_offset(
                InstDir, max(0, BadSeg), max(0, BadOff)
            ),
            {ok, P2} = bondy_oplog_wal:start_link(
                instance_id(), Opts
            ),
            ok = bondy_oplog_wal:close(P2),
            {ok, Clamped} = bondy_oplog_wal_consumer_offset:read(InstDir),
            ClampedSeg =
                bondy_oplog_wal_consumer_offset:committed_segment(
                    Clamped
                ),
            ClampedOff =
                bondy_oplog_wal_consumer_offset:committed_frame_offset(
                    Clamped
                ),
            %% Clamped segment is live (post-clamp it must be ≤ head;
            %% since we never rotated, every event is in segment 0).
            InLive = ClampedSeg =:= HeadSeg,
            %% Clamped offset is ≤ head_offset of head segment.
            WithinBound = ClampedOff =< HeadOff,
            %% Clamped offset is at a frame boundary (one of the
            %% appended positions, the segment header, or head_offset).
            ValidBoundaries =
                [?SEG_HEADER, HeadOff |
                 [Off || {S, Off} <- Positions, S =:= HeadSeg]],
            AtBoundary = lists:member(ClampedOff, ValidBoundaries),
            InLive andalso WithinBound andalso AtBoundary
        end)
    ).

%% =============================================================================
%% Batched fsync + `await_durable/3` correctness
%% =============================================================================

%% For any sequence of N appends in `batched` mode, with size and time
%% triggers configured high enough that no fsync runs implicitly, the
%% following must hold:
%%
%% - Before any explicit `sync/1`, every per-append end position lies
%%   strictly above `durable_position/1`. `await_durable/3` with a
%%   zero timeout returns `{error, timeout}` for each.
%%
%% - After `sync/1`, `durable_position/1` equals the head, and
%%   `await_durable/3` with a zero timeout returns `ok` for every
%%   recorded end position.
%%
%% In other words: durability is reached at fsync boundaries and only
%% at fsync boundaries; `await_durable/3` reports the position's status
%% correctly with respect to the boundary.
prop_await_durable_correctness() ->
    ?FORALL(
        N,
        choose(1, 20),
        with_wal_dir(fun(Dir) ->
            HLC = bondy_oplog_hlc:new(),
            Events = generate_events(HLC, N),
            Opts = #{
                dir => Dir,
                origin => origin(),
                fsync_mode => batched,
                batched_fsync_interval => 60_000,
                batched_fsync_bytes => 100 * 1024 * 1024,
                max_segment_bytes => 100 * 1024 * 1024
            },
            {ok, Pid} = bondy_oplog_wal:start_link(instance_id(), Opts),
            EndPositions = lists:map(
                fun(E) ->
                    {ok, _, _} = bondy_oplog_wal:append(Pid, E),
                    Info = bondy_oplog_wal:info(Pid),
                    {maps:get(current_segment, Info),
                     maps:get(head_offset, Info)}
                end,
                Events
            ),
            BeforeSync = [
                {error, timeout} =:= bondy_oplog_wal:await_durable(
                    Pid, Pos, 0
                )
             || Pos <- EndPositions
            ],
            ok = bondy_oplog_wal:sync(Pid),
            LastPos = lists:last(EndPositions),
            DurableAfter = bondy_oplog_wal:durable_position(Pid),
            AfterSync = [
                ok =:= bondy_oplog_wal:await_durable(Pid, Pos, 0)
             || Pos <- EndPositions
            ],
            ok = bondy_oplog_wal:close(Pid),
            DurableAfter =:= LastPos
                andalso lists:all(fun(X) -> X end, BeforeSync)
                andalso lists:all(fun(X) -> X end, AfterSync)
        end)
    ).

%% =============================================================================
%% EUnit wrapper — runs each property with the configured numtests count
%% so the suite participates in CI under `rebar3 eunit`. The full 24h
%% fuzz job runs PropEr directly via `rebar3 proper`.
%% =============================================================================

properties_test_() ->
    {timeout, 600,
     fun() ->
        FrameOpts = [{to_file, user}, {numtests, ?DEFAULT_NUMTESTS}],
        WalOpts = [{to_file, user}, {numtests, ?WAL_NUMTESTS}],
        FrameProps = [
            prop_frame_roundtrip(),
            prop_frame_bit_flip_detection()
        ],
        WalProps = [
            prop_wal_single_event_roundtrip(),
            prop_wal_hlc_monotonicity(),
            prop_wal_roundtrip(),
            prop_index_consistency(),
            prop_truncation_safety(),
            prop_manifest_atomicity(),
            prop_consumer_offset_clamping(),
            prop_await_durable_correctness()
        ],
        lists:foreach(
            fun(Prop) -> ?assert(proper:quickcheck(Prop, FrameOpts)) end,
            FrameProps
        ),
        lists:foreach(
            fun(Prop) -> ?assert(proper:quickcheck(Prop, WalOpts)) end,
            WalProps
        )
     end}.

%% =============================================================================
%% Helpers — kept here so subsequent phases can reuse them.
%% =============================================================================

flip_bit(Bin, BitIdx) ->
    ByteIdx = BitIdx div 8,
    BitInByte = BitIdx rem 8,
    Mask = 1 bsl (7 - BitInByte),
    <<Pre:ByteIdx/binary, B, Post/binary>> = Bin,
    <<Pre/binary, (B bxor Mask):8, Post/binary>>.

%% --- WAL helpers --------------------------------------------------------

instance_id() ->
    <<"wal-proper-instance">>.

origin() ->
    <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16>>.

%% Each property generates its events lazily inside the test body so we
%% get a strictly-increasing HLC sequence per shrink trial.
generate_events(HLC, N) ->
    [
        begin
            Hlc = bondy_oplog_hlc:now(HLC),
            Key = bondy_oplog_event:key(Hlc, origin(), Seq),
            bondy_oplog_event:new(Key, {op, Hlc}, undefined)
        end
     || Seq <- lists:seq(1, N)
    ].

%% Approximate single-event frame size. Used to pick a rotation-
%% friendly `max_segment_bytes`. A small `op` payload encodes to
%% ~60–80 bytes; pick 100 to leave slack.
estimated_frame_size() -> 100.

is_strictly_increasing([_]) -> true;
is_strictly_increasing([A, B | Rest]) when A < B ->
    is_strictly_increasing([B | Rest]);
is_strictly_increasing(_) -> false.

%% Spawns a temporary directory, runs `Fun(Dir)`, deletes the directory
%% afterwards regardless of the property outcome. The property result
%% (boolean) is returned verbatim.
with_wal_dir(Fun) ->
    Dir = mktemp_dir(),
    try
        Fun(Dir)
    after
        _ = file:del_dir_r(Dir)
    end.

mktemp_dir() ->
    Base = filename:join(
        ["/tmp", io_lib:format("bondy_oplog_wal_prop_~p_~p",
                              [erlang:system_time(microsecond),
                               erlang:unique_integer([positive])])]
    ),
    Dir = lists:flatten(Base),
    ok = filelib:ensure_path(Dir),
    Dir.

%% Walks every segment file from 0 up to `HeadSegId` inclusive and
%% returns the concatenated event list. Each frame body is a one-
%% element list under the current single-event batch-of-1 framing.
scan_all_segments(Dir, InstanceId, HeadSegId) ->
    lists:flatmap(
        fun(SegId) -> scan_segment(Dir, InstanceId, SegId) end,
        lists:seq(0, HeadSegId)
    ).

scan_segment(Dir, InstanceId, SegId) ->
    Path = filename:join(
        [Dir, InstanceId, bondy_oplog_wal_segment:filename(SegId)]
    ),
    {ok, Bin} = file:read_file(Path),
    <<_:?SEG_HEADER/binary, Frames/binary>> = Bin,
    scan_frames(Frames).

scan_frames(<<>>) -> [];
scan_frames(<<_:32, FrameLen:32, _/binary>> = Bin) ->
    <<Frame:FrameLen/binary, Rest/binary>> = Bin,
    {ok, Body, _} = bondy_oplog_wal_frame:decode(Frame),
    [Event] = binary_to_term(Body),
    [Event | scan_frames(Rest)].

%% Drains a bounded (non-follow) reader to a flat list of events. Used
%% by `prop_wal_roundtrip/0`.
drain_reader(Iter, Acc) ->
    case bondy_oplog_wal_reader:next(Iter) of
        {ok, Batch, _Hlcs, _Pos, NewIter} ->
            drain_reader(NewIter, Acc ++ Batch);
        end_of_log ->
            ok = bondy_oplog_wal_reader:close(Iter),
            Acc;
        {error, _} = E ->
            ok = bondy_oplog_wal_reader:close(Iter),
            E
    end.

%% --- Recovery-test helpers --------------------------------------------

%% Reads every appended event from the WAL via a fresh reader.
read_all_events(Pid) ->
    {ok, Iter} = bondy_oplog_wal_reader:open(Pid, beginning),
    drain_reader(Iter, []).

%% Returns `{ok, NonNegInteger}` with the file's byte size, or
%% `{error, Reason}` if the file is unreachable.
file_size(Path) ->
    case file:read_file_info(Path) of
        {ok, FI} -> {ok, element(2, FI)};
        {error, _} = E -> E
    end.

%% Truncates `Path` to exactly `NewSize` bytes (no-op if the file is
%% already ≤ NewSize).
truncate_file(Path, NewSize) ->
    {ok, Fd} = file:open(Path, [read, write, raw, binary]),
    try
        {ok, _} = file:position(Fd, NewSize),
        ok = file:truncate(Fd)
    after
        ok = file:close(Fd)
    end.

%% Writes a `consumer.offset` file directly, bypassing the setter
%% guards. Used by P10 to seed arbitrary (including invalid) offsets
%% that recovery must still clamp safely.
seed_raw_consumer_offset(InstDir, Seg, Off) ->
    Path = filename:join(
        InstDir, ?BONDY_OPLOG_WAL_CONSUMER_OFFSET_FILENAME
    ),
    Content = io_lib:format(
        "{committed_segment, ~w}.~n"
        "{committed_frame_offset, ~w}.~n"
        "{committed_hlc, undefined}.~n"
        "{commit_count, 0}.~n"
        "{schema_version, 1}.~n",
        [Seg, Off]
    ),
    file:write_file(Path, iolist_to_binary(Content)).
