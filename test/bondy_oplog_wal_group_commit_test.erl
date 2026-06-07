%% =============================================================================
%% Group-commit (boxcar) tests for `bondy_oplog_wal`.
%%
%% Group commit coalesces concurrently-queued `per_write` appends into a
%% single `datasync` per group: the writer writes every queued frame, then
%% issues one fsync and replies to every caller only after it. Durability
%% is identical to plain per_write (durable-on-return); the win is that one
%% fsync amortises across the whole group, removing the
%% one-fsync-per-concurrent-appender wall.
%%
%% The coalescing is made deterministic (no scheduler races) by suspending
%% the writer with `sys:suspend/1`, enqueuing N async requests in HLC order
%% from a single process (so the mailbox holds all N before any is
%% processed), then `sys:resume/1`. The `fsync_count` gauge in `info/1` is
%% the observable:
%%
%% - group_commit = true  : N appends -> 1 datasync (or ceil(N/max)).
%% - group_commit = false : N appends -> N datasyncs (the control).
%%
%% Same workload; the only difference is the `group_commit` flag.
%% =============================================================================

-module(bondy_oplog_wal_group_commit_test).

-include_lib("eunit/include/eunit.hrl").
-include("bondy_oplog.hrl").
-include("bondy_oplog_wal.hrl").

%% =============================================================================
%% Fixture helpers (mirrors bondy_oplog_wal_durability_test)
%% =============================================================================

mktemp_dir() ->
    Base = filename:join(
        [
            "/tmp",
            io_lib:format(
                "bondy_oplog_wal_group_commit_test_~p_~p",
                [
                    erlang:system_time(microsecond),
                    erlang:unique_integer([positive])
                ]
            )
        ]
    ),
    Dir = lists:flatten(Base),
    ok = filelib:ensure_path(Dir),
    Dir.

rmrf(Dir) ->
    _ = file:del_dir_r(Dir),
    ok.

instance_id() ->
    <<"wal-group-commit-test-instance">>.

origin() ->
    <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16>>.

base_opts() ->
    #{origin => origin()}.

with_wal(Opts, Fun) ->
    Dir = mktemp_dir(),
    try
        AllOpts0 = (base_opts())#{dir => Dir},
        AllOpts = maps:merge(AllOpts0, Opts),
        {ok, Pid} = bondy_oplog_wal:start_link(instance_id(), AllOpts),
        try
            Fun(Pid, Dir)
        after
            ok = bondy_oplog_wal:close(Pid)
        end
    after
        rmrf(Dir)
    end.

mk_event(Hlc, Seq) ->
    Key = bondy_oplog_event:key(Hlc, origin(), Seq),
    bondy_oplog_event:new(Key, {op, Hlc}, undefined).

%% N events with strictly increasing HLCs (one clock, N ticks).
mk_monotonic_events(N) ->
    Clock = bondy_oplog_hlc:new(),
    [mk_event(bondy_oplog_hlc:now(Clock), Seq) || Seq <- lists:seq(1, N)].

%% Force `Events` (one single-event batch each) into the writer's mailbox
%% deterministically: suspend, enqueue all N async in order, resume, then
%% collect the unwrapped replies in submit order.
suspend_enqueue_resume(Pid, Events) ->
    ok = sys:suspend(Pid),
    Reqs = [
        gen_server:send_request(Pid, {append_batch, [E]})
     || E <- Events
    ],
    ok = sys:resume(Pid),
    [unwrap_response(gen_server:receive_response(R, 5000)) || R <- Reqs].

unwrap_response({reply, Reply}) -> Reply;
unwrap_response(Other) -> Other.

expect_open_error(Expected, Fun) ->
    OldFlag = process_flag(trap_exit, true),
    try
        Got = Fun(),
        ?assertEqual({error, Expected}, Got),
        receive
            {'EXIT', _, _} -> ok
        after 0 -> ok
        end
    after
        process_flag(trap_exit, OldFlag)
    end.

%% =============================================================================
%% Defaults + info
%% =============================================================================

group_commit_on_by_default_test() ->
    with_wal(#{}, fun(Pid, _Dir) ->
        Info = bondy_oplog_wal:info(Pid),
        ?assertEqual(true, maps:get(group_commit, Info)),
        ?assertEqual(1024, maps:get(group_commit_max, Info)),
        ?assertEqual(0, maps:get(fsync_count, Info))
    end).

%% =============================================================================
%% Coalescing (the headline) + the control
%% =============================================================================

%% N concurrently-queued appends collapse to ONE datasync.
group_commit_coalesces_concurrent_appends_test() ->
    with_wal(#{group_commit => true}, fun(Pid, _Dir) ->
        N = 50,
        Events = mk_monotonic_events(N),
        Replies = suspend_enqueue_resume(Pid, Events),
        %% Every append succeeded...
        [?assertMatch({ok, [_]}, R) || R <- Replies],
        Info = bondy_oplog_wal:info(Pid),
        %% ...and exactly one fsync covered the whole group.
        ?assertEqual(1, maps:get(fsync_count, Info)),
        ?assertEqual(N, maps:get(append_count, Info))
    end).

%% Control: the SAME workload with group commit off fsyncs per append.
%% This is the falsifying comparison — flipping the flag is the only
%% difference, and it changes the fsync count from 1 to N.
group_commit_disabled_fsyncs_per_append_test() ->
    with_wal(#{group_commit => false}, fun(Pid, _Dir) ->
        N = 50,
        Events = mk_monotonic_events(N),
        Replies = suspend_enqueue_resume(Pid, Events),
        [?assertMatch({ok, [_]}, R) || R <- Replies],
        Info = bondy_oplog_wal:info(Pid),
        ?assertEqual(N, maps:get(fsync_count, Info)),
        ?assertEqual(N, maps:get(append_count, Info))
    end).

%% The per-group cap bounds the coalescing: 25 appends at max 10 -> 3
%% datasyncs (10 + 10 + 5).
group_commit_respects_max_cap_test() ->
    Opts = #{group_commit => true, group_commit_max => 10},
    with_wal(Opts, fun(Pid, _Dir) ->
        N = 25,
        Events = mk_monotonic_events(N),
        Replies = suspend_enqueue_resume(Pid, Events),
        [?assertMatch({ok, [_]}, R) || R <- Replies],
        Info = bondy_oplog_wal:info(Pid),
        ?assertEqual(3, maps:get(fsync_count, Info)),
        ?assertEqual(N, maps:get(append_count, Info))
    end).

%% =============================================================================
%% Durability contract preserved
%% =============================================================================

%% A group-committed append is durable on return — the per_write
%% durable-on-return contract is unchanged (here a group of one, via the
%% synchronous append/2 path).
group_commit_append_is_durable_on_return_test() ->
    with_wal(#{group_commit => true}, fun(Pid, _Dir) ->
        Clock = bondy_oplog_hlc:new(),
        Hlc = bondy_oplog_hlc:now(Clock),
        E = mk_event(Hlc, 1),
        {ok, Hlc, {Seg, _Start}} = bondy_oplog_wal:append(Pid, E),
        Info = bondy_oplog_wal:info(Pid),
        EndOff = maps:get(head_offset, Info),
        %% durable == head immediately on return
        ?assertEqual({Seg, EndOff}, bondy_oplog_wal:durable_position(Pid)),
        ?assertEqual(EndOff, maps:get(durable_offset, Info)),
        ?assertEqual(
            ok, bondy_oplog_wal:await_durable(Pid, {Seg, EndOff}, 0)
        ),
        %% one fsync for the one-append group
        ?assertEqual(1, maps:get(fsync_count, Info))
    end).

%% After a coalesced group, the durable position covers every event in the
%% group (all replies are durable, not just the last).
group_commit_whole_group_is_durable_test() ->
    with_wal(#{group_commit => true}, fun(Pid, _Dir) ->
        N = 20,
        Events = mk_monotonic_events(N),
        Replies = suspend_enqueue_resume(Pid, Events),
        %% Each reply carries that event's frame position; all must be at
        %% or below the durable boundary.
        DurablePos = bondy_oplog_wal:durable_position(Pid),
        lists:foreach(
            fun({ok, [{_Hlc, Pos}]}) ->
                ?assert(Pos =< DurablePos)
            end,
            Replies
        ),
        Info = bondy_oplog_wal:info(Pid),
        ?assertEqual(1, maps:get(fsync_count, Info))
    end).

%% A small segment cap forces ≥1 rotation INSIDE a single coalesced group.
%% Frames written before a rotation are made durable by the rotation's own
%% datasync; frames after it by the group's final head fsync. The whole
%% multi-segment group must end durable, and only one head fsync
%% (do_fsync_head) is charged for the group — rotations datasync
%% separately and are not counted in `fsync_count`.
group_commit_mid_group_rotation_all_durable_test() ->
    Opts = #{group_commit => true, max_segment_bytes => 200},
    with_wal(Opts, fun(Pid, _Dir) ->
        N = 6,
        Events = mk_monotonic_events(N),
        Replies = suspend_enqueue_resume(Pid, Events),
        Positions = [Pos || {ok, [{_Hlc, Pos}]} <- Replies],
        ?assertEqual(N, length(Positions)),
        Segs = [S || {S, _Off} <- Positions],
        %% rotation happened mid-group: events span more than one segment
        ?assert(lists:max(Segs) > 0),
        ?assertEqual(0, lists:min(Segs)),
        %% the whole group is durable, including the pre-rotation frames
        DurablePos = bondy_oplog_wal:durable_position(Pid),
        [?assert(Pos =< DurablePos) || Pos <- Positions],
        %% one explicit head fsync for the entire multi-segment group
        Info = bondy_oplog_wal:info(Pid),
        ?assertEqual(1, maps:get(fsync_count, Info))
    end).

%% =============================================================================
%% Opt validation
%% =============================================================================

invalid_group_commit_rejected_test() ->
    Dir = mktemp_dir(),
    try
        expect_open_error(
            {invalid_opt, group_commit, not_a_boolean},
            fun() ->
                bondy_oplog_wal:start_link(
                    instance_id(),
                    #{
                        dir => Dir,
                        origin => origin(),
                        group_commit => not_a_boolean
                    }
                )
            end
        )
    after
        rmrf(Dir)
    end.

invalid_group_commit_max_rejected_test() ->
    Dir = mktemp_dir(),
    try
        expect_open_error(
            {invalid_opt, group_commit_max, 0},
            fun() ->
                bondy_oplog_wal:start_link(
                    instance_id(),
                    #{
                        dir => Dir,
                        origin => origin(),
                        group_commit_max => 0
                    }
                )
            end
        )
    after
        rmrf(Dir)
    end.
