%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_applier).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").
-include("bondy_oplog.hrl").
-include("bondy_oplog_wal.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Per-instance applier loop.

Sits between the per-instance WAL writer and the per-instance
`bondy_oplog_instance` gen_server. Responsibilities:

- On start, choose a resume position based on the live MST's
  high-watermark + the snapshot watermark (see `resume_position/2`)
  and open a non-following `bondy_oplog_wal_reader` there.
- Drain the reader in batches, calling
  `bondy_oplog_instance:apply_events/2` so the instance installs the
  events in the MST and replies to the original `append` caller(s).
- After applying each batch, persist `consumer.offset` (atomic write)
  and notify the WAL writer of the new committed segment so retention
  can sweep.

## Resume position

For a *durable* MST backend the live MST holds the highest applied
event from the previous run; for a *volatile* (ETS) backend it is
empty after a subtree restart. In both cases the resume HLC is
`max(MST_last_key.hlc, snapshot_watermark.hlc)`, falling back to
`beginning` when both are unknown. `bondy_mst:put` is content-
addressable so the small overlap that `{hlc, _}` includes around the
resume frame is an idempotent no-op.

## Configuration

| Option              | Default | Meaning |
|---|---|---|
| `commit_every`      | `64`    | Apply this many events between `consumer.offset` flushes. |
| `poll_interval_ms`  | `5`     | Backstop sleep when `await_durable/3` returns sooner than expected. The hot path long-polls rather than sleeping; this only affects the rare error fallback. |
""").

-record(state, {
    instance_id :: instance_id(),
    instance_pid :: pid(),
    wal_pid :: pid(),
    wal_dir :: file:filename_all(),
    iter :: bondy_oplog_wal_reader:t() | undefined,
    consumer_offset :: bondy_oplog_wal_consumer_offset:t(),
    %% Number of events applied since the last `commit/1`. Used to
    %% batch consumer.offset writes — flushed at `commit_every` or
    %% when the reader returns `end_of_log`.
    uncommitted :: non_neg_integer(),
    commit_every :: pos_integer(),
    %% Milliseconds between polling ticks when the reader returns
    %% `end_of_log`. Constant for now; the writer publishes an atomics
    %% durable position so a future revision could long-poll instead.
    poll_interval_ms :: pos_integer()
}).

-type opts() :: #{
    instance_id := instance_id(),
    wal_dir := file:filename_all(),
    commit_every => pos_integer(),
    poll_interval_ms => pos_integer()
}.

-export_type([opts/0]).

-export([start_link/1]).
-export([child_spec/1]).
-export([stop/1]).

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).

-define(DEFAULT_COMMIT_EVERY, 64).
%% The applier long-polls the WAL via `await_durable/3` on
%% `end_of_log` rather than sleeping between ticks, so this only
%% bounds the wake-up cadence when the WAL is idle. A small interval
%% keeps responsiveness if `await_durable/3` ever returns sooner than
%% expected or a future revision drops the long-poll path.
-define(DEFAULT_POLL_INTERVAL_MS, 5).
%% Soft inner timeout for the `await_durable/3` long-poll. Bounded so
%% supervisor shutdown messages and any future control-plane signals
%% are processed in a timely fashion.
-define(AWAIT_DURABLE_TIMEOUT_MS, 200).

%% =============================================================================
%% API
%% =============================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.

start_link(#{instance_id := _, wal_dir := _} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec child_spec(opts()) -> supervisor:child_spec().

child_spec(#{instance_id := InstanceId} = Opts) ->
    #{
        id => {?MODULE, InstanceId},
        start => {?MODULE, start_link, [Opts]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
    }.

-spec stop(pid()) -> ok.

stop(Pid) when is_pid(Pid) ->
    try gen_server:stop(Pid, normal, 5000) of
        ok -> ok
    catch
        exit:noproc -> ok;
        exit:{noproc, _} -> ok
    end.

%% =============================================================================
%% gen_server CALLBACKS
%% =============================================================================

init(#{instance_id := InstanceId, wal_dir := WalDir} = Opts) ->
    process_flag(trap_exit, true),
    CommitEvery = maps:get(commit_every, Opts, ?DEFAULT_COMMIT_EVERY),
    PollMs = maps:get(poll_interval_ms, Opts, ?DEFAULT_POLL_INTERVAL_MS),
    case resolve_siblings(InstanceId) of
        {ok, InstP, WalP, MST, Watermark} ->
            CO = read_consumer_offset(WalDir),
            StartPos = resume_position(MST, Watermark),
            case bondy_oplog_wal_reader:open(
                WalP, StartPos, [{follow, false}]
            ) of
                {ok, Iter} ->
                    State = #state{
                        instance_id = InstanceId,
                        instance_pid = InstP,
                        wal_pid = WalP,
                        wal_dir = WalDir,
                        iter = Iter,
                        consumer_offset = CO,
                        uncommitted = 0,
                        commit_every = CommitEvery,
                        poll_interval_ms = PollMs
                    },
                    ok = bondy_oplog_registry:set_applier_pid(
                        InstanceId, self()
                    ),
                    self() ! drain,
                    {ok, State};
                {error, Reason} ->
                    {stop, {reader_open_failed, Reason}}
            end;
        {error, _} = Err ->
            {stop, Err}
    end.

handle_call(_Req, _From, State) ->
    {reply, {error, badcall}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(drain, State0) ->
    case drain_loop(State0) of
        {ok, State1} ->
            ok = await_or_idle(State1),
            self() ! drain,
            {noreply, State1};
        {stop, Reason, State1} ->
            {stop, Reason, State1}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{iter = Iter, consumer_offset = CO,
                          wal_dir = Dir, uncommitted = N}) ->
    case N > 0 of
        true -> _ = bondy_oplog_wal_consumer_offset:write(Dir, CO);
        false -> ok
    end,
    case Iter of
        undefined -> ok;
        _ -> bondy_oplog_wal_reader:close(Iter)
    end,
    ok.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
%% Looks up the three pids and the MST/watermark snapshot the applier
%% needs from the per-instance registry row. Returns a structured
%% error pointing at the first missing field so operators can diagnose
%% supervisor-start-order or registry-publish races.
resolve_siblings(InstanceId) ->
    InstancePid = bondy_oplog_registry:instance_pid(InstanceId),
    WalPid = bondy_oplog_registry:wal_pid(InstanceId),
    MST = bondy_oplog_registry:mst(InstanceId),
    Watermark = bondy_oplog_registry:watermark(InstanceId),
    case missing_sibling(InstancePid, WalPid, MST) of
        none ->
            {ok, InstancePid, WalPid, MST, Watermark};
        Field ->
            {missing_sibling, #{
                instance_id => InstanceId,
                missing => Field,
                instance_pid => InstancePid,
                wal_pid => WalPid,
                mst_present => MST =/= undefined
            }}
    end.

%% @private
missing_sibling(undefined, _, _) -> instance_pid;
missing_sibling(_, undefined, _) -> wal_pid;
missing_sibling(_, _, undefined) -> mst;
missing_sibling(_, _, _) -> none.

%% @private
%% Resume from `max(last_MST_key.hlc, watermark.hlc)`. The reader's
%% `{hlc, T}` start finds the first frame whose first event HLC is
%% `>= T`, so the frame that contained our resume HLC is re-read and
%% its events are re-applied — that is safe because `bondy_mst:put`
%% is content-addressable and verify+install are idempotent.
%%
%% Falls back to `beginning` when both inputs are absent (fresh
%% instance with empty MST and no compaction history) or when the
%% MST handle is missing.
resume_position(MST, Watermark) ->
    case resume_hlc(MST, Watermark) of
        undefined -> beginning;
        HLC -> {hlc, HLC}
    end.

%% @private
resume_hlc(MST, Watermark) ->
    MstHlc = mst_last_hlc(MST),
    WmHlc = watermark_hlc(Watermark),
    case {MstHlc, WmHlc} of
        {undefined, undefined} -> undefined;
        {undefined, H} -> H;
        {H, undefined} -> H;
        {A, B} when A >= B -> A;
        {_, B} -> B
    end.

%% @private
mst_last_hlc(undefined) ->
    undefined;
mst_last_hlc(MST) ->
    case bondy_mst:last(MST) of
        undefined -> undefined;
        {Key, _Value} -> bondy_oplog_event:key_hlc(Key)
    end.

%% @private
watermark_hlc(undefined) ->
    undefined;
watermark_hlc(Key) ->
    bondy_oplog_event:key_hlc(Key).

%% @private
%% Reads the on-disk `consumer.offset`. Used to seed the in-memory
%% commit accumulator so a brand-new applier doesn't overwrite a
%% live consumer.offset with a fresh `{0, 48}`. The resume position
%% itself comes from `resume_position/2` (MST + watermark), not from
%% this file, so a missing or stale offset is safe.
read_consumer_offset(WalDir) ->
    case bondy_oplog_wal_consumer_offset:read(WalDir) of
        {ok, CO} -> CO;
        {error, _} -> bondy_oplog_wal_consumer_offset:new()
    end.

%% @private
%% Drains the reader until it returns `end_of_log` or `{error, _}`.
%% On every batch it applies the events and bumps the in-memory
%% consumer offset; consumer.offset and `set_committed_segment` are
%% persisted at `commit_every` events or on `end_of_log`.
drain_loop(#state{iter = Iter} = State0) ->
    case bondy_oplog_wal_reader:next(Iter) of
        {ok, Batch, _Hlcs, {NextSeg, NextOff}, NewIter} ->
            ApplyResult = apply_batch(State0, Batch),
            ok = log_batch_outcome(State0, ApplyResult),
            {LastHlc, Count} = batch_summary(Batch),
            State1 = bump_offset(
                State0#state{iter = NewIter},
                NextSeg, NextOff, LastHlc, Count
            ),
            State2 = maybe_commit(State1),
            drain_loop(State2);
        end_of_log ->
            {ok, commit_now(State0)};
        {error, Reason} ->
            ?LOG_ERROR(#{
                description =>
                    "bondy_oplog_applier reader returned an error; "
                    "stopping so the supervisor can restart the subtree "
                    "and recovery can reconcile the on-disk state",
                instance_id => State0#state.instance_id,
                reason => Reason
            }),
            {stop, {reader_error, Reason}, State0}
    end.

%% @private
%% The instance handles partial-batch refusal internally (signature
%% failure, equivocation), so we always advance past the batch. A
%% refusal is logged here; it has already been quarantined or logged
%% at the instance side. Stopping on refusal would crash-loop the
%% subtree under always-from-beginning replay.
log_batch_outcome(_State, ok) ->
    ok;
log_batch_outcome(#state{instance_id = Id}, {error, Reason}) ->
    ?LOG_WARNING(#{
        description =>
            "bondy_oplog_applier advancing past a batch that the "
            "instance refused to apply; subsequent passes will not "
            "retry these events",
        instance_id => Id,
        reason => Reason
    }),
    ok.

%% @private
%% One gen_server round-trip per batch — the instance applies every
%% event in order inside a single handle_call and replies once.
apply_batch(#state{instance_pid = InstancePid}, Batch) ->
    bondy_oplog_instance:apply_events(InstancePid, Batch).

%% @private
batch_summary(Batch) ->
    LastEvent = lists:last(Batch),
    LastHlc = bondy_oplog_event:key_hlc(
        bondy_oplog_event:key(LastEvent)
    ),
    {LastHlc, length(Batch)}.

%% @private
bump_offset(#state{consumer_offset = CO0, uncommitted = U} = State,
            Seg, Off, LastHlc, Count) ->
    CO1 = bondy_oplog_wal_consumer_offset:with_position(CO0, Seg, Off),
    CO2 = bondy_oplog_wal_consumer_offset:with_hlc(CO1, LastHlc),
    Old = bondy_oplog_wal_consumer_offset:commit_count(CO2),
    CO3 = bondy_oplog_wal_consumer_offset:with_commit_count(CO2, Old + 1),
    State#state{consumer_offset = CO3, uncommitted = U + Count}.

%% @private
maybe_commit(#state{uncommitted = U, commit_every = N} = State)
        when U >= N ->
    commit_now(State);
maybe_commit(State) ->
    State.

%% @private
commit_now(#state{uncommitted = 0} = State) ->
    State;
commit_now(#state{
    instance_id = InstanceId,
    wal_dir = Dir,
    wal_pid = WalPid,
    consumer_offset = CO
} = State) ->
    case bondy_oplog_wal_consumer_offset:write(Dir, CO) of
        ok ->
            Seg = bondy_oplog_wal_consumer_offset:committed_segment(CO),
            ok = notify_committed_segment(InstanceId, WalPid, Seg),
            State#state{uncommitted = 0};
        {error, Reason} ->
            ?LOG_WARNING(#{
                description =>
                    "bondy_oplog_applier could not persist consumer.offset; "
                    "retrying on the next commit boundary",
                instance_id => InstanceId,
                reason => Reason
            }),
            %% Keep uncommitted > 0 so the next commit boundary retries.
            State
    end.

%% @private
%% Tells the WAL writer to advance its committed-segment marker so the
%% retention sweep can drop fully-applied segments. A narrow `noproc`
%% catch covers the benign supervisor-shutdown race where the WAL has
%% already exited; any other error is logged so it doesn't get
%% swallowed silently.
notify_committed_segment(InstanceId, WalPid, Seg) ->
    try bondy_oplog_wal:set_committed_segment(WalPid, Seg) of
        ok ->
            ok;
        {error, Reason} ->
            ?LOG_WARNING(#{
                description =>
                    "bondy_oplog_wal refused a set_committed_segment "
                    "request; retention sweep may lag until the next "
                    "commit boundary",
                instance_id => InstanceId,
                segment => Seg,
                reason => Reason
            }),
            ok
    catch
        exit:{noproc, _} -> ok;
        exit:noproc -> ok
    end.

%% @private
%% Block (briefly) until the WAL's durable position advances past the
%% reader's current offset, or the inner timeout fires. The applier's
%% main loop schedules an immediate re-drain after this returns; the
%% `poll_interval_ms` is only used as a backstop if `await_durable/3`
%% replies sooner than the inner timeout (which shouldn't normally
%% happen but is bounded here defensively).
await_or_idle(#state{iter = Iter, wal_pid = WalPid,
                     poll_interval_ms = PollMs}) ->
    {Seg, Off} = bondy_oplog_wal_reader:position(Iter),
    case bondy_oplog_wal:await_durable(WalPid, {Seg, Off},
                                       ?AWAIT_DURABLE_TIMEOUT_MS) of
        ok -> ok;
        {error, timeout} -> ok;
        {error, _} ->
            %% Defensive: any other error from `await_durable/3` is a
            %% WAL-level failure that the next `next/1` will surface
            %% on its own. Fall back to a short sleep so we don't burn
            %% CPU re-asking immediately.
            timer:sleep(PollMs)
    end,
    ok.
