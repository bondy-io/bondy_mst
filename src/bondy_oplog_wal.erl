%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_wal).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").
-include("bondy_oplog.hrl").
-include("bondy_oplog_wal.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Per-instance Write-Ahead Log writer.

See `_design/WAL_DESIGN.md` §8. Current behaviour:

- `open/2` creates a fresh per-instance WAL directory or recovers an
  existing one via `bondy_oplog_wal_recovery`.
- `append/2` writes a single event as a one-element batch frame
  (`term_to_binary([Event], ...)`), and either fsyncs immediately
  (`fsync_mode = per_write`) or defers fsync to a batched boundary
  (`fsync_mode = batched`). Returns the event's HLC plus the
  `{Segment, Offset}` of the frame start.
- Rotation happens between appends when the next frame would push the
  head segment past `max_segment_bytes`. The manifest is rewritten
  atomically via `bondy_oplog_wal_manifest:write/2`. Rotation also
  fsyncs the just-sealed segment, advancing `durable_position/1`.
- Per-frame the writer feeds the sparse-index accumulator
  (`bondy_oplog_wal_idx`); on rotation the sealed segment's `.qidx` is
  flushed to disk, on `terminate/2` the head segment's `.qidx` is
  flushed.
- `sync/1` forces a head fsync, advancing durable and notifying any
  `await_durable/3` waiters covered by the new durable position.
- `durable_position/1` and `await_durable/3` expose the durability
  boundary distinct from the (possibly leading) head position. In
  `per_write` mode head ≡ durable so `await_durable/3` always returns
  immediately; in `batched` mode head can lead durable by up to one
  fsync interval.
- `close/1` fsyncs and stops the writer; `info/1` exposes the writer's
  current state (including `head_offset`, `durable_offset`,
  `fsync_mode`, `last_fsync_at`).

Body framing uses the design's batch shape (body is a list of events)
so atomic batch frames can be exposed via a future `append_batch/2`
without changing the on-disk format. Retention, backpressure, the
applier integration, and the full stateful-PropEr fault-injection
harness are still to land.
""").

%% Pending `await_durable/3` caller. Sorted ascending by `pos` in
%% `state.waiters` so satisfying on a durable advance is a
%% `lists:splitwith/2` over the head of the list.
-record(waiter, {
    id    :: reference(),
    pos   :: {bondy_oplog_wal_segment:segment_id(), non_neg_integer()},
    from  :: gen_server:from(),
    %% `infinity` ⇒ no deadline; otherwise the timer ref the writer
    %% will receive on timeout (and cancel on satisfy).
    tref  :: reference() | infinity
}).

-record(state, {
    instance_id        :: instance_id(),
    dir                :: file:filename_all(),
    origin             :: bondy_oplog_origin:t(),
    max_segment_bytes  :: pos_integer(),
    retention          :: [{atom(), term()}],
    head_fd            :: file:fd() | undefined,
    segment_id         :: bondy_oplog_wal_segment:segment_id(),
    current_offset     :: non_neg_integer(),
    first_hlc          :: bondy_oplog_hlc:hlc() | undefined,
    last_hlc           :: bondy_oplog_hlc:hlc() | undefined,
    append_count       :: non_neg_integer(),
    %% In-memory shadow of the on-disk manifest. Updated in place on
    %% rotation; flushed atomically via `bondy_oplog_wal_manifest:write/2`
    %% (tmp + datasync + rename + dir-fsync) so on-disk and in-memory
    %% never diverge.
    manifest           :: bondy_oplog_wal_manifest:t() | undefined,
    %% Two-slot atomics ref published to tail readers (`bondy_oplog_wal_reader`).
    %% Slot 1: head segment id. Slot 2: head offset within that segment.
    %% Updated in this order on rotation so a reader who races never sees
    %% the new offset paired with the old segment id; see
    %% `publish_head_pos/3` and `publish_head_offset/2`.
    head_pos_ref       :: atomics:atomics_ref() | undefined,
    %% Sparse-index accumulator (§7) for the current head segment.
    %% Entries are flushed to `.qidx` on rotation (sealed segment) and
    %% on `terminate/2` (live head segment). Per-frame I/O cost is zero —
    %% entries live in memory until a flush boundary.
    idx_acc            :: bondy_oplog_wal_idx:accumulator() | undefined,
    idx_interval_bytes :: pos_integer(),
    %% --- Durability state -----------------------------------------------------
    %% `per_write`: each `append/2` includes a `prim_file:datasync/1`
    %% and returns durable. `batched`: appends accumulate until the
    %% size threshold or the interval timer fires; durability is
    %% reached at a fsync boundary or via `sync/1`/`await_durable/3`.
    fsync_mode         :: per_write | batched,
    %% Batched-mode timer interval (ms) — bound on the lag between a
    %% successful `append/2` and the next fsync.
    batched_fsync_interval :: pos_integer(),
    %% Batched-mode size trigger (bytes) — bound on un-fsynced data.
    batched_fsync_bytes :: pos_integer(),
    %% Bytes written since the last fsync (head segment only; rotation
    %% always fsyncs before sealing).
    pending_fsync_bytes :: non_neg_integer(),
    %% Monotonic timestamp of the most recent fsync, used by
    %% `last_fsync_at` in `info/1` and lag-monitoring telemetry.
    last_fsync_at      :: integer() | undefined,
    %% Active `send_after/3` timer that will fire `flush_tick`. Set when
    %% pending bytes accrue in batched mode; cancelled on fsync.
    flush_timer        :: reference() | undefined,
    %% Two-slot atomics ref mirroring `head_pos_ref`'s shape but carrying
    %% the durable position (slot 1: durable segment id, slot 2:
    %% durable byte offset). Tail readers / appliers may poll this
    %% wait-free; cross-segment reads are subject to the same race as
    %% `head_pos_ref` (see comment on `publish_durable_pos/3`). For a
    %% coherent snapshot use `durable_position/1` which reads in-memory
    %% state through the writer's gen_server.
    durable_pos_ref    :: atomics:atomics_ref() | undefined,
    %% In-memory mirror of the durable position. The authoritative copy
    %% for `durable_position/1` and `await_durable/3` (both serialise
    %% through the gen_server). Advances on fsync; on rotation jumps to
    %% the new segment's header boundary (the new segment file is
    %% datasync'd at create time).
    durable_segment_id :: bondy_oplog_wal_segment:segment_id(),
    durable_offset     :: non_neg_integer(),
    %% Pending `await_durable/3` callers, sorted ascending by `#waiter.pos`.
    %% Walked head-first on durable advance; replaced wholesale on each
    %% advance via `satisfy_waiters_up_to/2`.
    waiters            :: [#waiter{}]
}).

-type opts() :: #{
    dir := file:filename_all(),
    origin := bondy_oplog_origin:t(),
    max_segment_bytes => pos_integer(),
    retention => [{atom(), term()}],
    idx_interval_bytes => pos_integer(),
    fsync_mode => per_write | batched,
    batched_fsync_interval => pos_integer(),
    batched_fsync_bytes => pos_integer()
}.

-type wal() :: pid().

-type segment_id() :: bondy_oplog_wal_segment:segment_id().
-type offset() :: non_neg_integer().
-type position() :: {segment_id(), offset()}.

-export_type([opts/0]).
-export_type([wal/0]).
-export_type([position/0]).

-export([start/2]).
-export([start_link/2]).
-export([child_spec/2]).
-export([open/2]).
-export([close/1]).
-export([append/2]).
-export([sync/1]).
-export([durable_position/1]).
-export([await_durable/3]).
-export([info/1]).
-export([reader_view/1]).

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).

-define(DEFAULT_MAX_SEGMENT_BYTES, 64 * 1024 * 1024).
-define(SEG_HEADER_BYTES, ?BONDY_OPLOG_WAL_SEGMENT_HEADER_BYTES).
-define(FRAME_HEADER_BYTES, ?BONDY_OPLOG_WAL_FRAME_HEADER_BYTES).

%% =============================================================================
%% API
%% =============================================================================

?DOC("""
Starts a WAL writer linked to the caller. See `open/2` for the public
contract.
""").
-spec start_link(instance_id(), opts()) ->
    {ok, pid()} | {error, term()}.

start_link(InstanceId, Opts) when
    is_binary(InstanceId), byte_size(InstanceId) > 0, is_map(Opts)
->
    gen_server:start_link(?MODULE, {InstanceId, Opts}, []).

?DOC("""
Starts a WAL writer **without** linking to the caller. Useful in tests
that exercise init-failure paths, where a linked exit signal would kill
the test process. Production code should prefer `start_link/2` or
`open/2` so the writer participates in supervision.
""").
-spec start(instance_id(), opts()) ->
    {ok, pid()} | {error, term()}.

start(InstanceId, Opts) when
    is_binary(InstanceId), byte_size(InstanceId) > 0, is_map(Opts)
->
    gen_server:start(?MODULE, {InstanceId, Opts}, []).

?DOC("""
Returns a `supervisor:child_spec/0` for hosting a WAL writer under a
supervisor. Used by `bondy_oplog_wal_sup` and by the future
per-instance supervisor that will host the writer, applier, and
instance API as a one_for_all subtree.
""").
-spec child_spec(instance_id(), opts()) -> supervisor:child_spec().

child_spec(InstanceId, Opts) ->
    #{
        id => {?MODULE, InstanceId},
        start => {?MODULE, start_link, [InstanceId, Opts]},
        restart => permanent,
        shutdown => 30000,
        type => worker,
        modules => [?MODULE]
    }.

?DOC("""
Opens a per-instance WAL, creating a fresh one or recovering an
existing one transparently.

Returns `{ok, Pid}` where `Pid` is the writer gen_server.

If `{Dir}/{InstanceId}/manifest` does not exist, this is a **fresh
open**: the directory is created, segment 0 is written with its
header, and the initial manifest is fsynced.

If a manifest exists, this is a **recovery open**: `bondy_oplog_wal_recovery`
validates the manifest, cleans orphan files left from interrupted
operations, validates the headers of all live sealed segments,
rebuilds any missing `.qidx` files, scans the head segment forward
break-and-truncate-style to its last valid frame, truncates the file
if necessary, and clamps `consumer.offset` (if present) to a real
frame boundary. The writer resumes appending immediately after the
last recovered frame.

`Opts` must contain:
- `dir` — the parent WAL directory; `InstanceId` is appended.
- `origin` — the 16-byte replica id stamped in each segment header
  (`bondy_oplog_origin:t()`).

Optional:
- `max_segment_bytes` — rotation threshold; defaults to 64 MiB.
- `retention` — proplist persisted in the manifest verbatim.
- `idx_interval_bytes` — sparse index emit interval; defaults to 64 KiB.
- `fsync_mode` — `per_write` (default) or `batched`. Per-write fsyncs
  every `append/2`; batched defers fsync to a size or time boundary
  (see `batched_fsync_*` below) and exposes durability via
  `durable_position/1` and `await_durable/3`.
- `batched_fsync_interval` — batched-mode time trigger in
  milliseconds; defaults to 50 ms. The writer fsyncs at most this
  long after the first un-fsynced append.
- `batched_fsync_bytes` — batched-mode size trigger; defaults to 1
  MiB. The writer fsyncs when accumulated un-fsynced bytes exceed
  this threshold.

Recovery errors are returned through `start_link/2`'s usual
`{error, Reason}` channel. Possible recovery-time failures:
- `{manifest, _}` — manifest unreadable or fails validation.
- `{instance_id_mismatch, Expected, Found}` — WAL belongs to another
  instance.
- `{head_segment, SegId, Reason}` — head segment header invalid or
  truncate failed.
- `{sealed_segment, SegId, Reason}` — a sealed segment's header
  doesn't match this instance/origin.
- `{consumer_offset, _}` — `consumer.offset` is malformed.
""").
-spec open(instance_id(), opts()) -> {ok, wal()} | {error, term()}.

open(InstanceId, Opts) ->
    start_link(InstanceId, Opts).

?DOC("""
Stops the WAL writer. The head segment is fsynced and the fd is closed
in `terminate/2`. Idempotent — calling `close/1` on a dead pid returns
`ok`. `is_process_alive/1` is intentionally **not** used: it would race
with the actual stop and tell us nothing the try/catch doesn't already
handle.
""").
-spec close(wal()) -> ok.

close(Pid) when is_pid(Pid) ->
    try gen_server:stop(Pid, normal, 30000) of
        ok -> ok
    catch
        exit:noproc -> ok;
        exit:{noproc, _} -> ok
    end.

?DOC("""
Appends a single event as a one-element batch frame.

Returns `{ok, Hlc, {Segment, Offset}}` where:
- `Hlc` is the event's own HLC (taken from its key).
- `Segment` is the head segment id at the time of the append.
- `Offset` is the byte offset of the frame's first byte within the
  segment file. The next-frame offset (used by consumer commits later)
  is `Offset + FrameLen`.

In `per_write` mode every successful `append/2` includes a
`prim_file:datasync/1` and the returned position is durable. In
`batched` mode (`fsync_mode = batched`) the call returns as soon as
the frame has been written; durability is reached at a later fsync
boundary observable via `durable_position/1` or awaitable via
`await_durable/3`.
""").
-spec append(wal(), bondy_oplog_event:t()) ->
    {ok, bondy_oplog_hlc:hlc(), position()}
    | {error, term()}.

append(Pid, #bondy_oplog_event{} = Event) when is_pid(Pid) ->
    gen_server:call(Pid, {append, Event}, infinity).

?DOC("""
Forces an fsync of the head segment file descriptor.

In `per_write` mode this is a barrier (every prior `append/2` was
already fsynced); the call still completes the protocol — advancing
`durable_position/1` to the current head and notifying any
`await_durable/3` waiters covered by the new durable boundary. In
`batched` mode this is the user-facing way to force durability.
""").
-spec sync(wal()) -> ok | {error, term()}.

sync(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, sync, infinity).

?DOC("""
Returns the current durable position `{Segment, Offset}` — the highest
byte offset that has been fsynced to disk.

In `per_write` mode this equals the head position at any quiescent
moment; in `batched` mode it may lag the head by up to one fsync
interval. The result is read from the writer's serialised state so
the pair is always consistent (unlike the `durable_pos_ref` atomics
ref exposed via `reader_view/1`, which may race across segment
rotations).
""").
-spec durable_position(wal()) -> position().

durable_position(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, durable_position, infinity).

?DOC("""
Blocks until the durable position reaches `{Segment, Offset}`, or
until `Timeout` milliseconds (or `infinity`) elapse.

`Pos` is the offset of the **byte just past** the data the caller
wants durable — the same coordinate as `head_offset` / `durable_offset`
in `info/1` and the same convention applier/consumer code uses for
"the next byte to be written". Equivalently: an append that returned
`{ok, _, {Seg, Off}}` for a frame of length `FrameLen` is durable
when `durable_position/1` returns a position `>= {Seg, Off + FrameLen}`.

Returns:
- `ok` — the position is (or has become) durable.
- `{error, timeout}` — durability not reached within `Timeout`.

In `per_write` mode `await_durable/3` always returns `ok` immediately:
appends are durable on return, so any `Pos <= head_offset` is
already covered.
""").
-spec await_durable(wal(), position(), timeout()) ->
    ok | {error, timeout} | {error, term()}.

await_durable(Pid, {Seg, Off} = Pos, Timeout) when
    is_pid(Pid), is_integer(Seg), Seg >= 0,
    is_integer(Off), Off >= 0,
    (Timeout =:= infinity orelse (is_integer(Timeout) andalso Timeout >= 0))
->
    %% Use a client-side `infinity` `gen_server:call/3` timeout —
    %% the writer enforces `Timeout` internally and replies (with
    %% `{error, timeout}` if applicable) when the deadline fires.
    %% This avoids the server-side waiter being satisfied just as
    %% the client-side gen_server timeout elapses, which would leak
    %% an `{Tag, Reply}` message into the caller's mailbox.
    gen_server:call(Pid, {await_durable, Pos, Timeout}, infinity).

?DOC("""
Returns a diagnostic snapshot of the writer's state. Suitable for
operator status pages and tests; not a load-bearing protocol surface.

Current shape:

```erlang
#{
    instance_id            => instance_id(),
    dir                    => file:filename_all(),
    origin                 => bondy_oplog_origin:t(),
    max_segment_bytes      => pos_integer(),
    current_segment        => segment_id(),
    head_offset            => non_neg_integer(),
    durable_segment        => segment_id(),
    durable_offset         => non_neg_integer(),
    first_hlc              => hlc() | undefined,
    last_hlc               => hlc() | undefined,
    append_count           => non_neg_integer(),
    fsync_mode             => per_write | batched,
    batched_fsync_interval => pos_integer(),
    batched_fsync_bytes    => pos_integer(),
    pending_fsync_bytes    => non_neg_integer(),
    last_fsync_at          => integer() | undefined,
    waiter_count           => non_neg_integer()
}
```

`_design/WAL_DESIGN.md` §13.5 lists additional keys
(`live_segments`, `deleted_through`, `committed_segment`,
`committed_offset`, `committed_hlc`, `snapshot_watermark`,
`bytes_total`, `backpressure`) that arrive with the retention and
backpressure features.
""").
-spec info(wal()) -> map().

info(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, info).

?DOC("""
Returns the static + atomics-published state a reader needs to open
itself against this writer:

```erlang
#{
    instance_id      => instance_id(),
    dir              => file:filename_all(),
    origin           => bondy_oplog_origin:t(),
    head_pos_ref     => atomics:atomics_ref(),
    durable_pos_ref  => atomics:atomics_ref(),
    head_pos         => {segment_id(), non_neg_integer()},
    current_segment  => segment_id(),
    live_segments    => [{segment_id(), hlc() | undefined}],
    deleted_through  => segment_id(),
    head_first_hlc   => hlc() | undefined,
    head_idx_entries => [{hlc(), non_neg_integer()}]
}
```

`head_pos_ref` is the wait-free reference the reader uses in its hot
loop. `head_pos` is a one-shot consistent snapshot of the writer's
head segment id and head offset, read under the gen_server's serial
handle_call — readers resolving a `tail` start use this instead of
two separate `atomics:get/2` calls which can race with rotation.

`durable_pos_ref` is the sibling atomics ref for the durable
boundary; in `per_write` mode it tracks `head_pos_ref` after every
append, in `batched` mode it may lag by up to one fsync interval.
Cross-segment reads are subject to the same race as `head_pos_ref` —
callers needing a coherent snapshot should use `durable_position/1`
(gen_server-serialised) instead.

`live_segments` carries `{SegmentId, FirstHlc}` pairs as recorded in
the manifest. The head segment's `FirstHlc` is `undefined` until the
next rotation persists it; `head_first_hlc` exposes the writer's live
value so HLC seek can address the head segment via the sparse index.

`head_idx_entries` is the writer's in-memory sparse-index accumulator
for the head segment (entries it has not yet flushed to disk — flush
happens on rotation and on terminate). Readers wrap it via
`bondy_oplog_wal_idx:from_entries/1` to seek the head segment without
a disk round-trip.

`current_segment` and `live_segments` are a snapshot at call time and
may be stale before the reader has finished walking them (the atomics
ref keeps the reader correct even if the writer rotates afterwards).
The reader uses `live_segments` only to resolve `beginning` /
`{offset, Seg, Off}` starts and HLC-seek candidate selection.
""").
-spec reader_view(wal()) -> map().

reader_view(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, reader_view).

%% =============================================================================
%% gen_server CALLBACKS
%% =============================================================================

init({InstanceId, Opts}) ->
    process_flag(trap_exit, true),
    case do_open(InstanceId, Opts) of
        {ok, State} -> {ok, State};
        {error, Reason} -> {stop, Reason}
    end.

handle_call({append, Event}, _From, State0) ->
    case do_append(State0, Event) of
        {ok, Hlc, Pos, State1} ->
            {reply, {ok, Hlc, Pos}, State1};
        {error, _} = E ->
            {reply, E, State0}
    end;
handle_call(sync, _From, #state{head_fd = Fd} = State) when Fd =/= undefined ->
    case do_fsync_head(State) of
        {ok, State1} -> {reply, ok, State1};
        {error, _} = E -> {reply, E, State}
    end;
handle_call(durable_position, _From, State) ->
    {reply,
     {State#state.durable_segment_id, State#state.durable_offset},
     State};
handle_call({await_durable, Pos, Timeout}, From, State) ->
    handle_await_durable(Pos, Timeout, From, State);
handle_call(info, _From, State) ->
    {reply, build_info(State), State};
handle_call(reader_view, _From, State) ->
    {reply, build_reader_view(State), State};
handle_call(_Req, _From, State) ->
    {reply, {error, badcall}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Batched-mode interval timer fired. Reset the timer ref (we re-arm on
%% the next un-fsynced append) and fsync if any bytes are pending. If
%% the writer crashes between scheduling and firing, the timer message
%% is dropped with the process exit.
handle_info(flush_tick, State0) ->
    State1 = State0#state{flush_timer = undefined},
    case maybe_batched_fsync(State1) of
        {ok, State2} -> {noreply, State2};
        {error, _} -> {noreply, State1}
    end;
%% `await_durable/3` deadline elapsed. Remove the matching waiter from
%% the pending list (if still present) and reply `{error, timeout}`.
%% If the waiter was already satisfied on an fsync that arrived ahead of
%% the timer message, the lookup is empty and we drop the timer event.
handle_info({timeout, _TRef, {await_timeout, WaiterId}}, State) ->
    {noreply, expire_waiter(WaiterId, State)};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{head_fd = undefined} = State) ->
    _ = cancel_flush_timer(State),
    ok;
terminate(_Reason, #state{head_fd = Fd,
                          segment_id = Seg,
                          current_offset = Off} = State0) ->
    State = case prim_file:datasync(Fd) of
        ok ->
            %% The head fd is now durable up to `current_offset`.
            %% Advance the durable boundary so any waiter at or below
            %% head receives `ok` (their position IS durable) before
            %% the writer exits; pollers of `durable_pos_ref` see the
            %% accurate final state instead of a stale snapshot. Above-
            %% head waiters get the natural `noproc` exit (their
            %% position is unreachable in this writer's lifetime). The
            %% call also cancels the flush timer as part of its
            %% bookkeeping.
            advance_durable(State0, Seg, Off);
        {error, _} ->
            _ = cancel_flush_timer(State0),
            State0
    end,
    %% Flush the head segment's sparse index. Best-effort: on failure we
    %% log and continue closing the fd. Recovery rebuilds the `.qidx`
    %% from a segment scan if it's missing or stale.
    _ = flush_head_idx(State),
    _ = prim_file:close(Fd),
    ok.

%% @private
%% Cancel any active batched-fsync interval timer. Returns the state
%% with `flush_timer = undefined`. Pending `await_durable/3` waiters
%% are not replied to here — clients' `gen_server:call/3` raises with
%% the writer's exit signal, which is the natural shutdown contract.
cancel_flush_timer(#state{flush_timer = undefined} = S) -> S;
cancel_flush_timer(#state{flush_timer = TRef} = S) when is_reference(TRef) ->
    _ = erlang:cancel_timer(TRef),
    S#state{flush_timer = undefined}.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
do_open(InstanceId, Opts) ->
    case maps:find(origin, Opts) of
        {ok, Origin} ->
            case bondy_oplog_origin:validate(Origin) of
                ok ->
                    open_after_origin_validated(InstanceId, Origin, Opts);
                {error, R} ->
                    {error, {invalid_origin, R}}
            end;
        error ->
            {error, {missing_opt, origin}}
    end.

%% @private
open_after_origin_validated(InstanceId, Origin, Opts) ->
    case validate_durability_opts(Opts) of
        ok ->
            open_after_opts_validated(InstanceId, Origin, Opts);
        {error, _} = E ->
            E
    end.

%% @private
%% Reject malformed `fsync_mode` / interval / size opts at init time so
%% the gen_server doesn't start with a state that would explode on
%% first batched-mode timer arming.
validate_durability_opts(Opts) ->
    case maps:get(fsync_mode, Opts, ?BONDY_OPLOG_WAL_FSYNC_MODE_DEFAULT) of
        per_write -> ok;
        batched -> validate_batched_opts(Opts);
        Other -> {error, {invalid_opt, fsync_mode, Other}}
    end.

%% @private
validate_batched_opts(Opts) ->
    Interval = maps:get(
        batched_fsync_interval, Opts,
        ?BONDY_OPLOG_WAL_BATCHED_FSYNC_INTERVAL_DEFAULT_MS
    ),
    Bytes = maps:get(
        batched_fsync_bytes, Opts,
        ?BONDY_OPLOG_WAL_BATCHED_FSYNC_BYTES_DEFAULT
    ),
    case is_integer(Interval) andalso Interval >= 1 of
        false -> {error, {invalid_opt, batched_fsync_interval, Interval}};
        true ->
            case is_integer(Bytes) andalso Bytes >= 1 of
                false -> {error, {invalid_opt, batched_fsync_bytes, Bytes}};
                true -> ok
            end
    end.

%% @private
open_after_opts_validated(InstanceId, Origin, Opts) ->
    case maps:find(dir, Opts) of
        {ok, BaseDir} ->
            Dir = per_instance_dir(BaseDir, InstanceId),
            MaxBytes = maps:get(
                max_segment_bytes, Opts, ?DEFAULT_MAX_SEGMENT_BYTES
            ),
            Retention = maps:get(retention, Opts, []),
            IdxInterval = maps:get(
                idx_interval_bytes,
                Opts,
                ?BONDY_OPLOG_WAL_IDX_DEFAULT_INTERVAL_BYTES
            ),
            FsyncMode = maps:get(
                fsync_mode, Opts, ?BONDY_OPLOG_WAL_FSYNC_MODE_DEFAULT
            ),
            Interval = maps:get(
                batched_fsync_interval, Opts,
                ?BONDY_OPLOG_WAL_BATCHED_FSYNC_INTERVAL_DEFAULT_MS
            ),
            Bytes = maps:get(
                batched_fsync_bytes, Opts,
                ?BONDY_OPLOG_WAL_BATCHED_FSYNC_BYTES_DEFAULT
            ),
            State0 = #state{
                instance_id = InstanceId,
                dir = Dir,
                origin = Origin,
                max_segment_bytes = MaxBytes,
                retention = Retention,
                segment_id = 0,
                current_offset = ?SEG_HEADER_BYTES,
                append_count = 0,
                idx_interval_bytes = IdxInterval,
                idx_acc = bondy_oplog_wal_idx:new(IdxInterval),
                fsync_mode = FsyncMode,
                batched_fsync_interval = Interval,
                batched_fsync_bytes = Bytes,
                pending_fsync_bytes = 0,
                last_fsync_at = undefined,
                flush_timer = undefined,
                durable_segment_id = 0,
                durable_offset = ?SEG_HEADER_BYTES,
                waiters = []
            },
            open_or_recover(Dir, InstanceId, Origin, IdxInterval, State0);
        error ->
            {error, {missing_opt, dir}}
    end.

%% @private
%% Branches on whether the WAL directory has been used before:
%%
%% - No manifest: this is a fresh WAL. Create the directory and the
%%   first segment (`bootstrap/1`).
%% - Manifest exists: this WAL has prior state. Run the recovery
%%   procedure (`bondy_oplog_wal_recovery`) to validate the manifest,
%%   clean orphans, scan and truncate the head segment, rebuild missing
%%   `.qidx` files, and clamp the consumer offset to a real frame
%%   boundary.
open_or_recover(Dir, InstanceId, Origin, IdxInterval, State0) ->
    case filelib:ensure_path(Dir) of
        ok ->
            ManifestPath = filename:join(
                Dir, ?BONDY_OPLOG_WAL_MANIFEST_FILENAME
            ),
            case filelib:is_regular(ManifestPath) of
                false ->
                    bootstrap(State0);
                true ->
                    case bondy_oplog_wal_recovery:recover(
                        Dir, InstanceId, Origin, IdxInterval
                    ) of
                        {ok, Result} ->
                            install_recovery(State0, Result);
                        {error, _} = E ->
                            E
                    end
            end;
        {error, _} = E ->
            E
    end.

%% @private
per_instance_dir(BaseDir, InstanceId) ->
    filename:join(BaseDir, InstanceId).

%% @private
%% Builds a `#state{}` from the recovery result and publishes the head
%% atomics. The recovery procedure already opened the head fd R/W and
%% positioned it past the last valid frame; we only need to wrap it
%% in state plus initialise the atomics ref the readers use.
install_recovery(State0, Result) ->
    #{
        manifest := Manifest,
        head_fd := Fd,
        head_segment_id := SegId,
        head_offset := Off,
        first_hlc := FirstHlc,
        last_hlc := LastHlc,
        append_count := N,
        idx_acc := IdxAcc
    } = Result,
    HeadRef = atomics:new(2, [{signed, false}]),
    DurableRef = atomics:new(2, [{signed, false}]),
    publish_head_pos(HeadRef, SegId, Off),
    %% On recovery, every frame on disk has been fsynced (the head's
    %% break-and-truncate scan only accepts CRC-valid frames, and the
    %% writer datasyncs before publishing head_pos on every prior
    %% successful append). Durable ≡ head at this instant.
    publish_durable_pos(DurableRef, SegId, Off),
    State = State0#state{
        head_fd = Fd,
        segment_id = SegId,
        current_offset = Off,
        first_hlc = FirstHlc,
        last_hlc = LastHlc,
        append_count = N,
        manifest = Manifest,
        head_pos_ref = HeadRef,
        idx_acc = IdxAcc,
        durable_pos_ref = DurableRef,
        durable_segment_id = SegId,
        durable_offset = Off
    },
    {ok, State}.

%% @private
bootstrap(#state{} = State) ->
    SegId = State#state.segment_id,
    SegPath = segment_path(State#state.dir, SegId),
    case bondy_oplog_wal_segment:create(
        SegPath, SegId, State#state.instance_id, State#state.origin
    ) of
        {ok, Fd, _Header} ->
            Manifest = bondy_oplog_wal_manifest:new(
                State#state.instance_id, SegId, State#state.retention
            ),
            case bondy_oplog_wal_manifest:write(State#state.dir, Manifest) of
                ok ->
                    HeadRef = atomics:new(2, [{signed, false}]),
                    DurableRef = atomics:new(2, [{signed, false}]),
                    publish_head_pos(HeadRef, SegId, ?SEG_HEADER_BYTES),
                    publish_durable_pos(
                        DurableRef, SegId, ?SEG_HEADER_BYTES
                    ),
                    %% Fresh-open: segment header is datasync'd inside
                    %% `bondy_oplog_wal_segment:create/4`, so durable
                    %% equals head from the first instant after open.
                    {ok, State#state{
                        head_fd = Fd,
                        manifest = Manifest,
                        head_pos_ref = HeadRef,
                        durable_pos_ref = DurableRef,
                        durable_segment_id = SegId,
                        durable_offset = ?SEG_HEADER_BYTES
                    }};
                {error, _} = E ->
                    %% The .qdata is on disk but the manifest write
                    %% failed — without a manifest the segment is an
                    %% orphan that would confuse recovery. Roll back
                    %% here so retries see a clean directory.
                    _ = prim_file:close(Fd),
                    _ = prim_file:delete(SegPath),
                    E
            end;
        {error, _} = E ->
            E
    end.

%% @private
segment_path(Dir, SegId) ->
    filename:join(Dir, bondy_oplog_wal_segment:filename(SegId)).

%% @private
do_append(#state{} = State0, Event) ->
    Hlc = bondy_oplog_event:key_hlc(bondy_oplog_event:key(Event)),
    Body = term_to_binary(
        [Event], [{minor_version, 2}, deterministic]
    ),
    BodySize = byte_size(Body),
    FrameLen = ?FRAME_HEADER_BYTES + BodySize,
    case maybe_rotate(State0, FrameLen) of
        {ok, State1} ->
            write_frame(State1, Body, FrameLen, Hlc);
        {error, _} = E ->
            E
    end.

%% @private
%% Rotate only when the segment already holds at least one frame.
%% Without that guard a frame larger than `max_segment_bytes - 48`
%% would loop forever; the future atomic-batch path will surface that
%% as `{error, batch_too_large}` instead.
maybe_rotate(
    #state{current_offset = Cur, max_segment_bytes = Max} = State, FrameLen
) when Cur > ?SEG_HEADER_BYTES, Cur + FrameLen > Max ->
    rotate(State);
maybe_rotate(State, _FrameLen) ->
    {ok, State}.

%% @private
rotate(#state{
    head_fd = OldFd, segment_id = OldSegId, current_offset = OldOff
} = State0) ->
    case prim_file:datasync(OldFd) of
        ok ->
            %% The just-sealed segment is now fully durable. Advance the
            %% durable boundary so any `await_durable/3` waiters at
            %% offsets ≤ OldOff get woken before the rotation moves on.
            %% In batched mode this is also the implicit fsync of any
            %% pending writes — clear `pending_fsync_bytes` and cancel
            %% the interval timer here so the next batched window
            %% starts fresh in the new segment.
            State1 = advance_durable(State0, OldSegId, OldOff),
            close_sealed_segment(OldFd, OldSegId),
            %% `.qidx` is a best-effort accelerator (recovery rebuilds
            %% from the segment scan, per WAL_DESIGN.md §7.3). A flush
            %% failure here must not abort rotation — aborting after
            %% the old fd has already been closed would leave the
            %% writer's state holding a stale closed fd. Log and
            %% continue; recovery will rebuild the file next open.
            _ = flush_sealed_idx(State1),
            open_next_segment(State1#state{head_fd = undefined});
        {error, _} = E ->
            E
    end.

%% @private
%% Closing a successfully-datasynced fd is a courtesy: the data is
%% already durable, so a close failure does not cost durability. Log
%% and keep going rather than crash the writer mid-rotation.
close_sealed_segment(Fd, SegId) ->
    case prim_file:close(Fd) of
        ok ->
            ok;
        {error, Reason} ->
            ?LOG_WARNING(#{
                description =>
                    "prim_file:close/1 of sealed WAL segment fd failed; "
                    "data is already datasync'd so durability is intact, "
                    "but the fd may leak until the port is GC'd",
                segment => SegId,
                reason => Reason
            }),
            ok
    end.

%% @private
open_next_segment(#state{
    segment_id = OldSegId,
    first_hlc = OldFirstHlc,
    dir = Dir,
    instance_id = InstanceId,
    origin = Origin,
    head_pos_ref = HeadRef,
    durable_pos_ref = DurableRef,
    idx_interval_bytes = IdxInterval
} = State) ->
    NewSegId = OldSegId + 1,
    NewPath = segment_path(Dir, NewSegId),
    case bondy_oplog_wal_segment:create(
        NewPath, NewSegId, InstanceId, Origin
    ) of
        {ok, NewFd, _Header} ->
            case commit_rotation(State, NewSegId, OldFirstHlc) of
                {ok, NewManifest} ->
                    publish_head_pos(HeadRef, NewSegId, ?SEG_HEADER_BYTES),
                    %% Segment header is datasync'd inside
                    %% `bondy_oplog_wal_segment:create/4`, so the new
                    %% segment's first 48 bytes are durable. Advance
                    %% durable to match — and wake any waiter parked
                    %% at the new segment's empty boundary (rare, but
                    %% legal under the `await_durable/3` contract).
                    publish_durable_pos(
                        DurableRef, NewSegId, ?SEG_HEADER_BYTES
                    ),
                    State1 = State#state{
                        head_fd = NewFd,
                        segment_id = NewSegId,
                        current_offset = ?SEG_HEADER_BYTES,
                        first_hlc = undefined,
                        manifest = NewManifest,
                        idx_acc = bondy_oplog_wal_idx:new(IdxInterval),
                        durable_segment_id = NewSegId,
                        durable_offset = ?SEG_HEADER_BYTES
                    },
                    State2 = notify_durable_waiters(State1),
                    {ok, State2};
                {error, _} = E ->
                    %% Pre-commit failure: the new segment file is on
                    %% disk but the manifest still names the old one.
                    %% Delete the orphan eagerly so a retry of `rotate`
                    %% can `create/4` the same segment id without
                    %% colliding on the `exclusive` open.
                    _ = prim_file:close(NewFd),
                    _ = prim_file:delete(NewPath),
                    E
            end;
        {error, _} = E ->
            E
    end.

%% @private
%% Commit point of rotation: update the cached manifest in memory and
%% atomically rewrite the on-disk file. On success the new manifest is
%% returned so the caller can install it in state.
commit_rotation(
    #state{manifest = M0, dir = Dir}, NewSegId, PrevSegmentFirstHlc
) ->
    M1 = bondy_oplog_wal_manifest:with_current_segment(
        M0, NewSegId, PrevSegmentFirstHlc
    ),
    case bondy_oplog_wal_manifest:write(Dir, M1) of
        ok -> {ok, M1};
        {error, _} = E -> E
    end.

%% @private
%% Writes a frame to the head segment and performs the mode-specific
%% durability step:
%%
%% - `per_write`: datasync immediately; advance durable and notify any
%%   `await_durable/3` waiters covered by the new boundary.
%% - `batched`: accumulate bytes; trigger a fsync if the size threshold
%%   is reached; otherwise arm (or leave armed) the interval timer so
%%   `flush_tick` will fsync within `batched_fsync_interval` ms.
%%
%% The head_pos_ref publish happens after the durability step so that
%% in per_write mode tail readers only ever see frames whose bytes are
%% durable. In batched mode tail readers may observe non-durable frames
%% — the applier must `await_durable/3` before committing past them
%% (per WAL_DESIGN.md §8.2).
write_frame(
    #state{head_fd = Fd, current_offset = Off, segment_id = Seg,
           head_pos_ref = HeadRef, idx_acc = Acc0} = State0,
    Body, FrameLen, Hlc
) ->
    Frame = bondy_oplog_wal_frame:encode(Body),
    case prim_file:write(Fd, Frame) of
        ok ->
            NewOff = Off + FrameLen,
            %% Record the frame in the sparse-index accumulator after
            %% the write succeeded but before any fsync. A crash
            %% between write and fsync drops the frame from the on-disk
            %% tail (recovery's break-and-truncate); the in-memory idx
            %% entry never persists because the writer dies with it.
            Acc1 = bondy_oplog_wal_idx:note_frame(
                Acc0, Hlc, Off, FrameLen
            ),
            State1 = State0#state{
                current_offset = NewOff,
                first_hlc = pick_first_hlc(State0#state.first_hlc, Hlc),
                last_hlc = Hlc,
                append_count = State0#state.append_count + 1,
                idx_acc = Acc1
            },
            case post_write_durability(State1, FrameLen) of
                {ok, State2} ->
                    publish_head_offset(HeadRef, NewOff),
                    {ok, Hlc, {Seg, Off}, State2};
                {error, _} = E ->
                    %% Datasync failed in per_write mode. The byte
                    %% range is on disk but not durable — surface the
                    %% error and leave `state.current_offset` advanced
                    %% so a retry doesn't double-write the same frame.
                    %% The caller decides whether to retry; recovery
                    %% would truncate any partially-persisted frame on
                    %% the next open.
                    publish_head_offset(HeadRef, NewOff),
                    E
            end;
        {error, _} = E ->
            E
    end.

%% @private
%% Mode-specific durability step. The return contract differs by mode:
%%
%% - `per_write`: returns `{ok, State}` on a successful datasync, or
%%   `{error, Reason}` so the caller surfaces the failure to the
%%   client (durability was promised — silently swallowing the error
%%   would break the contract).
%%
%% - `batched`: ALWAYS returns `{ok, State}`. A failed size-triggered
%%   fsync is logged and retried via the interval timer; the batched
%%   contract is "best-effort fsync at some later boundary", so a
%%   single failed attempt is not promoted to a per-append error.
post_write_durability(#state{fsync_mode = per_write} = State, _FrameLen) ->
    case do_fsync_head(State) of
        {ok, _} = OK -> OK;
        {error, _} = E -> E
    end;
post_write_durability(
    #state{fsync_mode = batched, pending_fsync_bytes = P} = State, FrameLen
) ->
    State1 = State#state{pending_fsync_bytes = P + FrameLen},
    case maybe_size_trigger_fsync(State1) of
        {ok, State2} ->
            {ok, maybe_arm_flush_timer(State2)};
        {error, _} ->
            {ok, maybe_arm_flush_timer(State1)}
    end.

%% @private
pick_first_hlc(undefined, Hlc) -> Hlc;
pick_first_hlc(Existing, _) -> Existing.

%% =============================================================================
%% Durability + waiter management
%% =============================================================================

%% @private
%% Fsync the head fd and advance the durable boundary. Used by
%% `sync/1`, by per_write append, and by batched-mode fsyncs (size and
%% timer-triggered via `maybe_batched_fsync/1`).
do_fsync_head(
    #state{head_fd = Fd, segment_id = Seg, current_offset = Off} = State
) when Fd =/= undefined ->
    case prim_file:datasync(Fd) of
        ok ->
            {ok, advance_durable(State, Seg, Off)};
        {error, _} = E ->
            E
    end.

%% @private
%% Try a size-triggered fsync. Returns the original state if the
%% threshold has not been crossed; otherwise fsyncs and advances
%% durable.
maybe_size_trigger_fsync(
    #state{pending_fsync_bytes = P, batched_fsync_bytes = T} = State
) when P >= T ->
    do_fsync_head(State);
maybe_size_trigger_fsync(State) ->
    {ok, State}.

%% @private
%% Fsync if any bytes are pending. Called from the `flush_tick` timer
%% handler. Returns `{ok, State}` (possibly unchanged) or `{error, _}`
%% on datasync failure — the timer handler treats the error as
%% best-effort and leaves the pending bytes for a later attempt.
maybe_batched_fsync(#state{pending_fsync_bytes = 0} = S) -> {ok, S};
maybe_batched_fsync(#state{head_fd = undefined} = S) -> {ok, S};
maybe_batched_fsync(State) ->
    do_fsync_head(State).

%% @private
%% Arm a `flush_tick` interval timer if one isn't already scheduled and
%% there are pending bytes. Per_write callers are a no-op (they never
%% accumulate pending bytes; defensive guard).
maybe_arm_flush_timer(#state{fsync_mode = per_write} = S) -> S;
maybe_arm_flush_timer(#state{flush_timer = T} = S) when is_reference(T) -> S;
maybe_arm_flush_timer(#state{pending_fsync_bytes = 0} = S) -> S;
maybe_arm_flush_timer(#state{batched_fsync_interval = Ms} = S) ->
    TRef = erlang:send_after(Ms, self(), flush_tick),
    S#state{flush_timer = TRef}.

%% @private
%% Advance the durable position to `{Seg, Off}`. Publishes to the
%% atomics ref, resets the batched-mode bookkeeping, and wakes any
%% `await_durable/3` waiters at or below the new position.
advance_durable(
    #state{durable_segment_id = DSeg, durable_offset = DOff} = State,
    Seg, Off
) when {DSeg, DOff} >= {Seg, Off} ->
    %% Idempotent / monotonic: the durable boundary only moves
    %% forward. A redundant call (e.g. `sync/1` with no new bytes) is
    %% a no-op for durable state but still resets pending bookkeeping
    %% — datasync was issued, so any in-flight pending bytes were
    %% serviced and `last_fsync_at` should advance.
    State#state{
        pending_fsync_bytes = 0,
        last_fsync_at = erlang:monotonic_time(millisecond),
        flush_timer = cancel_and_clear_timer(State#state.flush_timer)
    };
advance_durable(State, Seg, Off) ->
    publish_durable_pos(State#state.durable_pos_ref, Seg, Off),
    State1 = State#state{
        durable_segment_id = Seg,
        durable_offset = Off,
        pending_fsync_bytes = 0,
        last_fsync_at = erlang:monotonic_time(millisecond),
        flush_timer = cancel_and_clear_timer(State#state.flush_timer)
    },
    notify_durable_waiters(State1).

%% @private
notify_durable_waiters(
    #state{
        durable_segment_id = Seg, durable_offset = Off, waiters = Ws
    } = State
) ->
    State#state{waiters = satisfy_waiters_up_to({Seg, Off}, Ws)}.

%% @private
%% `Waiters` is sorted ascending by `#waiter.pos`. Reply `ok` to each
%% waiter at or below `DurablePos`; cancel its timer; return the
%% unsatisfied tail.
satisfy_waiters_up_to(DurablePos, Waiters) ->
    {Satisfied, Pending} = lists:splitwith(
        fun(#waiter{pos = WPos}) -> WPos =< DurablePos end,
        Waiters
    ),
    lists:foreach(
        fun(#waiter{from = From, tref = TRef}) ->
            cancel_and_clear_timer(TRef),
            gen_server:reply(From, ok)
        end,
        Satisfied
    ),
    Pending.

%% @private
%% Handles a `{await_durable, Pos, Timeout}` gen_server call. Replies
%% immediately if already durable; otherwise registers a waiter and
%% returns `{noreply, _}` so the caller blocks until satisfied or the
%% timer fires.
handle_await_durable(
    {Seg, Off}, _Timeout, _From,
    #state{durable_segment_id = DSeg, durable_offset = DOff} = State
) when {Seg, Off} =< {DSeg, DOff} ->
    {reply, ok, State};
handle_await_durable(_Pos, 0, _From, State) ->
    %% Zero-timeout fast path: never blocks. Useful for "is this pos
    %% durable?" probes without the gen_server round-trip churn of a
    %% start_timer / cancel_timer pair.
    {reply, {error, timeout}, State};
handle_await_durable(Pos, Timeout, From, State) ->
    WaiterId = make_ref(),
    TRef = case Timeout of
        infinity ->
            infinity;
        T when is_integer(T), T > 0 ->
            erlang:start_timer(T, self(), {await_timeout, WaiterId})
    end,
    W = #waiter{id = WaiterId, pos = Pos, from = From, tref = TRef},
    NewWaiters = insert_waiter(W, State#state.waiters),
    {noreply, State#state{waiters = NewWaiters}}.

%% @private
%% Ordered insert by `#waiter.pos` ascending. Waiters at the same
%% position append after existing ones (FIFO for tie-breaking).
insert_waiter(#waiter{pos = WPos} = New, Waiters) ->
    {Before, After} = lists:splitwith(
        fun(#waiter{pos = P}) -> P =< WPos end,
        Waiters
    ),
    Before ++ [New | After].

%% @private
%% Remove a waiter by id (timeout path). Replies `{error, timeout}` if
%% the waiter is still in the list; no-op if it was satisfied between
%% the timer firing and this handler running.
expire_waiter(WaiterId, #state{waiters = Ws} = State) ->
    case lists:partition(
        fun(#waiter{id = Id}) -> Id =:= WaiterId end, Ws
    ) of
        {[#waiter{from = From}], Rest} ->
            gen_server:reply(From, {error, timeout}),
            State#state{waiters = Rest};
        {[], _} ->
            State
    end.

%% @private
%% Cancel a timer if one is set, returning `undefined` so the field can
%% be reset uniformly. Accepts the three possible field values:
%% `undefined` (no timer set), `infinity` (sentinel for "no deadline" on
%% a `#waiter{}`), and a real `reference()` from `erlang:start_timer/3`.
cancel_and_clear_timer(undefined) -> undefined;
cancel_and_clear_timer(infinity) -> undefined;
cancel_and_clear_timer(TRef) when is_reference(TRef) ->
    _ = erlang:cancel_timer(TRef),
    undefined.

%% @private
build_info(#state{} = State) ->
    #{
        instance_id => State#state.instance_id,
        dir => State#state.dir,
        origin => State#state.origin,
        max_segment_bytes => State#state.max_segment_bytes,
        current_segment => State#state.segment_id,
        head_offset => State#state.current_offset,
        durable_segment => State#state.durable_segment_id,
        durable_offset => State#state.durable_offset,
        first_hlc => State#state.first_hlc,
        last_hlc => State#state.last_hlc,
        append_count => State#state.append_count,
        fsync_mode => State#state.fsync_mode,
        batched_fsync_interval => State#state.batched_fsync_interval,
        batched_fsync_bytes => State#state.batched_fsync_bytes,
        pending_fsync_bytes => State#state.pending_fsync_bytes,
        last_fsync_at => State#state.last_fsync_at,
        waiter_count => length(State#state.waiters)
    }.

%% @private
build_reader_view(#state{manifest = Manifest} = State) ->
    #{
        instance_id => State#state.instance_id,
        dir => State#state.dir,
        origin => State#state.origin,
        head_pos_ref => State#state.head_pos_ref,
        durable_pos_ref => State#state.durable_pos_ref,
        %% Atomic snapshot of the writer's current head position, read
        %% from gen_server state under the call's serialisation. The
        %% reader uses this for `tail` start positions instead of two
        %% separate `atomics:get/2` calls (which can race with
        %% rotation and yield an inconsistent `{SegA, 48}` pair where
        %% the actual head is `{SegA+1, 48}`).
        head_pos => {State#state.segment_id, State#state.current_offset},
        current_segment => State#state.segment_id,
        %% Sealed segments carry the FirstHlc the manifest captured at
        %% rotation; the head segment's manifest entry has FirstHlc =
        %% `undefined` until it rotates and the post-rotation manifest
        %% write commits the captured value. The reader patches the
        %% head segment's entry with `head_first_hlc` below for HLC
        %% seek via the sparse index.
        live_segments =>
            bondy_oplog_wal_manifest:live_segments(Manifest),
        deleted_through =>
            bondy_oplog_wal_manifest:deleted_through(Manifest),
        %% Live first-HLC of the head segment, captured by the writer
        %% on the first append into the segment but not yet persisted
        %% in the manifest (the manifest only learns about it on the
        %% next rotation). `undefined` if no events have been written
        %% into the head segment yet.
        head_first_hlc => State#state.first_hlc,
        %% Sparse-index entries the writer has accumulated for the head
        %% segment but not yet flushed to `.qidx` (the file is written
        %% only on rotation and `terminate/2`). Readers doing HLC seek
        %% into the head segment wrap these via
        %% `bondy_oplog_wal_idx:from_entries/1` to avoid a redundant
        %% file read of data the writer already has in RAM.
        head_idx_entries => head_idx_entries(State)
    }.

%% @private
head_idx_entries(#state{idx_acc = undefined}) -> [];
head_idx_entries(#state{idx_acc = Acc}) ->
    bondy_oplog_wal_idx:entries(Acc).

%% @private
%% Writes the in-memory index accumulator for the just-sealed segment to
%% disk as `<seg>.qidx`. Called from `rotate/1` after the head segment's
%% `.qdata` is datasynced and closed, so the index is durably paired
%% with the data it describes before the manifest commit publishes the
%% new head segment.
flush_sealed_idx(#state{
    dir = Dir, segment_id = SegId, idx_acc = Acc
}) ->
    Entries = bondy_oplog_wal_idx:entries(Acc),
    Path = idx_path(Dir, SegId),
    case bondy_oplog_wal_idx:write_file(Path, Entries) of
        ok ->
            ok;
        {error, Reason} = E ->
            %% A failed `.qidx` write does not lose any committed event
            %% data — recovery rebuilds the file from a segment scan.
            %% Log and bubble up so the writer caller can decide
            %% whether to retry rotation.
            ?LOG_WARNING(#{
                description =>
                    "bondy_oplog_wal_idx:write_file/2 failed during "
                    "rotation; .qidx for the sealed segment will be "
                    "rebuilt on next recovery",
                segment => SegId,
                reason => Reason
            }),
            E
    end.

%% @private
%% Flushes the head segment's `.qidx` on normal `terminate/2`. Best
%% effort: a failure is logged but does not block shutdown, since
%% recovery rebuilds the file from a segment scan.
%%
%% Gate on the accumulator's entry count, not `append_count`. After
%% rotation `append_count` reflects events across all segments, while
%% `idx_acc` is reset per-segment — `append_count > 0` would falsely
%% trigger a 16-byte empty `.qidx` write for a head segment that has
%% had no appends since the last rotation.
flush_head_idx(#state{idx_acc = Acc} = State) ->
    case bondy_oplog_wal_idx:entry_count(Acc) of
        0 ->
            ok;
        _ ->
            do_flush_head_idx(State)
    end.

%% @private
do_flush_head_idx(#state{dir = Dir, segment_id = SegId, idx_acc = Acc}) ->
    Entries = bondy_oplog_wal_idx:entries(Acc),
    Path = idx_path(Dir, SegId),
    case bondy_oplog_wal_idx:write_file(Path, Entries) of
        ok ->
            ok;
        {error, Reason} ->
            ?LOG_WARNING(#{
                description =>
                    "bondy_oplog_wal_idx:write_file/2 failed during "
                    "shutdown; head segment .qidx will be rebuilt "
                    "on next recovery",
                segment => SegId,
                reason => Reason
            }),
            ok
    end.

%% @private
idx_path(Dir, SegId) ->
    filename:join(Dir, bondy_oplog_wal_idx:filename(SegId)).

%% @private
%% Publishes a full (SegId, Offset) head position. Updates `seg_id`
%% first, then `offset` — a racing reader that catches the intermediate
%% state sees (new_seg_id, old_offset) which interprets correctly as
%% "my segment is sealed; read it to EOF", which is true since the
%% writer has already datasynced and closed the previous fd before
%% calling here (see `rotate/1` → `close_sealed_segment/2`).
publish_head_pos(undefined, _SegId, _Offset) -> ok;
publish_head_pos(Ref, SegId, Offset) ->
    ok = atomics:put(Ref, 1, SegId),
    ok = atomics:put(Ref, 2, Offset).

%% @private
%% Publishes a new head offset within the current head segment. No
%% segment-id update — that's only done on rotation via
%% `publish_head_pos/3`.
publish_head_offset(undefined, _Offset) -> ok;
publish_head_offset(Ref, Offset) ->
    ok = atomics:put(Ref, 2, Offset).

%% @private
%% Publishes a full (SegId, Offset) durable position. Mirrors
%% `publish_head_pos/3` (slot 1 first, slot 2 second). A racing
%% wait-free reader using both slots may, during the cross-segment
%% transition, observe `(NewSegId, OldOff)` — that intermediate is
%% larger in tuple order than the prior consistent state, so reader
%% monotonicity is preserved, but the offset is meaningless to the new
%% segment (e.g., 64 MB into a segment that contains only 48 bytes).
%% `durable_position/1` (gen_server-serialised) is the consistent-
%% snapshot API for callers that need a coherent pair; atomics polling
%% is exposed for future hot-path consumers (the applier) which
%% compare against their own sub-segment positions and tolerate
%% same-segment monotonic reads.
publish_durable_pos(undefined, _SegId, _Offset) -> ok;
publish_durable_pos(Ref, SegId, Offset) ->
    ok = atomics:put(Ref, 1, SegId),
    ok = atomics:put(Ref, 2, Offset).
