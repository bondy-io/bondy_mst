%% =============================================================================
%%  bondy_oplog_wal_mem.erl -
%%
%%  Copyright (c) 2024-2026 Leapsight. All rights reserved.
%%
%%  Licensed under the Apache License, Version 2.0 (the "License");
%%  you may not use this file except in compliance with the License.
%%  You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%%  Unless required by applicable law or agreed to in writing, software
%%  distributed under the License is distributed on an "AS IS" BASIS,
%%  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%  See the License for the specific language governing permissions and
%%  limitations under the License.
%% =============================================================================

-module(bondy_oplog_wal_mem).
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("bondy_doc.hrl").
-include("bondy_oplog.hrl").

?MODULEDOC("""
In-memory (ETS) write-ahead log backend for **ephemeral fused** instances.

This is the throughput lever for the ephemeral 20k goal. The disk WAL
(`bondy_oplog_wal`) welds the drain's *visibility* of an event to the durable
position, which only advances at an fsync boundary; so even in `batched` mode
the fused drain consumes a batch only after a `datasync` completes. That
durability latency — not the install round-trip (H1, already removed) — is the
binding constraint at ~42% scheduler utilisation.

For an **ephemeral** instance that constraint is redundant: the projection and
MST are themselves ETS (lost on node death) and durability is already
cluster-provided via anti-entropy (`integrate_peer_root` is not gated on
durability). This module makes the WAL ephemeral too: events live in a
`protected ordered_set` keyed by a dense monotonic `Seq`, the fused drain reads
them via `bondy_oplog_wal_mem_reader` the instant they are inserted, and there
is **no fsync and no disk I/O on the ack path**. `head == durable` at all times.

### Shared protocol, one different surface

This gen_server speaks the **same `gen_server` message protocol** as
`bondy_oplog_wal` for the producer + control surfaces:

- `{append_batch, Events}` — assign a `Seq` range, insert, bump head.
- `{await_durable, {Seg, Off}, Timeout}` — block until `head_seq >= Off`.
- `durable_position` — `{?MEM_SEG, head_seq}` (head is always durable here).
- `{set_committed_segment, Seg}` — retention marker (no-op for now; GC is PR-3).

So callers reach this module through the existing `bondy_oplog_wal:append_batch/2`,
`await_durable/3`, `durable_position/1` and `set_committed_segment/2` wrappers
(all of which are module-agnostic `gen_server:call/2,3` shims) with the mem-WAL
pid — **no producer-side dispatch is needed**. The single surface that genuinely
differs is the *reader*: the disk reader reads segment files via an fd, the mem
reader reads the ETS table. That is dispatched in the fused drain
(`bondy_oplog_instance`) on a `wal_backend` flag; `bondy_oplog_wal`,
`bondy_oplog_wal_reader` and `bondy_oplog_applier` stay untouched.

### Durability decision (cluster-durable)

Dropping the fsync widens the acked-but-not-yet-replicated loss window to also
include a BEAM crash with surviving disk (the disk WAL would replay it). This is
accepted by design for ephemeral and covered by anti-entropy in normal
operation — see `_design/latest/EPHEMERAL_ETS_WAL_PLAN.md` §2 (Decision A).

### Deferred (PR-3)

- Crash recovery: an `heir`-owned table survives a writer process crash. Today
  the table dies with this gen_server (node/process death → re-sync from peers).
- Retention/GC: delete entries below the committed `Seq` via `ets:select_delete`.
  Until then the log grows for the run; the byte cap (`max_total_wal_size`) bounds
  it via `{error, wal_full}` backpressure, sufficient for bounded benches.
""").

%% A mem WAL has a single logical segment; `Seq` plays the byte-offset role in
%% the `{Segment, Offset}` position shape the consumer-offset machinery expects.
-define(MEM_SEG, 0).

%% Backpressure cap on the LIVE (un-GC'd) event count: `head_seq -
%% committed_seq`. With commit-cadence GC the live set is small in steady
%% state, so this is a safety bound for a stalled drain, not the steady-state
%% size. Count-based (not bytes) so append stays allocation-free — no
%% per-event `external_size`.
-define(DEFAULT_MAX_LIVE_EVENTS, 2_000_000).

-record(waiter, {
    id :: pos_integer(),
    from :: gen_server:from(),
    target :: pos_integer(),
    timer :: undefined | reference()
}).

-record(state, {
    instance_id :: binary(),
    origin :: bondy_oplog_origin:t(),
    tab :: ets:tid(),
    head_seq = 0 :: non_neg_integer(),
    committed_seq = 0 :: non_neg_integer(),
    max_live_events :: pos_integer(),
    append_count = 0 :: non_neg_integer(),
    waiter_seq = 0 :: non_neg_integer(),
    waiters = [] :: [#waiter{}]
}).

%% API
-export([start_link/2]).
-export([reader_view/1]).
-export([set_committed_seq/2]).
-export([info/1]).

%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).

%% =============================================================================
%% API
%% =============================================================================

?DOC("""
Starts the per-instance in-memory WAL writer. Same start contract as
`bondy_oplog_wal:start_link/2` so `bondy_oplog_instance_sup` can swap the child
module on `wal_backend => mem`.
""").
-spec start_link(binary(), map()) -> {ok, pid()} | {error, term()}.

start_link(InstanceId, Opts) when is_binary(InstanceId), is_map(Opts) ->
    gen_server:start_link(?MODULE, {InstanceId, Opts}, []).

?DOC("""
Returns the read-side view the mem reader needs: the ETS tid and the logical
segment id. The table is `protected`, so any process holding the tid may read
it lock-free.
""").
-spec reader_view(pid()) -> #{tab => ets:tid(), mem_seg => non_neg_integer()}.

reader_view(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, reader_view, infinity).

?DOC("""
Marks every event with `Seq =< CommittedSeq` as consumed by the drain (read +
installed) and GCs them from the table. Cast (best-effort, non-blocking): the
drain calls this at each commit boundary with its reader cursor. Safe because an
ephemeral mem WAL is never replayed from disk on restart — durability is
cluster-provided (re-sync from peers), so an installed event is dead weight.
""").
-spec set_committed_seq(pid(), non_neg_integer()) -> ok.

set_committed_seq(Pid, Seq) when is_pid(Pid), is_integer(Seq), Seq >= 0 ->
    gen_server:cast(Pid, {set_committed_seq, Seq}).

?DOC("Diagnostic snapshot of the mem WAL writer state.").
-spec info(pid()) -> map().

info(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, info, infinity).

%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================

init({InstanceId, Opts}) ->
    process_flag(trap_exit, true),
    Tab = ets:new(bondy_oplog_wal_mem, [
        ordered_set,
        protected,
        {read_concurrency, true}
    ]),
    MaxLive = maps:get(max_live_events, Opts, ?DEFAULT_MAX_LIVE_EVENTS),
    Origin = maps:get(origin, Opts, bondy_oplog_origin:default()),
    %% Publish our pid exactly as the disk WAL does, so `ensure_wal_pid/1`,
    %% the caller-side `fast_wal_append_batch/2`, and the fused drain all
    %% resolve this process via the registry.
    ok = bondy_oplog_registry:set_wal_pid(InstanceId, self()),
    {ok, #state{
        instance_id = InstanceId,
        origin = Origin,
        tab = Tab,
        max_live_events = MaxLive
    }}.

handle_call({append_batch, Events}, _From, State) ->
    do_append_batch(Events, State);
handle_call({await_durable, {_Seg, Off}, Timeout}, From, State) ->
    do_await_durable(Off, Timeout, From, State);
handle_call(durable_position, _From, #state{head_seq = H} = State) ->
    {reply, {?MEM_SEG, H}, State};
handle_call({set_committed_segment, _Seg}, _From, State) ->
    %% Retention marker. The committed *Seq* is tracked via the consumer
    %% offset on the drain side; segment-level retention is a no-op for the
    %% single-segment mem log. GC by committed Seq is PR-3.
    {reply, ok, State};
handle_call(reader_view, _From, #state{tab = Tab} = State) ->
    {reply, #{tab => Tab, mem_seg => ?MEM_SEG}, State};
handle_call(info, _From, State) ->
    {reply, info_map(State), State};
handle_call(_Msg, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({set_committed_seq, Seq}, State) ->
    {noreply, gc_committed(Seq, State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({await_timeout, Id}, State) ->
    {noreply, expire_waiter(Id, State)};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    %% The `protected` table is owned by this process and is deleted
    %% automatically on exit. An `heir` for process-crash recovery is PR-3.
    ok.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
%% Assign a contiguous `Seq` range, insert each event keyed by its `Seq`, bump
%% the head, satisfy any waiters now covered, and reply with the assigned
%% positions. No fsync, no file write — the reply is the ack. Backpressure is
%% on the LIVE (un-GC'd) event count: `head - committed + batch > cap` rejects
%% the whole batch (all-or-nothing, matching the disk WAL's `check_backpressure`).
do_append_batch(Events, State0) ->
    #state{head_seq = Head, committed_seq = Committed, max_live_events = Max} =
        State0,
    N = length(Events),
    case (Head - Committed) + N > Max of
        true ->
            telemetry:execute(
                [bondy_oplog, wal_mem, wal_full],
                #{live => Head - Committed, batch => N},
                #{instance_id => State0#state.instance_id}
            ),
            {reply, {error, wal_full}, State0};
        false ->
            {Entries, State1} = insert_events(Events, State0),
            State2 = signal_waiters(State1),
            {reply, {ok, Entries}, State2}
    end.

%% @private
insert_events(Events, State) ->
    insert_events(Events, State, []).

%% @private
insert_events([], #state{} = State, Acc) ->
    {lists:reverse(Acc), State#state{
        append_count = State#state.append_count + length(Acc)
    }};
insert_events([Event | Rest], #state{tab = Tab, head_seq = H} = State, Acc) ->
    Seq = H + 1,
    true = ets:insert(Tab, {Seq, Event}),
    Hlc = bondy_oplog_event:key_hlc(bondy_oplog_event:key(Event)),
    Entry = {Hlc, {?MEM_SEG, Seq}},
    insert_events(Rest, State#state{head_seq = Seq}, [Entry | Acc]).

%% @private
%% Replies `ok` immediately if `head_seq` already covers `Off`; otherwise
%% registers a waiter (with a timeout) and replies later from `signal_waiters/1`
%% or `expire_waiter/2`. The caller (the fused idle-waiter helper) ignores the
%% reply value — it re-drains on its own `DOWN` — so the protocol only needs to
%% release the helper once the position is visible or the deadline fires.
do_await_durable(Off, _Timeout, _From, #state{head_seq = H} = State) when
    H >= Off
->
    {reply, ok, State};
do_await_durable(Off, Timeout, From, State0) ->
    #state{waiter_seq = WS0, waiters = Ws} = State0,
    Id = WS0 + 1,
    TimerRef = arm_timeout(Timeout, Id),
    Waiter = #waiter{id = Id, from = From, target = Off, timer = TimerRef},
    {noreply, State0#state{waiter_seq = Id, waiters = [Waiter | Ws]}}.

%% @private
arm_timeout(infinity, _Id) ->
    undefined;
arm_timeout(Timeout, Id) when is_integer(Timeout), Timeout >= 0 ->
    erlang:send_after(Timeout, self(), {await_timeout, Id}).

%% @private
%% Release every waiter whose target Seq is now durable (== visible).
signal_waiters(#state{waiters = []} = State) ->
    State;
signal_waiters(#state{waiters = Ws, head_seq = H} = State) ->
    {Ready, Pending} = lists:partition(
        fun(#waiter{target = T}) -> T =< H end, Ws
    ),
    _ = [reply_waiter(W, ok) || W <- Ready],
    State#state{waiters = Pending}.

%% @private
expire_waiter(Id, #state{waiters = Ws} = State) ->
    case lists:keytake(Id, #waiter.id, Ws) of
        {value, W, Rest} ->
            _ = reply_waiter(W, {error, timeout}),
            State#state{waiters = Rest};
        false ->
            State
    end.

%% @private
reply_waiter(#waiter{from = From, timer = Timer}, Reply) ->
    _ = cancel_timer(Timer),
    gen_server:reply(From, Reply).

%% @private
cancel_timer(undefined) -> ok;
cancel_timer(Ref) -> erlang:cancel_timer(Ref).

%% @private
%% Delete every consumed row (`Seq =< Committed`) from the head of the
%% ordered_set. The matches are a contiguous prefix, so `ets:select_delete`
%% touches only the entries it removes (each deleted once → amortised O(1) per
%% event). `committed_seq` is monotonic. This is what keeps the table — and the
%% `ets:next` reader walk — bounded; without it the log grew to the whole run.
gc_committed(Seq, #state{committed_seq = Old} = State) when Seq =< Old ->
    State;
gc_committed(Seq, #state{tab = Tab, head_seq = Head} = State) ->
    Bounded = min(Seq, Head),
    MatchSpec = [{{'$1', '_'}, [{'=<', '$1', Bounded}], [true]}],
    _ = ets:select_delete(Tab, MatchSpec),
    State#state{committed_seq = Bounded}.

%% @private
info_map(#state{} = S) ->
    #{
        instance_id => S#state.instance_id,
        backend => mem,
        head_seq => S#state.head_seq,
        committed_seq => S#state.committed_seq,
        live_events => S#state.head_seq - S#state.committed_seq,
        max_live_events => S#state.max_live_events,
        append_count => S#state.append_count,
        waiters => length(S#state.waiters)
    }.
