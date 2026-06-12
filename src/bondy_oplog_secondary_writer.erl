%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_secondary_writer).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("bondy_doc.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Asynchronous writer for one secondary-index shard
(`MST_DB_DESIGN.md` §13). One lightweight gen_server per
`(NS, IndexName, SecShard)` triple, supervised by
`bondy_oplog_secondary_sup`.

## Why a separate process (not a `bondy_oplog_instance`)

The index keyspace has no WAL, no MST, and no signing — it is a pure
deterministic function of the primary, rebuilt from it on cold start
(IDX-4 backfill). A full `bondy_oplog_instance` subtree (`one_for_all`,
co-supervising WAL + MST it would never use) is the wrong shape. The
writer is a mini-applier over the index's ETS projection: it batches the
index ops the primary applier dispatches, read-modify-writes each touched
index cell through the `index_entry` fold, and bumps the shard's
freshness + high-water marks so `{max_lag, Ms}` reads stop refusing.

## Dispatch contract

The primary applier computes the term-diff of every cell it materialises
(old terms vs new terms of the cell's value) and `enqueue/3`s the
resulting index ops to the writer owning each touched secondary shard,
**after** its own projection `put_batch` returns ok. The writer therefore
only ever indexes durably-applied primary state. Ops are
`index_entry`-shaped against an explicit `(SecBucket, SecKey)` cell:

```
{put,    SecBucket, SecKey, Columns :: binary(), Hlc} |
{remove, SecBucket, SecKey, Hlc}
```

`SecKey` is the `(Term, PrimaryKey)` composite from
`bondy_oplog_index_key:encode/2`; `Hlc` is the *primary* cell's HLC, so
the index cell's LWW-over-presence fold converges identically regardless
of whether the op arrived from a local WAL drain or a peer replay.

## Coalescing

Ops buffer and flush on a short timer (`coalesce_ms`, default 5 ms) so a
burst of primary writes to the same secondary shard collapses into one
`put_batch`. `flush_sync/1` forces an immediate flush and is the
deterministic barrier tests (and a future read-side `await_index`) use
to observe a write without polling.

## Handles are re-resolved per flush

The writer caches nothing across flushes: each flush re-`lookup/3`s its
registry row for the projection adapter/handle, cache pair, freshness
atomics, and high-water ref. This keeps it correct across a registry
re-registration (epoch change) for free — and it never owns the ETS
tables it writes (the topology's DB-scoped owner does), so a writer
crash/restart loses only buffered, not-yet-flushed ops, which IDX-4's
rebuild recovers.

## Registry stamp

At init (and on the registry-restart epoch event) the writer stamps its
pid onto its registry row via `set_writer_pid/4` so the primary applier
can find it. The stamp does **not** transfer the registry monitor — the
projection-handle owner keeps it.
""").

-export([start_link/1]).
-export([enqueue/3]).
-export([flush_sync/1]).
-export([reset/1]).

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-type op() ::
    {put, SecBucket :: binary(), SecKey :: binary(), Columns :: binary(),
        Hlc :: non_neg_integer()}
    | {remove, SecBucket :: binary(), SecKey :: binary(),
        Hlc :: non_neg_integer()}.

-export_type([op/0]).

-record(state, {
    ns :: atom(),
    index_name :: atom(),
    shard :: non_neg_integer(),
    %% Buffered ops since the last flush, in reverse arrival order.
    buffer = [] :: [op()],
    %% Outstanding coalescing timer, or `undefined` when the buffer is
    %% empty / just flushed.
    flush_timer = undefined :: undefined | reference(),
    coalesce_ms :: non_neg_integer()
}).

-define(DEFAULT_COALESCE_MS, 5).
%% The native op-based CRDT backing every secondary-index cell (PR-Z; the
%% op-based twin of the retired `bondy_oplog_fold_index_entry`).
-define(INDEX_CRDT, bondy_oplog_crdt_index_entry).

%% =============================================================================
%% API
%% =============================================================================

-spec start_link(map()) -> {ok, pid()} | {error, term()}.

start_link(#{ns := _, index_name := _, shard := _} = Args) ->
    gen_server:start_link(?MODULE, Args, []).

-doc """
Enqueue a batch of index ops for this secondary shard. Fire-and-forget;
ops buffer and flush on the coalescing timer. `Hlc` is the primary
batch's max HLC, carried for telemetry only — the writer derives the
authoritative high-water mark from the ops it actually materialises.
""".
-spec enqueue(pid(), [op()], non_neg_integer()) -> ok.

enqueue(Pid, Ops, Hlc) when is_pid(Pid), is_list(Ops) ->
    gen_server:cast(Pid, {idx_update, Ops, Hlc}).

-doc """
Force an immediate flush of any buffered ops and return once the
projection write (and freshness/high-water bumps) have completed. The
deterministic barrier for tests and operational tooling: because casts
are processed in arrival order, a `flush_sync/1` issued after a sequence
of `enqueue/3`s observes all of them.
""".
-spec flush_sync(pid()) -> ok.

flush_sync(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, flush_sync, infinity).

-doc """
Discard the buffered ops without writing them and cancel the flush timer.
Used by the rebuild orchestrator (IDX-4) before a re-fold: the buffer may
hold stale ops (e.g. a `put` for a term the primary value no longer
yields, whose retracting `remove` was dropped on saturation), so applying
them would resurrect orphaned index entries. The rebuild discards the
buffer and resets the in-flight counter together, then re-derives the
correct ops from the primary.
""".
-spec reset(pid()) -> ok.

reset(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, reset, infinity).

%% =============================================================================
%% gen_server callbacks
%% =============================================================================

init(#{ns := NS, index_name := IName, shard := Shard} = Args) ->
    %% Per-op `{idx_update, …}` cast receiver under write load: keep the
    %% mailbox off the process heap so a transient backlog isn't re-scanned
    %% by the GC (same rationale as the instance/applier/WAL processes).
    process_flag(message_queue_data, off_heap),
    CoalesceMs = maps:get(coalesce_ms, Args, ?DEFAULT_COALESCE_MS),
    %% Stamp our pid so the primary applier can dispatch to us. The row
    %% was registered by `bondy_db` provisioning before we were started,
    %% so this normally succeeds; a `not_found` only happens if the index
    %% shard was torn down underneath us (we then idle until terminated).
    _ = bondy_db_core_registry:set_writer_pid(NS, IName, Shard, self()),
    %% Re-stamp if the registry restarts (epoch change drops every row).
    ok = bondy_db_core_events:subscribe(bondy_db_core_registry_started),
    %% IDX-4 crash recovery: if this shard was previously populated (AE
    %% bumped past the stale sentinel), already flagged, or left in-flight
    %% reservations behind (a buffer lost to the crash), a restart lost the
    %% buffer — request a rebuild to recover it. A first-ever start (still
    %% sentinel-stale, no flag, zero counter) is left to the startup backfill.
    ok = maybe_request_rebuild(NS, IName, Shard),
    {ok, #state{
        ns = NS,
        index_name = IName,
        shard = Shard,
        coalesce_ms = CoalesceMs
    }}.

%% @private
maybe_request_rebuild(NS, IName, Shard) ->
    Trigger =
        case bondy_db_core_registry:lookup(NS, IName, Shard) of
            {ok, Entry} ->
                %% Rebuild when: the shard is flagged; OR a crash/kill left
                %% un-flushed in-flight reservations behind (the lost buffer
                %% the rebuild recovers — `terminate/2` does not run on a
                %% `kill`, and the leaked reservation is the only post-restart
                %% signal that the dead instance had buffered work); OR the
                %% shard was previously populated (AE past the stale sentinel).
                %% A first-ever start is sentinel-stale, unflagged, with a zero
                %% counter, so it is left to the startup backfill.
                bondy_db_core_registry:index_needs_rebuild(Entry) orelse
                    bondy_db_core_registry:index_inflight(Entry) > 0 orelse
                    bondy_db_core_registry:entry_ever_freshened(Entry);
            not_found ->
                false
        end,
    case Trigger of
        true ->
            catch bondy_oplog_index_rebuild:request(NS, IName),
            ok;
        false ->
            ok
    end.

handle_call(flush_sync, _From, State0) ->
    State1 = cancel_timer(State0),
    State2 = do_flush(State1),
    {reply, ok, State2};
handle_call(reset, _From, State0) ->
    %% Drop the buffer unwritten and cancel the timer. The rebuild resets
    %% the in-flight counter separately, so they stay consistent.
    State1 = cancel_timer(State0),
    {reply, ok, State1#state{buffer = []}};
handle_call(_Req, _From, State) ->
    {reply, {error, unknown}, State}.

handle_cast({idx_update, Ops, _Hlc}, #state{buffer = Buf} = State0) ->
    State1 = State0#state{buffer = prepend(Ops, Buf)},
    {noreply, arm_timer(State1)};
handle_cast(_, State) ->
    {noreply, State}.

handle_info(flush, State0) ->
    State1 = State0#state{flush_timer = undefined},
    {noreply, do_flush(State1)};
handle_info(
    {bondy_db_core_event, bondy_db_core_registry_started, _Epoch},
    #state{ns = NS, index_name = IName, shard = Shard} = State
) ->
    %% The registry restarted: every row (including our writer_pid stamp)
    %% was lost. Re-stamp best-effort. If the projection-handle owner has
    %% not re-registered the row yet this returns `not_found`; the index
    %% is rebuildable, so we do not block on it.
    _ = bondy_db_core_registry:set_writer_pid(NS, IName, Shard, self()),
    {noreply, State};
handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% =============================================================================
%% INTERNAL — buffering
%% =============================================================================

%% Prepend a batch (kept reversed; restored to arrival order at flush).
prepend([], Buf) -> Buf;
prepend([Op | Rest], Buf) -> prepend(Rest, [Op | Buf]).

arm_timer(#state{flush_timer = Ref} = State) when is_reference(Ref) ->
    State;
arm_timer(#state{coalesce_ms = Ms} = State) ->
    Ref = erlang:send_after(Ms, self(), flush),
    State#state{flush_timer = Ref}.

cancel_timer(#state{flush_timer = undefined} = State) ->
    State;
cancel_timer(#state{flush_timer = Ref} = State) ->
    _ = erlang:cancel_timer(Ref),
    %% Drain a possibly already-delivered `flush` so it does not fire a
    %% redundant (harmless, empty) flush after we handled it synchronously.
    receive
        flush -> ok
    after 0 -> ok
    end,
    State#state{flush_timer = undefined}.

%% =============================================================================
%% INTERNAL — flush
%% =============================================================================

do_flush(#state{buffer = []} = State) ->
    State#state{flush_timer = undefined};
do_flush(
    #state{ns = NS, index_name = IName, shard = Shard, buffer = Buf} = State
) ->
    Ops = lists:reverse(Buf),
    case bondy_db_core_registry:lookup(NS, IName, Shard) of
        not_found ->
            ?LOG_DEBUG(#{
                description =>
                    "bondy_oplog_secondary_writer flush dropped: the "
                    "index shard's registry row is gone (torn down or "
                    "registry restarted before re-registration). The "
                    "index is rebuildable from the primary.",
                namespace => NS,
                index_name => IName,
                shard => Shard,
                dropped => length(Ops)
            }),
            State#state{buffer = [], flush_timer = undefined};
        {ok, Entry} ->
            do_write(NS, IName, Shard, Entry, Ops),
            State#state{buffer = [], flush_timer = undefined}
    end.

do_write(NS, IName, Shard, Entry, Ops) ->
    %% These ops leave the buffer now; release their in-flight reservation
    %% (added by the primary applier at dispatch) regardless of the write
    %% outcome — the back-pressure accounting tracks buffered ops, not
    %% successful writes.
    bondy_db_core_registry:index_inflight_sub(Entry, length(Ops)),
    Adapter = bondy_db_core_registry:entry_projection_adapter(Entry),
    Handle = bondy_db_core_registry:entry_projection_handle(Entry),
    CacheAdapter = bondy_db_core_registry:entry_cache_adapter(Entry),
    CacheHandle = bondy_db_core_registry:entry_cache_handle(Entry),
    AeRef = bondy_db_core_registry:entry_ae_atomics(Entry),
    HwRef = bondy_db_core_registry:entry_high_water_ref(Entry),
    T0 = erlang:monotonic_time(microsecond),
    {Writes, MaxHlc} = build_writes(Adapter, Handle, Ops),
    case Writes of
        [] ->
            ok;
        _ ->
            case Adapter:put_batch(Handle, Writes) of
                ok ->
                    lists:foreach(
                        fun({B, K, _F}) ->
                            invalidate_cache(CacheAdapter, CacheHandle, B, K)
                        end,
                        Writes
                    ),
                    bump_ae(AeRef),
                    advance_high_water(HwRef, MaxHlc),
                    telemetry:execute(
                        [bondy_oplog, secondary_writer, flush],
                        #{
                            duration_us =>
                                erlang:monotonic_time(microsecond) - T0,
                            cells => length(Writes),
                            ops => length(Ops),
                            inflight =>
                                bondy_db_core_registry:index_inflight(Entry)
                        },
                        #{namespace => NS, index_name => IName, shard => Shard}
                    );
                {error, Reason} ->
                    ?LOG_WARNING(#{
                        description =>
                            "bondy_oplog_secondary_writer projection write "
                            "failed; the affected index cells will be "
                            "rebuilt from the primary (IDX-4) or re-applied "
                            "on the next dispatch.",
                        namespace => NS,
                        index_name => IName,
                        shard => Shard,
                        cells => length(Writes),
                        reason => Reason
                    })
            end
    end.

%% Fold every op into a per-cell shadow state (read-modify-write against
%% the live projection, with in-batch reads seeing prior in-batch writes),
%% then encode one V2 `value_equals_state` frame per touched cell. The
%% high-water mark is the max HLC across the resulting states — the actual
%% HLC now materialised, not merely the max op HLC (an older-HLC op that
%% the fold rejects must not advance it).
build_writes(Adapter, Handle, Ops) ->
    Shadow = lists:foldl(
        fun(Op, Acc) ->
            {Bucket, Key, Event, _H} = op_parts(Op),
            State0 = current_state(Adapter, Handle, Acc, Bucket, Key),
            %% The native op-based index-entry CRDT (`apply_op/3`); `Key` (the
            %% event dot) is unused — the entry carries its own primary HLC in
            %% the operation. Byte-identical to the retired fold.
            State1 = ?INDEX_CRDT:apply_op(State0, Event, undefined),
            Acc#{{Bucket, Key} => State1}
        end,
        #{},
        Ops
    ),
    %% One pass over the shadow map builds the write list and the batch
    %% high-water HLC together (order is irrelevant — it is a put_batch).
    maps:fold(
        fun({Bucket, Key}, State, {Ws, MaxH}) ->
            Hlc = ?INDEX_CRDT:hlc(State),
            Frame = bondy_oplog_cell_frame:encode(
                Hlc,
                ?INDEX_CRDT:encode_state(State),
                undefined,
                true
            ),
            {[{Bucket, Key, Frame} | Ws], erlang:max(MaxH, Hlc)}
        end,
        {[], 0},
        Shadow
    ).

op_parts({put, Bucket, Key, Cols, Hlc}) ->
    {Bucket, Key, {put, Cols, Hlc}, Hlc};
op_parts({remove, Bucket, Key, Hlc}) ->
    {Bucket, Key, {remove, Hlc}, Hlc}.

current_state(Adapter, Handle, Shadow, Bucket, Key) ->
    case maps:get({Bucket, Key}, Shadow, undefined) of
        undefined ->
            case Adapter:get(Handle, Bucket, Key) of
                not_found ->
                    ?INDEX_CRDT:init();
                {ok, Frame} ->
                    {_Hlc, StateBytes, _Value} =
                        bondy_oplog_cell_frame:decode_full(Frame),
                    ?INDEX_CRDT:decode_state(StateBytes)
            end;
        State ->
            State
    end.

invalidate_cache(undefined, _Handle, _Bucket, _Key) ->
    ok;
invalidate_cache(_Adapter, undefined, _Bucket, _Key) ->
    ok;
invalidate_cache(Adapter, Handle, Bucket, Key) ->
    _ = catch Adapter:delete(Handle, Bucket, Key),
    ok.

bump_ae(undefined) ->
    ok;
bump_ae(Ref) ->
    atomics:put(Ref, 1, erlang:monotonic_time(millisecond)),
    ok.

advance_high_water(undefined, _Hlc) ->
    ok;
advance_high_water(Ref, Hlc) ->
    bondy_oplog_high_water:advance(Ref, Hlc).
