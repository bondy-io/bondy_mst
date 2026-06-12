%% =============================================================================
%%  bondy_oplog_wal_mem_reader.erl -
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

-module(bondy_oplog_wal_mem_reader).

-include("bondy_doc.hrl").

?MODULEDOC("""
Read side of the in-memory ephemeral WAL (`bondy_oplog_wal_mem`).

Mirrors the surface of `bondy_oplog_wal_reader` that the fused drain uses —
`open/3`, `next/1`, `position/1`, `close/1` — but reads events out of the mem
WAL's `ordered_set` ETS table instead of segment files. Because an inserted
event is visible to a reader immediately (no durable-position gate), this is
where the ephemeral path stops paying the WAL-durability latency.

`next/1` returns the same shape as `bondy_oplog_wal_reader:next/1`
(`{ok, Batch, Hlcs, {Seg, Off}, NewIter}`), with `Off` being the dense `Seq`
and `Seg` the mem WAL's single logical segment id — so the fused drain's
consumer-offset bookkeeping, idle-waiter and `collect_frames`-style aggregation
work unchanged on `{Seg, Seq}` positions.

The drain dispatches to this module (vs `bondy_oplog_wal_reader`) on the
instance's `wal_backend` flag; `bondy_oplog_wal_reader` itself is untouched.
""").

%% Default events read per `next/1` when the caller does not pass `{chunk, _}`.
%% Set at `open/3` from the fused drain's `apply_batch_max` so a mem batch
%% matches the disk path's batch size (the disk reader's `collect_frames`
%% aggregates frames up to `apply_batch_max`). A too-large chunk inflates the
%% install-batch latency that bounds the bounded-writer→await pipeline.
-define(DEFAULT_CHUNK, 256).

-record(mem_iter, {
    wal_pid :: pid(),
    tab :: ets:tid(),
    seg :: non_neg_integer(),
    cursor = 0 :: non_neg_integer(),
    %% For an `{hlc, T}` start: drop events with `key_hlc < min_hlc` from each
    %% batch (Seq order need not equal HLC order under concurrency, so we
    %% filter rather than seek). `undefined` for `beginning` / `tail` /
    %% `{offset, _, _}`. Re-applying an already-installed event is idempotent
    %% by the CRDT contract, so this only avoids redundant work.
    min_hlc :: undefined | term(),
    chunk = ?DEFAULT_CHUNK :: pos_integer()
}).

-opaque t() :: #mem_iter{}.
-export_type([t/0]).

-export([open/2]).
-export([open/3]).
-export([next/1]).
-export([position/1]).
-export([close/1]).

%% =============================================================================
%% API
%% =============================================================================

-spec open(pid(), bondy_oplog_wal_reader:start_position()) ->
    {ok, t()} | {error, term()}.

open(WalPid, Start) ->
    open(WalPid, Start, []).

?DOC("""
Opens a reader over the mem WAL's table at `Start`. `Opts` are accepted for
parity with `bondy_oplog_wal_reader:open/3` (e.g. `{follow, _}`) and ignored —
the mem reader never blocks; `next/1` simply returns `end_of_log` when the
cursor has caught up to the head.
""").
-spec open(pid(), bondy_oplog_wal_reader:start_position(), list()) ->
    {ok, t()} | {error, term()}.

open(WalPid, Start, Opts) when is_pid(WalPid) ->
    try bondy_oplog_wal_mem:reader_view(WalPid) of
        #{tab := Tab, mem_seg := Seg} ->
            Chunk = proplists:get_value(chunk, Opts, ?DEFAULT_CHUNK),
            Iter0 = #mem_iter{
                wal_pid = WalPid, tab = Tab, seg = Seg, chunk = Chunk
            },
            {ok, apply_start(Iter0, Start)}
    catch
        exit:{noproc, _} -> {error, wal_unavailable};
        exit:noproc -> {error, wal_unavailable};
        exit:{normal, _} -> {error, wal_unavailable};
        exit:{shutdown, _} -> {error, wal_unavailable}
    end.

?DOC("""
Returns the next chunk of events with `Seq > cursor` (up to `chunk`), or
`end_of_log` when the cursor has reached the head. The position returned is the
`Seq` of the last event in the batch.

Walks forward with `ets:next/2` (an O(log n) tree successor per step) — NOT
`ets:select` with a `{'>', key, cursor}` guard, which does not prune the
ordered_set traversal and rescans every already-consumed entry on each read
(O(consumed) per read → O(n²) over the log; measured 13µs near the head vs
8.8ms after 499k consumed). `ets:next` keeps reads O(chunk·log n) regardless of
how far the cursor has advanced or whether GC has caught up.
""").
-spec next(t()) -> bondy_oplog_wal_reader:next_result().

next(#mem_iter{seg = Seg, cursor = Cursor} = Iter) ->
    #mem_iter{tab = Tab, chunk = Chunk, min_hlc = Min} = Iter,
    case walk(Tab, Cursor, Chunk, Min, []) of
        {[], Cursor} ->
            %% Nothing past the cursor — caught up to the head.
            end_of_log;
        {[], NewCursor} ->
            %% Only skipped entries (below `min_hlc`); advance and retry.
            next(Iter#mem_iter{cursor = NewCursor});
        {AccRev, NewCursor} ->
            {ok, lists:reverse(AccRev), [], {Seg, NewCursor}, Iter#mem_iter{
                cursor = NewCursor
            }}
    end.

-spec position(t()) -> {non_neg_integer(), non_neg_integer()}.

position(#mem_iter{seg = Seg, cursor = Cursor}) ->
    {Seg, Cursor}.

-spec close(t()) -> ok.

close(#mem_iter{}) ->
    ok.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
apply_start(Iter, beginning) ->
    Iter#mem_iter{cursor = 0};
apply_start(Iter, tail) ->
    %% Start at the head: skip everything already present.
    #{head_seq := H} = bondy_oplog_wal_mem:info(Iter#mem_iter.wal_pid),
    Iter#mem_iter{cursor = H};
apply_start(Iter, {offset, _Seg, Off}) ->
    Iter#mem_iter{cursor = Off};
apply_start(Iter, {hlc, Hlc}) ->
    %% No persisted Seq↔HLC map (a fresh process has an empty table), so scan
    %% from the start and drop events below the watermark per batch.
    Iter#mem_iter{cursor = 0, min_hlc = Hlc}.

%% @private
%% Walk forward from `Cursor` collecting up to `K` kept events (reversed). Each
%% step is `ets:next/2` (O(log n)) + a point lookup. A `min_hlc` skip advances
%% the cursor WITHOUT consuming a slot (so already-installed events are stepped
%% over for free); a GC race (key vanished) is skipped likewise. Returns the
%% reversed events and the last Seq advanced to.
walk(_Tab, Cursor, 0, _Min, Acc) ->
    {Acc, Cursor};
walk(Tab, Cursor, K, Min, Acc) ->
    case ets:next(Tab, Cursor) of
        '$end_of_table' ->
            {Acc, Cursor};
        NextSeq ->
            case ets:lookup(Tab, NextSeq) of
                [{NextSeq, Event}] ->
                    case keep(Event, Min) of
                        true ->
                            walk(Tab, NextSeq, K - 1, Min, [Event | Acc]);
                        false ->
                            walk(Tab, NextSeq, K, Min, Acc)
                    end;
                [] ->
                    %% Raced with GC — the row was deleted between `next` and
                    %% `lookup`. Skip it (does not consume a slot).
                    walk(Tab, NextSeq, K, Min, Acc)
            end
    end.

%% @private
keep(_Event, undefined) ->
    true;
keep(Event, MinHlc) ->
    bondy_oplog_event:key_hlc(bondy_oplog_event:key(Event)) >= MinHlc.
