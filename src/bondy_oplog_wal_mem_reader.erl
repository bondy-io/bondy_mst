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

-include("bondy_mst.hrl").

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

%% Read up to this many rows per `ets:select`. The fused drain's
%% `collect_frames`-equivalent aggregates these into apply batches; a chunk
%% >= the apply-batch-max returns a full batch in one select.
-define(CHUNK, 512).

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
    chunk = ?CHUNK :: pos_integer()
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

open(WalPid, Start, _Opts) when is_pid(WalPid) ->
    try bondy_oplog_wal_mem:reader_view(WalPid) of
        #{tab := Tab, mem_seg := Seg} ->
            Iter0 = #mem_iter{wal_pid = WalPid, tab = Tab, seg = Seg},
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
""").
-spec next(t()) -> bondy_oplog_wal_reader:next_result().

next(#mem_iter{tab = Tab, seg = Seg, cursor = Cursor, chunk = Chunk} = Iter) ->
    %% `ets:select/3` with a match spec on the key — NOT tab2list + filter —
    %% so an ordered_set scan returns the next `Chunk` rows in Seq order.
    MatchSpec = [{{'$1', '$2'}, [{'>', '$1', Cursor}], [{{'$1', '$2'}}]}],
    case ets:select(Tab, MatchSpec, Chunk) of
        '$end_of_table' ->
            end_of_log;
        {Rows, _Cont} ->
            case filter_rows(Rows, Iter#mem_iter.min_hlc) of
                {[], LastSeq} ->
                    %% Every row in this chunk was below `min_hlc` (already
                    %% installed). Advance past them and try the next chunk.
                    next(Iter#mem_iter{cursor = LastSeq});
                {Events, LastSeq} ->
                    {ok, Events, [], {Seg, LastSeq},
                        Iter#mem_iter{cursor = LastSeq}}
            end
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
%% Split a chunk into (kept events, last Seq seen). When `min_hlc` is set, drop
%% events whose key HLC precedes it.
filter_rows(Rows, undefined) ->
    Events = [E || {_Seq, E} <- Rows],
    {Events, last_seq(Rows)};
filter_rows(Rows, MinHlc) ->
    Events = [
        E
     || {_Seq, E} <- Rows,
        bondy_oplog_event:key_hlc(bondy_oplog_event:key(E)) >= MinHlc
    ],
    {Events, last_seq(Rows)}.


%% @private
last_seq(Rows) ->
    {Seq, _E} = lists:last(Rows),
    Seq.
