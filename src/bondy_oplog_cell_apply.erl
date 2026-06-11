%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
-module(bondy_oplog_cell_apply).
-moduledoc """
The per-shard projection-write engine, factored out of
`bondy_oplog_applier` so both the applier gen_server and the (ephemeral)
fused writer can drive cell projection writes through identical code.

Side-effect-only over a `cell_apply_ctx()` + instance id: it reads the
old cell value (in-batch shadow → A3 frame-cache → projection `get/3`),
applies the cell kernel, writes the new frames in one `put_batch`,
invalidates the read cache, advances the per-shard high-water HLC, and
dispatches secondary-index ops. It never touches the MST or any
gen_server state. Pure relocation of the applier's prior internals —
behaviour is byte-identical.
""".

-include_lib("kernel/include/logger.hrl").

-define(DEFAULT_MAX_INFLIGHT, 100000).

-type shard_key() :: {atom(), atom(), non_neg_integer()}.
-type cell_apply_ctx() :: #{
    shard_key := shard_key(),
    adapter := module(),
    handle := term(),
    fold_module := atom() | undefined,
    %% Cache adapter pair captured at init time so the applier can
    %% keep the per-shard read cache coherent after every projection
    %% write. Without this, `bondy_db:apply/4` followed by `read/3` on
    %% a different process returns stale state — the cache is
    %% populate-on-miss and never invalidated by writers otherwise.
    cache_adapter => module() | undefined,
    cache_handle => term(),
    %% Per-shard high-water HLC mark. Advanced via
    %% `bondy_oplog_high_water:advance/2` after every successful
    %% projection write in `apply_one_cell/11`. `undefined` when the
    %% shard's registry entry has no ref (legacy entries created
    %% before PR-D1 §3 — defensive only; new registrations always
    %% allocate).
    high_water_ref => bondy_oplog_high_water:ref() | undefined,
    %% Secondary indexes declared on this primary table
    %% (`MST_DB_DESIGN.md` §13). Static descriptors resolved once at
    %% init from the applier opts; `[]` (the default) makes the index
    %% dispatch a strict no-op for non-indexed tables. For each cell the
    %% applier materialises, it term-diffs the cell's old vs new value
    %% per descriptor and dispatches `index_entry` ops to the
    %% `bondy_oplog_secondary_writer` owning each touched secondary
    %% shard (resolved live via `bondy_db_core_registry`), so the writer
    %% need not exist when the applier starts.
    secondary_indexes => [index_descriptor()]
}.
-type index_descriptor() :: #{
    index_name := atom(),
    spec := bondy_oplog_index_spec:spec(),
    sec_shard_count := pos_integer(),
    %% Per-shard in-flight back-pressure cap; read by `dispatch_index_ops/4`.
    %% Defaults to `?DEFAULT_MAX_INFLIGHT` when absent.
    max_inflight => non_neg_integer()
}.

-export_type([cell_apply_ctx/0]).
-export_type([index_descriptor/0]).

-export([apply_cell_batch/3]).
-export([apply_cell_pairs/3]).
-export([compute_one_cell/12]).
-export([oldstate_cache_new/2]).
-export([oldstate_cache_put_entries/2]).
-export([oldstate_cache_clear/1]).
-export([max_hlc/2]).
-export([sec_idx/1]).
-export([index_op/6]).
-export([merge_idx_ops/2]).
-export([dispatch_index_ops/4]).
-export([invalidate_cache/4]).
-export([advance_high_water/2]).

-ifdef(TEST).
%% A3 — exported for the bounded-eviction / hit-miss unit test.
-export([oldstate_cache_get/3]).
-endif.

%% =============================================================================
%% API
%% =============================================================================

%% @private
%% Apply a batch of `cell_apply` events to the projection. For each event,
%% read the cell's current frame from the projection (or the in-batch
%% shadow), decode to state via the fold's `decode_state/1`, fold the event in
%% via `apply_event/3`, encode back, and write the new frame via
%% `put_batch/2`. Bucket is a first-class call-time parameter on the
%% projection adapter; the applier passes it through verbatim.
apply_cell_batch(undefined, _Id, _Events) ->
    ok;
apply_cell_batch(_Ctx, _Id, []) ->
    ok;
apply_cell_batch(Ctx, Id, Events) ->
    #{adapter := Adapter, handle := Handle, kernel := Kernel} = Ctx,
    CacheAdapter = maps:get(cache_adapter, Ctx, undefined),
    CacheHandle = maps:get(cache_handle, Ctx, undefined),
    HighWaterRef = maps:get(high_water_ref, Ctx, undefined),
    OldStateCache = maps:get(oldstate_cache, Ctx, undefined),
    SecIdx = sec_idx(Ctx),

    %% PR-PS-15b: collect all per-event writes into a single
    %% `Adapter:put_batch/2` call.
    %%
    %% Correctness: when two events in the batch target the same
    %% `{Bucket, Key}`, the second must observe the first's write.
    %% We thread a `LocalWrites :: #{{Bucket, Key} => Frame}` shadow
    %% through the fold so the per-event read path checks the local
    %% map before falling back to `Adapter:get/3`. After the fold,
    %% we issue ONE `put_batch` with the deduped {Bucket, Key, Frame}
    %% list (last write wins per key — consistent with the previous
    %% sequential-per-key semantics).
    %%
    %% IDX-3: a third accumulator `IdxAcc :: #{{IndexName, SecShard} =>
    %% [IndexOp]}` collects the secondary-index ops every cell yields
    %% (empty for non-indexed tables); they are dispatched to the
    %% secondary writers *after* the primary `put_batch` returns ok.
    {LocalWrites, MaxHlc, IdxAcc} = lists:foldl(
        fun(Event, {WAcc, HlcAcc, IAcc}) ->
            case bondy_oplog_event:op(Event) of
                {cell_apply, Bucket, Key, FoldEvent} ->
                    Meta = bondy_oplog_event:key(Event),
                    Context = bondy_oplog_event:meta(Event),
                    case
                        compute_one_cell(
                            Id,
                            Adapter,
                            Handle,
                            Kernel,
                            WAcc,
                            Bucket,
                            Key,
                            FoldEvent,
                            Meta,
                            Context,
                            SecIdx,
                            OldStateCache
                        )
                    of
                        {ok, NewFrame, NewHlc, IdxOps} ->
                            WAcc1 = WAcc#{{Bucket, Key} => NewFrame},
                            {
                                WAcc1,
                                max_hlc(HlcAcc, NewHlc),
                                merge_idx_ops(IAcc, IdxOps)
                            };
                        skip ->
                            {WAcc, HlcAcc, IAcc}
                    end;
                _ ->
                    {WAcc, HlcAcc, IAcc}
            end
        end,
        {#{}, undefined, #{}},
        Events
    ),

    case map_size(LocalWrites) of
        0 ->
            ok;
        _ ->
            PutT0 = erlang:monotonic_time(microsecond),
            Entries = [{B, K, F} || {{B, K}, F} <- maps:to_list(LocalWrites)],
            PutResult = Adapter:put_batch(Handle, Entries),
            telemetry:execute(
                [bondy_oplog, applier, batch_cell_put],
                #{
                    duration_us => erlang:monotonic_time(microsecond) - PutT0,
                    count => length(Entries)
                },
                #{instance_id => Id}
            ),
            case PutResult of
                ok ->
                    %% Cache invalidate per unique key (dedup via
                    %% the LocalWrites map's key set, not the Inval
                    %% list which may have duplicates).
                    maps:foreach(
                        fun({B, K}, _F) ->
                            invalidate_cache(CacheAdapter, CacheHandle, B, K)
                        end,
                        LocalWrites
                    ),
                    %% A3 — write-through the now-durable frames into the
                    %% applier's OldValue cache (no-op when disabled).
                    oldstate_cache_put_entries(OldStateCache, Entries),
                    case MaxHlc of
                        undefined -> ok;
                        _ -> advance_high_water(HighWaterRef, MaxHlc)
                    end,
                    %% Only after the primary write is durable do we let
                    %% the index see these terms. The live drain path
                    %% enforces the back-pressure cap (Bypass = false).
                    dispatch_index_ops(SecIdx, IdxAcc, MaxHlc, false);
                {error, Reason} ->
                    %% Primary write failed: do NOT dispatch index ops —
                    %% the cells (and their index entries) are re-applied
                    %% on the next replay of these events.
                    ?LOG_WARNING(#{
                        description =>
                            "bondy_oplog_applier projection batch write "
                            "failed; the cells will be re-applied on the "
                            "next replay of these events",
                        instance_id => Id,
                        count => map_size(LocalWrites),
                        reason => Reason
                    }),
                    ok
            end
    end,
    ok.

%% @private
%% Per-event compute (read + apply + encode). Returns the new frame +
%% HLC to the batch caller, which collects and writes them all at once.
%%
%% Reads first consult `LocalWrites` so in-batch updates to the same
%% `{Bucket, Key}` see each other (the substrate has not been written
%% yet at this point). Then falls back to `Adapter:get/3`.
%%
%% Per-event telemetry boundaries `cell_read` + `cell_apply_event`
%% remain (each cell still pays the read + compute cost). The
%% PR-PS-15a `cell_put` and `cell_side_effects` events are GONE in
%% PR-PS-15b — the put + side-effects now happen once per batch and
%% are measured by `batch_cell_put` in `apply_cell_batch/2`.
compute_one_cell(
    Id,
    Adapter,
    Handle,
    Kernel,
    LocalWrites,
    Bucket,
    Key,
    FoldEvent,
    Meta,
    Context,
    SecIdx,
    OldStateCache
) ->
    try
        ReadT0 = erlang:monotonic_time(microsecond),
        %% OldValue read precedence: in-batch shadow (`LocalWrites`) →
        %% A3 frame-cache → projection `get/3`. A cache hit returns
        %% byte-identical `{OldState, OldValueOpt}` to a projection read
        %% (the cache is a write-through mirror of the durable frame), so
        %% the kernel result is unchanged — A3 only removes the read I/O.
        {OldState, OldValueOpt} =
            case maps:get({Bucket, Key}, LocalWrites, undefined) of
                undefined ->
                    read_old_value(
                        OldStateCache, Adapter, Handle, Kernel, Id, Bucket, Key
                    );
                LocalFrame ->
                    decode_old_frame(Kernel, LocalFrame)
            end,
        telemetry:execute(
            [bondy_oplog, applier, cell_read],
            #{duration_us => erlang:monotonic_time(microsecond) - ReadT0},
            #{instance_id => Id}
        ),

        ApplyT0 = erlang:monotonic_time(microsecond),
        %% The cell kernel ({fold, _} legacy or {crdt, _} operation-based)
        %% applies one operation and returns every frame component. The
        %% fold-vs-CRDT branch lives in `bondy_oplog_cell_kernel`, not here.
        {NewState, Hlc, NewStateBytes, NewValueBytes, ValueEqualsState} =
            bondy_oplog_cell_kernel:apply(
                Kernel, OldState, OldValueOpt, FoldEvent, Meta, Context
            ),
        NewFrame = bondy_oplog_cell_frame:encode(
            Hlc,
            NewStateBytes,
            NewValueBytes,
            ValueEqualsState
        ),
        telemetry:execute(
            [bondy_oplog, applier, cell_apply_event],
            #{duration_us => erlang:monotonic_time(microsecond) - ApplyT0},
            #{instance_id => Id}
        ),
        %% IDX-3: term-diff the cell's old vs new value into secondary
        %% index ops. Wrapped in its own try (`index_ops_for_cell/8`) so a
        %% malformed spec degrades only the index (rebuildable) and never
        %% drops the primary write.
        IdxOps = index_ops_for_cell(
            SecIdx, Id, Kernel, Bucket, Key, OldState, NewState, Hlc
        ),
        {ok, NewFrame, Hlc, IdxOps}
    catch
        C:R:S ->
            ?LOG_ERROR(#{
                description =>
                    "bondy_oplog_applier cell_apply raised; the cell "
                    "has been skipped. Batch continues with remaining cells.",
                instance_id => Id,
                bucket => Bucket,
                cell_key => Key,
                kernel => Kernel,
                class => C,
                reason => R,
                stacktrace => S
            }),
            skip
    end.

%% @private
%% A3 — resolve OldValue from the frame-cache (hit) or the projection
%% (miss). Emits a `[bondy_oplog, applier, oldstate_cache]` hit/miss
%% event only when the cache is enabled (zero overhead when off).
read_old_value(OldStateCache, Adapter, Handle, Kernel, Id, Bucket, Key) ->
    case oldstate_cache_get(OldStateCache, Bucket, Key) of
        {hit, Frame} ->
            emit_cache_result(OldStateCache, Id, hit),
            decode_old_frame(Kernel, Frame);
        miss ->
            emit_cache_result(OldStateCache, Id, miss),
            case Adapter:get(Handle, Bucket, Key) of
                not_found ->
                    {bondy_oplog_cell_kernel:init(Kernel), undefined};
                {ok, OldFrame} ->
                    decode_old_frame(Kernel, OldFrame)
            end
    end.

%% @private
%% Decode a stored cell frame into `{OldState, OldValueOpt}` — the exact
%% shape `compute_one_cell/11` consumes. Shared by the in-batch shadow,
%% the A3 cache-hit, and the projection-read paths so all three are
%% byte-for-byte equivalent.
decode_old_frame(Kernel, Frame) ->
    {_PrevHlc, StateBytes, ValueBytes} =
        bondy_oplog_cell_frame:decode_full(Frame),
    {bondy_oplog_cell_kernel:decode_state(Kernel, StateBytes), ValueBytes}.

%% @private
%% A3 OldValue frame-cache constructor. `{Tab, Max}` when enabled,
%% `undefined` when disabled (every cache op below is then a no-op and
%% behaviour is byte-identical to pre-A3). The table is `private` — only
%% the owning applier process reads or writes it.
oldstate_cache_new(false, _Max) ->
    undefined;
oldstate_cache_new(true, Max) ->
    {ets:new(applier_oldstate_cache, [set, private]), Max}.

%% @private
%% Look up the cached frame for `{Bucket, Key}`.
oldstate_cache_get(undefined, _Bucket, _Key) ->
    miss;
oldstate_cache_get({Tab, _Max}, Bucket, Key) ->
    case ets:lookup(Tab, {Bucket, Key}) of
        [{_, Frame}] -> {hit, Frame};
        [] -> miss
    end.

%% @private
%% Write-through the just-written `{Bucket, Key, Frame}` entries. Called
%% only after the projection `put_batch` returns ok, so the cache mirrors
%% exactly what is durable. Bounded: if the table is at the cap, it is
%% cleared before inserting (coarse evict — the cache is rebuildable, so a
%% clear only costs re-warm misses on the next cycle).
oldstate_cache_put_entries(undefined, _Entries) ->
    ok;
oldstate_cache_put_entries({Tab, Max}, Entries) ->
    case ets:info(Tab, size) >= Max of
        true -> ets:delete_all_objects(Tab);
        false -> ok
    end,
    lists:foreach(
        fun({Bucket, Key, Frame}) ->
            ets:insert(Tab, {{Bucket, Key}, Frame})
        end,
        Entries
    ),
    ok.

%% @private
%% Drop every cached frame. Used when the projection is written outside
%% the write-through paths (catalogue install) so the cache cannot serve
%% a pre-install frame on the next read. No-op when disabled.
oldstate_cache_clear(undefined) ->
    ok;
oldstate_cache_clear({Tab, _Max}) ->
    true = ets:delete_all_objects(Tab),
    ok.

%% @private
emit_cache_result(undefined, _Id, _Result) ->
    ok;
emit_cache_result(_Cache, Id, Result) ->
    telemetry:execute(
        [bondy_oplog, applier, oldstate_cache],
        #{count => 1},
        #{instance_id => Id, result => Result}
    ).

%% @private
%% Tracks the maximum HLC seen across a batch so the per-shard
%% high-water mark can be advanced once at the end instead of once
%% per cell event.
max_hlc(undefined, Hlc) -> Hlc;
max_hlc(A, B) when A >= B -> A;
max_hlc(_, B) -> B.

%% @private
%% Bundle the namespace + resolved secondary-index descriptors out of the
%% cell-apply ctx into the compact `{NS, [Descriptor]}` pair threaded
%% through the cell compute/dispatch path. `[]` (no indexes) makes every
%% downstream step a strict no-op.
sec_idx(Ctx) ->
    {NS, _Index, _Shard} = maps:get(shard_key, Ctx),
    {NS, maps:get(secondary_indexes, Ctx, [])}.

%% @private
%% Term-diff one cell's old vs new value into a flat list of
%% `{IndexName, SecShard, IndexOp}` across every declared secondary index.
%% Own try/catch: a malformed spec (e.g. a term type the codec rejects)
%% degrades only the index — the caller's primary write proceeds.
index_ops_for_cell(
    {_NS, []}, _Id, _Kernel, _Bucket, _Key, _OldState, _NewState, _Hlc
) ->
    [];
index_ops_for_cell(
    {_NS, SecIndexes}, Id, Kernel, Bucket, Key, OldState, NewState, Hlc
) ->
    try
        OldValue = bondy_oplog_cell_kernel:to_value(Kernel, OldState),
        NewValue = bondy_oplog_cell_kernel:to_value(Kernel, NewState),
        lists:flatmap(
            fun(Desc) ->
                index_ops_for_one(Desc, Bucket, Key, OldValue, NewValue, Hlc)
            end,
            SecIndexes
        )
    catch
        C:R:S ->
            ?LOG_ERROR(#{
                description =>
                    "bondy_oplog_applier secondary-index op computation "
                    "raised; the index is degraded for this cell "
                    "(rebuildable from the primary). The primary write "
                    "is unaffected.",
                instance_id => Id,
                bucket => Bucket,
                cell_key => Key,
                class => C,
                reason => R,
                stacktrace => S
            }),
            []
    end.

%% @private
%% A `put` for every current term (an idempotent re-put also refreshes the
%% denormalised columns when sibling fields changed) and a `remove` for
%% every term the value no longer yields. The op HLC is the *primary*
%% cell's new HLC, so the index cell's LWW-over-presence fold converges
%% regardless of arrival order.
index_ops_for_one(
    #{index_name := IName, spec := Spec, sec_shard_count := SCount},
    Bucket,
    Key,
    OldValue,
    NewValue,
    Hlc
) ->
    OldTerms = lists:usort(bondy_oplog_index_spec:terms(Spec, OldValue)),
    NewTerms = lists:usort(bondy_oplog_index_spec:terms(Spec, NewValue)),
    SecBucket = bondy_oplog_index_key:bucket(Bucket, IName),
    Cols = bondy_oplog_index_spec:project(Spec, NewValue),
    Removed = OldTerms -- NewTerms,
    [
        index_op(IName, SecBucket, SCount, T, Key, {put, Cols, Hlc})
     || T <- NewTerms
    ] ++
        [
            index_op(IName, SecBucket, SCount, T, Key, {remove, Hlc})
         || T <- Removed
        ].

%% @private
index_op(IName, SecBucket, SCount, Term, PrimaryKey, EventDelta) ->
    SecShard = bondy_oplog_index_key:shard(SecBucket, Term, SCount),
    SecKey = bondy_oplog_index_key:encode(Term, PrimaryKey),
    Op =
        case EventDelta of
            {put, Cols, Hlc} -> {put, SecBucket, SecKey, Cols, Hlc};
            {remove, Hlc} -> {remove, SecBucket, SecKey, Hlc}
        end,
    {IName, SecShard, Op}.

%% @private
%% Group the cell's `{IndexName, SecShard, Op}` triples into
%% `#{{IndexName, SecShard} => [Op]}` (ops accumulate in reverse; the
%% dispatcher restores arrival order). A no-op for an empty op list, so
%% non-indexed tables pay nothing.
merge_idx_ops(Acc, []) ->
    Acc;
merge_idx_ops(Acc, [{IName, SecShard, Op} | Rest]) ->
    K = {IName, SecShard},
    merge_idx_ops(Acc#{K => [Op | maps:get(K, Acc, [])]}, Rest).

%% @private
%% Dispatch the grouped index ops to the secondary writer owning each
%% touched shard, resolved live from the registry (so the writer need not
%% have existed when the applier started). A missing writer pid or row is
%% dropped silently — the index is rebuildable from the primary.
%%
%% IDX-4 back-pressure: each `(IName, SecShard)` carries an in-flight op
%% counter. On the live drain path (`Bypass = false`) a batch that would
%% push the counter past the index's `max_inflight` cap is dropped, the
%% shard is marked `needs_rebuild`, its freshness reset to stale (so reads
%% refuse), and a rebuild requested. A rebuild's own re-fold dispatches
%% with `Bypass = true` so it can reload the full working set in one pass.
dispatch_index_ops({_NS, []}, _IdxAcc, _MaxHlc, _Bypass) ->
    ok;
dispatch_index_ops({NS, SecIndexes}, IdxAcc, MaxHlc, Bypass) ->
    Hlc =
        case MaxHlc of
            undefined -> 0;
            _ -> MaxHlc
        end,
    Caps = maps:from_list([
        {
            maps:get(index_name, D),
            maps:get(max_inflight, D, ?DEFAULT_MAX_INFLIGHT)
        }
     || D <- SecIndexes
    ]),
    maps:foreach(
        fun({IName, SecShard}, RevOps) ->
            Cap = maps:get(IName, Caps, ?DEFAULT_MAX_INFLIGHT),
            dispatch_one_index(
                NS, IName, SecShard, lists:reverse(RevOps), Hlc, Cap, Bypass
            )
        end,
        IdxAcc
    ).

%% @private
dispatch_one_index(NS, IName, SecShard, Ops, MaxHlc, Cap, Bypass) ->
    case bondy_db_core_registry:lookup(NS, IName, SecShard) of
        {ok, Entry} ->
            case bondy_db_core_registry:entry_writer_pid(Entry) of
                Pid when is_pid(Pid) ->
                    NumOps = length(Ops),
                    Accept =
                        Bypass orelse
                            bondy_db_core_registry:index_inflight(Entry) +
                                NumOps =< Cap,
                    case Accept of
                        true ->
                            _ = bondy_db_core_registry:index_inflight_add(
                                Entry, NumOps
                            ),
                            bondy_oplog_secondary_writer:enqueue(
                                Pid, Ops, MaxHlc
                            );
                        false ->
                            secondary_saturation_drop(
                                NS, IName, SecShard, Entry, NumOps
                            )
                    end;
                undefined ->
                    ok
            end;
        not_found ->
            ok
    end.

%% @private
%% Saturation: the secondary writer's in-flight backlog would exceed the
%% cap. Drop the batch (the index is a deterministic function of the
%% primary, so it is rebuildable), mark the shard for rebuild, reset its
%% freshness so `index_get`/`index_range` refuse until the rebuild
%% completes, and request the rebuild.
secondary_saturation_drop(NS, IName, SecShard, Entry, NumOps) ->
    bondy_db_core_registry:index_mark_rebuild(Entry),
    bondy_db_core_registry:reset_stale_ae(Entry),
    telemetry:execute(
        [bondy_oplog, secondary_writer, saturated],
        #{dropped_ops => NumOps},
        #{namespace => NS, index_name => IName, shard => SecShard}
    ),
    catch bondy_oplog_index_rebuild:request(NS, IName),
    ok.

%% @private
%% Walks the `{Key, Value}` pairs from the MST (or its diff) and
%% dispatches every `cell_apply` op through the batched compute path.
%% Non-cell ops are skipped here — the per-instance fold owns them and
%% has already seen them via the WAL drain.
%%
%% PR-PS-15b: same collect-then-batch shape as `apply_cell_batch/2`.
%% Per-key shadow map preserves in-batch read-your-own-writes when
%% two pairs target the same `{Bucket, Key}`.
%%
%% Index dispatch respects the back-pressure cap (`Bypass = false`): a
%% peer-event replay that overflows a writer is dropped and self-heals via
%% a marked rebuild. The full-rebuild path no longer routes through here —
%% it re-indexes from the converged projection (`reindex_from_projection/3`).
apply_cell_pairs(Ctx, Id, Pairs) ->
    #{adapter := Adapter, handle := Handle, kernel := Kernel} = Ctx,
    CacheAdapter = maps:get(cache_adapter, Ctx, undefined),
    CacheHandle = maps:get(cache_handle, Ctx, undefined),
    HighWaterRef = maps:get(high_water_ref, Ctx, undefined),
    OldStateCache = maps:get(oldstate_cache, Ctx, undefined),
    SecIdx = sec_idx(Ctx),
    try
        {LocalWrites, MaxHlc, N, IdxAcc} = lists:foldl(
            fun
                (
                    {MstKey, {
                        {cell_apply, Bucket, CellKey, FoldEvent},
                        EventMeta,
                        _Prev,
                        _Sig
                    }},
                    {WAcc, HlcAcc, NAcc, IAcc}
                ) ->
                    case
                        compute_one_cell(
                            Id,
                            Adapter,
                            Handle,
                            Kernel,
                            WAcc,
                            Bucket,
                            CellKey,
                            FoldEvent,
                            MstKey,
                            EventMeta,
                            SecIdx,
                            OldStateCache
                        )
                    of
                        {ok, NewFrame, NewHlc, IdxOps} ->
                            WAcc1 = WAcc#{{Bucket, CellKey} => NewFrame},
                            {
                                WAcc1,
                                max_hlc(HlcAcc, NewHlc),
                                NAcc + 1,
                                merge_idx_ops(IAcc, IdxOps)
                            };
                        skip ->
                            {WAcc, HlcAcc, NAcc, IAcc}
                    end;
                (_, Acc) ->
                    Acc
            end,
            {#{}, undefined, 0, #{}},
            Pairs
        ),
        case map_size(LocalWrites) of
            0 ->
                ok;
            _ ->
                Entries = [
                    {B, K, F}
                 || {{B, K}, F} <- maps:to_list(LocalWrites)
                ],
                case Adapter:put_batch(Handle, Entries) of
                    ok ->
                        maps:foreach(
                            fun({B, K}, _F) ->
                                invalidate_cache(
                                    CacheAdapter, CacheHandle, B, K
                                )
                            end,
                            LocalWrites
                        ),
                        %% A3 — write-through the peer/replay frames too,
                        %% so a subsequent local read sees the durable
                        %% value (no-op when disabled).
                        oldstate_cache_put_entries(OldStateCache, Entries),
                        case MaxHlc of
                            undefined -> ok;
                            _ -> advance_high_water(HighWaterRef, MaxHlc)
                        end,
                        dispatch_index_ops(SecIdx, IdxAcc, MaxHlc, false);
                    {error, Reason} ->
                        ?LOG_WARNING(#{
                            description =>
                                "bondy_oplog_applier replay batch write "
                                "failed; the cells will be re-applied on "
                                "the next sync tick",
                            instance_id => Id,
                            count => map_size(LocalWrites),
                            reason => Reason
                        })
                end
        end,
        N
    catch
        C:R:S ->
            ?LOG_WARNING(#{
                description =>
                    "bondy_oplog_applier replay_cell_events raised; "
                    "the projection may be temporarily stale on this "
                    "node — the next sync tick re-issues the replay.",
                instance_id => Id,
                class => C,
                reason => R,
                stacktrace => S
            }),
            0
    end.

%% @private
%% Invalidate the per-shard read cache entry for `{Bucket, Key}` after a
%% projection write so the next reader repopulates it. Both `(a)` the
%% applier cannot insert the correct cached value (the cache wants the
%% post-overlay-merge value but the applier has only the *decoded* fold
%% state) and `(b)` the next reader's `slow_read_traced/3` will repopulate
%% the cache anyway.
invalidate_cache(undefined, _Handle, _Bucket, _Key) ->
    ok;
invalidate_cache(_Adapter, undefined, _Bucket, _Key) ->
    ok;
invalidate_cache(Adapter, Handle, Bucket, Key) ->
    %% `delete/3` is the cache_adapter callback. Swallow any errors —
    %% a failed cache eviction must not stop the drain.
    _ = catch Adapter:delete(Handle, Bucket, Key),
    ok.

%% @private
%% Advance the per-shard high-water HLC mark
%% (`bondy_oplog_high_water:advance/2`) after a successful projection
%% write. The ref may be `undefined` defensively (older
%% `bondy_db_core_registry` entries created before PR-D1 §3); new
%% registrations always allocate, so this branch is dead in practice
%% but keeps the applier resilient to a partial rollback.
advance_high_water(undefined, _Hlc) ->
    ok;
advance_high_water(Ref, Hlc) ->
    bondy_oplog_high_water:advance(Ref, Hlc).
