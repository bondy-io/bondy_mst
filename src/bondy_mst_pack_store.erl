%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_mst_pack_store).

-compile({no_auto_import, [put/2, get/2]}).

-behaviour(bondy_mst_store).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").
-include("bondy_mst_pack.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Persistent `bondy_mst_store` backend backed by an append-only
content-addressed pack-file format (see `bondy_mst_pack_writer` /
`bondy_mst_pack_reader` and `_design/latest/MST_PAGE_STORE_DESIGN.md`).

## State model

A pack-store instance owns a directory containing:

- `manifest` — authoritative view of live sealed packs and current
  root (`bondy_mst_pack_manifest`).
- `incoming.pack` — append-only log of recent puts (created lazily
  on first put after open / seal).
- `pack-NNNN.pack` + `pack-NNNN.idx` — sealed immutable packs.

The store state record carries:

- a `bondy_mst_pack_writer` owning `incoming.pack` and the in-memory
  pending hash → offset map,
- a list of `sealed_view` records (one per sealed pack), each holding
  a parsed `.idx` and an open read fd,
- a `free_set` of hashes tombstoned via `free/3` or `delete/2`, kept
  for the next compaction (the actual deletion is performed by
  `gc/2`).

Pages are serialised as `term_to_binary({Level, Low, List},
[deterministic, {minor_version, 2}])` — the same form
`bondy_mst_page:hash/2` hashes over — so `sha256(stored_bytes)`
matches the canonical page hash with no double work.

## Concurrency

The state record is opaque to outside processes: the writer's raw
fd is owned by the calling process and cannot be shared. Callers
must serialise mutations through a single owner process (typically
a gen_server above this module). Read concurrency is also single-
owner for the same reason.

## Error handling

Diverges intentionally from the in-memory `bondy_mst_map_store` /
`bondy_mst_ets_store` siblings: those operate on data structures
that cannot fail at I/O, so they never raise. This backend wraps
real disk and treats unrecoverable I/O failures (manifest write
refused, sealed pack read error, file system gone) as raised
errors of the form `error({Op, Reason})` — e.g. `{set_root, _}`,
`{put, _}`, `{get, _}`, `{gc_open_view, _, _}`. Recoverable
conditions (missing hash, no-op seal, GC epoch unsupported)
return ordinary tagged results.

The `{gc_open_view, _, _}` failure mode is special: it can only
occur *after* the manifest swap has been durably committed, so the
on-disk state already reflects the compaction. We retry the view
open once before raising (most failures here — EMFILE, EAGAIN,
transient EIO — are recoverable, and the just-fsync'd idx is hot
in the OS page cache). On persistent failure we still raise so
the calling process restarts and recovers from the manifest
on next open.

The reasoning matches the codebase's WAL modules: a backend that
cannot persist what the caller asked it to has no honest
return value, and forcing every caller to thread an extra
`{error, _}` branch on top of `bondy_mst_store`'s callback
signatures defeats the abstraction. The owning gen_server is
expected to catch and surface these via its own restart/log path.

## Auto-seal

Two open-time options bound the size of `incoming.pack` between
explicit `seal/1` calls:

- `auto_seal_records` — seal after this many pending records.
  Default: `10_000` (`?BONDY_MST_PACK_DEFAULT_AUTO_SEAL_RECORDS`).
- `auto_seal_bytes`   — seal once `incoming.pack` reaches this byte
  size (including header). Default: `16_000_000`
  (`?BONDY_MST_PACK_DEFAULT_AUTO_SEAL_BYTES`).

Whichever threshold fires first triggers the seal. Either can be
set to `infinity` to disable; both `infinity` reverts to fully
caller-driven seal (the prior default). When a threshold is
crossed during a `put/2`, the store rolls over before returning.
Auto-seal failures are logged at WARNING and do not fail the put;
the page has already been durably appended to `incoming.pack` and
the next put will re-evaluate the thresholds. Bounding incoming
pack size keeps the linear `scan_incoming/3` cost on reopen
proportional to the threshold rather than to the lifetime put
volume.
""").

-record(?MODULE, {
    writer            :: bondy_mst_pack_writer:t(),
    %% Invariant: stored in DESCENDING `pack_id` order (newest first).
    %% The read paths (`do_get/2`, `do_has/2`) iterate this list and
    %% short-circuit on the first hit, so newer pages — the most-
    %% recently-written and typically the most-recently-accessed — are
    %% probed first. Every mutation that grows the list (`seal/1`,
    %% `finalise_compaction/6`) funnels through `newest_first/1` to
    %% preserve the invariant. Callers that need ascending order
    %% (`apply_compaction/3` reporting) sort at the use-site rather
    %% than reshuffling the canonical field.
    sealed_views      :: [#sealed_view{}],
    free_set          :: sets:set(binary()),
    hashing_algorithm :: atom(),
    opts              :: map(),
    %% Auto-seal thresholds. After every successful `put/2` the store
    %% checks pending record count and `incoming.pack` byte size; if
    %% either threshold is crossed it rolls over via `seal/1`. Defaults
    %% bound the resume-scan cost on reopen (see `bondy_mst_pack.hrl`);
    %% set either to `infinity` to disable. The check is opportunistic
    %% — a seal failure during auto-seal is logged but does not fail
    %% the put, since the page is already durable in incoming.pack
    %% and the next put will re-evaluate.
    auto_seal_records :: pos_integer() | infinity,
    auto_seal_bytes   :: pos_integer() | infinity,
    %% Tombstones-flush debounce. The `tombstones` file uses the same
    %% tmp+datasync+rename+fsync_dir pattern as the manifest (4 fsyncs
    %% per write). `bondy_mst:put/3` issues one `free/3` per spine
    %% modification — typically ~5 per put on a populated tree — so a
    %% naive per-call write costs ~20 fsyncs per MST put. We keep the
    %% in-memory `free_set` current and persist on the same shape as
    %% the set_root debounce: when `tombstones_unsynced_count` reaches
    %% the records threshold, or the wall-clock floor has elapsed, the
    %% next mutation flushes. Seal / auto-seal / GC / close / explicit
    %% `flush/1` force a flush so the on-disk tombstones never lag the
    %% in-memory set for long.
    tombstones_flush_every_records :: pos_integer() | infinity,
    tombstones_flush_every_ms      :: pos_integer() | infinity,
    tombstones_unsynced_count = 0  :: non_neg_integer(),
    last_tombstones_flush_ms       :: integer(),
    tombstones_dirty = false       :: boolean()
}).

-type t() :: #?MODULE{}.
-type page() :: bondy_mst_page:t().
-type opts() :: opts_map() | [{atom(), term()}].
-type opts_map() :: #{
    dir         := file:filename_all(),
    instance_id := binary(),
    atom() => term()
}.

-export_type([t/0]).
-export_type([page/0]).
-export_type([opts/0]).

%% bondy_mst_store callbacks
-export([open/2]).
-export([close/1]).
-export([capabilities/1]).
-export([copy/3]).
-export([delete/1]).
-export([delete/2]).
-export([free/3]).
-export([gc/2]).
-export([get/2]).
-export([get_root/1]).
-export([has/2]).
-export([list/1]).
-export([missing_set/2]).
-export([page_refs/1]).
-export([put/2]).
-export([set_root/2]).

%% Pack-store-specific extensions
-export([seal/1]).
-export([dir/1]).
-export([instance_id/1]).
-export([sealed_pack_ids/1]).

%% =============================================================================
%% bondy_mst_store CALLBACKS
%% =============================================================================

-spec open(Algo :: atom(), Opts :: opts()) -> t() | no_return().

open(Algo, Opts) when is_atom(Algo), is_list(Opts) ->
    open(Algo, maps:from_list(Opts));
open(sha256, Opts) when is_map(Opts) ->
    Dir = required(dir, Opts),
    InstanceId = required(instance_id, Opts),
    ok = ensure_dir(Dir),
    WriterOpts0 = #{instance_id => InstanceId, hash_algo => sha256},
    WriterOpts1 = forward_opt(sync_every_records, Opts, WriterOpts0),
    WriterOpts2 = forward_opt(sync_every_ms, Opts, WriterOpts1),
    WriterOpts3 = forward_opt(root_flush_every_records, Opts, WriterOpts2),
    WriterOpts  = forward_opt(root_flush_every_ms, Opts, WriterOpts3),
    AutoSealR = validated_auto_seal(auto_seal_records, Opts),
    AutoSealB = validated_auto_seal(auto_seal_bytes, Opts),
    TsR = validated_tombstones_flush(tombstones_flush_every_records, Opts),
    TsMs = validated_tombstones_flush(tombstones_flush_every_ms, Opts),
    Now = erlang:monotonic_time(millisecond),
    case bondy_mst_pack_writer:open(Dir, WriterOpts) of
        {ok, W} ->
            Manifest = bondy_mst_pack_writer:manifest(W),
            SealedIds = bondy_mst_pack_manifest:sealed_packs(Manifest),
            case open_sealed_views(Dir, SealedIds) of
                {ok, Views} ->
                    #?MODULE{
                        writer = W,
                        sealed_views = newest_first(Views),
                        free_set = load_tombstones(Dir),
                        hashing_algorithm = sha256,
                        opts = Opts,
                        auto_seal_records = AutoSealR,
                        auto_seal_bytes = AutoSealB,
                        tombstones_flush_every_records = TsR,
                        tombstones_flush_every_ms = TsMs,
                        tombstones_unsynced_count = 0,
                        last_tombstones_flush_ms = Now,
                        tombstones_dirty = false
                    };
                {error, R} ->
                    _ = bondy_mst_pack_writer:close(W),
                    error({pack_store_open, R})
            end;
        {error, R} ->
            error({pack_store_open, R})
    end;
open(Algo, _Opts) ->
    error({unsupported_hash_algorithm, Algo}).

-spec close(t()) -> ok.

close(#?MODULE{} = T) ->
    %% Force a final tombstones flush so clean shutdown is lossless;
    %% errors are swallowed because there is no caller to return them
    %% to and the next reopen rebuilds the in-memory free_set from
    %% disk anyway.
    _ = do_flush_tombstones(T),
    lists:foreach(
        fun(#sealed_view{pack_fd = Fd}) -> _ = prim_file:close(Fd) end,
        T#?MODULE.sealed_views
    ),
    bondy_mst_pack_writer:close(T#?MODULE.writer),
    ok.

-spec capabilities(t()) -> map().

capabilities(#?MODULE{}) ->
    #{
        transactions      => false,
        read_concurrency  => false,
        concurrent_writes => false
    }.

-spec get_root(t()) -> binary() | undefined.

get_root(#?MODULE{writer = W}) ->
    bondy_mst_pack_writer:current_root(W).

-spec set_root(t(), binary() | undefined) -> t().

set_root(#?MODULE{writer = W} = T, Root) ->
    case bondy_mst_pack_writer:set_root(W, Root) of
        {ok, W1} ->
            T#?MODULE{writer = W1};
        {error, R} ->
            error({set_root, R})
    end.

-spec get(t(), binary()) -> page() | undefined.

get(#?MODULE{} = T, Hash) when is_binary(Hash) ->
    case sets:is_element(Hash, T#?MODULE.free_set) of
        true ->
            undefined;
        false ->
            do_get(T, Hash)
    end.

-spec has(t(), binary()) -> boolean().

has(#?MODULE{} = T, Hash) when is_binary(Hash) ->
    case sets:is_element(Hash, T#?MODULE.free_set) of
        true -> false;
        false -> do_has(T, Hash)
    end.

-spec put(t(), page()) -> {binary(), t()}.

put(#?MODULE{writer = W, hashing_algorithm = Algo} = T, Page) ->
    Bytes = serialise(Page),
    case bondy_mst_pack_writer:append(W, Bytes) of
        {ok, Hash, W1} ->
            %% The canonical page hash must equal sha256 of the
            %% serialised body for the on-disk store to be content-
            %% addressed — verify in debug builds; in release builds
            %% the equivalence is by construction.
            Hash = bondy_mst_page:hash(Page, Algo),
            T1 = maybe_persist_free_set(
                T#?MODULE{writer = W1},
                sets:del_element(Hash, T#?MODULE.free_set), put),
            {Hash, maybe_auto_seal(T1)};
        {error, R} ->
            error({put, R})
    end.

-spec delete(t(), binary()) -> t().

delete(#?MODULE{} = T, Hash) when is_binary(Hash) ->
    maybe_persist_free_set(T,
        sets:add_element(Hash, T#?MODULE.free_set), delete).

-spec copy(t(), bondy_mst_store:t(), binary()) -> t().

copy(#?MODULE{} = T, OtherStore, Hash) ->
    case bondy_mst_store:get(OtherStore, Hash) of
        undefined ->
            T;
        Page ->
            Refs = bondy_mst_store:page_refs(OtherStore, Page),
            T1 = lists:foldl(
                fun(Ref, Acc) -> copy(Acc, OtherStore, Ref) end,
                T,
                Refs
            ),
            {_Hash, T2} = put(T1, Page),
            T2
    end.

-spec list(t()) -> [page()].

list(#?MODULE{} = T) ->
    Hashes = enumerate_hashes(T),
    lists:filtermap(
        fun(H) ->
            case do_get(T, H) of
                undefined -> false;
                Page      -> {true, Page}
            end
        end,
        Hashes
    ).

-spec free(t(), binary(), page()) -> t().

free(#?MODULE{} = T, Hash, _Page) when is_binary(Hash) ->
    maybe_persist_free_set(T,
        sets:add_element(Hash, T#?MODULE.free_set), free).

?DOC("""
Pack-rewrite compaction. Given a list of `KeepRoots`, computes the
transitively reachable hash set, intersects it with the set of
non-tombstoned hashes, and rewrites every sealed pack into a single
new sealed pack containing only those entries.

* Integer `Epoch` is currently rejected with a no-op (pages carry no
  epoch on this backend); the metadata reports `reason =>
  epoch_unsupported`.
* If there are no sealed packs to compact, the call is a no-op.
* If no entries would be dropped and there is exactly one sealed
  pack, the call is a no-op (coalescing multiple packs into one is
  still performed even with zero drops, since reducing fd count and
  improving lookup locality is the other point of GC).
* Pending pages (still in `incoming.pack`) are not touched; the next
  `seal/1` deposits them. Tombstones whose target is still pending
  are preserved in `free_set`; tombstones whose target was applied
  by compaction are cleared.

On any I/O failure mid-compaction the call logs, leaves the store
in its pre-call state, and reports `compacted => false` with the
error in metadata. The single non-recoverable case — failing to
open a sealed view *after* the manifest swap — raises; the next
reopen recovers via the on-disk manifest.
""").
-spec gc(t(), [binary()] | epoch()) -> {t(), map()}.

gc(#?MODULE{} = T, Epoch) when is_integer(Epoch) ->
    {T, #{compacted => false, reason => epoch_unsupported}};
gc(#?MODULE{sealed_views = []} = T, KeepRoots) when is_list(KeepRoots) ->
    {T, gc_noop_meta()};
gc(#?MODULE{} = T, KeepRoots) when is_list(KeepRoots) ->
    do_gc(T, KeepRoots).

-spec missing_set(t(), binary()) -> sets:set(binary()).

missing_set(#?MODULE{} = T, Root) when is_binary(Root) ->
    do_missing_set(T, Root, sets:new([{version, 2}])).

-spec page_refs(page()) -> [binary()].

page_refs(Page) ->
    bondy_mst_page:refs(Page).

-spec delete(t()) -> ok.

delete(#?MODULE{writer = W} = T) ->
    Dir = bondy_mst_pack_writer:dir(W),
    ok = close(T),
    _ = file:del_dir_r(Dir),
    ok.

%% =============================================================================
%% API — extensions
%% =============================================================================

?DOC("""
Seals the current `incoming.pack` into a new sealed `pack-NNNN`
pair and refreshes the sealed-view cache.  Returns `{ok, T1}` for
the post-seal state (regardless of whether a new pack was created
or the call was a no-op against an empty incoming).
""").
-spec seal(t()) -> {ok, t()} | {error, term()}.

seal(#?MODULE{writer = W} = T) ->
    %% Seal is a write barrier — any staged tombstones are flushed
    %% so the on-disk state after seal/1 is fully durable.
    case do_flush_tombstones(T) of
        {ok, T0} ->
            case bondy_mst_pack_writer:seal(W) of
                {ok, no_op, W1} ->
                    {ok, T0#?MODULE{writer = W1}};
                {ok, PackId, W1} ->
                    Dir = bondy_mst_pack_writer:dir(W1),
                    case open_sealed_view(Dir, PackId) of
                        {ok, View} ->
                            Views = newest_first([View | T0#?MODULE.sealed_views]),
                            {ok, T0#?MODULE{writer = W1, sealed_views = Views}};
                        {error, _} = E ->
                            E
                    end;
                {error, _} = E ->
                    E
            end;
        {error, _} = E ->
            E
    end.

-spec dir(t()) -> file:filename_all().
dir(#?MODULE{writer = W}) -> bondy_mst_pack_writer:dir(W).

-spec instance_id(t()) -> binary().
instance_id(#?MODULE{writer = W}) -> bondy_mst_pack_writer:instance_id(W).

-spec sealed_pack_ids(t()) -> [non_neg_integer()].
sealed_pack_ids(#?MODULE{sealed_views = Views}) ->
    [V#sealed_view.pack_id || V <- Views].

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
required(K, M) ->
    case maps:find(K, M) of
        {ok, V} -> V;
        error   -> error({missing_opt, K})
    end.

%% @private
forward_opt(K, Src, Dst) ->
    case maps:find(K, Src) of
        {ok, V} -> Dst#{K => V};
        error   -> Dst
    end.

%% @private
%% Validates and returns an auto-seal threshold from `Opts`. Defaults to
%% the value in `bondy_mst_pack.hrl` (`?BONDY_MST_PACK_DEFAULT_AUTO_SEAL_*`);
%% `infinity` explicitly disables the threshold. A positive integer
%% enables the threshold; any other value is rejected with
%% `error({invalid_opt, K, V})` to fail fast at open time rather than
%% silently disabling the threshold.
validated_auto_seal(K, Opts) ->
    case maps:get(K, Opts, default_for(K)) of
        infinity -> infinity;
        N when is_integer(N), N > 0 -> N;
        Bad -> error({invalid_opt, K, Bad})
    end.

%% @private
%% Same shape as `validated_auto_seal/2` — `infinity` disables, a
%% positive integer enables, any other value is rejected.
validated_tombstones_flush(K, Opts) ->
    case maps:get(K, Opts, default_for(K)) of
        infinity -> infinity;
        N when is_integer(N), N > 0 -> N;
        Bad -> error({invalid_opt, K, Bad})
    end.

%% @private
default_for(auto_seal_records) ->
    ?BONDY_MST_PACK_DEFAULT_AUTO_SEAL_RECORDS;
default_for(auto_seal_bytes) ->
    ?BONDY_MST_PACK_DEFAULT_AUTO_SEAL_BYTES;
default_for(tombstones_flush_every_records) ->
    ?BONDY_MST_PACK_DEFAULT_TOMBSTONES_FLUSH_EVERY_RECORDS;
default_for(tombstones_flush_every_ms) ->
    ?BONDY_MST_PACK_DEFAULT_TOMBSTONES_FLUSH_EVERY_MS.

%% @private
ensure_dir(Dir) ->
    case filelib:ensure_path(Dir) of
        ok -> ok;
        {error, R} -> error({ensure_dir, Dir, R})
    end.

%% @private
%% Reads `tombstones` at `Dir`. Missing file → empty set (the
%% normal case for a fresh instance). Corrupt or unreadable file
%% is logged at WARNING and treated as empty so a single bad
%% tombstone file does not stop the store from opening — the
%% effect is that previously-deleted hashes become queryable
%% until the next compaction reclaims them.
load_tombstones(Dir) ->
    case bondy_mst_pack_tombstones:read(Dir) of
        {ok, Set} ->
            Set;
        {error, enoent} ->
            sets:new([{version, 2}]);
        {error, Reason} ->
            ?LOG_WARNING(#{
                event => mst_pack_store_tombstones_unreadable,
                dir => Dir,
                reason => Reason
            }),
            sets:new([{version, 2}])
    end.

%% @private
%% Auto-seal: triggers a seal when either threshold is crossed.
%%
%% A failure here is logged but does NOT propagate — the page that just
%% went into `incoming.pack` is already durable, and crossing the
%% threshold is "we should roll over now", not "we cannot accept
%% further writes". The next put will re-evaluate and retry the seal.
%% This matches the opportunistic tone of the durability batching in
%% the writer.
%%
%% Both thresholds default to `infinity`; in that case the function
%% short-circuits to avoid even the inspection calls.
maybe_auto_seal(#?MODULE{auto_seal_records = infinity,
                         auto_seal_bytes   = infinity} = T) ->
    T;
maybe_auto_seal(#?MODULE{writer = W,
                         auto_seal_records = RMax,
                         auto_seal_bytes   = BMax} = T) ->
    Records = bondy_mst_pack_writer:pending_count(W),
    Bytes = bondy_mst_pack_writer:incoming_offset(W),
    case threshold_crossed(Records, RMax)
         orelse threshold_crossed(Bytes, BMax) of
        false ->
            T;
        true ->
            case seal(T) of
                {ok, T1} ->
                    T1;
                {error, Reason} ->
                    ?LOG_WARNING(#{
                        event => mst_pack_store_auto_seal_failed,
                        reason => Reason,
                        pending_records => Records,
                        pending_bytes => Bytes
                    }),
                    T
            end
    end.

%% @private
threshold_crossed(_, infinity) -> false;
threshold_crossed(V, Max)      -> V >= Max.

%% @private
%% Updates the in-memory `free_set` and decides whether to persist
%% the change to disk. Size equality with the prior set means a
%% no-op (re-tombstoning an already-tombstoned hash, or
%% un-tombstoning a hash that wasn't tombstoned — both common from
%% the MST's page-revision loop). Otherwise the staged set replaces
%% the in-memory one and the debounce policy
%% (`tombstones_flush_every_records` / `tombstones_flush_every_ms`)
%% decides whether to fsync the tombstones file now or piggy-back
%% on the next seal / GC / explicit flush. Crash semantics match
%% the set_root debounce: in-memory is authoritative; on reopen
%% the WAL applier re-derives any unflushed tombstones from its
%% own watermark.
maybe_persist_free_set(#?MODULE{free_set = Old} = T, New, Op) ->
    case sets:size(New) =:= sets:size(Old) of
        true ->
            T;
        false ->
            T1 = T#?MODULE{
                free_set = New,
                tombstones_unsynced_count =
                    T#?MODULE.tombstones_unsynced_count + 1,
                tombstones_dirty = true
            },
            case tombstones_flush_due(T1) of
                true ->
                    case do_flush_tombstones(T1) of
                        {ok, T2}   -> T2;
                        {error, R} -> error({Op, {tombstones, R}})
                    end;
                false ->
                    T1
            end
    end.

%% @private
%% Mirrors `bondy_mst_pack_writer:root_flush_due/1`. Threshold-based
%% (records first, wall-clock second); both `infinity` disables
%% on-put flushing entirely — seal / GC / close / explicit flush
%% are then the only persistence drivers.
tombstones_flush_due(#?MODULE{tombstones_dirty = false}) ->
    false;
tombstones_flush_due(#?MODULE{
        tombstones_unsynced_count = N,
        tombstones_flush_every_records = K}) when
        is_integer(K), N >= K ->
    true;
tombstones_flush_due(#?MODULE{tombstones_flush_every_ms = infinity}) ->
    false;
tombstones_flush_due(#?MODULE{
        last_tombstones_flush_ms = Last,
        tombstones_flush_every_ms = TMs}) ->
    erlang:monotonic_time(millisecond) - Last >= TMs.

%% @private
%% Forces a tombstones file rewrite if there is a pending change.
%% Idempotent — no-op if `tombstones_dirty = false`.
do_flush_tombstones(#?MODULE{tombstones_dirty = false} = T) ->
    {ok, T};
do_flush_tombstones(#?MODULE{writer = W, free_set = FreeSet} = T) ->
    Dir = bondy_mst_pack_writer:dir(W),
    case bondy_mst_pack_tombstones:write(Dir, FreeSet) of
        ok ->
            {ok, reset_tombstones_flush_counters(T)};
        {error, _} = E ->
            E
    end.

%% @private
reset_tombstones_flush_counters(#?MODULE{} = T) ->
    T#?MODULE{
        tombstones_unsynced_count = 0,
        last_tombstones_flush_ms = erlang:monotonic_time(millisecond),
        tombstones_dirty = false
    }.

%% @private
open_sealed_views(_Dir, []) ->
    {ok, []};
open_sealed_views(Dir, [Id | Rest]) ->
    case open_sealed_view(Dir, Id) of
        {ok, V} ->
            case open_sealed_views(Dir, Rest) of
                {ok, Vs} -> {ok, [V | Vs]};
                {error, _} = E ->
                    _ = prim_file:close(V#sealed_view.pack_fd),
                    E
            end;
        {error, _} = E ->
            E
    end.

%% @private
open_sealed_view(Dir, PackId) ->
    IdxPath = bondy_mst_pack_paths:sealed_idx_path(Dir, PackId),
    PackPath = bondy_mst_pack_paths:sealed_pack_path(Dir, PackId),
    case prim_file:read_file(IdxPath) of
        {ok, IdxBin} ->
            case bondy_mst_pack_index:open(IdxBin) of
                {ok, Idx} ->
                    case prim_file:open(PackPath, [read, raw, binary]) of
                        {ok, Fd} ->
                            {ok, #sealed_view{
                                pack_id = PackId, idx = Idx, pack_fd = Fd
                            }};
                        {error, R} ->
                            {error, {sealed_pack, PackId, R}}
                    end;
                {error, R} ->
                    {error, {sealed_idx, PackId, R}}
            end;
        {error, R} ->
            {error, {sealed_idx, PackId, R}}
    end.

%% @private
newest_first(Views) ->
    lists:reverse(lists:keysort(#sealed_view.pack_id, Views)).

%% @private
do_get(#?MODULE{writer = W, sealed_views = Views}, Hash) ->
    case bondy_mst_pack_writer:pending_read(W, Hash) of
        {ok, Bytes} ->
            deserialise(Bytes);
        not_found ->
            get_from_sealed(Views, Hash);
        {error, R} ->
            error({get, R})
    end.

%% @private
get_from_sealed([], _Hash) ->
    undefined;
get_from_sealed([V | Rest], Hash) ->
    case bondy_mst_pack_index:lookup(V#sealed_view.idx, Hash) of
        not_found ->
            get_from_sealed(Rest, Hash);
        {ok, Offset} ->
            case bondy_mst_pack_io:read_record(V, Hash, Offset) of
                {ok, Bytes} -> deserialise(Bytes);
                not_found   -> get_from_sealed(Rest, Hash);
                {error, R}  -> error({get, R})
            end
    end.

%% @private
do_has(#?MODULE{writer = W, sealed_views = Views}, Hash) ->
    case bondy_mst_pack_writer:pending_lookup(W, Hash) of
        {ok, _} ->
            true;
        not_found ->
            lists:any(
                fun(#sealed_view{idx = Idx}) ->
                    bondy_mst_pack_index:lookup(Idx, Hash) =/= not_found
                end,
                Views
            )
    end.

%% @private
%% Enumerate every hash known to the store: pending first, then
%% every sealed view in newest-first order. Hashes are de-duplicated;
%% `free_set` members are excluded.
enumerate_hashes(#?MODULE{writer = W, sealed_views = Views,
                         free_set = FreeSet}) ->
    Pending = bondy_mst_pack_writer:pending_hashes(W),
    Seen0 = lists:foldl(
        fun(H, M) ->
            case sets:is_element(H, FreeSet) of
                true  -> M;
                false -> M#{H => true}
            end
        end,
        #{},
        Pending
    ),
    Seen = lists:foldl(
        fun(#sealed_view{idx = Idx}, M) ->
            lists:foldl(
                fun({H, _}, A) ->
                    case sets:is_element(H, FreeSet) of
                        true  -> A;
                        false -> A#{H => true}
                    end
                end,
                M,
                bondy_mst_pack_index:entries(Idx)
            )
        end,
        Seen0,
        Views
    ),
    maps:keys(Seen).

%% @private
do_missing_set(T, Hash, Acc) ->
    case get(T, Hash) of
        undefined ->
            sets:add_element(Hash, Acc);
        Page ->
            lists:foldl(
                fun(Ref, A) -> do_missing_set(T, Ref, A) end,
                Acc,
                page_refs(Page)
            )
    end.

%% @private
%% Page serialisation: only the hash-bearing subset `{Level, Low,
%% List}` is written — `freed_at` is per-replica metadata that
%% `bondy_mst_page:hash/2` deliberately excludes, so persisting it
%% would break content-addressing across replicas.
serialise(Page) ->
    Level = bondy_mst_page:level(Page),
    Low = bondy_mst_page:low(Page),
    List = bondy_mst_page:list(Page),
    erlang:term_to_binary({Level, Low, List},
                          [deterministic, {minor_version, 2}]).

%% @private
deserialise(Bytes) ->
    {Level, Low, List} = erlang:binary_to_term(Bytes, [safe]),
    bondy_mst_page:new(Level, Low, List).

%% =============================================================================
%% PRIVATE — gc
%% =============================================================================

%% @private
gc_noop_meta() ->
    #{compacted => false, retired => [], new_pack => undefined,
      kept => 0, dropped => 0}.

%% @private
do_gc(#?MODULE{} = T, KeepRoots) ->
    Reachable = reachable_set(T, KeepRoots),
    {KeptHashes, Dropped} = partition_sealed(T, Reachable),
    case should_compact(T, Dropped) of
        false ->
            {T, gc_noop_meta()};
        true ->
            apply_compaction(T, KeptHashes, Dropped)
    end.

%% @private
%% Transitive page-ref walk starting from `KeepRoots`. Missing refs
%% (root or transitive) are silently skipped — the resulting set is
%% the largest subset of the store's hashes reachable from the given
%% roots given the current page contents.
reachable_set(T, KeepRoots) ->
    lists:foldl(
        fun(R, Acc) -> walk_reachable(T, R, Acc) end,
        sets:new([{version, 2}]),
        KeepRoots
    ).

%% @private
walk_reachable(_T, undefined, Acc) ->
    Acc;
walk_reachable(T, Hash, Acc) when is_binary(Hash) ->
    case sets:is_element(Hash, Acc) of
        true ->
            Acc;
        false ->
            case do_get(T, Hash) of
                undefined ->
                    Acc;
                Page ->
                    Acc1 = sets:add_element(Hash, Acc),
                    lists:foldl(
                        fun(Ref, A) -> walk_reachable(T, Ref, A) end,
                        Acc1,
                        bondy_mst_page:refs(Page)
                    )
            end
    end.

%% @private
%% Walk every sealed entry once. Newest-first dedup: if the same hash
%% appears in multiple sealed packs (legal because content is
%% identical), it's accounted for exactly once.
partition_sealed(#?MODULE{sealed_views = Views, free_set = FreeSet},
                Reachable) ->
    Init = {[], 0, sets:new([{version, 2}])},
    {Kept, Dropped, _Seen} = lists:foldl(
        fun(#sealed_view{idx = Idx}, Acc) ->
            lists:foldl(
                fun({H, _Off}, {K, D, S}) ->
                    case sets:is_element(H, S) of
                        true ->
                            {K, D, S};
                        false ->
                            S1 = sets:add_element(H, S),
                            Keep = sets:is_element(H, Reachable)
                                andalso not sets:is_element(H, FreeSet),
                            case Keep of
                                true  -> {[H | K], D, S1};
                                false -> {K, D + 1, S1}
                            end
                    end
                end,
                Acc,
                bondy_mst_pack_index:entries(Idx)
            )
        end,
        Init,
        Views
    ),
    {lists:sort(Kept), Dropped}.

%% @private
%% Compact whenever something would actually change on disk: dropped
%% entries to remove, OR multiple sealed packs to coalesce into one.
should_compact(_, Dropped) when Dropped > 0 ->
    true;
should_compact(#?MODULE{sealed_views = Views}, 0) ->
    length(Views) > 1.

%% @private
%% A reader closure that, given a hash, returns the bytes stored in
%% whichever sealed view holds it. Used by `bondy_mst_pack_writer:
%% create_sealed_pack/6` to stream the compacted pack record-by-record.
sealed_reader(Views) ->
    fun(Hash) -> read_sealed_bytes(Views, Hash) end.

%% @private
read_sealed_bytes([], Hash) ->
    {error, {gc_missing_sealed, Hash}};
read_sealed_bytes([V | Rest], Hash) ->
    case bondy_mst_pack_index:lookup(V#sealed_view.idx, Hash) of
        not_found ->
            read_sealed_bytes(Rest, Hash);
        {ok, Off} ->
            case bondy_mst_pack_io:read_record(V, Hash, Off) of
                {ok, Body} ->
                    {ok, Body};
                not_found ->
                    %% bloom false positive that survived binary search —
                    %% try the next pack
                    read_sealed_bytes(Rest, Hash);
                {error, _} = E ->
                    E
            end
    end.

%% @private
apply_compaction(T, KeptHashes, Dropped) ->
    #?MODULE{writer = W, sealed_views = Views} = T,
    Dir = bondy_mst_pack_writer:dir(W),
    IH = bondy_mst_pack_writer:instance_hash(W),
    Algo = bondy_mst_pack_writer:hash_algo(W),
    %% `sealed_views` is newest-first (see record-field invariant);
    %% sort here to surface retired ids in ascending order for the
    %% `compaction_meta()` map — `remove_sealed_packs/2` itself does
    %% not require sorted input.
    OldIds = lists:sort([V#sealed_view.pack_id || V <- Views]),
    NewPackId = lists:max(OldIds) + 1,
    case write_compacted_pack(Dir, IH, Algo, NewPackId, KeptHashes,
                              sealed_reader(Views)) of
        ok ->
            commit_compaction(T, Dir, OldIds, NewPackId, KeptHashes,
                              Dropped);
        {error, R} ->
            ?LOG_ERROR(#{
                event => mst_pack_store_gc_write_failed,
                pack_id => NewPackId,
                reason => R
            }),
            {T, #{compacted => false, error => R}}
    end.

%% @private
write_compacted_pack(_Dir, _IH, _Algo, _NewPackId, [], _Reader) ->
    %% Empty kept set — no new pack to write, just retire the old ones.
    ok;
write_compacted_pack(Dir, IH, Algo, NewPackId, Hashes, Reader) ->
    bondy_mst_pack_writer:create_sealed_pack(
        Dir, IH, Algo, NewPackId, Hashes, Reader
    ).

%% @private
commit_compaction(T, Dir, OldIds, NewPackId, KeptHashes, Dropped) ->
    W0 = T#?MODULE.writer,
    M0 = bondy_mst_pack_writer:manifest(W0),
    M1 = bondy_mst_pack_manifest:remove_sealed_packs(M0, OldIds),
    M2 = case KeptHashes of
        [] -> M1;
        _  -> bondy_mst_pack_manifest:add_sealed_pack(M1, NewPackId)
    end,
    M3 = bondy_mst_pack_manifest:with_last_compacted_at(
        M2, erlang:system_time(millisecond)
    ),
    case bondy_mst_pack_manifest:write(Dir, M3) of
        ok ->
            finalise_compaction(T, M3, OldIds, NewPackId, KeptHashes, Dropped);
        {error, R} ->
            %% Roll back: delete the just-written sealed pack (if any).
            case KeptHashes of
                [] -> ok;
                _  -> bondy_mst_pack_writer:delete_sealed_pack_files(Dir,
                                                                     NewPackId)
            end,
            ?LOG_ERROR(#{
                event => mst_pack_store_gc_manifest_swap_failed,
                reason => R
            }),
            {T, #{compacted => false, error => R}}
    end.

%% @private
%% Manifest is durable; the rest is bookkeeping. Opening the just-
%% written sealed view is the last step. The failure window is narrow,
%% but the typical failure modes — EMFILE, EAGAIN, transient EIO — are
%% recoverable. We retry once before raising: the idx was just fsync'd,
%% so it is hot in the OS page cache and the retry is essentially free.
%% If the second attempt also fails the on-disk state is still correct
%% and the next reopen rebuilds the in-memory view from the manifest.
finalise_compaction(T, M, OldIds, NewPackId, KeptHashes, Dropped) ->
    OldViews = T#?MODULE.sealed_views,
    W1 = bondy_mst_pack_writer:set_manifest(T#?MODULE.writer, M),
    Dir = bondy_mst_pack_writer:dir(W1),
    lists:foreach(
        fun(#sealed_view{pack_fd = Fd}) -> _ = prim_file:close(Fd) end,
        OldViews
    ),
    NewViews = case KeptHashes of
        [] ->
            [];
        _ ->
            open_new_view_or_raise(Dir, NewPackId)
    end,
    lists:foreach(
        fun(Id) -> bondy_mst_pack_writer:delete_sealed_pack_files(Dir, Id) end,
        OldIds
    ),
    %% GC just rewrote the manifest and the sealed packs; ride the
    %% tombstones rewrite along so the on-disk state is internally
    %% consistent post-compaction (matches the per-PR architectural
    %% rule that GC commit yields fully durable state).
    Pruned = prune_applied_tombstones(W1, T#?MODULE.free_set),
    T0 = T#?MODULE{
        writer       = W1,
        sealed_views = NewViews,
        free_set     = Pruned,
        tombstones_dirty = true
    },
    T1 = case do_flush_tombstones(T0) of
        {ok, FlushedT} ->
            FlushedT;
        {error, Reason} ->
            error({gc, {tombstones, Reason}})
    end,
    Meta = #{
        compacted         => true,
        retired           => OldIds,
        new_pack          =>
            case KeptHashes of [] -> undefined; _ -> NewPackId end,
        kept              => length(KeptHashes),
        dropped           => Dropped,
        last_compacted_at => bondy_mst_pack_manifest:last_compacted_at(M)
    },
    {T1, Meta}.

%% @private
%% Opens the just-written sealed view with a single retry on failure.
%% The first attempt failure is logged at WARNING for observability;
%% the second attempt failure logs at ERROR and raises so the calling
%% process restarts and recovers from the manifest. We surface the
%% second reason (not the first) in the raise so the caller's logs
%% point at the persistent fault rather than the transient one.
open_new_view_or_raise(Dir, PackId) ->
    case open_sealed_view(Dir, PackId) of
        {ok, V} ->
            [V];
        {error, R1} ->
            ?LOG_WARNING(#{
                event => mst_pack_store_gc_open_view_retry,
                pack_id => PackId,
                reason => R1
            }),
            case open_sealed_view(Dir, PackId) of
                {ok, V} ->
                    [V];
                {error, R2} ->
                    ?LOG_ERROR(#{
                        event => mst_pack_store_gc_open_view_failed,
                        pack_id => PackId,
                        first_reason => R1,
                        second_reason => R2
                    }),
                    error({gc_open_view, PackId, R2})
            end
    end.

%% @private
%% A tombstone on a hash that lived only in sealed packs has now been
%% applied — the entry isn't in the new pack. Drop those.  Tombstones
%% targeting hashes still in `incoming.pack` (pending) stay alive; the
%% next seal will fold them into a future compaction.
prune_applied_tombstones(W, FreeSet) ->
    Pending = bondy_mst_pack_writer:pending_hashes(W),
    PendingSet = sets:from_list(Pending, [{version, 2}]),
    sets:filter(
        fun(H) -> sets:is_element(H, PendingSet) end,
        FreeSet
    ).
