%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_mst_pack_writer).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").
-include("bondy_mst_pack.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Pack-store writer: owns the open `incoming.pack` fd, the in-memory
pending hash index, and the seal flow that rotates the incoming
pack into a numbered sealed pack with its companion `.idx`.

See `_design/latest/MST_PAGE_STORE_DESIGN.md` §3 (pack format)
and §5 (seal flow).

## Purity boundary

The pure codec modules (`bondy_mst_pack_codec`,
`bondy_mst_pack_index`, `bondy_mst_pack_manifest`) own the
on-disk wire format and pure record arithmetic. This module is
the *first* place that performs sustained file I/O against an
instance directory: it opens `incoming.pack`, appends records,
and on `seal/1` writes the immutable `pack-NNNN.pack` /
`pack-NNNN.idx` files and atomically updates the manifest.

## Lifecycle

```
open(Dir, #{instance_id := Id})
    → reads or creates manifest
    → opens / creates incoming.pack
    → if manifest says incoming_pack=present, scans incoming.pack
      to rebuild the pending hash → offset map
    → returns writer state

append(W, Page)
    → Hash = sha256(Page)
    → if Hash already in pending, no-op (idempotent)
    → else encode record, write to incoming.pack, advance offset,
      datasync the fd (durability of in-progress writes is
      controlled by the caller; for now we datasync per record)

seal(W)
    → Read every record body out of incoming.pack into memory
      (one pread per pending entry)
    → Sort by hash, dedup
    → Write `pack-NNNN.pack.tmp` (header, sorted records, trailer)
      and `pack-NNNN.idx.tmp`
    → fsync both, rename to final names, fsync dir
    → Update manifest atomically (sealed_packs += [N],
      incoming_pack := absent)
    → Unlink incoming.pack (the next append re-creates it)

close(W) → close incoming.pack fd; manifest unchanged
```

## Crash safety

The seal flow follows the §5 ordering: every new file is durable
before the manifest swap, so a crash mid-seal cannot leave a
manifest pointing at a pack that isn't on disk. The reverse —
orphan `.pack`/`.idx` files on disk that aren't in the manifest,
left by a crash between rename and manifest swap (seal) or between
manifest swap and unlink (GC) — is cleaned by the orphan scanner
in `do_open/4` before the writer surfaces `{ok, t()}`. Any
`*.tmp` rename artefacts are deleted at the same point.

Per-append durability is governed by the batching policy
(`sync_every_records` / `sync_every_ms`, see `open_opts()`).
Defaults (see `bondy_mst_pack.hrl`): `sync_every_records = 32`,
`sync_every_ms = 200`. The pack store sits beneath a WAL that is
the authoritative source of truth, so per-record fsync is
unnecessary in the common case — the WAL applier re-derives any
unsynced pages on recovery. Callers that need stricter durability
(e.g. when the pack store is the source of truth) set
`sync_every_records = 1`.

## What this module does NOT do

- Multi-writer arbitration (caller must serialise — the gen_server
  wrapper does so by owning the single writer instance).
- Recovery / truncation of partially-written records (deferred).
- Compaction / GC across sealed packs (separate module).
- Cross-pack read lookup (that's `bondy_mst_pack_reader`).
""").

-record(?MODULE, {
    dir              :: file:filename_all(),
    instance_id      :: binary(),
    hash_algo        :: atom(),
    instance_hash    :: non_neg_integer(),
    manifest         :: bondy_mst_pack_manifest:t(),
    incoming_fd      :: file:fd() | undefined,
    incoming_offset  :: non_neg_integer(),
    %% Hash => {Offset, PageLen}. Offset is the byte offset of the
    %% record header in incoming.pack; PageLen is the body length so
    %% seal can pread the body without first re-parsing the header.
    pending          :: #{binary() => {non_neg_integer(), non_neg_integer()}},
    next_pack_id      :: pos_integer(),
    %% Durability policy. After each successful append, the writer
    %% datasyncs when EITHER `unsynced_count >= sync_every_records`
    %% OR `monotonic_ms - last_sync_ms >= sync_every_ms`. The
    %% T-based threshold is opportunistic — it only fires when an
    %% append happens; callers wanting a true wall-clock guarantee
    %% should run their own timer that calls `flush/1`.
    sync_every_records :: pos_integer(),
    sync_every_ms      :: pos_integer() | infinity,
    unsynced_count = 0 :: non_neg_integer(),
    last_sync_ms       :: integer()
}).

-type t() :: #?MODULE{}.

-type open_opts() :: #{
    instance_id := binary(),
    hash_algo   => atom(),
    sync_every_records => pos_integer(),
    sync_every_ms      => pos_integer() | infinity
}.

-type open_error() ::
    {missing_field, atom()}
    | {manifest, term()}
    | {incoming, term()}
    | {pending_scan, term()}
    | {orphan_cleanup, term()}
    | needs_recovery
    | {instance_id_mismatch, binary(), binary()}
    | {hash_algo_mismatch, atom(), atom()}.

-type append_error() ::
    {write, term()}
    | {sync, term()}.

-type seal_error() ::
    {seal, term()}
    | {idx_build, bondy_mst_pack_index:build_error()}
    | {manifest, term()}.

-export_type([t/0]).
-export_type([open_opts/0]).
-export_type([open_error/0]).
-export_type([append_error/0]).
-export_type([seal_error/0]).

%% Lifecycle
-export([open/2]).
-export([close/1]).

%% Mutation
-export([append/2]).
-export([flush/1]).
-export([seal/1]).
-export([set_root/2]).
-export([set_manifest/2]).

%% Sealed-pack I/O helpers (shared with gc/2 in `bondy_mst_pack_store`)
-export([create_sealed_pack/6]).
-export([delete_sealed_pack_files/2]).

%% Inspection
-export([dir/1]).
-export([manifest/1]).
-export([instance_id/1]).
-export([instance_hash/1]).
-export([hash_algo/1]).
-export([pending_count/1]).
-export([pending_hashes/1]).
-export([pending_lookup/2]).
-export([pending_read/2]).
-export([current_root/1]).
-export([incoming_offset/1]).
-export([next_pack_id/1]).
-export([unsynced_count/1]).

%% =============================================================================
%% API — lifecycle
%% =============================================================================

?DOC("""
Opens (or creates) a pack-store instance at `Dir`.

`Opts` must carry `instance_id` (a non-empty binary). `hash_algo`
defaults to `sha256`. On first open in an empty directory, a
fresh manifest is written. On reopen, the existing manifest is
loaded and validated against the supplied options.

If the manifest declares `incoming_pack = present` and the file
exists, the writer scans it to rebuild the pending hash→offset
map. Any decode error in the scan returns `{error, needs_recovery}`;
the dedicated recovery path (next phase) is responsible for
truncating partial records.
""").
-spec open(file:filename_all(), open_opts()) ->
    {ok, t()} | {error, open_error()}.

open(Dir, Opts) when is_list(Dir) orelse is_binary(Dir), is_map(Opts) ->
    case maps:find(instance_id, Opts) of
        {ok, InstanceId} when is_binary(InstanceId), byte_size(InstanceId) > 0 ->
            HashAlgo = maps:get(hash_algo, Opts, sha256),
            Policy = #{
                sync_every_records =>
                    maps:get(sync_every_records, Opts,
                             ?BONDY_MST_PACK_DEFAULT_SYNC_EVERY_RECORDS),
                sync_every_ms      =>
                    maps:get(sync_every_ms, Opts,
                             ?BONDY_MST_PACK_DEFAULT_SYNC_EVERY_MS)
            },
            do_open(Dir, InstanceId, HashAlgo, Policy);
        _ ->
            {error, {missing_field, instance_id}}
    end.

?DOC("""
Closes the incoming.pack fd. The on-disk manifest and any sealed
packs are untouched. Idempotent: calling `close/1` on an already-
closed writer is a no-op.
""").
-spec close(t()) -> ok.

close(#?MODULE{incoming_fd = undefined}) ->
    ok;
close(#?MODULE{incoming_fd = Fd} = W) ->
    _ = flush(W),
    _ = prim_file:close(Fd),
    ok.

%% =============================================================================
%% API — mutation
%% =============================================================================

?DOC("""
Appends a page to the incoming pack. Returns `{ok, Hash, W1}`.

The hash is derived from `Page` using the writer's configured
algorithm (currently always sha256). If the same hash is already
pending in the in-memory index, the call is a no-op — the file
is not touched and the returned state is byte-for-byte identical.

CRC + record bytes are written to the OS page cache, then a
`datasync` is issued only when the batching policy's threshold is
crossed (`sync_every_records` or `sync_every_ms`, configured at
`open/2`). Defaults batch ~32 appends or 200 ms (see
`bondy_mst_pack.hrl`). Callers needing per-record durability set
`sync_every_records = 1`; callers needing an explicit boundary
drive `flush/1` directly.

A failure surfaces as `{error, {write, Reason}}` (record write) or
`{error, {sync, Reason}}` (deferred datasync triggered by this
append); the in-memory pending map is not updated. The on-disk
byte cursor may have moved if the failure occurred partway
through; recovery on reopen will detect and truncate any partial
trailing record.
""").
-spec append(t(), Page :: binary()) ->
    {ok, Hash :: binary(), t()} | {error, append_error()}.

append(#?MODULE{} = W, Page) when is_binary(Page) ->
    Hash = compute_hash(W#?MODULE.hash_algo, Page),
    case maps:is_key(Hash, W#?MODULE.pending) of
        true ->
            {ok, Hash, W};
        false ->
            case ensure_incoming_open(W) of
                {ok, W1} ->
                    do_append(W1, Hash, Page);
                {error, _} = E ->
                    E
            end
    end.

?DOC("""
Forces a `datasync` of the incoming pack fd if there are any unsynced
records buffered. Idempotent: if the writer has no unsynced data (or
the incoming pack hasn't been created yet), returns `{ok, W}` without
touching the disk.

The batching policy normally takes care of durability automatically;
this exists for callers that need an explicit boundary — e.g. before
a snapshot, before close, or in response to an external durability
request.
""").
-spec flush(t()) -> {ok, t()} | {error, term()}.

flush(#?MODULE{unsynced_count = 0} = W) ->
    {ok, W};
flush(#?MODULE{incoming_fd = undefined} = W) ->
    {ok, W};
flush(#?MODULE{} = W) ->
    do_sync(W).

?DOC("""
Seals the incoming pack into a numbered sealed pack.

If the pending set is empty, returns `{ok, no_op, W}` without
touching disk — there's nothing to seal. The manifest's
`incoming_pack` flag is reconciled to `absent` if it was lingering
`present`. (Deferred: empty-seal might still want to remove the
incoming.pack file if it has only a header.)

Otherwise, the four-step seal:

1. Read all record bodies back from incoming.pack via pread.
2. Sort by hash, dedup adjacent duplicates (keep first), write
   `pack-NNNN.pack.tmp` (header + records + trailer) and
   `pack-NNNN.idx.tmp`.
3. fsync both files, rename to final, fsync directory.
4. Atomically swap the manifest to include `pack-NNNN` and clear
   `incoming_pack`.  Then delete `incoming.pack`.

Returns `{ok, PackId, W1}` on success or a typed `{error, _}` if
any step fails. Failure at step 1–3 leaves the manifest unchanged
and any `.tmp` orphans visible for recovery. Failure at step 4
(post-manifest-swap) leaves a stale `incoming.pack` on disk that
the recovery scanner removes on next open.
""").
-spec seal(t()) ->
    {ok, no_op, t()} | {ok, PackId :: pos_integer(), t()} | {error, seal_error()}.

seal(#?MODULE{pending = P} = W) when map_size(P) =:= 0 ->
    case bondy_mst_pack_manifest:incoming_pack(W#?MODULE.manifest) of
        absent ->
            {ok, no_op, W};
        present ->
            %% Pending is empty but manifest says present — reconcile.
            M1 = bondy_mst_pack_manifest:with_incoming_pack(W#?MODULE.manifest, absent),
            case bondy_mst_pack_manifest:write(W#?MODULE.dir, M1) of
                ok ->
                    {ok, no_op, W#?MODULE{manifest = M1}};
                {error, R} ->
                    {error, {manifest, R}}
            end
    end;
seal(#?MODULE{} = W) ->
    do_seal(W).

?DOC("""
Persists `Root` as the manifest's `current_root`. Atomically rewrites
the manifest via tmp + rename + fsync_dir. The change is visible in
memory only after the rewrite succeeds; failure returns `{error, _}`
and the writer's manifest is unchanged.
""").
-spec set_root(t(), Root :: binary() | undefined) ->
    {ok, t()} | {error, term()}.

set_root(#?MODULE{} = W, Root) when is_binary(Root); Root =:= undefined ->
    M1 = bondy_mst_pack_manifest:with_current_root(W#?MODULE.manifest, Root),
    case bondy_mst_pack_manifest:write(W#?MODULE.dir, M1) of
        ok ->
            {ok, W#?MODULE{manifest = M1}};
        {error, _} = E ->
            E
    end.

?DOC("""
Refreshes the writer's cached manifest and `next_pack_id` after an
out-of-band manifest swap (e.g., compaction in `bondy_mst_pack_store:gc/2`).

The caller is responsible for having already persisted `M` to disk; this
just updates the in-memory copies.
""").
-spec set_manifest(t(), bondy_mst_pack_manifest:t()) -> t().

set_manifest(#?MODULE{} = W, M) ->
    W#?MODULE{
        manifest = M,
        next_pack_id = next_pack_id_from(M)
    }.

?DOC("""
Streams a new sealed `pack-NNNN.pack` + `.idx` pair from `Hashes` (sorted
ascending, no duplicates) and a `Reader` function that returns the body
bytes for each hash on demand.

For each hash the writer pread's the body via `Reader`, encodes the
record, writes it to the tmp pack file, and folds it into a running
sha256 context — so peak RAM is one record body, not the full pack.
After the last record the running sha256 becomes the pack's trailer.
The `.idx` is built from the offsets accumulated during the stream
(small — 32-byte hash + 8-byte offset per entry).

Performs the full pipeline: tmp write, datasync, rename, dir fsync,
with tmp cleanup on any failure. Does NOT touch the manifest or
`incoming.pack` — those are the caller's concern (the writer's own
`seal/1` does the manifest swap; `gc/2` in `bondy_mst_pack_store` does
its own atomic swap).

A `Reader` failure for any hash aborts the stream and surfaces as
`{error, _}`; the tmp files are cleaned up.
""").
-spec create_sealed_pack(
        Dir :: file:filename_all(),
        InstanceHash :: non_neg_integer(),
        HashAlgo :: atom(),
        PackId :: pos_integer(),
        Hashes :: [binary()],
        Reader :: fun((binary()) -> {ok, binary()} | {error, term()})
    ) -> ok | {error, term()}.

create_sealed_pack(Dir, InstanceHash, HashAlgo, PackId, Hashes, Reader) ->
    case stream_sealed_pack(Dir, InstanceHash, HashAlgo, PackId, Hashes,
                            Reader) of
        {ok, Entries} ->
            case write_sealed_idx_from_entries(Dir, PackId, Entries) of
                ok ->
                    case rename_sealed_pair(Dir, PackId) of
                        ok ->
                            ok;
                        {error, _} = E ->
                            cleanup_tmp(Dir, PackId),
                            E
                    end;
                {error, R} ->
                    cleanup_tmp(Dir, PackId),
                    {error, R}
            end;
        {error, R} ->
            cleanup_tmp(Dir, PackId),
            {error, R}
    end.

?DOC("""
Deletes the on-disk `pack-NNNN.pack` and `pack-NNNN.idx` for a
retired sealed pack. Missing files are tolerated (idempotent — safe
to call on a half-rolled-back compaction).
""").
-spec delete_sealed_pack_files(file:filename_all(), non_neg_integer()) -> ok.

delete_sealed_pack_files(Dir, PackId) ->
    _ = prim_file:delete(bondy_mst_pack_paths:sealed_pack_path(Dir, PackId)),
    _ = prim_file:delete(bondy_mst_pack_paths:sealed_idx_path(Dir, PackId)),
    ok.

%% =============================================================================
%% API — inspection
%% =============================================================================

-spec dir(t()) -> file:filename_all().
dir(#?MODULE{dir = D}) -> D.

-spec manifest(t()) -> bondy_mst_pack_manifest:t().
manifest(#?MODULE{manifest = M}) -> M.

-spec instance_id(t()) -> binary().
instance_id(#?MODULE{instance_id = Id}) -> Id.

-spec instance_hash(t()) -> non_neg_integer().
instance_hash(#?MODULE{instance_hash = IH}) -> IH.

-spec hash_algo(t()) -> atom().
hash_algo(#?MODULE{hash_algo = A}) -> A.

-spec pending_count(t()) -> non_neg_integer().
pending_count(#?MODULE{pending = P}) -> map_size(P).

-spec pending_hashes(t()) -> [binary()].
pending_hashes(#?MODULE{pending = P}) -> lists:sort(maps:keys(P)).

?DOC("""
Returns the on-disk record offset of `Hash` in `incoming.pack`,
or `not_found`. Used by an in-process reader (the gen_server)
to resolve a recent put without opening a separate reader.
""").
-spec pending_lookup(t(), binary()) ->
    {ok, {non_neg_integer(), non_neg_integer()}} | not_found.

pending_lookup(#?MODULE{pending = P}, Hash) ->
    case maps:find(Hash, P) of
        {ok, V} -> {ok, V};
        error   -> not_found
    end.

?DOC("""
Reads the page body associated with `Hash` from `incoming.pack` via
the writer's own fd. Returns `{ok, Body}` for a present hash, where
`Body` is the bytes that were passed to `append/2` (so callers can
deserialise back to their domain object), `not_found` for an absent
hash, or `{error, _}` on I/O.
""").
-spec pending_read(t(), binary()) ->
    {ok, binary()} | not_found | {error, term()}.

pending_read(#?MODULE{pending = P, incoming_fd = Fd}, Hash) ->
    case maps:find(Hash, P) of
        {ok, {_Offset, 0}} ->
            {ok, <<>>};
        {ok, {Offset, Len}} when Fd =/= undefined ->
            BodyOff = Offset + bondy_mst_pack_codec:record_header_bytes(),
            case prim_file:pread(Fd, BodyOff, Len) of
                {ok, Body} when byte_size(Body) =:= Len -> {ok, Body};
                {ok, _} -> {error, short_body};
                eof     -> {error, short_body};
                {error, _} = E -> E
            end;
        {ok, _} ->
            %% Pending entry but fd not open — invariant break.
            {error, fd_not_open};
        error ->
            not_found
    end.

-spec current_root(t()) -> binary() | undefined.
current_root(#?MODULE{manifest = M}) ->
    bondy_mst_pack_manifest:current_root(M).

-spec incoming_offset(t()) -> non_neg_integer().
incoming_offset(#?MODULE{incoming_offset = Off}) -> Off.

-spec next_pack_id(t()) -> pos_integer().
next_pack_id(#?MODULE{next_pack_id = N}) -> N.

?DOC("""
Number of records that have been written to `incoming.pack` but not
yet `datasync`'d to disk. Decreases to 0 after each successful sync
(either from the batching policy or an explicit `flush/1`).
""").
-spec unsynced_count(t()) -> non_neg_integer().
unsynced_count(#?MODULE{unsynced_count = N}) -> N.

%% =============================================================================
%% PRIVATE — open
%% =============================================================================

%% @private
do_open(Dir, InstanceId, HashAlgo, Policy) ->
    case ensure_dir(Dir) of
        ok ->
            case load_or_create_manifest(Dir, InstanceId, HashAlgo) of
                {ok, Manifest} ->
                    case validate_manifest(Manifest, InstanceId, HashAlgo) of
                        ok ->
                            case cleanup_orphan_packs(Dir, Manifest) of
                                ok ->
                                    open_incoming(Dir, InstanceId, HashAlgo,
                                                  Manifest, Policy);
                                {error, R} ->
                                    {error, {orphan_cleanup, R}}
                            end;
                        {error, _} = E ->
                            E
                    end;
                {error, R} ->
                    {error, {manifest, R}}
            end;
        {error, R} ->
            {error, {manifest, R}}
    end.

%% @private
ensure_dir(Dir) ->
    case filelib:ensure_path(Dir) of
        ok -> ok;
        {error, _} = E -> E
    end.

%% @private
load_or_create_manifest(Dir, InstanceId, HashAlgo) ->
    case bondy_mst_pack_manifest:read(Dir) of
        {ok, M} ->
            {ok, M};
        {error, enoent} ->
            M = bondy_mst_pack_manifest:new(InstanceId, HashAlgo),
            case bondy_mst_pack_manifest:write(Dir, M) of
                ok          -> {ok, M};
                {error, _} = E -> E
            end;
        {error, _} = E ->
            E
    end.

%% @private
validate_manifest(M, InstanceId, HashAlgo) ->
    case bondy_mst_pack_manifest:instance_id(M) of
        InstanceId ->
            case bondy_mst_pack_manifest:hash_algo(M) of
                HashAlgo ->
                    ok;
                Other ->
                    {error, {hash_algo_mismatch, Other, HashAlgo}}
            end;
        Other ->
            {error, {instance_id_mismatch, Other, InstanceId}}
    end.

%% @private
open_incoming(Dir, InstanceId, HashAlgo, Manifest, Policy) ->
    InstanceHash = erlang:phash2(InstanceId, 1 bsl 32),
    Path = bondy_mst_pack_paths:incoming_pack_path(Dir),
    Declared = bondy_mst_pack_manifest:incoming_pack(Manifest),
    Exists = filelib:is_regular(Path),
    case {Declared, Exists} of
        {absent, false} ->
            %% Defer creating incoming.pack until the first append.
            %% Keeps `open/close` cycles idempotent: the on-disk state
            %% matches the manifest's declared state at all times.
            {ok, fresh_state(Dir, InstanceId, HashAlgo, InstanceHash,
                             Manifest, Policy)};
        {present, true} ->
            resume_incoming(Dir, Path, InstanceId, HashAlgo,
                            InstanceHash, Manifest, Policy);
        {absent, true} ->
            %% Orphan from a previous crash; recovery's job.
            {error, needs_recovery};
        {present, false} ->
            {error, needs_recovery}
    end.

%% @private
fresh_state(Dir, InstanceId, HashAlgo, InstanceHash, Manifest, Policy) ->
    #?MODULE{
        dir = Dir,
        instance_id = InstanceId,
        hash_algo = HashAlgo,
        instance_hash = InstanceHash,
        manifest = Manifest,
        incoming_fd = undefined,
        incoming_offset = 0,
        pending = #{},
        next_pack_id = next_pack_id_from(Manifest),
        sync_every_records = maps:get(sync_every_records, Policy),
        sync_every_ms      = maps:get(sync_every_ms, Policy),
        unsynced_count     = 0,
        last_sync_ms       = erlang:monotonic_time(millisecond)
    }.

%% @private
%% Lazily creates incoming.pack on the writer's first append. Writes
%% the header, flips the manifest's incoming_pack flag to `present`,
%% and atomically swaps the manifest so the on-disk state matches the
%% in-memory state. A crash between creating the file and flipping the
%% manifest leaves an orphan that the recovery scanner removes.
ensure_incoming_open(#?MODULE{incoming_fd = Fd} = W) when Fd =/= undefined ->
    {ok, W};
ensure_incoming_open(#?MODULE{} = W) ->
    Path = bondy_mst_pack_paths:incoming_pack_path(W#?MODULE.dir),
    Header = bondy_mst_pack_codec:encode_pack_header(#{
        version       => bondy_mst_pack_codec:version(),
        flags         => 0,
        pack_id       => 0,
        instance_hash => W#?MODULE.instance_hash,
        hash_algo     => W#?MODULE.hash_algo,
        created_at    => erlang:system_time(millisecond),
        record_count  => 0
    }),
    case prim_file:open(Path, [read, write, raw, binary, exclusive]) of
        {ok, Fd} ->
            case prim_file:write(Fd, Header) of
                ok ->
                    case bondy_mst_io:datasync(Fd) of
                        ok ->
                            _ = bondy_mst_io:fsync_dir(W#?MODULE.dir),
                            flip_manifest_to_present(W, Fd, byte_size(Header));
                        {error, R} ->
                            _ = prim_file:close(Fd),
                            {error, {write, R}}
                    end;
                {error, R} ->
                    _ = prim_file:close(Fd),
                    {error, {write, R}}
            end;
        {error, R} ->
            {error, {write, R}}
    end.

%% @private
flip_manifest_to_present(W, Fd, HeaderSize) ->
    M = bondy_mst_pack_manifest:with_incoming_pack(W#?MODULE.manifest, present),
    case bondy_mst_pack_manifest:write(W#?MODULE.dir, M) of
        ok ->
            %% The header write was just datasync'd in ensure_incoming_open;
            %% rebase the T-timer so it counts from now rather than from
            %% `open/2` (which may have been many ms earlier and would
            %% spuriously trip a tight `sync_every_ms` on the first append).
            {ok, W#?MODULE{
                manifest        = M,
                incoming_fd     = Fd,
                incoming_offset = HeaderSize,
                last_sync_ms    = erlang:monotonic_time(millisecond)
            }};
        {error, R} ->
            _ = prim_file:close(Fd),
            _ = prim_file:delete(bondy_mst_pack_paths:incoming_pack_path(
                W#?MODULE.dir
            )),
            {error, {manifest, R}}
    end.

%% @private
resume_incoming(Dir, Path, InstanceId, HashAlgo, InstanceHash, Manifest,
                Policy) ->
    case prim_file:open(Path, [read, write, raw, binary]) of
        {ok, Fd} ->
            case scan_incoming(Fd, InstanceHash, HashAlgo) of
                {ok, EndOffset, Pending} ->
                    {ok, #?MODULE{
                        dir = Dir,
                        instance_id = InstanceId,
                        hash_algo = HashAlgo,
                        instance_hash = InstanceHash,
                        manifest = Manifest,
                        incoming_fd = Fd,
                        incoming_offset = EndOffset,
                        pending = Pending,
                        next_pack_id = next_pack_id_from(Manifest),
                        sync_every_records =
                            maps:get(sync_every_records, Policy),
                        sync_every_ms =
                            maps:get(sync_every_ms, Policy),
                        unsynced_count = 0,
                        last_sync_ms = erlang:monotonic_time(millisecond)
                    }};
                {error, R} ->
                    _ = prim_file:close(Fd),
                    {error, R}
            end;
        {error, R} ->
            {error, {incoming, R}}
    end.

%% @private
%% Scan an existing incoming.pack — parse header, walk records until EOF
%% or a decode error. Any decode error short of EOF aborts with
%% `needs_recovery`; the recovery phase will truncate.
scan_incoming(Fd, ExpectedInstanceHash, ExpectedAlgo) ->
    HeaderBytes = bondy_mst_pack_codec:header_bytes(),
    case prim_file:pread(Fd, 0, HeaderBytes) of
        {ok, HBin} when byte_size(HBin) =:= HeaderBytes ->
            case bondy_mst_pack_codec:decode_pack_header(HBin) of
                {ok, #{instance_hash := IH, hash_algo := A}}
                    when IH =:= ExpectedInstanceHash, A =:= ExpectedAlgo ->
                    scan_records(Fd, HeaderBytes, #{});
                {ok, _} ->
                    {error, {pending_scan, header_mismatch}};
                {error, R} ->
                    {error, {pending_scan, R}}
            end;
        {ok, _Short} ->
            {error, needs_recovery};
        eof ->
            {error, needs_recovery};
        {error, R} ->
            {error, {incoming, R}}
    end.

%% @private
scan_records(Fd, Offset, Pending) ->
    HdrBytes = bondy_mst_pack_codec:record_header_bytes(),
    case prim_file:pread(Fd, Offset, HdrBytes) of
        eof ->
            {ok, Offset, Pending};
        {ok, <<>>} ->
            {ok, Offset, Pending};
        {ok, Bin} when byte_size(Bin) < HdrBytes ->
            {error, needs_recovery};
        {ok, Bin} ->
            case bondy_mst_pack_codec:decode_record_header(Bin) of
                {ok, #{hash := H, page_len := L} = Header} ->
                    BodyOffset = Offset + HdrBytes,
                    case verify_scanned_body(Fd, BodyOffset, L, Header, H) of
                        ok ->
                            scan_records(
                                Fd,
                                BodyOffset + L,
                                Pending#{H => {Offset, L}}
                            );
                        {error, _} = E ->
                            E
                    end;
                {error, _} ->
                    {error, needs_recovery}
            end;
        {error, R} ->
            {error, {incoming, R}}
    end.

%% @private
%% Reading a record body during resume verifies both the per-record CRC
%% AND that the record's stored hash matches sha256(body). The two
%% checks together catch the case where a record was fully written but
%% an on-disk bit-flip silently corrupted the body — the seal path
%% would otherwise re-encode the corrupted body under the original
%% hash, producing a content-address-violating sealed pack.
%%
%% Treats any failure as `needs_recovery`; the dedicated recovery path
%% will decide whether to truncate or rebuild.
verify_scanned_body(_Fd, _BodyOffset, 0, Header, Hash) ->
    %% Zero-length body — pread returns `eof`, so verify against <<>>.
    case bondy_mst_pack_codec:verify_record(Header, <<>>) of
        ok ->
            case crypto:hash(sha256, <<>>) of
                Hash -> ok;
                _    -> {error, needs_recovery}
            end;
        {error, _} ->
            {error, needs_recovery}
    end;
verify_scanned_body(Fd, BodyOffset, L, Header, Hash) ->
    case prim_file:pread(Fd, BodyOffset, L) of
        {ok, Body} when byte_size(Body) =:= L ->
            case bondy_mst_pack_codec:verify_record(Header, Body) of
                ok ->
                    case crypto:hash(sha256, Body) of
                        Hash -> ok;
                        _    -> {error, needs_recovery}
                    end;
                {error, _} ->
                    {error, needs_recovery}
            end;
        _ ->
            {error, needs_recovery}
    end.

%% @private
next_pack_id_from(Manifest) ->
    Highest = case bondy_mst_pack_manifest:sealed_packs(Manifest) of
        [] -> bondy_mst_pack_manifest:deleted_through(Manifest);
        L  -> lists:max(L)
    end,
    Highest + 1.

%% @private
%% Orphan sealed-pack scanner (design doc §10.1, step 2).
%%
%% A crash between the sealed `.pack`/`.idx` rename-into-place (seal
%% step 3) and the manifest swap (seal step 4) leaves on-disk files
%% the manifest does not reference. The same shape arises post-GC
%% when retired packs are deleted: a crash between the manifest swap
%% and the unlink leaves the now-retired files behind.
%%
%% On every open we enumerate `pack-NNNN.{pack,idx}` and any
%% `*.tmp` siblings, then delete:
%%
%%   - `pack-NNNN.pack` / `pack-NNNN.idx` whose `NNNN` is not in
%%     the manifest's `sealed_packs` — they are orphans from one of
%%     the crash windows above.
%%   - any `*.tmp` rename artefacts — these only exist mid-seal, are
%%     never the source of truth once a crash interrupts the rename,
%%     and would otherwise accumulate.
%%
%% Cleanup is best-effort: individual deletion failures are logged
%% and skipped; the open continues. Only a `list_dir` failure
%% aborts the open (`{error, {orphan_cleanup, R}}`), because we
%% cannot safely proceed without knowing what is on disk.
cleanup_orphan_packs(Dir, Manifest) ->
    case prim_file:list_dir(Dir) of
        {ok, Names} ->
            Sealed = sets:from_list(
                bondy_mst_pack_manifest:sealed_packs(Manifest),
                [{version, 2}]
            ),
            lists:foreach(
                fun(Name) -> maybe_delete_orphan(Dir, Name, Sealed) end,
                Names
            ),
            ok;
        {error, R} ->
            {error, R}
    end.

%% @private
maybe_delete_orphan(Dir, Name, Sealed) ->
    case parse_pack_basename(Name) of
        {pack, Id} ->
            maybe_delete_sealed_orphan(Dir, Name, Id, Sealed, "pack");
        {idx, Id} ->
            maybe_delete_sealed_orphan(Dir, Name, Id, Sealed, "idx");
        {pack_tmp, _Id} ->
            force_delete_orphan(Dir, Name, "pack.tmp");
        {idx_tmp, _Id} ->
            force_delete_orphan(Dir, Name, "idx.tmp");
        not_pack ->
            ok
    end.

%% @private
maybe_delete_sealed_orphan(Dir, Name, Id, Sealed, Kind) ->
    case sets:is_element(Id, Sealed) of
        true ->
            ok;
        false ->
            force_delete_orphan(Dir, Name, Kind)
    end.

%% @private
force_delete_orphan(Dir, Name, Kind) ->
    Path = filename:join(Dir, Name),
    case prim_file:delete(Path) of
        ok ->
            ?LOG_NOTICE(#{
                event => mst_pack_store_orphan_deleted,
                kind => Kind,
                path => Path
            }),
            ok;
        {error, enoent} ->
            ok;
        {error, R} ->
            ?LOG_WARNING(#{
                event => mst_pack_store_orphan_delete_failed,
                kind => Kind,
                path => Path,
                reason => R
            }),
            ok
    end.

%% @private
%% Parse a directory entry into its pack-store classification.
%% Anything not matching `pack-<digits>.(pack|idx)(.tmp)?` is left
%% alone (manifest files, root file, future filenames, etc).
parse_pack_basename("pack-" ++ Rest) ->
    case split_digits(Rest) of
        {[], _} ->
            not_pack;
        {Digits, ".pack"} ->
            {pack, list_to_integer(Digits)};
        {Digits, ".idx"} ->
            {idx, list_to_integer(Digits)};
        {Digits, ".pack.tmp"} ->
            {pack_tmp, list_to_integer(Digits)};
        {Digits, ".idx.tmp"} ->
            {idx_tmp, list_to_integer(Digits)};
        _ ->
            not_pack
    end;
parse_pack_basename(_) ->
    not_pack.

%% @private
split_digits(S) ->
    lists:splitwith(fun(C) -> C >= $0 andalso C =< $9 end, S).

%% =============================================================================
%% PRIVATE — append
%% =============================================================================

%% @private
%% Append is split into (a) write the record, (b) optionally datasync per
%% the writer's batching policy. The write itself goes into the OS page
%% cache and is immediately visible to subsequent preads on the same fd;
%% durability against a kernel/power crash requires the datasync.
do_append(#?MODULE{incoming_fd = Fd, incoming_offset = Off} = W, Hash, Page) ->
    Record = bondy_mst_pack_codec:encode_record(Hash, Page),
    case prim_file:write(Fd, Record) of
        ok ->
            HdrBytes = bondy_mst_pack_codec:record_header_bytes(),
            NewOff = Off + HdrBytes + byte_size(Page),
            Pending = (W#?MODULE.pending)#{
                Hash => {Off, byte_size(Page)}
            },
            W1 = W#?MODULE{
                incoming_offset = NewOff,
                pending         = Pending,
                unsynced_count  = W#?MODULE.unsynced_count + 1
            },
            case maybe_sync_after_append(W1) of
                {ok, W2}       -> {ok, Hash, W2};
                {error, _} = E -> E
            end;
        {error, R} ->
            {error, {write, R}}
    end.

%% @private
maybe_sync_after_append(#?MODULE{unsynced_count = N,
                                 sync_every_records = K} = W) when N >= K ->
    do_sync(W);
maybe_sync_after_append(#?MODULE{sync_every_ms = infinity} = W) ->
    {ok, W};
maybe_sync_after_append(#?MODULE{last_sync_ms = Last,
                                 sync_every_ms = T} = W) ->
    Now = erlang:monotonic_time(millisecond),
    case Now - Last >= T of
        true  -> do_sync(W);
        false -> {ok, W}
    end.

%% @private
do_sync(#?MODULE{incoming_fd = Fd} = W) ->
    case bondy_mst_io:datasync(Fd) of
        ok ->
            {ok, W#?MODULE{
                unsynced_count = 0,
                last_sync_ms   = erlang:monotonic_time(millisecond)
            }};
        {error, R} ->
            {error, {sync, R}}
    end.

%% =============================================================================
%% PRIVATE — seal
%% =============================================================================

%% @private
do_seal(#?MODULE{
    dir = Dir, instance_hash = IH, hash_algo = HashAlgo,
    manifest = M, incoming_fd = Fd, pending = Pending,
    next_pack_id = PackId
} = W) ->
    Hashes = lists:sort(maps:keys(Pending)),
    Reader = pending_reader(Fd, Pending),
    case create_sealed_pack(Dir, IH, HashAlgo, PackId, Hashes, Reader) of
        ok ->
            commit_seal(Dir, M, PackId, W);
        {error, R} ->
            {error, {seal, R}}
    end.

%% @private
%% A reader closure over the writer's incoming.pack fd + in-memory
%% pending map. The streaming sealed-pack writer calls this once per
%% hash in sorted order; we pread the body without materialising the
%% rest of the pending set.
pending_reader(Fd, Pending) ->
    fun(Hash) -> read_pending_body(Fd, Pending, Hash) end.

%% @private
read_pending_body(Fd, Pending, Hash) ->
    case maps:find(Hash, Pending) of
        {ok, {_Offset, 0}} ->
            %% Zero-length body — `prim_file:pread(_, _, 0)` returns
            %% `eof`, not `{ok, <<>>}`; short-circuit.
            {ok, <<>>};
        {ok, {Offset, Len}} ->
            BodyOff = Offset + bondy_mst_pack_codec:record_header_bytes(),
            case prim_file:pread(Fd, BodyOff, Len) of
                {ok, Body} when byte_size(Body) =:= Len ->
                    {ok, Body};
                Other ->
                    {error, {body_read, Hash, Offset, Other}}
            end;
        error ->
            {error, {missing_pending, Hash}}
    end.

%% @private
%% Streams the sealed pack to disk one record at a time, accumulating
%% the running sha256 in a `crypto:hash_init/1` context. Returns
%% `{ok, Entries}` (the `.idx` entries built inline) on success.
stream_sealed_pack(Dir, IH, HashAlgo, PackId, Hashes, Reader) ->
    TmpPath = bondy_mst_pack_paths:sealed_pack_tmp_path(Dir, PackId),
    Header = bondy_mst_pack_codec:encode_pack_header(#{
        version       => bondy_mst_pack_codec:version(),
        flags         => 0,
        pack_id       => PackId,
        instance_hash => IH,
        hash_algo     => HashAlgo,
        created_at    => erlang:system_time(millisecond),
        record_count  => length(Hashes)
    }),
    case prim_file:open(TmpPath, [write, raw, binary, exclusive]) of
        {ok, Fd} ->
            try
                stream_sealed_pack_body(Fd, Header, Hashes, Reader)
            after
                _ = prim_file:close(Fd)
            end;
        {error, _} = E ->
            E
    end.

%% @private
stream_sealed_pack_body(Fd, Header, Hashes, Reader) ->
    case prim_file:write(Fd, Header) of
        ok ->
            Ctx = crypto:hash_update(crypto:hash_init(sha256), Header),
            stream_records(Fd, Ctx, byte_size(Header), Hashes, Reader, []);
        {error, _} = E ->
            E
    end.

%% @private
stream_records(Fd, Ctx, _Off, [], _Reader, Acc) ->
    Trailer = crypto:hash_final(Ctx),
    case prim_file:write(Fd, Trailer) of
        ok ->
            case bondy_mst_io:datasync(Fd) of
                ok             -> {ok, lists:reverse(Acc)};
                {error, _} = E -> E
            end;
        {error, _} = E ->
            E
    end;
stream_records(Fd, Ctx, Off, [Hash | Rest], Reader, Acc) ->
    case Reader(Hash) of
        {ok, Body} when is_binary(Body) ->
            Record = bondy_mst_pack_codec:encode_record(Hash, Body),
            case prim_file:write(Fd, Record) of
                ok ->
                    Ctx1 = crypto:hash_update(Ctx, Record),
                    RecBytes = bondy_mst_pack_codec:record_header_bytes()
                              + byte_size(Body),
                    stream_records(Fd, Ctx1, Off + RecBytes, Rest, Reader,
                                   [{Hash, Off} | Acc]);
                {error, _} = E ->
                    E
            end;
        {error, _} = E ->
            E
    end.

%% @private
%% Entries are `[{Hash, Offset}]` already in sort-by-hash order — the
%% stream produced them in that order because the caller passes
%% `Hashes` sorted.
write_sealed_idx_from_entries(Dir, PackId, Entries) ->
    case bondy_mst_pack_index:build(Entries) of
        {ok, IO} ->
            write_sealed_idx_bin(Dir, PackId, iolist_to_binary(IO));
        {error, Reason} ->
            {error, {idx_build, Reason}}
    end.

%% @private
write_sealed_idx_bin(Dir, PackId, Bin) ->
    TmpPath = bondy_mst_pack_paths:sealed_idx_tmp_path(Dir, PackId),
    case prim_file:open(TmpPath, [write, raw, binary, exclusive]) of
        {ok, Fd} ->
            try
                case prim_file:write(Fd, Bin) of
                    ok ->
                        bondy_mst_io:datasync(Fd);
                    {error, _} = E ->
                        E
                end
            after
                _ = prim_file:close(Fd)
            end;
        {error, _} = E ->
            E
    end.

%% @private
%% Step 4 of seal: the sealed `pack-NNNN` pair is already renamed into
%% place by `create_sealed_pack/6`; here we atomically swap the manifest
%% (sealed_packs += [PackId], incoming_pack := absent), then close +
%% unlink the now-superseded incoming.pack.
commit_seal(Dir, M, PackId, W) ->
    M1 = bondy_mst_pack_manifest:with_incoming_pack(
        bondy_mst_pack_manifest:add_sealed_pack(M, PackId),
        absent
    ),
    case bondy_mst_pack_manifest:write(Dir, M1) of
        ok ->
            close_and_unlink_incoming(W#?MODULE.incoming_fd, Dir),
            reopen_fresh_incoming(W, M1, PackId);
        {error, R} ->
            {error, {manifest, R}}
    end.

%% @private
rename_sealed_pair(Dir, PackId) ->
    PackTmp = bondy_mst_pack_paths:sealed_pack_tmp_path(Dir, PackId),
    Pack    = bondy_mst_pack_paths:sealed_pack_path(Dir, PackId),
    IdxTmp  = bondy_mst_pack_paths:sealed_idx_tmp_path(Dir, PackId),
    Idx     = bondy_mst_pack_paths:sealed_idx_path(Dir, PackId),
    case bondy_mst_io:rename(PackTmp, Pack) of
        ok ->
            case bondy_mst_io:rename(IdxTmp, Idx) of
                ok ->
                    _ = bondy_mst_io:fsync_dir(Dir),
                    ok;
                {error, R} ->
                    _ = prim_file:delete(Pack),
                    {error, {rename_idx, R}}
            end;
        {error, R} ->
            {error, {rename_pack, R}}
    end.

%% @private
close_and_unlink_incoming(undefined, Dir) ->
    _ = prim_file:delete(bondy_mst_pack_paths:incoming_pack_path(Dir)),
    _ = bondy_mst_io:fsync_dir(Dir),
    ok;
close_and_unlink_incoming(Fd, Dir) ->
    _ = prim_file:close(Fd),
    _ = prim_file:delete(bondy_mst_pack_paths:incoming_pack_path(Dir)),
    _ = bondy_mst_io:fsync_dir(Dir),
    ok.

%% @private
%% After a successful seal the writer resets to a fresh state with no
%% open incoming fd; the next append will lazily create incoming.pack
%% and flip the manifest. This keeps the on-disk state consistent with
%% the manifest at every observable point.
reopen_fresh_incoming(W, M1, PackId) ->
    Fresh = fresh_state(
        W#?MODULE.dir,
        W#?MODULE.instance_id,
        W#?MODULE.hash_algo,
        W#?MODULE.instance_hash,
        M1,
        #{sync_every_records => W#?MODULE.sync_every_records,
          sync_every_ms      => W#?MODULE.sync_every_ms}
    ),
    {ok, PackId, Fresh#?MODULE{next_pack_id = PackId + 1}}.

%% @private
cleanup_tmp(Dir, PackId) ->
    _ = prim_file:delete(bondy_mst_pack_paths:sealed_pack_tmp_path(Dir, PackId)),
    _ = prim_file:delete(bondy_mst_pack_paths:sealed_idx_tmp_path(Dir, PackId)),
    ok.

%% =============================================================================
%% PRIVATE — misc
%% =============================================================================

%% @private
compute_hash(sha256, Page) -> crypto:hash(sha256, Page).
