%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_mst_pack_writer).

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

## Crash safety (Phase 3 scope)

The seal flow follows the §5 ordering: every new file is durable
before the manifest swap, so a crash mid-seal cannot leave a
manifest pointing at a pack that isn't on disk. The reverse
(orphan `.pack`/`.idx` files on disk that aren't in the manifest)
is the recovery scanner's job and is *not* implemented here.

Single-record durability during `append` uses `datasync` per
record — conservative but correct. A subsequent phase can batch
syncs behind a configurable threshold.

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
    next_pack_id     :: pos_integer()
}).

-type t() :: #?MODULE{}.

-type open_opts() :: #{
    instance_id := binary(),
    hash_algo   => atom()
}.

-type open_error() ::
    {missing_field, atom()}
    | {manifest, term()}
    | {incoming, term()}
    | {pending_scan, term()}
    | needs_recovery
    | {instance_id_mismatch, binary(), binary()}
    | {hash_algo_mismatch, atom(), atom()}.

-type append_error() :: {write, term()}.

-type seal_error() ::
    {seal, term()}
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
-export([seal/1]).

%% Inspection
-export([dir/1]).
-export([manifest/1]).
-export([instance_id/1]).
-export([pending_count/1]).
-export([pending_hashes/1]).
-export([pending_lookup/2]).
-export([incoming_offset/1]).
-export([next_pack_id/1]).

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
            do_open(Dir, InstanceId, HashAlgo);
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
close(#?MODULE{incoming_fd = Fd}) ->
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

CRC + record bytes are written and datasync'd before the call
returns. A failure to write surfaces as `{error, {write, Reason}}`;
the state is rolled back (the in-memory pending map is not
updated). The on-disk byte cursor may have moved if the failure
occurred partway through; recovery on reopen will detect and
truncate any partial trailing record.
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

%% =============================================================================
%% API — inspection
%% =============================================================================

-spec dir(t()) -> file:filename_all().
dir(#?MODULE{dir = D}) -> D.

-spec manifest(t()) -> bondy_mst_pack_manifest:t().
manifest(#?MODULE{manifest = M}) -> M.

-spec instance_id(t()) -> binary().
instance_id(#?MODULE{instance_id = Id}) -> Id.

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

-spec incoming_offset(t()) -> non_neg_integer().
incoming_offset(#?MODULE{incoming_offset = Off}) -> Off.

-spec next_pack_id(t()) -> pos_integer().
next_pack_id(#?MODULE{next_pack_id = N}) -> N.

%% =============================================================================
%% PRIVATE — open
%% =============================================================================

%% @private
do_open(Dir, InstanceId, HashAlgo) ->
    case ensure_dir(Dir) of
        ok ->
            case load_or_create_manifest(Dir, InstanceId, HashAlgo) of
                {ok, Manifest} ->
                    case validate_manifest(Manifest, InstanceId, HashAlgo) of
                        ok ->
                            open_incoming(Dir, InstanceId, HashAlgo, Manifest);
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
open_incoming(Dir, InstanceId, HashAlgo, Manifest) ->
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
                             Manifest)};
        {present, true} ->
            resume_incoming(Dir, Path, InstanceId, HashAlgo,
                            InstanceHash, Manifest);
        {absent, true} ->
            %% Orphan from a previous crash; recovery's job.
            {error, needs_recovery};
        {present, false} ->
            {error, needs_recovery}
    end.

%% @private
fresh_state(Dir, InstanceId, HashAlgo, InstanceHash, Manifest) ->
    #?MODULE{
        dir = Dir,
        instance_id = InstanceId,
        hash_algo = HashAlgo,
        instance_hash = InstanceHash,
        manifest = Manifest,
        incoming_fd = undefined,
        incoming_offset = 0,
        pending = #{},
        next_pack_id = next_pack_id_from(Manifest)
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
                    case bondy_oplog_wal_io:datasync(Fd) of
                        ok ->
                            _ = bondy_oplog_wal_io:fsync_dir(W#?MODULE.dir),
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
            {ok, W#?MODULE{
                manifest = M,
                incoming_fd = Fd,
                incoming_offset = HeaderSize
            }};
        {error, R} ->
            _ = prim_file:close(Fd),
            _ = prim_file:delete(bondy_mst_pack_paths:incoming_pack_path(
                W#?MODULE.dir
            )),
            {error, {manifest, R}}
    end.

%% @private
resume_incoming(Dir, Path, InstanceId, HashAlgo, InstanceHash, Manifest) ->
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
                        next_pack_id = next_pack_id_from(Manifest)
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

%% =============================================================================
%% PRIVATE — append
%% =============================================================================

%% @private
do_append(#?MODULE{incoming_fd = Fd, incoming_offset = Off} = W, Hash, Page) ->
    Record = bondy_mst_pack_codec:encode_record(Hash, Page),
    case prim_file:write(Fd, Record) of
        ok ->
            case bondy_oplog_wal_io:datasync(Fd) of
                ok ->
                    HdrBytes = bondy_mst_pack_codec:record_header_bytes(),
                    NewOff = Off + HdrBytes + byte_size(Page),
                    Pending = (W#?MODULE.pending)#{
                        Hash => {Off, byte_size(Page)}
                    },
                    W1 = W#?MODULE{
                        incoming_offset = NewOff,
                        pending = Pending
                    },
                    {ok, Hash, W1};
                {error, R} ->
                    {error, {write, R}}
            end;
        {error, R} ->
            {error, {write, R}}
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
    case read_pending_bodies(Fd, Pending) of
        {ok, Records} ->
            case write_sealed_pack(Dir, IH, HashAlgo, PackId, Records) of
                ok ->
                    case write_sealed_idx(Dir, PackId, Records) of
                        ok ->
                            commit_seal(Dir, M, PackId, W);
                        {error, R} ->
                            cleanup_tmp(Dir, PackId),
                            {error, {seal, R}}
                    end;
                {error, R} ->
                    cleanup_tmp(Dir, PackId),
                    {error, {seal, R}}
            end;
        {error, R} ->
            {error, {seal, R}}
    end.

%% @private
%% Returns a list of {Hash, Page} pairs in sort-by-hash order. We read
%% them all into memory; for production we'd stream, but the typical
%% incoming pack is small (the gen_server seals periodically).
read_pending_bodies(Fd, Pending) ->
    Sorted = lists:keysort(1, maps:to_list(Pending)),
    read_pending_bodies_loop(Fd, Sorted, []).

read_pending_bodies_loop(_Fd, [], Acc) ->
    {ok, lists:reverse(Acc)};
read_pending_bodies_loop(Fd, [{Hash, {_Offset, 0}} | Rest], Acc) ->
    %% Zero-length body — nothing to pread. `prim_file:pread(_, _, 0)`
    %% returns `eof` rather than `{ok, <<>>}` on Linux/macOS, so this
    %% case must be short-circuited.
    read_pending_bodies_loop(Fd, Rest, [{Hash, <<>>} | Acc]);
read_pending_bodies_loop(Fd, [{Hash, {Offset, Len}} | Rest], Acc) ->
    BodyOff = Offset + bondy_mst_pack_codec:record_header_bytes(),
    case prim_file:pread(Fd, BodyOff, Len) of
        {ok, Body} when byte_size(Body) =:= Len ->
            read_pending_bodies_loop(Fd, Rest, [{Hash, Body} | Acc]);
        Other ->
            {error, {body_read, Hash, Offset, Other}}
    end.

%% @private
write_sealed_pack(Dir, IH, HashAlgo, PackId, Records) ->
    TmpPath = bondy_mst_pack_paths:sealed_pack_tmp_path(Dir, PackId),
    Header = bondy_mst_pack_codec:encode_pack_header(#{
        version       => bondy_mst_pack_codec:version(),
        flags         => 0,
        pack_id       => PackId,
        instance_hash => IH,
        hash_algo     => HashAlgo,
        created_at    => erlang:system_time(millisecond),
        record_count  => length(Records)
    }),
    Body = [
        Header
        | [bondy_mst_pack_codec:encode_record(H, P) || {H, P} <- Records]
    ],
    Trailer = bondy_mst_pack_codec:compute_trailer(Body),
    Bytes = [Body, Trailer],
    case prim_file:open(TmpPath, [write, raw, binary, exclusive]) of
        {ok, Fd} ->
            try
                case prim_file:write(Fd, Bytes) of
                    ok ->
                        bondy_oplog_wal_io:datasync(Fd);
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
write_sealed_idx(Dir, PackId, Records) ->
    TmpPath = bondy_mst_pack_paths:sealed_idx_tmp_path(Dir, PackId),
    %% Offsets are relative to the sealed pack we just laid out: header,
    %% then each record at running offsets. Records are already in
    %% sort-by-hash order, so the .idx's sort matches the pack's sort.
    HdrBytes = bondy_mst_pack_codec:header_bytes(),
    RecHdrBytes = bondy_mst_pack_codec:record_header_bytes(),
    {Entries, _} = lists:foldl(
        fun({H, P}, {Acc, Off}) ->
            {[{H, Off} | Acc], Off + RecHdrBytes + byte_size(P)}
        end,
        {[], HdrBytes},
        Records
    ),
    Bin = iolist_to_binary(
        bondy_mst_pack_index:build(lists:reverse(Entries))
    ),
    case prim_file:open(TmpPath, [write, raw, binary, exclusive]) of
        {ok, Fd} ->
            try
                case prim_file:write(Fd, Bin) of
                    ok ->
                        bondy_oplog_wal_io:datasync(Fd);
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
%% Step 3 of seal: rename .tmp files into place, fsync dir, then atomically
%% swap the manifest. Cleans up the incoming.pack last.
commit_seal(Dir, M, PackId, W) ->
    PackTmp = bondy_mst_pack_paths:sealed_pack_tmp_path(Dir, PackId),
    Pack    = bondy_mst_pack_paths:sealed_pack_path(Dir, PackId),
    IdxTmp  = bondy_mst_pack_paths:sealed_idx_tmp_path(Dir, PackId),
    Idx     = bondy_mst_pack_paths:sealed_idx_path(Dir, PackId),
    case bondy_oplog_wal_io:rename(PackTmp, Pack) of
        ok ->
            case bondy_oplog_wal_io:rename(IdxTmp, Idx) of
                ok ->
                    _ = bondy_oplog_wal_io:fsync_dir(Dir),
                    M1 = bondy_mst_pack_manifest:with_incoming_pack(
                        bondy_mst_pack_manifest:add_sealed_pack(M, PackId),
                        absent
                    ),
                    case bondy_mst_pack_manifest:write(Dir, M1) of
                        ok ->
                            close_and_unlink_incoming(W#?MODULE.incoming_fd,
                                                      Dir),
                            reopen_fresh_incoming(W, M1, PackId);
                        {error, R} ->
                            {error, {manifest, R}}
                    end;
                {error, R} ->
                    _ = prim_file:delete(Pack),
                    {error, {seal, {rename_idx, R}}}
            end;
        {error, R} ->
            {error, {seal, {rename_pack, R}}}
    end.

%% @private
close_and_unlink_incoming(undefined, Dir) ->
    _ = prim_file:delete(bondy_mst_pack_paths:incoming_pack_path(Dir)),
    _ = bondy_oplog_wal_io:fsync_dir(Dir),
    ok;
close_and_unlink_incoming(Fd, Dir) ->
    _ = prim_file:close(Fd),
    _ = prim_file:delete(bondy_mst_pack_paths:incoming_pack_path(Dir)),
    _ = bondy_oplog_wal_io:fsync_dir(Dir),
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
        M1
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
