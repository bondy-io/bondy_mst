%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_wal_io).

-include("bondy_mst.hrl").
-include_lib("kernel/include/logger.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Low-level I/O primitives shared by the `bondy_oplog_wal` modules.

This is a private helper. It exists so platform-specific tightening
(e.g. macOS `F_FULLFSYNC` via a NIF, Linux `O_TMPFILE`-based tmp writes)
can be done in one place rather than duplicated across the segment and
manifest implementations.
""").

-export([fsync_dir/1]).
-export([datasync/1]).
-export([rename/2]).

%% =============================================================================
%% API
%% =============================================================================

?DOC("""
Fsyncs the enclosing directory so a freshly-renamed or freshly-created
dirent is durable.

This is **required** on ext4 / xfs and most POSIX filesystems: without
it, an atomic `rename/2` can be lost across a power failure even after
the file content has been fsynced. The cost is one extra fsync per
manifest rewrite or segment creation, negligible compared to rotation /
sweep cadence.

Platform behaviour:
- Linux (ext4, xfs, btrfs): standard `open(dir, O_RDONLY)` + `fsync` works.
- macOS (APFS/HFS+): does not have the same metadata-vs-data ordering
  issue as ext4; the per-file datasync is the strongest practical
  guarantee. Opening the directory may return `eisdir`, which we treat
  as "directory fsync not supported on this platform" and skip silently.
- Other platforms: any other error is treated as "skip silently" but
  logged at WARNING level once per VM lifetime so an unexpected
  platform misbehaviour surfaces in operational telemetry.
""").
-spec fsync_dir(file:filename_all()) -> ok | {error, term()}.

fsync_dir(Dir) ->
    case prim_file:open(Dir, [read, raw, binary]) of
        {ok, DirFd} ->
            Res =
                case prim_file:datasync(DirFd) of
                    ok -> ok;
                    {error, _} = E1 -> E1
                end,
            ok = prim_file:close(DirFd),
            Res;
        {error, eisdir} ->
            %% Expected on platforms (notably macOS) that refuse to
            %% expose a directory through the file API.
            ok;
        {error, enotsup} ->
            ok;
        {error, Reason} = E ->
            warn_once(fsync_dir_unrecognised_error, Dir, Reason),
            E
    end.

?DOC("""
WAL datasync seam. Every disk-durability point in the WAL — the head
segment, sealed segments, the sparse index, the manifest, the consumer
offset, the snapshot watermark, and the recovery truncate — funnels
through here so:

1. Platform-specific tightening (macOS `F_FULLFSYNC` via a NIF, Linux
   `io_uring`-based sync, etc.) lands in one place.
2. The test suite has a single named callsite to `meck:expect/3` for
   fault injection (P14 — `prop_failed_fsync/0`). Mocking `prim_file`
   itself is unsafe because `file:write_file/2`, `file:open/2`, and the
   emulator's own I/O flow through it.

The wrapper is intentionally thin — production behaviour is
byte-identical to `prim_file:datasync/1`. Test code that fault-injects
this function must hold the `?MODULE` global lock (see
`with_io_fault_lock/1` in the test suite) so concurrent test modules
don't see another suite's mock.
""").
-spec datasync(file:fd()) -> ok | {error, term()}.

datasync(Fd) ->
    prim_file:datasync(Fd).

?DOC("""
WAL rename seam — same rationale as `datasync/1`. Every atomic
rename-into-place in the WAL (manifest, sparse index, consumer offset,
snapshot watermark) funnels through here.
""").
-spec rename(file:filename_all(), file:filename_all()) ->
    ok | {error, term()}.

rename(From, To) ->
    prim_file:rename(From, To).

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
%% Logs a WARNING once per VM lifetime per `Tag`. Used to surface
%% unexpected platform behaviour without flooding the logs.
warn_once(Tag, Dir, Reason) ->
    Key = {?MODULE, warn_once, Tag},
    case persistent_term:get(Key, undefined) of
        undefined ->
            ok = persistent_term:put(Key, true),
            ?LOG_WARNING(#{
                description =>
                    "bondy_oplog_wal_io:fsync_dir/1 returned an unrecognised "
                    "error; durability of dirent operations may be weaker "
                    "than designed on this platform",
                tag => Tag,
                dir => Dir,
                reason => Reason
            });
        _ ->
            ok
    end.
