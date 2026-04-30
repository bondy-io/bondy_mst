%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_snapshot_store).

-include("bondy_mst.hrl").
-include("bondy_oplog.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Behaviour for the per-instance snapshot store
(`_design/10_new_design.md` §8).

A snapshot is the output of one compaction cycle: the consolidated
CRDT state at a particular compaction watermark. Compaction reads the
current snapshot, folds the stable event prefix through
`interpret_cog/2`, writes the new snapshot, and truncates the MST.

The library defines the storage interface; implementations choose
durability and serialisation:

- `bondy_oplog_snapshot_store_ets` — in-memory; default for
  tests and ephemeral instances.
- `bondy_oplog_snapshot_store_file` — file-backed (atomic
  rename); durable single-snapshot persistence.

Consumers that need different durability characteristics (RocksDB,
S3, etc.) implement this behaviour themselves.

## Single-snapshot policy

The library keeps exactly one snapshot per instance: the most-recent
one. Older snapshots are not retained. This matches the architecture's
"snapshot is a checkpoint, not history" framing — the MST plus the
latest snapshot fully reconstruct the live state.

Consumers that want versioned snapshots build them on top of the
behaviour (e.g. by writing each snapshot with a separate id and
keeping a roll-up).
""").

-type state() :: term().

-export_type([state/0]).

-callback init(InstanceId :: instance_id(), Opts :: map()) ->
    {ok, state()} | {error, Reason :: term()}.

-callback put_snapshot(
    State :: state(),
    Watermark :: bondy_oplog_event:event_key(),
    Snapshot :: term()
) -> ok | {error, term()}.

-callback get_snapshot(State :: state()) ->
    {ok, Watermark :: bondy_oplog_event:event_key(), Snapshot :: term()}
    | not_found.

-callback current_watermark(State :: state()) ->
    bondy_oplog_event:event_key() | undefined.

-callback close(State :: state()) -> ok.
