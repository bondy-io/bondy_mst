%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_projection_adapter).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Behaviour for **projection backings** — the persistent KV layer that
materialises folded cell values (`MST_DB_DESIGN.md` §6).

A projection adapter owns the persistent state of one
`(namespace, index, shard)` triple's keyspace. Each cell is stored as a
binary frame:

```
<<HlcLen:16, Hlc:HlcLen/binary, FoldedValueBytes/binary>>
```

The substrate does not name a specific persistent store. Implementations
can wrap Leveled, RocksDB, mnesia disc_copies, dets, or any other KV with
sorted-key range support. Adapters are responsible for any namespacing
(bucket-per-triple, prefix encoding, etc.); the substrate calls them with
the conceptual `(NS, Index, Shard, Key)` and lets the adapter decide how
to translate.

## Required callbacks

- `open/4` — open the keyspace for an `(NS, Index, Shard)` triple. Called
  once at instance startup; returns a handle threaded through all
  subsequent calls.
- `close/1` — release the handle. Called on instance shutdown.
- `get/2` — single-key read; returns the on-disk frame (encoded by
  `bondy_oplog_cell_frame:encode/2`).
- `put_batch/2` — batched write. Called by the applier; the substrate
  itself never single-writes the projection.
- `range/4` — single-shot range scan; returns up to `limit` rows.
  `bondy_mst_db` wraps it with overlay merging. Consumers that need
  more than one page must call again with a higher `limit` or scatter
  via `shard => N`. Multi-batch streaming was considered and rejected
  for the substrate: overlay merging interacts poorly with stateful
  pagination, and the consumer base today does not need it. Adapters
  MAY truncate at their own internal limit, but SHOULD return all rows
  in `[Low, High)` up to the caller's `limit`.
- `delete/2` — single-key delete. Used for GC and compaction.
- `info/1` — implementation-specific introspection (size, file paths,
  cache stats, etc.).

Adapters MUST be safe under concurrent readers; `put_batch/2` may be
single-writer (the substrate guarantees one applier per shard).

## Lifecycle and owner-death

`close/1` is called on instance shutdown and on explicit shard
unregister. It is **not** called by the substrate when the registering
process dies — see `bondy_mst_db_registry`'s "Owner DOWN cleanup"
section. Adapters that own external resources (file handles, durable
KV connections, sub-processes) MUST monitor their owning process
internally and release resources on owner death. The substrate does
not do it for them.

See `bondy_oplog_cache_adapter` for the orthogonal read-cache surface.
""").

-export_type([
    handle/0,
    range_opts/0
]).

-type handle()       :: any().
-type range_opts()   :: #{
    limit => pos_integer(),
    direction => asc | desc,
    atom() => term()
}.

%% =============================================================================
%% BEHAVIOUR CALLBACKS
%% =============================================================================

-callback open(
    Namespace :: atom(),
    Index :: atom(),
    Shard :: non_neg_integer(),
    Opts :: map()
) -> {ok, handle()} | {error, term()}.

-callback close(handle()) -> ok.

-callback get(handle(), Key :: term()) ->
    {ok, Frame :: binary()} | not_found.

-callback put_batch(
    handle(),
    [{Key :: term(), Frame :: binary()}]
) -> ok | {error, term()}.

-callback range(
    handle(),
    Low :: term(),
    High :: term(),
    Opts :: range_opts()
) -> {ok, [{Key :: term(), Frame :: binary()}]}
   | {error, term()}.

-callback delete(handle(), Key :: term()) -> ok.

-callback info(handle()) -> #{atom() => term()}.
