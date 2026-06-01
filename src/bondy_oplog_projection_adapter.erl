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

## Bucket is a first-class call-time parameter

Every data callback takes a `Bucket :: term()` as its primary argument
alongside `Key`. The adapter maps `(Bucket, Key)` onto its native
keyspace exactly as leveled / Riak do — Bucket is the storage-layer
partition, not a sub-shard inside the handle.

This keeps a single `handle()` reusable across every Bucket inside the
shard. New Buckets do not require new handles, new registry entries, or
adapter `open/4` calls; they just appear on the wire when the caller
passes a new Bucket value to `get/3`, `put_batch/2`, `range/5`, or
`delete/3`.

## Required callbacks

- `open/4` — open the keyspace for an `(NS, Index, Shard)` triple.
  Returns a Bucket-agnostic handle threaded through all subsequent
  calls.
- `close/1` — release the handle.
- `get/3` — single-key read; returns the on-disk frame.
- `put_batch/2` — batched write. Each entry carries its own `Bucket`
  so a single batch can mix buckets when the caller wants to.
- `range/5` — single-shot range scan within one `Bucket`. Cross-bucket
  scans are the caller's responsibility (scatter and merge).
- `delete/3` — single-key delete inside a Bucket.
- `info/1` — implementation-specific introspection.

## Optional callbacks

- `head/3` — fast-path read that returns the substrate's HEAD wire
  format (`<<HlcLen:16, Hlc/binary, ValueBytes/binary>>`) without
  decoding the full V2 frame. Adapters that have a native HEAD
  mechanism (e.g. leveled's tag extractor + `book_head/4`) implement
  it; adapters that don't can omit the export and the substrate falls
  back to `get/3 + bondy_oplog_cell_frame:extract_head/1`.

Adapters MUST be safe under concurrent readers; `put_batch/2` may be
single-writer (the substrate guarantees one applier per shard).

## Lifecycle and owner-death

`close/1` is called on instance shutdown and on explicit shard
unregister. It is **not** called by the substrate when the registering
process dies — see `bondy_db_core_registry`'s "Owner DOWN cleanup"
section. Adapters that own external resources (file handles, durable
KV connections, sub-processes) MUST monitor their owning process
internally and release resources on owner death. The substrate does
not do it for them.

See `bondy_oplog_cache_adapter` for the orthogonal read-cache surface.
""").

-export_type([
    handle/0,
    bucket/0,
    range_opts/0
]).

-type handle() :: any().
-type bucket() :: term().
-type range_opts() :: #{
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

-callback get(handle(), bucket(), Key :: term()) ->
    {ok, Frame :: binary()} | not_found.

-callback put_batch(
    handle(),
    [{bucket(), Key :: term(), Frame :: binary()}]
) -> ok | {error, term()}.

-callback range(
    handle(),
    bucket(),
    Low :: term(),
    High :: term(),
    Opts :: range_opts()
) ->
    {ok, [{Key :: term(), Frame :: binary()}]}
    | {error, term()}.

-callback delete(handle(), bucket(), Key :: term()) -> ok.

-callback info(handle()) -> #{atom() => term()}.

-callback head(handle(), bucket(), Key :: term()) ->
    {ok, HeadBytes :: binary()} | not_found.

-optional_callbacks([head/3]).
