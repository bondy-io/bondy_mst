# bondy_mst: the Merkle Search Tree

> Audience: anyone who wants to know what a Merkle Search Tree is, why
> it's at the heart of this library, and how peers converge without
> consensus.
> Time to read: ~20 min.

`bondy_mst` is the package name *and* the name of the data structure
at its centre. The name is overloaded on purpose — the MST is the
single idea that makes everything else worth building. This chapter
introduces the structure, the page-store backend, and the anti-entropy
protocol peers use to converge.

## What problem does the MST solve?

Suppose two peers each have ~10⁶ events. They want to know which
events the other one is missing, without shipping all 10⁶ over the
network. With a flat data structure you'd ship a Bloom filter at best;
with a key-value store you'd run a diff per key.

A **Merkle Search Tree** turns this into log-N comparisons:

- The tree is **balanced by hash**, not by insertion order — two
  peers that hold the same set of items build the **same tree**
  deterministically.
- Each internal node holds a hash of its subtree (Merkle).
- To compare two trees, peers exchange root hashes. Equal? Done.
  Different? Recurse only into the children that disagree.

The result: peers find their disagreement in **O(diff)**, not
O(set size).

> The construction is from Auvolat & Taïani, *Merkle Search Trees:
> Efficient State-Based CRDTs in Open Networks*, SRDS 2019. The
> Erlang implementation in `bondy_mst.erl` is ported from the
> authors' reference Elixir prototype and extended with multiple
> backends and GC.

## Anatomy of an MST

```mermaid
flowchart TB
    R["Root<br/>hash=H<sub>0</sub>"]
    L1A["Layer-1 page<br/>keys: a..f"]
    L1B["Layer-1 page<br/>keys: g..k"]
    L1C["Layer-1 page<br/>keys: l..z"]
    L0A["Layer-0 leaf<br/>a, b, c"]
    L0B["Layer-0 leaf<br/>d, e, f"]
    L0C["Layer-0 leaf<br/>g, h, i"]
    L0D["Layer-0 leaf<br/>j, k"]
    L0E["Layer-0 leaf<br/>l..z"]

    R --> L1A
    R --> L1B
    R --> L1C
    L1A --> L0A
    L1A --> L0B
    L1B --> L0C
    L1B --> L0D
    L1C --> L0E
```

A few things to note:

- **Pages are the unit of content.** A page holds keys, hashes of
  child pages, and an optional value list.
- **A key's layer is decided by hashing the key.** This is the
  trick. Two peers inserting the same key end up with the key in the
  same layer — so the trees they build agree by construction.
- **Pages are content-addressed.** A page's address *is* its hash.
  Two equal pages are the same page; there is no concept of "this
  page on node A vs this page on node B".

## The store interface

`bondy_mst_store` is the behaviour every backend implements. Three
backends ship:

| Backend | Persistent? | Use it for |
|---|---|---|
| `bondy_mst_ets_store` | No (RAM) | Tests, ephemeral instances |
| `bondy_mst_map_store` | No (process state) | Tiny embedded use |
| `bondy_mst_pack_store` | **Yes** | Production |

The behaviour is intentionally small:

```mermaid
classDiagram
    class bondy_mst_store {
      <<behaviour>>
      +open(HashAlgorithm, Opts) Store
      +close(Store) ok
      +get_root(Store) Hash
      +set_root(Store, Hash) Store
      +get(Store, Hash) Page
      +has(Store, Hash) bool
      +put(Store, Page) {Hash, Store}
      +delete(Store, Hash) Store
      +copy(Store, Store2, Hash) Store
      +free(Store, Hash, Page) Store
      +gc(Store, KeepRoots) Store
      +list(Store) [Hash]
      +missing_set(Store, Root) [Hash]
      +page_refs(Page) [Hash]
      +destroy(Store) ok
      +transaction(Store, Fun) Result
      +capabilities(Store) Caps
    }
    bondy_mst_store <|-- bondy_mst_ets_store
    bondy_mst_store <|-- bondy_mst_map_store
    bondy_mst_store <|-- bondy_mst_pack_store
```

`list`, `missing_set`, and `gc` all walk the tree from a root via
`page_refs/1`. **GC is mark-from-root**: keep only pages reachable
from the live root set, drop everything else. `free/3` is the
soft-delete used during truncation — the backend records the hash as
tombstoned and the next `gc/2` (pack-rewrite for `pack_store`)
reclaims the disk.

## The pack store on disk

The production backend is a **content-addressed packfile** — git's
object database, adapted for MST pages.

```
{mst_dir}/{instance_id}/
    manifest           # current packs, root pointer (atomic swap)
    tombstones         # logically deleted hashes
    incoming.pack      # mutable, in-progress (index kept in memory, not on disk)
    pack-NNNN.pack     # sealed, immutable (NNNN = zero-padded id, e.g. 0001)
    pack-NNNN.idx      # companion sorted-hash index with fanout + bloom
    ...
```

The pack store has four pieces; each one earns its keep.

### 1. The pack file — sorted by hash, write-once

```mermaid
flowchart LR
    H["Pack header<br/>48 bytes"]
    R0["Record 0<br/>smallest hash"]
    R1[Record 1]
    R2[...]
    RN[Record N]

    H --> R0 --> R1 --> R2 --> RN
```

Each record is:

```
Hash (32 B) | PageLen (4) | PageCrc32 (4) | Page bytes
```

Sorted by hash so binary search is possible. Sealed packs are
**immutable** — that is what makes mmap'd reads and concurrent GC
safe.

### 2. The index — fanout + bloom + sorted hashes

A companion `.idx` per pack:

```mermaid
flowchart TB
    H["Index header · 16 B"]
    BL["Bloom section · variable<br/>partitioned, k slices"]
    FT["Fanout table · 256 × 4 B<br/>fanout[i] = count of hashes ≤ first-byte i"]
    HA["Sorted hash array<br/>RecordCount × 32 B"]
    OA["Offset array<br/>RecordCount × 8 B"]
    TR["sha256 trailer · 32 B"]

    H --> BL --> FT --> HA --> OA --> TR
```

The fanout table is the same trick git uses: take the hash's first
byte, look up `fanout[H[0]-1]` and `fanout[H[0]]`, and you've narrowed
binary search to ~`N/256` candidates. The bloom filter is a recent
addition that closes the multi-pack negative-lookup gap (28–42×
hit-path speedup, 2–4× miss-path; see `bench/benchmarks/mst_pack_get.exs`).

### 3. The manifest — the atomic swap point

The manifest names the current packs and the current MST root. It is
written tmp + rename + fsync — the standard atomic-file trick.

```mermaid
sequenceDiagram
    participant Writer
    participant ManifestTmp as manifest.tmp
    participant Manifest as manifest

    Writer->>ManifestTmp: write new contents
    Writer->>ManifestTmp: fsync
    Writer->>Manifest: rename tmp → manifest
    Writer->>Writer: fsync dir
```

If the process crashes mid-rename, either the old manifest is intact
or the new manifest is intact — never a half-written one.

### 4. Tombstones — for the "I deleted this page" case

The map/ETS backends have an in-memory free set. The pack store
persists it to a `tombstones` file (same atomic-write shape as the
manifest) so reopening doesn't re-expose deleted pages.

## A read inside the pack store

```mermaid
sequenceDiagram
    autonumber
    participant Caller
    participant Reader as pack_reader
    participant Bloom
    participant Idx as idx pread
    participant Pack as pack pread

    Caller->>Reader: get(Hash)
    Reader->>Bloom: probe(Hash)
    alt bloom says no
        Bloom-->>Reader: false
        Reader-->>Caller: undefined
    else bloom says maybe
        Reader->>Idx: fanout[H[0]-1..H[0]]
        Idx-->>Reader: [Lo, Hi)
        Reader->>Idx: binary search hash array
        alt not found
            Idx-->>Reader: not found
            Reader-->>Caller: undefined
        else found
            Idx-->>Reader: Offset
            Reader->>Pack: pread(Offset, 40)
            Pack-->>Reader: record header
            Reader->>Pack: pread(Offset+40, PageLen)
            Pack-->>Reader: page bytes
            Reader-->>Caller: Page
        end
    end
```

For a hot read, the bloom and fanout are in OS page cache; cost is
~2–3 `pread` calls.

## Anti-entropy: the protocol that earns its keep

Now the payoff. Two peers want to converge. They each have an MST
root hash. The session goes like this:

```mermaid
sequenceDiagram
    autonumber
    participant A as Peer A (sync_session)
    participant B as Peer B (responder)

    A->>B: request(get_root)
    B-->>A: PeerRoot = H<sub>B</sub>
    alt H<sub>A</sub> == H<sub>B</sub>
        Note over A,B: in sync — done
    else divergent
        A->>A: missing = bondy_oplog_instance:missing_set(I, H<sub>B</sub>)
        A->>B: request(get_pages, missing)
        B-->>A: Pages
        A->>A: bondy_oplog_instance:integrate_peer_root(H<sub>B</sub>)
        Note over A: integrate_peer_root merges<br/>and re-truncates ≤ watermark
    end
```

The crucial property: **`missing_set` walks the trees in parallel and
prunes whole subtrees whose hashes match**. If A and B disagree on a
small region of the keyspace, only the pages in that region get
shipped. The transport carries opaque request/response frames; the
verb names above are illustrative (`bondy_oplog_transport:request/4`
in code).

The session is implemented in `bondy_oplog_sync_session.erl` and the
peer side is `bondy_oplog_responder.erl`. The transport is pluggable.

## Where do MST pages come from?

The MST is **derived from events**. The applier ([chapter 04](04_applier.md)) is the
process that:

1. Takes events from the WAL drain (or from peer integration).
2. Calls the bondy_mst library to compute the page-level deltas.
3. Calls `bondy_mst_store:put(Page)` for each new page.
4. Calls `bondy_mst_store:set_root(NewRoot)` at the end.

```mermaid
flowchart LR
    EV[Event] --> ENC[encode key/value]
    ENC --> INS["mst:insert(K, V)"]
    INS --> PAGES[new + modified pages]
    PAGES --> STORE[(pack_store)]
    PAGES --> ROOT[new root hash]
    ROOT --> SR[set_root]
```

Compaction snapshots the **stable prefix** of events into a single
file, then truncates the MST. The frontier between "snapshotted" and
"live" is the **compaction watermark**, advanced by
`bondy_oplog_compaction` (not by the applier) — see [chapter 06](06_compaction_and_bootstrap.md).

## Truncation is physical page deletion

`bondy_mst:delete/2` is not a tombstone. The implementation in
`bondy_mst.erl` (`delete_at/5` → `delete_below_level/5` →
`delete_from_level/4` → `merge_subtrees/4`) locates the page that
holds the key, calls `bondy_mst_store:free/3` on it (tombstone the
hash + drop in-memory entry), rewrites the parent without the entry,
and merges the orphaned sibling subtree back into the previous
entry's subtree:

```mermaid
flowchart LR
    DEL["delete(Key)"]
    LOC["locate page · delete_at/5"]
    FREE["bondy_mst_store:free/3<br/>tombstone hash"]
    REW["rewrite parent page<br/>without entry"]
    MRG["merge_subtrees/4"]
    NEW[new root hash]

    DEL --> LOC --> FREE
    LOC --> REW --> MRG --> NEW
```

The freed pages enter the store's tombstone set; the **pack-rewrite
GC** (next section) reclaims their disk space.

This matters because:

- Truncated keys don't appear in `missing_set/2` results, so AE
  never offers them back to a peer that has already truncated them.
- Compaction is deterministic — every replica that truncates the
  same prefix arrives at bit-identical pages and the same root hash.
- The disk footprint of a fully-converged instance is bounded by
  snapshot size, not by write history.

## Garbage collection

```mermaid
flowchart LR
    R[live root] --> WALK[walk via page_refs]
    WALK --> REACH[reachable set]
    REACH --> KEEP[keep in new pack]
    OLD[all packs] --> KEEP
    OLD --> DROP[drop unreachable]
```

The pack-rewrite GC reads every reachable page, writes a new pack,
swaps the manifest, and unlinks the old packs. It is the same code
path as compaction — "rewrite packs omitting hashes unreachable from
current MST root".

## Things to keep in mind

- **The MST is deterministic.** Two peers with the same set of events
  build bit-identical trees.
- **Pages are content-addressed and immutable.** This is what makes
  AE, GC, and replication safe.
- **The pack store is git's object database, scoped down.** If you
  know git internals, you already know most of `bondy_mst_pack_*`.
- **AE finds disagreement in O(diff), not O(N).** This is the
  unique property — without it, the whole stack doesn't work at
  cluster scale.

## Pointers

- Paper: Auvolat & Taïani, *Merkle Search Trees*, SRDS 2019,
  Inria HAL-02303490.
- Core: `bondy_mst.erl` (put/put_batch with bulk canonical
  construction, merge, truncate/2, read-only diff_to_list),
  `bondy_mst_store.erl` (the behaviour), `bondy_mst_page.erl`.
- Backends: `bondy_mst_ets_store.erl`, `bondy_mst_map_store.erl`,
  `bondy_mst_pack_store.erl`.
- Pack-store internals (each module's doc carries the on-disk
  format detail): `bondy_mst_pack_writer.erl` (live pack + seal
  trigger), `bondy_mst_pack_seal.erl` (the seal pipeline),
  `bondy_mst_pack_reader.erl` + `bondy_mst_pack_sealed_view.erl`
  (sealed reads), `bondy_mst_pack_index.erl` +
  `bondy_mst_pack_bloom.erl` (lookups), `bondy_mst_pack_codec.erl`
  (record format), `bondy_mst_pack_manifest.erl`,
  `bondy_mst_pack_tombstones.erl`, `bondy_mst_pack_recovery.erl`
  (crash recovery), `bondy_mst_pack_idx_rebuild.erl` (self-healing
  `.idx` rebuild).
- Consumers: `bondy_oplog_sync_session.erl`,
  `bondy_oplog_responder.erl`.
