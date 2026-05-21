# Bondy MST — Architecture, the friendly version

This folder is the **gentle introduction** to the `bondy_mst` substrate.
The truth-source design notes live in `_design/latest/`; they are dense
and exhaustive. These pages restate the same architecture in a
conversational, "read it on the train" style, with one idea per
mermaid diagram instead of one diagram per system.

If you have read this far before reading any code, you are in the right
place. Read the chapters in order — each one assumes you have read the
previous one.

## Reading order

| # | Doc | What you'll learn |
|---|---|---|
| 00 | [Overview](00_overview.md) | The three packages — `bondy_db`, `bondy_mst`, `bondy_oplog` — and how a single `write` becomes a `read`. |
| 01 | [bondy_oplog](01_bondy_oplog.md) | The write side: instances, WAL, sync sessions, and how peers exchange events without a leader. |
| 02 | [bondy_mst](02_bondy_mst.md) | The Merkle Search Tree itself: pages, hashes, the pack-store backend, and how anti-entropy gets to "we agree" quickly. |
| 03 | [bondy_db](03_bondy_db.md) | The read side: cache + overlay + projection, the freshness fence, secondary indexes. |
| 04 | [Applier](04_applier.md) | The reconciler loop that ties writes, the MST, and the projection together. |
| 05 | [Fold strategies](05_fold_strategies.md) | The op-based CRDT merge contract — and why one substrate can serve LWW, OR-Set, presence, strict-uniqueness, … |
| 06 | [Compaction & bootstrap](06_compaction_and_bootstrap.md) | Why the oplog is bounded: causal stability, the compaction watermark, physical MST truncation, and how new replicas join via snapshot transfer. |

## Style notes

These are presented as a guided tour, in the spirit of the CMU SEI
**Views and Beyond** method (a documented software architecture is a
set of views, each suited to one audience). The difference is the
register: blog post, not architecture handbook. Where you want
implementation-level rigor, the chapter ends with a pointer back to
`_design/latest/<doc>` and the relevant source modules.

## Conventions

- **Diagrams** are mermaid. Render in any modern markdown viewer.
- **`bondy_mst`** (lowercase) is the package / library name. **MST**
  (uppercase) is the Merkle Search Tree data structure inside it.
- **"Substrate"** means the published library API — what your
  application sees. Bondy is the canonical consumer; this folder
  describes the substrate, not Bondy.
