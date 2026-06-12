# Splitting the repo: `bondy_mst` (stays) vs `bondy_db` / `bondy_oplog` (moves)

> **Status: PROPOSED (2026-06-12).** Dependency analysis + extraction plan.
> Grounded in a full cross-module reference scan of `src/` at the time of
> writing (130 modules, 4 headers). No code has moved yet.

> **Goal.** Separate the consumer/replication layer
> (`bondy_db` + `bondy_oplog` + the op-based CRDT catalogue) from the
> Merkle-Search-Tree replication structure (`bondy_mst`). `bondy_mst` stays
> in this repository as a pure library; the rest moves to a new repository
> that depends on it.
>
> **Non-goals.** No behaviour change, no API redesign, no renaming of public
> functions. This is a packaging/layering change only.

---

## 1. Headline finding — the split is already clean

The codebase is **already layered** the way the split requires. The entire
`bondy_mst_*` group (27 modules) contains exactly **two** code references that
point "up" into `bondy_db` / `bondy_oplog`, and **both live in the OTP boot
scaffolding** — not in a single tree, store, codec, or algorithm module:

| MST module | Upward edge | Nature |
|---|---|---|
| `src/bondy_mst_app.erl:34` | `bondy_oplog_leveled_tag:install()` | application `start/2` |
| `src/bondy_mst_sup.erl:46` | child spec for `bondy_oplog_sup` | supervision tree |

That is precisely the part you rewrite when you split one OTP application into
two. **There is no tangled cross-coupling in the data-structure layer.**

Everything else a naïve grep flags is **comment / moduledoc only** (verified by
a comment-and-docstring-stripping scan):

- `bondy_mst_admin.erl` — the cold backup/restore tool — names
  `bondy_oplog:start/stop_instance` *only* inside its `?MODULEDOC` example. Its
  548 lines of code touch no oplog.
- `bondy_mst_ets_store.erl`, `bondy_mst_pack_manifest.erl`, `bondy_mst.erl` —
  oplog appears only in explanatory comments / docstrings.

### Evidence (reproducible)

```sh
# Exhaustive code-only back-edge scan (strips % comments and """ docstrings):
#   bondy_mst_app.erl  -> bondy_oplog_leveled_tag
#   bondy_mst_sup.erl  -> bondy_oplog_sup
# (nothing else)
```

---

## 2. Target topology

Two OTP applications, dependency arrow pointing **down only**:

```
┌──────────────────────────────────────────────────────────┐
│  NEW REPO  —  bondy_oplog (umbrella or single app)        │
│                                                            │
│   bondy_db*            (13)  consumer facade + topologies  │
│   bondy_oplog*         (69)  write/replication framework   │
│   bondy_oplog_crdt_*   (19)  pure op-based CRDT catalogue  │
│   bondy_dvvset, bondy_metrics  (2)  support                │
│   include/bondy_oplog.hrl, bondy_oplog_wal.hrl             │
└───────────────────────────┬──────────────────────────────┘
                            │  depends on (library dep)
                            ▼
┌──────────────────────────────────────────────────────────┐
│  THIS REPO  —  bondy_mst (pure library, 27 modules)       │
│                                                            │
│   bondy_mst, bondy_mst_store/_page/_io                     │
│   bondy_mst_crdt        (state-based MST merge engine)     │
│   bondy_mst_pack_*      (18  durable pack store)           │
│   bondy_mst_ets_store, bondy_mst_map_store                 │
│   bondy_mst_admin       (cold backup/restore)             │
│   bondy_mst_config/_utils/_coalescing_queue               │
│   include/bondy_mst.hrl, bondy_mst_pack.hrl               │
└──────────────────────────────────────────────────────────┘
```

`bondy_mst` has **no long-lived processes of its own** — `bondy_mst_app` and
`bondy_mst_sup` exist today *only* to boot the oplog layer
(`bondy_mst_app:start/2` installs the oplog leveled tag then starts the sup;
`bondy_mst_sup:init/1` runs `bondy_mst_config:init()` then starts
`bondy_oplog_sup` as its sole child). Pack-store background workers are started
per-store, not under a global supervisor. So after the split `bondy_mst` becomes
a **library application** (or keeps a trivial app that only runs
`bondy_mst_config:init/0`).

---

## 3. The public API surface `bondy_mst` must freeze

The new repo consumes the MST library through a small, well-defined surface.
These are the contracts the extracted library must keep stable (call counts
from db/oplog → mst):

| MST module | refs | role exposed |
|---|---|---|
| `bondy_mst` | 233 | the tree: `new/put/get/merge/diff/truncate/compact/…` |
| `bondy_mst_io` | 26 | page (de)serialisation |
| `bondy_mst_page` | 4 | page accessors |
| `bondy_mst_store` | 3 | store behaviour |
| `bondy_mst_pack_store` | 3 | durable store impl |
| `bondy_mst_pack_manifest` | 3 | manifest read/write |
| `bondy_mst_crdt` | 2 | state-based merge engine (`bondy_oplog_instance` only) |
| `bondy_mst_map_store` | 1 | in-memory store impl |
| `bondy_mst_ets_store` | 1 | ETS store impl |

`?ROOT_KEY` and `?T` (in `bondy_mst.hrl`) have **zero** db/oplog users — they are
genuinely MST-internal and need not be exported.

---

## 4. Shared concerns to divide (none are blockers)

### 4.1 Headers — the only meaningful mechanical churn

`include/bondy_mst.hrl` is `-include`d by **~88 db/oplog modules**, but only for:

- `?MODULEDOC` / `?DOC` doc-helper macros — **100 users** (the real reason).
- `?BONDY_FOLD_TAG` — **2 users** (a leveled fold-tag concept, not MST).

→ **Action:** the new repo ships its own ~10-line doc-macros header; **move the
`?BONDY_FOLD_TAG` definition into `include/bondy_oplog.hrl`**. This avoids a
compile-time include dependency on the library's private header. The ~88
`-include` line edits are the bulk of the diff but are purely cosmetic.

| header | home |
|---|---|
| `include/bondy_mst.hrl` | this repo (strip `?BONDY_FOLD_TAG`) |
| `include/bondy_mst_pack.hrl` | this repo |
| `include/bondy_oplog.hrl` | new repo (gains `?BONDY_FOLD_TAG` + doc macros) |
| `include/bondy_oplog_wal.hrl` | new repo |

### 4.2 The two "support" modules are not actually shared

- `bondy_dvvset` — used only by 11 CRDT/applier modules. **0 MST consumers.**
- `bondy_metrics` — used only by `bondy_db_core_metrics`, `bondy_oplog_latency`,
  `bondy_oplog_sup`. **0 MST consumers.**

Both move to the new repo. (`bondy_metrics` is generic enough to later become its
own tiny library if another consumer appears, but nothing requires that now.)

### 4.3 External dependencies, split by actual usage

| dep | `bondy_mst` keeps | new repo gets |
|---|---|---|
| `bloomfi` | ✅ (pack bloom) | — |
| `key_value` | ✅ | — |
| `utils` | ✅ (`apply_lazy`, `hash`, `implements_behaviour`, …) | — |
| `app_config` | ✅ | ✅ |
| `memory` | ✅ | ✅ |
| `telemetry` | ✅ | ✅ |
| `leveled` | ❌ **drop** (only used via the oplog tag install, which leaves) | ✅ |
| `bondy_mst` | n/a | ✅ (new library dep) |

> Trimming `leveled` from `bondy_mst.app.src`/`rebar.config` is the one
> dependency-list change on the staying side.

---

## 5. PR sequence

Done in two movements: **decouple in place** (lands in *this* repo, single-app
build stays green throughout — each PR independently verifiable), then
**extract** (creates the new repo). Per project convention every PR ends with an
Architecture QA review against this plan.

### Decouple in place (this repo)

**PR-1 — Break the header coupling.**
Create a dedicated doc-macros header (e.g. `include/bondy_doc.hrl`) holding
`?MODULEDOC`/`?DOC`; repoint all `-include` sites. Move the `?BONDY_FOLD_TAG`
definition out of `bondy_mst.hrl` into `bondy_oplog.hrl` and repoint its 2 users
+ the `bondy_mst_app` comment. No behaviour change.
*Gate:* full suite green; `rebar3 as test compile` clean.

**PR-2 — Invert the boot wiring.**
Introduce `bondy_oplog_app` + a top-level `bondy_oplog_sup` that does what the
two back-edge lines do today (install the leveled tag, then start the existing
`bondy_oplog_sup` children). Reduce `bondy_mst_app`/`bondy_mst_sup` to a library
app (or a trivial app that only calls `bondy_mst_config:init/0`). After this PR
the MST group has **zero** upward edges.
*Gate:* the back-edge scan returns empty —
`grep -lE 'bondy_(db|oplog)' src/bondy_mst*.erl` shows only comment/docstring
hits (none in code); full suite green; release boots (oplog app starts MST as a
dependency).

**PR-3 — Freeze the `bondy_mst` public API.**
Audit `-export`s of the §3 surface; confirm everything db/oplog calls is public
and intended; add `-moduledoc`/`-doc` where the contract was previously implicit.
No new behaviour; this is the "stable seam" PR that makes the later physical
move a no-op for consumers.
*Gate:* dialyzer/xref clean across the surface; full suite green.

### Extract (new repo)

**PR-4 — Carve out the new repo.**
Move the 103 modules + `bondy_oplog.hrl` + `bondy_oplog_wal.hrl` + their tests
into the new repository. Add `rebar.config` deps (`leveled`, `memory`,
`telemetry`, `app_config`) + `{bondy_mst, {git|path, …}}`. Wire `bondy_oplog_app`
as the release entry point.
*Gate:* new repo `rebar3 as test eunit` green (the migrated suites, incl. the
CRDT PropEr suites); Jepsen sibling still builds against the moved oplog.

**PR-5 — Trim the `bondy_mst` repo.**
Delete the moved modules/headers/tests; drop `leveled` from `bondy_mst.app.src`
and `rebar.config`; make `bondy_mst_app` the library/minimal app. Update the
root `README.md` to describe `bondy_mst` as the standalone replication-structure
library (and link out to the new repo for `bondy_db`/`bondy_oplog`).
*Gate:* `bondy_mst` repo builds and tests green **with no leveled/oplog deps**;
24 MST test files pass.

**PR-6 — CI / release / docs in both repos.**
Split the architecture chapters: `02_bondy_mst.md` stays here;
`00/01/03/04/05/06/07/08` move to the new repo (they document oplog/db/CRDTs).
Set up CI in both; pin the cross-repo `bondy_mst` dependency version.
*Gate:* `gmake docs` green in both repos; cross-links resolve.

---

## 6. Risks & watch-items

1. **`bondy_mst_admin` placement.** It stays MST-side (cold backup of MST store
   files; its only oplog mention is a moduledoc example). If operators expect a
   single "backup the whole db" entry point, that orchestration belongs in the
   new repo and would *call* `bondy_mst_admin` — keep the lib primitive generic.
2. **`bondy_mst_config` ownership.** `bondy_mst_config:init/0` is invoked from
   `bondy_mst_sup:init/1` today. After PR-2 it must run during MST app start (or
   be invoked by the oplog app before it uses any store). Decide explicitly.
3. **Leveled tag registration timing.** `bondy_oplog_leveled_tag:install/0` must
   run before any leveled bookie opens. Moving it into `bondy_oplog_app:start/2`
   preserves ordering *as long as* the oplog app is the one that starts bookies
   (it is — via `bondy_db_leveled_sup` / topologies). Verify in PR-2.
4. **Cross-repo version pinning.** The frozen §3 surface is the contract; bump a
   `bondy_mst` minor version on any surface change and pin it in the new repo.
5. **Jepsen sibling.** `jepsen/bondy_mst_jepsen` depends on the lib via a
   `{path, …}` dep and exercises the *oplog/CRDT* layer — it must repoint at the
   new repo, not at `bondy_mst`. Handle in PR-4.

---

## 7. One-line summary

The only true coupling from `bondy_mst` to `bondy_db`/`bondy_oplog` is the
application/supervision boot wiring (2 lines). Invert that wiring, split one
shared doc-macro header, move two support modules and the `leveled` dep, and the
two layers separate cleanly with a stable ~9-module public API surface between
them.
