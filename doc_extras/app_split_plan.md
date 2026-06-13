# Splitting the layers: `bondy_mst` (stays) → `bondy_oplog` + `bondy_db` (move to the `bondy` umbrella)

> **Status: IN PROGRESS (2026-06-13).** In-repo restaging DONE; umbrella
> extraction PENDING. Grounded in a full cross-module reference scan of `src/`.

> **Goal.** Extract the consumer/replication layer out of this repo and into the
> **existing `bondy` umbrella** (`~/Work/Bondy/bondy/apps/`) as **two** OTP apps:
>
> ```
> apps/bondy_db      → depends on bondy_oplog + leveled   (consumer facade + storage topologies)
> apps/bondy_oplog   → depends on bondy_mst               (write/replication framework + db_core substrate + CRDT catalogue)
> bondy_mst (dep)    → pure Merkle-Search-Tree library    (THIS repo)
> ```
>
> `bondy_mst` stays here as a pure library. The `bondy` umbrella **already**
> declares `bondy_mst` as a git dep (`rebar.config`, `{branch, develop}`) and
> already pulls `leveled` transitively through it.
>
> **Non-goals.** No behaviour change, no public-API redesign. Module renames are
> limited to the two listed below (agreed 2026-06-13).

> **Supersedes** the earlier "one new repo" plan. The target is now two apps
> inside the `bondy` umbrella, not a standalone repository.

---

## 1. The split does NOT follow the module-name prefixes

A naïve `bondy_db_*` / `bondy_oplog_*` cut is **impossible** — the dependency
graph crosses the prefixes in two places:

### 1a. The `*_core_*` cluster is in a cycle with oplog → it belongs in the LOWER app

The five `bondy_db_core_*` modules and the `bondy_oplog_*` group form a single
strongly-connected cluster:

- **oplog → core:** `applier`, `instance`, `cell_apply`, `secondary_writer`,
  `index_rebuild`, `catalogue_snapshot`, `sync_session` all call
  `…_core_registry:*` / `…_core:publish`; `bondy_oplog_sup` starts the four
  `…_core_*` child specs.
- **core → oplog:** `…_core` and `…_core_registry` call down into
  `bondy_oplog_cell_kernel`, `bondy_oplog_db_overlay`, `bondy_oplog_high_water`,
  `bondy_oplog_hlc`, `bondy_oplog_event`.

Two modules in a cycle cannot live in two OTP apps with a one-way app
dependency. So the `*_core_*` substrate sits in **`bondy_oplog`**, and — per the
2026-06-13 decision — is **renamed `bondy_db_core_* → bondy_oplog_core_*`** so the
app has a single coherent prefix.

| was | now |
|---|---|
| `bondy_db_core` | `bondy_oplog_core` |
| `bondy_db_core_registry` | `bondy_oplog_core_registry` |
| `bondy_db_core_events` | `bondy_oplog_core_events` |
| `bondy_db_core_dispatcher` | `bondy_oplog_core_dispatcher` |
| `bondy_db_core_metrics` | `bondy_oplog_core_metrics` |

> The rename also moved the derived atoms these modules own — telemetry metric
> names (`bondy_db_core_reads_total` → `bondy_oplog_core_reads_total`, …), the
> internal pub/sub event names (`*_registry_started`, `*_dispatcher_started`) and
> ETS table names (`*_registry_tab`, …). All are internal to the extraction set
> (verified: referenced only inside the renamed modules + their tests). **Note
> for umbrella dashboards: the emitted metric names changed.**

### 1b. Only two `bondy_oplog_*` modules touch `leveled` → they move UP to the db app

`leveled` is a `bondy_db` dependency only. The single MST grep
`\bleveled[a-z_]*:` matched exactly two oplog-prefixed modules:
`bondy_oplog_projection_leveled` and `bondy_oplog_leveled_tag`. Their only
non-doc callers are the boot (`bondy_oplog_app`) and the Layer-2 leveled
topologies (the `projection_ets → projection_leveled` and
`cell_frame → leveled_tag` references are **doc-only**). They move into the
`bondy_db` app and — per the 2026-06-13 decision — are **renamed**:

| was | now |
|---|---|
| `bondy_oplog_projection_leveled` | `bondy_db_projection_leveled` |
| `bondy_oplog_leveled_tag` | `bondy_db_leveled_tag` |

After this, **`bondy_oplog` has zero `leveled` references** (verified).

---

## 2. Dependency-true module inventory (101 modules: 90 + 11)

### `apps/bondy_oplog` — 90 modules — depends on `bondy_mst` (no `leveled`)

- 83 × `bondy_oplog_*` (the framework + the 19-module `bondy_oplog_crdt_*`
  catalogue), **minus** the 2 leveled modules that moved up.
- 5 × `bondy_oplog_core_*` (the renamed `db_core` substrate — registry,
  dispatcher, events, metrics, core).
- 2 × support: `bondy_dvvset` (used by 11 CRDT/applier modules, 0 MST users),
  `bondy_metrics` (used by `…_core_metrics`, `…_latency`, `…_sup`).

### `apps/bondy_db` — 11 modules — depends on `bondy_oplog` + `leveled`

- `bondy_db` (public table facade).
- 7 × `bondy_db_topology*` (`topology`, `_leveled_common`, `_memory`,
  `_memory_owner`, `_per_entity`, `_shared_shards`, `_single_bookie`).
- `bondy_db_leveled_sup` (leveled bookie supervisor).
- `bondy_db_projection_leveled` + `bondy_db_leveled_tag` (renamed, moved up).

### Stays in THIS repo — `bondy_mst` library — 27 modules

`bondy_mst`, `bondy_mst_store/_page/_io`, `bondy_mst_crdt` (state-based merge
engine), `bondy_mst_pack_*` (18, durable pack store), `bondy_mst_ets_store`,
`bondy_mst_map_store`, `bondy_mst_admin`, `bondy_mst_config/_utils/_coalescing_queue`,
`bondy_mst_app/_sup` (now the pure library app/sup).

---

## 3. The only two edges that still break the clean direction

Both are tolerated by the current **single-app** staging build (everything
compiles in one app), but must be resolved when the apps are physically carved:

1. **Boot: leveled-tag install.** `bondy_oplog_app:start/2` calls
   `bondy_db_leveled_tag:install/0` — an oplog→db edge. **Fix:** move the install
   into `bondy_db_app:start/2`. `bondy_db` owns the bookies (via
   `bondy_db_leveled_sup` + topologies), so the "tag installed before any bookie
   opens" ordering is preserved within `bondy_db`'s own boot.
2. **`bondy_oplog_latency:probe_write`.** `bondy_oplog_latency:425` calls
   `bondy_db:probe_write/1` — an oplog→facade edge. **Fix:** inject the probe MFA
   into the latency tick via config (the latency instance is configured with a
   `probe_fun`), or push the probe primitive down into `bondy_oplog_instance`.

No other oplog→db code edges exist (verified by the upward-edge scan).

---

## 4. Headers & deps

| header | home |
|---|---|
| `include/bondy_mst.hrl`, `bondy_mst_pack.hrl` | THIS repo (`bondy_mst`) |
| `include/bondy_doc.hrl` (doc macros) | copy into the umbrella (or a shared include) |
| `include/bondy_oplog.hrl` (has `?BONDY_FOLD_TAG`), `bondy_oplog_wal.hrl` | `apps/bondy_oplog/include/` |

- `bondy_db_leveled_tag` uses `?BONDY_FOLD_TAG` → after the move it includes it
  via `-include_lib("bondy_oplog/include/bondy_oplog.hrl")` (cross-app include).
- **Umbrella deps:** add `leveled` as a **direct** top-level dep (today it is only
  transitive through `bondy_mst`, which drops it in the trim PR). `bondy_mst`
  itself is already declared. `app_config`/`utils`/`memory`/`resulto`/`telemetry`
  are already present.
- **`bondy_mst.app.src` (this repo):** revert `{mod, {bondy_oplog_app,…}}` →
  `{mod, {bondy_mst_app,…}}`; drop `leveled` from `applications` and
  `rebar.config`.

---

## 5. PR sequence

### Done — decouple in place (this repo)

- **PR-1 / PR-2 (commit `b3797ba`)** — header decoupling (`bondy_doc.hrl`;
  `?BONDY_FOLD_TAG` → `bondy_oplog.hrl`) + boot inversion (`bondy_oplog_app`
  owns boot; `bondy_mst_*` has zero upward edges).
- **Restage (2026-06-13, uncommitted)** — moved the 101 modules into
  `src/bondy_oplog/` (90) + `src/bondy_db/` (11); applied the §1a/§1b renames;
  widened the `erlfmt` glob to cover the subfolders. **Single-app build green;
  all 1700 eunit tests pass.** This is the literal staging of the two future apps
  as `src/` subfolders.

### Pending — extract into the `bondy` umbrella

- **PR-E1 — Carve `apps/bondy_oplog`.** Create `apps/bondy_oplog/{src,include}`;
  move the 90 modules + `bondy_oplog.hrl` + `bondy_oplog_wal.hrl` + their tests;
  `bondy_oplog.app.src` (`{mod,{bondy_oplog_app,…}}`, `applications` includes
  `bondy_mst`). Resolve **edge §3.1** (drop the leveled-tag install from
  `bondy_oplog_app`) and **edge §3.2** (inject the latency probe). *Gate:*
  `apps/bondy_oplog` compiles and its eunit/PropEr suites pass against the
  `bondy_mst` dep.
- **PR-E2 — Carve `apps/bondy_db`.** Create `apps/bondy_db`; move the 11 modules
  + their tests; `bondy_db.app.src` (`{mod,{bondy_db_app,…}}`, `applications`
  includes `bondy_oplog`); add `bondy_db_sup` + `bondy_db_app` that install the
  leveled tag then start `bondy_db_leveled_sup` + topologies. Add `leveled` to
  the umbrella `rebar.config`. *Gate:* umbrella boots; db→oplog→mst start order
  correct.
- **PR-E3 — Trim `bondy_mst` (this repo).** Delete `src/bondy_oplog/` +
  `src/bondy_db/` + moved headers + moved tests; revert `bondy_mst.app.src` to
  `bondy_mst_app`; drop `leveled` from `app.src`/`rebar.config`. *Gate:*
  `bondy_mst` builds & tests green with **no leveled/oplog deps**.
- **PR-E4 — Docs/CI.** Move architecture chapters `00/01/03/04/05/06/07/08`
  (oplog/db/CRDTs) into the umbrella; `02_bondy_mst.md` stays here. Sweep the
  rename through the remaining `doc_extras/*.md` (currently still mention
  `bondy_db_core` / old leveled names). Repoint the `jepsen/` sibling at the
  umbrella apps. Pin the cross-repo `bondy_mst` version.

---

## 6. One-line summary

The clean stack is `bondy_db → bondy_oplog → bondy_mst`, but it cuts *across* the
old prefixes: the `*_core_*` substrate moves DOWN into `bondy_oplog` (renamed
`bondy_oplog_core_*`) because it cycles with oplog, and the two `leveled` modules
move UP into `bondy_db` (renamed `bondy_db_*`). Only two stray edges
(leveled-tag boot install, latency probe) need a code fix; everything else is
already a clean downward edge. In-repo staging + renames are done and green
(1700 tests); what remains is the physical carve into `apps/bondy_oplog` +
`apps/bondy_db`.
