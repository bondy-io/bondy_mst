# Re-grounding bondy_db on Operation-Based CRDTs (Canteen × MST)

> **Status: SHIPPED (2026-06).** This re-grounding is complete — the
> pure operation-based architecture it prescribes is the as-built
> system. The authoritative description now lives in the architecture
> chapters, principally
> [The CRDT model](architecture/05_crdt_model.md). This document is
> retained as the historical *rationale*: why the state-based fold
> design was wrong and how the correction was derived from the
> reference papers.

> Architectural assessment + correction plan. Grounded **only** in the two
> reference papers (`reference/canteen.pdf`, `reference/mst.pdf`) and the
> source at the time of writing.
>
> Companion visual: [`architecture_regrounding.html`](architecture_regrounding.html)
> (then-current vs target SVG diagrams).

---

## 1. Context — why this change

The project's objective is an **operation-based CRDT (CmRDT)** datastore in the
Canteen model: a partially-ordered log of *operations* replayed through a
**COG-Interpreter** (`interpret_cog`). Over time the catalogue (`bondy_db`)
implementation drifted into **state-based CRDTs (CvRDT)** — per-cell `apply_event`
folds that carry a `merge_states` join. The drift is costly: it forces a single
collapsed per-cell state (so non-commutative CRDTs are impossible), and it was the
root of the convergence confusion this effort exists to end.

The substrate has been heavily **performance-tuned** (WAL group-commit, pack-store
sealing, applier batching/caching, head-only projections, sharding). The
correction must **keep all of that** ("don't throw the baby out with the
bathwater") and must **not** reuse the now-deprecated state-based modules (using
them re-introduces the drift).

Intended outcome: one authoritative semantics — `interpret_cog` — for the
catalogue, with native operation-based CRDTs, the perf infra intact, and the
state-based machinery removed.

---

## 2. Assessment — where the architecture sits today

The system is **two layers, and only the second drifted.**

### Layer 1 — the op-log substrate (CORRECT — the *MST paper*)
The MST is a **grow-only set of operations**, keyed by the dot
`{HLC, Origin, Seq}` (`bondy_oplog_event.erl:44`), reconciled **state-based** by
set-union anti-entropy (Auvolat & Taïani). This is the one place "state-based" is
*correct*: it reconciles the op-*set*, not the application semantics. The MST
paper §IV-C even sanctions "gossip individual operations and apply them as
received" when only eventual consistency is needed. Modules: `bondy_mst`,
`bondy_mst_pack_*`, `bondy_oplog_wal*`, `bondy_oplog_sync_*`. **This layer is the
Canteen δ-swap/DAG substrate (HLC-dots instead of explicit ancestor-hashes) and
stays as-is.**

### Layer 2 — the application CRDT (DRIFTED — the *Canteen paper*)
Application state should be produced **operation-based** by replaying the op-log
through the COG-Interpreter `interpret_cog(COG, State)`. Instead the catalogue
builds the projection by **incremental `apply_event` in arrival order**
(`bondy_oplog_applier.erl:1714`), using state-based folds with a `merge_states`
join (`bondy_oplog_applier.erl:2587`, bootstrap "merge mode"). The 13 fold modules
implement only `bondy_oplog_fold`; **zero** implement `bondy_oplog_crdt`.

### What already exists (and is right)
- `bondy_oplog_crdt` behaviour (`init/0`, `interpret_cog/2`, `query/2`) — the
  sanctioned COG-Interpreter contract — but `interpret_cog` is invoked **only** at
  compaction for monolithic CRDT instances (`bondy_oplog_instance.erl:3007`) and
  in the dormant hot-query path (`bondy_oplog_query.erl:57`). Only
  `test/bondy_oplog_test_counter.erl` implements it.
- The **projection adapters** (leveled head-only / ETS ephemeral) are *literally*
  Canteen's Fig-2 **"Domain-Specific Database"** — the serial, metadata-free stable
  store.
- **Compaction** (`compute_frontier_for`, longest common prefix across peer roots)
  **is** Canteen COG-stability GC; the MST is already bounded by it (landed).
- **No causal hold-back buffer exists** — application is pure HLC-key order. Under
  the COG model this is **not a gap** (see §5).

### The single drift seam
`bondy_oplog_applier.erl:1714` — `compute_one_cell/11` calls
`bondy_oplog_fold:apply_event/3` per event. Everything else (cell-frame, read
path, secondary-index term-diff, bootstrap merge-mode) hangs off that one choice.

---

## 3. The vision (from the papers), mapped to this codebase

| Canteen concept | bondy_mst realisation | Status |
|---|---|---|
| Partially-ordered op-log (DAG) | MST = grow-only op-set, dots `{HLC,Origin,Seq}` | ✅ keep |
| δ-swap gossip; roots as logical clocks | MST anti-entropy + root-hash peer compare | ✅ keep |
| COG-stability → snapshot → truncate | `compute_frontier_for` + compaction checkpoint | ✅ keep (make checkpoint real) |
| **COG-Interpreter `interpret_cog`** | `bondy_oplog_crdt:interpret_cog/2` | ⚠️ exists, **must become the catalogue kernel** |
| Domain-Specific DB (serial store) | projection adapters (leveled/ETS) | ✅ keep |
| Two-tier query (stable / unstable) | `bondy_oplog_query` + projection reads | ⚠️ extend to the catalogue |
| Non-commutative CRDTs | — (impossible under folds) | ❌ unlock via `interpret_cog` |

MST = Layer 1 (op-set reconciliation, legitimately state-based).
Canteen COG-Interpreter = Layer 2 (op-based application semantics). The drift
collapsed Layer 2 into a second state-based layer; the fix restores it.

---

## 4. Target architecture — keep / change / remove

### KEEP UNCHANGED (the performance-tuned "baby" — all fold-independent)
- **WAL**: group commit, fsync modes, segment rotation/retention (`bondy_oplog_wal*`).
- **MST pack store**: auto-seal, tombstone-flush debounce, content-hash, idx rebuild (`bondy_mst_pack_*`, `bondy_mst*`).
- **Applier throughput levers**: coarse batching (A2), OldValue/hot-key frame cache (A3), install/write coalescing, per-shard install-slot flow control, one-instance-per-shard. *Only the per-cell compute kernel inside the batch changes.*
- **Projection adapters** (leveled head-only batched `book_mput` / ETS ephemeral), per-namespace backend selection, secondary-index writer/back-pressure/rebuild, the cell-frame codec (`bondy_oplog_cell_frame`).
- **Sync scheduler + bootstrap-lifecycle gate** (`pre_bootstrap→live`) + peer-state + catalogue-snapshot transport.

### CHANGE
- `bondy_oplog_crdt` — extend the contract with the projection-seam callbacks the folds used to provide: `to_value/1`, `hlc/1`, `encode_state/1`, `decode_state/1` (+ optional `value_equals_state/0`, `order_independent/0` as a *commutativity marker*, `gc_threshold/1`). **No `encode_event/decode_event`** — ops travel as opaque terms in the WAL/MST (`bondy_oplog_wal_codec` does not call the fold), the single biggest simplification.
- `bondy_oplog_applier` — redirect the seam (`compute_one_cell`, `:1714`) from `apply_event` to `interpret_cog`; drop the `apply_value_delta` delta-plumbing (`compose_value_bytes`, `:2268`) — value becomes `to_value(state)`.
- `bondy_oplog_instance` — catalogue compaction. ✅ **DONE (step 4, reinterpreted)** — the per-cell `interpret_cog` checkpoint **is** the durable projection (maintained by the applier kernel since 3/3b), so compaction only bounds the MST; `projection_managed` retained (a separate checkpoint would duplicate the projection). *Replay-before-truncate* safety kept. The monolithic-CRDT path (`:3007`) already folds via `interpret_cog`.
- `bondy_oplog_core` — read/overlay path projects via `to_value` / interprets overlay via `interpret_cog`. ✅ **DONE (step 3b)** — all read helpers go through `bondy_oplog_cell_kernel` (`interpret_overlay/4` + `decode_value_bytes/2`); the CRDT read path calls `interpret_cog`, never `apply_event`.
- `bondy_oplog_sync_session` — `install_mode/1` (`:359`) collapses `merge`→ checkpoint-install + op-replay.

### REMOVE (end state)
- `bondy_oplog_crdt_fold` (state-based bridge), `bondy_oplog_fold`, all `bondy_oplog_fold_*` (already `-deprecated`).
- `merge_states` + bootstrap merge-mode (`handle_cell merge`, `bondy_oplog_applier.erl:2567`); `apply_value_delta`/Contract-C delta plumbing; `value_equals_state` cell-frame branching (unless kept as a pure g-set storage optimisation).

---

## 5. Causal delivery — resolved, no hold-back buffer needed

The classic CmRDT requirement is *causal delivery* via a version-vector hold-back
buffer. This system needs none, structurally:

1. **The MST is the buffer.** Anti-entropy reconciles the op-*set*, not a stream;
   interpretation reads the converged set, so "arrival before predecessor" is a
   non-event at the interpretation layer.
2. **`interpret_cog` consumes the set in canonical dot order, not arrival order.**
   HLC respects happens-before, so the dot order is a *causal linearization* of the
   set. Same set ⇒ same order ⇒ same state on every replica.
3. **Convergence rests on set-convergence (MST) + deterministic key-ordered
   interpretation** — exactly what lets non-commutative CRDTs work without a buffer.
4. The drift's *incremental on-arrival `apply_event`* is the only thing that made
   arrival order matter. Removing it removes the requirement. The one real ordering
   obligation — don't truncate a stable event before it's folded — is already
   enforced by the replay-before-truncate compaction guard.

---

## 6. KEY DECISION — how the projection is maintained · **DECIDED: Option B**

> **Decision (2026-06-08):** **Option B — eager-materialised value**, with the
> kernel split by commutativity (below). The rollout proceeds on this basis.

This is the one genuine fork. The catalogue projection is the hot read path (WAMP
registry: read-your-writes, reads ≫ writes), so read latency is a hard constraint.

- **Option A — faithful-lazy Canteen.** Projection holds only *stable* snapshots
  (written by `interpret_cog` at compaction). Writes touch only the log; unstable
  reads fold the cell's live events via `interpret_cog`. Simplest, most faithful —
  but every read of a just-written cell pays an `interpret_cog` fold (read-latency
  cliff on the hottest path).
- **Option B — eager-materialised value (RECOMMENDED).** The applier keeps the
  projection's materialised `value` current on write, with `interpret_cog` as the
  **sole** kernel. Reads stay O(1) (single projection/HEAD read), matching today.

**Recommendation: Option B, with the kernel split by commutativity (not a re-drift):**
- **Commutative CRDTs (12 of 13: registers, sets, counters, presence):** the
  applier folds the new op onto the materialised state — which for a commutative
  CRDT *is* `interpret_cog` (order cannot change the result). O(1), no per-cell
  live-log.
- **Non-commutative CRDTs (`aw_map`; future `bounded_counter`):** the applier
  re-interprets the cell's live group `interpret_cog(checkpoint, live_events)` on
  write (cost O(live-log), bounded by compaction). These need the group anyway —
  there is no correct O(1) path — so the live-log is stored *only* for these rare
  cells.

The `order_independent/0` marker (a *property of the CRDT module*, validated by
test) selects the path.

> Alternative: if read-your-writes can be served by the existing in-memory overlay
> (which already stages recent events), Option A becomes simpler and more faithful.
> The rollout below assumes B.

---

## 7. Sequenced rollout (one PR each; each shippable + Architecture-QA'd)

Early steps are additive/flag-guarded and reversible; the irreversible cutover
(default flip, deletions) is deferred until the non-commutative CRDTs and the
throughput/latency gates have proven out. **No PR adds a new caller of a deprecated
module; the new kernel calls `interpret_cog` exclusively.**

1. **Contract + sanctioned commutative helper (additive, no behaviour change).**
   ✅ **LANDED (2026-06-08, uncommitted).** Extended `bondy_oplog_crdt` with the
   projection-seam callbacks (`to_value/1`, `hlc/1`, `encode_state/1`,
   `decode_state/1` required; `gc_threshold/1`, `value_equals_state/0`,
   `order_independent/0` optional; **no** `encode_event/decode_event`). Built
   `bondy_oplog_crdt_commutative` — a *pure ops-based* helper (`interpret_cog/3` =
   key-ordered fold of a per-op `apply_op/3`; `apply_op/4` = the O(1) eager step)
   that replaces the deprecated `bondy_oplog_crdt_fold` and depends **only** on
   `bondy_oplog_event`. Added `bondy_oplog_crdt_tags` (stable sub-CRDT tag numbers,
   wire-identical to the fold-era allocation, dependency-free). Worked example
   `bondy_oplog_crdt_gset_example` (test/) + property test
   `bondy_oplog_crdt_commutative_test`: `interpret_cog` is a deterministic function
   of the event *set*, and the eager single-op path equals the key-ordered batch.
   `bondy_oplog_test_counter` made contract-conformant. 1710 tests pass (+8).
2. **`bounded_counter` — the non-commutative proof (greenfield).**
   ✅ **LANDED (2026-06-08, uncommitted).** `src/bondy_oplog_crdt_bounded_counter`:
   first native non-commutative `bondy_oplog_crdt` CRDT (Canteen's
   `zero_bounded_counter`). State `{Value≥0, Hlc}`; `interpret_cog/2` nets the
   group (increments-before-decrements) and clamps at the group boundary —
   `max(0, V0 + Σinc − Σdec)` — a pure function of the event *set*. Declares
   `order_independent() -> false`. No production coupling (not referenced by the
   applier/registry/instance). `test/bondy_oplog_crdt_bounded_counter_test`
   (12 tests): the de-risk **witness** — naive per-op clamped application diverges
   by arrival order (0 vs 1) while `interpret_cog` converges — plus two-replica
   reorder convergence, clamp-at-stability "forgotten-debt" semantics, cell_apply
   unwrap, HLC/GC/encode round trips. 1722 tests (+12); 1 pre-existing full-suite
   flake (`bondy_mst_pack_writer_test:t_threshold_fires_eventually_test`, Layer-1,
   passes 3/3 in isolation).
3. **Redirect the applier seam to `interpret_cog` behind a per-instance selector.**
   ✅ **LANDED (2026-06-08, uncommitted).** New `bondy_oplog_cell_kernel`
   (`{fold,Mod} | {crdt,Mod}`) localizes the selector at the per-cell seam: every
   `bondy_oplog_fold:F(Fold,...)` in `compute_one_cell`/read/index paths is now
   `bondy_oplog_cell_kernel:F(Kernel,...)`. `from_modules/2` selects `crdt_module`
   over `fold_module`; the delta plumbing (`compose_value_bytes`/`decode_old_value`)
   moved into the kernel's fold branch. Option B hybrid: the **commutative** branch
   is the O(1) `apply_op/3` step (== `interpret_cog`), value = `to_value(state)`, no
   delta; the **non-commutative** branch is refused with a clear error (its live-log
   path is step 5). New native `bondy_oplog_crdt_lww_register` wired end-to-end:
   `crdt_module` threaded through `bondy_oplog_core_registry` (entry/config/accessor)
   and `bondy_db:open_table` (`provision_shards`/`provision_shard` + `info/1`), into
   the applier `cell_apply_ctx`. e2e tests (`bondy_oplog_crdt_lww_e2e_test`, 8):
   real applier → projection → read on the CRDT kernel + the public `open_table`
   path. The fold path is **byte-identical** (kernel fold-branch equivalence test +
   71 `bondy_db_test` + `cell_apply_test` all green); selector defaults to fold, so
   reversible per table. The kernel calls **no** `-deprecated` fold function and the
   CRDT path is fold-free. 1747 tests (+25). **Carry-overs:** (a) ✅ **CLOSED by
   step 3b** (below); (b) the formal **throughput/read-latency gate** is deferred to
   the step-6 default-flip on Fly/Linux (macOS is storage-bound/unrepresentative; the
   fold path is byte-identical so existing tables carry zero regression risk);
   (c) compaction checkpoint for CRDT cell instances is step 4.
   - **3b. Kernel-ify the `bondy_oplog_core` read/overlay path.** ✅ **LANDED
     (2026-06-09, uncommitted).** The symmetric other half of the seam: the **read**
     path now interprets the COG instead of folding events. Added
     `bondy_oplog_cell_kernel:interpret_overlay/4` (the operation-based overlay merge
     — `{crdt,Mod}` calls the CRDT's own `interpret_cog/2` over the overlay group on
     the projection state, **not** `apply_event`; `{fold,Mod}` is byte-identical to
     the old `fold_state/4`) and `decode_value_bytes/2` (kernel-aware value-slot
     decode). Threaded `Kernel` (via new `kernel_for/1` = `from_modules(fold,crdt)`)
     through **every** read helper in `bondy_oplog_core` — `read_state`, the
     `slow_read_*` chain, `read_projection_state[_with_hlc]`, `fenced_read` (batch),
     `do_range`/`merge_range`/`emit_range_cell`, `do_read_at_hlc` — removing all
     direct `bondy_oplog_fold:*` calls (the only `entry_fold_module` left is inside
     `kernel_for/1`, building the kernel). A CRDT table's read path now touches
     **zero** deprecated fold functions. Tests: +5 `bondy_oplog_cell_kernel_test`
     (interpret_overlay COG-interpretation + cell_apply unwrap + fold byte-equiv +
     empty-passthrough + decode_value_bytes roundtrip) and +3
     `bondy_oplog_crdt_lww_e2e_test` (a CRDT shard with overlay **ENABLED**: highest-
     HLC-wins/order-independent merge, clear-clears, below-projection-ignored — the
     full read seam end to end). Fold path proven unchanged (120 read-coupled tests
     green). 1755 tests (+8). Selector still defaults to fold; overlay may now be
     enabled on CRDT tables without the prior "fold_module must agree on
     value_equals_state" footgun.
4. **Real catalogue compaction checkpoint.** ✅ **LANDED (2026-06-09, uncommitted)
   — reinterpreted (see DEVIATION).** Verify: post-compaction read == from-scratch
   `interpret_cog`, on the native `crdt_module` kernel
   (`bondy_oplog_catalogue_compaction_test:crdt_kernel_compaction_matches_from_scratch`:
   two instances apply the same LWW-overwrite+clear event set; one compacts to MST
   truncation, the other never compacts; every cell reads identically on both and
   equals the hand-computed `interpret_cog` winner). Replay-before-truncate guard
   (`ensure_projection_caught_up`) kept. 1756 tests.
   - **DEVIATION (per the empirically-better-implementation rule):** the literal
     plan text — *fold the stable prefix per cell into a separate `interpret_cog`
     checkpoint; remove the `projection_managed` sentinel* — is **rejected as a
     regression** and is **already achieved** by steps 3/3b. The catalogue's per-cell
     `interpret_cog` checkpoint **IS the durable projection**: post-3/3b the applier's
     cell kernel maintains each cell via `interpret_cog` (`apply_op` ≡ `interpret_cog`
     for commutative) and the read path merges live events via `interpret_cog`. A
     separate per-cell checkpoint blob would **duplicate the projection** (double
     storage + writes) for zero gain — the checkpoint store is explicitly *"not a
     durability layer for the projection"* (`bondy_oplog_compaction_checkpoint`
     moduledoc); it is a replay-cost cache, and the durable projection already serves
     that role for a catalogue. Checked every config (durable leveled projection
     survives restart and holds the state; ephemeral instances are non-durable by
     construction): no correct config benefits from a separate checkpoint. So
     `projection_managed` is **retained** as the correct representation ("state lives
     in the projection"); compaction's only catalogue job is replay-before-truncate +
     MST bound. Step-4's genuine deliverable was the **verification** (above, now on
     the native CRDT path — prior tests covered only the fold kernel) plus reframing
     the misleading "truncate-only sentinel / PR-2 step 1" code comments.
5. **`aw_map` as a native per-dot observed-remove COG CRDT.** The production
   non-commutative case; retires the deprecated aw_map. Regression test on the
   tombstone→lower-HLC-revive witness, converging *natively* (not by re-fold).
6. **Migrate the remaining commutative folds → native CRDTs; flip the default
   selector to `crdt_module`.** Each a mechanical lift (`apply_event/3` body →
   `apply_op/3`, drop the delta; copy `to_value/hlc/encode_state/decode_state`).
   Per-CRDT equivalence + convergence tests gate each flip.
7. **Remove bootstrap merge-mode + `merge_states`.** Install = checkpoint-replace +
   op-replay via `interpret_cog`; collapse `install_mode/1`. Two-replica
   bootstrap-then-converge test.
8. **Decommission.** Delete `bondy_oplog_crdt_fold`, `bondy_oplog_fold`, all
   `bondy_oplog_fold_*`; remove the selector + dead plumbing; reconcile docs in
   `doc_extras/architecture/`.

---

## 8. Risks & non-regression gates

- **R1 — Registry read-latency regression.** Mitigated by Option B (eager value ⇒
  O(1) reads). Hard acceptance gate in step 3; selector allows instant fallback.
- **R2 — Write-throughput regression** (per-batch `interpret_cog` vs per-event
  `apply_event`). Bounded live-log (compaction) + preserved A2/A3. If a hot-cell
  regression appears, the response is the `order_independent` fast path **over the
  correct base** — never a revert to folds. Measured in step 3.
- **R3 — Mixed-kernel divergence during migration.** Commutative folds converge
  identically on both kernels by construction; non-commutative cases are proven
  (steps 2, 5) before the default flip (step 6). `merge_states` kept until step 7.
- **R4 — Truncation outrunning the projection.** Already solved by
  replay-before-truncate; preserved in step 4.
- **R5 — Bootstrap correctness without merge-mode.** Deferred to step 7 (after real
  checkpoints exist); replay is idempotent, making `replace`+replay safe.

---

## 9. Verification (per the rollout)

- **Per-CRDT determinism/convergence:** `interpret_cog(perm(Events))` invariant after
  canonical sort; commutative subset additionally equals the key-ordered incremental
  fold; non-commutative cases (`aw_map`, `bounded_counter`) converge across two
  replicas under reordering (incl. the tombstone→lower-HLC-revive witness).
- **Compaction:** post-compaction read == from-scratch `interpret_cog`.
- **Throughput/latency gates (step 3):** durable write throughput + registry read
  latency stay in the write-stack target band (hard gate).
- **Regression:** full `rebar3 as test eunit` green; clean `/tmp` WAL/leveled
  artifacts after each batch.
- **Each PR** ends in an Architecture QA against this plan.
