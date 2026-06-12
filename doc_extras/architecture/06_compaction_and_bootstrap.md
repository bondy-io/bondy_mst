# Causal stability, compaction & bootstrap

> Audience: anyone who wants to know **why the log is bounded**, why a
> long-running cluster eventually carries no live events at all, and
> how a brand-new replica gets up to speed.
> Time to read: ~20 min.

This chapter covers the property that distinguishes `bondy_oplog` from
a plain replicated log: **the oplog is self-truncating**. Once peers
have caught up, the events that they all hold can be removed from the
MST — not marked deleted, *physically* removed — and replaced with a
single compacted state snapshot. At full convergence the live MST is
**empty**; the cluster's persistent state is the snapshot alone.

The mechanism in this codebase is the COG (Concurrent Operation
Group) idea — `bondy_oplog_compaction` orchestrates;
`bondy_oplog_instance` runs the cycle; `bondy_oplog_gc_scheduler`
fires it on a timer; `bondy_mst:truncate/2` does the physical
deletion; `bondy_oplog_sync_session` (`bootstrap/3` /
`bootstrap_catalogue/3`) carries the snapshot to new replicas.

## The intuition: the MST is not a log, it's a *window*

Everywhere else in the architecture documentation we talk about
"events in the MST". That is true, but it understates the lifecycle.
Pictured over time, an instance looks like this:

```mermaid
flowchart TD
    subgraph T0["t=0 fresh instance"]
        S0["snapshot=∅"]
        M0["MST: e1 .. e_n<br/>live"]
    end
    subgraph T1["t=1 partial convergence"]
        S1["snapshot(e1 .. e_k)"]
        W1["watermark = e_k"]
        M1["MST: e_{k+1} .. e_n<br/>live"]
    end
    subgraph T2["t=2 full convergence"]
        S2["snapshot(e1 .. e_n)"]
        W2["watermark = e_n"]
        M2["MST: empty"]
    end

    T0 --> T1 --> T2
```

The MST is the **live window** of unstable events. The snapshot is the
**closed past**. The watermark separates the two. As peers exchange
events and confirm they have them, the boundary advances.

A handful of consequences fall out of this:

1. **The oplog is bounded by network propagation, not by app
   lifetime.** A long-quiet cluster's MST shrinks to nothing.
2. **A new replica syncs the snapshot once, then catches up the
   small live tail.** Not the whole history.
3. **The MST root hash changes when truncation happens** — and that
   is fine, because every replica that has seen the same set of
   peers truncates to the same watermark and arrives at the same
   tree.

## What does "stable" mean here?

An event is **stable** when every (fresh, non-stale) peer is known to
hold it. The library tracks this through per-peer root hashes,
recorded by sync sessions:

```mermaid
flowchart TB
    SESS[bondy_oplog_sync_session]
    PSTATE["bondy_oplog_peer_state<br/>ETS: {peer_id, instance_id} → root_hash"]
    COMP[bondy_oplog_compaction]

    SESS -->|on success| PSTATE
    COMP -->|read| PSTATE
```

`bondy_oplog_peer_state` records, per `(peer, instance)`, the most
recent root hash observed on a successful sync, plus a `last_seen`
timestamp. Peers we haven't heard from in `peer_timeout_ms` (default
30s) are filtered out — silent peers must not pin the watermark
forever.

## Computing the stability frontier

The frontier is the highest event key K such that **every fresh peer
has every key ≤ K**. The algorithm — `compute_frontier_for/2` in
`bondy_oplog_instance.erl` — is:

```mermaid
flowchart TB
    L["LocalKeys = sort(keys(MST))"]
    P["For each peer root R: PeerKeys_R = keys_set_at_root(MST, R)"]
    LCP["Frontier = longest_common_prefix(LocalKeys, [PeerKeys_R, ...])"]
    F["return Frontier · or undefined if no fresh peers"]

    L --> LCP
    P --> LCP
    LCP --> F
```

A few subtle things:

- **`keys_set_at_root(MST, R)`** walks the *local* MST as if its root
  were R, recovering the set of keys present at that historical
  state. Page content-addressing makes this cheap — the historical
  root's pages are still present if they have not been GC'd.
- **The frontier is the longest common *prefix*** because keys are
  HLC-ordered. Stability is monotonic in HLC: if a peer has key K,
  it transitively has every key < K.
- **No coordination is required.** Two replicas that have observed
  the same set of peers compute the same frontier independently.

## The compaction cycle

`bondy_oplog_gc_scheduler` ticks every `gc_interval_ms` (default 1s)
and spawns one short-lived worker per instance. The worker just **issues
the compaction call**; the instance gen_server runs the whole five-step
cycle **synchronously**:

```mermaid
sequenceDiagram
    autonumber
    participant Sched as gc_scheduler
    participant Worker as per-instance worker
    participant Comp as oplog_compaction
    participant Inst as oplog_instance (gen_server)
    participant Crdt as CRDT module
    participant CK as compaction_checkpoint

    Sched->>Worker: spawn (so the scheduler never blocks)
    Worker->>Comp: compact(InstanceId)
    Comp->>Inst: compact(InstanceId, PeerRoots)  [gen_server:call]
    Note over Inst: runs SYNCHRONOUSLY inside the gen_server
    Inst->>Inst: compute_frontier_for(MST, PeerRoots)
    alt catalogue (projection-backed) instance
        Note over Inst: fast path — no re-fold.<br/>The projection IS the per-cell<br/>interpret_cog checkpoint,<br/>maintained eagerly on write.
    else bare single-CRDT instance
        Inst->>Crdt: interpret_cog(Events, BaseCheckpoint)
        Crdt-->>Inst: NewCheckpoint
        Inst->>CK: put_checkpoint(frontier, NewCheckpoint)
    end
    Inst->>Inst: truncate MST + watermark advance + HLC bump
    Inst-->>Comp: {ok, {compacted, Frontier, N}}
    Comp-->>Worker: result
```

Three design choices worth noting:

- **Compaction runs synchronously inside the instance gen_server.**
  Frontier computation, `interpret_cog`, checkpoint persistence, and the
  truncate/watermark/HLC commit all run in the gen_server's `compact`
  handler. The only off-process actor is the gc_scheduler's per-instance
  worker, which merely issues the call — so the *scheduler* never blocks.
  While a compaction runs the instance is busy, but lock-free reads and
  stateless appends are unaffected (they bypass the gen_server); only the
  applier's install casts queue behind it.
- **The cycle is concurrency-guarded.** Only one compaction per
  instance is in flight at a time; overlapping requests reply
  `{ok, no_change}` and the next tick retries. Compaction is
  idempotent, so a missed tick is not a problem.
- **No overlay-drain barrier.** The frontier derives from peer-synced
  roots, which only ever reflect installed, published events — it is
  always at or below the installed watermark, strictly below anything
  still pending in the overlay. `compact/1` therefore runs safely
  under sustained write load; in fused mode the yielding drain
  guarantees the compact request is actually serviced between drain
  slices. (`truncate_prefix/2`, whose watermark is caller-supplied
  and arbitrary, keeps its barrier.)

The cycle yields `{ok, no_change}` (no fresh peers, empty
intersection, or frontier ≤ current watermark) far more often than
`{ok, {compacted, _, _}}`. That is expected — most ticks are no-ops
that just confirm there is nothing new to compact.

## "Truncate" means **delete pages**, not tombstone events

The MST truncation step is the load-bearing one. It is not a
soft-delete: the instance calls `bondy_mst:truncate/2` with the
watermark, a **structural prefix-truncate** that removes every key
`≤ Watermark` in one pass.

```mermaid
flowchart TB
    TR["bondy_mst:truncate(MST, Watermark)"]
    SPINE["walk the LEFT spine only<br/>(the prefix lives there — keys are HLC-ordered)"]
    DROP["drop entries ≤ W together with<br/>their left subtrees (freed for GC)"]
    REBUILD["rewrite the O(log N) spine pages<br/>that straddle W"]
    NEWROOT[new root hash]

    TR --> SPINE --> DROP --> REBUILD --> NEWROOT
```

Unlike calling `bondy_mst:delete/2` once per stale key — `O(P·log N)`
in the prefix size, one spine rebuild per key — `truncate/2` walks the
left spine **once**, rewrites only the `O(log N)` pages that straddle
the watermark, and leaves the dropped subtrees unreferenced for the
store's GC. Because the MST is history-independent, the result is
**byte-identical** (same root hash) to the equivalent sequence of
deletes and to a fresh tree built from the surviving keys — a
property pinned by equivalence tests.

This matters because:

- **No tombstones to ship over AE.** Sync sessions exchange only
  pages that exist; truncated keys are simply gone.
- **GC reclaims the space.** The pack-store rewrite GC reads only
  pages reachable from the live root; freed pages are not written
  to the new pack and the old packs are unlinked. (The ETS store
  prunes freed pages by epoch.)
- **Truncation is deterministic.** Every replica that truncates the
  same prefix arrives at the same tree (same pages, same root hash).

`bondy_mst:truncate/2` is implemented in `bondy_mst.erl`
(`truncate_at` / `truncate_scan` / `rebuild_truncated`);
`bondy_mst:delete/2` remains for single-key structural deletion.

## The compaction checkpoint

For a **bare single-CRDT instance**, the checkpoint is the output of
`CrdtMod:interpret_cog(Events, BaseState)` folded over every event in
the stable prefix since the last compaction. For a **catalogue
(projection-backed) instance** no separate fold is needed — the
projection, maintained eagerly on write through the cell kernel, *is*
the per-cell `interpret_cog` checkpoint; compaction only truncates
the MST.

The substrate stores the single-CRDT checkpoint via the
`bondy_oplog_compaction_checkpoint` behaviour (named to disambiguate
it from the *catalogue snapshot* used by bootstrap, below):

```mermaid
classDiagram
    class bondy_oplog_compaction_checkpoint {
      <<behaviour>>
      +init(InstanceId, Opts)
      +put_checkpoint(State, Watermark, Checkpoint)
      +get_checkpoint(State)
      +current_watermark(State)
      +close(State)
    }
    bondy_oplog_compaction_checkpoint <|-- bondy_oplog_compaction_checkpoint_ets
    bondy_oplog_compaction_checkpoint <|-- bondy_oplog_compaction_checkpoint_file
```

Two storage implementations ship; the default is **context-sensitive**
— file-backed when `storage_path` is set, ETS otherwise:

| Backend | Durability | Use |
|---|---|---|
| `bondy_oplog_compaction_checkpoint_ets` | in-memory | tests, ephemeral instances |
| `bondy_oplog_compaction_checkpoint_file` | tmp + datasync + rename + fsync-dir, at `<storage_path>/<shard>/<InstanceId>/checkpoint.etf` | production |

The file backend treats a decode failure as `{error, {corrupted, _}}`
and the instance **refuses to start** on a corrupted checkpoint —
loud, not silent.

The library policy is **one checkpoint per instance** — the most
recent one. Older checkpoints are not retained. The live MST plus the
latest checkpoint fully reconstruct the application state.

## The CRDT module (the COG interpreter)

The substrate is meaning-agnostic. Each instance is bound at start
time to a CRDT module implementing the `bondy_oplog_crdt` behaviour:

```mermaid
classDiagram
    class bondy_oplog_crdt {
      <<behaviour>>
      +causal_tier() tier
      +init() State
      +interpret_cog(Events, State) State'
      +query(Query, State) Result
    }
```

Three things to know about `interpret_cog/2`:

1. **It must be deterministic.** Same `(Events, State)` ⇒ same
   `State'`, on every replica. Non-determinism breaks Strong
   Eventual Consistency.
2. **It receives events in key (HLC) order.** Concurrent operations
   are co-batched; the interpreter resolves conflicts however its
   CRDT semantics require.
3. **It is called both for compaction and for live queries.** During
   compaction it folds the stable prefix into the snapshot; during
   reads `bondy_db` may also fold live events on top of the latest
   snapshot.

`interpret_cog` is the **COG interpreter** of the original Canteen
design — same role, same contract, same determinism invariant.

## The empty-MST steady state

Imagine three peers, a moderate write rate, and steady AE. Over time:

```mermaid
sequenceDiagram
    participant W as writer
    participant L as local MST
    participant AE as anti-entropy
    participant P as peers
    participant GC as gc_scheduler

    loop
        W->>L: append events
        AE->>P: sync sessions (record peer roots)
        GC->>L: compaction tick · frontier advances · MST shrinks
    end
    Note over L,GC: At quiescence:<br/>frontier = latest event key<br/>MST = ∅<br/>snapshot holds entire state
```

In a quiescent, fully-converged cluster the live MST is empty. New
appends populate it briefly; the next compaction tick (~1s) drains it
again. The cluster's *durable* persistent state is the snapshot —
the MST is a transient buffer for "events the cluster has not yet
all agreed on".

This is the precise opposite of the conventional log-replication
mental model. Practically, it means:

- **Old replicas don't carry old log.** Their disk footprint is
  bounded by snapshot size, not write history.
- **AE bandwidth is bounded.** Catching up is at worst the size of
  the live tail, never the size of history.
- **New replicas don't replay the world.** They get the snapshot
  and a small live tail (next section).

## Bootstrap: how a new peer joins

The complement to truncation is **snapshot transfer**. A replica
joining a cluster — or recovering from a long outage — has a stale
or empty MST. Plain anti-entropy would have to ship the entire
history; instead, the replica bootstraps from a peer:

```mermaid
sequenceDiagram
    autonumber
    participant New as new replica
    participant Peer as established peer
    participant Local as local instance

    New->>Peer: get_snapshot
    alt peer has snapshot
        Peer-->>New: {Watermark, Snapshot}
        New->>Local: load_snapshot(Watermark, Snapshot)
        Local->>Local: persist snapshot · truncate live MST below Watermark · advance HLC
    else peer has no snapshot
        Peer-->>New: no_snapshot
        Note over New,Peer: Cluster is small / fresh.<br/>Fall through to plain AE.
    end
    New->>Peer: get_root + pull_until_complete
    Peer-->>New: MST pages for live tail
```

The entrypoint is `bondy_oplog_sync_session:bootstrap/3`. After the
snapshot is installed, normal AE picks up at the watermark and
catches the live tail (typically a few seconds of events) — not the
whole history.

`bondy_oplog_instance:load_snapshot/3` enforces watermark monotonicity:

- If the local watermark is `undefined`, install the snapshot.
- If the peer's watermark is strictly greater, install and advance.
- Otherwise refuse with `{error, watermark_not_advancing}` and fall
  through to plain AE.

> **Terminology.** The sequence above is the **single-CRDT** (bare
> instance) bootstrap: what travels is the instance's *compaction
> checkpoint* (the wire request is still named `get_snapshot`).
> Catalogue instances — the `bondy_db` common case, where state is a
> per-cell projection rather than one CRDT state — use the
> **catalogue snapshot** protocol below. Same lifecycle gate, a
> different payload.

### Catalogue-mode bootstrap (multi-cell snapshot)

A catalogue's state is millions of cells, not one term, so the
transfer is a **cursor-paginated cell stream**
(`bondy_oplog_sync_session:bootstrap_catalogue/3`):

```mermaid
sequenceDiagram
    autonumber
    participant New as new replica
    participant Peer as established peer
    participant Local as local instance

    New->>Peer: get_catalogue_snapshot_init
    Peer-->>New: {Watermark, Cursor, first cell chunk}
    loop until cursor exhausted
        New->>Local: install_catalogue_batch(Cells, Mode)
        New->>Peer: {get_catalogue_snapshot_next, Cursor}
        Peer-->>New: next chunk
    end
    New->>Local: finalize (watermark advance + truncate + mark_live)
    Note over New,Peer: plain AE picks up the live tail
```

`install_catalogue_batch` has two modes: **replace** (fresh replica —
cells written as-is; uses the projection adapter's `head/3` fast path
when exported) and **merge** (recovering replica with surviving local
state — each incoming cell is merged through the CRDT). The
finalize step performs the same monotonic watermark advance and
`mark_live/1` ordering as the single-CRDT path.

## Bootstrap lifecycle: gating the applier

There is a subtle hazard in the bootstrap story above: between "instance
process starts" and "snapshot installed", what stops the applier from
draining live events onto an empty projection? Nothing, historically —
the applier would happily apply `+100` onto bottom state and converge
to wrong values. The failure is silent and affects **every** fold
strategy, not just counters.

The fix is an explicit, substrate-enforced two-state lifecycle per
instance:

```mermaid
stateDiagram-v2
    [*] --> pre_bootstrap
    pre_bootstrap --> live : bootstrap/3 success
    pre_bootstrap --> live : seed: true on first open
```

- **`pre_bootstrap`** — default state for a freshly-opened
  *persistent* instance. The WAL accepts writes from local appenders
  and sync sessions (the WAL is the durable buffer); the applier does
  not drain.
- **`live`** — applier drains normally.

The lifecycle bit lives in `<instance_dir>/lifecycle.live`, an empty
flag file. Presence ⇒ `live`. Absence ⇒ `pre_bootstrap`. The
transition is atomic via `file:rename/2` — no parsing, no checksum,
no version handshake. A boolean mirror in `atomics` keeps the
applier's hot-loop check syscall-free.

### Bootstrap completion ordering

`bondy_oplog_sync_session:bootstrap/3` performs three durable effects.
Order matters:

1. `load_snapshot` installs the snapshot and advances the watermark
   to `H_boot` (idempotent under crash-replay).
2. `mark_live/1` writes the durable flag file — **the marker that
   "everything before me succeeded."**
3. Plain AE picks up events past the new watermark.

`mark_live/1` MUST run last. A crash between (1) and (2) leaves no
flag file; on restart the lifecycle goes back to `pre_bootstrap` and
the operator re-runs `bootstrap/3`, which idempotently re-installs
the snapshot.

### Genesis: `seed: true`

The first peer in a fresh cluster has nothing to bootstrap from. To
declare a genesis peer, pass `seed => true` in the instance opts.
On first open this flips the lifecycle directly to `live` and (when
`storage_path` is configured) writes the flag file so subsequent
restarts also see `live` without needing the opt again.

### Ephemeral instances

Instances without `storage_path` cannot persist a flag file. For
those the lifecycle is in-memory only and defaults to `live` —
there is no persistent state to bootstrap from, and tests that
don't think about lifecycle work unchanged. `seed: false` is still
honoured for tests that want to exercise the gate.

### Summary

| Configuration | Initial state |
|---|---|
| `lifecycle.live` exists on disk | `live` |
| `seed: true` in opts | `live` (writes flag file if persistent) |
| No `storage_path` (ephemeral) | `live` |
| Persistent, no flag file, `seed: false` | `pre_bootstrap` |

Modules: `bondy_oplog_bootstrap_lifecycle.erl` (the durable bit +
atomic mirror), `bondy_oplog_instance.erl` (`mark_live/1`,
`lifecycle_state/1`), `bondy_oplog_applier.erl` (gate in
`drain_loop/1`).

## Auto-bootstrap and dispatch policy

The lifecycle gate above guarantees correctness — the applier does
not drain until the snapshot is in place. But the gate alone is
inert: someone has to call `bondy_oplog_sync_session:bootstrap/3`
to flip a fresh persistent replica from `pre_bootstrap` to `live`.
Before this layer, that someone was application code; consumers
who forgot to call `bootstrap/3` ended up with replicas that
silently never drained.

`bondy_oplog_sync_scheduler` closes the loop. The default tick
inspects each running instance's lifecycle and routes
accordingly:

```mermaid
flowchart TD
    Tick[Scheduler tick] --> Lifecycle{lifecycle_state}
    Lifecycle -->|pre_bootstrap| Backoff{In backoff?}
    Lifecycle -->|live| FanOut[Fan out AE sync per peer]
    Lifecycle -->|undefined| NoOp[No-op, retry next tick]

    Backoff -->|yes| Deferred[Skip · emit telemetry]
    Backoff -->|no| Cap{Under in-flight cap?}

    Cap -->|no| Capped[Skip · emit telemetry]
    Cap -->|yes| Pick[Pick peer via strategy]
    Pick --> Spawn[Spawn bootstrap session]
    Spawn --> Monitor[Monitor pid · track in-flight]
    Monitor --> DOWN{Session exits}
    DOWN -->|normal| Clear[Clear backoff entry]
    DOWN -->|other| Bump[Bump fail count · schedule retry]
```

A `pre_bootstrap` instance is dispatched to **exactly one** peer
per tick. Bootstrap ships a full projection (catalogue mode) or a
full MST snapshot (single-CRDT mode), so multi-peer dispatch would
duplicate I/O without improving correctness. A `live` instance
fans out one async pull-direction sync session per peer — the
historical AE behaviour, unchanged.

### Four knobs in front of one decision

| Knob | Default | Purpose |
|---|---|---|
| `bootstrap_peer_strategy` | `first` | Which peer is selected when dispatching: `first` (deterministic head of the peer list), `random` (uniform), or `round_robin` (per-instance index). |
| `max_inflight_bootstraps` | `4` | Global cap on parallel bootstrap sessions. Over-cap dispatches are skipped; the instance stays `pre_bootstrap` and retries on the next tick as in-flight sessions drain. `0` disables dispatch — operator escape hatch. |
| `bootstrap_retry_base_ms` | `500` | Initial backoff after a non-`normal` session exit. Doubles on each consecutive failure. |
| `bootstrap_retry_max_ms` | `30000` | Upper bound on the exponential backoff. |
| `bootstrap_retry_jitter` | `true` | Multiplies the wait by uniform `[0.5, 1.5]` to spread retries when many instances fail in the same window. |

Each knob has a runtime setter on `bondy_oplog_sync_scheduler` (no
restart, no recompile). The current configuration is visible via
`bondy_oplog_sync_scheduler:info/0`.

### Gate ordering

The two skip gates run in this order on every `pre_bootstrap`
dispatch:

1. **Backoff** — if `now < NextRetryMs` for this instance, skip.
   An instance in backoff does NOT consume an in-flight cap slot
   (so other instances still dispatch).
2. **Cap** — if `inflight_count >= max_inflight_bootstraps`,
   skip. Within a single tick, the in-flight count is monotonic
   (DOWN messages queue but are not processed until the tick
   completes), so the cap is deterministic per-tick.

### Lifecycle state machine extended

```mermaid
stateDiagram-v2
    [*] --> pre_bootstrap
    pre_bootstrap --> dispatched : tick · under cap · not in backoff
    dispatched --> live : bootstrap success (DOWN normal)
    dispatched --> pre_bootstrap : bootstrap failure (DOWN other)
    pre_bootstrap --> live : seed: true on first open
```

A failed bootstrap session keeps the instance in `pre_bootstrap`;
the next tick re-evaluates the lifecycle and (assuming backoff has
elapsed) re-dispatches. This is the structural self-healing
property — there is no separate retry machinery, just the standard
tick + lifecycle re-evaluation.

### Telemetry surface

Each gate emits a dedicated event so operators can monitor
retry pressure without sampling logs:

| Event | When | Meta |
|---|---|---|
| `[bondy_oplog, sync_scheduler, dispatch_bootstrap]` | A session was spawned. | `instance_id`, `peer`, `mode` (`catalogue \| single_crdt`), `strategy` |
| `[bondy_oplog, sync_scheduler, bootstrap_session, started]` | Session pid was added to the in-flight set. | `instance_id`, `pid` |
| `[bondy_oplog, sync_scheduler, bootstrap_session, ended]` | Session pid exited (any reason). | `instance_id`, `pid`, `reason` |
| `[bondy_oplog, sync_scheduler, bootstrap_capped]` | Dispatch skipped because in-flight cap was hit. | `instance_id` |
| `[bondy_oplog, sync_scheduler, bootstrap_backoff_deferred]` | Dispatch skipped because `now < NextRetryMs`. | `instance_id` |
| `[bondy_oplog, sync_scheduler, bootstrap_retry_scheduled]` | A failure bumped the fail count + wrote a new retry time. | `instance_id`, `wait_ms`, `fail_count` |

### Operator playbook

| Symptom | Action |
|---|---|
| New cluster cold-start storms peer network/disk. | Lower `max_inflight_bootstraps`. Default `4` is conservative; very large clusters may want lower. |
| One specific peer is hot under bootstrap load. | Switch `bootstrap_peer_strategy` to `round_robin` or `random`. |
| A specific replica keeps failing to bootstrap. | Watch `bootstrap_retry_scheduled` telemetry for that `instance_id` — `fail_count` rising past 5+ means the peer-pool is genuinely unreachable for this replica, not a flake. |
| Need to quiesce bootstrap traffic without disabling AE. | `set_max_inflight_bootstraps(0)` — sessions in flight drain naturally; no new ones are spawned. |
| Tests need deterministic retry timing. | `set_bootstrap_retry_base_ms(0)` + `set_bootstrap_retry_jitter(false)`. |

## The independent-watermark reconciliation rule

Two replicas may compact at different rates. Say Replica X has
truncated up to `e6` while Replica Y has only truncated up to `e4`.
When AE pulls Y's pages into X, X must not "un-truncate" by accepting
events it already folded into its snapshot. The mechanism is in
`bondy_oplog_instance.erl:do_handle_call({integrate_peer_root, _})`:

```erlang
%% bondy_oplog_instance.erl — do_handle_call({integrate_peer_root, _})
MST1 = bondy_mst:merge(MST0, MST0, PeerRoot),
MST2 = case State#state.watermark of
           undefined -> MST1;
           W         -> truncate_below_or_equal(MST1, W)
       end,
```

The peer's pages are merged in, then `truncate_below_or_equal/2`
re-runs against the local watermark, dropping any keys X has already
compacted away. The local-append side uses the same idea via
`below_or_equal_watermark/2` — appended or peer-supplied events whose
key is ≤ the local watermark are rejected at the door.

```mermaid
flowchart LR
    X["X · watermark=e6 · MST=e7..e10"]
    Y["Y · watermark=e4 · MST=e5..e10"]
    AE["integrate_peer_root(PeerRoot=Y.root)"]
    MERGE["bondy_mst:merge → MST has e5..e10"]
    RETRUNC["truncate_below_or_equal(MST, e6)"]
    AFTER["X.MST = e7..e10 (unchanged)"]

    X --> AE
    Y --> AE
    AE --> MERGE --> RETRUNC --> AFTER
```

Two replicas with different watermarks are **not divergent** — they
agree on application state. One is just slightly more compact. On Y's
next GC tick, Y reads X's advanced root via `bondy_oplog_peer_state`
and computes a new frontier that absorbs `e5..e6` into its own
snapshot.

## What can go wrong (and what catches it)

| Hazard | Catch |
|---|---|
| Silent peer pins the watermark forever. | `peer_timeout_ms` filters stale peer entries (default 30s); the frontier ignores them. |
| Replica truncates a prefix the application still cares about. | `interpret_cog` must consume the prefix into the snapshot first. The compaction cycle is `frontier → events → interpret_cog → snapshot → truncate` — the snapshot is durable *before* the MST mutation. |
| Two compactions race. | One-at-a-time guard in `bondy_oplog_instance`: a second `compact` request while one is in flight replies `{ok, no_change}`. |
| Peer ships events the local replica has already truncated. | Integrate path drops events with `key ≤ watermark`. |
| Bootstrap snapshot is older than local. | `load_snapshot/3` refuses with `watermark_not_advancing` and falls through to plain AE. |
| `interpret_cog/2` is non-deterministic. | Convergence breaks silently. The behaviour documentation flags this as the invariant; PropEr suites for each CRDT verify it (`bondy_mst_crdt_SUITE.erl`). |
| Fresh persistent replica never flips to `live` because nobody calls `bootstrap/3`. | The default scheduler dispatch is lifecycle-aware: `pre_bootstrap` instances are auto-bootstrapped from the first available peer on the next tick. |
| Cluster cold-start fires N parallel snapshot transfers and saturates the peer pool. | `max_inflight_bootstraps` cap (default `4`) gates concurrent sessions. Over-cap dispatches are deferred to the next tick as in-flight sessions drain. |
| A replica with an unreachable peer-pool retries every 500 ms forever. | Per-instance exponential backoff (`500 ms → 30 s` ceiling, optional ±50 % jitter) drops the steady-state retry rate to ~1/30 Hz after a few failures. |
| Multiple replicas hammer the same first peer. | `bootstrap_peer_strategy` (`first` \| `random` \| `round_robin`) — switch to `round_robin` or `random` to spread load. |

## Tests that pin this down

The COG truncation pipeline is the most-tested part of the substrate
because everything else relies on it. The relevant suites:

- `test/bondy_oplog_compaction_test.erl` — frontier computation,
  watermark advance, idempotency, no-change cases, MST shrinkage.
- `test/bondy_oplog_gc_scheduler_test.erl` — tick cadence, semaphore
  cap, set_interval/set_trigger races, per-instance isolation.
- `test/bondy_oplog_bootstrap_test.erl` — snapshot transfer,
  watermark monotonicity, no-snapshot fallback, post-bootstrap AE.
- `test/bondy_mst_crdt_SUITE.erl` — end-to-end determinism + Strong
  Eventual Consistency for the test CRDT (`bondy_mst_test_crdt_server`).
- `test/bondy_oplog_sync_scheduler_bootstrap_test.erl` — lifecycle-
  aware dispatch (pre_bootstrap routes to bootstrap; live fans out).
- `test/bondy_oplog_sync_scheduler_peer_strategy_test.erl` —
  `first` / `random` / `round_robin` selection.
- `test/bondy_oplog_sync_scheduler_cap_test.erl` — in-flight cap
  honoured within a tick; DOWN cleanup; `0` escape hatch.
- `test/bondy_oplog_sync_scheduler_backoff_test.erl` —
  exponential progression, normal-exit clear, deferred telemetry.

## Things to keep in mind

- **The MST is a *window*, not a log.** Bounded by network
  propagation, not by app lifetime.
- **Truncation is real deletion.** Pages are freed; the next
  pack-store GC reclaims the disk.
- **Convergence implies emptiness.** At full quiescence, the live
  MST is `∅`; the snapshot holds the entire state.
- **The CRDT module is the only meaning.** The substrate truncates;
  `interpret_cog/2` decides what the truncated prefix folds into.
- **New replicas don't replay the world.** Snapshot bootstrap +
  small live tail catch-up.

## Pointers

Implementation:

- `bondy_oplog_compaction.erl` — orchestrator (reads peer roots,
  delegates to instance).
- `bondy_oplog_gc_scheduler.erl` — periodic tick, semaphore cap,
  per-instance worker.
- `bondy_oplog_instance.erl`:
    - `compute_frontier_for/2` — frontier as longest common prefix.
    - `do_compact_sync/2` + `run_compaction/8` — the cycle, run
      synchronously in the instance gen_server.
    - `commit_compaction/3` — atomic truncate + watermark + HLC bump.
    - `truncate_below_or_equal/2` — drops keys ≤ watermark.
    - `do_load_snapshot/3` + `apply_loaded_snapshot/3` — bootstrap
      install with monotonicity guard.
    - `do_handle_call({integrate_peer_root, _}, _, _)` — merge +
      re-truncate on AE integration.
    - `below_or_equal_watermark/2` — append-side filter.
- `bondy_oplog_peer_state.erl` — per-(peer, instance) root cache;
  `get_instance_peer_states/1` filters by `peer_timeout_ms` (default
  30 000 ms).
- `bondy_oplog_compaction_checkpoint.erl` + `_ets.erl` + `_file.erl` —
  one-checkpoint-per-instance behaviour and implementations
  (context-sensitive default: file when `storage_path` is set).
- `bondy_oplog_sync_session.erl:bootstrap/3` — fetch peer snapshot
  then pull live tail; falls back to plain AE on `no_snapshot`.
  Calls `mark_live/1` *after* the snapshot install succeeds — the
  durable barrier that flips the lifecycle.
- `bondy_oplog_sync_session.erl:bootstrap_catalogue/3` — the
  catalogue (multi-cell) bootstrap: cursor-paginated cell stream
  (`get_catalogue_snapshot_init` / `..._next`),
  `install_catalogue_batch` replace/merge modes, finalize +
  `mark_live/1` last.
- `bondy_oplog_bootstrap_lifecycle.erl` — the `<instance_dir>/lifecycle.live`
  flag file + atomics mirror; `open/2`, `is_live/1`, `mark_live/1`.
- `bondy_oplog_sync_scheduler.erl`:
    - `default_dispatch/2` — lifecycle-aware routing
      (`pre_bootstrap` → bootstrap; `live` → fan-out AE).
    - `maybe_dispatch_bootstrap/2` — backoff gate, then cap gate.
    - `pick_bootstrap_peer/3` — `first` / `random` /
      `round_robin` strategies. RR counter lives in the
      `bondy_oplog_sync_scheduler_rr` named ETS table.
    - `track_inflight/2` — monitors the spawned session pid and
      records `{Pid, InstanceId}` in the
      `bondy_oplog_sync_scheduler_inflight` named ETS table.
    - `update_backoff/2` — DOWN-reason classifier; clears the
      entry on `normal`, bumps fail-count + writes a new
      `NextRetryMs` otherwise. State lives in the
      `bondy_oplog_sync_scheduler_backoff` named ETS table.
    - `set_bootstrap_peer_strategy/1`,
      `set_max_inflight_bootstraps/1`,
      `set_bootstrap_retry_base_ms/1`,
      `set_bootstrap_retry_max_ms/1`,
      `set_bootstrap_retry_jitter/1` — runtime setters; all
      write through to `bondy_mst` app env so the choice
      survives a scheduler restart within the same VM lifetime.
    - `info/0` — current configuration including
      `current_inflight_bootstraps`.
- `bondy_oplog_sync_session.erl`:
    - `start_bootstrap/3` — async single-CRDT bootstrap spawner.
    - `start_bootstrap_catalogue/3` — async catalogue bootstrap
      spawner. Both return `{ok, Pid}`; the scheduler monitors
      the pid and translates its exit reason into the backoff
      decision.
- `bondy_oplog_crdt.erl` — `interpret_cog/2` callback.
- `bondy_mst.erl:truncate/2` (`truncate_at` / `truncate_scan` /
  `rebuild_truncated`) — the structural prefix-truncate; `delete/2`
  + `merge_subtrees/3` for single-key structural deletion.

The dispatch-policy chain (auto-bootstrap routing, peer strategy,
in-flight cap, retry backoff) is documented in the auto-bootstrap
section above; the scheduler module docs carry the option tables.

Tests:

- `test/bondy_oplog_compaction_test.erl`
- `test/bondy_oplog_gc_scheduler_test.erl`
- `test/bondy_oplog_bootstrap_test.erl`
- `test/bondy_oplog_bootstrap_lifecycle_test.erl` — the durable flag
  file + `mark_live` ordering, in isolation.
- `test/bondy_oplog_bootstrap_lifecycle_e2e_test.erl` — applier-gate
  integration: appends sit in the overlay until `mark_live` flips the
  lifecycle.
- `test/bondy_mst_crdt_SUITE.erl` (+ `bondy_mst_test_crdt_server.erl`)

Background / origin:

- Preston McCrary, *Canteen* — UC Berkeley EECS-2022-160 (source of
  the COG / interpret_cog vocabulary). The local code does not depend
  on the paper.
