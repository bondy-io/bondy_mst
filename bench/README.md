# bondy_mst — Benchmarks

Performance benchmarks for `bondy_mst` driven from an Elixir Mix
project so we can lean on [Benchee](https://hex.pm/packages/benchee)
+ [benchee_html](https://hex.pm/packages/benchee_html) for statistics
and graphical reports (latency percentiles p50/p75/p90/p95/p99,
throughput, memory usage, reduction count).

## Quick start

From the project root:

```sh
just bench              # full suite (~10–20 min)
just bench-quick        # smoke run (~10 s)
just bench-mst          # MST primitives only
just bench-primitives   # HLC / cell-frame / overlay
just bench-folds        # native CRDT primitives (apply_op, interpret_cog)
just bench-db           # bondy_mst_db substrate
just bench-oplog        # oplog instance end-to-end
just bench-wal          # WAL append / fsync / batch
just bench-one mst_put  # single script

# Concurrency (sustained-load, multi-worker)
just bench-concurrency 10        # full suite, 10s per scenario
just bench-concurrency-oplog 10  # oplog only
just bench-concurrency-db 10     # mst_db only
just bench-concurrency-wal 8     # WAL only

# End-to-end pipeline (multi-shard substrate + ECharts dashboard)
just bench-e2e 10                # write_only / read_only / mixed_70r_30w

just bench-open         # open the most recent HTML report
just bench-clean        # wipe _output / _build / deps
```

Outputs land in `bench/_output/<scenario>/index.html`. The Mix
project under `bench/` reuses the rebar3-built beams from
`_build/default/lib` at runtime, so it never duplicates compilation.

## Scripts

### Pure MST (no oplog substrate)

| Script                       | What it measures                                 |
|------------------------------|--------------------------------------------------|
| `benchmarks/mst_put.exs`     | Single `put/3` against a pre-built tree (1k/10k/100k, map + ets store) |
| `benchmarks/mst_get.exs`     | `get/2` hit + miss against pre-built trees       |
| `benchmarks/mst_fold.exs`    | Full-tree `to_list/1` and `fold/3`               |
| `benchmarks/mst_merge.exs`   | CRDT `merge/2` — disjoint + identical pairs      |
| `benchmarks/mst_bulk_put.exs`| Building a tree from scratch (throughput)        |

### Substrate primitives

| Script                       | What it measures                                 |
|------------------------------|--------------------------------------------------|
| `benchmarks/primitives.exs`  | `bondy_oplog_hlc` (now/peek/update/encode/decode), `bondy_oplog_cell_frame` (encode/decode at 64B/1KB/64KB), `bondy_oplog_db_overlay` (insert/events_for/range) |
| `benchmarks/folds.exs`       | Native op-based CRDT primitives — `apply_op/3`, `interpret_cog/2` (the SEC group fold), state codec for `lww_register`, `g_set`, `pn_counter`, `aw_map` (PR-Z; the fold family is retired) |

### Substrate end-to-end

| Script                       | What it measures                                 |
|------------------------------|--------------------------------------------------|
| `benchmarks/mst_db.exs`      | `bondy_mst_db` `read/3`, `read_batch/2`, `range/4`, `ensure_fresh/2` across cache hit-rate sweep (cold 0%, warm 50%, hot 99%) |
| `benchmarks/oplog.exs`       | `bondy_oplog` `append`, `append_many`, `append + await_apply`, `get`, `size`, `root_hash`, `fold_range` |
| `benchmarks/wal.exs`         | `bondy_oplog_wal` `append`, `append_batch`, `sync`, `info`, `durable_position` — per-write vs batched fsync |

### End-to-end pipeline (ECharts dashboard, not Benchee)

The `Bench.E2E` harness provisions a multi-shard `bondy_db_core`
substrate (per-shard projection + cache + overlay) and starts one
`bondy_oplog` instance per shard with the substrate wired as the
applier's `cell_apply_target`. Writes flow through the full pipeline
(`append → WAL → applier → projection`); reads go through
`bondy_db_core.read/4` (cache-fast, projection on miss).

Per-stage telemetry is collected via handlers on
`[bondy_oplog, wal, append|fsync]`, `[bondy_oplog, applier, *]` and
`[bondy_db_core, read|range|range_all]`. Each scenario emits JSON +
a self-contained ECharts dashboard with:

- **Pipeline Sankey** — event flow between substrate sinks.
- **Latency sundial** — sunburst over (pipeline → stage → percentile).
- **Per-shard heatmap** — distribution by (stage × shard).
- **Latency rose** — polar bars of p99 per stage (log radial axis).
- **Throughput** — workload ops/sec and per-stage event count.
- **Histograms** — log-bucketed latency distribution per stage and
  workload.

Reports land under `bench/_output/e2e_pipeline/<name>/index.html`
with a top-level `index.html` linking every scenario.

| Script                              | What it measures                            |
|-------------------------------------|---------------------------------------------|
| `benchmarks/e2e_pipeline.exs`       | `write_only_w<W>`, `read_only_w<R>` and `mixed_70r_30w_w<W+R>` over a 4-shard substrate with N pre-populated keys |

Set `DURATION_S`, `WARMUP_MS`, `SHARDS`, `PREPOPULATE`, `WRITERS`,
`READERS` to tune the run.

#### Ephemeral vs durable (leveled) tables

`BACKENDS` accepts whole-stack **profiles**, not just a projection
swap, so you can measure an ephemeral (ets-backed, in-memory) table
against the fully-durable leveled-backed stack:

| Profile     | Projection | MST snapshot | WAL fsync   |
|-------------|------------|--------------|-------------|
| `ephemeral` | ets (RAM)  | ets (RAM)    | `batched`   |
| `durable`   | leveled    | pack-store   | `per_write` |

```bash
just bench-ephemeral-vs-leveled            # local, all scenarios
just bench-fly-8x-ephemeral-vs-leveled 120 8   # perf-8x, 120s × 8 shards
```

Reports list `<scenario>_ephemeral` vs `<scenario>_durable` side by
side. The write scenarios are the headline: ephemeral touches no disk
and fsyncs rarely (batched), while durable pays a `per_write` fsync per
event plus the leveled journal and pack-store MST. (Legacy `ets` /
`leveled` values stay projection-only and honour `MST_BACKEND` /
`WAL_FSYNC`, so existing recipes are unchanged.)

**Op-based CRDT model + throughput targets.** Since PR-Z every table is a
native `bondy_oplog_crdt` (the fold family is gone); the e2e pipeline
writes through the cell kernel's `apply_op` and is registered with
`crdt_module` (default `bondy_oplog_crdt_lww_register`; override with
`CRDT=g_set` / `pn_counter` / `aw_map`). The console summary for each
`write_only` run prints **applier ops/s per instance** and a PASS/BELOW
verdict against the per-instance write-throughput targets:

| Stack       | Target (writes/s/instance) |
|-------------|----------------------------|
| `durable`   | **4,000** (leveled)        |
| `ephemeral` | **20,000** (ets)           |

Per-instance throughput is the end-to-end applier rate divided by the
shard count, so the head-to-head answers "did the op-based model gain or
lose throughput vs the targets?" directly. Note macOS is ~8× slower on the
write path than Linux (see `_design/latest` QA #14); the targets are
validated on the Fly perf-8x Linux substrate
(`just bench-fly-8x-ephemeral-vs-leveled`).

> **Durable instances need `seed: true`.** A durable MST backend
> (`storage_path` set) gates the applier on the bootstrap lifecycle —
> it won't drain the WAL until it bootstraps from a peer. A
> single-process bench has no cluster, so without `seed: true` each
> shard hangs in `pre_bootstrap` and `await_apply` times out (this is
> *not* a pack-store defect). The `durable` profile sets it; any new
> manual-substrate bench using a durable backend must too.

### Concurrency (sustained-load harness, not Benchee)

The `Bench.Concurrency` harness drives N worker processes through a
per-workload op fn for a fixed wall-clock duration, captures per-op
latency in a `:counters`-backed log histogram, and writes a custom
JSON + self-contained HTML report (with Chart.js charts) to
`bench/_output/concurrency/<scenario>/`.

| Script                              | What it measures                            |
|-------------------------------------|---------------------------------------------|
| `benchmarks/concurrency_oplog.exs`  | N writers / N readers / mixed against one `bondy_oplog` instance — gen_server contention curve |
| `benchmarks/concurrency_mst_db.exs` | N readers across cache hit-rate sweep + N readers + M `write_through` callers — validates lock-free read claim |
| `benchmarks/concurrency_wal.exs`    | N writers per_write vs batched fsync — shows whether batched throughput holds under contention |
| `benchmarks/concurrency_smoke.exs`  | Trivial 3s scenario used to validate the harness wiring |

Set `DURATION_S=<seconds>` (or pass to the just recipe) to override
the per-scenario run length. Default is 10s for oplog/mst_db and 8s
for WAL.

### Runners

| Script                       | What it does                                     |
|------------------------------|--------------------------------------------------|
| `benchmarks/quick.exs`       | Cheap smoke run used by `just bench-quick`       |
| `benchmarks/all.exs`         | Entry point used by `just bench` — runs every Benchee script in sequence |

## Output

Each script writes to a dedicated subdirectory:

```
bench/_output/<name>/
  index.html                           — landing page (comparison + charts)
  index_<input>_<scenario>.html        — per-scenario page
  data.json                            — raw measurements (benchee_json)
```

Benchee statistics in scope:

- ips (iterations per second) and average / median / min / max
- percentiles: **p50, p75, p90, p95, p99**
- standard deviation
- memory usage per call
- reduction count (BEAM scheduler load proxy)

CPU utilisation isn't sampled by Benchee directly; latency × ips ×
parallelism is the substitute, with reduction count as the BEAM-side
load metric.

## Notes

- `benchmarks/wal.exs` writes to `/tmp/bondy_mst_bench_wal/` and
  cleans up afterwards. Numbers are highly disk-dependent; compare
  per-write fsync vs batched fsync to understand the floor.
- `benchmarks/mst_db.exs` ships its own in-memory projection adapter
  (`lib/bench/projection_ets.ex`) so the bench does not require the
  rebar3 test profile to be built.

## Reading the concurrency results

Two structural properties shape what these numbers mean:

1. **Reads are lock-free.** `get/2` and `fold_range/5` go straight to
   the registry-published MST handle and the overlay ETS table — no
   gen_server hop. Per-process reads scale with cores.
2. **Writes take the lock-free fast path when the validator is
   stateless** (the default `bondy_oplog_validator_trust` advertises
   `is_stateless/0 -> true`). The caller builds the event, calls the
   WAL gen_server directly, and stages the overlay row inline; the
   instance gen_server is no longer on the hot write path. Only
   stateful validators (e.g. `bondy_oplog_validator_crypto`) route
   through the instance gen_server. The WAL gen_server remains the
   one serialisation point for the WAL file itself.

For the default validator, multi-writer throughput on a single
instance is bounded by the slower of (a) the fsync rate in
`per_write` mode (~5 k events/s on commodity NVMe) and (b) the WAL
gen_server's serial processing rate (millions of events/s in
`batched` mode).

### Reference numbers (8-writer, 14-scheduler M-series Mac, single
instance)

| Scenario                | `per_write` writer ips | `batched` writer ips |
|-------------------------|------------------------|----------------------|
| `oplog_writers_8`       | ~4,200                 | ~4.7 M               |
| `oplog_mixed_8w_8r`     | ~500                   | ~4.4 M               |

In `per_write` mode the bottleneck is the device fsync rate — both
the standalone and mixed cases are bounded by it (mixed is lower
because readers compete for the same schedulers). In `batched`
mode the fast path delivers near-linear scaling: with 8 readers
running concurrently, writers stay above 4 M/s and readers above
1 M/s.

### When the mixed-load floor still bites

- `per_write` instances cannot exceed the device fsync rate. Use
  `per_write` only for namespaces that must be on disk before
  `append/2` returns; everything else should use `batched` plus
  `await_durable/3` for explicit durability checkpoints.
- Stateful validators (`bondy_oplog_validator_crypto` and any
  caller-supplied validator that doesn't export
  `is_stateless/0 -> true`) bypass the fast path and still serialise
  through the instance gen_server. Signing is also CPU-heavy
  (~80 µs/append for Ed25519), so even with batched WAL the
  per-instance write rate is bounded by signing throughput on a
  single core.
- Shard hot instances when a single causal log isn't required —
  the WAL gen_server is per-instance, so separate `bondy_oplog`
  instances do not contend with each other.

### Code changes worth knowing about

- `bondy_oplog_cache_ets` opens its table with
  `write_concurrency: true` + `decentralized_counters: true`.
- `bondy_oplog_instance` / `_wal` / `_applier` set
  `message_queue_data => off_heap` in `init/1` so mailbox depth
  does not trigger GCs on the main heap.
- `bondy_oplog_instance:maybe_publish/2` compares only the
  *published* fields (`published_fingerprint/1`) — appends mutate
  per-process counters that no reader sees, so the registry write
  is skipped.
- `bondy_oplog_validator` has an optional `is_stateless/0`
  callback. Returning `true` opts the validator into the
  `append_fast/3` path.
- `bondy_oplog_instance:append_fast/3` is the lock-free append.
  `bondy_oplog:append/2,3` routes through it unconditionally;
  the implementation falls back to the gen_server when the
  registry's `fast_path` bundle is `undefined`.
- The overlay-row count + byte estimate live in an `atomics`
  array (`overlay_counters`) so the fast path's backpressure
  check is a couple of atomic reads.
