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
just bench-folds        # CRDT fold strategies
just bench-db           # bondy_mst_db substrate
just bench-oplog        # oplog instance end-to-end
just bench-wal          # WAL append / fsync / batch
just bench-one mst_put  # single script

# Concurrency (sustained-load, multi-worker)
just bench-concurrency 10        # full suite, 10s per scenario
just bench-concurrency-oplog 10  # oplog only
just bench-concurrency-db 10     # mst_db only
just bench-concurrency-wal 8     # WAL only

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
| `benchmarks/folds.exs`       | CRDT fold strategies — `apply_event/2`, `merge_states/2`, codec for `lww_register`, `or_set`, `presence_basic`, `strict_register` |

### Substrate end-to-end

| Script                       | What it measures                                 |
|------------------------------|--------------------------------------------------|
| `benchmarks/mst_db.exs`      | `bondy_mst_db` `read/3`, `read_batch/2`, `range/4`, `ensure_fresh/2` across cache hit-rate sweep (cold 0%, warm 50%, hot 99%) |
| `benchmarks/oplog.exs`       | `bondy_oplog` `append`, `append_many`, `append + await_apply`, `get`, `size`, `root_hash`, `fold_range` |
| `benchmarks/wal.exs`         | `bondy_oplog_wal` `append`, `append_batch`, `sync`, `info`, `durable_position` — per-write vs batched fsync |

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

Two structural properties shape what these numbers can and can't tell
you:

1. **Writes serialise on the instance gen_server.** Every `append/2`
   call on a given instance goes through one process: the WAL append
   and the overlay insert happen inline before reply. Multi-writer
   throughput on a single instance is therefore bounded by the slower
   of (a) the fsync rate in `per_write` mode (~5 k events/s on
   commodity NVMe) and (b) the gen_server's serial processing rate.
2. **Reads are lock-free.** `get/2` and `fold_range/5` go straight to
   the registry-published MST handle and the overlay ETS table — no
   gen_server hop. Per-process reads scale with cores.

This produces a real asymmetry under mixed load on a single hot
instance:

| Scenario                | Writer ips | Reader ips |
|-------------------------|------------|------------|
| `oplog_writers_8`       | ~4,100     | —          |
| `oplog_readers_16`      | —          | ~2.9 M     |
| `oplog_mixed_8w_8r`     | **~400**   | ~1.7 M     |

The mixed-load writer drop is **not** a bug: 8 CPU-bound readers
soak up scheduler time that the instance gen_server (and the WAL it
calls) would otherwise use to ack writers. Mitigations available
to consumers:

- **Use `batched` fsync** for high-churn namespaces — about 75 %
  faster than `per_write` in mixed mode, with bounded durability
  windows (see `bondy_oplog_wal` moduledoc).
- **Shard hot instances**: the gen_server bottleneck is per-instance,
  so separate `bondy_oplog` instances do not contend with each
  other.
- **Avoid hot single-instance reader floods** if writer latency
  matters — the lock-free read path will happily absorb millions of
  reads/sec, but those reductions come out of the same scheduler
  budget the writer needs.

Inside the code, the writer-cliff investigation drove three changes
worth knowing about:

- `bondy_oplog_cache_ets` opens its table with
  `write_concurrency: true` + `decentralized_counters: true`.
- `bondy_oplog_instance` / `_wal` / `_applier` set
  `message_queue_data => off_heap` in `init/1` so mailbox depth does
  not trigger GCs on the main heap.
- `bondy_oplog_instance:maybe_publish/2` compares only the
  *published* fields (`published_fingerprint/1`) — appends mutate
  per-process counters that no reader sees, so the registry write is
  skipped.
