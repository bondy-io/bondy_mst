# bondy_mst — task runner (https://just.systems)

set shell := ["bash", "-cu"]

bench_dir := justfile_directory() / "bench"
output_dir := bench_dir / "_output"

# Show every recipe.
default:
    @just --list

# Run the full benchmark suite. Compiles bondy_mst with rebar3,
# fetches Elixir deps, then runs every script under bench/benchmarks.
# Reports land in bench/_output/<name>/index.html.
bench:
    rebar3 compile
    cd {{bench_dir}} && mix deps.get
    cd {{bench_dir}} && mix run benchmarks/all.exs
    @echo ""
    @echo "Reports:"
    @ls -1 {{output_dir}} 2>/dev/null | sed "s|^|  {{output_dir}}/|"

# Fast smoke benchmark — 1s time / 1s warmup, single tree size.
# Use to validate the harness without burning minutes.
bench-quick:
    rebar3 compile
    cd {{bench_dir}} && mix deps.get
    cd {{bench_dir}} && mix run benchmarks/quick.exs

# Run a single benchmark script by name (without the .exs).
#   just bench-one mst_put
bench-one name:
    rebar3 compile
    cd {{bench_dir}} && mix deps.get
    cd {{bench_dir}} && mix run benchmarks/{{name}}.exs

# Run only the MST primitive benchmarks (put/get/fold/merge/bulk).
bench-mst:
    rebar3 compile
    cd {{bench_dir}} && mix deps.get
    cd {{bench_dir}} && mix run benchmarks/mst_put.exs
    cd {{bench_dir}} && mix run benchmarks/mst_get.exs
    cd {{bench_dir}} && mix run benchmarks/mst_fold.exs
    cd {{bench_dir}} && mix run benchmarks/mst_merge.exs
    cd {{bench_dir}} && mix run benchmarks/mst_bulk_put.exs

# Run the substrate read-path primitive benchmarks (HLC, codec, overlay).
bench-primitives:
    rebar3 compile
    cd {{bench_dir}} && mix deps.get
    cd {{bench_dir}} && mix run benchmarks/primitives.exs

# Run the CRDT fold benchmarks (apply_event, merge_states, codec).
bench-folds:
    rebar3 compile
    cd {{bench_dir}} && mix deps.get
    cd {{bench_dir}} && mix run benchmarks/folds.exs

# Run the bondy_mst_db substrate read-path benchmarks across cache hit rates.
bench-db:
    rebar3 compile
    cd {{bench_dir}} && mix deps.get
    cd {{bench_dir}} && mix run benchmarks/mst_db.exs

# Run the bondy_oplog instance end-to-end benchmarks.
bench-oplog:
    rebar3 compile
    cd {{bench_dir}} && mix deps.get
    cd {{bench_dir}} && mix run benchmarks/oplog.exs

# Run the WAL benchmarks. Disk-dependent — writes to /tmp/bondy_mst_bench_wal.
bench-wal:
    rebar3 compile
    cd {{bench_dir}} && mix deps.get
    cd {{bench_dir}} && mix run benchmarks/wal.exs

# Concurrency: full suite. Pass DURATION_S=N to override per-scenario seconds.
bench-concurrency duration="10":
    rebar3 compile
    cd {{bench_dir}} && mix deps.get
    DURATION_S={{duration}} cd {{bench_dir}} && \
      mix run benchmarks/concurrency_oplog.exs && \
      mix run benchmarks/concurrency_mst_db.exs && \
      mix run benchmarks/concurrency_wal.exs

# Concurrency: oplog instance only.
bench-concurrency-oplog duration="10":
    rebar3 compile
    cd {{bench_dir}} && mix deps.get
    DURATION_S={{duration}} cd {{bench_dir}} && mix run benchmarks/concurrency_oplog.exs

# Concurrency: mst_db substrate only.
bench-concurrency-db duration="10":
    rebar3 compile
    cd {{bench_dir}} && mix deps.get
    DURATION_S={{duration}} cd {{bench_dir}} && mix run benchmarks/concurrency_mst_db.exs

# Concurrency: WAL only.
bench-concurrency-wal duration="8":
    rebar3 compile
    cd {{bench_dir}} && mix deps.get
    DURATION_S={{duration}} cd {{bench_dir}} && mix run benchmarks/concurrency_wal.exs

# End-to-end pipeline benchmark with ECharts dashboard. Drives the
# full bondy_db substrate (WAL → applier → MST → projection → cache
# → reads) under three scenarios and writes a dashboard per scenario
# to bench/_output/e2e_pipeline/<name>/index.html with a top-level
# index linking them.
#
# Knobs (all optional, see e2e_pipeline.exs for defaults):
#   duration  : DURATION_S (seconds per scenario)
#   shards    : SHARDS (number of independent WAL+applier shards)
#   fsync     : WAL_FSYNC (per_write | batched)
#   batch     : BATCH_SIZE (writer batch size; 1 = single append/2)
#   cache     : BYPASS_CACHE (true | false; bypass the substrate cache)
#   backends  : BACKENDS (ets,leveled — pass one or both)
#
# `+SDio <shards>` pins one dirty-I/O scheduler per shard so the WAL
# fsyncs run in parallel rather than time-slicing on the default 10
# dirty-I/O threads. Matching count keeps the disk-bound work
# scheduler-pinned.
bench-e2e duration="10" shards="4" fsync="per_write" batch="1" cache="false" backends="ets,leveled":
    rebar3 compile
    cd {{bench_dir}} && mix deps.get
    cd {{bench_dir}} && \
      ELIXIR_ERL_OPTIONS="+SDio {{shards}}" \
      DURATION_S={{duration}} \
      SHARDS={{shards}} \
      WAL_FSYNC={{fsync}} \
      BATCH_SIZE={{batch}} \
      BYPASS_CACHE={{cache}} \
      BACKENDS={{backends}} \
      mix run benchmarks/e2e_pipeline.exs

# Open the most recently generated HTML report (macOS / Linux).
bench-open:
    @latest=$(ls -1t {{output_dir}}/*/index.html 2>/dev/null | head -1); \
    if [ -z "$latest" ]; then \
      echo "no reports under {{output_dir}}"; exit 1; \
    fi; \
    echo "opening $latest"; \
    if command -v open >/dev/null; then open "$latest"; \
    elif command -v xdg-open >/dev/null; then xdg-open "$latest"; \
    else echo "open the file manually: $latest"; fi

# Wipe generated bench artefacts.
bench-clean:
    rm -rf {{output_dir}}
    rm -rf {{bench_dir}}/_build {{bench_dir}}/deps
