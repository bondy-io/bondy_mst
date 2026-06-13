# bondy_mst — task runner (https://just.systems)
#
# MST-library benchmarks only. The oplog/db-layer benchmarks (e2e
# pipeline, oplog, wal, concurrency, projections) and the Fly.io bench
# substrate + Jepsen harness live in the bondy umbrella's `justfile`
# alongside the bondy_oplog/bondy_db apps they exercise.

set shell := ["bash", "-cu"]

bench_dir := justfile_directory() / "bench"
output_dir := bench_dir / "_output"

# Show every recipe.
default:
    @just --list

# Run the full MST benchmark suite. Compiles bondy_mst with rebar3,
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

# Run the pack-store benchmarks (append throughput vs sync_every,
# seal throughput). Disk-dependent — writes to /tmp/bondy_mst_bench_pack.
bench-pack:
    rebar3 compile
    cd {{bench_dir}} && mix deps.get
    cd {{bench_dir}} && mix run benchmarks/mst_pack_put.exs
    cd {{bench_dir}} && mix run benchmarks/mst_pack_seal.exs
    cd {{bench_dir}} && mix run benchmarks/mst_pack_get.exs

# Cross-store lookup comparison (map | ets | pack with bloom on).
# Disk-dependent — writes to /tmp/bondy_mst_bench_pack.
bench-stores:
    rebar3 compile
    cd {{bench_dir}} && mix deps.get
    cd {{bench_dir}} && mix run benchmarks/mst_store_get.exs

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

# Wipe generated bench artefacts plus the `/tmp/bondy_mst_*` test scratch
# (WAL/leveled dirs can accumulate to several GB across test runs — see
# the project-wide standing rule on cleaning /tmp after tests).
clean: bench-clean
    rm -rf /tmp/bondy_mst_*
