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

# Run the native CRDT primitive benchmarks (apply_op, interpret_cog, codec).
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

# Ephemeral (ets-backed, fully in-memory) vs durable (leveled-backed)
# tables, head-to-head across the e2e scenarios. Each value of BACKENDS
# here is a whole-stack *profile*, not just a projection swap:
#   ephemeral = ets projection + in-memory MST + batched fsync
#               (the `durability => ephemeral` table — nothing durable)
#   durable   = leveled projection + pack-store MST + per_write fsync
#               (the fully-durable, leveled-backed production stack;
#                each shard runs as a genesis peer via seed: true)
# Reports land as `<scenario>_ephemeral` vs `<scenario>_durable` so the
# index page lists them side-by-side. Needs the leveled adapter, so this
# compiles the bench profile (`rebar3 as bench compile`) — which leaves
# eunit unable to find utils.app; run `rebar3 as test compile` before
# `just test` afterwards.
bench-ephemeral-vs-leveled duration="15" shards="4" cache="false":
    rebar3 as bench compile
    cd {{bench_dir}} && mix deps.get
    cd {{bench_dir}} && \
      ELIXIR_ERL_OPTIONS="+SDio {{shards}}" \
      DURATION_S={{duration}} \
      SHARDS={{shards}} \
      BYPASS_CACHE={{cache}} \
      BACKENDS=ephemeral,durable \
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

# Wipe generated jepsen artefacts (leiningen + rebar3 build + leiningen
# test scratch). Preserves:
#   - `jepsen/store_runs/` — manually-archived run outputs
#   - `jepsen/bondy_mst_jepsen/_checkouts/` — rebar3 dev-mode local-dep links
#   - `jepsen/docker/shared/jepsen-bot{,.pub}` — SSH keys, NEVER delete
#
# Total reclaim is ~165MB (the rebar3 `_build` dominates at ~145MB).
jepsen-clean:
    rm -rf {{justfile_directory()}}/jepsen/jepsen.bondymst/target
    rm -rf {{justfile_directory()}}/jepsen/jepsen.bondymst/store
    rm -rf {{justfile_directory()}}/jepsen/bondy_mst_jepsen/_build

# Wipe ALL generated artefacts across bench + jepsen, plus the
# `/tmp/bondy_mst_*` test scratch (WAL/leveled dirs can accumulate
# to several GB across test runs — see the project-wide standing
# rule on cleaning /tmp after tests).
#
# Does NOT touch `jepsen/store_runs/` (archived runs), the rebar3
# top-level `_build`, or any committed file.
clean: bench-clean jepsen-clean
    rm -rf /tmp/bondy_mst_*

# -----------------------------------------------------------------------------
# Fly.io Linux bench substrate (pack-store QA #14).
#
# See `bench/fly/README.md` for the operator runbook and
# `_design/latest/PACK_STORE_WRITE_PATH_FLOOR_BENCH_PLAN.md` §10 for
# the bench plan context.
#
# fly.toml lives at the repo root because flyctl resolves the build
# context relative to its location, and the Dockerfile's `COPY .`
# needs the repo source. All other Fly assets stay under bench/fly/.
# Recipes invoke `fly` from the repo root with no `--config` flag —
# fly auto-discovers fly.toml in cwd.
#
# Cost-control rule: always `just bench-fly-down` when you're done.
# `bench-fly-bench-all` leaves the VM running so you can re-run
# individual layers; call `bench-fly-down` after.
# -----------------------------------------------------------------------------

# Local Dockerfile validation via docker buildx (~3-5 min, needs Docker Desktop).
# NOTE: On Apple Silicon, the linux/amd64 emulation under QEMU has a known
# bug that can segfault during Elixir compilation of certain hex deps
# (jason, benchee, etc). The build will get through `rebar3 compile` +
# NIFs + `mix deps.get` cleanly, then crash at `mix compile`. Fly's
# remote builder runs on native amd64 hardware and is not affected — if
# this fails locally past `mix deps.get`, deploy to Fly directly to
# validate the rest.
bench-fly-build-local:
    docker buildx build --platform linux/amd64 \
      -f bench/fly/Dockerfile -t bondy-mst-bench:local .

# First-time setup: create app + volume + initial deploy (interactive).
#
# App is created in the Leapsight org (`--org leapsight`). Fly app
# names are globally unique — if `bondy-mst-bench` already exists in
# another org (e.g. a prior `personal` deploy) the create will fail.
# Run `just bench-fly-destroy` on the old app first, or pick a fresh
# name here.
bench-fly-init:
    fly apps create bondy-mst-bench --org leapsight
    fly volumes create bench_data --size 10 --region lhr --app bondy-mst-bench --yes
    just bench-fly-deploy

# Build + deploy the image to Fly (remote build, preserves volume cache).
bench-fly-deploy:
    fly deploy --remote-only

# Start the VM (idempotent, no-op if already running).
bench-fly-up:
    fly machine start

# Interactive ssh into the VM (auto-starts if stopped).
bench-fly-shell:
    fly ssh console -C "bash -c 'cd /opt/bondy_mst && exec bash'"

# Run a single bench script on the VM, tee output to /data/results/. Example: `just bench-fly-run profile_syscalls`.
#
# `\$(...)` escapes the local justfile shell so the command substitution
# runs on the VM, not locally. Same for `\$ts` etc in the recipes below.
bench-fly-run name:
    fly ssh console -C \
      "bash -c 'mkdir -p /data/results && cd /opt/bondy_mst && just bench-one {{name}} 2>&1 | tee /data/results/{{name}}_\$(date +%Y%m%d_%H%M%S).txt'"

# Run all four QA #14 bench layers in sequence on the VM (tees each to /data/results/).
bench-fly-bench-all:
    fly ssh console -C \
      "bash -c 'set -e; mkdir -p /data/results; cd /opt/bondy_mst; \
        ts=\$(date +%Y%m%d_%H%M%S); \
        echo \"=== Layer 1 — syscall isolation ===\" | tee /data/results/run_\$ts.log; \
        just bench-one profile_syscalls    2>&1 | tee /data/results/layer1_syscalls_\$ts.txt; \
        echo \"=== Layer 2 — single-put microbench ===\" | tee -a /data/results/run_\$ts.log; \
        just bench-one profile_pack_one_put 2>&1 | tee /data/results/layer2_pack_one_put_\$ts.txt; \
        echo \"=== Layer 3a — sustained pack-put ===\" | tee -a /data/results/run_\$ts.log; \
        just bench-one mst_pack_put         2>&1 | tee /data/results/layer3a_mst_pack_put_\$ts.txt; \
        echo \"=== Layer 3b — e2e pipeline (30s/scenario) ===\" | tee -a /data/results/run_\$ts.log; \
        just bench-e2e 30                   2>&1 | tee /data/results/layer3b_e2e_\$ts.txt; \
        echo \"All layers complete. Results in /data/results/*_\$ts.txt\"'"

# Applier pipeline residual profiling sweep on the VM (Run A + Run B).
#
# Drives the e2e bench against leveled-only twice — Run A with full
# per-stage applier telemetry attached, Run B with the per-stage
# handlers detached (control). Tees both to /data/results/applier_profile_*.
#
# Erlang-side `telemetry:execute/3` calls in `apply_batch/2` fire in
# both runs; only the bench-side handler attachment differs. The
# delta isolates bench collection overhead from the always-paid
# Erlang-side `monotonic_time` cost.
#
# Run C (fprof on shard-0 applier) is a follow-up that needs its own
# `applier_fprof.exs` driver — not yet implemented.
#
# See `_design/latest/APPLIER_PIPELINE_RESIDUAL_PLAN.md` §4.
bench-fly-applier-profile duration="30":
    fly ssh console -C \
      "bash -c 'set -e; mkdir -p /data/results; cd /opt/bondy_mst; \
        ts=\$(date +%Y%m%d_%H%M%S); \
        echo \"=== Run A — full per-stage applier telemetry ===\" | tee /data/results/applier_profile_run_\$ts.log; \
        APPLIER_PROFILE=full \
          just bench-e2e {{duration}} 4 per_write 1 false leveled \
            2>&1 | tee /data/results/applier_profile_runA_full_\$ts.txt; \
        echo \"=== Run B — control (per-stage handlers detached) ===\" | tee -a /data/results/applier_profile_run_\$ts.log; \
        APPLIER_PROFILE=control \
          just bench-e2e {{duration}} 4 per_write 1 false leveled \
            2>&1 | tee /data/results/applier_profile_runB_control_\$ts.txt; \
        echo \"Done. Results in /data/results/applier_profile_*_\$ts.txt\"'"

# Long stability run on a single scenario (Run A only, multiple reps).
#
# Drives the e2e bench N reps back-to-back with full instrumentation,
# filtered to ONE scenario via SCENARIOS env var. Use to:
#   * smooth Firecracker burst-credit / noisy-neighbour variance
#   * confirm per-stage µs/event is stable across reps before picking
#     a mitigation
#   * burn through the volume's burst IOPS to land at steady-state
#
# Defaults: 3 reps × 120s = 6 min per scenario. Tees each rep to its
# own file so you can compare them post-hoc.
#
# `scenario` matches by name prefix — `write_only` matches
# `write_only_w4_leveled`. Pass `write_only`, `mixed`,
# `concurrent_rw`, or `read_only`.
#
# Examples:
#   just bench-fly-applier-profile-long
#   just bench-fly-applier-profile-long mixed 180 5
#
# See `_design/latest/APPLIER_PIPELINE_RESIDUAL_PLAN.md` §4 (longer-run
# follow-up after the initial Run A surfaced substrate-noise issues).
bench-fly-applier-profile-long scenario="write_only" duration="120" reps="3":
    fly ssh console -C \
      "bash -c 'set -e; mkdir -p /data/results; cd /opt/bondy_mst; \
        ts=\$(date +%Y%m%d_%H%M%S); \
        echo \"=== Long stability run — {{scenario}}, {{reps}} reps × {{duration}}s ===\" \
          | tee /data/results/applier_long_{{scenario}}_\$ts.log; \
        for rep in \$(seq 1 {{reps}}); do \
          echo \"--- Rep \$rep/{{reps}} ---\" | tee -a /data/results/applier_long_{{scenario}}_\$ts.log; \
          APPLIER_PROFILE=full SCENARIOS={{scenario}} \
            just bench-e2e {{duration}} 4 per_write 1 false leveled \
              2>&1 | tee /data/results/applier_long_{{scenario}}_rep\${rep}_\$ts.txt; \
        done; \
        echo \"Done. Results in /data/results/applier_long_{{scenario}}_rep*_\$ts.txt\" \
          | tee -a /data/results/applier_long_{{scenario}}_\$ts.log'"

# Run vmstat/iostat/strace companion data collection in a second terminal during a layer-3 bench.
bench-fly-companion seconds="60":
    fly ssh console -C \
      "bash -c 'mkdir -p /data/results; ts=\$(date +%Y%m%d_%H%M%S); \
        vmstat 1 {{seconds}} > /data/results/vmstat_\$ts.txt & \
        iostat -x 1 {{seconds}} > /data/results/iostat_\$ts.txt & \
        beam_pid=\$(pgrep -f beam.smp || true); \
        if [ -n \"\$beam_pid\" ]; then \
          strace -c -p \"\$beam_pid\" 2> /data/results/strace_\$ts.txt & \
          sleep 10; kill %3 2>/dev/null || true; \
        else \
          echo \"no BEAM running — start a bench layer first\" >&2; \
        fi; \
        wait'"

# Pull /data/results from the VM into a fresh local dir. Default is timestamped
# under ./fly-bench-results-<ts>. Override `dest=` to pick a specific name.
#
# Tarballs the results on the VM, sftp's the single tar file, untars locally.
# Avoids `fly ssh sftp get -r`'s "won't overwrite" + "won't auto-start machine"
# behaviour. `fly ssh console` auto-starts the machine if it's stopped.
bench-fly-results dest="":
    #!/usr/bin/env bash
    set -eu
    dest='{{dest}}'
    if [ -z "$dest" ]; then
      dest="./fly-bench-results-$(date +%Y%m%d_%H%M%S)"
    fi
    if [ -e "$dest" ]; then
      echo "destination '$dest' already exists — pick another or remove it first" >&2
      exit 1
    fi
    mkdir -p "$dest"
    remote_tar="/tmp/fly-bench-results-$$.tgz"
    local_tar_basename="fly-bench-results-fetch-$$.tgz"
    echo "tarring /data/results on the VM..."
    fly ssh console -C "bash -c 'tar czf $remote_tar -C /data results'"
    # `fly ssh sftp get` writes to the CWD with the basename of the
    # remote path; it does not honour stdout redirects. Drive it from
    # inside `$dest` and rename the result.
    echo "sftp'ing $remote_tar → $dest/_fetch.tgz"
    ( cd "$dest" && fly ssh sftp get "$remote_tar" )
    mv "$dest/$(basename "$remote_tar")" "$dest/_fetch.tgz"
    echo "extracting → $dest"
    tar xzf "$dest/_fetch.tgz" -C "$dest" --strip-components=1
    rm -f "$dest/_fetch.tgz"
    fly ssh console -C "bash -c 'rm -f $remote_tar'" || true
    echo "done: $dest"

# Tail the VM's logs (boot, entrypoint, stdout). Useful for debugging deploy failures.
bench-fly-logs:
    fly logs

# Stop the VM (idle cost drops to volume-only, ~$1.50/mo for 10 GB).
# `fly machine stop` without args is interactive; fan out across all
# of the app's machines (usually one) so the recipe works unattended.
bench-fly-down:
    #!/usr/bin/env bash
    set -eu
    ids=$(fly machines list --json | jq -r '.[].id')
    if [ -z "$ids" ]; then
      echo "no machines to stop"; exit 0
    fi
    for id in $ids; do
      echo "stopping $id..."
      fly machines stop "$id"
    done

# DESTRUCTIVE — destroys the app AND its volume (results lost forever). Pull results first.
bench-fly-destroy:
    @echo "This will destroy the bondy-mst-bench app and its volume."
    @echo "Volume data (including /data/results) will be PERMANENTLY LOST."
    @echo "Pull results first with: just bench-fly-results"
    @echo ""
    @read -p "Type 'destroy' to confirm: " confirm && \
      [ "$confirm" = "destroy" ] || (echo "aborted"; exit 1)
    fly apps destroy bondy-mst-bench --yes

# -----------------------------------------------------------------------------
# Fly.io Linux bench substrate — performance-8x variant.
#
# Parallel to the bench-fly-* recipes above. Uses fly-8x.toml (perf-8x
# VM + 40 GB volume in the Leapsight org) instead of fly.toml (perf-2x
# + 10 GB volume).
#
# Purpose: distinguish substrate-limited from code-limited throughput.
# perf-2x is shared-CPU on a Firecracker slot with a small burst-IOPS
# budget; perf-8x is dedicated-CPU with a much larger budget. If
# applier ops/s scales meaningfully on perf-8x then we're substrate-
# bound on perf-2x; if it doesn't, the bottleneck is in our code
# (leveled / applier / WAL) and the next investigation should focus
# there.
#
# Every recipe passes `--config fly-8x.toml` explicitly — flyctl only
# auto-discovers `fly.toml`. The app name `bondy-mst-bench-8x` is
# separate so the perf-2x app can coexist (Fly app names are
# globally unique).
#
# Cost: ~$0.85 per 2h bench run + ~$6/mo standing volume.
# Always `just bench-fly-8x-down` after a session.
# -----------------------------------------------------------------------------

# First-time setup: create perf-8x app + 40 GB volume + initial deploy.
bench-fly-8x-init:
    fly apps create bondy-mst-bench-8x --org leapsight
    fly volumes create bench_data_8x --size 40 --region lhr --app bondy-mst-bench-8x --yes
    just bench-fly-8x-deploy

# Build + deploy the image to the perf-8x app (remote build).
bench-fly-8x-deploy:
    fly deploy --config fly-8x.toml --remote-only

# Start the perf-8x VM (idempotent).
bench-fly-8x-up:
    fly machine start --config fly-8x.toml

# Interactive ssh into the perf-8x VM (auto-starts if stopped).
bench-fly-8x-shell:
    fly ssh console --config fly-8x.toml -C "bash -c 'cd /opt/bondy_mst && exec bash'"

# Long stability run on perf-8x — same shape as bench-fly-applier-profile-long
# but against the dedicated-CPU app. Direct apples-to-apples comparison
# with the perf-2x results in /Users/aramallo/Work/Bondy/bondy_mst/results.
#
# `fsync` arg picks the WAL fsync mode: `per_write` (fsync after every
# event — gives 1-2 event batches at the applier) or `batched` (writers
# buffer events between fsyncs — applier picks up bigger batches).
# Result filenames embed the fsync mode so per_write + batched runs
# don't clobber each other.
#
# Defaults: 3 reps × 120s of write_only with per_write fsync. Override:
#   just bench-fly-8x-applier-profile-long mixed 180 5 per_write
#   just bench-fly-8x-applier-profile-long write_only 120 3 batched
bench-fly-8x-applier-profile-long scenario="write_only" duration="120" reps="3" fsync="per_write":
    fly ssh console --config fly-8x.toml -C \
      "bash -c 'set -e; mkdir -p /data/results; cd /opt/bondy_mst; \
        ts=\$(date +%Y%m%d_%H%M%S); \
        echo \"=== perf-8x long stability run — {{scenario}} ({{fsync}}), {{reps}} reps × {{duration}}s ===\" \
          | tee /data/results/applier_long_8x_{{scenario}}_{{fsync}}_\$ts.log; \
        for rep in \$(seq 1 {{reps}}); do \
          echo \"--- Rep \$rep/{{reps}} ---\" | tee -a /data/results/applier_long_8x_{{scenario}}_{{fsync}}_\$ts.log; \
          APPLIER_PROFILE=full SCENARIOS={{scenario}} \
            just bench-e2e {{duration}} 4 {{fsync}} 1 false leveled \
              2>&1 | tee /data/results/applier_long_8x_{{scenario}}_{{fsync}}_rep\${rep}_\$ts.txt; \
        done; \
        echo \"Done. Results in /data/results/applier_long_8x_{{scenario}}_{{fsync}}_rep*_\$ts.txt\" \
          | tee -a /data/results/applier_long_8x_{{scenario}}_{{fsync}}_\$ts.log'"

# Ephemeral (ets-backed, in-memory) vs durable (leveled-backed) tables on
# perf-8x — the head-to-head the rollout's `durability => ephemeral` was
# built for. Runs both whole-stack profiles across every e2e scenario and
# tees the result tables + HTML reports under /data/results. Pull with
# `just bench-fly-8x-results`.
#
#   ephemeral = ets projection + in-memory MST + batched fsync
#   durable   = leveled projection + pack-store MST + per_write fsync
#
# The write scenarios are the headline: ephemeral touches no disk and pays
# almost no fsync (batched), while the durable stack pays per_write fsync
# on every event + writes through the leveled journal/ledger + the durable
# pack-store MST. The image already has the leveled adapter compiled
# (`rebar3 as bench compile` at Docker build), so `durable` is not skipped.
#
# Defaults: 60s per scenario, 4 shards, cache on (realistic read-your-writes).
# `cache=true` bypasses the read cache to expose the projection read delta.
#   just bench-fly-8x-ephemeral-vs-leveled 120 8
bench-fly-8x-ephemeral-vs-leveled duration="60" shards="4" cache="false":
    fly ssh console --config fly-8x.toml -C \
      "bash -c 'set -e; mkdir -p /data/results; cd /opt/bondy_mst; \
        ts=\$(date +%Y%m%d_%H%M%S); \
        out=/data/results/ephemeral_vs_leveled_8x_\$ts.txt; \
        echo \"=== perf-8x ephemeral vs leveled — {{duration}}s x {{shards}} shards (cache_bypass={{cache}}) ===\" \
          | tee \$out; \
        just bench-e2e {{duration}} {{shards}} per_write 1 {{cache}} ephemeral,durable \
          2>&1 | tee -a \$out; \
        echo \"Done. Result table in \$out; HTML under bench/_output/e2e_pipeline/\" \
          | tee -a \$out'"

# Durable per-shard ceiling + linear shard-scaling on perf-8x (Linux,
# fast NVMe). The validation WRITE_STACK_THROUGHPUT_PLAN §10 calls for:
# measure the TRUE per-shard durable ceiling (Linux fsync ~20-50µs vs
# macOS's ~8x distortion) and confirm shards scale linearly.
#
# Full durable stack: leveled projection + pack-store MST
# (MST_BACKEND=pack — the macOS A2/A4 numbers used pack, so this is
# apples-to-apples) + per_write WAL + A2/A4 defaults (256/16).
# write_only scenario, one writer per shard (WRITERS=shards), so each
# run isolates per-shard throughput and aggregate/shards = per-shard.
#
# Sweeps SHARDS over `shard_list`; linear scaling => aggregate applier
# ops/s doubles as shards double, until the 8 vCPU saturate.
#
# `writers_per_shard` (default 2, matching the macOS A2 baseline's 4w/2s)
# sets WRITERS = writers_per_shard × shards, so each shard's applier is
# fed by >1 writer — otherwise a single writer's per_write fsync floor
# (~266µs on the Fly volume) under-feeds the applier and you measure the
# WAL fsync ceiling instead of the applier ceiling.
#
# `oldstate_cache` (default false) toggles the A3 applier OldValue
# frame-cache, so the same sweep doubles as the A3 A/B:
#   just bench-fly-8x-shard-scaling 90 "1 4" 2 false   # A3 off (baseline)
#   just bench-fly-8x-shard-scaling 90 "1 4" 2 true    # A3 on
#
#   just bench-fly-8x-shard-scaling                  # 90s × {1,2,4,8}, 2 w/shard
#   just bench-fly-8x-shard-scaling 120 "1 2 4" 4    # 4 writers/shard
#   just bench-fly-8x-shard-scaling 90 "1 2 4" 2 true batched   # A3 + batched fsync
bench-fly-8x-shard-scaling duration="90" shard_list="1 2 4 8" writers_per_shard="2" oldstate_cache="false" fsync="per_write":
    fly ssh console --config fly-8x.toml -C \
      "bash -c 'set -e; mkdir -p /data/results; cd /opt/bondy_mst; \
        ts=\$(date +%Y%m%d_%H%M%S); \
        out=/data/results/shard_scaling_8x_oc{{oldstate_cache}}_{{fsync}}_\$ts.log; \
        echo \"=== perf-8x durable shard-scaling — write_only, pack MST, {{fsync}}, {{duration}}s/point, shards={{shard_list}}, {{writers_per_shard}} writers/shard, oldstate_cache={{oldstate_cache}} ===\" \
          | tee \$out; \
        for s in {{shard_list}}; do \
          w=\$((s * {{writers_per_shard}})); \
          echo \"--- shards=\$s writers=\$w oldstate_cache={{oldstate_cache}} fsync={{fsync}} ---\" | tee -a \$out; \
          MST_BACKEND=pack WRITERS=\$w SCENARIOS=write_only \
          APPLY_BATCH_MAX_EVENTS=256 INSTALL_COALESCE_MAX=16 \
          OLDSTATE_CACHE={{oldstate_cache}} \
            just bench-e2e {{duration}} \$s {{fsync}} 1 false leveled \
              2>&1 | tee /data/results/shard_scaling_8x_oc{{oldstate_cache}}_{{fsync}}_s\${s}_\$ts.txt \
              | tee -a \$out; \
        done; \
        echo \"Done. Per-point tables: /data/results/shard_scaling_8x_oc{{oldstate_cache}}_{{fsync}}_s*_\$ts.txt\" \
          | tee -a \$out'"

# Ephemeral fused-writer A/B on perf-8x — the Step-5 validation of the
# fused-writer rollout (EPHEMERAL_FUSED_WRITER_PLAN §5). Reproduces the
# documented ~11k/instance H1 ceiling baseline (project_ephemeral_20k_ceiling:
# write_only, BACKENDS=ephemeral = ets projection + ets MST + batched fsync,
# 4 writers/shard, 60s/point) and runs it for BOTH the non-fused (`false`,
# the applier↔instance install round-trip = H1) and fused (`true`, H1
# removed — the instance drains+installs inline, no applier) arms, so the
# per-instance lift is a same-VM A/B. Filenames embed the arm + shard count.
#
# Target: fused 1-shard ≥ 20k/instance (vs the ~11k non-fused ceiling).
# Watch the first sample's `applied` ramp for the QA Finding-6 first-merge
# full-fold latency (single-node has no merge, so it should be flat here).
#
#   just bench-fly-8x-fused-scaling                  # 60s × {1,2,4}, 4 w/shard, both arms
#   just bench-fly-8x-fused-scaling 90 "1 2 4 8" 6   # heavier: 8 shards, 6 w/shard
bench-fly-8x-fused-scaling duration="60" shard_list="1 2 4" writers_per_shard="4":
    fly ssh console --config fly-8x.toml -C \
      "bash -c 'set -e; mkdir -p /data/results; cd /opt/bondy_mst; \
        ts=\$(date +%Y%m%d_%H%M%S); \
        out=/data/results/fused_ab_8x_\$ts.log; \
        echo \"=== perf-8x EPHEMERAL fused A/B — write_only, ets MST, batched fsync, {{duration}}s/point, shards={{shard_list}}, {{writers_per_shard}} w/shard ===\" \
          | tee \$out; \
        for f in false true; do \
          for s in {{shard_list}}; do \
            w=\$((s * {{writers_per_shard}})); \
            echo \"--- FUSED=\$f shards=\$s writers=\$w ---\" | tee -a \$out; \
            FUSED=\$f WRITERS=\$w SCENARIOS=write_only PREPOPULATE=10000 \
              just bench-e2e {{duration}} \$s per_write 1 false ephemeral \
                2>&1 | tee /data/results/fused_ab_8x_f\${f}_s\${s}_\$ts.txt \
                | tee -a \$out; \
          done; \
        done; \
        echo \"Done. Per-point: /data/results/fused_ab_8x_f*_s*_\$ts.txt\" \
          | tee -a \$out'"

# Ephemeral ETS WAL A/B on perf-8x — the PR-4 gate of the ETS-WAL rollout
# (EPHEMERAL_ETS_WAL_PLAN §6). Isolates the WAL BACKEND: both arms are fused
# (`FUSED=true`), so disk (the Step-5 ~12.5k/instance fused baseline, WAL
# durability-latency-bound at ~42% util) vs mem (`bondy_oplog_wal_mem` — events
# in ETS, drain reads them with no durable-position gate) measures exactly the
# fsync-on-the-ack-path removal. write_only, ets MST, 4 writers/shard.
#
# Target: mem 1-shard ≥ 20k/instance. Burst-credit control: shard-outer /
# arm-inner (disk & mem for a shard run adjacently under similar credits) and
# the inner arm order ALTERNATES per rep, so neither arm is systematically
# first (the Step-5 confound). Compare per-rep, same-shard pairs; ignore
# absolute swings across reps.
#
#   just bench-fly-8x-wal-scaling                  # 60s × {1,2,4}, 4 w/shard, 3 reps
#   just bench-fly-8x-wal-scaling 90 "1" 4 4       # 1-shard gate, 90s, 4 reps
bench-fly-8x-wal-scaling duration="60" shard_list="1 2 4" writers_per_shard="4" reps="3":
    fly ssh console --config fly-8x.toml -C \
      "bash -c 'set -e; mkdir -p /data/results; cd /opt/bondy_mst; \
        ts=\$(date +%Y%m%d_%H%M%S); \
        out=/data/results/wal_ab_8x_\$ts.log; \
        echo \"=== perf-8x EPHEMERAL WAL-backend A/B (fused) — write_only, ets MST, {{duration}}s/point, shards={{shard_list}}, {{writers_per_shard}} w/shard, {{reps}} reps ===\" \
          | tee \$out; \
        for r in \$(seq 1 {{reps}}); do \
          if [ \$((r % 2)) -eq 0 ]; then order=\"disk mem\"; else order=\"mem disk\"; fi; \
          for s in {{shard_list}}; do \
            w=\$((s * {{writers_per_shard}})); \
            for b in \$order; do \
              echo \"--- rep=\$r WAL_BACKEND=\$b shards=\$s writers=\$w ---\" | tee -a \$out; \
              WAL_BACKEND=\$b FUSED=true WRITERS=\$w SCENARIOS=write_only PREPOPULATE=10000 \
                just bench-e2e {{duration}} \$s per_write 1 false ephemeral \
                  2>&1 | tee /data/results/wal_ab_8x_r\${r}_\${b}_s\${s}_\$ts.txt \
                  | tee -a \$out; \
            done; \
          done; \
        done; \
        echo \"Done. Per-point: /data/results/wal_ab_8x_r*_*_s*_\$ts.txt\" \
          | tee -a \$out'"

# Ephemeral ETS WAL — 1-shard WRITER-DEPTH sweep on perf-8x. After PR-4 found
# the 1-shard floor is `mst_install` gated by the bounded-writer→await pipeline
# (not the WAL), this sweeps writers {4,8,16,32} at a SINGLE shard for both arms
# (fused+disk vs fused+mem) to find whether deepening the pipeline lets mem's
# no-fsync immediate visibility approach the ~install-rate ceiling (and whether
# 20k/instance is reachable). Disk stays fsync-gated no matter the writer count.
# Credit-controlled: arm order alternates per rep.
#
#   just bench-fly-8x-wal-writers                    # 60s, writers {4,8,16,32}, 2 reps
#   just bench-fly-8x-wal-writers 90 "8 16 32 64" 3
bench-fly-8x-wal-writers duration="60" writer_list="4 8 16 32" reps="2":
    fly ssh console --config fly-8x.toml -C \
      "bash -c 'set -e; mkdir -p /data/results; cd /opt/bondy_mst; \
        ts=\$(date +%Y%m%d_%H%M%S); \
        out=/data/results/wal_writers_8x_\$ts.log; \
        echo \"=== perf-8x EPHEMERAL 1-shard writer-depth A/B (fused) — write_only, ets MST, {{duration}}s/point, writers={{writer_list}}, {{reps}} reps ===\" \
          | tee \$out; \
        for r in \$(seq 1 {{reps}}); do \
          if [ \$((r % 2)) -eq 0 ]; then order=\"disk mem\"; else order=\"mem disk\"; fi; \
          for w in {{writer_list}}; do \
            for b in \$order; do \
              echo \"--- rep=\$r WAL_BACKEND=\$b shards=1 writers=\$w ---\" | tee -a \$out; \
              WAL_BACKEND=\$b FUSED=true WRITERS=\$w SCENARIOS=write_only PREPOPULATE=10000 \
                just bench-e2e {{duration}} 1 per_write 1 false ephemeral \
                  2>&1 | tee /data/results/wal_writers_8x_r\${r}_\${b}_w\${w}_\$ts.txt \
                  | tee -a \$out; \
            done; \
          done; \
        done; \
        echo \"Done. Per-point: /data/results/wal_writers_8x_r*_*_w*_\$ts.txt\" \
          | tee -a \$out'"

# Ephemeral ETS WAL — bounded-MST A/B on perf-8x. After the PR-6 fix (fused drain
# yields so compaction actually runs under load), this isolates the effect of a
# BOUNDED MST on mem 1-shard throughput: COMPACT=true (MST truncated every
# `interval`ms → small, uniform `mst_install`) vs COMPACT=false (MST grows
# unbounded → install p99 tail inflates 23.7ms→75ms). Tests whether bounding the
# MST lifts mem past the ~17.5k single-drain plateau toward 20k. mem WAL, fused,
# 1 shard, fixed writers. Credit-controlled: COMPACT order alternates per rep.
#
#   just bench-fly-8x-wal-compact                    # 60s, 8 writers, 3 reps, 500ms
#   just bench-fly-8x-wal-compact 90 16 3 250
bench-fly-8x-wal-compact duration="60" writers="8" reps="3" compact_interval="500":
    fly ssh console --config fly-8x.toml -C \
      "bash -c 'set -e; mkdir -p /data/results; cd /opt/bondy_mst; \
        ts=\$(date +%Y%m%d_%H%M%S); \
        out=/data/results/wal_compact_8x_\$ts.log; \
        echo \"=== perf-8x EPHEMERAL bounded-MST A/B (mem, fused) — write_only, 1 shard, {{writers}} writers, {{duration}}s/point, {{reps}} reps, compact_interval={{compact_interval}}ms ===\" \
          | tee \$out; \
        for r in \$(seq 1 {{reps}}); do \
          if [ \$((r % 2)) -eq 0 ]; then order=\"false true\"; else order=\"true false\"; fi; \
          for c in \$order; do \
            echo \"--- rep=\$r COMPACT=\$c writers={{writers}} ---\" | tee -a \$out; \
            WAL_BACKEND=mem FUSED=true WRITERS={{writers}} SCENARIOS=write_only PREPOPULATE=10000 \
              COMPACT=\$c COMPACT_INTERVAL_MS={{compact_interval}} \
              just bench-e2e {{duration}} 1 per_write 1 false ephemeral \
                2>&1 | tee /data/results/wal_compact_8x_r\${r}_c\${c}_\$ts.txt \
                | tee -a \$out; \
          done; \
        done; \
        echo \"Done. Per-point: /data/results/wal_compact_8x_r*_c*_\$ts.txt\" \
          | tee -a \$out'"

# Pull /data/results from the perf-8x VM into a fresh local dir.
# Same pattern as bench-fly-results — tarball-then-sftp to dodge
# `sftp get -r`'s won't-overwrite + won't-auto-start behaviour.
bench-fly-8x-results dest="":
    #!/usr/bin/env bash
    set -eu
    dest='{{dest}}'
    if [ -z "$dest" ]; then
      dest="./fly-bench-results-8x-$(date +%Y%m%d_%H%M%S)"
    fi
    if [ -e "$dest" ]; then
      echo "destination '$dest' already exists — pick another or remove it first" >&2
      exit 1
    fi
    mkdir -p "$dest"
    remote_tar="/tmp/fly-bench-results-8x-$$.tgz"
    echo "tarring /data/results on the perf-8x VM..."
    fly ssh console --config fly-8x.toml -C "bash -c 'tar czf $remote_tar -C /data results'"
    local_tar="$dest/_fetch.tgz"
    echo "sftp'ing $remote_tar → $local_tar"
    fly ssh sftp get --config fly-8x.toml "$remote_tar" > "$local_tar"
    echo "extracting → $dest"
    tar xzf "$local_tar" -C "$dest" --strip-components=1
    rm -f "$local_tar"
    fly ssh console --config fly-8x.toml -C "bash -c 'rm -f $remote_tar'" || true
    echo "done: $dest"

# Tail the perf-8x VM's logs.
bench-fly-8x-logs:
    fly logs --config fly-8x.toml

# Stop the perf-8x VM (idle cost drops to volume-only, ~$6/mo for 40 GB).
bench-fly-8x-down:
    fly machine stop --config fly-8x.toml

# DESTRUCTIVE — destroys the perf-8x app AND its 40 GB volume. Pull results first.
bench-fly-8x-destroy:
    @echo "This will destroy the bondy-mst-bench-8x app and its 40 GB volume."
    @echo "Volume data (including /data/results) will be PERMANENTLY LOST."
    @echo "Pull results first with: just bench-fly-8x-results"
    @echo ""
    @read -p "Type 'destroy' to confirm: " confirm && \
      [ "$confirm" = "destroy" ] || (echo "aborted"; exit 1)
    fly apps destroy bondy-mst-bench-8x --yes

# Write→readable latency sampling overhead on perf-8x. Two measurements:
#   1. microbench (latency_sampling.exs) — the exact per-write cost of the
#      sampling hot path (disabled gate vs enabled gate+2xmono+record).
#   2. e2e A/B — ephemeral write_only, LATENCY_SAMPLING off then on, so the
#      macro throughput delta (expected: within noise) is visible.
# Tees everything to /data/results; pull with `just bench-fly-8x-results`.
#
#   just bench-fly-8x-latency             # micro + 60s/arm e2e, 4 shards
#   just bench-fly-8x-latency 90 8        # 90s/arm, 8 shards
bench-fly-8x-latency e2e_duration="60" shards="4":
    fly ssh console --config fly-8x.toml -C \
      "bash -c 'set -e; mkdir -p /data/results; cd /opt/bondy_mst; \
        ts=\$(date +%Y%m%d_%H%M%S); \
        log=/data/results/latency_8x_\$ts.log; \
        echo \"=== perf-8x latency sampling overhead — \$ts ===\" | tee \$log; \
        echo \"--- microbench: per-write sampling cost ---\" | tee -a \$log; \
        just bench-one latency_sampling 2>&1 | tee /data/results/latency_micro_\$ts.txt | tee -a \$log; \
        echo \"--- e2e A/B: ephemeral write_only, sampling OFF ---\" | tee -a \$log; \
        LATENCY_SAMPLING=off SCENARIOS=write_only \
          just bench-e2e {{e2e_duration}} {{shards}} batched 1 false ephemeral \
          2>&1 | tee /data/results/latency_e2e_off_\$ts.txt | tee -a \$log; \
        echo \"--- e2e A/B: ephemeral write_only, sampling ON ---\" | tee -a \$log; \
        LATENCY_SAMPLING=on SCENARIOS=write_only \
          just bench-e2e {{e2e_duration}} {{shards}} batched 1 false ephemeral \
          2>&1 | tee /data/results/latency_e2e_on_\$ts.txt | tee -a \$log; \
        echo \"Done. Results: /data/results/latency_*_\$ts.*\" | tee -a \$log'"
