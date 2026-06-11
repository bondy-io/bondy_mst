#!/bin/sh
# macOS e2e smoke: fused, mem WAL, 1 shard, 8 writers, write_only, 5s,
# compaction on. Mirrors the PR-6 validation run.
cd "$(dirname "$0")" || exit 1
WAL_BACKEND=mem FUSED=true WRITERS=8 SCENARIOS=write_only PREPOPULATE=10000 \
COMPACT=true COMPACT_INTERVAL_MS=500 DURATION_S=5 SHARDS=1 \
WAL_FSYNC=per_write BATCH_SIZE=1 BYPASS_CACHE=false BACKENDS=ephemeral \
exec mix run benchmarks/e2e_pipeline.exs
