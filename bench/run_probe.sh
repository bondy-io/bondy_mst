#!/bin/sh
# Runs the MST perf probe part given as $1 (default p4), errors to /dev/null.
cd "$(dirname "$0")/.." || exit 1
exec escript bench/mst_perf_probe.escript "${1:-p4}" 2>/dev/null
