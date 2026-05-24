#!/usr/bin/env bash
# =============================================================================
# Fly machine boot — wire the persistent volume into the paths the
# bench tooling and the pack-store WAL artefacts expect, then hand
# off to whatever the container's CMD is (usually `tail -f /dev/null`
# to keep the VM alive for `fly ssh console`).
# =============================================================================

set -euo pipefail

VOLUME_ROOT=/data

mkdir -p "${VOLUME_ROOT}/tmp"
mkdir -p "${VOLUME_ROOT}/_build"
mkdir -p "${VOLUME_ROOT}/results"

# Bench artefacts (WAL segments, leveled stores, pack stores) land
# under /tmp by convention — see the various test/bench files that
# build paths like /tmp/bondy_mst_*. On Fly we want them on the
# volume so they survive restarts and aren't capped by the small
# rootfs.
if [ ! -L /tmp ] || [ "$(readlink /tmp)" != "${VOLUME_ROOT}/tmp" ]; then
    rm -rf /tmp
    ln -s "${VOLUME_ROOT}/tmp" /tmp
fi

# rebar3 _build lives on the volume so a `rebar3 compile` after a
# `git pull` doesn't re-do work on every machine restart.
if [ -d /opt/bondy_mst/_build ] && [ ! -L /opt/bondy_mst/_build ]; then
    # First boot — image baked _build into the layer; move it to
    # the volume so subsequent boots reuse it.
    if [ ! -d "${VOLUME_ROOT}/_build/default" ]; then
        mv /opt/bondy_mst/_build/* "${VOLUME_ROOT}/_build/" 2>/dev/null || true
    fi
    rm -rf /opt/bondy_mst/_build
fi
if [ ! -L /opt/bondy_mst/_build ]; then
    ln -s "${VOLUME_ROOT}/_build" /opt/bondy_mst/_build
fi

# Bench's Mix _build is independent of rebar3's — same treatment.
if [ -d /opt/bondy_mst/bench/_build ] && [ ! -L /opt/bondy_mst/bench/_build ]; then
    if [ ! -d "${VOLUME_ROOT}/_build/bench" ]; then
        mkdir -p "${VOLUME_ROOT}/_build/bench"
        mv /opt/bondy_mst/bench/_build/* "${VOLUME_ROOT}/_build/bench/" 2>/dev/null || true
    fi
    rm -rf /opt/bondy_mst/bench/_build
fi
if [ ! -L /opt/bondy_mst/bench/_build ]; then
    ln -s "${VOLUME_ROOT}/_build/bench" /opt/bondy_mst/bench/_build
fi

exec "$@"
