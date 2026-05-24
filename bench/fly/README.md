# bondy_mst — Linux bench substrate on Fly.io

Long-lived SSH-only Debian 12 VM for executing pack-store QA #14
(write-path floor) on Linux. See
`_design/latest/PACK_STORE_WRITE_PATH_FLOOR_BENCH_PLAN.md` for the
bench plan itself; this README is the operator runbook for the
Fly.io substrate.

## Image contents

- Debian 12 (bookworm) slim
- kerl-built **OTP 28**
- **Elixir 1.18** (compatible with OTP 28)
- rebar3, mix, `just`
- `sysstat` (iostat, vmstat), `strace`, `procps` — companion data
  tools the bench plan §4 calls for
- The bondy_mst repo at `/opt/bondy_mst`, pre-compiled under both
  rebar3 and mix so the first ssh session doesn't pay compile cost

## One-time setup

```bash
# Install fly CLI: https://fly.io/docs/flyctl/install/
fly auth login

# From the project root (the build context is the repo root, not
# bench/fly — the Dockerfile copies the whole project).
cd bench/fly

# Create the persistent volume in the same region as the app.
# Volumes are region-local; if you change primary_region in
# fly.toml, recreate the volume.
fly volumes create bench_data --size 10 --region lhr

# First deploy. Builds the image remotely (~10-15 min cold; ~2-3
# min on rebuild thanks to layer caching on the OTP build).
fly deploy
```

## Per-bench-session workflow

```bash
# Start the machine (auto-stops when idle to control cost).
fly machine start

# Land in an interactive shell on the VM.
fly ssh console
```

Once inside the VM:

```bash
cd /opt/bondy_mst

# Sanity check — should print OTP 28 and your toolchain versions.
erl -eval 'io:format("OTP ~s~n", [erlang:system_info(otp_release)]), halt().' -noshell
elixir --version
just --version

# Pull the latest source if needed.
git pull

# Recompile if source changed. Volume-cached _build means this is
# fast except after a fresh git pull.
rebar3 compile
cd bench && mix deps.get && mix compile && cd ..

# Run benches per the QA #14 plan. `tee` the output to the volume
# so it survives machine shutdown.
mkdir -p /data/results

# Layer 1 — syscall isolation
just bench-one profile_syscalls 2>&1 | tee /data/results/layer1_syscalls_$(date +%Y%m%d_%H%M).txt

# Layer 2 — single-put microbench
just bench-one profile_pack_one_put 2>&1 | tee /data/results/layer2_pack_one_put_$(date +%Y%m%d_%H%M).txt

# Layer 3 — sustained throughput (full deployment stack)
just bench-one mst_pack_put 2>&1 | tee /data/results/layer3a_pack_put_$(date +%Y%m%d_%H%M).txt
just bench-e2e 30          2>&1 | tee /data/results/layer3b_e2e_$(date +%Y%m%d_%H%M).txt

# Companion data — run a layer-3 bench in one terminal and these
# in another (fly ssh console -C "..." or a second ssh).
vmstat 1 60       > /data/results/vmstat_$(date +%Y%m%d_%H%M).txt &
iostat -x 1 60    > /data/results/iostat_$(date +%Y%m%d_%H%M).txt &
# strace needs the BEAM pid — capture it from `ps`.
strace -c -p $(pgrep -f beam.smp) &
sleep 10
kill %3
```

## Collecting results back

```bash
# SFTP into the volume's results dir from your laptop.
fly ssh sftp shell
# At the sftp> prompt:
#   get -R /data/results ./fly-bench-results
#   quit
```

Then write the `_RESULTS.md` companion document per bench plan §5.

## Stopping the machine

```bash
fly machine stop
```

Idle cost drops to volume-only (~$1.50/month for 10 GB). Next
`fly ssh console` auto-starts within ~5 s.

## Costs (as of plan-write, 2026-05-24)

| Item                          | Rate              | Per-month (24/7) | Per-bench session |
|-------------------------------|-------------------|------------------|-------------------|
| performance-2x dedicated CPU  | $0.043/hour       | ~$32             | ~$0.10 (2h)       |
| 10 GB volume                  | $0.15/GB/month    | $1.50            | n/a (standing)    |
| Egress                        | first 100 GB free | $0               | $0                |
| **Total** (idle most of time) |                   | **~$2/month**    | **+$0.10/run**    |

If you forget to stop the machine, worst case is ~$32/month — set
a calendar reminder or use `fly machine stop` immediately after
each session.

## Caveats

The bench-plan §10 appendix lists four caveats that should be
reflected in any results doc; the key ones for the substrate:

1. **Firecracker microVM**, not bare metal. Adds ~1-3% I/O
   overhead vs the host kernel. Doesn't change the OS-specific
   vs architectural verdict at the ~70× signal level, but the
   `_RESULTS.md` environment section must note the substrate.
2. **OTP 28** — if production ships on a different OTP minor,
   the bench numbers may be off by whatever `prim_file` driver
   changes happened between versions. Re-pin the `OTP_VERSION`
   build arg when production OTP is fixed.

## Tearing down

```bash
fly apps destroy bondy-mst-bench   # also destroys the volume
```

Volume data is **not recoverable** after `apps destroy`. Pull
results first.
