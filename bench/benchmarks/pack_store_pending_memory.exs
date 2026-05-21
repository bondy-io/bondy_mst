Bench.setup()

# Pack-store pending-map memory audit — pack-store QA #15.
#
# The writer's pending map keeps every live page body resident until
# seal, bounded per-instance by `auto_seal_bytes` (16 MB default).
# `bondy_db` can host many instances simultaneously (per-entity
# topology with N shards per entity). The question this bench
# answers: does opening many pack-store instances under realistic
# write pressure stay within the M*N*auto_seal_bytes budget, or does
# the BEAM-side overhead inflate that?
#
# Methodology:
#   - For each `N` instance count, open N pack-store instances.
#   - Drive each instance with `PER_INSTANCE_PUTS` puts so the
#     pending map grows toward (but stays below) `auto_seal_bytes`.
#   - Sample `:erlang.memory()` before, after open, after fill.
#   - Sum `pending_bytes` across instances via
#     `:bondy_mst_pack_store.info/1` (the on-disk lower bound for
#     resident memory).
#   - Report actual delta vs the sum-of-pending-bytes lower bound
#     and vs the worst-case M*auto_seal_bytes budget.
#
# Auto-seal is disabled (`auto_seal_records => :infinity,
# auto_seal_bytes => :infinity`) so the pending map grows
# monotonically and the bench observes the peak before any seal.
# This is the worst-case scenario per instance.

ns_env = System.get_env("INSTANCE_COUNTS", "1,4,16,32")
ns =
  ns_env
  |> String.split(",", trim: true)
  |> Enum.map(&String.to_integer/1)

per_instance_puts = String.to_integer(System.get_env("PER_INSTANCE_PUTS", "8000"))

IO.puts(
  "[pack_store_pending_memory] instance_counts=#{inspect(ns)} " <>
    "per_instance_puts=#{per_instance_puts}"
)

# -- helpers ---------------------------------------------------------

mb = fn bytes -> Float.round(bytes / (1024 * 1024), 2) end

# Reach the pack_store backend out of a :bondy_mst.tree.
pack_info = fn tree ->
  store = :bondy_mst.store(tree)
  {:bondy_mst_store, _, backend, _} = store
  :bondy_mst_pack_store.info(backend)
end

# Open a fresh pack-store-backed tree with auto-seal disabled.
open_no_autoseal = fn ->
  Bench.PackStore.open(%{
    sync_every_records: 1000,
    sync_every_ms: :infinity,
    root_flush_every_records: 1000,
    root_flush_every_ms: :infinity,
    auto_seal_records: :infinity,
    auto_seal_bytes: :infinity
  })
end

# Pre-build a list of distinct keys; each put goes into a distinct
# slot so the writer's pending map grows by ~1 entry per put. Using
# integer keys keeps the value small relative to the MST page
# overhead, so the dominant memory cost is the page itself.
keys = fn n ->
  for i <- 1..n do
    "k:" <> String.pad_leading(Integer.to_string(i), 8, "0")
  end
end

fill = fn tree, ks ->
  Enum.reduce(ks, tree, fn k, acc -> :bondy_mst.put(acc, k, k) end)
end

snapshot = fn ->
  :erlang.garbage_collect()
  mem = :erlang.memory()
  Keyword.new(mem)
end

mem_total = fn snap -> Keyword.fetch!(snap, :total) end
mem_processes = fn snap -> Keyword.fetch!(snap, :processes) end
mem_binary = fn snap -> Keyword.fetch!(snap, :binary) end

ks = keys.(per_instance_puts)

run_one = fn n ->
  IO.puts("\n--- N=#{n} instances ----------------------------------------")

  # Baseline: VM at rest (no instances open yet).
  base = snapshot.()

  # Open N instances.
  instances =
    for _ <- 1..n do
      open_no_autoseal.()
    end

  after_open = snapshot.()

  # Fill each instance with `per_instance_puts` puts.
  populated =
    Enum.map(instances, fn {tree, dir, cleanup} ->
      tree1 = fill.(tree, ks)
      {tree1, dir, cleanup}
    end)

  after_fill = snapshot.()

  # Per-instance pending byte total.
  pending_total =
    Enum.reduce(populated, 0, fn {tree, _dir, _cleanup}, acc ->
      info = pack_info.(tree)
      acc + Map.get(info, :pending_bytes, 0)
    end)

  # Average pending bytes per instance.
  avg_pending = if n > 0, do: pending_total / n, else: 0

  # Delta from baseline.
  delta_total = mem_total.(after_fill) - mem_total.(base)
  delta_open = mem_total.(after_open) - mem_total.(base)
  delta_fill = mem_total.(after_fill) - mem_total.(after_open)

  # Amplification: how many bytes of BEAM memory we burned per byte of
  # pending-pack content. 1.0 == perfect; > 1.0 == map/record overhead;
  # < 1.0 == some sharing (e.g. binary refcounting on identical keys).
  amplification =
    if pending_total > 0, do: delta_fill / pending_total, else: 0.0

  IO.puts(
    "  total memory:        base=#{mb.(mem_total.(base))} MB  " <>
      "after_open=#{mb.(mem_total.(after_open))} MB  " <>
      "after_fill=#{mb.(mem_total.(after_fill))} MB"
  )

  IO.puts(
    "  delta:               open=#{mb.(delta_open)} MB  " <>
      "fill=#{mb.(delta_fill)} MB  " <>
      "total=#{mb.(delta_total)} MB"
  )

  IO.puts(
    "  binary delta:        " <>
      "#{mb.(mem_binary.(after_fill) - mem_binary.(base))} MB"
  )

  IO.puts(
    "  process delta:       " <>
      "#{mb.(mem_processes.(after_fill) - mem_processes.(base))} MB"
  )

  IO.puts(
    "  per-instance avg:    " <>
      "pending=#{mb.(avg_pending)} MB  " <>
      "fill_delta=#{Float.round(delta_fill / n / (1024 * 1024), 2)} MB"
  )

  IO.puts(
    "  pending sum:         #{mb.(pending_total)} MB  " <>
      "(M*auto_seal_bytes budget at default = #{mb.(n * 16_000_000)} MB)"
  )

  IO.puts(
    "  amplification:       #{Float.round(amplification, 2)}x " <>
      "(fill delta / pending sum)"
  )

  # Cleanup.
  Enum.each(populated, fn {tree, _dir, cleanup} -> cleanup.(tree) end)

  %{
    n: n,
    base_mb: mb.(mem_total.(base)),
    after_open_mb: mb.(mem_total.(after_open)),
    after_fill_mb: mb.(mem_total.(after_fill)),
    delta_fill_mb: mb.(delta_fill),
    pending_sum_mb: mb.(pending_total),
    avg_pending_mb: mb.(avg_pending),
    amplification: Float.round(amplification, 2)
  }
end

results = Enum.map(ns, run_one)

IO.puts("\n=== summary ===")

IO.puts(
  String.pad_trailing("N", 6) <>
    String.pad_trailing("base_mb", 12) <>
    String.pad_trailing("fill_mb", 12) <>
    String.pad_trailing("delta_mb", 12) <>
    String.pad_trailing("pending_mb", 14) <>
    String.pad_trailing("avg_pend_mb", 14) <>
    "amp"
)

for r <- results do
  IO.puts(
    String.pad_trailing(Integer.to_string(r.n), 6) <>
      String.pad_trailing(:erlang.float_to_binary(r.base_mb / 1.0, decimals: 2), 12) <>
      String.pad_trailing(:erlang.float_to_binary(r.after_fill_mb / 1.0, decimals: 2), 12) <>
      String.pad_trailing(:erlang.float_to_binary(r.delta_fill_mb / 1.0, decimals: 2), 12) <>
      String.pad_trailing(:erlang.float_to_binary(r.pending_sum_mb / 1.0, decimals: 2), 14) <>
      String.pad_trailing(:erlang.float_to_binary(r.avg_pending_mb / 1.0, decimals: 2), 14) <>
      :erlang.float_to_binary(r.amplification / 1.0, decimals: 2)
  )
end

# Always sweep the root bench dir at the end so /tmp doesn't bloat.
Bench.PackStore.cleanup_root()
