Bench.setup()

# Pack-store `seal/1` throughput as a function of incoming-pack size.
#
# Seal sorts the pending hash list, streams every body out of
# `incoming.pack`, hashes + encodes records into `pack-NNNN.pack.tmp`,
# fsyncs, and atomically swaps the manifest. The streaming refactor
# keeps peak RAM at O(idx + max page size); this bench measures wall
# time so we can spot regressions and confirm seal is dominated by
# fsync + write cost, not memory.
#
# We don't use Benchee here because the pack writer holds `prim_file`
# fds with a controlling process: Benchee runs the measured fn in a
# worker process, so any pread on a fd opened by `before_each` fails
# with `:not_on_controlling_process`. Manual `:timer.tc/1` keeps the
# whole open → build → seal cycle in one process.

# Tunables.
inputs       = [{"incoming=500", 500}, {"incoming=2k", 2_000}]
samples      = 10
build_opts   = %{sync_every_records: 1_000}

# Extract the pack_store backend from the bondy_mst store wrapper.
# Wrapper record: {bondy_mst_store, Mod, State, Transactions}.
backend = fn tree ->
  store = :bondy_mst.store(tree)
  :erlang.element(3, store)
end

# Build a fresh tree of N pages, seal it once, return seal wall time
# in microseconds.
measure_one_seal = fn n ->
  {tree, _dir, cleanup} = Bench.PackStore.build(n, build_opts)

  {us, result} =
    :timer.tc(fn -> :bondy_mst_pack_store.seal(backend.(tree)) end)

  case result do
    {:ok, _sealed} -> :ok
    other -> raise "seal failed: #{inspect(other)}"
  end

  # The post-seal store handle owns the new incoming.pack fd; we discard
  # it (cleanup just rms the dir). The pre-seal `tree` still references
  # the closed writer state, which is fine.
  cleanup.(tree)
  us
end

percentile = fn sorted, p ->
  idx = min(length(sorted) - 1, trunc(p / 100 * length(sorted)))
  Enum.at(sorted, idx)
end

IO.puts("\nseal benchmark (#{samples} samples per input, K=1000 during build)")
IO.puts(String.duplicate("-", 70))
IO.puts(String.pad_trailing("input", 16) <> "  min      p50      p90      p99      max")

# Discard one warmup sample per input to avoid cold-cache skew.
results =
  for {label, n} <- inputs do
    _warmup = measure_one_seal.(n)

    samples_us =
      for _ <- 1..samples, do: measure_one_seal.(n)

    sorted = Enum.sort(samples_us)

    fmt = fn us -> String.pad_leading("#{div(us, 1000)} ms", 8) end

    IO.puts(
      String.pad_trailing(label, 16) <>
        "  " <> fmt.(Enum.min(sorted)) <>
        " " <> fmt.(percentile.(sorted, 50)) <>
        " " <> fmt.(percentile.(sorted, 90)) <>
        " " <> fmt.(percentile.(sorted, 99)) <>
        " " <> fmt.(Enum.max(sorted))
    )

    {label, sorted}
  end

IO.puts("")
Bench.PackStore.cleanup_root()
results
