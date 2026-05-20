Bench.setup()

# Cross-store lookup benchmark: bondy_mst.get/2 over the three
# supported store backends.
#
#   * map_store  — pure in-memory Erlang map.
#   * ets_store  — ETS-backed (read_concurrency option not enabled
#                  here; default config).
#   * pack_store — content-addressed pack files. Built with periodic
#                  seals so the lookup path actually traverses several
#                  sealed packs (otherwise everything sits in pending
#                  and bloom is bypassed). Defaults: K=1000, bloom on.
#
# Measures hit and miss latency. For pack_store, miss is the
# negative-lookup case that the bloom filter is sized for.
#
# Manual `:timer.tc/1` rather than Benchee because the pack reader's
# prim_file fds have a controlling process and Benchee runs measured
# fns in worker processes (same constraint as mst_pack_seal.exs).

# ----- tunables -----
n_keys       = 10_000
seal_every   = 1_000      # pack_store sealing cadence during build
queries      = 20_000
runs         = 10

# Wrapper record positional helpers (cheaper than exposing accessors).
backend_of = fn tree ->
  store = :bondy_mst.store(tree)
  :erlang.element(3, store)
end

replace_backend = fn tree, new_backend ->
  old_store = :bondy_mst.store(tree)
  new_store = :erlang.setelement(3, old_store, new_backend)
  :erlang.setelement(2, tree, new_store)
end

# ---- Builders ----

build_map = fn n ->
  tree = :bondy_mst.new(%{store: :bondy_mst_map_store})

  populated =
    Enum.reduce(Bench.gen_keys(n), tree, fn k, acc ->
      :bondy_mst.put(acc, k, k)
    end)

  cleanup = fn -> :ok end
  {populated, cleanup}
end

build_ets = fn n ->
  name = "bench_stores_" <> Integer.to_string(System.unique_integer([:positive]))

  tree =
    :bondy_mst.new(%{
      store: :bondy_mst_ets_store,
      store_opts: %{name: name, persistent: false}
    })

  populated =
    Enum.reduce(Bench.gen_keys(n), tree, fn k, acc ->
      :bondy_mst.put(acc, k, k)
    end)

  cleanup = fn -> :bondy_mst_store.close(:bondy_mst.store(populated)) end
  {populated, cleanup}
end

# pack_store: build N records, sealing every `seal_every` so we end
# up with several sealed packs (negative lookups must traverse them).
build_pack = fn n, seal_every ->
  {tree, dir, _open_cleanup} =
    Bench.PackStore.open(%{sync_every_records: 1_000})

  keys = Bench.gen_keys(n)
  chunks = Enum.chunk_every(keys, seal_every)

  final_tree =
    Enum.reduce(chunks, tree, fn chunk, acc_tree ->
      filled = Enum.reduce(chunk, acc_tree, &:bondy_mst.put(&2, &1, &1))
      backend = backend_of.(filled)
      {:ok, sealed} = :bondy_mst_pack_store.seal(backend)
      replace_backend.(filled, sealed)
    end)

  cleanup = fn ->
    try do
      :bondy_mst_store.close(:bondy_mst.store(final_tree))
    rescue _ -> :ok
    catch _, _ -> :ok
    end

    _ = File.rm_rf(dir)
    :ok
  end

  {final_tree, cleanup}
end

# ---- Workload generation ----

# Stable shuffled subset of keys for hits.
hit_keys =
  Bench.gen_keys(n_keys)
  |> Enum.shuffle()
  |> Enum.take(queries)

# Keys with the same generator pattern but indices outside [1, n_keys] —
# guaranteed misses without colliding with hits by accident.
miss_keys =
  for i <- (n_keys + 1)..(n_keys + queries) do
    width = max(8, byte_size(Integer.to_string(n_keys)))
    "k:" <> String.pad_leading(Integer.to_string(i), width, "0")
  end

# ---- Measurement ----

percentile = fn sorted, p ->
  idx = min(length(sorted) - 1, trunc(p / 100 * length(sorted)))
  Enum.at(sorted, idx)
end

measure = fn tree, keys ->
  {us, _} =
    :timer.tc(fn ->
      Enum.each(keys, fn k -> _ = :bondy_mst.get(tree, k) end)
    end)

  div(us * 1_000, length(keys))
end

stats = fn samples ->
  sorted = Enum.sort(samples)
  {Enum.min(sorted), percentile.(sorted, 50), percentile.(sorted, 90)}
end

fmt = fn n -> n |> Integer.to_string() |> String.pad_leading(8) end

print_row = fn label, {mn, p50, p90} ->
  IO.puts(
    String.pad_trailing(label, 22) <>
      "  " <> fmt.(mn) <>
      "  " <> fmt.(p50) <>
      "  " <> fmt.(p90)
  )
end

# Run each scenario in turn, since pack_store needs disk and we don't
# want concurrent /tmp churn.
scenarios = [
  {"map_store",
   fn -> build_map.(n_keys) end},
  {"ets_store",
   fn -> build_ets.(n_keys) end},
  {"pack_store (bloom on)",
   fn -> build_pack.(n_keys, seal_every) end}
]

IO.puts("\nmst_store_get — bondy_mst.get/2 across backends")
IO.puts("n_keys = #{n_keys}, queries = #{queries} × #{runs} runs (ns / query)")
IO.puts("pack_store: K=1000 fsync batching, seal every #{seal_every} records")
IO.puts(String.duplicate("-", 70))

results =
  Enum.map(scenarios, fn {name, build_fn} ->
    {tree, cleanup} = build_fn.()

    hit_runs  = for _ <- 1..runs, do: measure.(tree, hit_keys)
    miss_runs = for _ <- 1..runs, do: measure.(tree, miss_keys)

    hit_stats  = stats.(hit_runs)
    miss_stats = stats.(miss_runs)

    cleanup.()

    {name, hit_stats, miss_stats}
  end)

IO.puts(String.pad_trailing("scenario", 22) <> "    min       p50       p90")

Enum.each(results, fn {name, hit, miss} ->
  print_row.(name <> " / hit", hit)
  print_row.(name <> " / miss", miss)
end)

IO.puts("\nrelative to map_store / hit (p50):")
{_, {_, map_hit_p50, _}, _} = Enum.find(results, fn {n, _, _} -> n == "map_store" end)

Enum.each(results, fn {name, {_, hit_p50, _}, {_, miss_p50, _}} ->
  hit_ratio  = Float.round(hit_p50 / map_hit_p50, 2)
  miss_ratio = Float.round(miss_p50 / map_hit_p50, 2)

  IO.puts(
    String.pad_trailing(name, 22) <>
      " — hit: " <> Float.to_string(hit_ratio) <> "x   " <>
      "miss: " <> Float.to_string(miss_ratio) <> "x"
  )
end)

Bench.PackStore.cleanup_root()
