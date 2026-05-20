Bench.setup()

# Pack-store multi-pack lookup benchmark.
#
# Builds a store with `packs` sealed packs of `per_pack` records each,
# then measures `bondy_mst_pack_store:get/2` latency for:
#
#   * HIT  — a hash sampled from the store; the lookup walks newest-
#     first sealed_views until the bloom + binary search hits.
#   * MISS — a random 32-byte hash that is NOT in the store; every
#     sealed pack must reject. This is the negative-lookup scenario
#     the per-pack bloom filter is designed to short-circuit.
#
# Two index modes:
#   * bloom_on  — default index build (partitioned bloom, p=0.01).
#   * bloom_off — same store, but the .idx files are rebuilt without
#     bloom (entries extracted from the bloom-on index, re-emitted
#     with `bloom: false`). The .pack bodies are untouched.
#
# We don't use Benchee — the pack reader holds prim_file fds with a
# controlling process and Benchee runs measured fns in workers
# (same constraint as mst_pack_seal.exs). Manual `:timer.tc/1` keeps
# everything in one process.

# ----- tunables -----
configs = [
  {"packs=4 / per_pack=500",  4,  500},
  {"packs=8 / per_pack=500",  8,  500},
  {"packs=16 / per_pack=500", 16, 500}
]

queries = 5_000
runs    = 5

# Wrapper records (positional access is necessary because the Erlang
# records aren't exposed in Elixir):
#   #bondy_mst_store{mod = e2, state = e3, ...}
#   #bondy_mst_pack_store{writer = e2, sealed_views = e3, ...}
#   #bondy_mst{store = e2, ...}
backend_of = fn tree ->
  store = :bondy_mst.store(tree)
  :erlang.element(3, store)
end

writer_of = fn backend ->
  :erlang.element(2, backend)
end

replace_backend = fn tree, new_backend ->
  old_store = :bondy_mst.store(tree)
  new_store = :erlang.setelement(3, old_store, new_backend)
  :erlang.setelement(2, tree, new_store)
end

# Build a store with `packs` sealed packs of `per_pack` records.
# Returns {backend, tree, cleanup, all_hashes}.
build_multi_pack = fn packs, per_pack ->
  {tree, _dir, cleanup} = Bench.PackStore.open(%{sync_every_records: 1_000})

  {final_tree, hashes_rev} =
    Enum.reduce(0..(packs - 1), {tree, []}, fn pack_ix, {acc_tree, acc} ->
      keys =
        for j <- 1..per_pack do
          "p:" <> Integer.to_string(pack_ix) <> ":" <>
            String.pad_leading(Integer.to_string(j), 8, "0")
        end

      filled = Enum.reduce(keys, acc_tree, &:bondy_mst.put(&2, &1, &1))
      backend = backend_of.(filled)
      pending = :bondy_mst_pack_writer.pending_hashes(writer_of.(backend))

      {:ok, sealed_backend} = :bondy_mst_pack_store.seal(backend)
      new_tree = replace_backend.(filled, sealed_backend)

      {new_tree, [pending | acc]}
    end)

  final_backend = backend_of.(final_tree)
  all_hashes = hashes_rev |> Enum.reverse() |> List.flatten()
  {final_backend, final_tree, cleanup, all_hashes}
end

# Rebuild every sealed .idx in the store's dir without bloom. Reads
# entries from the currently-open bloom-on index, re-emits the .idx
# with `bloom: false`, overwrites the file. Pack bodies untouched.
rebuild_idx_without_bloom = fn backend ->
  pack_ids = :bondy_mst_pack_store.sealed_pack_ids(backend)
  dir = :bondy_mst_pack_store.dir(backend)

  Enum.each(pack_ids, fn pack_id ->
    idx_path = :bondy_mst_pack_paths.sealed_idx_path(dir, pack_id)
    {:ok, idx_bin} = :prim_file.read_file(idx_path)
    {:ok, idx} = :bondy_mst_pack_index.open(idx_bin)
    entries = :bondy_mst_pack_index.entries(idx)
    new_bin =
      :bondy_mst_pack_index.build(entries, %{bloom: false})
      |> :erlang.iolist_to_binary()

    File.write!(idx_path, new_bin)
  end)

  :ok
end

sample_with_replacement = fn list, n ->
  arr = List.to_tuple(list)
  size = tuple_size(arr)
  for _ <- 1..n, do: elem(arr, :rand.uniform(size) - 1)
end

random_miss = fn -> :crypto.strong_rand_bytes(32) end

percentile = fn sorted, p ->
  idx = min(length(sorted) - 1, trunc(p / 100 * length(sorted)))
  Enum.at(sorted, idx)
end

# Returns ns-per-query.
measure_get = fn backend, hashes ->
  {us, _} =
    :timer.tc(fn ->
      Enum.each(hashes, fn h -> _ = :bondy_mst_pack_store.get(backend, h) end)
    end)

  div(us * 1_000, length(hashes))
end

IO.puts("\nmst_pack_get — multi-pack lookup bench")
IO.puts("queries per scenario: #{queries} × #{runs} runs (ns / query)")
IO.puts(String.duplicate("-", 78))

Enum.each(configs, fn {label, packs, per_pack} ->
  IO.puts("\n## #{label}")

  {backend, _tree, cleanup, all_hashes} = build_multi_pack.(packs, per_pack)

  hit_hashes  = sample_with_replacement.(all_hashes, queries)
  miss_hashes = for _ <- 1..queries, do: random_miss.()

  # --- bloom on ---
  hit_on  = for _ <- 1..runs, do: measure_get.(backend, hit_hashes)
  miss_on = for _ <- 1..runs, do: measure_get.(backend, miss_hashes)

  # --- bloom off ---
  :ok = rebuild_idx_without_bloom.(backend)

  dir = :bondy_mst_pack_store.dir(backend)
  instance_id = :bondy_mst_pack_store.instance_id(backend)
  :ok = :bondy_mst_pack_store.close(backend)

  reopened =
    :bondy_mst_pack_store.open(
      :sha256,
      %{dir: dir, instance_id: instance_id}
    )

  hit_off  = for _ <- 1..runs, do: measure_get.(reopened, hit_hashes)
  miss_off = for _ <- 1..runs, do: measure_get.(reopened, miss_hashes)

  :ok = :bondy_mst_pack_store.close(reopened)
  # cleanup closure expects the tree, but the backend has been closed —
  # pass the original tree value; cleanup wraps close in a try/catch.
  _ = File.rm_rf(dir)

  stats = fn samples ->
    sorted = Enum.sort(samples)
    {Enum.min(sorted), percentile.(sorted, 50), percentile.(sorted, 90)}
  end

  rows = [
    {"hit  (bloom on)",  stats.(hit_on)},
    {"hit  (bloom off)", stats.(hit_off)},
    {"miss (bloom on)",  stats.(miss_on)},
    {"miss (bloom off)", stats.(miss_off)}
  ]

  IO.puts(String.pad_trailing("scenario", 24) <> "    min       p50       p90")

  Enum.each(rows, fn {name, {mn, p50, p90}} ->
    IO.puts(
      String.pad_trailing(name, 24) <>
        "  " <> String.pad_leading("#{mn}", 8) <>
        "  " <> String.pad_leading("#{p50}", 8) <>
        "  " <> String.pad_leading("#{p90}", 8)
    )
  end)

  {_, miss_on_med, _} = elem(Enum.at(rows, 2), 1)
  {_, miss_off_med, _} = elem(Enum.at(rows, 3), 1)

  speedup =
    if miss_on_med > 0,
      do: Float.round(miss_off_med / miss_on_med, 2),
      else: 0.0

  IO.puts("  → bloom miss speedup: #{speedup}x (p50)")
  _ = cleanup
end)

Bench.PackStore.cleanup_root()
