Bench.setup()

# Bulk insertion: how long does it take to build a tree of N items
# from scratch? This is the "throughput per second" workload —
# distinct from per-op `put` which holds the tree at fixed size.

inputs = %{
  "N=1k"  => 1_000,
  "N=10k" => 10_000
}

build = fn n, new_tree ->
  Enum.reduce(Bench.gen_keys(n), new_tree.(), fn k, acc ->
    :bondy_mst.put(acc, k, k)
  end)
end

scenarios = %{
  "map_store / bulk insert" =>
    fn n ->
      build.(n, fn -> :bondy_mst.new(%{store_mod: :bondy_mst_map_store}) end)
    end,
  "ets_store / bulk insert" =>
    fn n ->
      name = "bench_bulk_" <> Integer.to_string(System.unique_integer([:positive]))
      build.(n, fn ->
        :bondy_mst.new(%{
          store_mod: :bondy_mst_ets_store,
          store_opts: %{name: name, persistent: false}
        })
      end)
    end
}

Benchee.run(scenarios, [inputs: inputs] ++ Bench.benchee_opts("mst_bulk_put"))
