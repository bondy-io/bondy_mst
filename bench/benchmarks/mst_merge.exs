Bench.setup()

# CRDT merge — joins two MSTs of the same size with different keys.
# Measures the bandwidth-conserving path (Merkle equality skip) and
# the worst case (fully disjoint keys).

inputs = %{
  "size=1k"  => 1_000,
  "size=10k" => 10_000
}

build_disjoint = fn n, store_mod, store_opts_a, store_opts_b ->
  tree_a =
    Enum.reduce(1..n, :bondy_mst.new(%{store_mod: store_mod, store_opts: store_opts_a}),
      fn i, acc ->
        k = "a:" <> String.pad_leading(Integer.to_string(i), 8, "0")
        :bondy_mst.put(acc, k, k)
      end
    )

  tree_b =
    Enum.reduce(1..n, :bondy_mst.new(%{store_mod: store_mod, store_opts: store_opts_b}),
      fn i, acc ->
        k = "b:" <> String.pad_leading(Integer.to_string(i), 8, "0")
        :bondy_mst.put(acc, k, k)
      end
    )

  {tree_a, tree_b}
end

build_identical = fn n, store_mod, store_opts_a, store_opts_b ->
  base = Bench.build_tree(n, store_mod, store_opts_a)
  twin = Bench.build_tree(n, store_mod, store_opts_b)
  {base, twin}
end

scenarios = %{
  "map_store / merge disjoint" =>
    {fn {a, b} -> :bondy_mst.merge(a, b) end,
     before_scenario: fn n -> build_disjoint.(n, :bondy_mst_map_store, %{}, %{}) end},
  "map_store / merge identical" =>
    {fn {a, b} -> :bondy_mst.merge(a, b) end,
     before_scenario: fn n -> build_identical.(n, :bondy_mst_map_store, %{}, %{}) end},
  "ets_store / merge disjoint" =>
    {fn {a, b} -> :bondy_mst.merge(a, b) end,
     before_scenario: fn n ->
       sa = "bench_merge_a_" <> Integer.to_string(System.unique_integer([:positive]))
       sb = "bench_merge_b_" <> Integer.to_string(System.unique_integer([:positive]))
       build_disjoint.(
         n,
         :bondy_mst_ets_store,
         %{name: sa, persistent: false},
         %{name: sb, persistent: false}
       )
     end}
}

Benchee.run(scenarios, [inputs: inputs] ++ Bench.benchee_opts("mst_merge"))
