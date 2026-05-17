Bench.setup()

# Insert a fresh key into a pre-built tree of size N. The hook
# rebuilds the tree per iteration so we measure single-`put` cost,
# not amortised insertion.

inputs = %{
  "tree=1k"   => 1_000,
  "tree=10k"  => 10_000,
  "tree=100k" => 100_000
}

scenarios = %{
  "map_store / put" =>
    {fn {tree, key} -> :bondy_mst.put(tree, key, key) end,
     before_each: fn n ->
       tree = Bench.build_tree(n, :bondy_mst_map_store, %{})
       next = n + 1
       k = "k:" <> String.pad_leading(Integer.to_string(next), 8, "0")
       {tree, k}
     end},
  "ets_store / put" =>
    {fn {tree, key} -> :bondy_mst.put(tree, key, key) end,
     before_each: fn n ->
       name = "bench_ets_" <> Integer.to_string(System.unique_integer([:positive]))
       tree = Bench.build_tree(n, :bondy_mst_ets_store, %{name: name, persistent: false})
       next = n + 1
       k = "k:" <> String.pad_leading(Integer.to_string(next), 8, "0")
       {tree, k}
     end}
}

Benchee.run(scenarios, [inputs: inputs] ++ Bench.benchee_opts("mst_put"))
