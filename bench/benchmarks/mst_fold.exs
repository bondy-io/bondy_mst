Bench.setup()

# Full-tree scans. Two flavours:
#   * to_list/1            — materialise every {k, v} pair
#   * fold/3 counting only — pay only the walk cost

inputs = %{
  "tree=1k"   => 1_000,
  "tree=10k"  => 10_000,
  "tree=100k" => 100_000
}

build = fn n, store_mod, store_opts ->
  Bench.build_tree(n, store_mod, store_opts)
end

scenarios = %{
  "map_store / to_list" =>
    {fn tree -> :bondy_mst.to_list(tree) end,
     before_scenario: fn n -> build.(n, :bondy_mst_map_store, %{}) end},
  "ets_store / to_list" =>
    {fn tree -> :bondy_mst.to_list(tree) end,
     before_scenario: fn n ->
       name = "bench_fold_" <> Integer.to_string(System.unique_integer([:positive]))
       build.(n, :bondy_mst_ets_store, %{name: name, persistent: false})
     end},
  "map_store / fold count" =>
    {fn tree -> :bondy_mst.fold(tree, fn _e, acc -> acc + 1 end, 0) end,
     before_scenario: fn n -> build.(n, :bondy_mst_map_store, %{}) end}
}

Benchee.run(scenarios, [inputs: inputs] ++ Bench.benchee_opts("mst_fold"))
