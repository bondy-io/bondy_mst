# Quick smoke run — short time budget, single tree size. Used by
# `just bench-quick` to validate the harness in seconds, not minutes.

Bench.setup()

opts =
  Bench.benchee_opts("quick",
    time: 1,
    warmup: 1,
    memory_time: 0.5,
    reduction_time: 0.5
  )

Benchee.run(
  %{
    "put / map_store / tree=1k" =>
      {fn {tree, key} -> :bondy_mst.put(tree, key, key) end,
       before_each: fn _ ->
         tree = Bench.build_tree(1_000, :bondy_mst_map_store, %{})
         {tree, "k:00001001"}
       end},
    "get / map_store / tree=1k" =>
      {fn {tree, key} -> :bondy_mst.get(tree, key) end,
       before_scenario: fn _ ->
         tree = Bench.build_tree(1_000, :bondy_mst_map_store, %{})
         {tree, "k:00000500"}
       end}
  },
  [inputs: %{"smoke" => :ok}] ++ opts
)
