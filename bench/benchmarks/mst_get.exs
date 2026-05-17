Bench.setup()

# Random-key lookup against a tree pre-populated to size N. The tree
# is built once per input via `before_scenario`; each iteration picks
# a key from a pre-shuffled list at index `:counters`-tracked offset.

inputs = %{
  "tree=1k"   => 1_000,
  "tree=10k"  => 10_000,
  "tree=100k" => 100_000
}

make_state = fn n, store_mod, store_opts ->
  tree = Bench.build_tree(n, store_mod, store_opts)
  keys = Bench.gen_keys_shuffled(n)
  cursor = :atomics.new(1, [{:signed, false}])
  %{tree: tree, keys: List.to_tuple(keys), n: n, cursor: cursor}
end

next_key = fn %{keys: keys, n: n, cursor: cursor} ->
  idx = :atomics.add_get(cursor, 1, 1)
  elem(keys, rem(idx - 1, n))
end

scenarios = %{
  "map_store / get hit" =>
    {fn state -> :bondy_mst.get(state.tree, next_key.(state)) end,
     before_scenario: fn n -> make_state.(n, :bondy_mst_map_store, %{}) end},
  "ets_store / get hit" =>
    {fn state -> :bondy_mst.get(state.tree, next_key.(state)) end,
     before_scenario: fn n ->
       name = "bench_get_" <> Integer.to_string(System.unique_integer([:positive]))
       make_state.(n, :bondy_mst_ets_store, %{name: name, persistent: false})
     end},
  "map_store / get miss" =>
    {fn state -> :bondy_mst.get(state.tree, "miss:" <> next_key.(state)) end,
     before_scenario: fn n -> make_state.(n, :bondy_mst_map_store, %{}) end}
}

Benchee.run(scenarios, [inputs: inputs] ++ Bench.benchee_opts("mst_get"))
