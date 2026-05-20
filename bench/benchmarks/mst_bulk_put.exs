Bench.setup()

# Bulk insertion: how long does it take to build a tree of N items
# from scratch? This is the "throughput per second" workload —
# distinct from per-op `put` which holds the tree at fixed size.
#
# Three backends compared:
#
#   * map_store — pure in-memory Erlang map, baseline
#   * ets_store — ETS, baseline with concurrent-read story
#   * pack_store — durable content-addressed pack files. Two
#     fsync policies: K=1000 (production-realistic batched datasync
#     to amortise fdatasync over many appends) and K=1 (per-record
#     fsync, surfaces the conservative durability cost).
#
# pack_store cleans up its tmp directory after each iteration via the
# benchee `after_each` hook so the bench can run for many iterations
# without filling /tmp.

inputs = %{
  "N=1k"  => 1_000,
  "N=10k" => 10_000
}

build = fn n, new_tree ->
  Enum.reduce(Bench.gen_keys(n), new_tree.(), fn k, acc ->
    :bondy_mst.put(acc, k, k)
  end)
end

build_pack = fn n, k ->
  {tree, _dir, cleanup} = Bench.PackStore.open(%{sync_every_records: k})

  final =
    Enum.reduce(Bench.gen_keys(n), tree, fn key, acc ->
      :bondy_mst.put(acc, key, key)
    end)

  cleanup.(final)
  :ok
end

scenarios = %{
  "map_store / bulk insert" =>
    fn n ->
      build.(n, fn -> :bondy_mst.new(%{store: :bondy_mst_map_store}) end)
    end,
  "ets_store / bulk insert" =>
    fn n ->
      name = "bench_bulk_" <> Integer.to_string(System.unique_integer([:positive]))
      build.(n, fn ->
        :bondy_mst.new(%{
          store: :bondy_mst_ets_store,
          store_opts: %{name: name, persistent: false}
        })
      end)
    end,
  "pack_store / bulk insert (K=1000)" =>
    fn n -> build_pack.(n, 1_000) end,
  "pack_store / bulk insert (K=1)" =>
    fn n -> build_pack.(n, 1) end
}

Benchee.run(scenarios, [inputs: inputs] ++ Bench.benchee_opts("mst_bulk_put"))

Bench.PackStore.cleanup_root()
