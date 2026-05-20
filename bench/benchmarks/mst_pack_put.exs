Bench.setup()

# pack-store append throughput vs the batched-datasync knob.
#
# Each scenario times "build a fresh tree of N keys" against a single
# `sync_every_records` threshold. The default (K=1) preserves the
# original per-record datasync; higher K amortises fsync over more
# appends so the cost approaches encode+write only.
#
# Inputs are tree sizes; scenarios sweep K.

inputs = %{
  "N=500"  => 500,
  "N=2k"   => 2_000
}

build = fn n, opts ->
  {tree, _dir, cleanup} = Bench.PackStore.open(opts)
  keys = Bench.gen_keys(n)

  final =
    Enum.reduce(keys, tree, fn k, acc ->
      :bondy_mst.put(acc, k, k)
    end)

  cleanup.(final)
  :ok
end

scenarios = %{
  "pack_store / K=1 (per-record fsync)" =>
    fn n -> build.(n, %{sync_every_records: 1}) end,
  "pack_store / K=100" =>
    fn n -> build.(n, %{sync_every_records: 100}) end,
  "pack_store / K=1000" =>
    fn n -> build.(n, %{sync_every_records: 1_000}) end,
  "pack_store / K=infinity (flush-on-close)" =>
    fn n -> build.(n, %{sync_every_records: 1_000_000_000}) end
}

opts =
  [inputs: inputs] ++ Bench.benchee_opts("mst_pack_put", time: 4, warmup: 1)

Benchee.run(scenarios, opts)

Bench.PackStore.cleanup_root()
