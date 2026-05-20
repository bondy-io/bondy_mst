Bench.setup()

# Microbench + fprof profile for follow-up #12 in
# memory/project_mst_pack_store_qa_2026_05_20.md.
#
# Goal: measure per-put cost on three MST backends in isolation
# (no WAL, no projection, no oplog gen_server) and use fprof to
# rank the dominant functions inside the pack-store hot path.
#
# Configurations:
#   - ets   : reference baseline
#   - pack defaults (sync_every_records=32, root_flush_every_records=32)
#   - pack relaxed  (sync_every_records=1000, root_flush_every_records=1000)
#     to separate the fsync cost from the rest of the work
#   - pack strict   (sync_every_records=1, root_flush_every_records=1)
#     to surface the worst-case per-put fsync cost as a floor
#
# Each run inserts `n_puts` *new* keys into a freshly-built tree of
# `prepopulate` entries — same shape as the e2e applier (steady-state
# put on a non-empty tree, not initial build).

n_puts = String.to_integer(System.get_env("N_PUTS", "2000"))
prepopulate = String.to_integer(System.get_env("PREPOPULATE", "5000"))
fprof = System.get_env("FPROF", "0") == "1"

IO.puts(
  "[profile_mst_put] n_puts=#{n_puts} prepopulate=#{prepopulate} fprof=#{fprof}"
)

# ----- shared helpers -----

bench_keys = fn n, offset ->
  width = max(8, byte_size(Integer.to_string(offset + n)))

  for i <- (offset + 1)..(offset + n) do
    "k:" <> String.pad_leading(Integer.to_string(i), width, "0")
  end
end

time_puts = fn tree, keys ->
  {usec, final} =
    :timer.tc(fn ->
      Enum.reduce(keys, tree, fn k, acc -> :bondy_mst.put(acc, k, k) end)
    end)

  {usec, final}
end

report = fn label, usec, n ->
  ops_per_s = if usec > 0, do: trunc(n * 1_000_000 / usec), else: 0
  us_per_op = if n > 0, do: usec / n, else: 0.0

  IO.puts(
    :io_lib.format(
      "  ~-44s  total=~7w µs   ~9w ops/s   ~7.2f µs/op",
      [label, usec, ops_per_s, us_per_op]
    )
  )
end

# ----- build initial trees in each backend -----

build_ets = fn ->
  name = "prof_ets_" <> Integer.to_string(System.unique_integer([:positive]))
  Bench.build_tree(prepopulate, :bondy_mst_ets_store, %{name: name, persistent: false})
end

build_pack = fn opts_map ->
  {tree, _dir, cleanup} = Bench.PackStore.build(prepopulate, opts_map)
  {tree, cleanup}
end

# Workload keys are fresh — new puts on a pre-populated tree.
new_keys = bench_keys.(n_puts, prepopulate)

# ----- run each backend -----

IO.puts("\n=== gross timings (each backend, single shot, hot cache) ===")

# ets baseline
ets_tree = build_ets.()
{ets_us, _} = time_puts.(ets_tree, new_keys)
report.("ets_store", ets_us, n_puts)

# pack defaults — current shipping config
{pack_def_tree, pack_def_cleanup} =
  build_pack.(%{sync_every_records: 32, root_flush_every_records: 32})

{pack_def_us, pack_def_final} = time_puts.(pack_def_tree, new_keys)
report.("pack_store / defaults (32/32)", pack_def_us, n_puts)
pack_def_cleanup.(pack_def_final)

# pack relaxed — fsyncs as rare as possible without disabling them entirely
{pack_relax_tree, pack_relax_cleanup} =
  build_pack.(%{sync_every_records: 1000, root_flush_every_records: 1000})

{pack_relax_us, pack_relax_final} = time_puts.(pack_relax_tree, new_keys)
report.("pack_store / relaxed (1k/1k)", pack_relax_us, n_puts)
pack_relax_cleanup.(pack_relax_final)

# pack strict — per-record fsync, worst case
{pack_strict_tree, pack_strict_cleanup} =
  build_pack.(%{sync_every_records: 1, root_flush_every_records: 1})

{pack_strict_us, pack_strict_final} = time_puts.(pack_strict_tree, new_keys)
report.("pack_store / strict (1/1)", pack_strict_us, n_puts)
pack_strict_cleanup.(pack_strict_final)

IO.puts("\n=== gap vs ets ===")

cond do
  ets_us > 0 ->
    [
      {"defaults (32/32)", pack_def_us},
      {"relaxed (1k/1k)", pack_relax_us},
      {"strict  (1/1)", pack_strict_us}
    ]
    |> Enum.each(fn {lbl, us} ->
      gap = if ets_us > 0, do: Float.round(us / ets_us, 1), else: 0.0
      IO.puts("  pack #{lbl}: #{gap}× ets")
    end)

  true ->
    IO.puts("  ets baseline too fast to compare")
end

# ----- optional fprof on the relaxed config -----
#
# fprof captures call-graph timings: a single hot-spot function will
# stand out clearly. We profile the relaxed config so the data isn't
# drowned by fsync cost.

if fprof do
  IO.puts("\n=== eprof (pack defaults, hot path) ===")
  tools_ebin = "/Users/aramallo/otp/28.3.1/lib/tools-4.1.3/ebin"
  if File.dir?(tools_ebin), do: Code.prepend_path(tools_ebin)
  Application.ensure_all_started(:tools)

  {tree2, _dir2, cleanup2} =
    Bench.PackStore.open(%{sync_every_records: 32, root_flush_every_records: 32})

  # Pre-populate without profiling.
  warm_keys = bench_keys.(prepopulate, 0)

  warm_tree =
    Enum.reduce(warm_keys, tree2, fn k, acc -> :bondy_mst.put(acc, k, k) end)

  prof_keys = bench_keys.(n_puts, prepopulate)

  :eprof.start()
  :eprof.start_profiling([self()])

  final_tree =
    Enum.reduce(prof_keys, warm_tree, fn k, acc -> :bondy_mst.put(acc, k, k) end)

  :eprof.stop_profiling()

  out_path = "/tmp/profile_mst_put_eprof.analysis"
  _ = File.rm(out_path)
  :eprof.log(String.to_charlist(out_path))
  :eprof.analyze(:total, sort: :time)
  :eprof.stop()
  IO.puts("  eprof log written to #{out_path}")

  cleanup2.(final_tree)
end

Bench.PackStore.cleanup_root()
