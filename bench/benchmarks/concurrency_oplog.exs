Bench.setup()

# Sustained-load concurrency tests against a single `bondy_oplog`
# instance. The instance gen_server is the serialisation point —
# we want to see how throughput and tail latency change as we add
# concurrent callers.
#
# Tune duration via DURATION_S env var (default 10s).

duration_s = String.to_integer(System.get_env("DURATION_S", "10"))

unique_id = fn prefix ->
  "concbench-" <>
    prefix <> "-" <> Integer.to_string(System.unique_integer([:positive]))
end

# Pre-warm an instance with N events; return the instance id and a
# tuple of event keys so the read scenarios have a sample to draw from.
make_ctx = fn prefix, n ->
  id = unique_id.(prefix)
  {:ok, _pid} = :bondy_oplog.start_instance(id)

  keys = for i <- 1..n, do: :bondy_oplog.append(id, {:warmup, i})
  :ok = :bondy_oplog.await_apply(id)

  %{id: id, keys: List.to_tuple(keys), n: n,
    cursor: :atomics.new(1, [{:signed, false}])}
end

cleanup_ctx = fn %{id: id} -> :ok = :bondy_oplog.stop_instance(id) end

# Ops.
append_op = fn %{id: id} -> :bondy_oplog.append(id, :op) end

read_op = fn %{id: id, keys: keys, n: n, cursor: cursor} ->
  i = :atomics.add_get(cursor, 1, 1)
  :bondy_oplog.get(id, elem(keys, rem(i - 1, n)))
end

# Run a scenario, building/tearing down the instance around it.
run = fn name, workloads ->
  Bench.Concurrency.run(
    name: name,
    duration_seconds: duration_s,
    setup: fn -> make_ctx.(name, 10_000) end,
    cleanup: cleanup_ctx,
    workloads: workloads
  )
end

# ----- Writers-only: contention curve on one gen_server -----

run.("oplog_writers_1", %{writers: %{count: 1, op: append_op}})
run.("oplog_writers_4", %{writers: %{count: 4, op: append_op}})
run.("oplog_writers_8", %{writers: %{count: 8, op: append_op}})
run.("oplog_writers_16", %{writers: %{count: 16, op: append_op}})

# ----- Readers-only -----

run.("oplog_readers_1", %{readers: %{count: 1, op: read_op}})
run.("oplog_readers_8", %{readers: %{count: 8, op: read_op}})
run.("oplog_readers_16", %{readers: %{count: 16, op: read_op}})

# ----- Mixed read/write -----

run.("oplog_mixed_1w_8r", %{
  writers: %{count: 1, op: append_op},
  readers: %{count: 8, op: read_op}
})

run.("oplog_mixed_8w_8r", %{
  writers: %{count: 8, op: append_op},
  readers: %{count: 8, op: read_op}
})
