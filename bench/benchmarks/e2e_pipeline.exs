Bench.setup()

# End-to-end pipeline benchmark.
#
# Provisions a multi-shard `bondy_db_core` substrate against
# `Bench.ProjectionEts` (in-memory) and `:bondy_oplog_cache_ets`,
# then starts a `bondy_oplog` instance per shard with the substrate
# wired as the applier's `cell_apply_target`. Writes flow:
#
#     client → :bondy_oplog.append({cell_apply, B, K, Event})
#            → WAL → applier.drain → fold → ProjectionEts.put_batch
#
# Reads flow through the substrate: cache-fast, projection on miss
# (which populates the cache).
#
# Tune via env: DURATION_S (default 10), WARMUP_MS (default 500),
# SHARDS (default 4), PREPOPULATE (default 10000), WRITERS (default 4),
# READERS (default 8).

duration_s = String.to_integer(System.get_env("DURATION_S", "10"))
warmup_ms = String.to_integer(System.get_env("WARMUP_MS", "500"))
shard_count = String.to_integer(System.get_env("SHARDS", "4"))
prepopulate = String.to_integer(System.get_env("PREPOPULATE", "10000"))
writers = String.to_integer(System.get_env("WRITERS", "4"))
readers = String.to_integer(System.get_env("READERS", "8"))

# WAL durability mode for the oplog instances the bench starts. Each
# shard's WAL is a separate gen_server with its own fsync cadence;
# `per_write` fsyncs after every batch frame, `batched` lets the WAL
# coalesce multiple frames into one fsync (`batched_fsync_interval` /
# `batched_fsync_bytes` thresholds). `per_write` is the default for
# durability; switch to `batched` to compare against the
# `concurrency_wal` bench's high-throughput numbers.
wal_fsync_mode =
  case System.get_env("WAL_FSYNC", "per_write") do
    "batched" -> :batched
    _ -> :per_write
  end

# Writer batch size. `1` = one event per `append/2` call (the original
# bench shape). Anything > 1 routes the writer through
# `append_many/2`, which lets the WAL coalesce multiple events into a
# single frame and a single fsync — the combination needed to
# approach the `wal_batched_batch16_8` 1.2M events/s number.
batch_size = String.to_integer(System.get_env("BATCH_SIZE", "1"))

# Applier→instance demand cap. Default 16. Set to 2 to demonstrate
# the gate firing aggressively (writer should backpressure quickly);
# set to a very large value to disable the cap and reproduce the
# pre-flow-control behaviour.
max_in_flight = String.to_integer(System.get_env("MAX_IN_FLIGHT", "16"))

IO.puts(
  "[e2e] config: shards=#{shard_count} writers=#{writers} readers=#{readers} " <>
    "fsync=#{wal_fsync_mode} batch_size=#{batch_size} " <>
    "dirty_io_schedulers=#{:erlang.system_info(:dirty_io_schedulers)}"
)

# Backends to compare. `BACKENDS=ets,leveled` runs each scenario
# twice; `BACKENDS=ets` runs only the in-memory path. `leveled` is
# skipped automatically when the bench profile hasn't been compiled
# yet (so a plain `mix run` on a fresh checkout still works).
backends =
  System.get_env("BACKENDS", "ets,leveled")
  |> String.split(",", trim: true)
  |> Enum.map(&String.to_atom/1)
  |> Enum.filter(fn
    :leveled -> Bench.leveled_available?() or (IO.puts(
                  "[e2e] skipping leveled backend (run `rebar3 as bench compile`)"
                ) && false)
    _ -> true
  end)

bucket = ""
fold = :bondy_oplog_fold_lww_register

leveled_root = "/tmp/bondy_mst_bench_leveled/#{:os.getpid()}"

# When true, swap `:bondy_oplog_cache_ets` for `Bench.CacheNoop` so
# every read falls through to the projection adapter. Isolates raw
# ETS vs leveled projection-read performance — the default cache
# absorbs ~99.8% of reads with a 10k key space, so without bypass the
# bench measures the cache, not the backend.
bypass_cache? = System.get_env("BYPASS_CACHE", "false") in ["1", "true"]

{cache_adapter, cache_label} =
  if bypass_cache? do
    {Bench.CacheNoop, "_nocache"}
  else
    {:bondy_oplog_cache_ets, ""}
  end

unique_ns = fn prefix ->
  String.to_atom(
    "bench_e2e_" <>
      prefix <> "_" <> Integer.to_string(System.unique_integer([:positive]))
  )
end

unique_prefix = fn prefix ->
  "bench-e2e-" <>
    prefix <> "-" <> Integer.to_string(System.unique_integer([:positive]))
end

# Same routing the substrate uses internally.
shard_for = fn key -> :erlang.phash2({bucket, key}, shard_count) end

# Per-shard projection handle/bookie/etc. for a given backend.
# Returns `{adapter_module, projection_handle, bookie_or_nil}`. The
# bookie pid is held in ctx so cleanup can shut it down.
open_projection = fn
  :ets, ns, shard ->
    {:ok, ph} = Bench.ProjectionEts.open(ns, :primary, shard, %{})
    {Bench.ProjectionEts, ph, nil}

  :leveled, ns, shard ->
    # Per-shard leveled Bookie. Each bookie owns its own
    # journal/ledger files under leveled_root/<scenario>/<shard>.
    # Small per-shard cache + journal to keep the bench
    # initialisation snappy; the goal is to measure steady-state, not
    # cold-start.
    dir =
      Path.join([leveled_root, Atom.to_string(ns), Integer.to_string(shard)])

    File.mkdir_p!(dir)

    {:ok, bookie} =
      :leveled_bookie.book_start([
        {:root_path, String.to_charlist(dir)},
        {:max_journalsize, 1_000_000_000},
        {:cache_size, 2_000},
        {:sync_strategy, :none}
      ])

    {:ok, ph} =
      Bench.ProjectionLeveled.open(ns, :primary, shard, %{bookie: bookie})

    {Bench.ProjectionLeveled, ph, bookie}
end

close_projection = fn
  Bench.ProjectionEts, ph, _ ->
    Bench.ProjectionEts.close(ph)

  Bench.ProjectionLeveled, ph, bookie ->
    Bench.ProjectionLeveled.close(ph)
    # `book_close/1` flushes the ledger cache + closes journal cleanly.
    # Used in production by riak_kv; safe to call at scenario teardown.
    _ = :leveled_bookie.book_close(bookie)
    :ok
end

# Provision N shards: projection + cache + overlay + registry +
# oplog instance, each instance's applier targets its shard.
make_ctx = fn prefix, backend ->
  ns = unique_ns.(prefix)
  inst_prefix = unique_prefix.(prefix)

  shards =
    for shard <- 0..(shard_count - 1), into: %{} do
      {adapter, ph, bookie} = open_projection.(backend, ns, shard)
      {:ok, ch} = cache_adapter.init(ns, :primary, shard, %{})
      ov = :bondy_oplog_db_overlay.new()

      config = %{
        shard_count: shard_count,
        cache_adapter: cache_adapter,
        cache_handle: ch,
        projection_adapter: adapter,
        projection_handle: ph,
        overlay: ov,
        fold_module: fold,
        owner: self()
      }

      :ok = :bondy_db_core_registry.register(ns, :primary, shard, config)

      instance_id = inst_prefix <> "-" <> Integer.to_string(shard)

      {:ok, _sup} =
        :bondy_oplog.start_instance(instance_id, %{
          fold_module: fold,
          fsync_mode: wal_fsync_mode,
          max_install_in_flight: max_in_flight,
          applier: %{
            cell_apply_target: {ns, :primary, shard}
          }
        })

      {shard,
       %{
         instance_id: instance_id,
         projection_adapter: adapter,
         projection: ph,
         bookie: bookie,
         cache: ch,
         overlay: ov
       }}
    end

  %{
    ns: ns,
    bucket: bucket,
    backend: backend,
    shards: shards,
    instance_prefix: inst_prefix,
    n_keys: prepopulate,
    write_cursor: :atomics.new(1, [{:signed, false}]),
    read_cursor: :atomics.new(1, [{:signed, false}])
  }
end

# Per-shard contiguous key block — used by both `populate` and the
# bench-time ops. Each writer/reader worker hashes its `self()` to
# pick a shard once and stays there for the run, so its calls all hit
# the same WAL → one frame per `append_many`, one fsync.
shard_key = fn shard, offset ->
  pad =
    offset
    |> Integer.to_string()
    |> String.pad_leading(8, "0")

  "s" <> Integer.to_string(shard) <> ":k:" <> pad
end

worker_shard = fn ctx -> :erlang.phash2(self(), map_size(ctx.shards)) end

keys_per_shard = max(div(prepopulate, shard_count), 1)

# Pre-populate every shard's owned keyspace so reads have data and
# writes do read-modify-write against the same cells (not pure
# inserts). Each shard gets `keys_per_shard` cells with keys shaped
# like `"s<shard>:k:<padded-offset>"` so the bench-time ops can
# regenerate them without hashing.
populate = fn ctx, n ->
  hlc_base = :erlang.system_time(:nanosecond)
  per_shard = max(div(n, map_size(ctx.shards)), 1)

  Enum.each(ctx.shards, fn {shard, %{instance_id: id}} ->
    Enum.each(1..per_shard, fn offset ->
      key = shard_key.(shard, offset)
      hlc = hlc_base + shard * per_shard + offset
      event = {:set, hlc, "v" <> Integer.to_string(offset)}
      _ = :bondy_oplog.append(id, {:cell_apply, ctx.bucket, key, event})
    end)
  end)

  Enum.each(ctx.shards, fn {_s, %{instance_id: id}} ->
    :ok = :bondy_oplog.await_apply(id, 60_000)
  end)
end

cleanup = fn ctx ->
  Enum.each(ctx.shards, fn {shard,
                            %{
                              instance_id: id,
                              projection_adapter: adapter,
                              projection: ph,
                              bookie: bookie,
                              cache: ch,
                              overlay: ov
                            }} ->
    # Stop the oplog instance first so no further appends reach the
    # applier while we tear down its sinks. Then close the cache /
    # projection / leveled bookie, then drop the overlay ETS table.
    _ = :bondy_oplog.stop_instance(id)
    _ = :bondy_db_core_registry.unregister(ctx.ns, :primary, shard)
    _ = close_projection.(adapter, ph, bookie)
    _ = cache_adapter.close(ch)
    _ = :bondy_oplog_db_overlay.delete(ov)
  end)

  pid = :os.getpid()

  case File.ls("/tmp/bondy_oplog_wal/#{pid}") do
    {:ok, names} ->
      Enum.each(names, fn n ->
        if String.starts_with?(n, ctx.instance_prefix) do
          _ = File.rm_rf("/tmp/bondy_oplog_wal/#{pid}/#{n}")
        end
      end)

    _ ->
      :ok
  end

  # Drop leveled per-NS dirs. The bookie has closed by now; remove the
  # files so the 5 GB cap in feedback_cleanup_tmp_after_tests is not
  # tripped across repeated bench runs.
  if ctx.backend == :leveled do
    _ =
      File.rm_rf(
        Path.join(leveled_root, Atom.to_string(ctx.ns))
      )
  end
end

# ----- ops -----
#
# Writes are fire-and-forget: `append/2` enqueues into the WAL and
# returns once the frame is durable; the per-instance applier drains
# the WAL asynchronously into the projection. The pipeline-drain
# barrier (`barrier_fun`) runs after the workers stop and before stats
# collection, so every appended event has been applied by the time
# the `applier_applied` telemetry counter is read.

build_cell_op = fn ctx, key ->
  hlc = :erlang.system_time(:nanosecond)
  event = {:set, hlc, "v" <> Integer.to_string(hlc)}
  {:cell_apply, ctx.bucket, key, event}
end

write_op =
  if batch_size <= 1 do
    fn ctx ->
      shard = worker_shard.(ctx)
      i = :atomics.add_get(ctx.write_cursor, 1, 1)
      offset = rem(i - 1, keys_per_shard) + 1
      key = shard_key.(shard, offset)
      instance_id = ctx.shards[shard].instance_id
      :bondy_oplog.append(instance_id, build_cell_op.(ctx, key))
    end
  else
    fn ctx ->
      shard = worker_shard.(ctx)
      base = :atomics.add_get(ctx.write_cursor, 1, batch_size)
      first_offset = base - batch_size + 1

      items =
        for n <- 0..(batch_size - 1) do
          offset = rem(first_offset - 1 + n, keys_per_shard) + 1
          {build_cell_op.(ctx, shard_key.(shard, offset)), :undefined}
        end

      instance_id = ctx.shards[shard].instance_id
      :bondy_oplog.append_many(instance_id, items)
    end
  end

read_op = fn ctx ->
  shard = worker_shard.(ctx)
  i = :atomics.add_get(ctx.read_cursor, 1, 1)
  offset = rem(i - 1, keys_per_shard) + 1
  key = shard_key.(shard, offset)
  :bondy_db_core.read(ctx.ns, :primary, ctx.bucket, key)
end

mixed_op = fn ctx ->
  Enum.each(1..7, fn _ -> _ = read_op.(ctx) end)
  Enum.each(1..3, fn _ -> _ = write_op.(ctx) end)
  :ok
end

# Drain every shard's applier — used as `barrier_fun`. Each shard is
# awaited sequentially since `await_apply` is now event-driven
# (instance pid call, not 5 ms-poll loop) and the shard count is
# small.
drain_shards = fn ctx ->
  Enum.each(ctx.shards, fn {_shard, %{instance_id: id}} ->
    :ok = :bondy_oplog.await_apply(id, 120_000)
  end)
end

# ----- scenario runner -----
#
# `Bench.E2E.run/1` attaches telemetry handlers using the namespace
# and instance_prefix the run is filtered by. Those values only exist
# after `make_ctx` runs. We materialise the context up-front, pass
# it through to the harness's `setup`, and bind workload ops to a
# captured `ctx` so workers don't have to re-resolve it.

run_scenario = fn base_name, backend, workload_specs ->
  name = "#{base_name}_#{backend}#{cache_label}"
  ctx = make_ctx.(name, backend)
  populate.(ctx, prepopulate)

  workloads =
    Map.new(workload_specs, fn {label, %{count: c, op: op}} ->
      {label, %{count: c, op: fn _ignored -> op.(ctx) end}}
    end)

  Bench.E2E.run(
    name: name,
    duration_seconds: duration_s,
    warmup_ms: warmup_ms,
    shard_count: shard_count,
    instance_prefix: ctx.instance_prefix,
    namespace: ctx.ns,
    setup: fn -> ctx end,
    cleanup: cleanup,
    workloads: workloads,
    barrier: drain_shards
  )
end

# ----- scenarios -----
#
# Each base scenario is run once per backend. The scenario name is
# suffixed with `_<backend>` so the index page lists e.g.
# `write_only_w4_ets` and `write_only_w4_leveled` side-by-side.

scenarios = [
  {"write_only_w#{writers}", %{
    "writer" => %{count: writers, op: write_op}
  }},
  {"read_only_w#{readers}", %{
    "reader" => %{count: readers, op: read_op}
  }},
  # Single-worker-doing-both shape — realistic for a client whose
  # business logic is read-then-write on one connection.
  {"mixed_70r_30w_w#{writers + readers}", %{
    "mixed" => %{count: writers + readers, op: mixed_op}
  }},
  # Concurrent R/W with dedicated worker pools — isolates "do reads
  # affect writes?". Readers and writers are separate processes
  # against the same shards.
  {"concurrent_rw_r#{readers}w#{writers}", %{
    "reader" => %{count: readers, op: read_op},
    "writer" => %{count: writers, op: write_op}
  }}
]

runs =
  for backend <- backends,
      {base_name, specs} <- scenarios,
      do: run_scenario.(base_name, backend, specs)

Bench.E2E.write_index(runs)

IO.puts("\n[e2e] open bench/_output/e2e_pipeline/index.html for the dashboards.")
