defmodule Bench.E2E do
  @moduledoc """
  End-to-end pipeline harness.

  Drives the full bondy_db substrate (WAL → applier → MST →
  projection → cache → reads) under sustained load, captures per-
  workload latencies AND per-pipeline-stage telemetry, and writes a
  self-contained ECharts dashboard with a Sankey of event flows, a
  sunburst "sundial" of latency-by-stage, a per-shard heatmap, a
  polar bar latency rose, and per-stage throughput.

  ## Scenario shape

      Bench.E2E.run(
        name: "mixed_70r_30w",
        duration_seconds: 10,
        warmup_ms: 500,
        shard_count: 4,
        instance_prefix: "bench-e2e-mixed",
        namespace: :bench_e2e_mixed,
        setup: fn -> ctx end,        # returns workload context
        cleanup: fn ctx -> :ok end,
        workloads: %{
          writer: %{count: 4, op: fn ctx -> ... end},
          reader: %{count: 4, op: fn ctx -> ... end}
        },
        # Optional. Runs after workers stop and before telemetry is
        # collected. Write-heavy scenarios drain the applier here so
        # the `applier_applied` stage count reflects every event the
        # workers appended.
        barrier: fn ctx -> :ok end
      )

  Each scenario writes its dashboard to
  `bench/_output/e2e_pipeline/<name>/index.html` and the raw
  measurements alongside as `data.json`. Multiple scenarios in the
  same script can call `Bench.E2E.run/1` repeatedly; an index page is
  written when `Bench.E2E.write_index/2` is called explicitly.
  """

  alias Bench.E2E.{Hist, Telemetry, Report}

  @output_subdir "e2e_pipeline"

  @doc """
  Runs a scenario. Returns the stat map for downstream consumption
  (the index page renderer aggregates these).
  """
  def run(opts) do
    name = Keyword.fetch!(opts, :name)
    duration_s = Keyword.fetch!(opts, :duration_seconds)
    workloads = Keyword.fetch!(opts, :workloads)
    setup_fun = Keyword.fetch!(opts, :setup)
    cleanup_fun = Keyword.fetch!(opts, :cleanup)
    shard_count = Keyword.fetch!(opts, :shard_count)
    instance_prefix = Keyword.fetch!(opts, :instance_prefix)
    namespace = Keyword.fetch!(opts, :namespace)
    warmup_ms = Keyword.get(opts, :warmup_ms, 500)
    barrier_fun = Keyword.get(opts, :barrier, fn _ctx -> :ok end)

    IO.puts(
      "\n==> e2e: #{name}  (duration=#{duration_s}s, warmup=#{warmup_ms}ms, " <>
        "shards=#{shard_count})"
    )

    for {label, %{count: n}} <- workloads do
      IO.puts("    workload[#{label}]: count=#{n}")
    end

    telemetry_state = Telemetry.attach(name, namespace, instance_prefix, shard_count)

    ctx = setup_fun.()

    # Sample peak BEAM memory while the workers run. We trigger a GC
    # first so the baseline reflects steady-state, then sample every
    # 100 ms from a small probe process. Cheap and good enough to
    # catch run-to-run regressions / mailbox blowups.
    _ = :erlang.garbage_collect()
    mem_start = :erlang.memory(:total)
    {mem_probe, mem_probe_ref} = spawn_monitor_mem_probe(mem_start)

    op_hists = Map.new(workloads, fn {label, _} -> {label, Hist.new()} end)

    op_errors =
      Map.new(workloads, fn {label, _} ->
        {label, :counters.new(1, [:write_concurrency])}
      end)

    parent = self()
    Process.flag(:trap_exit, true)

    record_after = :erlang.monotonic_time(:millisecond) + warmup_ms

    deadline_ms =
      :erlang.monotonic_time(:millisecond) + warmup_ms + duration_s * 1000

    workers =
      for {label, %{count: n, op: op}} <- workloads, _ <- 1..n do
        hist = op_hists[label]
        errs = op_errors[label]

        pid =
          spawn_link(fn ->
            worker_loop(op, ctx, hist, errs, record_after, deadline_ms, parent)
          end)

        {pid, label}
      end

    wait_for_workers(workers)

    op_stats =
      Map.new(workloads, fn {label, %{count: n}} ->
        h = op_hists[label]
        errs = :counters.get(op_errors[label], 1)
        count = Hist.total(h)
        pcts = Hist.percentiles(h, [50, 90, 95, 99, 99.9])
        total_attempts = count + errs

        {label,
         %{
           workers: n,
           count: count,
           errors: errs,
           error_rate:
             if(total_attempts > 0, do: errs / total_attempts, else: 0.0),
           ops_per_sec: count / duration_s,
           percentiles_us: Map.new(pcts, fn {p, ns} -> {p, ns / 1_000} end),
           histogram: Hist.bins(h)
         }}
      end)

    # Drain the pipeline before snapshotting telemetry so the
    # `applier_applied` count covers everything the workers appended,
    # not just whatever happened to be drained while they were busy.
    # Measure the drain so reports can show how long the tail took.
    drain_t0 = :erlang.monotonic_time(:millisecond)
    _ = barrier_fun.(ctx)
    drain_ms = :erlang.monotonic_time(:millisecond) - drain_t0

    stage_stats = Telemetry.collect(telemetry_state)

    # Stop the memory probe before cleanup so we don't capture the
    # teardown noise as part of the run's peak.
    mem_peak = stop_mem_probe(mem_probe, mem_probe_ref, mem_start)

    cleanup_fun.(ctx)

    # `mem_end` is captured AFTER cleanup + a forced GC so it reflects
    # what's leaked into the long-lived process tree, not what was
    # transiently held by the running scenario. A small positive
    # delta is normal (ETS tables, code-loading, atom interning);
    # anything large means a process or ETS table from the scenario
    # is still alive.
    _ = :erlang.garbage_collect()
    mem_end = :erlang.memory(:total)

    applier_applied = get_in(stage_stats, [:applier_applied, :count]) || 0
    total_seconds = duration_s + drain_ms / 1_000

    mib = fn b -> Float.round(b / (1024 * 1024), 1) end

    run = %{
      name: name,
      duration_seconds: duration_s,
      drain_ms: drain_ms,
      warmup_ms: warmup_ms,
      shard_count: shard_count,
      ops: op_stats,
      stages: stage_stats,
      total_ops:
        Enum.reduce(op_stats, 0, fn {_l, s}, acc -> acc + s.count end),
      applier_ops_per_sec:
        if(total_seconds > 0, do: applier_applied / total_seconds, else: 0.0),
      memory: %{
        peak_mb: mib.(mem_peak),
        end_mb: mib.(mem_end),
        delta_mb: mib.(mem_end - mem_start)
      }
    }

    Report.write(@output_subdir, run)
    Report.print_console(run)

    run
  end

  @doc """
  Writes a top-level `index.html` linking every scenario's dashboard.
  Call once after every `Bench.E2E.run/1` in the script.
  """
  def write_index(runs) when is_list(runs) do
    Report.write_index(@output_subdir, runs)
  end

  # ----- worker loop -----

  defp worker_loop(op, ctx, hist, errors, record_after, deadline_ms, parent) do
    now = :erlang.monotonic_time(:millisecond)

    cond do
      now >= deadline_ms ->
        send(parent, {:worker_done, self()})
        :ok

      now < record_after ->
        _ = op.(ctx)
        worker_loop(op, ctx, hist, errors, record_after, deadline_ms, parent)

      true ->
        t0 = :erlang.monotonic_time(:nanosecond)
        result = op.(ctx)
        t1 = :erlang.monotonic_time(:nanosecond)
        record_result(result, hist, errors, t1 - t0)
        worker_loop(op, ctx, hist, errors, record_after, deadline_ms, parent)
    end
  end

  defp record_result({:error, _}, _hist, errors, _ns) do
    :counters.add(errors, 1, 1)
  end

  defp record_result(_other, hist, _errors, ns) do
    Hist.record(hist, ns)
  end

  defp wait_for_workers(workers) do
    expected = MapSet.new(workers, fn {pid, _} -> pid end)
    do_wait(expected)
  end

  # ----- memory probe -----
  #
  # Spawns a process that samples `:erlang.memory(:total)` every 100 ms
  # and tracks the running max. On `:stop` it replies with the peak.
  # Linked-monitored so a crash in the probe is observable but
  # non-fatal — the harness falls back to `mem_start` as the "peak".

  defp spawn_monitor_mem_probe(mem_start) do
    parent = self()

    pid =
      spawn(fn ->
        send(parent, {:mem_probe_ready, self()})
        mem_probe_loop(parent, mem_start)
      end)

    ref = Process.monitor(pid)
    receive do
      {:mem_probe_ready, ^pid} -> :ok
    after
      1_000 -> :ok
    end

    {pid, ref}
  end

  defp mem_probe_loop(parent, peak) do
    receive do
      {:stop, from} ->
        send(from, {:mem_peak, peak})
        :ok
    after
      100 ->
        m = :erlang.memory(:total)
        mem_probe_loop(parent, max(peak, m))
    end
  end

  defp stop_mem_probe(pid, ref, fallback) do
    send(pid, {:stop, self()})

    receive do
      {:mem_peak, peak} ->
        Process.demonitor(ref, [:flush])
        peak

      {:DOWN, ^ref, :process, ^pid, _} ->
        fallback
    after
      2_000 ->
        Process.demonitor(ref, [:flush])
        fallback
    end
  end

  defp do_wait(remaining) do
    if MapSet.size(remaining) == 0 do
      :ok
    else
      receive do
        {:worker_done, pid} ->
          do_wait(MapSet.delete(remaining, pid))

        {:EXIT, pid, :normal} ->
          do_wait(MapSet.delete(remaining, pid))

        {:EXIT, pid, reason} ->
          IO.puts("[e2e] worker #{inspect(pid)} crashed: #{inspect(reason)}")
          do_wait(MapSet.delete(remaining, pid))
      end
    end
  end
end
