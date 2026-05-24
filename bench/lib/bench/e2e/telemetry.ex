defmodule Bench.E2E.Telemetry do
  @moduledoc """
  Pipeline-stage telemetry collector for the E2E benchmark.

  Attaches handlers to the substrate's `bondy_db_core` reads/ranges
  and the oplog's WAL + applier events, accumulates per-stage latency
  histograms and counters under `:counters`, and exposes the result as
  the JSON the dashboard consumes.

  Handlers filter by the bench's namespace (`bondy_db_core` events) or
  by instance_id prefix (`bondy_oplog_*` events). Any event from
  unrelated traffic running in the same VM is dropped — important
  when the bench shares the application with leftover instances from a
  prior scenario.
  """

  alias Bench.E2E.Hist

  # `:telemetry` is loaded at runtime via Bench.setup/0 (it's in
  # _build/default/lib/telemetry/ebin); Mix's compile-time check can't
  # see it.
  @compile {:no_warn_undefined, [:telemetry]}

  # Per-stage capture: {telemetry_path, stage_key, latency_field, event_field}
  # - latency_field is the measurements key carrying the latency in µs
  #   (nil when the stage is count-only).
  # - event_field is the measurements key carrying the number of
  #   underlying events this telemetry call represents (e.g. WAL
  #   batches and applier batches emit one telemetry event per N
  #   underlying ops). nil means one event per call. The dashboard's
  #   "events" count uses this for events/sec, while the raw call
  #   count is still tracked separately as the "batch" count.
  #
  # `applier_applied` is the canonical end-to-end event-count of
  # everything the applier has fully processed, regardless of which
  # sub-path each event takes (fold / cell_apply / publish).
  # `applier_published` is the narrower path-specific count emitted
  # only when a `publish_fun` / `publish_ns` is configured.
  @stages [
    {[:bondy_db_core, :read], :db_core_read, :duration_us, nil},
    {[:bondy_db_core, :range], :db_core_range, :duration_us, nil},
    {[:bondy_db_core, :range_all], :db_core_range_all, :duration_us, nil},
    {[:bondy_oplog, :wal, :append], :wal_append, nil, :batch_size},
    {[:bondy_oplog, :wal, :fsync], :wal_fsync, :duration_us, nil},
    {[:bondy_oplog, :applier, :applied], :applier_applied, nil, :count},
    {[:bondy_oplog, :applier, :published], :applier_published, nil, :count},
    # Per-stage breakdown of `apply_batch/2`. Each emits a latency
    # histogram (`duration_us`) and an event count (`count`), so
    # `events / calls` gives the mean batch size at that stage and
    # the histogram p50/p99 give per-batch wall-time. See applier
    # pipeline residual investigation plan §3.1.
    {[:bondy_oplog, :applier, :batch_verify], :batch_verify, :duration_us, :count},
    {[:bondy_oplog, :applier, :batch_fold], :batch_fold, :duration_us, :count},
    {[:bondy_oplog, :applier, :batch_cell_apply], :batch_cell_apply, :duration_us, :count},
    {[:bondy_oplog, :applier, :batch_publish], :batch_publish, :duration_us, :count},
    {[:bondy_oplog, :applier, :batch_install_cast], :batch_install_cast, :duration_us, :count},
    # Per-cell compute breakdown of `compute_one_cell/9` (PR-PS-15b
    # split apply_one_cell/11 into a per-event compute + batched
    # write). Each fires once per event; `cell_put` and
    # `cell_side_effects` from PR-PS-15a are gone — the put + cache
    # invalidation + high-water advance now happen once per BATCH
    # and are measured by `batch_cell_put` (below).
    {[:bondy_oplog, :applier, :cell_read], :cell_read, :duration_us, nil},
    {[:bondy_oplog, :applier, :cell_apply_event], :cell_apply_event, :duration_us, nil},
    # Per-batch substrate write (PR-PS-15b). Fires once per
    # `apply_cell_batch/2` invocation that has at least one cell to
    # write; `count` is the number of unique `{Bucket, Key}` entries
    # in the batch (post-dedup). Measures the cost of the single
    # `Adapter:put_batch/2` call that replaces the previous per-event
    # `book_put` storm.
    {[:bondy_oplog, :applier, :batch_cell_put], :batch_cell_put, :duration_us, :count}
  ]

  # Subset gated by APPLIER_PROFILE=control. The Erlang-side
  # `telemetry:execute/3` calls in `apply_batch/2` + `apply_one_cell/11`
  # still fire (their `monotonic_time` boundaries are unconditional),
  # but no handler is attached so the bench's per-event counter +
  # histogram update cost is skipped. Lets the residual-profiling sweep
  # isolate bench-side collection overhead from the always-paid
  # Erlang-side cost. See `_design/latest/APPLIER_PIPELINE_RESIDUAL_PLAN.md` §4.
  @applier_breakdown_keys [
    :batch_verify,
    :batch_fold,
    :batch_cell_apply,
    :batch_publish,
    :batch_install_cast,
    :cell_read,
    :cell_apply_event,
    :batch_cell_put
  ]

  @doc """
  Attaches handlers and returns an opaque state passed to `collect/1`
  and `detach/1`.

  ## Arguments

  - `name` — used in handler IDs (must be unique across concurrent
    benchmark runs).
  - `ns` — atom; bench's namespace. `bondy_db_core` events with a
    different `namespace` are dropped.
  - `instance_prefix` — binary; `bondy_oplog_*` events whose
    `instance_id` does not start with this are dropped.
  - `shard_count` — number of shards in the run. Per-shard counters
    are pre-allocated so the hot path does not allocate.
  """
  def attach(name, ns, instance_prefix, shard_count) do
    # Per-stage state:
    #   - hist: latency histogram (from `latency_field`)
    #   - calls: number of telemetry events seen (batches, for the
    #     applier/WAL paths)
    #   - events: sum of `event_field` from measurements (or +1 when
    #     event_field is nil); this is the underlying event count
    #     used to compute events/sec
    #   - hits/misses: cache source breakdown (db_core only)
    stage_state =
      Map.new(@stages, fn {_path, key, _lat, _evt} ->
        {key,
         %{
           hist: Hist.new(),
           calls: :counters.new(1, [:write_concurrency]),
           events: :counters.new(1, [:write_concurrency]),
           hits: :counters.new(1, [:write_concurrency]),
           misses: :counters.new(1, [:write_concurrency])
         }}
      end)

    # Per-shard event counters keyed by {stage_key, shard}. Tracks
    # the same "underlying event" count as `events` above so the
    # heatmap reflects real per-shard throughput rather than batches.
    per_shard =
      for {_path, key, _lat, _evt} <- @stages,
          shard <- 0..(shard_count - 1),
          into: %{} do
        {{key, shard}, :counters.new(1, [:write_concurrency])}
      end

    config = %{
      stages: stage_state,
      per_shard: per_shard,
      ns: ns,
      instance_prefix: instance_prefix,
      shard_count: shard_count
    }

    Enum.each(stages_to_attach(), fn {path, key, lat, evt} ->
      handler_id = handler_id(name, path)

      :telemetry.attach(
        handler_id,
        path,
        &__MODULE__.handle/4,
        Map.put(config, :stage, %{key: key, latency: lat, events: evt})
      )
    end)

    %{name: name, config: config}
  end

  defp stages_to_attach do
    case System.get_env("APPLIER_PROFILE") do
      "control" ->
        Enum.reject(@stages, fn {_path, key, _lat, _evt} ->
          key in @applier_breakdown_keys
        end)

      _ ->
        @stages
    end
  end

  @doc false
  def handle(_event, measurements, metadata, %{stage: stage} = cfg) do
    if relevant?(metadata, cfg) do
      state = Map.fetch!(cfg.stages, stage.key)
      :counters.add(state.calls, 1, 1)
      events = event_count(measurements, stage.events)
      :counters.add(state.events, 1, events)

      if lat = stage.latency do
        if us = Map.get(measurements, lat) do
          Hist.record(state.hist, us * 1_000)
        end
      end

      record_source(metadata, state)
      record_per_shard(metadata, cfg, stage.key, events)
    end

    :ok
  catch
    kind, reason ->
      # Telemetry handlers must never crash — they're called from the
      # caller's process. Log to console and continue.
      IO.warn("Bench.E2E.Telemetry handler failed: #{inspect({kind, reason})}")
      :ok
  end

  defp event_count(_measurements, nil), do: 1

  defp event_count(measurements, field) do
    case Map.get(measurements, field) do
      n when is_integer(n) and n >= 0 -> n
      _ -> 1
    end
  end

  defp relevant?(%{namespace: ns}, %{ns: ns}), do: true
  defp relevant?(%{namespaces: nss}, %{ns: ns}), do: ns in nss

  defp relevant?(%{instance_id: id}, %{instance_prefix: prefix})
       when is_binary(id) and is_binary(prefix) do
    String.starts_with?(id, prefix)
  end

  defp relevant?(_metadata, _cfg), do: false

  defp record_source(%{source: :cache}, state),
    do: :counters.add(state.hits, 1, 1)

  defp record_source(%{source: :projection}, state),
    do: :counters.add(state.misses, 1, 1)

  defp record_source(_, _state), do: :ok

  defp record_per_shard(%{shard: shard}, cfg, stage_key, events)
       when is_integer(shard) do
    case Map.get(cfg.per_shard, {stage_key, shard}) do
      nil -> :ok
      c -> :counters.add(c, 1, events)
    end
  end

  defp record_per_shard(%{instance_id: id}, cfg, stage_key, events)
       when is_binary(id) do
    # `<prefix>-<shard>` — extract trailing integer.
    case Integer.parse(String.replace_prefix(id, cfg.instance_prefix <> "-", "")) do
      {shard, ""} when shard >= 0 and shard < cfg.shard_count ->
        case Map.get(cfg.per_shard, {stage_key, shard}) do
          nil -> :ok
          c -> :counters.add(c, 1, events)
        end

      _ ->
        :ok
    end
  end

  defp record_per_shard(_, _, _, _), do: :ok

  @doc """
  Detach handlers and snapshot every stage's counters into a plain map.

  Each stage exposes:
  - `count` — underlying event count (sum of measurements.count or
    .batch_size, falling back to +1 per telemetry call). Use this for
    events/sec.
  - `batches` — number of telemetry events fired. Equals `count` when
    each call represents a single event.
  """
  def collect(%{name: name, config: cfg}) do
    stages =
      Map.new(cfg.stages, fn {key, s} ->
        calls = :counters.get(s.calls, 1)
        events = :counters.get(s.events, 1)
        hits = :counters.get(s.hits, 1)
        misses = :counters.get(s.misses, 1)

        per_shard =
          for shard <- 0..(cfg.shard_count - 1), into: %{} do
            c = Map.fetch!(cfg.per_shard, {key, shard})
            {shard, :counters.get(c, 1)}
          end

        pcts = Hist.percentiles(s.hist, [50, 90, 95, 99, 99.9])

        {key,
         %{
           count: events,
           batches: calls,
           hits: hits,
           misses: misses,
           per_shard: per_shard,
           percentiles_us: Map.new(pcts, fn {p, ns} -> {p, ns / 1_000} end),
           histogram: Hist.bins(s.hist)
         }}
      end)

    Enum.each(@stages, fn {path, _key, _lat, _evt} ->
      :telemetry.detach(handler_id(name, path))
    end)

    stages
  end

  defp handler_id(name, path) do
    "bench-e2e-" <> name <> "-" <> Enum.map_join(path, "-", &to_string/1)
  end
end
