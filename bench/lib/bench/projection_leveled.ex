defmodule Bench.ProjectionLeveled do
  @moduledoc """
  Leveled-backed `bondy_oplog_projection_adapter` for benchmarks.

  Mirrors `test/bondy_oplog_projection_leveled.erl` (Erlang) but lives
  in the bench app so we don't pull test code into the bench code path.

  The adapter is a **pure mapper**: the Bookie pid is supplied via
  `open/4`'s opts (`%{bookie: pid()}`). Bookie lifecycle is owned by
  the bench scenario — one Bookie per shard, started in the scenario's
  setup and stopped in cleanup. Snapshots are NOT explicitly managed
  here because:

  - `book_get/4` is a single point read served from the live Bookie's
    ledger cache; no snapshot involved.
  - `book_objectfold/6` with `SnapPreFold = true` takes a snapshot at
    call time and wraps the fold in `leveled_runner:wrap_runner/2`,
    which is implemented as
    `try FoldAction() after AfterAction() end`. The snapshot is freed
    even if the fold throws — see leveled_runner.erl. We still wrap
    our **caller-side** invocation of the returned `Runner` in
    try/catch/after to (a) translate the `{limit_reached, _}` throw
    we use for early termination back into a normal return and (b)
    surface any unexpected exception with the bucket/range so a
    failing scenario is debuggable.
  """

  # leveled beams are loaded at runtime via Code.prepend_path, so Mix
  # can't see them at compile time.
  @compile {:no_warn_undefined, [:leveled_bookie]}

  # Mirrors `-define(STD_TAG, o).` in leveled.hrl. Pinning here keeps
  # us decoupled from include-file resolution from Elixir.
  @std_tag :o

  def open(_ns, _index, _shard, %{bookie: pid} = _opts) when is_pid(pid) do
    {:ok, %{bookie: pid}}
  end

  def open(_ns, _index, _shard, opts) when is_map(opts) do
    {:error, {:invalid_opts, opts}}
  end

  def close(%{bookie: _pid}), do: :ok

  def get(%{bookie: pid}, bucket, key)
      when is_binary(bucket) and is_binary(key) do
    case :leveled_bookie.book_get(pid, bucket, key, @std_tag) do
      {:ok, frame} -> {:ok, frame}
      :not_found -> :not_found
    end
  end

  def put_batch(%{bookie: pid}, entries) when is_list(entries) do
    do_put_batch(pid, entries)
  end

  def range(%{bookie: pid}, bucket, low, high, opts)
      when is_binary(bucket) and is_binary(low) and is_binary(high) and
             is_map(opts) do
    limit = Map.get(opts, :limit, 1000)
    direction = Map.get(opts, :direction, :asc)
    fold_fun = make_range_fold_fun(limit, high)

    # `SnapPreFold = true` → snapshot taken inside `book_objectfold`
    # at call time. The runner returned below executes the fold
    # against that snapshot; leveled's wrap_runner ensures the
    # snapshot is closed in its own `after` clause.
    {:async, runner} =
      :leveled_bookie.book_objectfold(
        pid,
        @std_tag,
        bucket,
        {low, high},
        {fold_fun, {0, []}},
        true
      )

    {_n, acc_rev} =
      try do
        runner.()
      catch
        # Early termination — fold reached `limit`. The thrown state
        # is the same {N, Items} shape we'd return on natural
        # completion. leveled has already closed the snapshot for us
        # via wrap_runner's `after`.
        :throw, {:limit_reached, state} ->
          state
      end

    asc = Enum.reverse(acc_rev)

    case direction do
      :asc -> {:ok, asc}
      :desc -> {:ok, Enum.reverse(asc)}
    end
  end

  def delete(%{bookie: pid}, bucket, key)
      when is_binary(bucket) and is_binary(key) do
    case :leveled_bookie.book_delete(pid, bucket, key, []) do
      :ok -> :ok
      :pause -> :ok
    end
  end

  def info(%{bookie: pid}) do
    %{backend: :leveled, bookie: pid, tag: @std_tag}
  end

  # ------------------------------------------------------------------

  defp do_put_batch(_pid, []), do: :ok

  defp do_put_batch(pid, [{bucket, key, frame} | rest])
       when is_binary(bucket) and is_binary(key) and is_binary(frame) do
    case :leveled_bookie.book_put(pid, bucket, key, frame, []) do
      :ok -> do_put_batch(pid, rest)
      # leveled returns `pause` under load to ask the caller to slow
      # down. We accept it as success here (the write IS durable)
      # rather than propagating — the bench's job is to measure raw
      # throughput, not implement back-pressure handling.
      :pause -> do_put_batch(pid, rest)
    end
  end

  defp make_range_fold_fun(limit, high) do
    fn _bucket, k, v, {n, items} ->
      cond do
        k == high ->
          # Half-open contract: substrate exposes [Low, High).
          # leveled's range is inclusive on both ends, so we drop
          # the high endpoint explicitly.
          {n, items}

        true ->
          n1 = n + 1
          state = {n1, [{k, v} | items]}
          if n1 >= limit, do: throw({:limit_reached, state}), else: state
      end
    end
  end
end
