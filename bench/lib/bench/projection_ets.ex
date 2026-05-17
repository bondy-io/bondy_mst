defmodule Bench.ProjectionEts do
  @moduledoc """
  In-memory `bondy_oplog_projection_adapter` for benchmarks. Mirrors
  `test/bondy_oplog_projection_ets.erl` but lives in the bench project
  so we don't need the test profile beams on the code path.
  """

  # The Erlang behaviour module is loaded at runtime via Code.prepend_path,
  # so we don't declare `@behaviour` — Mix can't verify it.

  def open(_ns, _index, _shard, _opts) do
    tab =
      :ets.new(__MODULE__, [
        :ordered_set,
        :public,
        {:read_concurrency, true}
      ])

    {:ok, tab}
  end

  def close(tab) do
    true = :ets.delete(tab)
    :ok
  end

  def get(tab, key) do
    case :ets.lookup(tab, key) do
      [{_, frame}] -> {:ok, frame}
      [] -> :not_found
    end
  end

  def put_batch(tab, entries) do
    true = :ets.insert(tab, entries)
    :ok
  end

  def range(tab, low, high, opts) do
    limit = Map.get(opts, :limit, 1000)
    direction = Map.get(opts, :direction, :asc)

    ms = [
      {{:"$1", :"$2"},
       [{:>=, :"$1", {:const, low}}, {:<, :"$1", {:const, high}}],
       [{{:"$1", :"$2"}}]}
    ]

    result =
      case :ets.select(tab, ms, limit) do
        :"$end_of_table" -> []
        {found, _cont} -> found
      end

    ordered =
      case direction do
        :asc -> result
        :desc -> Enum.reverse(result)
      end

    {:ok, ordered}
  end

  def delete(tab, key) do
    true = :ets.delete(tab, key)
    :ok
  end

  def info(tab) do
    %{size: :ets.info(tab, :size), memory: :ets.info(tab, :memory)}
  end
end
