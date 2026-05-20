defmodule Bench.PackStore do
  @moduledoc """
  Helpers for benchmarks driving the `:bondy_mst_pack_store` backend.

  Each `open/1` materialises a fresh instance directory under
  `/tmp/bondy_mst_bench_pack/` so concurrent benches don't collide,
  and returns a populated `:bondy_mst` tree plus a `cleanup/0` fun
  that closes the store and removes the directory.
  """

  @compile {:no_warn_undefined, [:bondy_mst, :bondy_mst_store, :bondy_mst_pack_store]}

  @root "/tmp/bondy_mst_bench_pack"

  @doc """
  Open a fresh pack-store-backed tree.

  Options (all optional):
    * `:sync_every_records` (pos integer, default 1) — datasync after
      N appends. Set higher to amortise fsync cost.
    * `:sync_every_ms` (pos integer or `:infinity`, default `:infinity`)
      — opportunistic wall-clock sync threshold.

  Returns `{tree, dir, cleanup_fun}` where `cleanup_fun/0` closes the
  store and removes the instance directory.
  """
  def open(opts \\ %{}) do
    File.mkdir_p!(@root)
    suffix = Integer.to_string(System.unique_integer([:positive]))
    dir = Path.join(@root, "i_" <> suffix)
    File.mkdir_p!(dir)

    store_opts =
      %{dir: dir, instance_id: "bench_" <> suffix}
      |> maybe_put(:sync_every_records, opts)
      |> maybe_put(:sync_every_ms, opts)

    tree =
      :bondy_mst.new(%{
        store: :bondy_mst_pack_store,
        store_opts: store_opts
      })

    cleanup = fn final_tree ->
      try do
        :bondy_mst_store.close(:bondy_mst.store(final_tree))
      rescue
        _ -> :ok
      catch
        _, _ -> :ok
      end

      _ = File.rm_rf(dir)
      :ok
    end

    {tree, dir, cleanup}
  end

  @doc """
  Build a populated pack-store tree of `count` keys. Returns
  `{tree, dir, cleanup}` where `cleanup/1` takes the *final* tree
  (after additional mutation in the bench) so the right store handle
  is closed.
  """
  def build(count, opts \\ %{}) do
    {tree, dir, cleanup} = open(opts)
    keys = Bench.gen_keys(count)

    populated =
      Enum.reduce(keys, tree, fn k, acc ->
        :bondy_mst.put(acc, k, k)
      end)

    {populated, dir, cleanup}
  end

  @doc "Remove the root bench directory entirely. Safe to call repeatedly."
  def cleanup_root, do: File.rm_rf(@root)

  defp maybe_put(map, key, source) when is_map(source) do
    case Map.fetch(source, key) do
      {:ok, v} -> Map.put(map, key, v)
      :error -> map
    end
  end
end
