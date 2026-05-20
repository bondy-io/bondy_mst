Bench.setup()

# Decomposition bench for `bondy_mst_pack_store:walk_reachable/3` —
# the GC reachability walk that currently decodes every page just to
# read `bondy_mst_page:refs/1`. The followup #15 asks whether a
# refs-only fast path that skips `binary_to_term` is worth building.
#
# To answer that, we need to know what fraction of `walk_reachable`
# time is actually spent in `binary_to_term`. This bench measures
# four costs over the same reachable set:
#
#   total           — read bytes + decode + page-record + refs + walk
#                     (the current production path)
#   io_only         — read bytes via `bondy_mst_pack_reader:get/2`,
#                     walk a precomputed adjacency map (no decode)
#                     → I/O floor; refs-only fast path can't go below
#   decode_only     — `binary_to_term` over the pre-read body list
#                     → upper bound on what skipping it could save
#   record_plus_refs— from a pre-decoded {Level, Low, List}, run
#                     `bondy_mst_page:new/3` + `refs/1`
#                     → tells us how much of the post-decode work is
#                     wasted vs. extracting refs straight from the
#                     3-tuple
#
# Two value-size regimes:
#   * small — values are integers (the MST default)
#   * large — values are 1 KB binaries (projection-store-flavoured)
#
# Same constraints as the other pack benches: prim_file fds belong
# to the controlling process, so we can't use Benchee. Manual
# :timer.tc/1.

# ----- tunables -----
configs = [
  {"small  / N=2k",   2_000, :small},
  {"small  / N=5k",   5_000, :small},
  {"large  / N=2k",   2_000, {:binary, 1024}},
  {"xlarge / N=2k",   2_000, {:binary, 8192}},
  {"xxlarge/ N=1k",   1_000, {:binary, 65536}}
]

runs = 5

# Wrapper-record positional access (same as mst_pack_get.exs).
#   #bondy_mst_store{mod = e2, state = e3, ...}
#   #bondy_mst_pack_store{writer = e2, sealed_views = e3, ...}
#   #bondy_mst{store = e2, ...}
backend_of = fn tree ->
  store = :bondy_mst.store(tree)
  :erlang.element(3, store)
end

replace_backend = fn tree, new_backend ->
  old_store = :bondy_mst.store(tree)
  new_store = :erlang.setelement(3, old_store, new_backend)
  :bondy_mst.set_store(tree, new_store)
end

mk_value = fn
  :small, k -> k
  {:binary, bytes}, _ -> :crypto.strong_rand_bytes(bytes)
end

# Build a populated, fully-sealed pack-store tree. Returns
# {dir, root_hash, cleanup}.
build = fn n, vsize ->
  {tree, _dir, cleanup} = Bench.PackStore.open(%{sync_every_records: 1_000})

  keys = Bench.gen_keys(n)

  populated =
    Enum.reduce(keys, tree, fn k, acc ->
      :bondy_mst.put(acc, k, mk_value.(vsize, k))
    end)

  {:ok, sealed_backend} = :bondy_mst_pack_store.seal(backend_of.(populated))
  sealed_tree = replace_backend.(populated, sealed_backend)
  dir = :bondy_mst_pack_store.dir(sealed_backend)
  root = :bondy_mst.root(sealed_tree)

  cleanup_fn = fn ->
    try do
      _ = cleanup.(sealed_tree)
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end
  end

  {dir, root, cleanup_fn}
end

# Walk the reachable set via the reader, collecting (hash → bytes)
# AND (hash → refs). This is the precomputation used by the
# `io_only` measurement (refs map replaces decode), and gives us the
# byte list for `decode_only`.
collect_bytes_and_refs = fn reader, root ->
  walk = fn walk, h, {bytes_acc, refs_acc} ->
    cond do
      h == :undefined -> {bytes_acc, refs_acc}
      Map.has_key?(bytes_acc, h) -> {bytes_acc, refs_acc}
      true ->
        {:ok, body} = :bondy_mst_pack_reader.get(reader, h)
        {level, low, list} = :erlang.binary_to_term(body, [:safe])
        page = :bondy_mst_page.new(level, low, list)
        refs = :bondy_mst_page.refs(page)
        b1 = Map.put(bytes_acc, h, body)
        r1 = Map.put(refs_acc, h, refs)
        Enum.reduce(refs, {b1, r1}, fn r, acc -> walk.(walk, r, acc) end)
    end
  end

  walk.(walk, root, {%{}, %{}})
end

# ---------- the four measured walks ----------

# (1) total: production path — read bytes, decode, build page, refs
walk_total = fn reader, root ->
  walk = fn walk, h, acc ->
    cond do
      h == :undefined -> acc
      :sets.is_element(h, acc) -> acc
      true ->
        {:ok, body} = :bondy_mst_pack_reader.get(reader, h)
        {level, low, list} = :erlang.binary_to_term(body, [:safe])
        page = :bondy_mst_page.new(level, low, list)
        refs = :bondy_mst_page.refs(page)
        acc1 = :sets.add_element(h, acc)
        Enum.reduce(refs, acc1, fn r, a -> walk.(walk, r, a) end)
    end
  end

  walk.(walk, root, :sets.new(version: 2))
end

# (2) io_only: read bytes; use precomputed refs map (no decode at all)
walk_io_only = fn reader, root, refs_map ->
  walk = fn walk, h, acc ->
    cond do
      h == :undefined -> acc
      :sets.is_element(h, acc) -> acc
      true ->
        {:ok, _body} = :bondy_mst_pack_reader.get(reader, h)
        acc1 = :sets.add_element(h, acc)
        refs = Map.get(refs_map, h, [])
        Enum.reduce(refs, acc1, fn r, a -> walk.(walk, r, a) end)
    end
  end

  walk.(walk, root, :sets.new(version: 2))
end

# (3) decode_only: pre-loaded body list — just binary_to_term each.
decode_only = fn bodies ->
  Enum.each(bodies, fn b -> _ = :erlang.binary_to_term(b, [:safe]) end)
end

# (4) record_plus_refs: pre-decoded 3-tuples — page record + refs/1
record_plus_refs = fn triples ->
  Enum.each(triples, fn {level, low, list} ->
    page = :bondy_mst_page.new(level, low, list)
    _ = :bondy_mst_page.refs(page)
  end)
end

# ---------- timing helpers ----------
median = fn samples ->
  sorted = Enum.sort(samples)
  Enum.at(sorted, div(length(sorted), 2))
end

us = fn fun ->
  {us, _} = :timer.tc(fun)
  us
end

IO.puts("\nwalk_reachable decomposition (median of #{runs} runs, in μs)")
IO.puts(String.duplicate("-", 78))
IO.puts(
  String.pad_trailing("config", 18) <>
    String.pad_leading("pages", 8) <>
    String.pad_leading("total", 10) <>
    String.pad_leading("io_only", 10) <>
    String.pad_leading("decode", 10) <>
    String.pad_leading("rec+refs", 10) <>
    "    decode%"
)
IO.puts(String.duplicate("-", 78))

Enum.each(configs, fn {label, n, vsize} ->
  {dir, root, cleanup} = build.(n, vsize)

  {:ok, reader} = :bondy_mst_pack_reader.open(dir)

  try do
    {bytes_map, refs_map} = collect_bytes_and_refs.(reader, root)
    bodies = Map.values(bytes_map)
    triples = Enum.map(bodies, &:erlang.binary_to_term(&1, [:safe]))
    page_count = map_size(bytes_map)

    # warmup
    _ = walk_total.(reader, root)

    total_samples = for _ <- 1..runs, do: us.(fn -> walk_total.(reader, root) end)
    io_samples    = for _ <- 1..runs, do: us.(fn -> walk_io_only.(reader, root, refs_map) end)
    dec_samples   = for _ <- 1..runs, do: us.(fn -> decode_only.(bodies) end)
    rec_samples   = for _ <- 1..runs, do: us.(fn -> record_plus_refs.(triples) end)

    total_med = median.(total_samples)
    io_med    = median.(io_samples)
    dec_med   = median.(dec_samples)
    rec_med   = median.(rec_samples)

    decode_pct =
      if total_med > 0,
        do: Float.round(100.0 * dec_med / total_med, 1),
        else: 0.0

    IO.puts(
      String.pad_trailing(label, 18) <>
        String.pad_leading("#{page_count}", 8) <>
        String.pad_leading("#{total_med}", 10) <>
        String.pad_leading("#{io_med}", 10) <>
        String.pad_leading("#{dec_med}", 10) <>
        String.pad_leading("#{rec_med}", 10) <>
        "    " <> String.pad_leading("#{decode_pct}%", 7)
    )
  after
    :ok = :bondy_mst_pack_reader.close(reader)
    cleanup.()
  end
end)

Bench.PackStore.cleanup_root()
