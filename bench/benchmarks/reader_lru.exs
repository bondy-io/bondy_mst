Bench.setup()

# Reader fd-pressure / LRU decomposition bench — answers followup #9
# ("reader holds an fd per sealed pack; insert LRU when fan-out grows").
# Same shape as followup #15's `walk_reachable.exs`: measure first,
# decide whether the optimisation pays for itself.
#
# An LRU layer keyed by `pack_id` would let `bondy_mst_pack_reader`
# stop opening an fd per sealed pack at `open/1` time and instead
# open lazily on lookup, evicting cold packs. The questions are:
#
#   * How does eager-open scale with pack count? (current cost)
#   * What does an LRU miss cost — open + pread + close — vs a
#     resident-fd lookup? (miss penalty)
#   * Is the lookup ratio big enough that a small LRU pays off, or
#     small enough that the engineering complexity is wasted?
#
# Four measurements per `packs` config:
#
#   open_total      — time to `bondy_mst_pack_reader:open/1` across N
#                     packs (i.e., N sequential `prim_file:open` plus
#                     N `prim_file:read_file` for the .idx blobs)
#   hit_cached      — current production path, per-lookup μs:
#                     `bondy_mst_pack_reader:get/2` against a resident
#                     fd (steady state)
#   hit_lru_miss    — simulated LRU miss penalty, per-lookup μs:
#                     `prim_file:open` + `pread_record` + `prim_file:close`
#                     on every lookup (worst-case miss-every-time LRU)
#   open_close_only — `prim_file:open + close` in isolation, to isolate
#                     fd-cycle cost from pread cost
#
# Workload: lookups hit a hash sampled from the OLDEST sealed pack
# (the worst case for current code, which iterates newest-first — but
# also the worst case for LRU miss, since a cold pack must be opened).
# This is intentionally the harshest scenario for each path.
#
# Same constraints as the other pack benches: prim_file fds belong
# to the controlling process, so we can't use Benchee. Manual
# :timer.tc/1.

# ----- tunables -----
# Per-seal fsync cost dominates the build (each seal writes pack+idx+
# manifest tmp files, fsyncs, renames, fsyncs the dir). On macOS APFS
# that's seconds per seal, so we stop at 64 packs. The scaling-with-N
# trend across 4/16/64 plus the per-pack-open isolation measurement
# is enough to extrapolate to higher fan-out without timing out.
configs = [
  {"packs=4",  4,  250},
  {"packs=16", 16, 250},
  {"packs=64", 64, 250}
]

# Number of lookups in each "batch" for the per-op average.
queries = 500
runs    = 3

# Wrapper-record positional access (same as mst_pack_get.exs).
backend_of = fn tree ->
  store = :bondy_mst.store(tree)
  :erlang.element(3, store)
end

writer_of = fn backend ->
  :erlang.element(2, backend)
end

replace_backend = fn tree, new_backend ->
  old_store = :bondy_mst.store(tree)
  new_store = :erlang.setelement(3, old_store, new_backend)
  :bondy_mst.set_store(tree, new_store)
end

# Build a store with `packs` sealed packs of `per_pack` records each,
# capturing the hashes added per pack so we can target the oldest one.
build_multi_pack = fn packs, per_pack ->
  {tree, dir, cleanup} = Bench.PackStore.open(%{sync_every_records: 1_000})

  {final_tree, hashes_by_pack_rev} =
    Enum.reduce(0..(packs - 1), {tree, []}, fn pack_ix, {acc_tree, acc} ->
      keys =
        for j <- 1..per_pack do
          "p:" <> Integer.to_string(pack_ix) <> ":" <>
            String.pad_leading(Integer.to_string(j), 8, "0")
        end

      filled = Enum.reduce(keys, acc_tree, &:bondy_mst.put(&2, &1, &1))
      backend = backend_of.(filled)
      pending = :bondy_mst_pack_writer.pending_hashes(writer_of.(backend))

      {:ok, sealed_backend} = :bondy_mst_pack_store.seal(backend)
      new_tree = replace_backend.(filled, sealed_backend)

      {new_tree, [pending | acc]}
    end)

  final_backend = backend_of.(final_tree)
  :ok = :bondy_mst_pack_store.close(final_backend)

  # Oldest pack's hashes are at the head after Enum.reverse (we
  # prepended; oldest pack was emitted first).
  hashes_by_pack = Enum.reverse(hashes_by_pack_rev)
  oldest_pack_hashes = hd(hashes_by_pack)

  {dir, oldest_pack_hashes, cleanup}
end

# Measure eager-open time (μs). Closes the reader before returning.
measure_open = fn dir ->
  {us, {:ok, r}} = :timer.tc(fn -> :bondy_mst_pack_reader.open(dir) end)
  :ok = :bondy_mst_pack_reader.close(r)
  us
end

# Measure resident-fd lookup, μs / query.
measure_hit_cached = fn dir, hashes ->
  {:ok, reader} = :bondy_mst_pack_reader.open(dir)

  try do
    {us, _} =
      :timer.tc(fn ->
        Enum.each(hashes, fn h ->
          {:ok, _} = :bondy_mst_pack_reader.get(reader, h)
        end)
      end)

    us / length(hashes)
  after
    :ok = :bondy_mst_pack_reader.close(reader)
  end
end

# Simulate "LRU miss every time": for each lookup, open the target
# pack's .pack fd, pread record + body, close. We use the .idx (still
# memory-resident in the reader) to locate the record offset, then
# open/pread/close the .pack fd freshly. This isolates the fd-cycle
# overhead the LRU would amortise.
measure_lru_miss = fn dir, hashes ->
  {:ok, reader} = :bondy_mst_pack_reader.open(dir)
  # Take a snapshot of the (id, idx, pack_path) for every sealed pack;
  # we'll use it to locate the record without consulting the resident
  # fd map. The reader's pack_fd is still open in `reader`, but the
  # measurement does not use it — it opens a fresh fd per call.
  pack_ids = :bondy_mst_pack_reader.sealed_pack_ids(reader)
  # Build {pack_id => {idx, pack_path}} via the reader's internals.
  # No public accessor for the idx; rebuild by re-reading the .idx
  # file. Cheap one-shot setup not on the measured path.
  idx_paths =
    for pid <- pack_ids, into: %{} do
      idx_path = :bondy_mst_pack_paths.sealed_idx_path(dir, pid)
      {:ok, bin} = :prim_file.read_file(idx_path)
      {:ok, idx} = :bondy_mst_pack_index.open(bin)
      pack_path = :bondy_mst_pack_paths.sealed_pack_path(dir, pid)
      {pid, {idx, pack_path}}
    end

  # All target hashes live in the OLDEST pack. Pre-locate offsets so
  # the measurement only times the fd cycle + pread (the LRU-miss
  # equivalent in production code would also need the .idx lookup,
  # but that's the same cost in either path so we factor it out).
  oldest_pid = List.first(Enum.sort(pack_ids))
  {oldest_idx, oldest_pack_path} = Map.fetch!(idx_paths, oldest_pid)

  offsets =
    Enum.map(hashes, fn h ->
      {:ok, off} = :bondy_mst_pack_index.lookup(oldest_idx, h)
      {h, off}
    end)

  hdr_bytes = :bondy_mst_pack_codec.record_header_bytes()

  try do
    {us, _} =
      :timer.tc(fn ->
        Enum.each(offsets, fn {_h, off} ->
          # The LRU-miss cycle: open + pread header + pread body + close.
          {:ok, fd} =
            :prim_file.open(oldest_pack_path, [:read, :raw, :binary])

          {:ok, hbin} = :prim_file.pread(fd, off, hdr_bytes)
          {:ok, header} = :bondy_mst_pack_codec.decode_record_header(hbin)
          plen = Map.get(header, :page_len)
          _ =
            if plen > 0 do
              {:ok, _body} = :prim_file.pread(fd, off + hdr_bytes, plen)
            else
              :ok
            end

          _ = :prim_file.close(fd)
        end)
      end)

    us / length(hashes)
  after
    :ok = :bondy_mst_pack_reader.close(reader)
  end
end

# Cost of just open + close (no pread). Tells us how much of the LRU
# miss penalty is the prim_file port cycle vs the actual I/O.
measure_open_close_only = fn dir, pack_id, count ->
  pack_path = :bondy_mst_pack_paths.sealed_pack_path(dir, pack_id)

  {us, _} =
    :timer.tc(fn ->
      Enum.each(1..count, fn _ ->
        {:ok, fd} = :prim_file.open(pack_path, [:read, :raw, :binary])
        _ = :prim_file.close(fd)
      end)
    end)

  us / count
end

# ---------- timing helpers ----------
median = fn samples ->
  sorted = Enum.sort(samples)
  Enum.at(sorted, div(length(sorted), 2))
end

fmt = fn x when is_float(x) -> :erlang.float_to_binary(x, decimals: 2)
        x -> Integer.to_string(x) end

IO.puts("\nreader-LRU decomposition (median of #{runs} runs)")
IO.puts(String.duplicate("-", 90))
IO.puts(
  String.pad_trailing("config", 12) <>
    String.pad_leading("open_us", 12) <>
    String.pad_leading("open_per_pack", 16) <>
    String.pad_leading("hit_cached_us", 16) <>
    String.pad_leading("lru_miss_us", 14) <>
    String.pad_leading("open+close_us", 16) <>
    String.pad_leading("miss/hit", 10)
)
IO.puts(String.duplicate("-", 90))

Enum.each(configs, fn {label, packs, per_pack} ->
  {dir, oldest_hashes, _cleanup} = build_multi_pack.(packs, per_pack)

  # Choose `queries` hashes from the oldest pack (with replacement so
  # all configs measure the same query count).
  arr = List.to_tuple(oldest_hashes)
  size = tuple_size(arr)
  hashes = for _ <- 1..queries, do: elem(arr, :rand.uniform(size) - 1)

  # warmup
  _ = measure_open.(dir)
  _ = measure_hit_cached.(dir, Enum.take(hashes, 100))
  _ = measure_lru_miss.(dir, Enum.take(hashes, 100))

  open_samples =
    for _ <- 1..runs, do: measure_open.(dir)

  hit_samples =
    for _ <- 1..runs, do: measure_hit_cached.(dir, hashes)

  miss_samples =
    for _ <- 1..runs, do: measure_lru_miss.(dir, hashes)

  # Use any existing sealed pack id for the open/close measurement;
  # the oldest one matches the LRU-miss path so the comparison is
  # apples-to-apples (same pack, same path-lookup cost).
  {:ok, reader_for_ids} = :bondy_mst_pack_reader.open(dir)
  oldest_for_oc = List.first(Enum.sort(:bondy_mst_pack_reader.sealed_pack_ids(reader_for_ids)))
  :ok = :bondy_mst_pack_reader.close(reader_for_ids)

  oc_samples =
    for _ <- 1..runs, do: measure_open_close_only.(dir, oldest_for_oc, 1_000)

  open_med = median.(open_samples)
  hit_med = median.(hit_samples)
  miss_med = median.(miss_samples)
  oc_med = median.(oc_samples)

  per_pack_open = open_med / packs

  ratio =
    if hit_med > 0 do
      Float.round(miss_med / hit_med, 1)
    else
      0.0
    end

  IO.puts(
    String.pad_trailing(label, 12) <>
      String.pad_leading(fmt.(open_med), 12) <>
      String.pad_leading(fmt.(per_pack_open), 16) <>
      String.pad_leading(fmt.(hit_med), 16) <>
      String.pad_leading(fmt.(miss_med), 14) <>
      String.pad_leading(fmt.(oc_med), 16) <>
      String.pad_leading("#{ratio}x", 10)
  )

  _ = File.rm_rf(dir)
end)

Bench.PackStore.cleanup_root()
