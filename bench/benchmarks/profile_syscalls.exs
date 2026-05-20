Bench.setup()

# Per-syscall cost on macOS APFS — strips MST + pack-store entirely.
# Hypothesis: the remaining write-path gap is the prim_file syscall
# round-trip cost (port to file_io_server), not the bytes themselves.
#
# Workload:
#   - 10 000 small writes (4 KB payload, no fsync)
#   - 10 000 preads (re-read each)
#   - sha256 on the payload
#   - same volume as a 2 000-put MST traversal at depth ~5
#
# Per-call cost should print sub-µs if syscall round-trip is free,
# or ~10-100 µs if it's the bottleneck.

n = 10_000
payload = :crypto.strong_rand_bytes(4096)

tmpdir = "/tmp/bondy_mst_profile_syscalls"
File.rm_rf(tmpdir)
File.mkdir_p!(tmpdir)
path = Path.join(tmpdir, "scratch.bin")
{:ok, fd} = :prim_file.open(String.to_charlist(path), [:read, :write, :raw, :binary])

# Write phase.
{write_us, _} =
  :timer.tc(fn ->
    Enum.reduce(0..(n - 1), 0, fn _i, off ->
      :ok = :prim_file.write(fd, payload)
      off + byte_size(payload)
    end)
  end)

IO.puts(
  "  prim_file:write   x#{n}   total=#{write_us} µs   #{Float.round(write_us / n, 2)} µs/call"
)

# Pread phase (random access — same fd).
{pread_us, _} =
  :timer.tc(fn ->
    Enum.each(0..(n - 1), fn i ->
      off = i * byte_size(payload)
      {:ok, _} = :prim_file.pread(fd, off, byte_size(payload))
    end)
  end)

IO.puts(
  "  prim_file:pread   x#{n}   total=#{pread_us} µs   #{Float.round(pread_us / n, 2)} µs/call"
)

# sha256 cost.
{hash_us, _} =
  :timer.tc(fn ->
    Enum.each(0..(n - 1), fn _ -> :crypto.hash(:sha256, payload) end)
  end)

IO.puts(
  "  :crypto.hash      x#{n}   total=#{hash_us} µs   #{Float.round(hash_us / n, 2)} µs/call"
)

# datasync cost (single).
{ds_us, _} = :timer.tc(fn -> :prim_file.datasync(fd) end)
IO.puts("  prim_file:datasync (1 call): #{ds_us} µs")

# datasync × 100 to amortise jitter.
{ds100_us, _} =
  :timer.tc(fn ->
    Enum.each(1..100, fn _ ->
      :ok = :prim_file.write(fd, "x")
      :ok = :prim_file.datasync(fd)
    end)
  end)

IO.puts(
  "  write+datasync    x100   total=#{ds100_us} µs   #{Float.round(ds100_us / 100, 2)} µs/call"
)

# Page-shape term_to_binary cost — the actual MST page payload.
# An MST page is roughly {Level, Low, List} where List is a list of
# {Key, Hash, ChildHash} triples. With branching ~16 entries per
# page, that's a moderately complex term.
sample_entries =
  for i <- 1..16 do
    k = "k:" <> String.pad_leading(Integer.to_string(i), 8, "0")
    h = :crypto.strong_rand_bytes(32)
    ch = :crypto.strong_rand_bytes(32)
    {k, h, ch}
  end

page_term = {3, :crypto.strong_rand_bytes(32), sample_entries}
page_bin = :erlang.term_to_binary(page_term, [:deterministic, {:minor_version, 2}])
IO.puts("\n  page-shaped term serialised size: #{byte_size(page_bin)} bytes")

{t2b_us, _} =
  :timer.tc(fn ->
    Enum.each(0..(n - 1), fn _ ->
      _ = :erlang.term_to_binary(page_term, [:deterministic, {:minor_version, 2}])
    end)
  end)

IO.puts(
  "  term_to_binary (det) x#{n}   total=#{t2b_us} µs   #{Float.round(t2b_us / n, 2)} µs/call"
)

{b2t_us, _} =
  :timer.tc(fn ->
    Enum.each(0..(n - 1), fn _ -> _ = :erlang.binary_to_term(page_bin, [:safe]) end)
  end)

IO.puts(
  "  binary_to_term       x#{n}   total=#{b2t_us} µs   #{Float.round(b2t_us / n, 2)} µs/call"
)

:prim_file.close(fd)
File.rm_rf(tmpdir)
