Bench.setup()

# Single-put accounting: measure how many pages the MST writes per
# put, and how big they are, with a fixed prepopulate. Tells us
# the actual volume of work behind one put_3 call.

prepopulate = 5000
n_puts = 200

# Build a populated pack-store tree.
{tree, dir, cleanup} = Bench.PackStore.build(prepopulate, %{sync_every_records: 1000, root_flush_every_records: 1000})

incoming = Path.join(dir, "incoming.pack")
{:ok, %{size: size_before}} = File.stat(incoming)

# Generate fresh keys.
new_keys =
  for i <- (prepopulate + 1)..(prepopulate + n_puts) do
    "k:" <> String.pad_leading(Integer.to_string(i), 8, "0")
  end

# Time the puts.
{us, final_tree} =
  :timer.tc(fn ->
    Enum.reduce(new_keys, tree, fn k, acc -> :bondy_mst.put(acc, k, k) end)
  end)

{:ok, %{size: size_after}} = File.stat(incoming)
bytes_written = size_after - size_before
avg_per_put = if n_puts > 0, do: bytes_written / n_puts, else: 0.0

IO.puts("\nSingle-put accounting (prepopulate=#{prepopulate}, n_puts=#{n_puts})")
IO.puts("  total time:          #{us} µs")
IO.puts("  per-put time:        #{Float.round(us / n_puts, 2)} µs")
IO.puts("  incoming bytes Δ:    #{bytes_written}")
IO.puts("  avg bytes / put:     #{Float.round(avg_per_put, 1)}")

# Approx: each record has a 40-byte header per pack codec.
record_hdr = 40
# Assume the avg page is reasonably sized; divide bytes by an
# estimate to back into avg_pages_per_put.
# Crude: write the prepopulate-build bytes for comparison.
IO.puts("\nFor reference (prepopulate build):")
{:ok, %{size: full_size}} = File.stat(incoming)
IO.puts("  incoming.pack size:  #{full_size} bytes")

# Now: open a fresh tree and time a SINGLE put, recording the pending
# counter before/after.
{tree2, _dir2, cleanup2} = Bench.PackStore.open(%{sync_every_records: 1000, root_flush_every_records: 1000})

# Prepopulate quietly.
warm =
  Enum.reduce(0..(prepopulate - 1), tree2, fn i, acc ->
    k = "k:" <> String.pad_leading(Integer.to_string(i + 1), 8, "0")
    :bondy_mst.put(acc, k, k)
  end)

# Single timed put.
target_k = "k:" <> String.pad_leading(Integer.to_string(prepopulate + 1), 8, "0")
{us1, _final} = :timer.tc(fn -> :bondy_mst.put(warm, target_k, target_k) end)
IO.puts("\nSingle put on prepopulated tree: #{us1} µs")

cleanup.(final_tree)
cleanup2.(warm)
Bench.PackStore.cleanup_root()
