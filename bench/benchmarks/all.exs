# Top-level runner — loads every benchmark script in sequence so a
# single `mix run benchmarks/all.exs` produces the full HTML report
# tree under `bench/_output/<name>/index.html`.

scripts = ~w(
  mst_put.exs
  mst_get.exs
  mst_fold.exs
  mst_merge.exs
  mst_bulk_put.exs
  mst_pack_put.exs
  mst_pack_seal.exs
  mst_pack_get.exs
  mst_store_get.exs
  primitives.exs
  folds.exs
  mst_db.exs
  oplog.exs
  wal.exs
)

base = Path.dirname(__ENV__.file)

Enum.each(scripts, fn name ->
  path = Path.join(base, name)
  IO.puts("\n==> " <> name)
  Code.eval_file(path)
end)

IO.puts("\n[bench] HTML reports → bench/_output/<name>/index.html")
