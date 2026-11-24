"""Container (Arrow IPC stream + index) against Parquet, one row group per shard.

Self-contained: four table shapes are generated from a seed. Both formats store the same table,
zstd level 3, with checksums written and verified. One pinned CPU, page cache dropped before each
timed read, 7 repetitions, median. "One column, own container" stores the ids and that column
apart from the other columns, in both formats. Prints IPC / Parquet ratios (lower is better) and the time of
one index commit at 4096 shards. Reads give two ratios, "a/b": against `ParquetFile.read` and
against `pq.read_table` (the datasets path, the one apps call today).

    python benchmarks/bench_container.py [--records 262144] [--blob-records 32768] [--seed 1]
"""
from __future__ import annotations

import argparse
import os
import random
import statistics
import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ms_flow.core.data import container as c

SHARD = 256
REPS = 7


def _words(rng, n):
    letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJ0123456789()=#[]@+-"
    return ["".join(rng.choice(letters) for _ in range(rng.randint(2, 7))) for _ in range(n)]


def _text(rng, vocab, count, low, high):
    return ["".join(rng.choices(vocab, k=rng.randint(low, high))) for _ in range(count)]


def shapes(records, blob_records, seed):
    rng = random.Random(seed)
    vocab = _words(rng, 512)
    ids = lambda n: pa.array(range(n), pa.uint64())  # noqa: E731
    short = pa.table({"id": ids(records), "record": _text(rng, vocab, records, 6, 12),
                      "value": [rng.random() for _ in range(records)]})
    long_count = max(SHARD, records // 8)
    long = pa.table({"id": ids(long_count), "record": _text(rng, vocab, long_count, 300, 500),
                     "value": [rng.random() for _ in range(long_count)]})
    numeric = pa.table({"id": ids(records),
                        **{f"f{k}": [rng.gauss(0, 1) for _ in range(records)] for k in range(8)},
                        **{f"i{k}": pa.array([rng.randrange(1000) for _ in range(records)], pa.int32())
                           for k in range(4)}})
    lines = [" ".join(f"{rng.uniform(-50, 50):8.3f}" for _ in range(3)) for _ in range(4096)]
    blobs = pa.table({
        "id": ids(blob_records),
        "group": pa.array([i // SHARD for i in range(blob_records)], pa.uint64()),
        "run": pa.array([rng.randrange(4) for _ in range(blob_records)], pa.int32()),
        "value": [rng.uniform(-12, 0) for _ in range(blob_records)],
        "kind": [rng.choice(["a", "b"]) for _ in range(blob_records)],
        "status": [rng.choice(["done", "failed"]) for _ in range(blob_records)],
        "flag": [rng.random() < 0.9 for _ in range(blob_records)],
        "blob": pa.array(["\n".join(rng.choices(lines, k=rng.randint(150, 300))).encode()
                          for _ in range(blob_records)], pa.binary()),
    })
    return {"text, short": (short, "value", None), "text, long": (long, "value", None),
            "numeric": (numeric, "f0", None), "results with blobs": (blobs, "value", "blob")}


def _drop_cache(*paths):
    for path in paths:
        if Path(path).exists():
            fd = os.open(path, os.O_RDONLY)
            try:
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
            finally:
                os.close(fd)


def _median(function, before=lambda: None):
    times = []
    for _ in range(REPS):
        before()
        start = time.perf_counter()
        function()
        times.append(time.perf_counter() - start)
    return statistics.median(times)


def _fsync(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_ipc(table, path):
    for p in (path, c.index_path(path)):
        p.unlink(missing_ok=True)
    appender = c.Appender.create(path, table.schema)

    def shards():
        for at in range(0, table.num_rows, SHARD):
            piece = table.slice(at, SHARD)
            yield c.encode(table.schema, piece.to_batches(),
                           dict(base_id=at, slot_count=piece.num_rows, n_records=piece.num_rows, part=str(at)))

    appender.commit(shards())


def write_parquet(table, path):
    pq.write_table(table, path, row_group_size=SHARD, compression="zstd", compression_level=3,
                   write_page_checksum=True)
    _fsync(path)


def _parquet(path):
    return pq.ParquetFile(path, page_checksum_verification=True)


def measure(table, column, blob, folder, rng):
    ipc_path, pq_path = folder / "t.arrows", folder / "t.parquet"
    cold_ipc = lambda: _drop_cache(ipc_path, c.index_path(ipc_path))  # noqa: E731
    cold_pq = lambda: _drop_cache(pq_path)  # noqa: E731
    out = {"write": (_median(lambda: write_ipc(table, ipc_path)), _median(lambda: write_parquet(table, pq_path)))}
    out["size"] = (os.path.getsize(ipc_path) + os.path.getsize(c.index_path(ipc_path)), os.path.getsize(pq_path))
    n_shards = -(-table.num_rows // SHARD)
    picks = [rng.randrange(n_shards) for _ in range(200)]

    def ipc_random():
        info, rows = c.read_index(ipc_path)
        for k in picks:
            c.read_table(ipc_path, info, [rows[k]])

    def pq_random():
        file = _parquet(pq_path)
        for k in picks:
            file.read_row_group(k)

    out["200 random shards"] = (_median(ipc_random, cold_ipc), _median(pq_random, cold_pq))
    out["full read"] = (_median(lambda: sum(t.num_rows for t in c.iter_tables(ipc_path, *c.read_index(ipc_path))),
                                cold_ipc),
                        _median(lambda: _parquet(pq_path).read(), cold_pq),
                        _median(lambda: pq.read_table(pq_path), cold_pq))

    def one_column(ipc_file, pq_file):
        return (_median(lambda: sum(t.num_rows for t in c.iter_tables(ipc_file, *c.read_index(ipc_file),
                                                                    columns=[column])),
                        lambda: _drop_cache(ipc_file, c.index_path(ipc_file))),
                _median(lambda: _parquet(pq_file).read(columns=[column]), lambda: _drop_cache(pq_file)),
                _median(lambda: pq.read_table(pq_file, columns=[column]), lambda: _drop_cache(pq_file)))

    if blob is not None:
        out["one column, same container as blobs"] = one_column(ipc_path, pq_path)
    # "own container": the queried column stored apart from the big ones, with its ids (section 1)
    queried = table.select(["id", column])
    ipc_path, pq_path = folder / "q.arrows", folder / "q.parquet"
    write_ipc(queried, ipc_path)
    write_parquet(queried, pq_path)
    out["one column, own container"] = one_column(ipc_path, pq_path)
    return out


def index_commit_ms(folder, shards=4096):
    schema = pa.schema([("id", pa.uint64())])
    path = folder / "index.arrows"
    appender = c.Appender.create(path, schema)
    one = pa.RecordBatch.from_arrays([pa.array([0], pa.uint64())], schema=schema)
    appender.commit(c.encode(schema, [one], dict(base_id=k, slot_count=1, n_records=1, part=f"p{k}"))
                    for k in range(shards))
    return 1000 * _median(lambda: c._save_index(path, appender.info, appender.rows))


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--records", type=int, default=262144)
    parser.add_argument("--blob-records", type=int, default=32768)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--cpu", type=int, default=None, help="CPU to pin to (default: the last one allowed)")
    parser.add_argument("--dir", default=None, help="folder on the disk to measure (default: a temp folder)")
    args = parser.parse_args()
    os.sched_setaffinity(0, {args.cpu if args.cpu is not None else max(os.sched_getaffinity(0))})
    rng = random.Random(args.seed)
    print(f"pyarrow {pa.__version__}, records {args.records}, blob records {args.blob_records}, seed {args.seed}")
    with tempfile.TemporaryDirectory(dir=args.dir) as tmp:
        folder = Path(tmp)
        for name, (table, column, blob) in shapes(args.records, args.blob_records, args.seed).items():
            result = measure(table, column, blob, folder, rng)
            print(f"{name}: " + ", ".join(f"{key} " + "/".join(f"{ipc_value / pq_value:.2f}" for pq_value in pq_values)
                                          for key, (ipc_value, *pq_values) in result.items()), flush=True)
        print(f"index commit at 4096 shards: {index_commit_ms(folder):.1f} ms")


if __name__ == "__main__":
    main()
