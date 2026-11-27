"""M13 scale gates that ms_flow alone can run (2026-11-27).

    PYTHONPATH=src:. python benchmarks/scale_gates.py receipts --root /tmp/gates --parts 16384
    PYTHONPATH=src:. python benchmarks/scale_gates.py sparse --root /tmp/gates

receipts  16k slots each returning one part (one shard of 256 rows, a result's summary): the parts
          are made first by a pool, then committed by one `ContainerWriter` in groups of 64 (what a
          delivery hands over). Says the commit rate, and the shortest task per slot for which it
          is 3x the arrival of 16k slots.
sparse    1000 containers of 4 shards of 256 records, all but 37 shards cut: the time to read the
          usable records, then the same after a repack (`repack`), against a library of only those
          37 shards: reads should follow the live records, not the containers.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

ROWS = 256


def _project(root: Path):
    from ms_flow.core.database import ProjectStore

    db = ProjectStore()
    db.connect(root / "project")
    return db


def _make_part(args) -> dict:
    """A worker: one part of one shard of `ROWS` summary-like rows."""
    import pyarrow as pa

    from ms_flow.core.data.shard import write_table

    folder, n = args
    path = Path(folder) / f"{n}.arrows"
    table = pa.table({"ligand_id": pa.array(range(n * ROWS, (n + 1) * ROWS), pa.int64()),
                      "score": pa.array([-(i % 97) / 10 for i in range(ROWS)], pa.float64()),
                      "status": pa.array(["done"] * ROWS)})
    write_table(path, table, part=f"r:{n}", base_id=n * ROWS, origin=n)
    return {"part": f"r:{n}", "path": str(path)}


def receipts(root: Path, parts: int) -> dict:
    from ms_flow.core.data import catalog

    shutil.rmtree(root, ignore_errors=True)
    db = _project(root)
    folder = root / "results"
    (folder / "parts").mkdir(parents=True)
    with ProcessPoolExecutor(14) as pool:
        made = list(pool.map(_make_part, [(str(folder / "parts"), n) for n in range(parts)], chunksize=64))
    os.sync()
    took = time.perf_counter()
    with catalog.ContainerWriter(db, folder, generation_id=None, role="summary", prefix="r:") as writer:
        for at in range(0, len(made), 64):
            writer.commit_parts(made[at:at + 64])
    took = time.perf_counter() - took
    rate = parts / took
    db.disconnect()
    return {"gate": "receipts", "parts": parts, "commit_s": round(took, 2), "parts_per_s": round(rate, 1),
            # 16k slots at one part per T seconds arrive at 16384/T: 3x margin needs T >= 3*16384/rate
            "min_task_s_for_3x": round(3 * 16384 / rate, 1),
            "containers": len(list(folder.glob("*.arrows")))}


def _library(db, root: Path, containers: int, per: int):
    """`containers` containers of `per` shards of `ROWS` records each (scope 1), active."""
    from types import SimpleNamespace

    from ms_flow.core.data import catalog
    from ms_flow.core.data.shard import PartWriter, PayloadKind, Serializer

    gen = catalog.create_generation(db, step="import")
    parts = root / "lib_parts"
    parts.mkdir(parents=True, exist_ok=True)
    n = 0
    for c in range(containers):
        receipts = []
        for _ in range(per):
            path = parts / f"{n}.arrows"
            with PartWriter(path, kind=PayloadKind.GENERIC_BYTES, serializer=Serializer.UTF8, scope=1,
                            like=SimpleNamespace(base_id=n * ROWS, slot_count=ROWS)) as w:
                for i in range(n * ROWS, (n + 1) * ROWS):
                    w.add(i, b"x" * 64, source_index=i)
            receipts.append(catalog.file_row(path, role=catalog.SOURCE, generation_id=gen, part=f"i:{n}"))
            n += 1
        with catalog.ContainerWriter(db, root / "lib", generation_id=gen, prefix="i:") as writer:
            writer.commit_parts(receipts)
            writer.seal()  # one container each, as many imports would leave
    catalog.activate_generation(db, gen)
    return gen, n


def _read_usable(db) -> tuple[float, int]:
    from ms_flow.core.data import catalog
    from ms_flow.core.data.shard import Shard

    took, n = time.perf_counter(), 0
    for row in catalog.shards(db):
        with Shard.open(row["path"], shard=row["shard_index"], record_ids=row.get("record_ids")) as shard:
            n += sum(1 for _ in shard)
    return time.perf_counter() - took, n


def sparse(root: Path, containers: int = 1000, per: int = 4, live: int = 37) -> dict:
    import numpy as np

    from ms_flow.core.data import catalog, exclusions, repack

    shutil.rmtree(root, ignore_errors=True)
    db = _project(root)
    _gen, shards = _library(db, root, containers, per)
    keep = set(np.linspace(0, shards - 1, live, dtype=int).tolist())  # spread over the containers
    exclusions.exclude(db, {1: [i for s in range(shards) if s not in keep for i in range(s * ROWS, (s + 1) * ROWS)]})
    before_s, before_n = _read_usable(db)
    new, elements = repack.plan(db, root / "repacked", records=ROWS)
    with catalog.ContainerWriter(db, root / "repacked", generation_id=new, prefix=f"repack:{new}:") as writer:
        writer.commit_parts([repack.part(element) for element in elements])
    repack.activate(db, new)
    after_s, after_n = _read_usable(db)
    db.disconnect()
    # the same live records as a library of their own: what reads that follow live records cost
    alone = root / "alone"
    db2 = _project(alone)
    _library(db2, alone, 1, live)
    alone_s, alone_n = _read_usable(db2)
    db2.disconnect()
    return {"gate": "sparse", "containers": containers, "shards": shards, "live_shards": live,
            "before_s": round(before_s, 3), "after_repack_s": round(after_s, 3), "alone_s": round(alone_s, 3),
            "records": [before_n, after_n, alone_n]}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("gate", choices=["receipts", "sparse"])
    ap.add_argument("--root", type=Path, required=True)
    ap.add_argument("--parts", type=int, default=16384)
    ap.add_argument("--out", type=Path)
    a = ap.parse_args()
    result = receipts(a.root, a.parts) if a.gate == "receipts" else sparse(a.root)
    print(json.dumps(result), flush=True)
    if a.out:
        with a.out.open("a") as fh:
            fh.write(json.dumps(result) + "\n")


if __name__ == "__main__":
    main()
