"""Table outputs (phase 4): a task's rows as a plain Parquet file of the catalog, what is done as
a done-set per key, a compaction that keeps one file per key and the newest row per value."""
import pyarrow as pa
import pyarrow.parquet as pq

from ms_flow.core.data import catalog, states
from ms_flow.core.data.shard import Serializer, ShardWriter
from ms_flow.core.database import ProjectStore


def _project(tmp_path):
    db = ProjectStore()
    db.connect(tmp_path / "project")
    return db


def _source(db, folder):
    """Two source files of three 4-id shards each, one active generation."""
    gen = catalog.create_generation(db, step="load")
    for f in range(2):
        path = folder / f"src{f}.arrows"
        with ShardWriter(path, dataset_id=None, shard_id=0, base_id=12 * f, kind=0, serializer=Serializer.UTF8,
                         slot_count=1 << 20) as writer:
            for lo in range(12 * f, 12 * f + 12, 4):
                for id_ in range(lo, lo + 4):
                    writer.add(id_, b"x")
                writer.end_group(scope=1, base_id=lo, slot_count=4)
        catalog.add_file(db, path, generation_id=gen)
    catalog.activate_generation(db, gen)
    return gen


def _task(db, folder, key, shards, gen, n, *, crash=False, value=0):
    """One task: a row per record of its shards. The parent's order: file → add_file → mark_done."""
    path = folder / f"{key}-{n}.parquet"
    ids = [i for s in shards for i in range(s["base_id"], s["base_id"] + s["slot_count"])]
    pq.write_table(pa.table({"record": ids, "value": [value] * len(ids)}), path)
    catalog.add_file(db, path, generation_id=gen, role=f"table:{key}")
    if not crash:
        states.mark_done(db, "step", key, [s["id"] for s in shards], total=6, generation_id=gen)

def test_a_cut_then_undo_leaves_only_the_narrowed_shards_pending(tmp_path):
    db = _project(tmp_path)
    try:
        _source(db, tmp_path)
        ids = [s["id"] for s in catalog.shards(db)]
        states.mark_done(db, "step", "k", ids[:4])
        states.mark_done(db, "step", "k", ids[4:], narrowed_by="7")  # ran while cut 7 was active
        assert states.pending(db, "step", "k", ids, narrowed_by="7") == []
        assert states.pending(db, "step", "k", ids) == ids[4:]  # cut undone: those run whole
        states.mark_done(db, "step", "k", ids[4:5])
        assert states.pending(db, "step", "k", ids) == ids[5:]
        assert states.mark_done(db, "step", "k", ids[4:5], narrowed_by="9") == 6  # whole stays whole
        assert states.pending(db, "step", "k", ids, narrowed_by="9") == ids[5:]
        assert states.done_by_file(db, "step", "k") == {ids[0] >> catalog.GROUP_BITS: 3,
                                                         ids[3] >> catalog.GROUP_BITS: 3}
    finally:
        db.dispose()


def test_a_done_set_keeps_its_generation_until_forgotten(tmp_path):
    db = _project(tmp_path)
    try:
        gen = _source(db, tmp_path)
        states.mark_done(db, "step", "k", [s["id"] for s in catalog.shards(db)], generation_id=gen)
        for step in ("one", "two", "three"):
            catalog.activate_generation(db, catalog.create_generation(db, step=step))
        assert (tmp_path / "src0.arrows").exists()
        states.forget(db, "step", "k")
        catalog.activate_generation(db, catalog.create_generation(db, step="four"))
        assert not (tmp_path / "src0.arrows").exists()
    finally:
        db.dispose()


def test_a_key_marked_under_a_new_generation_starts_again_and_remove_takes_a_role(tmp_path):
    db = _project(tmp_path)
    try:
        gen = _source(db, tmp_path)
        ids = [s["id"] for s in catalog.shards(db)]
        states.mark_done(db, "step", "k", ids, generation_id=gen)
        assert states.mark_done(db, "step", "k", [99], generation_id=gen + 1) == 1
        pq.write_table(pa.table({"record": [1]}), tmp_path / "t.parquet")
        catalog.add_file(db, tmp_path / "t.parquet", generation_id=gen, role="table:k")
        assert catalog.remove(db, "table:k") == 1 and not (tmp_path / "t.parquet").exists()
    finally:
        db.dispose()
