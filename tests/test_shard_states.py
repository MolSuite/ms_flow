"""Shard states: the done set as a sidecar, the fraction in the database, and the mini file
that carries only what is left to run with its parent's ids."""
import uuid

from ms_flow.core.data.shard import PayloadKind, Serializer, Shard, ShardWriter, merge_shards
from ms_flow.core.data.states import done, forget, mark_done, pending, progress
from ms_flow.core.database import ProjectStore


def _project(tmp_path):
    db = ProjectStore()
    db.connect(tmp_path / "project")
    return db


def test_done_grows_and_the_rest_stays_pending(tmp_path):
    db = _project(tmp_path)
    try:
        assert pending(db, "prep:ad4", "lib.parquet", [1, 2, 3]) == [1, 2, 3]
        assert mark_done(db, "prep:ad4", "lib.parquet", [2, 1, 1], total=4) == 2
        assert mark_done(db, "prep:ad4", "lib.parquet", [3], failed=1) == 3
        assert done(db, "prep:ad4", "lib.parquet").tolist() == [1, 2, 3]
        assert pending(db, "prep:ad4", "lib.parquet", [1, 2, 3, 4]) == [4]
        # Another kind, or another file, is its own state.
        assert pending(db, "dock:7", "lib.parquet", [1]) == [1]
        # The database alone answers "202/1000", with no id read.
        (row,) = progress(db, "prep:ad4")
        assert (row["done"], row["total"], row["failed"], row["status"]) == (3, 4, 1, "partial")
        assert mark_done(db, "prep:ad4", "lib.parquet", [4]) == 4
        assert progress(db, "prep:ad4")[0]["status"] == "complete"
    finally:
        db.disconnect()


def test_forgetting_runs_it_again(tmp_path):
    db = _project(tmp_path)
    try:
        mark_done(db, "prep:ad4", "lib.parquet", [1, 2, 3], total=3)
        assert forget(db, "prep:ad4", "lib.parquet", [2]) == 2
        assert pending(db, "prep:ad4", "lib.parquet", [1, 2, 3]) == [2]
        assert forget(db, "prep:ad4", "lib.parquet") == 0
        assert pending(db, "prep:ad4", "lib.parquet", [1, 2, 3]) == [1, 2, 3]
        assert progress(db)[0]["status"] == "inactive"
    finally:
        db.disconnect()


def test_a_mini_file_of_the_pending_shards_keeps_the_parent_ids(tmp_path):
    """What travels to a host that has most of the library done already: the few shards left,
    under the same ids, so its results pair back with the parent with nothing to translate."""
    parent = tmp_path / "lib.parquet"
    with ShardWriter(parent, dataset_id=uuid.UUID(int=1), shard_id=0, base_id=0, slot_count=8,
                     kind=PayloadKind.SDF, serializer=Serializer.UTF8) as writer:
        for i in range(8):
            writer.add(i, f"m{i}".encode())
            if i % 2:
                writer.end_group()  # four shards of two records

    mini = tmp_path / "mini.parquet"
    ranges = merge_shards([(parent, (4, 6)), (parent, (6, 8))], mini)

    assert [r["base_id"] for r in ranges] == [4, 6]
    with Shard.open(mini) as shard:
        assert [(r.id, r.load()) for r in shard] == [(i, f"m{i}") for i in range(4, 8)]
