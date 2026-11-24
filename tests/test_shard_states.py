"""Shard states: the done set as a sidecar, the fraction in the database, and the mini file
that carries only what is left to run with its parent's ids."""
import uuid

from ms_flow.core.data.shard import PayloadKind, Serializer, Shard, ShardWriter, merge_shards
from ms_flow.core.data.states import done, forget, mark_done, merge, pending, progress, sidecar
from ms_flow.core.database import ProjectStore


def _project(tmp_path):
    db = ProjectStore()
    db.connect(tmp_path / "project")
    return db


def test_done_grows_and_the_rest_stays_pending(tmp_path):
    db = _project(tmp_path)
    try:
        assert pending(db, "step:a", "lib.parquet", [1, 2, 3]) == [1, 2, 3]
        assert mark_done(db, "step:a", "lib.parquet", [2, 1, 1], total=4) == 2
        assert mark_done(db, "step:a", "lib.parquet", [3], failed=1) == 3
        assert done(db, "step:a", "lib.parquet").tolist() == [1, 2, 3]
        assert pending(db, "step:a", "lib.parquet", [1, 2, 3, 4]) == [4]
        # Another kind, or another file, is its own state.
        assert pending(db, "step:b", "lib.parquet", [1]) == [1]
        # The database alone answers "202/1000", with no id read.
        (row,) = progress(db, "step:a")
        assert (row["done"], row["total"], row["failed"], row["status"]) == (3, 4, 1, "partial")
        assert mark_done(db, "step:a", "lib.parquet", [4]) == 4
        assert progress(db, "step:a")[0]["status"] == "complete"
    finally:
        db.disconnect()


def test_forgetting_runs_it_again(tmp_path):
    db = _project(tmp_path)
    try:
        mark_done(db, "step:a", "lib.parquet", [1, 2, 3], total=3)
        assert forget(db, "step:a", "lib.parquet", [2]) == 2
        assert pending(db, "step:a", "lib.parquet", [1, 2, 3]) == [2]
        assert forget(db, "step:a", "lib.parquet") == 0
        assert pending(db, "step:a", "lib.parquet", [1, 2, 3]) == [1, 2, 3]
        assert progress(db)[0]["status"] == "inactive"
    finally:
        db.disconnect()


def test_a_mini_file_of_the_pending_shards_keeps_the_parent_ids(tmp_path):
    """What travels to a host that has most of the dataset done already: the few shards left,
    under the same ids, so its results pair back with the parent with nothing to translate."""
    parent = tmp_path / "lib.arrows"
    with ShardWriter(parent, dataset_id=uuid.UUID(int=1), shard_id=0, base_id=0, slot_count=8,
                     kind=PayloadKind.GENERIC_BYTES, serializer=Serializer.UTF8) as writer:
        for i in range(8):
            writer.add(i, f"m{i}".encode())
            if i % 2:
                writer.end_group()  # four shards of two records

    mini = tmp_path / "mini.arrows"
    ranges = merge_shards([(parent, 2), (parent, 3)], mini)

    assert [r["base_id"] for r in ranges] == [4, 6]
    with Shard.open(mini) as shard:
        assert [(r.id, r.load()) for r in shard] == [(i, f"m{i}") for i in range(4, 8)]


def test_what_another_host_did_comes_back_as_its_own_file_and_is_unioned(tmp_path):
    """A host keeps its own done set; back here it is merged, and what either did is done."""
    import shutil

    db, other = _project(tmp_path), _project(tmp_path / "other")
    try:
        mark_done(db, "run", "k", [1, 2], total=6)
        mark_done(other, "run", "k", [3, 4])
        mark_done(other, "run", "k", [5], narrowed_by="cut-1")
        came = tmp_path / "came.arrows"
        shutil.copyfile(sidecar(other, "run", "k"), came)

        assert merge(db, "run", "k", came) == 5
        assert [int(s) for s in done(db, "run", "k")] == [1, 2, 3, 4]
        assert [int(s) for s in done(db, "run", "k", narrowed_by="cut-1")] == [1, 2, 3, 4, 5]
        assert [row["done"] for row in progress(db, "run")] == [5]
    finally:
        db.disconnect()
        other.disconnect()
