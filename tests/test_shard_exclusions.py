"""Exclusions: cuts as sidecar files, their union, undo, and the shard rows they narrow."""
import uuid

from ms_flow.core.data.exclusions import (
    active_exclusions,
    exclude,
    list_exclusions,
    set_exclusion_active,
)
from ms_flow.core.data.shard import PayloadKind, Serializer, Shard, ShardWriter
from ms_flow.core.database import ProjectStore


def _project(tmp_path):
    db = ProjectStore()
    db.connect(tmp_path / "project")
    return db


def test_cuts_add_up_and_undo(tmp_path):
    db = _project(tmp_path)
    try:
        first = exclude(db, {1: [5, 3, 3], 2: [7]}, reason="filter")
        second = exclude(db, {1: [4]}, reason="model")
        assert exclude(db, {1: []}) is None
        out = active_exclusions(db)
        assert out.in_range(1, 0, 10).tolist() == [3, 4, 5] and out.in_range(2, 0, 10).tolist() == [7]
        set_exclusion_active(db, first, False)
        assert active_exclusions(db).in_range(1, 0, 10).tolist() == [4]
        assert [(c["id"], c["active"], c["reason"]) for c in list_exclusions(db)] == [
            (second, True, "model"), (first, False, "filter")]
    finally:
        db.disconnect()


def test_narrow_drops_whole_shards_and_lists_partial_ones(tmp_path):
    db = _project(tmp_path)
    try:
        exclude(db, {1: [0, 1, 2, 3, 6], 2: [99]})
        rows = [
            {"id": 1, "scope": 1, "base_id": 0, "slot_count": 4, "n_records": 4},   # all out
            {"id": 2, "scope": 1, "base_id": 4, "slot_count": 4, "n_records": 4},   # 6 out
            {"id": 3, "scope": 3, "base_id": 0, "slot_count": 4, "n_records": 4},   # untouched
            {"id": 4, "scope": 1, "base_id": 4, "slot_count": 4, "n_records": 2, "record_ids": [6, 7]},
        ]
        got = list(active_exclusions(db).narrow(rows))
        assert [r["id"] for r in got] == [2, 3, 4]
        assert got[0]["record_ids"] == [4, 5, 7] and got[0]["n_records"] == 3
        assert "record_ids" not in got[1]
        assert got[2]["record_ids"] == [7] and got[2]["n_records"] == 1
    finally:
        db.disconnect()


def test_narrow_reads_the_range_of_a_file_per_shard_from_its_index(tmp_path):
    db = _project(tmp_path)
    path = tmp_path / "s.arrows"
    with ShardWriter(path, dataset_id=uuid.UUID(int=1), shard_id=0, base_id=10, slot_count=3,
                     kind=PayloadKind.GENERIC_BYTES, serializer=Serializer.UTF8) as writer:
        for i in (10, 11, 12):
            writer.add(i, b"x")
    try:
        exclude(db, {1: [11]})
        (row,) = active_exclusions(db).narrow([{"scope": 1, "path": str(path), "n_records": 3}])
        assert row["record_ids"] == [10, 12]
    finally:
        db.disconnect()


def test_open_reads_only_the_record_ids_asked(tmp_path):
    path = tmp_path / "s.arrows"
    with ShardWriter(path, dataset_id=uuid.UUID(int=1), shard_id=0, base_id=0, slot_count=8,
                     kind=PayloadKind.GENERIC_BYTES, serializer=Serializer.UTF8) as writer:
        for i in range(8):
            writer.add(i, f"m{i}".encode())
            if i == 3:
                writer.end_group()
    with Shard.open(path, shard=1, record_ids=[1, 5, 7, 9]) as shard:
        assert [r.id for r in shard] == [5, 7] and len(shard) == 2 and shard.base_id == 4
    with Shard.open(path, record_ids=[2]) as shard:
        assert [r.load() for r in shard] == ["m2"]
    with Shard.open(path, record_ids=[]) as shard:
        assert len(shard) == 0


# --- exact holes (M3d): counts and narrowed ids come from the ids that have a usable record ---


def _sparse(path, adds, drops=(), *, slot=8):
    with ShardWriter(path, dataset_id=uuid.UUID(int=1), shard_id=0, base_id=0, slot_count=slot, scope=1,
                     kind=PayloadKind.GENERIC_BYTES, serializer=Serializer.UTF8) as writer:
        for i in sorted({*adds, *drops}):
            if i in adds:
                writer.add(i, f"m{i}".encode())
            else:
                writer.drop(i, "bad")
        writer.end_group(base_id=0, slot_count=slot)
    return path


def _registered(tmp_path, path):
    from ms_flow.core.data import catalog

    db = _project(tmp_path)
    gen = catalog.create_generation(db, step="import")
    catalog.activate_generation(db, gen)
    catalog.add_file(db, path, generation_id=gen)
    return db


def _counted(db):
    from ms_flow.core.data import catalog

    catalog.refresh_active(db)
    (row,) = catalog.files(db)
    return row["n_active"], list(catalog.shards(db))


def test_a_cut_on_a_hole_changes_no_count(tmp_path):
    db = _registered(tmp_path, _sparse(tmp_path / "h.arrows", {0, 2, 5}))
    try:
        exclude(db, {1: [1, 3]})  # holes only
        active, (shard,) = _counted(db)
        assert active == 3 and shard["n_records"] == 3
    finally:
        db.disconnect()


def test_narrowed_ids_never_name_a_hole(tmp_path):
    db = _registered(tmp_path, _sparse(tmp_path / "h.arrows", {0, 2, 5}))
    try:
        exclude(db, {1: [2]})
        active, (shard,) = _counted(db)
        assert shard["record_ids"] == [0, 5] and shard["n_records"] == 2 and active == 2
    finally:
        db.disconnect()


def test_narrowed_ids_never_name_a_drop(tmp_path):
    db = _registered(tmp_path, _sparse(tmp_path / "d.arrows", {0, 2}, {1}, slot=3))
    try:
        exclude(db, {1: [2]})
        active, (shard,) = _counted(db)
        assert shard["record_ids"] == [0] and shard["n_records"] == 1 and active == 1
    finally:
        db.disconnect()


def test_one_record_and_one_drop_count_one_active(tmp_path):
    db = _registered(tmp_path, _sparse(tmp_path / "d.arrows", {0}, {1}, slot=2))
    try:
        assert _counted(db)[0] == 1
        exclude(db, {1: [1]})  # the drop
        active, (shard,) = _counted(db)
        assert active == 1 and shard["n_records"] == 1
    finally:
        db.disconnect()


def test_counts_of_a_dense_shard_read_no_data(tmp_path):
    import pytest

    from ms_flow.core.data import container

    db = _registered(tmp_path, _sparse(tmp_path / "full.arrows", set(range(8))))
    try:
        exclude(db, {1: [3]})

        def refuse(*_args, **_kwargs):
            raise AssertionError("a count opened the data")

        with pytest.MonkeyPatch.context() as patch:
            for name in ("read_table", "iter_tables", "read_raw"):
                patch.setattr(container, name, refuse)
            active, (shard,) = _counted(db)
        assert active == 7 and shard["record_ids"] == [0, 1, 2, 4, 5, 6, 7]
    finally:
        db.disconnect()


def test_a_wide_sparse_shard_counts_from_its_id_column(tmp_path):
    """Over 65536 slots there is no bitmap: the ids are read (two small columns)."""
    db = _registered(tmp_path, _sparse(tmp_path / "wide.arrows", {0, 69_999}, {5}, slot=70_000))
    try:
        exclude(db, {1: [0, 5, 6]})
        active, (shard,) = _counted(db)
        assert shard["record_ids"] == [69_999] and active == 1
    finally:
        db.disconnect()
