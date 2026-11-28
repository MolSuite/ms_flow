"""ms_flow owns every storage write (M7b): a part writer for workers, plain tables written
atomically, a file made on another host registered once."""
import pandas as pd
import pyarrow as pa
import pytest

from ms_flow.core.data import catalog, tables
from ms_flow.core.data.shard import Group, PartWriter, Serializer, Shard, file_groups, read_rows
from ms_flow.core.database import ProjectStore


def _ranges(path):
    return [(g.base_id, g.slot_count, g.n_records, g.n_dropped) for g in file_groups(path)]


def test_a_part_takes_its_range_from_its_ids(tmp_path):
    path = tmp_path / "p.arrows"
    with PartWriter(path, kind=0, meta={"step": "x"}) as writer:
        for id_ in range(10, 14):
            writer.add(id_, b"r")
        writer.drop(14, "bad")
    assert _ranges(path) == [(10, 5, 4, 1)]
    with Shard.open(path) as shard:
        assert (shard.base_id, shard.slot_count, shard.metadata) == (10, 5, {"step": "x"})


def test_a_derived_part_keeps_the_range_of_the_shard_it_came_from(tmp_path):
    like = Group(base_id=100, slot_count=8, n_records=8, scope=None)
    path = tmp_path / "d.arrows"
    with PartWriter(path, kind=0, like=like, scope=3, origin=5) as writer:
        writer.add(101, b"a")
        writer.drop(103, "bad")
    (group,) = file_groups(path)
    assert (group.base_id, group.slot_count, group.scope, group.origin) == (100, 8, 3, 5)
    assert (group.n_records, group.n_dropped) == (1, 1)

    # Dropped whole, it still covers its origin's range.
    empty = tmp_path / "e.arrows"
    with PartWriter(empty, kind=0, like=like, origin=5) as writer:
        writer.drop(100, "bad")
    assert _ranges(empty) == [(100, 8, 0, 1)]


def test_group_size_splits_the_range_and_skips_the_groups_without_ids(tmp_path):
    like = Group(base_id=0, slot_count=10, n_records=0, scope=None)
    path = tmp_path / "g.arrows"
    with PartWriter(path, kind=0, like=like, group_size=4) as writer:
        for id_ in (0, 1, 9):
            writer.add(id_, b"r")
    assert _ranges(path) == [(0, 4, 2, 0), (8, 2, 1, 0)]


def test_a_part_keeps_a_source_index_and_drops_before_the_first_record(tmp_path):
    path = tmp_path / "s.arrows"
    with PartWriter(path, kind=0, serializer=Serializer.RAW) as writer:
        writer.drop(0, "too many")
        writer.add(1, b"\x00\x01", source_index=7)
    rows = read_rows(path)
    assert rows.column("source_index").to_pylist() == [None, 7]
    assert rows.column("reason").to_pylist() == ["too many", None]


def test_a_part_that_fails_leaves_nothing(tmp_path):
    path = tmp_path / "f.arrows"
    with pytest.raises(RuntimeError):
        with PartWriter(path, kind=0) as writer:
            writer.add(1, b"r")
            raise RuntimeError("worker died")
    assert list(tmp_path.iterdir()) == []


def test_a_table_is_written_whole_or_not_at_all(tmp_path):
    path = tmp_path / "t.parquet"
    tables.write(path, pa.table({"a": [1, 2]}))
    assert tables.read(path).column("a").to_pylist() == [1, 2]
    with pytest.raises(Exception):
        tables.write(path, object())  # nothing a table can be made of
    assert tables.read(path).column("a").to_pylist() == [1, 2]
    assert [p.name for p in tmp_path.iterdir()] == ["t.parquet"]

    frame = pd.DataFrame({"x": [0.5]})
    frame.attrs["evr"] = [0.9]
    tables.write(path, frame)
    assert pd.read_parquet(path).attrs == {"evr": [0.9]}


def test_a_table_round_trips_through_bytes():
    table = pa.table({"key": ["a", "b"], "value": ["1", "2"]})
    assert tables.from_bytes(tables.to_bytes(table)).equals(table)


def test_a_file_made_on_another_host_is_registered_once(tmp_path):
    db = ProjectStore()
    db.connect(tmp_path / "project")
    try:
        generation = catalog.create_generation(db, step="prep")
        there = catalog.add_remote(db, "/scratch/p.arrows", location="user@hpc:/scratch", role=catalog.SOURCE,
                                   counts={"n_shards": 2, "n_records": 8}, generation_id=generation)
        again = catalog.add_remote(db, "/scratch/p.arrows", location="user@hpc:/scratch", role=catalog.SOURCE,
                                   counts={"n_shards": 2, "n_records": 8}, generation_id=generation)
        (row,) = catalog.files(db, catalog.shard_spec(generation_id=generation))
        assert again == there and (row["location"], row["n_shards"]) == ("user@hpc:/scratch", 2)
    finally:
        db.disconnect()
