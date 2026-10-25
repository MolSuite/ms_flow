"""The shard container: indexed access with holes, and a file that survives the trip."""
import os
import uuid

import pyarrow.parquet as pq
import pytest

from ms_flow.core.data.shard import (
    MAX_RECORDS,
    PayloadKind,
    Serializer,
    Shard,
    ShardWriter,
)


def _write(path, ids, *, base_id=0, slot_count=4096, **kw):
    with ShardWriter(
        path,
        dataset_id=uuid.UUID(int=7),
        shard_id=0,
        base_id=base_id,
        kind=PayloadKind.SDF,
        serializer=Serializer.UTF8,
        slot_count=slot_count,
        **kw,
    ) as writer:
        for id_ in ids:
            writer.add(id_, f"mol-{id_}".encode())
    return path


def test_holes_keep_the_ids_they_belong_to(tmp_path):
    """The point of the container: a filtered-out record leaves a hole, not a renumbering."""
    shard = Shard.open(_write(tmp_path / "a.parquet", [100, 102, 105], base_id=100, slot_count=10))
    with shard:
        assert len(shard) == 3
        assert [record.id for record in shard] == [100, 102, 105]
        assert shard[102].load() == "mol-102"
        assert 101 not in shard and 999 not in shard
        with pytest.raises(KeyError):
            shard[101]


def test_slot_count_shrinks_to_the_last_id_written(tmp_path):
    """slot_count is a capacity; a short shard does not claim the ids it never had."""
    with Shard.open(_write(tmp_path / "b.parquet", range(10), slot_count=4096)) as shard:
        assert shard.slot_count == shard.record_count == 10
    with Shard.open(_write(tmp_path / "c.parquet", [0, 9], slot_count=4096)) as shard:
        assert shard.slot_count == 10 and shard.record_count == 2


def test_a_shard_bigger_than_the_cap_is_refused_up_front(tmp_path):
    """The writer holds the shard in memory, so the range is capped when it opens."""
    with pytest.raises(ValueError, match="slot_count"):
        ShardWriter(
            tmp_path / "d.parquet",
            dataset_id=None,
            shard_id=0,
            base_id=0,
            kind=0,
            serializer=Serializer.RAW,
            slot_count=MAX_RECORDS + 1,
        )


def test_ids_must_arrive_in_order_and_inside_the_range(tmp_path):
    writer = ShardWriter(
        tmp_path / "e.parquet", dataset_id=None, shard_id=0, base_id=10, kind=0,
        serializer=Serializer.RAW, slot_count=5,
    )
    writer.add(11, b"x")
    with pytest.raises(ValueError, match="increasing"):
        writer.add(11, b"y")
    with pytest.raises(ValueError, match="out of range"):
        writer.add(99, b"z")
    with pytest.raises(TypeError):
        writer.add(12, "not bytes")
    writer.close()


def test_a_corrupted_shard_refuses_to_open(tmp_path):
    path = tmp_path / "f.parquet"
    with ShardWriter(path, dataset_id=None, shard_id=0, base_id=0, kind=0,
                     serializer=Serializer.RAW, slot_count=8) as writer:
        for index in range(8):
            writer.add(index, os.urandom(20_000))  # incompressible: the middle is record data
    with Shard.open(path) as shard:
        assert shard.verify()
    data = bytearray(path.read_bytes())
    data[len(data) // 2] ^= 0xFF
    path.write_bytes(data)
    with pytest.raises(ValueError, match="unreadable shard"):
        Shard.open(path)


def test_a_failed_write_leaves_no_shard_behind(tmp_path):
    path = tmp_path / "g.parquet"
    with pytest.raises(RuntimeError):
        with ShardWriter(path, dataset_id=None, shard_id=0, base_id=0, kind=0,
                         serializer=Serializer.RAW, slot_count=8) as writer:
            writer.add(0, b"x")
            raise RuntimeError("boom")
    assert list(tmp_path.iterdir()) == []


def test_metadata_and_dataset_id_travel_with_the_file(tmp_path):
    dataset = uuid.UUID(int=7)
    path = _write(tmp_path / "h.parquet", range(3), metadata={"source": "lib.sdf"})
    with Shard.open(path, dataset_id=dataset) as shard:
        assert shard.metadata == {"source": "lib.sdf"} and shard.dataset_id == dataset
    with pytest.raises(ValueError, match="dataset_id mismatch"):
        Shard.open(path, dataset_id=uuid.UUID(int=8))


def test_a_shard_is_plain_parquet(tmp_path):
    """What makes it worth it: any Parquet reader sees ids and text, no container code."""
    table = pq.read_table(_write(tmp_path / "i.parquet", [3, 5], base_id=3, slot_count=4))
    assert table.to_pydict() == {"id": [3, 5], "record": ["mol-3", "mol-5"]}
    with Shard.open(tmp_path / "i.parquet") as shard:
        assert [bytes(view) for view in shard.iter_views()] == [b"mol-3", b"mol-5"]


def test_a_legacy_mshard_says_to_reimport(tmp_path):
    path = tmp_path / "old.mshard"
    path.write_bytes(b"MSHARD" + bytes(100))
    with pytest.raises(ValueError, match="re-import"):
        Shard.open(path)


def test_reading_a_shard_brings_no_database():
    """A worker opens shards; the package they live in must not load SQL (or pyarrow) with them."""
    import subprocess
    import sys

    code = (
        "import sys; from ms_flow.core.data.shard import Shard; "
        "assert 'sqlalchemy' not in sys.modules and 'pyarrow' not in sys.modules"
    )
    subprocess.run([sys.executable, "-c", code], check=True)
