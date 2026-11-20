"""The shard container: indexed access with holes, and a file that survives the trip."""
import json
import os
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ms_flow.core.data import shard as shard_module
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
    """The reader decodes a shard whole, so the range is capped when the writer opens."""
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


def test_a_big_shard_streams_one_row_group_at_a_time(tmp_path, monkeypatch):
    """Memory stays at one row group: the records reach the file as they come, not on close."""
    monkeypatch.setattr(shard_module, "_ROW_GROUP_BYTES", 1000)
    path = tmp_path / "big.parquet"
    with ShardWriter(path, dataset_id=None, shard_id=0, base_id=0, kind=0,
                     serializer=Serializer.UTF8, slot_count=MAX_RECORDS) as writer:
        for id_ in range(0, 2000, 2):
            writer.add(id_, f"mol-{id_}".encode())
        assert writer._tmp_path.stat().st_size > 0 and len(writer._ids) < 200
    assert pq.ParquetFile(path).num_row_groups > 5
    with Shard.open(path) as shard:
        assert shard.record_count == 1000 and shard.slot_count == 1999
        assert shard[1998].load() == "mol-1998" and 1997 not in shard


def test_a_shard_written_in_one_go_still_opens(tmp_path):
    """Shards from before streaming carry the header in the Arrow schema; they still read."""
    header = {"format": "ms_flow-shard-parquet-v1", "dataset_id": str(uuid.UUID(int=0)),
              "shard_id": 4, "base_id": 10, "slot_count": 2, "record_count": 2, "kind": 0,
              "serializer": Serializer.RAW, "codec": 0, "metadata": {"old": True}}
    table = pa.table({"id": pa.array([10, 11], pa.uint64()), "record": pa.array([b"a", b"b"])})
    path = tmp_path / "old.parquet"
    pq.write_table(table.replace_schema_metadata({b"ms_flow.shard": json.dumps(header).encode()}),
                   path, compression="zstd", write_page_checksum=True)
    with Shard.open(path) as shard:
        assert shard.shard_id == 4 and shard.metadata == {"old": True}
        assert shard[11].load() == b"b"


def test_bad_text_leaves_no_shard_behind(tmp_path):
    path = tmp_path / "bad.parquet"
    with pytest.raises(pa.ArrowInvalid):
        with ShardWriter(path, dataset_id=None, shard_id=0, base_id=0, kind=0,
                         serializer=Serializer.UTF8, slot_count=8) as writer:
            writer.add(0, b"\xff\xfe")
    assert list(tmp_path.iterdir()) == []


def test_an_empty_shard_is_a_valid_file(tmp_path):
    with Shard.open(_write(tmp_path / "empty.parquet", [])) as shard:
        assert len(shard) == 0 and shard.slot_count == 0


def test_one_file_many_shards_each_read_by_its_id_range(tmp_path):
    """A big file of logical shards: each is a row group, read alone by its id range."""
    path = tmp_path / "vol.parquet"
    with ShardWriter(path, dataset_id=None, shard_id=0, base_id=0, kind=PayloadKind.SMILES,
                     serializer=Serializer.UTF8, slot_count=1000, source_index=True) as writer:
        for id_ in range(1000):
            if id_ != 305:  # a hole inside the second shard
                writer.add(id_, f"mol-{id_}".encode(), source_index=10 * id_)
            if id_ % 256 == 255:
                writer.end_group()
    meta = pq.read_metadata(path)
    assert meta.num_row_groups == 4
    stats = {meta.schema.column(c).name: meta.row_group(0).column(c).is_stats_set for c in range(meta.num_columns)}
    assert stats == {"id": True, "record": False, "source_index": False}
    with Shard.open(path, ids=(256, 512)) as shard:
        assert (shard.base_id, shard.slot_count, shard.record_count) == (256, 256, 255)
        assert [r.id for r in shard][:2] == [256, 257] and 305 not in shard
        assert shard[511].load() == "mol-511" and 512 not in shard
    with Shard.open(path, ids=(250, 260)) as shard:  # straddles two row groups
        assert [r.id for r in shard] == list(range(250, 260))
    with Shard.open(path) as shard:  # the whole file still opens as one shard
        assert shard.record_count == 999
    assert pq.read_table(path, columns=["source_index"]).column(0)[3].as_py() == 30


def test_a_rewritten_file_is_not_read_through_a_stale_footer(tmp_path):
    path = _write(tmp_path / "f.parquet", range(10), slot_count=10)
    with Shard.open(path, ids=(0, 10)) as shard:
        assert len(shard) == 10
    _write(path, range(3), slot_count=10)
    os.utime(path, ns=(1, 1))  # same second on a coarse clock: the size still differs
    with Shard.open(path, ids=(0, 10)) as shard:
        assert len(shard) == 3


def test_merge_shards_adopts_files_into_one_even_over_a_source(tmp_path):
    from ms_flow.core.data.shard import merge_shards

    big = tmp_path / "big.parquet"
    with ShardWriter(big, dataset_id=uuid.UUID(int=7), shard_id=0, base_id=0, kind=PayloadKind.SDF,
                     serializer=Serializer.UTF8, slot_count=MAX_RECORDS) as writer:
        for lo in (0, 10):
            for id_ in (lo, lo + 1):
                writer.add(id_, f"mol-{id_}".encode())
            writer.end_group()
    late = _write(tmp_path / "late.parquet", [21], base_id=20, slot_count=5)
    # A redone shard (late) joins the shards that were already merged, into the same file.
    ranges = merge_shards([(big, (0, 10)), (big, (10, 20)), (late, None)], big, metadata={"merged": True})
    # Each shard keeps the range it was written with (the late one from its file's base_id).
    assert ranges == [{"base_id": 0, "slot_count": 2, "record_count": 2, "scope": None},
                      {"base_id": 10, "slot_count": 2, "record_count": 2, "scope": None},
                      {"base_id": 20, "slot_count": 2, "record_count": 1, "scope": None}]
    assert pq.ParquetFile(big).num_row_groups == 3
    with Shard.open(big, ids=(20, 22)) as shard:
        assert [r.load() for r in shard] == ["mol-21"] and shard.metadata["merged"] is True

