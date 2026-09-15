"""The shard container: indexed access with holes, and a file that survives the trip."""
import os
import uuid

import pytest

from ms_flow.core.data.shard import (
    MAX_RECORDS,
    NOT_PRESENT,
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
    shard = Shard.open(_write(tmp_path / "a.mshard", [100, 102, 105], base_id=100, slot_count=10))
    with shard:
        assert len(shard) == 3
        assert [record.id for record in shard] == [100, 102, 105]
        assert shard[102].load() == "mol-102"
        assert 101 not in shard and 999 not in shard
        with pytest.raises(KeyError):
            shard[101]


def test_contiguous_records_pay_no_slot_map(tmp_path):
    """slot_count is a capacity; a full-but-short shard shrinks to dense on close."""
    path = _write(tmp_path / "b.mshard", range(10), slot_count=4096)
    with Shard.open(path) as shard:
        assert shard.slot_count == shard.record_count == 10
        assert shard._dense
        assert [record.id for record in shard] == list(range(10))
    sparse = _write(tmp_path / "c.mshard", [0, 9], slot_count=4096)
    with Shard.open(sparse) as shard:
        assert shard.slot_count == 10 and shard.record_count == 2 and not shard._dense


def test_a_shard_too_big_for_the_slot_map_is_refused_up_front(tmp_path):
    """A record index of 0xFFFF is indistinguishable from `absent`, so the range is capped
    when the writer opens — not after gigabytes have been written."""
    assert MAX_RECORDS == NOT_PRESENT
    with pytest.raises(ValueError, match="slot_count"):
        ShardWriter(
            tmp_path / "d.mshard",
            dataset_id=None,
            shard_id=0,
            base_id=0,
            kind=0,
            serializer=Serializer.RAW,
            slot_count=MAX_RECORDS + 1,
        )


def test_ids_must_arrive_in_order_and_inside_the_range(tmp_path):
    writer = ShardWriter(
        tmp_path / "e.mshard", dataset_id=None, shard_id=0, base_id=10, kind=0,
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


def test_the_footer_catches_a_corrupted_shard(tmp_path):
    path = _write(tmp_path / "f.mshard", range(5))
    with Shard.open(path) as shard:
        assert shard.verify()
    data = bytearray(path.read_bytes())
    data[100] ^= 0xFF
    path.write_bytes(data)
    with Shard.open(path) as shard:
        assert not shard.verify()


def test_a_failed_write_leaves_no_shard_behind(tmp_path):
    path = tmp_path / "g.mshard"
    with pytest.raises(RuntimeError):
        with ShardWriter(path, dataset_id=None, shard_id=0, base_id=0, kind=0,
                         serializer=Serializer.RAW, slot_count=8) as writer:
            writer.add(0, b"x")
            raise RuntimeError("boom")
    assert not path.exists() and not path.with_suffix(".mshard.tmp").exists()
    assert list(tmp_path.iterdir()) == []


def test_metadata_and_dataset_id_travel_with_the_file(tmp_path):
    dataset = uuid.UUID(int=7)
    path = _write(tmp_path / "h.mshard", range(3), metadata={"source": "lib.sdf"})
    with Shard.open(path, dataset_id=dataset) as shard:
        assert shard.metadata == {"source": "lib.sdf"} and shard.dataset_id == dataset
    with pytest.raises(ValueError, match="dataset_id mismatch"):
        Shard.open(path, dataset_id=uuid.UUID(int=8))


def test_views_are_zero_copy_and_die_with_the_shard(tmp_path):
    shard = Shard.open(_write(tmp_path / "i.mshard", range(4)))
    assert [bytes(view) for view in shard.iter_views()] == [f"mol-{i}".encode() for i in range(4)]
    held = shard[1].view()
    with pytest.raises(BufferError):
        shard.close()
    held.release()
    shard.close()


def test_verify_does_not_read_the_file_into_memory(tmp_path):
    """A shard is sized to be shipped, so `verify()` must stay O(1) in memory."""
    import tracemalloc

    path = tmp_path / "j.mshard"
    with ShardWriter(path, dataset_id=None, shard_id=0, base_id=0, kind=0,
                     serializer=Serializer.RAW, slot_count=64) as writer:
        for index in range(64):
            writer.add(index, os.urandom(200_000))
    with Shard.open(path) as shard:
        tracemalloc.start()
        assert shard.verify()
        peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()
    assert os.path.getsize(path) > 12_000_000
    assert peak < 4 * (1 << 20)
