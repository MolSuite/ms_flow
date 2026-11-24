"""The shard container: indexed access with holes, and a file that survives the trip."""
import os
import uuid

import pyarrow as pa
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
        kind=PayloadKind.GENERIC_BYTES,
        serializer=Serializer.UTF8,
        slot_count=slot_count,
        **kw,
    ) as writer:
        for id_ in ids:
            writer.add(id_, f"spc-{id_}".encode())
    return path


def test_holes_keep_the_ids_they_belong_to(tmp_path):
    """The point of the container: a filtered-out record leaves a hole, not a renumbering."""
    shard = Shard.open(_write(tmp_path / "a.arrows", [100, 102, 105], base_id=100, slot_count=10))
    with shard:
        assert len(shard) == 3
        assert [record.id for record in shard] == [100, 102, 105]
        assert shard[102].load() == "spc-102"
        assert 101 not in shard and 999 not in shard
        with pytest.raises(KeyError):
            shard[101]


def test_slot_count_shrinks_to_the_last_id_written(tmp_path):
    """slot_count is a capacity; a short shard does not claim the ids it never had."""
    with Shard.open(_write(tmp_path / "b.arrows", range(10), slot_count=4096)) as shard:
        assert shard.slot_count == shard.record_count == 10
    with Shard.open(_write(tmp_path / "c.arrows", [0, 9], slot_count=4096)) as shard:
        assert shard.slot_count == 10 and shard.record_count == 2


def test_a_shard_bigger_than_the_cap_is_refused_up_front(tmp_path):
    """The reader decodes a shard whole, so the range is capped when the writer opens."""
    with pytest.raises(ValueError, match="slot_count"):
        ShardWriter(
            tmp_path / "d.arrows",
            dataset_id=None,
            shard_id=0,
            base_id=0,
            kind=0,
            serializer=Serializer.RAW,
            slot_count=MAX_RECORDS + 1,
        )


def test_ids_must_arrive_in_order_and_inside_the_range(tmp_path):
    writer = ShardWriter(
        tmp_path / "e.arrows", dataset_id=None, shard_id=0, base_id=10, kind=0,
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
    path = tmp_path / "f.arrows"
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
    path = tmp_path / "g.arrows"
    with pytest.raises(RuntimeError):
        with ShardWriter(path, dataset_id=None, shard_id=0, base_id=0, kind=0,
                         serializer=Serializer.RAW, slot_count=8) as writer:
            writer.add(0, b"x")
            raise RuntimeError("boom")
    assert list(tmp_path.iterdir()) == []


def test_metadata_and_dataset_id_travel_with_the_file(tmp_path):
    dataset = uuid.UUID(int=7)
    path = _write(tmp_path / "h.arrows", range(3), metadata={"source": "lib.csv"})
    with Shard.open(path, dataset_id=dataset) as shard:
        assert shard.metadata == {"source": "lib.csv"} and shard.dataset_id == dataset
    with pytest.raises(ValueError, match="dataset_id mismatch"):
        Shard.open(path, dataset_id=uuid.UUID(int=8))


def test_a_shard_is_a_plain_arrow_stream(tmp_path):
    """Any Arrow reader sees ids and text: the index says where the bytes are, pyarrow decodes them."""
    import pyarrow.ipc as ipc

    from ms_flow.core.data import container

    path = _write(tmp_path / "i.arrows", [3, 5], base_id=3, slot_count=4)
    info, rows = container.read_index(path)
    stream = container.read_head(path, info) + container.read_raw(path, rows[0])
    assert ipc.open_stream(bytes(stream)).read_all().to_pydict() == {"id": [3, 5], "record": ["spc-3", "spc-5"],
                                                                    "reason": [None, None]}
    with Shard.open(path) as shard:
        assert [bytes(view) for view in shard.iter_views()] == [b"spc-3", b"spc-5"]


def test_an_old_parquet_container_is_refused_with_a_way_out(tmp_path):
    import pyarrow.parquet as pq

    old = tmp_path / "old.arrows"
    pq.write_table(pa.table({"id": pa.array([1], pa.uint64())}), old)
    mshard = tmp_path / "older.arrows"
    mshard.write_bytes(b"MSHARD" + bytes(100))
    for path in (old, mshard):
        with pytest.raises(ValueError, match="re-import the project"):
            Shard.open(path)
    with pytest.raises(FileNotFoundError):
        Shard.open(tmp_path / "missing.arrows")


def test_a_shard_file_is_named_arrows(tmp_path):
    with pytest.raises(ValueError, match=r"\*\.arrows"):
        _write(tmp_path / "x.parquet", [1])


def test_reading_a_shard_brings_no_database():
    """A worker opens shards; the package they live in must not load SQL (or pyarrow) with them."""
    import subprocess
    import sys

    code = (
        "import sys; from ms_flow.core.data.shard import Shard; "
        "assert 'sqlalchemy' not in sys.modules and 'pyarrow' not in sys.modules"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


def test_a_big_shard_streams_one_batch_at_a_time(tmp_path, monkeypatch):
    """Memory stays near one batch of Python objects: the records become Arrow batches as they come."""
    from ms_flow.core.data import container

    monkeypatch.setattr(shard_module, "_BATCH_BYTES", 1000)
    path = tmp_path / "big.arrows"
    with ShardWriter(path, dataset_id=None, shard_id=0, base_id=0, kind=0,
                     serializer=Serializer.UTF8, slot_count=MAX_RECORDS) as writer:
        for id_ in range(0, 2000, 2):
            writer.add(id_, f"spc-{id_}".encode())
        assert len(writer._batches) > 5 and len(writer._ids) < 200
    assert container.read_index(path)[1][0]["n_batches"] > 5
    with Shard.open(path) as shard:
        assert shard.record_count == 1000 and shard.slot_count == 1999
        assert shard[1998].load() == "spc-1998" and 1997 not in shard


def test_many_shards_go_in_few_commits(tmp_path, monkeypatch):
    """Each commit fsyncs data, index and folder: shards wait for one until enough bytes do."""
    from ms_flow.core.data import container

    commits = []
    real = container.Appender.commit
    monkeypatch.setattr(container.Appender, "commit", lambda self, shards: commits.append(1) or real(self, shards))
    monkeypatch.setattr(shard_module, "_COMMIT_BYTES", 1500)
    path = tmp_path / "many.arrows"
    with ShardWriter(path, dataset_id=None, shard_id=0, base_id=0, kind=0, serializer=Serializer.RAW,
                     slot_count=MAX_RECORDS) as writer:
        for id_ in range(400):
            writer.add(id_, os.urandom(40))
            if id_ % 10 == 9:
                writer.end_group()
    assert len(shard_module.file_groups(path)) == 40 and 2 < len(commits) < 40


def test_bad_text_leaves_no_shard_behind(tmp_path):
    path = tmp_path / "bad.arrows"
    with pytest.raises(pa.ArrowInvalid):
        with ShardWriter(path, dataset_id=None, shard_id=0, base_id=0, kind=0,
                         serializer=Serializer.UTF8, slot_count=8) as writer:
            writer.add(0, b"\xff\xfe")
    assert list(tmp_path.iterdir()) == []


def test_an_empty_shard_is_a_valid_file(tmp_path):
    with Shard.open(_write(tmp_path / "empty.arrows", [])) as shard:
        assert len(shard) == 0 and shard.slot_count == 0


def test_one_file_many_shards_each_read_by_its_id_range(tmp_path):
    """A big file of logical shards: each is a row group, read alone by its number."""
    path = tmp_path / "vol.arrows"
    with ShardWriter(path, dataset_id=None, shard_id=0, base_id=0, kind=PayloadKind.GENERIC_BYTES,
                     serializer=Serializer.UTF8, slot_count=1000, source_index=True) as writer:
        for id_ in range(1000):
            if id_ != 305:  # a hole inside the second shard
                writer.add(id_, f"spc-{id_}".encode(), source_index=10 * id_)
            if id_ % 256 == 255:
                writer.end_group()
    assert len(shard_module.file_groups(path)) == 4
    with Shard.open(path, shard=1) as shard:
        assert (shard.base_id, shard.slot_count, shard.record_count) == (256, 256, 255)
        assert [r.id for r in shard][:2] == [256, 257] and 305 not in shard
        assert shard[511].load() == "spc-511" and 512 not in shard
    with Shard.open(path) as shard:  # the whole file still opens as one shard
        assert shard.record_count == 999
    from ms_flow.core.data import container

    info, rows = container.read_index(path)
    assert container.read_table(path, info, rows, ["source_index"]).column(0)[3].as_py() == 30
    # a raw copy keeps every column, source_index too
    from ms_flow.core.data.shard import merge_shards

    merge_shards([(path, 0)], tmp_path / "copy.arrows")
    info, rows = container.read_index(tmp_path / "copy.arrows")
    assert container.read_table(tmp_path / "copy.arrows", info, rows, ["source_index"]).column(0)[3].as_py() == 30


def test_a_rewritten_file_is_not_read_through_a_stale_index(tmp_path):
    path = _write(tmp_path / "f.arrows", range(10), slot_count=10)
    with Shard.open(path, shard=0) as shard:
        assert len(shard) == 10
    _write(path, range(3), slot_count=10)
    os.utime(path, ns=(1, 1))  # same second on a coarse clock: the size still differs
    with Shard.open(path, shard=0) as shard:
        assert len(shard) == 3


def test_merge_shards_adopts_files_into_one_even_over_a_source(tmp_path):
    from ms_flow.core.data.shard import merge_shards

    big = tmp_path / "big.arrows"
    with ShardWriter(big, dataset_id=uuid.UUID(int=7), shard_id=0, base_id=0, kind=PayloadKind.GENERIC_BYTES,
                     serializer=Serializer.UTF8, slot_count=MAX_RECORDS) as writer:
        for lo in (0, 10):
            for id_ in (lo, lo + 1):
                writer.add(id_, f"spc-{id_}".encode())
            writer.end_group()
    late = _write(tmp_path / "late.arrows", [21], base_id=20, slot_count=5)
    # A redone shard (late) joins the shards that were already merged, into the same file.
    ranges = merge_shards([(big, 0), (big, 1), (late, None)], big, metadata={"merged": True})
    # Each shard keeps the range it was written with (the late one from its file's base_id).
    assert ranges == [{"base_id": 0, "slot_count": 2, "record_count": 2, "scope": None, "origin": None, "n_dropped": 0},
                      {"base_id": 10, "slot_count": 2, "record_count": 2, "scope": None, "origin": None, "n_dropped": 0},
                      {"base_id": 20, "slot_count": 2, "record_count": 1, "scope": None, "origin": None, "n_dropped": 0}]
    assert len(shard_module.file_groups(big)) == 3
    with Shard.open(big, shard=2) as shard:
        assert [r.load() for r in shard] == ["spc-21"] and shard.metadata["merged"] is True



def _imports(path, *imports, origin=None):
    """One file, one shard per import: each import's ids start at 0 again (I7)."""
    with ShardWriter(path, dataset_id=uuid.UUID(int=7), shard_id=0, base_id=0, kind=PayloadKind.GENERIC_BYTES,
                     serializer=Serializer.UTF8, slot_count=MAX_RECORDS) as writer:
        for scope, texts in enumerate(imports, start=1):
            for id_, text in enumerate(texts):
                if text is None:
                    writer.drop(id_, "gone")
                else:
                    writer.add(id_, text.encode())
            writer.end_group(scope=scope, base_id=0, slot_count=len(texts), origin=origin)
    return path


def test_two_imports_in_one_file_open_as_their_own_shards(tmp_path):
    path = _imports(tmp_path / "two.arrows", ["a0", "a1", "a2"], ["b0", "b1"])
    with Shard.open(path, shard=0) as shard:
        assert [r.load() for r in shard] == ["a0", "a1", "a2"]
        assert (shard.base_id, shard.slot_count, shard.record_count) == (0, 3, 3)
    with Shard.open(path, shard=1, record_ids=[1, 9]) as shard:
        assert [r.load() for r in shard] == ["b1"] and shard[1].load() == "b1"
    with pytest.raises(ValueError, match="no shard 2"):
        Shard.open(path, shard=2)


def test_a_redone_shard_opens_its_own_records(tmp_path):
    from ms_flow.core.data.shard import merge_shards, read_dropped

    path = _imports(tmp_path / "redo.arrows", ["old0", "old1", "old2"], ["new0", None, "new2"], origin=5)
    with Shard.open(path, shard=1) as shard:
        assert [(r.id, r.load()) for r in shard] == [(0, "new0"), (2, "new2")]
    with Shard.open(path, shard=0) as shard:
        assert [r.load() for r in shard] == ["old0", "old1", "old2"]
    assert read_dropped(path, shard=1) == [(1, "gone")]
    # the redo alone, copied by its number
    assert merge_shards([(path, 1)], tmp_path / "only.arrows")[0]["record_count"] == 2
    with Shard.open(tmp_path / "only.arrows") as shard:
        assert [r.load() for r in shard] == ["new0", "new2"]


def test_an_empty_record_is_not_a_drop(tmp_path):
    from ms_flow.core.data.shard import file_groups, read_dropped

    path = _imports(tmp_path / "empty.arrows", ["", None, "x"])
    with Shard.open(path) as shard:
        assert [(r.id, r.load()) for r in shard] == [(0, ""), (2, "x")] and len(shard) == 2
    assert read_dropped(path) == [(1, "gone")]
    group = file_groups(path)[0]
    assert (group.n_records, group.n_dropped) == (2, 1)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["empty.arrows", "empty.arrows.index"]  # no sidecar


def test_drops_survive_a_raw_append(tmp_path):
    from ms_flow.core.data.shard import file_groups, merge_shards, read_dropped

    one = _imports(tmp_path / "one.arrows", ["a0", None, "a2"])
    two = _imports(tmp_path / "two.arrows", [None, "b1"])
    made = merge_shards([(one, None), (two, None)], tmp_path / "both.arrows")
    assert [m["n_dropped"] for m in made] == [1, 1]
    assert [(g.n_records, g.n_dropped) for g in file_groups(tmp_path / "both.arrows")] == [(2, 1), (1, 1)]
    assert read_dropped(tmp_path / "both.arrows", shard=0) == [(1, "gone")]
    assert read_dropped(tmp_path / "both.arrows", shard=1) == [(0, "gone")]
    with Shard.open(tmp_path / "both.arrows", shard=1) as shard:
        assert [r.load() for r in shard] == ["b1"]


def test_read_dropped_decodes_only_two_columns(tmp_path, monkeypatch):
    from ms_flow.core.data import container
    from ms_flow.core.data.shard import read_dropped

    path = _imports(tmp_path / "cols.arrows", ["a0", None], ["b0", "b1"])
    asked, real = [], container.read_table

    def spy(path, info, rows, columns=None):
        asked.append((columns, [r["shard"] for r in rows]))
        return real(path, info, rows, columns)

    monkeypatch.setattr(container, "read_table", spy)
    assert read_dropped(path) == [(1, "gone")]
    assert asked == [(["id", "reason"], [0])]  # the shard without drops is not read at all


def test_open_without_a_shard_refuses_an_ambiguous_file(tmp_path):
    path = _imports(tmp_path / "two.arrows", ["a0", "a1"], ["b0", "b1"])
    with pytest.raises(ValueError, match="open one by number"):
        Shard.open(path)
    # ids sorted across the file (or one shard): the whole file is still one read
    sorted_file = _write(tmp_path / "one.arrows", range(5))
    with Shard.open(sorted_file) as shard:
        assert len(shard) == 5
