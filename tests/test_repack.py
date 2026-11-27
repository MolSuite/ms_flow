"""M12b: after a strong cut, a library repacked into a new generation of fuller shards, record ids
kept; the previous one kept, gone back to without deleting anything, deleted only when nothing
names it."""
import numpy as np
import pytest

from ms_flow.core.data import catalog, container, exclusions, repack, states
from ms_flow.core.data.shard import PartWriter, PayloadKind, Serializer
from ms_flow.core.database import ProjectStore

SHARDS, SLOTS = 8, 32


def _library(tmp_path):
    """An imported library: 8 shards of 32 records (scope 1), ids 0..255, one container."""
    db = ProjectStore()
    db.connect(tmp_path / "project")
    gen = catalog.create_generation(db, step="import")
    receipts = []
    for n in range(SHARDS):
        path = tmp_path / "parts" / f"{n}.arrows"
        path.parent.mkdir(parents=True, exist_ok=True)
        like = type("Span", (), {"base_id": n * SLOTS, "slot_count": SLOTS})
        with PartWriter(path, kind=PayloadKind.GENERIC_BYTES, serializer=Serializer.UTF8, like=like, scope=1) as w:
            for i in range(n * SLOTS, (n + 1) * SLOTS):
                w.add(i, f"r{i}".encode(), source_index=i)
        receipts.append({**catalog.file_row(path, role=catalog.SOURCE, generation_id=gen, part=f"i:{n}")})
    with catalog.ContainerWriter(db, tmp_path / "lib", generation_id=gen, prefix="i:") as writer:
        writer.commit_parts(receipts)
    catalog.activate_generation(db, gen)
    return db, gen


def _cut(db, keep_every=4):
    """A strong filter: 3 records of every 4 left out."""
    ids = [i for i in range(SHARDS * SLOTS) if i % keep_every]
    return exclusions.exclude(db, {1: ids}, reason="filter")


def _records(db):
    """{id: bytes} of every usable record of the active generation's library."""
    from ms_flow.core.data.shard import Shard

    out = {}
    for row in catalog.shards(db):
        with Shard.open(row["path"], shard=row["shard_index"], record_ids=None) as shard:
            cut = exclusions.active_exclusions(db).in_range(row["scope"], row["base_id"],
                                                            row["base_id"] + row["slot_count"])
            for record in shard:
                if record.id not in set(cut.tolist()):
                    out[record.id] = bytes(record.view())
    return out


def _repacked(db, tmp_path, records=32):
    new, elements = repack.plan(db, tmp_path / "repacked", records=records)
    receipts = [repack.part(element) for element in elements]
    with catalog.ContainerWriter(db, tmp_path / "repacked", generation_id=new, prefix=f"repack:{new}:") as writer:
        assert not [r for r in writer.commit_parts(receipts) if "error" in r]
    repack.activate(db, new)
    return new


def test_repack_keeps_every_record_id(tmp_path):
    db, old = _library(tmp_path)
    try:
        _cut(db)
        before = _records(db)
        assert len(before) == SHARDS * SLOTS // 4
        new = _repacked(db, tmp_path)
        assert catalog.active_generation_id(db) == new
        assert _records(db) == before  # same ids, same bytes, and only those
        rows = list(catalog.shards(db))
        assert len(rows) == 2 and all(row["n_records"] == 32 for row in rows)  # fuller shards
        assert all(row["slot_count"] <= 65536 for row in rows)
    finally:
        db.disconnect()


def test_repack_is_not_offered_under_later_steps(tmp_path):
    db, gen = _library(tmp_path)
    try:
        assert repack.offer(db) is None
        states.mark_done(db, "docked", "k", [catalog.shards(db).__next__()["id"]], generation_id=gen)
        assert "state" in repack.offer(db)
        states.forget(db, "docked", "k")
        catalog.add_file(db, tmp_path / "lib" / "nothing.arrows", generation_id=gen, role="prepared",
                         counts={"n_shards": 1})
        assert "prepared" in repack.offer(db)
    finally:
        db.disconnect()


def test_the_previous_generation_stays_if_kept(tmp_path):
    db, old = _library(tmp_path)
    try:
        _cut(db)
        paths = [row["path"] for row in catalog.files(db, catalog.shard_spec(generation_id=old))]
        _repacked(db, tmp_path)
        assert all(__import__("os").path.isfile(p) for p in paths)
        # a later activation (another step) still leaves it: it is kept, not only the parent
        later = catalog.create_generation(db, step="import")
        catalog.activate_generation(db, later)
        assert [row["path"] for row in catalog.files(db, catalog.shard_spec(generation_id=old))] == paths
    finally:
        db.disconnect()


def test_going_back_deletes_nothing(tmp_path):
    db, old = _library(tmp_path)
    try:
        _cut(db)
        new = _repacked(db, tmp_path)
        files = {row["path"] for g in (old, new) for row in catalog.files(db, catalog.shard_spec(generation_id=g))}
        repack.go_back(db, old)
        assert catalog.active_generation_id(db) == old
        assert files == {row["path"] for g in (old, new) for row in catalog.files(db, catalog.shard_spec(generation_id=g))}
        assert all(__import__("os").path.isfile(p) for p in files)
    finally:
        db.disconnect()


def test_undo_works_after_going_back(tmp_path):
    """The repack left the cut records out; going back to the one before, undoing the cut brings
    them all back."""
    db, old = _library(tmp_path)
    try:
        cut = _cut(db)
        _repacked(db, tmp_path)
        repack.go_back(db, old)
        exclusions.set_exclusion_active(db, cut, False)
        assert len(_records(db)) == SHARDS * SLOTS
    finally:
        db.disconnect()


def test_delete_is_refused_while_named(tmp_path):
    db, old = _library(tmp_path)
    try:
        _cut(db)
        new = _repacked(db, tmp_path)
        with pytest.raises(ValueError, match="active"):
            repack.delete(db, new)
        states.mark_done(db, "docked", "k", [1], generation_id=old)
        with pytest.raises(ValueError, match="state"):
            repack.delete(db, old)
        states.forget(db, "docked", "k")
        paths = [row["path"] for row in catalog.files(db, catalog.shard_spec(generation_id=old))]
        repack.delete(db, old)
        assert not list(catalog.files(db, catalog.shard_spec(generation_id=old)))
        assert not any(__import__("os").path.exists(p) for p in paths)
        assert _records(db)  # the active one is untouched
    finally:
        db.disconnect()
