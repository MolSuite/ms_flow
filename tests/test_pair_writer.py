"""M6a: one task, two containers. The bulk is committed first, the index second, and the index
commit is what makes a part visible; its shard names the bulk shards it goes with."""
import json

import pyarrow as pa
import pytest

from ms_flow.core.data import catalog
from ms_flow.core.data.shard import file_groups, read_rows, write_table
from ms_flow.core.database import ProjectStore

BULK, INDEX = "bulk:k", "index:k"


def _setup(tmp_path):
    db = ProjectStore()
    db.connect(tmp_path / "project")
    gen = catalog.create_generation(db, step="import")
    catalog.activate_generation(db, gen)
    return db, gen, tmp_path / "out", tmp_path / "parts"


def _receipt(parts, part, attempt):
    """A task's two part containers: values in the index, a blob per value in the bulk."""
    name = f"{part.replace(':', '_')}-{attempt}"
    index = write_table(parts / f"{name}.index.arrows", pa.table({"id": [0, 1], "value": [attempt, attempt]}),
                        part=part, meta={"run": "r"}, origin=7)
    bulk = write_table(parts / f"{name}.bulk.arrows", pa.table({"id": [0, 1], "blob": [f"blob-{attempt}"] * 2}),
                       part=part)
    return {"part": part, "path": str(index), "bulk": str(bulk)}


def _live(db, role):
    """(shard id, group) of every live shard of `role`, oldest first."""
    found = []
    for row in catalog.files(db, catalog.shard_spec(role=role)):
        found += [(catalog.shard_id(row["id"], g.shard), g) for g in file_groups(row["path"]) if not g.retired]
    return found


def _bulk_of(db, group):
    """The bulk values the index shard `group` names, read where its link points."""
    values = []
    for shard in json.loads(group.meta)["bulk"]:
        file_id, number = catalog.split_shard_id(shard)
        values += read_rows(catalog.get_shard(db, shard)["path"], [number])["blob"].to_pylist()
    return values


def _writer(db, out, gen):
    return catalog.PairWriter(db, out, generation_id=gen, roles=(BULK, INDEX), prefix="t:")


def test_a_crash_between_payload_and_summary_shows_nothing_then_one_result(tmp_path, monkeypatch):
    db, gen, out, parts = _setup(tmp_path)
    try:
        def crash(step):
            if step == "between":
                raise KeyboardInterrupt

        monkeypatch.setattr(catalog, "_step", crash)
        with pytest.raises(KeyboardInterrupt), _writer(db, out, gen) as writer:
            writer.commit_parts([_receipt(parts, "t:1", 1)])
        assert _live(db, INDEX) == [] and len(_live(db, BULK)) == 1  # the bulk alone: nothing to read
        monkeypatch.undo()
        with _writer(db, out, gen) as writer:  # the task ran again: a new attempt of the same part
            (receipt,) = writer.commit_parts([_receipt(parts, "t:1", 2)])
        assert "error" not in receipt
        ((_id, index),) = _live(db, INDEX)
        assert index.part == "t:1" and len(_live(db, BULK)) == 2  # the orphan stays, never named
        assert read_rows(next(catalog.files(db, catalog.shard_spec(role=INDEX)))["path"])["value"].to_pylist() == [2, 2]
    finally:
        db.disconnect()


def test_scores_and_blobs_come_from_one_attempt(tmp_path, monkeypatch):
    db, gen, out, parts = _setup(tmp_path)
    try:
        monkeypatch.setattr(catalog, "_step", lambda step: (_ for _ in ()).throw(KeyboardInterrupt)
                            if step == "between" else None)
        with pytest.raises(KeyboardInterrupt), _writer(db, out, gen) as writer:
            writer.commit_parts([_receipt(parts, "t:1", 1)])
        monkeypatch.undo()
        with _writer(db, out, gen) as writer:
            first = writer.commit_parts([_receipt(parts, "t:1", 2), _receipt(parts, "t:2", 3)])
            replay = writer.commit_parts([_receipt(parts, "t:2", 4)])  # its index has it: both skipped
        assert [r["shards"] for r in replay] == [first[1]["shards"]]
        live = dict(_live(db, INDEX))
        assert [(g.part, json.loads(g.meta)["run"], _bulk_of(db, g)) for g in live.values()] == [
            ("t:1", "r", ["blob-2", "blob-2"]), ("t:2", "r", ["blob-3", "blob-3"])]
        assert len(_live(db, BULK)) == 3  # attempt 1's orphan, then 2 and 3; never 4
        # skipped or committed, no part is left behind; the crashed attempt's index part is the
        # task's leftover, as any part of a task that died before its commit
        assert sorted(p.name for p in parts.iterdir()) == ["t_1-1.index.arrows", "t_1-1.index.arrows.index"]
    finally:
        db.disconnect()
