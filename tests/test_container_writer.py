"""M4b: the one writer of a container, fed receipts of parts: group commit, idempotent by part id,
bounded containers that seal and roll (I1, I9-I12, I18)."""
import json
import os
import subprocess
import sys
import textwrap
import zlib

import pytest

from ms_flow.core.data import catalog, container
from ms_flow.core.data.shard import PayloadKind, Serializer, ShardWriter
from ms_flow.core.database import ProjectStore
from ms_flow.engine.journal import Busy


def _project(tmp_path):
    db = ProjectStore()
    db.connect(tmp_path / "project")
    return db


def _part(path, *bases):
    """A task's part: one shard of 4 records per base."""
    with ShardWriter(path, dataset_id=None, shard_id=0, base_id=min(bases), kind=PayloadKind.GENERIC_BYTES,
                     serializer=Serializer.UTF8, slot_count=1 << 20) as writer:
        for base in bases:
            for id_ in range(base, base + 4):
                writer.add(id_, f"r{id_}".encode())
            writer.end_group(scope=1, base_id=base, slot_count=4)
    return path


def _receipt(path, part):
    return {"part": part, "path": str(path)}


def _setup(tmp_path):
    db = _project(tmp_path)
    gen = catalog.create_generation(db, step="import")
    catalog.activate_generation(db, gen)
    return db, gen, tmp_path / "out", tmp_path / "parts"


def _bases(db):
    """Base ids of every live shard the catalog reads, from the indexes, repeats kept."""
    found = []
    for row in catalog.files(db):
        _info, rows = container.read_index(row["path"])
        found += [r["base_id"] for r in rows if not r["retired"]]
    return sorted(found)


def _containers(db):
    return [(row["status"], row["n_shards"]) for row in catalog.files(db)]


def test_a_replayed_group_commits_once(tmp_path):
    db, gen, out, parts = _setup(tmp_path)
    try:
        group = [_receipt(_part(parts / f"{n}.arrows", 4 * n), f"s:{n}") for n in range(3)]
        with catalog.ContainerWriter(db, out, generation_id=gen, prefix="s:") as writer:
            first = writer.commit_parts(group)
            assert [len(r["shards"]) for r in first] == [1, 1, 1]
            assert writer.commit_parts(group) == first  # the same writer
        with catalog.ContainerWriter(db, out, generation_id=gen, prefix="s:") as writer:
            assert writer.commit_parts(group) == first  # a new one reads the part ids from the index
        assert _bases(db) == [0, 4, 8]
        assert not any(parts.iterdir())  # committed parts are deleted
    finally:
        db.disconnect()


def test_a_receipt_whose_part_is_gone_is_accepted_if_committed(tmp_path):
    db, gen, out, parts = _setup(tmp_path)
    try:
        done = _receipt(_part(parts / "a.arrows", 0), "s:a")
        with catalog.ContainerWriter(db, out, generation_id=gen, prefix="s:") as writer:
            (first,) = writer.commit_parts([done])
        assert not (parts / "a.arrows").exists()
        lost = _receipt(parts / "never.arrows", "s:never")
        with catalog.ContainerWriter(db, out, generation_id=gen, prefix="s:") as writer:
            again, failed = writer.commit_parts([done, lost])
        assert again["shards"] == first["shards"] and "error" not in again
        assert "shards" not in failed and "never.arrows" in failed["error"]
        assert _bases(db) == [0]
    finally:
        db.disconnect()


def test_two_attempts_first_receipt_wins(tmp_path):
    db, gen, out, parts = _setup(tmp_path)
    try:
        one = _receipt(_part(parts / "one.arrows", 0), "s:x")
        two = _receipt(_part(parts / "two.arrows", 100), "s:x")  # the same work, done twice
        with catalog.ContainerWriter(db, out, generation_id=gen, prefix="s:") as writer:
            a, b = writer.commit_parts([one, two])
            assert a["shards"] == b["shards"]
            late = _receipt(_part(parts / "three.arrows", 200), "s:x")
            assert writer.commit_parts([late])[0]["shards"] == a["shards"]
        assert _bases(db) == [0]
        assert not any(parts.iterdir())  # the losers' parts go too
    finally:
        db.disconnect()


def test_two_writers_one_is_refused(tmp_path):
    db, gen, out, parts = _setup(tmp_path)
    try:
        first = catalog.ContainerWriter(db, out, generation_id=gen)
        first.commit_parts([_receipt(_part(parts / "a.arrows", 0), "s:a")])
        second = catalog.ContainerWriter(db, out, generation_id=gen)
        with pytest.raises(Busy, match=f"pid {os.getpid()}"):
            second.commit_parts([_receipt(_part(parts / "b.arrows", 4), "s:b")])
        first.close()
        second.commit_parts([_receipt(parts / "b.arrows", "s:b")])
        second.close()
        assert _bases(db) == [0, 4]
    finally:
        db.disconnect()


def test_a_part_never_spans_two_containers(tmp_path, monkeypatch):
    monkeypatch.setattr(catalog, "MAX_SHARDS", 4)
    db, gen, out, parts = _setup(tmp_path)
    try:
        group = [_receipt(_part(parts / "a.arrows", 0, 4, 8), "s:a"),
                 _receipt(_part(parts / "b.arrows", 12, 16, 20), "s:b"),
                 _receipt(_part(parts / "big.arrows", *range(24, 44, 4)), "s:big"),  # over the limit
                 _receipt(_part(parts / "c.arrows", 44), "s:c")]
        with catalog.ContainerWriter(db, out, generation_id=gen) as writer:
            done = writer.commit_parts(group)
        for receipt in done:
            assert len({catalog.split_shard_id(s)[0] for s in receipt["shards"]}) == 1
        assert _containers(db) == [(catalog.SEALED, 3), (catalog.SEALED, 3), (catalog.SEALED, 5),
                                   (catalog.READY, 1)]
    finally:
        db.disconnect()


def test_a_full_container_seals_and_rolls(tmp_path, monkeypatch):
    monkeypatch.setattr(catalog, "MAX_SHARDS", 2)
    db, gen, out, parts = _setup(tmp_path)
    try:
        with catalog.ContainerWriter(db, out, generation_id=gen) as writer:
            writer.commit_parts([_receipt(_part(parts / f"{n}.arrows", 4 * n), f"s:{n}") for n in range(5)])
        assert _containers(db) == [(catalog.SEALED, 2), (catalog.SEALED, 2), (catalog.READY, 1)]
        ids = [row["id"] for row in catalog.files(db)]
        assert ids == sorted(ids)  # newest = largest shard id
        assert _bases(db) == [0, 4, 8, 12, 16]
    finally:
        db.disconnect()


def test_a_full_container_by_bytes_rolls(tmp_path, monkeypatch):
    db, gen, out, parts = _setup(tmp_path)
    try:
        with catalog.ContainerWriter(db, out, generation_id=gen) as writer:
            writer.commit_parts([_receipt(_part(parts / "0.arrows", 0), "s:0")])
            monkeypatch.setattr(catalog, "MAX_BYTES", next(catalog.files(db))["end"] + 1)
            writer.commit_parts([_receipt(_part(parts / "1.arrows", 4), "s:1")])
        assert _containers(db) == [(catalog.SEALED, 1), (catalog.READY, 1)]
    finally:
        db.disconnect()


def test_appends_go_only_to_the_newest_container(tmp_path):
    db, gen, out, parts = _setup(tmp_path)
    try:
        older = catalog.add_file(db, _part(out / "older.arrows", 0), generation_id=gen)
        newer = catalog.add_file(db, _part(out / "newer.arrows", 4), generation_id=gen)
        before = (out / "older.arrows").read_bytes()
        with catalog.ContainerWriter(db, out, generation_id=gen) as writer:
            (done,) = writer.commit_parts([_receipt(_part(parts / "a.arrows", 8), "s:a")])
        assert catalog.split_shard_id(done["shards"][0])[0] == newer
        assert (out / "older.arrows").read_bytes() == before
        catalog.seal(db, newer)
        with catalog.ContainerWriter(db, out, generation_id=gen) as writer:
            (done,) = writer.commit_parts([_receipt(_part(parts / "b.arrows", 12), "s:b")])
        assert catalog.split_shard_id(done["shards"][0])[0] > newer > older
        assert _bases(db) == [0, 4, 8, 12]
    finally:
        db.disconnect()


def test_the_parent_never_decodes_a_part(tmp_path):
    """A part whose record bytes are garbage under a right checksum: only a decode would see it."""
    db, gen, out, parts = _setup(tmp_path)
    try:
        path = _part(parts / "a.arrows", 0)
        info, rows = container.read_index(path)
        (row,) = rows
        junk = os.urandom(row["length"])
        with open(path, "r+b") as file:
            file.seek(row["offset"])
            file.write(junk)
        container._save_index(path, info, [{**row, "crc": zlib.crc32(junk)}])
        with catalog.ContainerWriter(db, out, generation_id=gen) as writer:
            (done,) = writer.commit_parts([_receipt(path, "s:a")])
        assert len(done["shards"]) == 1 and "error" not in done
    finally:
        db.disconnect()


def test_a_damaged_part_fails_its_receipt_only(tmp_path):
    db, gen, out, parts = _setup(tmp_path)
    try:
        good = _receipt(_part(parts / "good.arrows", 0), "s:good")
        bad = _part(parts / "bad.arrows", 4)
        info, (row,) = container.read_index(bad)
        container._save_index(bad, info, [{**row, "crc": row["crc"] ^ 1}])  # found only while copying
        with catalog.ContainerWriter(db, out, generation_id=gen) as writer:
            ok, failed = writer.commit_parts([good, _receipt(bad, "s:bad")])
            assert ok["shards"] and "checksum" in failed["error"]
            assert bad.exists()  # kept: a failed part is the engine's to retry, not ours to delete
            later = writer.commit_parts([_receipt(_part(parts / "later.arrows", 8), "s:later")])
        assert later[0]["shards"] and _bases(db) == [0, 8]
    finally:
        db.disconnect()


def test_a_crash_between_index_and_states_is_repaired_by_the_retry(tmp_path, monkeypatch):
    db, gen, out, parts = _setup(tmp_path)
    try:
        group = [_receipt(_part(parts / f"{n}.arrows", 4 * n), f"s:{n}") for n in range(2)]
        seen = []

        def crash(step):
            if step == 7:
                raise KeyboardInterrupt  # dies after the index commit, before counts and states

        monkeypatch.setattr(catalog, "_step", crash)
        with catalog.ContainerWriter(db, out, generation_id=gen, on_commit=seen.extend) as writer:
            with pytest.raises(KeyboardInterrupt):
                writer.commit_parts(group)
        assert _bases(db) == [0, 4] and next(catalog.files(db))["n_shards"] == 0 and not seen
        monkeypatch.setattr(catalog, "_step", lambda step: None)
        with catalog.ContainerWriter(db, out, generation_id=gen, on_commit=seen.extend) as writer:
            done = writer.commit_parts(group)
        assert next(catalog.files(db))["n_shards"] == 2
        assert [r["part"] for r in seen] == ["s:0", "s:1"] and seen == done
        assert not any(parts.iterdir())
    finally:
        db.disconnect()


_CHILD = textwrap.dedent("""
    import json, os, sys
    from ms_flow.core.data import catalog
    from ms_flow.core.database import ProjectStore

    where, gen, out, kill, receipts = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4], json.loads(sys.argv[5])
    catalog.MAX_SHARDS = 2
    catalog._step = lambda step: os._exit(9) if str(step) == kill else None
    db = ProjectStore()
    db.connect(where)
    with catalog.ContainerWriter(db, out, generation_id=gen, prefix="s:") as writer:
        writer.commit_parts(receipts)
    os._exit(0)
""")


@pytest.mark.parametrize("kill", ["1", "2", "3", "4", "5", "5.5", "6", "6.5", "7", "8", "9"])
def test_a_crash_at_each_step_loses_and_doubles_nothing(tmp_path, monkeypatch, kill):
    """A real kill at the start of each step of `commit_parts` (5.5: between creating a container
    and its catalog row; 6.5: data written, index not replaced; 9: parts deleted, no return)."""
    monkeypatch.setattr(catalog, "MAX_SHARDS", 2)
    db, gen, out, parts = _setup(tmp_path)
    try:
        catalog.add_file(db, _part(out / "first.arrows", 100), generation_id=gen)  # room for one
        group = [_receipt(_part(parts / f"{n}.arrows", 4 * n), f"s:{n}") for n in range(3)]
        home = tmp_path / "home"
        home.mkdir()
        env = {**os.environ, "HOME": str(home), "PYTHONPATH": os.pathsep.join(sys.path)}
        child = subprocess.run([sys.executable, "-c", _CHILD, str(tmp_path / "project"), str(gen), str(out),
                                kill, json.dumps(group)], env=env, capture_output=True, text=True, timeout=120)
        assert child.returncode == 9, child.stderr
        with catalog.ContainerWriter(db, out, generation_id=gen, prefix="s:") as writer:
            done = writer.commit_parts(group)
        assert all(r.get("shards") for r in done), done
        assert _bases(db) == [0, 4, 8, 100]
        assert sum(row["n_shards"] for row in catalog.files(db)) == 4  # the counts match the indexes
        assert not any(parts.iterdir())
    finally:
        db.disconnect()
