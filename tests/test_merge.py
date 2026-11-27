"""M12a: small containers of a role merged into few, their live shards copied as bytes; a container
whose shard ids something names is left as it is; a kill leaves one readable copy."""
import json
import os
import subprocess
import sys
import textwrap

import pytest

from ms_flow.core.data import catalog, container, states
from ms_flow.core.database import ProjectStore
from test_container_writer import _part, _receipt


def _setup(tmp_path, n=3):
    """`n` sealed containers of role "made" in one folder, a shard of 4 records in each but the
    first, which has two: one of them retired."""
    db = ProjectStore()
    db.connect(tmp_path / "project")
    gen = catalog.create_generation(db, step="import")
    catalog.activate_generation(db, gen)
    out, parts = tmp_path / "out", tmp_path / "parts"
    with catalog.ContainerWriter(db, out, generation_id=gen, role="made", prefix="s:") as writer:
        first = writer.commit_parts([_receipt(_part(parts / "0.arrows", 0, 4), "s:0")])
        writer.retire(first[0]["shards"][:1])
        writer.seal()
        for i in range(1, n):
            writer.commit_parts([_receipt(_part(parts / f"{i}.arrows", 8 * i), f"s:{i}")])
            writer.seal()
    return db, gen, out


def _live(db, role="made"):
    """(base id, crc) of every live shard the catalog reads, repeats kept."""
    found = []
    for row in catalog.files(db, catalog.shard_spec(role=role)):
        if row["status"] in catalog.READABLE:
            found += [(r["base_id"], r["crc"]) for r in container.read_index(row["path"])[1] if not r["retired"]]
    return sorted(found)


def test_a_merge_decodes_nothing(tmp_path, monkeypatch):
    db, gen, out = _setup(tmp_path)
    try:
        before = _live(db)
        assert len(before) == 3 and len(list(catalog.files(db, catalog.shard_spec(role="made")))) == 3

        def decoded(*_a, **_k):
            raise AssertionError("a merge decoded records")

        for name in ("read_table", "iter_tables", "encode"):
            monkeypatch.setattr(container, name, decoded)
        assert catalog.merge(db, gen, "made") == 3
        rows = list(catalog.files(db, catalog.shard_spec(role="made")))
        assert len(rows) == 1 and rows[0]["n_retired"] == 0  # the retired shard's bytes are gone
        assert _live(db) == before
        assert sorted(p.name for p in out.iterdir() if p.suffix != ".lock") == sorted(
            [os.path.basename(rows[0]["path"]), os.path.basename(rows[0]["path"]) + ".index"])
        assert catalog.merge(db, gen, "made") == 0  # one container already
    finally:
        db.disconnect()


def test_a_referenced_container_is_not_merged(tmp_path):
    db, gen, out = _setup(tmp_path, n=4)
    try:
        files = sorted(catalog.files(db, catalog.shard_spec(role="made")), key=lambda r: r["id"])
        shard_of = {row["id"]: catalog.shard_id(row["id"], next(
            r["shard"] for r in container.read_index(row["path"])[1] if not r["retired"])) for row in files}
        # what another container derives from the second one names it by its shard id (an origin)
        with catalog.ContainerWriter(db, tmp_path / "derived", generation_id=gen, role="derived", prefix="d:") as w:
            w.commit_parts([_receipt(_part_of(tmp_path / "d.arrows", origin=shard_of[files[1]["id"]]), "d:0")])
        # and a state names the third one's
        states.mark_done(db, "docked", "k", [shard_of[files[2]["id"]]], generation_id=gen)
        assert catalog.merge(db, gen, "made") == 2  # the first and the last, the others as they were
        kept = {row["id"] for row in catalog.files(db, catalog.shard_spec(role="made"))}
        assert {files[1]["id"], files[2]["id"]} <= kept and len(kept) == 3
    finally:
        db.disconnect()


def _part_of(path, *, origin):
    """A part of one shard derived from the shard `origin`."""
    from ms_flow.core.data.shard import PartWriter, PayloadKind

    with PartWriter(path, kind=PayloadKind.GENERIC_BYTES, origin=origin) as writer:
        writer.add(1, b"x")
    return path


_CHILD = textwrap.dedent("""
    import os, sys
    from ms_flow.core.data import catalog
    from ms_flow.core.database import ProjectStore
    db = ProjectStore(); db.connect(sys.argv[1])
    kill = sys.argv[3]
    catalog._step = lambda step: os._exit(9) if str(step) == kill else None
    catalog.merge(db, int(sys.argv[2]), "made")
""")


@pytest.mark.parametrize("kill", ["merge:written", "merge:swapped"])
def test_a_kill_during_a_merge_leaves_one_copy_readable(tmp_path, kill):
    """Killed with the new container written and no row yet, or with the rows swapped and the old
    files still there: every live shard reads once, and `sweep` leaves only what the catalog names."""
    db, gen, out = _setup(tmp_path)
    try:
        before = _live(db)
        env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
        child = subprocess.run([sys.executable, "-c", _CHILD, str(tmp_path / "project"), str(gen), kill],
                               env=env, capture_output=True, text=True, timeout=120)
        assert child.returncode == 9, child.stderr
        assert _live(db) == before
        catalog.sweep(db, out)
        named = {os.path.basename(r["path"]) for r in catalog.files(db, catalog.shard_spec(role="made"))}
        assert {p.name.removesuffix(".index") for p in out.iterdir() if p.suffix != ".lock"} == named
        assert catalog.merge(db, gen, "made") in (0, 3)  # what was left undone is done now
        assert _live(db) == before
    finally:
        db.disconnect()
