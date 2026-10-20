"""Payload groups: several payloads in one needle, each with its own id."""
from __future__ import annotations

import pickle
import sqlite3
from contextlib import closing

import pytest

from ms_flow import payloads as P
from ms_flow.core.database import ProjectStore
from ms_flow.engine import Engine, Flow, ThreadExecutor, job, task
from ms_flow.payloads import Payload, PayloadGroup, PayloadStore
from ms_flow.sinks import graph_sink


@pytest.fixture
def project(tmp_path, monkeypatch):
    monkeypatch.setattr(P, "VOLUME_SIZE", 4096)  # a few volumes from a few groups
    store = ProjectStore.open_at(tmp_path / "project.db")
    with closing(sqlite3.connect(store.db_path)) as conn, conn:  # the app's schema
        conn.execute("CREATE TABLE app_items (id INTEGER PRIMARY KEY, key TEXT,"
                     " payload_id INTEGER REFERENCES ms_payloads(id))")
    yield store
    store.dispose()


def pose(key: str, rank: int) -> bytes:
    return (f"{key} pose {rank}\n" * 20).encode()


def rows(project, sql, params=()):
    with project.engine.connect() as c:
        return c.exec_driver_sql(sql, params).fetchall()


def in_sealed(project) -> int:
    """Rows a compaction would move: those not in the active volume."""
    return rows(project, "SELECT COUNT(*) FROM ms_payloads p JOIN ms_volumes v ON v.id = p.volume_id"
                         " WHERE v.state = 'sealed'")[0][0]


def needles(project) -> int:
    return rows(project, 'SELECT COUNT(*) FROM (SELECT DISTINCT volume_id, "offset" FROM ms_payloads)')[0][0]


def add(store, project, keys, parts=3):
    """One group per key, its parts referenced by app rows `<key>/<rank>`."""
    groups = []
    for key in keys:
        group = PayloadGroup(kind="sdf", series="poses", codec="none")  # sizes that fill volumes
        for rank in range(parts):
            group.add(pose(key, rank))
        groups.append(pickle.loads(pickle.dumps(group)))  # as it comes back from a worker: compressed
    with project.get_session() as session:
        ids = store.put(session, groups)
        conn = session.connection()
        names = [f"{key}/{rank}" for key in keys for rank in range(parts)]
        for name, pid in zip(names, ids):
            conn.exec_driver_sql("INSERT INTO app_items (key, payload_id) VALUES (?, ?)", (name, pid))
        session.commit()
    return dict(zip(names, ids))


def drop(project, names):
    with project.engine.begin() as conn:
        return [r[0] for r in conn.exec_driver_sql(
            f"DELETE FROM app_items WHERE key IN ({','.join('?' * len(names))}) RETURNING payload_id", tuple(names))]


def same_bytes(store, project):
    found = dict(rows(project, "SELECT key, payload_id FROM app_items"))
    got = store.get_many(found.values())
    assert {name: got[pid] for name, pid in found.items()} == {
        name: pose(name.split("/")[0], int(name.split("/")[1])) for name in found}


def test_a_group_is_one_needle_and_one_id_per_part_mixed_with_payloads(project):
    store = PayloadStore(project)
    group = PayloadGroup(kind="sdf", series="poses")
    parts = [group.add(pose("a", rank)) for rank in range(4)]
    assert parts[2].data == pose("a", 2) and len(group) == 4
    with project.get_session() as session:
        ids = store.put(session, [Payload(b"alone", series="poses"), group, PayloadGroup(series="poses")])
        session.commit()
    assert len(ids) == 5 and needles(project) == 2  # the empty group stores nothing
    assert store.get(ids[0]) == b"alone"
    assert [store.get(pid) for pid in ids[1:]] == [pose("a", rank) for rank in range(4)]
    assert store.get_many(ids[1:]) == {pid: pose("a", rank) for rank, pid in enumerate(ids[1:])}
    assert rows(project, "SELECT DISTINCT kind FROM ms_payloads WHERE part_offset IS NOT NULL") == [("sdf",)]
    assert store.check(full=True).ok
    store.close()


def test_a_pickled_group_compresses_once_for_all_its_parts(project):
    group = PayloadGroup(series="poses")
    parts = [group.add(pose("b", rank)) for rank in range(9)]
    back = pickle.loads(pickle.dumps(parts))
    assert all(part.group is back[0].group for part in back)  # one group, not nine
    payload, sizes = back[0].group.packed()
    assert payload.raw_length == sum(sizes) and len(payload.data) < sum(sizes)
    with pytest.raises(ValueError):
        back[0].group.add(b"late")


@task()
def docked(n: int) -> dict:
    group = PayloadGroup(kind="sdf", series="poses")
    return {"ligands": {"$ref": "l", "n": n},
            "poses": [{"ligand_ref": "l", "rank": rank, "pose_payload_id": group.add(pose(str(n), rank))}
                      for rank in range(3)]}


@job(task=docked, output=graph_sink(
    nodes=({"name": "ligands", "table": "ligands", "columns": ("n",)},
           {"name": "poses", "table": "poses", "columns": ("ligand_id", "rank", "pose_payload_id")}),
    relations=({"source_node": "poses", "source_ref_field": "ligand_ref", "target_node": "ligands",
                "fk_field": "ligand_id"},)))
def docking(params, project):
    yield from range(params["n"])


def test_a_sink_stores_a_group_once_and_each_row_gets_its_part(project):
    with closing(sqlite3.connect(project.db_path)) as conn, conn:
        conn.execute("CREATE TABLE ligands (id INTEGER PRIMARY KEY, n INT NOT NULL)")
        conn.execute("CREATE TABLE poses (id INTEGER PRIMARY KEY, ligand_id INT REFERENCES ligands(id),"
                     " rank INT, pose_payload_id INTEGER REFERENCES ms_payloads(id))")
    with Engine(cpus=2, mem_mb=0) as engine:
        engine.register("thread", ThreadExecutor(2))
        ended = engine.wait(Flow(engine, project).submit(docking, {"n": 10}, executor="thread"), 60)
        assert ended.status == "completed", ended.error
    found = rows(project, "SELECT l.n, p.rank, p.pose_payload_id FROM poses p JOIN ligands l ON l.id = p.ligand_id")
    assert len(found) == 30 and len({pid for *_, pid in found}) == 30 and needles(project) == 10
    got = project.payloads.get_many([pid for *_, pid in found])
    assert all(got[pid] == pose(str(n), rank) for n, rank, pid in found)


def test_a_needle_lives_while_one_part_does_and_compacts_once(project):
    store = PayloadStore(project)
    add(store, project, [f"k{i}" for i in range(60)])
    sealed = [r[0] for r in rows(project, "SELECT id FROM ms_volumes WHERE state = 'sealed'")]
    assert sealed and set(store.check().dead.values()) == {0.0}  # parts share their needle: counted once
    store.release(drop(project, [f"k{i}/{rank}" for i in range(60) for rank in (1, 2)]))
    assert set(store.check().dead.values()) == {0.0}  # every needle still has its first part
    store.release(drop(project, [f"k{i}/0" for i in range(60) if i % 3]))
    assert min(store.check().dead.values()) >= 0.5
    moving = in_sealed(project)
    stats = store.compact()
    assert stats["volumes"] and stats["moved"] == moving
    same_bytes(store, project)
    assert store.check(full=True).clean and store.compact()["volumes"] == 0
    store.close()


def test_compact_moves_a_group_once_and_every_part_follows(project):
    store = PayloadStore(project)
    add(store, project, [f"k{i}" for i in range(60)])
    store.release(drop(project, [f"k{i}/{rank}" for i in range(60) if i % 3 for rank in range(3)]))
    before, moving = needles(project), in_sealed(project)
    stats = store.compact()
    assert stats["moved"] == moving and moving % 3 == 0 and needles(project) == before
    same_bytes(store, project)
    assert store.check(full=True).clean
    store.close()


def test_a_project_from_before_groups_gets_the_columns_and_reads_as_before(tmp_path):
    project = ProjectStore.open_at(tmp_path / "project.db")
    with project.get_session() as session:
        (old,) = project.payloads.put(session, [Payload(b"old bytes", series="s")])
        session.commit()
    project.dispose()
    with closing(sqlite3.connect(tmp_path / "project.db")) as conn, conn:
        conn.execute("ALTER TABLE ms_payloads DROP COLUMN part_offset")
        conn.execute("ALTER TABLE ms_payloads DROP COLUMN part_length")
    project = ProjectStore.open_at(tmp_path / "project.db")
    store = PayloadStore(project)
    assert store.get(old) == b"old bytes"
    group = PayloadGroup(series="s")
    group.add(b"new")
    with project.get_session() as session:
        (new,) = store.put(session, [group])
        session.commit()
    assert store.get(new) == b"new"
    store.close()
    project.dispose()
