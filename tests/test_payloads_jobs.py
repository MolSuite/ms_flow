"""Payloads written from jobs: a task returns `Payload`s in its rows and the sink stores them (V2)."""
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

import pytest

from benchmarks.refactor_optimization import workers
from ms_flow.core.database.project import ProjectStore
from ms_flow.engine import Engine, Flow, ProcessExecutor, ThreadExecutor, job, task
from ms_flow.payloads import Payload
from ms_flow.sinks import graph_sink, table_sink


def structure(n: int) -> bytes:
    return f"molecule {n}\n".encode() * (n % 7 + 1)


@pytest.fixture
def project(tmp_path):
    store = ProjectStore.open_at(tmp_path / "project.db")
    with closing(sqlite3.connect(store.db_path)) as conn, conn:  # the app's schema: ms_flow never sees it
        conn.execute("CREATE TABLE ligands (id INTEGER PRIMARY KEY, n INT NOT NULL,"
                     " structure_payload_id INTEGER REFERENCES ms_payloads(id))")
        conn.execute("CREATE TABLE poses (id INTEGER PRIMARY KEY, ligand_id INT REFERENCES ligands(id),"
                     " rank INT, pose_payload_id INTEGER REFERENCES ms_payloads(id))")
    yield store
    store.dispose()


def rows(project, sql):
    with closing(sqlite3.connect(project.db_path)) as conn:
        return conn.execute(sql).fetchall()


def run(project, job, params=None, status="completed"):
    with Engine(cpus=4, mem_mb=0) as engine:
        engine.register("thread", ThreadExecutor(4))
        job_id = Flow(engine, project).submit(job, params, executor="thread")
        ended = engine.wait(job_id, 60)
        assert ended.status == status, ended.error
        return ended


def assert_clean(project, referenced):
    """Every payload is referenced once, and every volume ends where its last payload does: nothing half."""
    assert rows(project, "SELECT COUNT(*) FROM ms_payloads") == [(referenced,)]
    ends = rows(project, f"SELECT v.id, COALESCE(MAX(p.\"offset\" + 16 + p.length), 32) FROM ms_volumes v"
                         " LEFT JOIN ms_payloads p ON p.volume_id = v.id GROUP BY v.id")
    assert {project.payloads.path(vid).stat().st_size for vid, _ in ends} and all(
        project.payloads.path(vid).stat().st_size == end for vid, end in ends)
    assert sorted(path.name for path in project.payloads.directory.iterdir()) == sorted(
        project.payloads.path(vid).name for vid, _ in ends)


@task()
def ligand(n: int) -> dict:
    return {"n": n, "structure_payload_id": Payload(structure(n), kind="sdf", series="ligands")}


@job(task=ligand, output=table_sink("ligands", columns=("n", "structure_payload_id")))
def ligands(params, project):
    yield from range(params["n"])


@task()
def docked(n: int) -> dict:
    return {"ligands": {"$ref": "l", "n": n, "structure_payload_id": Payload(structure(n), series="ligands")},
            "poses": [{"ligand_ref": "l", "rank": rank, "pose_payload_id": Payload(structure(n + rank), series="poses")}
                      for rank in range(2)]}


@job(task=docked, output=graph_sink(
    nodes=({"name": "ligands", "table": "ligands", "columns": ("n", "structure_payload_id")},
           {"name": "poses", "table": "poses", "columns": ("ligand_id", "rank", "pose_payload_id")}),
    relations=({"source_node": "poses", "source_ref_field": "ligand_ref", "target_node": "ligands",
                "fk_field": "ligand_id"},)))
def docking(params, project):
    yield from range(params["n"])


def test_a_table_sink_stores_the_payloads_of_its_rows_and_keeps_their_ids(project):
    run(project, ligands, {"n": 40})
    stored = rows(project, "SELECT n, structure_payload_id FROM ligands")
    assert len(stored) == 40 and len({pid for _, pid in stored}) == 40
    assert rows(project, "SELECT COUNT(*), MIN(kind), MAX(kind) FROM ms_payloads") == [(40, "sdf", "sdf")]
    got = project.payloads.get_many([pid for _, pid in stored])
    assert all(got[pid] == structure(n) for n, pid in stored)


def test_a_graph_sink_stores_the_payloads_of_every_node(project):
    run(project, docking, {"n": 20})
    poses = rows(project, "SELECT l.n, p.rank, l.structure_payload_id, p.pose_payload_id"
                          " FROM poses p JOIN ligands l ON l.id = p.ligand_id")
    assert len(poses) == 40 and rows(project, "SELECT COUNT(*) FROM ms_payloads") == [(60,)]
    for n, rank, ligand_pid, pose_pid in poses:
        assert project.payloads.get(ligand_pid) == structure(n)
        assert project.payloads.get(pose_pid) == structure(n + rank)
    series = rows(project, "SELECT v.series, COUNT(*) FROM ms_payloads p JOIN ms_volumes v ON v.id = p.volume_id"
                           " GROUP BY v.series ORDER BY v.series")
    assert series == [("ligands", 20), ("poses", 40)]


@job(task=task()(workers.structure), output=table_sink("ligands", columns=("n", "structure_payload_id")))
def anywhere(params, project):
    yield from range(params["n"])


def _executor(name, tmp_path):
    if name == "process":
        return ProcessExecutor(2)
    if name == "thread":
        return ThreadExecutor(2)
    if name == "ray":
        pytest.importorskip("ray")
        from ms_flow.engine import RayExecutor
        return RayExecutor(num_cpus=2, shared=False)
    from ms_flow.remote import RemoteExecutor
    root = Path(__file__).resolve().parents[1]
    return RemoteExecutor(workdir=str(tmp_path / "far"), python=sys.executable, shared=False,
                          setup=f"export PYTHONPATH={root}:{root / 'src'}", poll_s=0.2, part_seconds=0.1)


@pytest.mark.slow
def test_every_executor_leaves_the_same_rows_and_bytes(tmp_path):
    seen = {}
    for name in ("process", "thread", "ray", "remote"):
        project = ProjectStore.open_at(tmp_path / name / "project.db")
        with closing(sqlite3.connect(project.db_path)) as conn, conn:
            conn.execute("CREATE TABLE ligands (id INTEGER PRIMARY KEY, n INT NOT NULL,"
                         " structure_payload_id INTEGER REFERENCES ms_payloads(id))")
        with Engine(cpus=2, mem_mb=0) as engine:
            engine.register(name, _executor(name, tmp_path / name))
            job_id = Flow(engine, project).submit(anywhere, {"n": 30}, executor=name)
            assert engine.wait(job_id, 180).status == "completed", name
        stored = rows(project, "SELECT n, structure_payload_id FROM ligands ORDER BY n")
        got = project.payloads.get_many([pid for _, pid in stored])
        seen[name] = [(n, got[pid]) for n, pid in stored]
        assert rows(project, "SELECT COUNT(*) FROM ms_payloads") == [(30,)], name
        project.dispose()
    assert seen["process"] == [(n, workers.structure(n)["structure_payload_id"].data) for n in range(30)]
    assert seen["thread"] == seen["ray"] == seen["remote"] == seen["process"]


@task()
def fails_on_fives(n: int) -> dict:
    if n % 5 == 0:
        raise RuntimeError("no structure")
    return ligand(n)


@job(task=fails_on_fives, output=table_sink("ligands", columns=("n", "structure_payload_id")))
def some_fail(params, project):
    yield from range(params["n"])


def test_a_task_that_fails_leaves_nothing(project):  # F1
    ended = run(project, some_fail, {"n": 20}, status="completed_with_errors")
    assert ended.chunks_failed == 4
    assert sorted(n for (n,) in rows(project, "SELECT n FROM ligands")) == [n for n in range(20) if n % 5]
    assert_clean(project, 16)


@task()
def no_number_on_fives(n: int) -> dict:
    return {**ligand(n), "n": None if n % 5 == 0 else n}


@job(task=no_number_on_fives, output=table_sink("ligands", columns=("n", "structure_payload_id")))
def some_rejected(params, project):
    yield from range(params["n"])


def test_a_write_that_fails_leaves_the_volume_as_it_was_and_no_rows(project):  # F2
    ended = run(project, some_rejected, {"n": 20}, status="completed_with_errors")
    assert ended.chunks_failed == 4 and "NOT NULL" in ended.error
    assert_clean(project, 16)

    before = {path.name: path.stat().st_size for path in project.payloads.directory.iterdir()}
    bad = {"ligands": {"$ref": "l", "n": 1, "structure_payload_id": Payload(b"x" * 1000, series="ligands")},
           "poses": {"ligand_id": 999_999, "rank": 0, "pose_payload_id": Payload(b"y" * 1000, series="poses")}}
    with pytest.raises(Exception, match="FOREIGN KEY"):  # the second node fails after the first one is in
        project.persist_output_spec(docking.__job__["output"], bad)
    assert {path.name: path.stat().st_size for path in project.payloads.directory.iterdir()} == before
    assert_clean(project, 16)  # and the "poses" volume made for this write is gone


def test_the_bytes_are_synced_before_their_rows_commit(project, monkeypatch):  # F6: the order I1 needs
    from sqlalchemy import event

    from ms_flow import payloads

    log = []
    sync, sync_dir = payloads._datasync, payloads._fsync_dir
    monkeypatch.setattr(payloads, "_datasync", lambda fd: (log.append("sync"), sync(fd)))
    monkeypatch.setattr(payloads, "_fsync_dir", lambda path: (log.append("dir"), sync_dir(path)))
    event.listen(project.engine, "commit", lambda conn: log.append("commit"))
    run(project, docking, {"n": 30})

    groups = "".join(entry[0] for entry in log).split("c")  # each commit and what came before it
    assert log[-1] == "commit" and groups[-1] == ""
    assert all(group.startswith("ss") for group in groups[:-1])  # the ligands and the poses volume
    assert groups[0] == "ssd"  # their files were new: the directory too
    assert rows(project, "SELECT COUNT(*) FROM poses") == [(60,)]
