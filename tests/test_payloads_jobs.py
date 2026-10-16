"""Payloads written from jobs: a task returns `Payload`s in its rows and the sink stores them (V2)."""
import sqlite3
from contextlib import closing

import pytest

from ms_flow.core.database.project import ProjectStore
from ms_flow.engine import Engine, Flow, ThreadExecutor, job, task
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


def run(project, job, params=None):
    with Engine(cpus=4, mem_mb=0) as engine:
        engine.register("thread", ThreadExecutor(4))
        job_id = Flow(engine, project).submit(job, params, executor="thread")
        assert engine.wait(job_id, 60).status == "completed"


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
