"""`@task`, `@job` and `Flow.submit` on the new engine: params, sinks, batch levels, table dependencies."""
import sqlite3
import time
from contextlib import closing

import pytest
from pydantic import BaseModel

from ms_flow.core.database.project import ProjectStore
from ms_flow.engine import Engine, Flow, ProcessExecutor, ThreadExecutor, job, task
from ms_flow.query import QuerySpec, Rows
from ms_flow.sinks import graph_sink, table_sink

SEEN = {}


class DockParams(BaseModel):
    ligands: int = 0
    poses: int = 3
    interactions: bool = False


@task(cpu=1, retries=1)
def dock(ligand: int, params: DockParams) -> dict:
    poses = [{"$ref": f"{ligand}-{rank}", "ligand_id": ligand, "pose_rank": rank} for rank in range(params.poses)]
    found = [{"pose_ref": pose["$ref"], "residue": pose["$ref"]} for pose in poses if params.interactions]
    return {"docking_results": poses, "interaction_results": found}


POSES = graph_sink(
    nodes=({"name": "docking_results", "table": "docking_results", "columns": ("ligand_id", "pose_rank")},
           {"name": "interaction_results", "table": "interaction_results",
            "columns": ("docking_result_id", "residue")}),
    relations=({"source_node": "interaction_results", "source_ref_field": "pose_ref",
                "target_node": "docking_results", "fk_field": "docking_result_id"},))


@job(task=dock, params_model=DockParams, reads=("ligands",), output=POSES)
def docking(params: DockParams, project):
    yield from range(params.ligands)


@task()
def dock_with_a_ref_every_element_repeats(ligand: int) -> dict:
    return {"docking_results": [{"$ref": "best", "ligand_id": ligand, "pose_rank": 0}],
            "interaction_results": [{"pose_ref": "best", "residue": f"{ligand}-0"}]}


@job(task=dock_with_a_ref_every_element_repeats, output=POSES)
def clashing(params, project):
    yield from range(1000, 1050)


@task(batch=4, batched=True)
def measure(elements: list) -> list:
    return [{"element": element, "travelled_with": len(elements)} for element in elements]


@job(task=measure, output=table_sink("sizes", columns=("element", "travelled_with")), reads=("docking_results",))
def sizes(params, project):
    with closing(sqlite3.connect(str(project.db_path))) as conn:
        SEEN["poses"] = conn.execute("SELECT COUNT(*) FROM docking_results").fetchone()[0]
    yield from range(params["n"])


@task()
def slowly(ligand: dict) -> dict:
    time.sleep(0.005)
    return {"ligand_id": ligand["id"], "pose_rank": 0}


@job(task=slowly, output=table_sink("docking_results", columns=("ligand_id", "pose_rank")))
def every_ligand(params, project):
    return Rows(project, QuerySpec(table="ligands", fields=("id",)), page_size=7)


@task()
def halve(n: int) -> dict:
    if n % 2:
        raise ValueError("odd")
    return {"half": n / 2}


@job(task=halve, output=table_sink("halves", columns=("half",)))
def odd_ones_fail(params, project):
    return range(4)


@pytest.fixture
def project(tmp_path):
    with closing(sqlite3.connect(tmp_path / "project.db")) as conn, conn:
        conn.execute("CREATE TABLE ligands (id INTEGER PRIMARY KEY, smiles TEXT)")
        conn.execute("CREATE TABLE docking_results (id INTEGER PRIMARY KEY, ligand_id INT, pose_rank INT)")
        conn.execute("CREATE TABLE interaction_results (id INTEGER PRIMARY KEY, docking_result_id INT, residue TEXT)")
        conn.execute("CREATE TABLE sizes (element INT, travelled_with INT)")
    return ProjectStore.open_at(tmp_path / "project.db")


def _until(true, seconds: float = 20.0):
    end = time.monotonic() + seconds
    while not true():
        assert time.monotonic() < end, "never happened"
        time.sleep(0.005)


def test_a_job_writes_its_tables_and_the_job_that_reads_them_waits(project):
    def rows(sql):
        with closing(sqlite3.connect(str(project.db_path))) as conn:
            return conn.execute(sql).fetchall()

    with Engine(cpus=4, mem_mb=0) as engine:
        engine.register("process", ProcessExecutor(4))
        flow = Flow(engine, project)

        docked = flow.submit(docking, {"ligands": 300, "interactions": True})
        sized = flow.submit(sizes, {"n": 10})  # reads what `docking` writes: its source is not read until then
        assert engine.wait(docked, 120).status == "completed"
        assert engine.wait(sized, 120).status == "completed"
        assert SEEN["poses"] == 900
        well_tied = ("SELECT COUNT(*) FROM interaction_results i JOIN docking_results d ON d.id = i.docking_result_id "
                     "WHERE i.residue = d.ligand_id || '-' || d.pose_rank")
        assert rows("SELECT COUNT(*) FROM interaction_results")[0][0] == rows(well_tied)[0][0] == 900
        assert sorted(rows("SELECT element, travelled_with FROM sizes")) == \
            [(i, 4) for i in range(8)] + [(8, 2), (9, 2)]  # the task's own batch

        with pytest.raises(Exception, match="ligands"):  # the params model speaks at submit
            flow.submit(docking, {"ligands": "many"})
        assert flow._writing == {}

        # the option off writes nothing to the second table; a batch given at submit wins
        before = rows("SELECT COUNT(*) FROM interaction_results")[0][0]
        assert engine.wait(flow.submit(docking, {"ligands": 20}), 120).status == "completed"
        assert rows("SELECT COUNT(*) FROM docking_results")[0][0] == 960
        assert rows("SELECT COUNT(*) FROM interaction_results")[0][0] == before
        assert engine.wait(flow.submit(sizes, {"n": 16}, batch=8), 120).status == "completed"
        assert rows("SELECT COUNT(*) FROM sizes WHERE travelled_with = 8")[0][0] == 16

        # a ref names a row of its element only: written in groups, none is tied to another's
        assert engine.wait(flow.submit(clashing), 120).chunks_done == 50
        assert rows(well_tied)[0][0] == 950

    # the store itself refuses two rows under one ref, and writes none of them
    twice = {"docking_results": [{"$ref": "best", "ligand_id": 7000 + i, "pose_rank": 0} for i in range(2)],
             "interaction_results": [{"pose_ref": "best", "residue": "7000-0"}]}
    with pytest.raises(Exception, match="duplicate ref 'best'"):
        project.persist_output_spec(POSES, twice)
    assert rows("SELECT COUNT(*) FROM docking_results WHERE ligand_id >= 7000")[0][0] == 0


def test_a_table_as_source_gives_the_total_and_keeps_out_what_arrives_later(project, tmp_path, monkeypatch):
    def sql(text, *args):
        with closing(sqlite3.connect(str(project.db_path))) as conn, conn:
            return conn.execute(text, args).fetchall()

    monkeypatch.setattr("ms_flow.engine.engine._RECOUNT", 0)  # count again at every chunk
    sql("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 400) "
        "INSERT INTO ligands (smiles) SELECT 'C' FROM n")
    with Engine(cpus=4, mem_mb=0) as engine:
        engine.attach("project", tmp_path / "executor.db")
        engine.register("thread", ThreadExecutor(4))
        flow = Flow(engine, project)
        submit = lambda **more: flow.submit(every_ligand, executor="thread", journal="project", **more)  # noqa: E731

        first = submit(key="all")
        assert flow._writing[first] == {"docking_results"}
        _until(lambda: engine.status(first).chunks_total == 400)
        sql("DELETE FROM ligands WHERE id BETWEEN 381 AND 390")  # not read yet: the total follows
        _until(lambda: engine.status(first).chunks_total == 390)
        _until(lambda: engine.status(first).chunks_done >= 20)
        engine.cancel(first)
        assert engine.wait(first, 30).status == "canceled"

        sql("INSERT INTO ligands (smiles) SELECT 'N' FROM ligands LIMIT 10")  # ids 401 to 410
        again = engine.wait(submit(key="all"), 60)
        assert (again.status, again.chunks_done, again.chunks_total) == ("completed", 390, 390)
        assert sql("SELECT count(*), count(DISTINCT ligand_id), max(ligand_id) FROM docking_results") == \
            [(390, 390, 400)]

        new = engine.wait(submit(), 60)  # another job: its own cap
        assert (new.status, new.chunks_total) == ("completed", 400)
        assert sql("SELECT max(ligand_id) FROM docking_results") == [(410,)]


def test_the_facade_runs_a_job_in_the_project_and_closing_it_interrupts(tmp_path, monkeypatch):
    from ms_flow.api import MolSuite

    monkeypatch.setattr("ms_flow.core.settings.manager.Path.home", lambda: tmp_path)
    monkeypatch.setattr("ms_flow.core.settings.models.Path.home", lambda: tmp_path)

    def sql(db, text):
        with closing(sqlite3.connect(str(db))) as conn, conn:
            return conn.execute(text).fetchall()

    ms = MolSuite(app_id="facade")
    try:
        assert ms.jobs() == []
        ms.create_or_open_project(name="one", folder=tmp_path / "one", description="", activate=True)
        db = ms.project_db.db_path
        sql(db, "CREATE TABLE ligands (id INTEGER PRIMARY KEY, smiles TEXT)")
        sql(db, "CREATE TABLE docking_results (id INTEGER PRIMARY KEY, ligand_id INT, pose_rank INT)")
        sql(db, "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 300) "
                "INSERT INTO ligands (smiles) SELECT 'C' FROM n")

        done = ms.engine.wait(ms.submit(every_ligand), 120)
        assert (done.status, done.chunks_done, done.progress) == ("completed", 300, 1.0)
        assert sql(db, "SELECT count(DISTINCT ligand_id) FROM docking_results") == [(300,)]

        # closing the project interrupts its job, with what it delivered written; the same key continues it
        cut = ms.submit(every_ligand, executor="thread", key="again")
        _until(lambda: ms.engine.status(cut).chunks_done >= 20)
        activity = ms.get_project_activity()
        assert (activity["active"], activity["can_switch_project"], activity["job_ids"]) == (True, False, [cut])
        with pytest.raises(RuntimeError, match="active jobs remain"):
            ms.close_project(cancel_running_tasks=False)
        ms.close_project()
        assert ms.get_project_activity()["active"] is False
        assert ms.engine.status(cut).status == "interrupted"
        written = sql(db, "SELECT count(*) FROM docking_results")[0][0]
        assert 320 <= written < 600

        # reopened: its jobs are read from the journal, before anything builds on the engine again
        ms.create_or_open_project(name="one", folder=tmp_path / "one", description="", activate=True)
        past = ms.jobs()
        assert [(job.id, job.status) for job in past] == [(None, "completed"), (None, "interrupted")]
        assert (past[0].chunks_done, past[0].progress, past[1].key) == (300, 1.0, "again")
        assert [job.key for job in ms.jobs(limit=1)] == ["again"]
        again = ms.engine.wait(ms.submit(every_ligand, executor="thread", key="again"), 120)
        assert (again.status, again.chunks_done) == ("completed", 300)
        assert sql(db, "SELECT count(*) FROM docking_results") == [(600,)]
        assert ms.get_project_activity()["jobs_active"] == 0
        assert [row["status"] for row in ms.engine.recorded("project")] == ["completed"] * 2
        # once the journal is open again, the engine answers for the ones it ran, the first included
        assert [(job.id, job.status) for job in ms.jobs()] == [(done.id, "completed"), (again.id, "completed")]

        sql(db, "CREATE TABLE halves (id INTEGER PRIMARY KEY, half REAL)")
        broken = ms.engine.wait(ms.submit(odd_ones_fail, executor="thread"), 60)
        assert (broken.status, broken.chunks_failed) == ("completed_with_errors", 2)
        failed = ms.job_failures(broken.key)
        assert len(failed) == 2 and all("ValueError: odd" in row["error"] for row in failed)
        assert ms.job_failures(again.key) == []
        assert (tmp_path / "one" / "executor.db").exists()
    finally:
        ms.shutdown()
    assert ms._engine is None
