"""`@task`, `@job` and `Flow.submit` on the new engine: params, sinks, batch levels, table dependencies."""
import sqlite3
from contextlib import closing

import pytest
from pydantic import BaseModel

from ms_flow.api import MolSuite, graph_sink, table_sink
from ms_flow.engine import Engine, Flow, ProcessExecutor, job, task

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


@pytest.fixture
def project(tmp_path, monkeypatch):
    monkeypatch.setattr("ms_flow.core.settings.manager.Path.home", lambda: tmp_path)
    monkeypatch.setattr("ms_flow.core.settings.models.Path.home", lambda: tmp_path)
    ms = MolSuite(app_id="testflow")
    ms.create_or_open_project(name="p", folder=tmp_path / "p", description="", scope="testing", activate=True)
    with closing(sqlite3.connect(str(ms.project_db.db_path))) as conn, conn:
        conn.execute("CREATE TABLE docking_results (id INTEGER PRIMARY KEY, ligand_id INT, pose_rank INT)")
        conn.execute("CREATE TABLE interaction_results (id INTEGER PRIMARY KEY, docking_result_id INT, residue TEXT)")
        conn.execute("CREATE TABLE sizes (element INT, travelled_with INT)")
    yield ms.project_db
    ms.shutdown()


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
