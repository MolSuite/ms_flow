"""`@task`, `@job` and `Flow.submit` on the new engine: params, sinks, batch levels, table dependencies."""
import json
import sqlite3
import time
from contextlib import closing

import pytest
from pydantic import BaseModel

from ms_flow.core.database.project import ProjectStore
from ms_flow.engine import Engine, Flow, ProcessExecutor, ThreadExecutor, job, task, writer
from ms_flow.query import QuerySpec, Rows
from ms_flow.progress import watch_job
from ms_flow.sinks import file_sink, graph_sink, table_sink

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


@task()
def line(n: int) -> str:
    return f"{n}\n"


@job(task=line, output=file_sink("out/lines.txt", fmt="text", append=True))
def appended(params, project):
    return range(params["n"])


@job(task=halve, output=file_sink("out/halves.json"))
def whole_file(params, project):
    return range(0, 8, 2)


class Lines:
    """A job's own writer: keeps what arrives and stores it, or refuses to, at the end."""
    tables = ("sizes",)

    def __init__(self, params, project):
        self.params, self.kept = params, []

    def write(self, results):
        self.kept.extend(results)

    def finish(self):
        if self.params.get("refuse"):
            raise RuntimeError("nothing worth keeping")
        SEEN["lines"] = sorted(self.kept)


def tidy_up(params, project, snapshot):
    SEEN["ended"] = snapshot.status


@job(task=line, output=Lines, finalize=tidy_up)
def kept_by_its_writer(params, project):
    return range(params["n"])


class Shouted:
    """A job's own writer that changes what arrives and leaves the writing to a sink."""

    def __init__(self, params, project):
        self.sink = writer(file_sink("out/shouted.txt", fmt="text", append=True), project)

    def write(self, results):
        self.sink.write([text.replace("\n", "!\n") for text in results])


@job(task=line, output=Shouted)
def changed_before_its_sink(params, project):
    return range(params["n"])


@job(task=line, reads=("sizes",))
def after_the_writer(params, project):
    SEEN["stored_when_read"] = "lines" in SEEN
    yield from range(1)  # a generator: its body runs when the engine reads it, not at submit


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


def test_a_job_writes_files_or_through_its_own_writer_which_has_the_last_word(project, tmp_path):
    with Engine(cpus=2, mem_mb=0) as engine:
        engine.register("thread", ThreadExecutor(2))
        flow = Flow(engine, project)
        submit = lambda job, params=None: engine.wait(flow.submit(job, params, executor="thread"), 60)

        assert submit(appended, {"n": 50}).status == "completed"
        assert sorted((tmp_path / "out/lines.txt").read_text().split(), key=int) == [str(n) for n in range(50)]
        assert submit(whole_file).status == "completed"  # one file, written when the job completes
        assert sorted(row["half"] for row in json.loads((tmp_path / "out/halves.json").read_text())) == [0, 1, 2, 3]

        assert submit(changed_before_its_sink, {"n": 3}).status == "completed"
        assert sorted((tmp_path / "out/shouted.txt").read_text().split()) == ["0!", "1!", "2!"]

        SEEN.pop("lines", None)
        kept = flow.submit(kept_by_its_writer, {"n": 5}, executor="thread")
        reader = flow.submit(after_the_writer, executor="thread")  # the writer says it writes `sizes`
        assert watch_job(engine, kept, poll_s=0.01).status == "completed"
        assert SEEN["lines"] == [f"{n}\n" for n in range(5)] and SEEN["ended"] == "completed"
        assert engine.wait(reader, 60).status == "completed" and SEEN["stored_when_read"]  # not before `finish`

        refused = submit(kept_by_its_writer, {"n": 5, "refuse": True})  # every element ran; the writer said no
        assert refused.status == "failed" and refused.chunks_done == 5 and "nothing worth keeping" in refused.error
        assert SEEN["ended"] == "failed"

    with pytest.raises(TypeError, match="file_sink"):
        job(task=line, output="lines.txt")


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
        assert flow._writing[first] == {"docking_results", "ms_payloads"}
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


def test_a_recorded_job_is_submitted_again_with_what_it_was_submitted_with(project, tmp_path):
    with closing(sqlite3.connect(str(project.db_path))) as conn, conn:
        conn.execute("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 400) "
                     "INSERT INTO ligands (smiles) SELECT 'C' FROM n")

    def flow_of(engine):
        engine.attach("project", tmp_path / "executor.db")
        engine.register("thread", ThreadExecutor(4))
        return Flow(engine, project)

    with Engine(cpus=4, mem_mb=0) as engine:
        flow = flow_of(engine)
        done = flow.submit(docking, {"ligands": 5, "poses": 2}, executor="thread", journal="project", priority=3)
        assert engine.wait(done, 30).status == "completed"
        assert flow.origin(engine.status(done).key, "project") == {
            "job": f"{docking.__module__}:docking", "params": {"ligands": 5, "poses": 2, "interactions": False},
            "options": {"executor": "thread", "priority": 3}}
        with pytest.raises(ValueError, match="already ended as completed"):
            flow.resume(engine.status(done).key, journal="project")
        cut = flow.submit(every_ligand, executor="thread", journal="project")
        _until(lambda: engine.status(cut).chunks_done >= 20)
        key = engine.status(cut).key
    assert engine.status(cut).status == "interrupted"

    with Engine(cpus=4, mem_mb=0) as engine:  # another run: all it has is the key
        again = engine.wait(flow_of(engine).resume(key, journal="project"), 60)
        assert (again.status, again.chunks_done, again.key) == ("completed", 400, key)
    with closing(sqlite3.connect(str(project.db_path))) as conn:
        assert conn.execute("SELECT count(*), count(DISTINCT ligand_id) FROM docking_results "
                            "WHERE pose_rank = 0 AND ligand_id > 4").fetchall() == [(396, 396)]


def test_the_facade_runs_a_job_in_the_project_and_closing_it_interrupts(tmp_path, monkeypatch):
    from ms_flow.api import Workspace

    monkeypatch.setattr("ms_flow.core.settings.manager.Path.home", lambda: tmp_path)
    monkeypatch.setattr("ms_flow.core.settings.models.Path.home", lambda: tmp_path)

    def sql(db, text):
        with closing(sqlite3.connect(str(db))) as conn, conn:
            return conn.execute(text).fetchall()

    ms = Workspace(app_id="facade")
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
        assert (ms.jobs()[-1].id, ms.jobs()[-1].executor) == (cut, "thread")  # seen at once, before the journal has it on disk
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
        assert (past[0].chunks_done, past[0].progress, past[1].key, past[1].executor) == (300, 1.0, "again", "thread")
        assert [job.key for job in ms.jobs(limit=1)] == ["again"]
        again = ms.engine.wait(ms.resume("again"), 120)  # on threads, as it was submitted
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
        assert sorted(row["input"] for row in failed) == ["1", "3"]  # which elements they were
        assert ms.job_failures(again.key) == []
        assert (tmp_path / "one" / "executor.db").exists()

        # a running job moves: canceled where it is, and continued on the executor named
        with pytest.raises(RuntimeError, match="`nowhere` is not active"):
            ms.move(done.key, "nowhere")
        rows = sql(db, "SELECT count(*) FROM docking_results")[0][0]
        moving = ms.submit(every_ligand, executor="thread", key="moved", cpu=2)
        _until(lambda: ms.engine.status(moving).chunks_done >= 20)
        moved = ms.engine.wait(ms.move("moved", "local"), 120)
        assert (ms.engine.status(moving).status, moved.status, moved.chunks_done) == ("canceled", "completed", 300)
        assert ms.origin("moved")["options"] == {"executor": "local", "cpu": 2}
        assert rows + 300 <= sql(db, "SELECT count(*) FROM docking_results")[0][0] < rows + 320
    finally:
        ms.shutdown()
    assert ms._engine is None
