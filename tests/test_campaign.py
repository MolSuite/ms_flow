"""Campaigns against this machine's `none` queue: the same commands a cluster gets, run by `sh` here."""
import json
import os
import shutil
import subprocess
import sys
import time
from functools import partial
from pathlib import Path

import pytest
from pydantic import BaseModel

from ms_flow.campaign import Campaign, From, Stage, campaign
from ms_flow.engine import Dir, Engine, File, Flow, ThreadExecutor, task
from ms_flow.remote import RemoteExecutor

pytestmark = pytest.mark.slow

_ROOT = Path(__file__).resolve().parents[1]
GOT = []  # what the campaign's writer was given, here


class Params(BaseModel):
    n: int = 6
    nap: float = 0
    run_id: str = ""


@task()
def square(element, params):
    assert params.run_id is None  # volatile: not the tasks' business
    if element["n"] == 3:
        raise ValueError("three is not for squaring")
    time.sleep(params.nap)
    wrote = Path(element["out"], f"{element['n']}.txt")
    wrote.write_text("left there")
    return {"stage": "vs", "n": element["n"], "v": element["n"] ** 2, "said": Path(element["f"]).read_text(),
            "wrote": str(wrote)}


def best(results, k):
    return sorted(results, key=lambda row: row["v"])[-k:]


@task()
def negate(row):
    return {"stage": "off", "n": row["n"], "v": -row["v"]}


def nothing(left, said):
    assert Path(said).read_text() == "from here"  # an argument's file travelled too
    return []


def count(results):
    return {"stage": "count", "n": sum(1 for _ in results)}


class _Collect:
    def write(self, results):
        GOT.extend(results)


@campaign(params_model=Params, output=lambda params, project: _Collect(), volatile=("run_id",))
def screen(params, project):
    said = Path(os.environ["CAMPAIGN_FILE"])
    out = Dir(said.parent / "work")
    return [Stage("vs", square, [{"n": n, "f": File(said), "out": out, "_cpu_required": 1} for n in range(params.n)],
                  reduce=partial(best, k=2)),
            Stage("off", negate, From("vs", list), deliver="none", reduce=count),
            Stage("count", negate, From("off", nothing, (File(said),)), deliver="none"),
            Stage("again", negate, From("vs", list), reduce=count, deliver="reduced")]


def _far(tmp_path, **options):
    return RemoteExecutor(workdir=str(tmp_path / "far"), python=sys.executable, poll_s=0.2, part_seconds=0.1, runner="--cpus 2",
                          setup=f"export PYTHONPATH={_ROOT}:{_ROOT / 'src'}:{_ROOT / 'tests'}:{os.environ.get('PYTHONPATH', '')}",
                          **options)


def _engine(tmp_path, far):
    engine = Engine(cpus=4)
    engine.register("thread", ThreadExecutor(4))
    engine.register("far", far)
    engine.attach("j", tmp_path / "j.db")
    return engine, Flow(engine, None)


def _dirs(tmp_path):
    return sorted(path.name for path in (tmp_path / "far").glob("c*-*"))


def _until(check, seconds=60):
    limit = time.monotonic() + seconds
    while not check():
        assert time.monotonic() < limit
        time.sleep(0.1)
    return True


def test_a_campaign_runs_its_stages_there_and_is_found_again_by_its_identity(tmp_path, monkeypatch):
    (tmp_path / "said.txt").write_text("from here")
    monkeypatch.setenv("CAMPAIGN_FILE", str(tmp_path / "said.txt"))
    GOT.clear()
    engine, flow = _engine(tmp_path, _far(tmp_path))
    with engine:
        ended = engine.wait(flow.submit(screen, {"run_id": "a"}, executor="far", journal="j"), 120)
        assert ended.status == "completed_with_errors" and ended.chunks_failed == 1, ended.error
        assert sorted(row["n"] for row in GOT if row["stage"] == "vs") == [0, 1, 2, 4, 5]
        assert {row["said"] for row in GOT if row["stage"] == "vs"} == {"from here"}  # the file travelled
        assert all(row["wrote"].startswith(str(tmp_path / "far")) for row in GOT if row["stage"] == "vs")
        assert not (tmp_path / "work").exists()  # what a task writes in a Dir stays there
        assert [row for row in GOT if row["stage"] != "vs"] == [{"stage": "count", "n": 2}]  # `none` stays there
        [first] = _dirs(tmp_path)
        assert (tmp_path / "far" / first / "ingested").exists()
        queued = (tmp_path / "far" / first / "queue.id").read_text()

        # Another run's id is the same campaign: what is there comes again, nothing runs again.
        GOT.clear()
        again = engine.wait(flow.submit(screen, {"run_id": "b"}, executor="far", journal="j"), 120)
        assert again.status == "completed_with_errors" and len(GOT) == 6
        assert _dirs(tmp_path) == [first] and (tmp_path / "far" / first / "queue.id").read_text() == queued

        # Canceled here, it is canceled there; submitted again, it is a new attempt.
        GOT.clear()
        slow = flow.submit(screen, {"n": 40, "nap": 0.2}, executor="far", journal="j")
        _until(lambda: len(_dirs(tmp_path)) == 2 and GOT)
        engine.cancel(slow)
        assert engine.status(slow).canceling and engine.status(slow).status == "running"  # until it is gone there
        assert engine.wait(slow, 60).status == "canceled" and not engine.status(slow).canceling
        [canceled] = set(_dirs(tmp_path)) - {first}
        # Only then: its queue job is gone, not left to run on its own.
        assert (tmp_path / "far" / canceled / "cancelled").exists()
        pid = (tmp_path / "far" / canceled / "queue.id").read_text().strip()
        assert subprocess.run(["ps", "-p", pid], capture_output=True).returncode != 0
        GOT.clear()
        done = engine.wait(flow.submit(screen, {"n": 40, "nap": 0.2}, executor="far", journal="j"), 120)
        assert done.status == "completed_with_errors" and len([row for row in GOT if row["stage"] == "vs"]) == 39
        assert canceled.endswith("-0") and set(_dirs(tmp_path)) == {first, canceled, canceled[:-1] + "1"}

        # Once all of it is here, a directory goes at once with `keep_after` 0.
        engine.executor("far").keep_after = 0
        assert engine.wait(flow.submit(screen, {"run_id": "c"}, executor="far", journal="j"), 120).chunks_done
        assert first not in _dirs(tmp_path)

    # Closed while it runs, it goes on there; submitted again, only what this side has not got comes.
    GOT.clear()
    engine, flow = _engine(tmp_path, _far(tmp_path))
    with engine:
        flow.submit(screen, {"n": 30, "nap": 0.3}, executor="far", journal="j")
        _until(lambda: len(GOT) >= 3)
    before = len(GOT)
    assert before < 30  # it was closed halfway
    engine, flow = _engine(tmp_path, _far(tmp_path))
    with engine:
        resumed = engine.wait(flow.submit(screen, {"n": 30, "nap": 0.3}, executor="far", journal="j"), 120)
        assert resumed.status == "completed_with_errors"
    vs = [row["n"] for row in GOT if row["stage"] == "vs"]
    assert sorted(vs) == [n for n in range(30) if n != 3] and len(GOT) > before  # each one once


_KEYS = """
import sys
from {module} import Params, screen, _far
from ms_flow.campaign import Campaign
from ms_flow.engine import content_key
far = Campaign(_far(__import__("pathlib").Path(sys.argv[1])), screen, Params(n=4, run_id="r"), None)
print(far.key, content_key({{"p": Params(n=4), "f": __import__("ms_flow.engine").engine.File("x")}}).hex())
far.close()
"""


def test_the_identity_is_the_same_in_every_process(tmp_path):
    # A pickle of params is not: the order of the set of fields it holds changes with the hash seed.
    (tmp_path / "said.txt").write_text("from here")
    code = _KEYS.format(module=__name__)
    env = {**os.environ, "CAMPAIGN_FILE": str(tmp_path / "said.txt"), "PYTHONPATH": os.pathsep.join(sys.path)}
    keys = {subprocess.run([sys.executable, "-c", code, str(tmp_path)], capture_output=True, text=True, check=True,
                           cwd=tmp_path, env={**env, "PYTHONHASHSEED": seed}).stdout
            for seed in ("1", "2", "3")}
    assert len(keys) == 1, keys


def test_a_campaign_asks_the_queue_for_itself_and_stays_the_same_campaign(tmp_path, monkeypatch):
    (tmp_path / "said.txt").write_text("from here")
    monkeypatch.setenv("CAMPAIGN_FILE", str(tmp_path / "said.txt"))
    far = RemoteExecutor(workdir=str(tmp_path / "far"), scheduler="slurm", setup="#SBATCH --qos=low\nexport A=1",
                         asks={"partition": "short", "cpus": 4, "modules": ["load gcc"]})
    try:
        plain = Campaign(far, screen, Params(n=2), None)
        more = Campaign(far, screen, Params(n=2), None, {"cpus": 16, "time": "2:00:00"})
        assert plain.key == more.key
        script = (more._tmp / "job.sh").read_text()
        assert "#SBATCH --partition=short\n#SBATCH --cpus-per-task=16\n#SBATCH --time=2:00:00\n#SBATCH --qos=low\n" \
               "module load gcc\nexport A=1\n" in script
        assert "--cpus-per-task=4" in (plain._tmp / "job.sh").read_text()
        plain.close(), more.close()
        with pytest.raises(ValueError, match="queue"):
            far.script_for({"queue": "x"})
    finally:
        far.shutdown()


@campaign(params_model=Params, output=lambda params, project: _Collect(), volatile=("run_id",))
def one_stage(params, project):
    said = Path(os.environ["CAMPAIGN_FILE"])
    return [Stage("vs", square, [{"n": n, "f": File(said), "out": Dir(said.parent / "work")} for n in range(params.n)])]


def test_a_campaign_of_one_stage_has_its_total_its_queue_and_says_where_it_is(tmp_path, monkeypatch):
    (tmp_path / "said.txt").write_text("from here")
    monkeypatch.setenv("CAMPAIGN_FILE", str(tmp_path / "said.txt"))
    engine, flow = _engine(tmp_path, _far(tmp_path))
    with engine:
        job = flow.submit(one_stage, {"n": 12, "nap": 1}, executor="far", journal="j")
        now = engine.status(job)
        assert now.chunks_total == 12 and now.executor == "far"  # from the start, not "thread" and "?"
        _until(lambda: "running in the queue (job " in (engine.status(job).note or ""))
        ended = engine.wait(job, 120)
    assert ended.chunks_total == 12 and ended.chunks_done == 11 and ended.note is None, ended.error
    assert [json.loads(row["spec"])["executor"] for row in engine.recorded("j")] == ["far"]


def test_a_campaign_whose_results_were_purged_there_goes_again(tmp_path, monkeypatch):
    (tmp_path / "said.txt").write_text("from here")
    monkeypatch.setenv("CAMPAIGN_FILE", str(tmp_path / "said.txt"))
    engine, flow = _engine(tmp_path, _far(tmp_path))
    with engine:
        assert engine.wait(flow.submit(screen, {"run_id": "a"}, executor="far", journal="j"), 120).chunks_done
        [first] = _dirs(tmp_path)
        for part in (tmp_path / "far" / first / "stages" / "vs" / "results").glob("part-*"):
            part.unlink()  # what a scratch purge does to old files
        GOT.clear()
        again = engine.wait(flow.submit(screen, {"run_id": "b"}, executor="far", journal="j"), 120)
        assert again.status == "completed_with_errors", again.error
        assert sorted(row["n"] for row in GOT if row["stage"] == "vs") == [0, 1, 2, 4, 5]
        assert _dirs(tmp_path) == [first, first[:-1] + "1"] and (tmp_path / "far" / first / "lost").exists()

        # Its whole directory gone while it runs: canceled there, and a new attempt again.
        GOT.clear()
        slow = flow.submit(screen, {"n": 20, "nap": 0.2}, executor="far", journal="j")
        _until(lambda: GOT)
        [running] = set(_dirs(tmp_path)) - {first, first[:-1] + "1"}
        gone = tmp_path / "far" / running  # what its runner writes meanwhile may stop one rmtree
        _until(lambda: shutil.rmtree(gone, ignore_errors=True) or not gone.exists())
        ended = engine.wait(slow, 120)
        assert ended.status == "completed_with_errors", ended.error
        assert {row["n"] for row in GOT if row["stage"] == "vs"} == {n for n in range(20) if n != 3}
        assert running[:-1] + "1" in _dirs(tmp_path)


def test_a_campaign_waits_for_a_host_that_does_not_answer(tmp_path, monkeypatch):
    from test_remote import _outage
    (tmp_path / "said.txt").write_text("from here")
    monkeypatch.setenv("CAMPAIGN_FILE", str(tmp_path / "said.txt"))
    GOT.clear()
    far = _far(tmp_path)
    engine, flow = _engine(tmp_path, far)
    with engine:
        left = _outage(far, 4)
        job = flow.submit(screen, {"run_id": "a"}, executor="far", journal="j")
        _until(lambda: engine.status(job).unreachable)
        engine.reconnect(job)  # through the campaign it follows
        ended = engine.wait(job, 120)
    assert ended.status == "completed_with_errors" and left == [0] and ended.unreachable is None, ended.error
    assert sorted(row["n"] for row in GOT if row["stage"] == "vs") == [0, 1, 2, 4, 5]


@pytest.mark.parametrize("rsync", [True, False])
def test_the_files_of_a_project_go_once_to_its_folder_there(tmp_path, monkeypatch, rsync):
    from ms_flow import campaign as module, locations
    from ms_flow.campaign import Campaign
    from ms_flow.core.database.project import ProjectStore

    project = ProjectStore()
    project.connect(tmp_path / "p")
    (tmp_path / "p" / "in").mkdir()
    (tmp_path / "p" / "in" / "said.txt").write_text("from here")
    monkeypatch.setenv("CAMPAIGN_FILE", str(tmp_path / "p" / "in" / "said.txt"))
    sent, send, sync = [], Campaign._send, Campaign._rsync
    monkeypatch.setattr(Campaign, "_send", lambda self, there, files, *a: (sent.extend(files), send(self, there, files, *a)))
    monkeypatch.setattr(Campaign, "_rsync", lambda self, there, paths: sync(self, there, paths) and not sent.extend(paths))
    if not rsync:  # this machine has none: tar
        monkeypatch.setattr(module.shutil, "which", lambda name: None)
    GOT.clear()
    far = _far(tmp_path)
    engine = Engine(cpus=4)
    engine.register("thread", ThreadExecutor(4))
    engine.register("far", far)
    flow = Flow(engine, project)
    with engine:
        assert engine.wait(flow.submit(screen, {"run_id": "a"}, executor="far"), 120).status == "completed_with_errors"
        there = Path(locations.root(far, project))
        assert (there / "in" / "said.txt").read_text() == "from here" and sent.count("in/said.txt") == 1
        assert {row["said"] for row in GOT if row["stage"] == "vs"} == {"from here"}
        assert list(locations.known(project, locations.host_of(far))) == ["in/said.txt"]

        # another campaign of the project finds it there
        assert engine.wait(flow.submit(screen, {"n": 5}, executor="far"), 120).status == "completed_with_errors"
        assert sent.count("in/said.txt") == 1 and len(_dirs(tmp_path)) == 2

        # gone from there (a purged scratch): reconcile says so, and the next campaign sends it again
        (there / "in" / "said.txt").unlink()
        assert locations.reconcile(project, far) == ["in/said.txt"]
        assert locations.known(project, locations.host_of(far)) == {}
        assert engine.wait(flow.submit(screen, {"n": 4}, executor="far"), 120).status == "completed_with_errors"
        assert sent.count("in/said.txt") == 2 and (there / "in" / "said.txt").exists()
