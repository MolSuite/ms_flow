"""`chain`: steps run in line on one element, a failed step only stops the steps built on it,
and what a step leaves for `cleanup` waits for the last one."""
import pickle
from pathlib import Path

import pytest

from ms_flow.engine import ThreadExecutor, chain, cleanup, task


@task(cpu=1, thread=True)
def dock(element, params=None):
    pose = Path(params["dir"]) / f"{element}.sdf"
    pose.write_text(f"pose of {element}")
    with cleanup() as later:
        later.callback(pose.unlink)
    if element == "bad":
        raise ValueError("no pose")
    return {"poses": [{"name": element, "path": str(pose)}]}


@task(cpu=2, mem_mb=500)
def contacts(element, previous):
    path = Path(previous["poses"][0]["path"])
    if element == "flaky":
        raise RuntimeError("detector")
    return {"interactions": {"name": element, "text": path.read_text()}}  # the file is still there


@task(cpu=1, thread=True)
def rescore(element, previous):
    return [{"name": element, "poses": len(previous["poses"])}]


def test_steps_run_in_line_and_the_files_go_after_the_last(tmp_path):
    run = chain(dock, contacts, rescore, needs={"rescore": ["dock"]})
    assert run.__task__["cpu"] == 2 and run.__task__["mem_mb"] == 500 and run.__task__["thread"] is False
    assert pickle.loads(pickle.dumps(run)).names == ["dock", "contacts", "rescore"]  # what a worker receives

    out = run("a", params={"dir": str(tmp_path)})
    assert out == {"poses": [{"name": "a", "path": str(tmp_path / "a.sdf")}],
                   "interactions": [{"name": "a", "text": "pose of a"}],
                   "rescore": [{"name": "a", "poses": 1}]}
    assert not list(tmp_path.iterdir())


def test_a_failed_step_stops_only_what_builds_on_it(tmp_path):
    seen = []
    run = chain(dock, contacts, rescore, needs={"rescore": ["dock"]},
                on_error=lambda step, element, error: {"failures": {"step": step, "error": str(error)}} if not seen.append(step) else None)
    out = run("flaky", params={"dir": str(tmp_path)})
    assert set(out) == {"poses", "rescore", "failures"} and out["failures"] == [{"step": "contacts", "error": "detector"}]
    with pytest.raises(ValueError, match="no pose"):  # every step failed or was skipped: the element fails
        run("bad", params={"dir": str(tmp_path)})
    assert not list(tmp_path.iterdir())


def test_a_chain_runs_on_a_thread_only_if_every_step_does():
    assert chain(dock, rescore).__task__["thread"] is True
    assert chain(dock, contacts, thread=True).__task__["thread"] is True


def test_a_chain_is_a_task_the_engine_runs(tmp_path):
    import functools

    from ms_flow.engine import Engine

    results = []
    with Engine(cpus=2) as engine:
        engine.register("thread", ThreadExecutor(2))
        run = chain(dock, rescore)
        job_id = engine.submit("thread", functools.partial(run, params={"dir": str(tmp_path)}), ["x", "bad"],
                               on_result=lambda key, result: results.append(result), **run.__task__)
        snapshot = engine.wait(job_id, timeout=30)
    assert snapshot.chunks_done == 1 and snapshot.chunks_failed == 1
    assert results == [{"poses": [{"name": "x", "path": str(tmp_path / "x.sdf")}], "rescore": [{"name": "x", "poses": 1}]}]
