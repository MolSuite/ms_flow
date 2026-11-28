"""`ms_flow.runner`: a staged directory run to its end, and one stopped and run again."""
import json
import os
import platform
import signal
import subprocess
import sys
import time
from collections import Counter

import pytest

from benchmarks.refactor_optimization import workers
from ms_flow.engine import File
from ms_flow.payloads import PayloadGroup
from ms_flow.runner import plain, read_part, stage, unplain

pytestmark = pytest.mark.slow


def _runner(home):
    return subprocess.Popen([sys.executable, "-m", "ms_flow.runner", str(home), "--cpus", "2"],
                            env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)})


def _progress(home):
    try:
        return json.loads((home / "progress.json").read_text())
    except OSError:
        return {}


def _indexes(home):
    found = []
    for part in sorted((home / "results").glob("part-*.jsonl")):
        with open(part, "rb") as file:
            found += [index for index, result in read_part(file) if result == {"ok": 1}]
    return found


def test_a_staged_job_runs_to_its_end_and_says_which_element_failed(tmp_path):
    home = tmp_path / "job"
    assert stage(home, workers.die_on_7, [{"idx": i} for i in range(12)], part_seconds=0.1) == 12
    assert _runner(home).wait(120) == 0
    told = _progress(home)
    assert (told["status"], told["total"], told["done"], told["failed"]) == ("completed_with_errors", 12, 11, 1)
    assert sorted(_indexes(home)) == [i for i in range(12) if i != 7]
    assert told["ran_on"]["python"] == platform.python_version() and "ms_flow" in told["ran_on"]  # its provenance
    (line,) = (home / "errors.jsonl").read_text().splitlines()
    assert json.loads(line)["index"] == 7 and "worker process died" in line
    assert _runner(home).wait(60) == 0 and len(_indexes(home)) == 11  # ended: a second run does nothing


def test_a_job_stopped_by_the_queue_goes_on_where_it_was(tmp_path):
    home = tmp_path / "job"
    stage(home, workers.nap, [{"s": 0.2, "n": i} for i in range(60)], part_seconds=0.1)
    first = _runner(home)
    deadline = time.monotonic() + 60
    while not _progress(home).get("done") and time.monotonic() < deadline:
        time.sleep(0.05)
    first.send_signal(signal.SIGTERM)
    assert first.wait(60) == 143
    told, before = _progress(home), _indexes(home)
    assert told["status"] == "interrupted" and 0 < told["done"] == len(before) < 60
    assert _runner(home).wait(120) == 0
    assert sorted(_indexes(home)) == list(range(60)) and _progress(home)["status"] == "completed"


def a_set_on_1(payload):
    return {"raw": {1}} if payload["n"] == 1 else {"n": payload["n"], "f": payload["f"], "b": payload["b"]}


def test_what_travels_is_data_and_names_and_what_is_not_is_refused(tmp_path):
    with pytest.raises(TypeError, match=r"element 1\.blob: a object is not data"):
        stage(tmp_path / "a", workers.noop, [{"n": 0}, {"blob": object()}])
    with pytest.raises(TypeError, match="is not found by it"):
        stage(tmp_path / "b", lambda payload: payload, [{"n": 0}])
    home = tmp_path / "c"
    stage(home, a_set_on_1, [{"n": n, "f": File("/x"), "b": b"\x00"} for n in range(3)], part_seconds=0.1)
    assert sorted(path.name for path in home.iterdir()) == ["elements.jsonl", "spec.json"]
    assert json.loads((home / "spec.json").read_text())["call"] == {"$call": f"{__name__}:a_set_on_1"}
    assert _runner(home).wait(120) == 0
    results = [pair for part in sorted((home / "results").glob("part-*.jsonl")) for pair in read_part(open(part, "rb"))]
    assert sorted(results) == [(0, {"n": 0, "f": "/x", "b": b"\x00"}), (2, {"n": 2, "f": "/x", "b": b"\x00"})]
    assert all(type(result["f"]) is File for _, result in results)  # a mark comes back a mark
    (line,) = (home / "errors.jsonl").read_text().splitlines()
    assert json.loads(line)["index"] == 1 and "a set is not data" in line


def test_parts_of_one_group_come_back_parts_of_one_group():
    group = PayloadGroup(kind="csv")
    rows = unplain(json.loads(json.dumps(plain([{"layout": group.add(b"one")}, {"layout": group.add(b"two")}], "rows"))))
    (whole, sizes), = {id(row["layout"].group): row["layout"].group.packed() for row in rows}.values()
    assert [row["layout"].index for row in rows] == [0, 1] and sizes == [3, 3] and whole.raw_length == 6
    assert unplain(json.loads(json.dumps(plain({7: "a", None: [1]}, "keys")))) == {7: "a", None: [1]}


def test_a_job_staged_with_pickles_is_refused_with_why(tmp_path):
    (tmp_path / "spec.json").write_text(json.dumps({"fn": "f", "total": 1, "python": "3.12"}))
    assert _runner(tmp_path).wait(60) == 1
    assert "staged by another ms_flow" in _progress(tmp_path)["error"]


def _committing(home, n, s, *, log):
    from ms_flow.runner import stage_campaign

    stages = [{"name": "all", "fn": workers.part_of, "needs": {}, "reduce": None, "from": None,
               "commit": workers.receipts_of}]
    return stage_campaign(home, stages, [{"n": i, "s": s, "log": str(log), "out": str(home / "out")} for i in range(n)],
                          part_seconds=0.1)


def _stage_results(home):
    found = []
    for part in sorted((home / "stages" / "all" / "results").glob("part-*.jsonl")):
        with open(part, "rb") as file:
            found += read_part(file)
    return found


def _committed_parts(home):
    """Every part id the containers there hold, repeats kept, and the files in its folder."""
    from ms_flow.core.data import container

    ids = []
    for path in sorted((home / "out").glob("*.arrows")):
        ids += [row["part"] for row in container.read_index(path)[1] if not row["retired"]]
    return sorted(ids), sorted(p.name for p in (home / "out").iterdir() if p.is_file())


def test_walltime_kill_loses_at_most_the_open_part(tmp_path):
    home, log = tmp_path / "job", tmp_path / "log"
    _committing(home, 40, 0.4, log=log)
    first = _runner(home)
    deadline = time.monotonic() + 60
    while len(_stage_results(home)) < 10 and time.monotonic() < deadline:
        time.sleep(0.02)
    first.send_signal(signal.SIGKILL)  # the queue's walltime, at its worst: no time to tidy
    first.wait(60)
    time.sleep(1)  # what its workers were at, they may still finish
    started = {int(line) for line in log.read_text().split()}
    written = {index for index, _ in _stage_results(home)}
    if (home / "stages" / "all" / "results" / "open.tmp").exists():
        with open(home / "stages" / "all" / "results" / "open.tmp", "rb") as file:
            written |= {index for index, _ in read_part(file)}
    assert _runner(home).wait(120) == 0
    runs = Counter(int(line) for line in log.read_text().split())
    again = {n for n, times in runs.items() if times > 1}
    assert again <= started - written, f"ran again {again}, open at the kill {started - written}"
    assert max(runs.values()) <= 2 and len(again) < 40 - len(written)
    ids, names = _committed_parts(home)
    assert ids == sorted(f"p:{i}" for i in range(40)), "lost or doubled"
    assert len([name for name in names if name.endswith(".arrows")]) == 1  # one container, not one per task
    assert not list((home / "out" / "parts").iterdir())  # committed parts go
    results = dict(_stage_results(home))
    assert sorted(results) == list(range(40))
    assert {entry["path"] for result in results.values() for entry in result["_committed"]} == {
        str(home / "out" / name) for name in names if name.endswith(".arrows")}


def test_a_new_job_skips_receipted_parts(tmp_path):
    """A part committed there before its result was noted (the kill came between the two): the
    task that runs again leaves a part that is not committed twice, and goes."""
    from ms_flow.runner import commit_there

    home, log = tmp_path / "job", tmp_path / "log"
    _committing(home, 6, 0, log=log)
    early = workers.part_of({"n": 3, "log": str(tmp_path / "early"), "out": str(home / "out")})
    (before,) = commit_there(home, workers.receipts_of(early))
    assert _runner(home).wait(120) == 0
    ids, _names = _committed_parts(home)
    assert ids == sorted(f"p:{i}" for i in range(6))
    assert not list((home / "out" / "parts").iterdir())
    assert dict(_stage_results(home))[3]["_committed"] == [before]


def test_containers_there_roll_at_their_bound(tmp_path, monkeypatch):
    """The containers there have the bound of the ones here (I18): past it, the next part opens another."""
    from ms_flow.core.data import catalog
    from ms_flow.runner import commit_there

    monkeypatch.setattr(catalog, "MAX_SHARDS", 4)
    home = tmp_path / "job"
    made = [workers.part_of({"n": n, "log": str(tmp_path / "log"), "out": str(home / "out")}) for n in range(10)]
    where = commit_there(home, [receipt for result in made for receipt in workers.receipts_of(result)])
    containers = {}
    for entry in where:
        containers.setdefault(entry["path"], []).extend(entry["shards"])
    assert sorted(len(shards) for shards in containers.values()) == [2, 4, 4]
    ids, names = _committed_parts(home)
    assert ids == sorted(f"p:{n}" for n in range(10))
    assert len([name for name in names if name.endswith(".arrows")]) == 3


def test_a_kept_folder_has_its_own_catalog_and_the_next_campaign_finds_its_parts(tmp_path):
    """What a campaign keeps for the next ones (a folder of the project there) is committed over a
    catalog of that folder's own: a later campaign finds a part in it by its id, and committing it
    again does not add it twice."""
    from ms_flow.runner import commit_there, found_there

    kept, log = tmp_path / "projects" / "u" / "made", str(tmp_path / "log")
    first, second = tmp_path / "c1", tmp_path / "c2"
    made = workers.part_of({"n": 1, "log": log, "out": str(first / "out")})
    (where,) = commit_there(first, [{**workers.receipts_of(made)[0], "folder": str(kept)}])
    assert found_there(kept, "p:1") == where and found_there(kept, "p:2") is None
    again = workers.part_of({"n": 1, "log": log, "out": str(second / "out")})
    assert commit_there(second, [{**workers.receipts_of(again)[0], "folder": str(kept)}]) == [where]
    assert not list((second / "out" / "parts").iterdir())
    names = sorted(path.name for path in kept.iterdir() if not path.name.endswith(".lock"))
    assert sorted(name.rsplit(".", 1)[-1] for name in names) == ["arrows", "db", "index"], names


def test_a_runner_on_ray_tells_how_its_cluster_is():
    """What the runner says of the Ray it drives, with every progress: no ssh to a node needed."""
    ray = pytest.importorskip("ray")
    from ms_flow.runner import ray_state

    ray.init(num_cpus=2, include_dashboard=False, log_to_driver=False)
    try:
        told = ray_state(nodes_asked=1)
    finally:
        ray.shutdown()
    assert (told["nodes"], told["nodes_asked"], told["cpus"]) == (1, 1, 2)
    assert 0 <= told["cpus_busy"] <= 2 and told["store_mb"] > 0 and 0 <= told["store_used_mb"] <= told["store_mb"]
