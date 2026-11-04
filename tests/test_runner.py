"""`ms_flow.runner`: a staged directory run to its end, and one stopped and run again."""
import json
import os
import signal
import subprocess
import sys
import time

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
    group = PayloadGroup(kind="sdf")
    rows = unplain(json.loads(json.dumps(plain([{"pose": group.add(b"one")}, {"pose": group.add(b"two")}], "rows"))))
    (whole, sizes), = {id(row["pose"].group): row["pose"].group.packed() for row in rows}.values()
    assert [row["pose"].index for row in rows] == [0, 1] and sizes == [3, 3] and whole.raw_length == 6
    assert unplain(json.loads(json.dumps(plain({7: "a", None: [1]}, "keys")))) == {7: "a", None: [1]}


def test_a_job_staged_with_pickles_is_refused_with_why(tmp_path):
    (tmp_path / "spec.json").write_text(json.dumps({"fn": "f", "total": 1, "python": "3.12"}))
    assert _runner(tmp_path).wait(60) == 1
    assert "staged by another ms_flow" in _progress(tmp_path)["error"]
