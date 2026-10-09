"""`ms_flow.runner`: a staged directory run to its end, and one stopped and run again."""
import json
import signal
import subprocess
import sys
import time

import pytest

from benchmarks.refactor_optimization import workers
from ms_flow.runner import read_part, stage

pytestmark = pytest.mark.slow


def _runner(home):
    return subprocess.Popen([sys.executable, "-m", "ms_flow.runner", str(home), "--cpus", "2"])


def _progress(home):
    try:
        return json.loads((home / "progress.json").read_text())
    except OSError:
        return {}


def _indexes(home):
    found = []
    for part in sorted((home / "results").glob("part-*.bin")):
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
