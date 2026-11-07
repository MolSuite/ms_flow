"""`RemoteExecutor` against this machine: the same commands, run by `sh` here instead of over ssh."""
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from benchmarks.refactor_optimization import workers
from ms_flow.engine import Dir, Engine, File
from ms_flow.remote import RemoteExecutor

pytestmark = pytest.mark.slow

_ROOT = Path(__file__).resolve().parents[1]


def _far(tmp_path, **options):
    return RemoteExecutor(workdir=str(tmp_path / "far"), python=sys.executable,
                          setup=f"export PYTHONPATH={_ROOT}:{_ROOT / 'src'}", poll_s=0.2, part_seconds=0.1, **options)


def _inside(folder) -> list:
    """The processes whose directory is `folder`: a job of the queue and its workers."""
    found = []
    for entry in Path("/proc").iterdir():
        try:
            if entry.name.isdigit() and os.readlink(entry / "cwd").startswith(str(folder)):
                found.append(int(entry.name))
        except OSError:
            pass
    return found


def _until(check, seconds=60):
    deadline = time.monotonic() + seconds
    while not check() and time.monotonic() < deadline:
        time.sleep(0.05)
    return check()


def test_a_job_is_run_as_jobs_of_the_other_machine(tmp_path):
    got, errors = [], []
    with Engine(cpus=2) as engine:
        engine.register("far", _far(tmp_path, elements_per_job=10, runner="--cpus 2"))
        job = engine.submit("far", workers.die_on_7, [{"idx": i} for i in range(25)], on_results=got.extend,
                            on_error=lambda key, error: errors.append(error))
        ended = engine.wait(job, 120)
    assert (ended.status, ended.chunks_done, ended.chunks_failed) == ("completed_with_errors", 24, 1)
    assert len(got) == 24 and all(result == {"ok": 1} for key, result in got)
    assert len(errors) == 1 and "worker process died" in errors[0]
    assert list((tmp_path / "far").iterdir()) == []  # what ended well leaves nothing there


def test_results_come_while_it_runs_and_a_job_out_of_the_queue_is_sent_again(tmp_path):
    got = []
    with Engine(cpus=2) as engine:
        engine.register("far", _far(tmp_path, runner="--cpus 1"))
        job = engine.submit("far", workers.nap, [{"s": 0.3, "n": i} for i in range(12)], on_results=got.extend)
        assert _until(lambda: engine.status(job).chunks_done)
        assert engine.status(job).status == "running" and engine.status(job).chunks_done < 12
        (home,) = (tmp_path / "far").iterdir()
        for pid in _inside(home):  # what a queue does to a job whose time ran out
            os.kill(pid, signal.SIGTERM)
        ended = engine.wait(job, 120)
    assert (ended.status, ended.chunks_done, len(got)) == ("completed", 12, 12)


def test_a_canceled_job_leaves_the_queue_and_nothing_behind(tmp_path):
    with Engine(cpus=2) as engine:
        engine.register("far", _far(tmp_path, runner="--cpus 1"))
        job = engine.submit("far", workers.nap, [{"s": 0.3, "n": i} for i in range(40)])
        assert _until(lambda: engine.status(job).chunks_done)
        (home,) = (tmp_path / "far").iterdir()
        assert _inside(home)
        engine.cancel(job)
        assert engine.wait(job, 60).status == "canceled"
        assert _until(lambda: not _inside(home) and not home.exists())


def test_a_file_an_element_names_goes_with_the_job_once(tmp_path):
    (tmp_path / "receptor.txt").write_text("here")
    got, far = [], _far(tmp_path, shared=False)
    with Engine(cpus=2) as engine:
        engine.register("far", far)
        job = engine.submit("far", workers.read, [{"path": File(tmp_path / "receptor.txt"), "n": i} for i in range(6)],
                            on_results=got.extend)
        assert engine.wait(job, 120).status == "completed"
    seen = {result["path"] for key, result in got}
    assert len(got) == 6 and all(result["text"] == "here" for key, result in got)
    assert len(seen) == 1 and seen.pop().startswith(str(tmp_path / "far")), "one copy, read where the job ran"


def test_what_a_result_names_in_a_dir_comes_back_to_it(tmp_path):
    got, poses = [], tmp_path / "poses"
    with Engine(cpus=2) as engine:
        engine.register("far", _far(tmp_path, shared=False, elements_per_job=4))
        job = engine.submit("far", workers.leave, [{"out": Dir(poses), "n": i} for i in range(7)], on_results=got.extend)
        assert engine.wait(job, 120).status == "completed"
    assert sorted(result["made"] for key, result in got) == [str(poses / f"{i}.txt") for i in range(7)]
    assert [(poses / f"{i}.txt").read_text() for i in range(7)] == [str(i) for i in range(7)]
    assert list((tmp_path / "far").iterdir()) == []


def test_a_job_that_cannot_run_there_fails_with_what_it_said(tmp_path):
    with Engine(cpus=2) as engine:
        engine.register("far", RemoteExecutor(workdir=str(tmp_path / "far"), python=sys.executable, poll_s=0.2,
                                              setup="export PYTHONPATH=/nowhere"))
        ended = engine.wait(engine.submit("far", workers.nap, [{"s": 0, "n": i} for i in range(3)]), 60)
    assert ended.status in ("failed", "completed_with_errors") and ended.chunks_done == 0
    assert len(list((tmp_path / "far").iterdir())) == 1  # kept, to look at its runner.log
    assert "No module named" in ended.error and "--- runner.log" in ended.error, ended.error  # its last lines


def _napping(engine, tmp_path, got, **options):
    engine.register("far", _far(tmp_path, runner="--cpus 1", **options))
    engine.attach("journal", tmp_path / "journal.db")
    return engine.submit("far", workers.nap, [{"s": 0.3, "n": i} for i in range(12)], journal="journal", key="naps",
                         on_results=got.extend)


def test_closing_leaves_a_job_with_a_key_running_and_it_is_taken_up_again(tmp_path):
    got = []
    with Engine(cpus=2) as engine:
        job = _napping(engine, tmp_path, got)
        assert _until(lambda: engine.status(job).chunks_done)
    before = len(got)
    (home,) = (tmp_path / "far").iterdir()
    assert 0 < before < 12 and _inside(home), "still running there, with the engine gone"

    with Engine(cpus=2) as engine:  # in other tasks than before: each waits for what is already there
        job = _napping(engine, tmp_path, got, elements_per_job=4, max_jobs=1)
        there = set()
        while engine.status(job).status == "running":
            there.update(tmp_path.glob("far/*-*"))
            time.sleep(0.02)
        assert there == {home}, "nothing was sent again"
        ended = engine.wait(job, 120)
        assert (ended.status, ended.chunks_done) == ("completed", 12)
        assert len({key for key, result in got}) == len(got) == 12
        assert _until(lambda: not home.exists())  # the engine said it ended


def test_what_was_left_there_is_listed_and_removed_by_hand(tmp_path):
    with Engine(cpus=2) as engine:
        job = _napping(engine, tmp_path, [])
        assert _until(lambda: engine.status(job).chunks_done)
        (home,) = (tmp_path / "far").iterdir()
        (row,) = engine.executor("far").jobs_there()
        assert (row["dir"], row["followed"]) == (home.name, True)
        with pytest.raises(ValueError, match="cancel the job"):
            engine.executor("far").remove_there(home.name)
    far = _far(tmp_path)  # another run, with no job of its own
    try:
        (row,) = far.jobs_there()
        assert (row["dir"], row["key"], row["total"], row["in_queue"], row["followed"]) == (home.name, "naps", 12, True, False)
        assert row["status"] == "running" and 0 < row["done"] < 12 and "nap" in row["fn"]
        with pytest.raises(ValueError, match="not a directory of a job"):
            far.remove_there("../" + home.name)
        far.remove_there(home.name)
        assert _until(lambda: not _inside(home)) and not home.exists() and far.jobs_there() == []
    finally:
        far.shutdown()


def test_canceling_stops_what_an_earlier_run_left_there(tmp_path):
    with Engine(cpus=2) as engine:
        job = _napping(engine, tmp_path, [])
        assert _until(lambda: engine.status(job).chunks_done)
    (home,) = (tmp_path / "far").iterdir()
    assert _inside(home)
    with Engine(cpus=2) as engine:
        job = _napping(engine, tmp_path, [])
        assert _until(lambda: engine.status(job).status == "running")
        engine.cancel(job)
        assert engine.wait(job, 60).status == "canceled"
        assert _until(lambda: not _inside(home) and not home.exists())


def test_an_env_gives_the_python_and_goes_first_on_the_path(tmp_path):
    env, seen = tmp_path / "env", tmp_path / "seen"
    (env / "bin").mkdir(parents=True)
    python = env / "bin" / "python"
    python.write_text(f'#!/bin/sh\necho "$PATH" > {seen}\nexec {sys.executable} "$@"\n')
    python.chmod(0o755)
    with Engine(cpus=2) as engine:
        engine.register("far", RemoteExecutor(workdir=str(tmp_path / "far"), env=str(env), poll_s=0.2, part_seconds=0.1,
                                              setup=f"export PYTHONPATH={_ROOT}:{_ROOT / 'src'}"))
        ended = engine.wait(engine.submit("far", workers.noop, [{"idx": 0}]), 60)
    assert ended.status == "completed"
    assert seen.read_text().startswith(f"{env}/bin:")


def _where(payload: dict) -> str:
    import socket

    time.sleep(0.5)
    return socket.gethostname() + ":" + Path(payload["file"]).read_text()


@pytest.mark.skipif("MS_FLOW_RAY_HOST" not in os.environ, reason="needs another machine: MS_FLOW_RAY_HOST=host:env")
def test_another_machine_joins_the_ray_of_this_one_and_runs_its_share(tmp_path):
    from ray import cloudpickle

    from ms_flow.ray_hosts import RayOverHosts

    cloudpickle.register_pickle_by_value(sys.modules[__name__])  # the other machine does not have this file
    host, env = os.environ["MS_FLOW_RAY_HOST"].split(":", 1)
    (tmp_path / "said").write_text("hello")
    got, errors = [], []
    with Engine(cpus=1) as engine:
        engine.register("ray", RayOverHosts([host], env))
        job = engine.submit("ray", _where, [{"file": File(str(tmp_path / "said"))} for _ in range(60)],
                            on_results=got.extend, on_error=lambda key, error: errors.append(error))
        seen = set()
        while engine.status(job).finished_at is None:
            seen.update(task.host for task in engine.tasks(job))
            time.sleep(0.1)
        assert engine.wait(job, 180).status == "completed", errors[:1]
    assert seen <= {"local", host, None} and host in seen  # None: one not heard of yet
    names = {result.split(":")[0] for key, result in got}
    assert len(names) == 2 and all(result.endswith(":hello") for key, result in got)


def _outage(far, rounds):
    """The other machine does not answer the next `rounds` rounds of asking; `seen` is what the jobs said."""
    from ms_env import HostError
    run, left = far.remote.run, [rounds]

    def flaky(command, data=b"", timeout=600):
        if ".poll." in command and left[0]:
            left[0] -= 1
            raise HostError("ssh: connect to host far port 22: Connection refused")
        return run(command, data, timeout)

    far.remote.run = flaky
    return left


def test_a_job_waits_for_a_host_that_does_not_answer_and_goes_on_once_it_does(tmp_path):
    far = _far(tmp_path)
    left = _outage(far, 4)
    with Engine(cpus=2) as engine:
        engine.register("far", far)
        job = engine.submit("far", workers.nap, [{"s": 0, "n": i} for i in range(3)])
        assert _until(lambda: engine.status(job).unreachable, 30)  # from the third miss on
        assert "Connection refused" in engine.status(job).unreachable
        engine.reconnect(job)  # not the five minutes a fifth miss would wait
        ended = engine.wait(job, 60)
    assert (ended.status, ended.chunks_done, ended.unreachable, left) == ("completed", 3, None, [0])


def test_grid_engine_says_canceled_only_when_qacct_says_who_deleted():
    """What Son of Grid Engine 8.1.9's qacct printed in the vHPC lab, and a `deleted_by` line of the
    Grid Engines that have it, read by the `state` command."""
    from ms_flow.remote import SCHEDULERS

    def state(qacct: str) -> str:
        command = SCHEDULERS["sge"]["state"].format(id=1).rpartition("done; ")[2].replace("qacct -j 1 2>/dev/null", "cat")
        return subprocess.run(["sh", "-c", command], input=qacct, capture_output=True, text=True).stdout.strip()

    assert state("failed       100 : assumedly after job\ndeleted_by   user@sge\nexit_status  137\n") == "CANCELLED"
    # qdel and a runner the system killed: SoGE says both alike, so neither is taken for a cancel
    assert state("failed       100 : assumedly after job\nexit_status  137                  (Killed)\n") == ""
    assert state("failed       0    \nexit_status  0                   \n") == ""
    assert state("failed       0    \nexit_status  1                   \n") == ""
    assert state("") == ""  # deleted while it waited: no record


def test_what_the_queue_says_a_job_used():
    """What sacct/sstat (Slurm 23 of the vHPC lab) and qacct/qstat (SoGE 8.1.9) printed, read by
    `read_usage`: a 2-CPU job that kept one CPU busy for 20 s with 200 MB."""
    from ms_flow.remote import read_usage

    running = read_usage("A|300|00:00:10|00:00:00|2||800M|RUNNING\nA|300.batch|00:00:10|00:00:00|2|||RUNNING\n"
                         "S|300.extern|00:00:00|2688K|1\nS|300.batch|00:00:09|214784K|1\n")
    assert (running["final"], running["wall_s"], running["cpu_s"], running["cpus"], running["mem_mb"]) == (False, 10, 9, 2, 800)
    assert round(running["peak_mem_mb"]) == 210
    ended = read_usage("A|300|00:00:21|00:20.131|2||800M|COMPLETED\nA|300.batch|00:00:21|00:20.129|2|214784K||COMPLETED\n"
                       "A|300.extern|00:00:21|00:00.001|2|2688K||COMPLETED\n")
    assert (ended["final"], ended["wall_s"], ended["cpu_s"], ended["cpus"]) == (True, 21, 20.131, 2)
    assert read_usage("A|7|1-02:00:00|2-00:00:00|4||2Gc|TIMEOUT\n")["mem_mb"] == 8192  # per CPU, before 21.08
    sge = read_usage("U|cpu=00:00:00, mem=0.00000 GB s, io=0.00000 GB, vmem=N/A, maxvmem=N/A\n"
                     "Q|slots        2                   \nQ|failed       0    \nQ|ru_wallclock 20s\n"
                     "Q|cpu          20.363s\nQ|maxvmem      211.738MB\n")
    assert (sge["final"], sge["wall_s"], sge["cpu_s"], sge["cpus"], round(sge["peak_mem_mb"])) == (True, 20, 20.363, 2, 212)
    assert read_usage("sacct: error: Problem talking to the database\n") == read_usage("")  # no accounting there


def test_the_nodes_of_a_queue_are_read_from_sinfo_and_qhost():
    from ms_flow.remote import read_nodes

    slurm = read_nodes("w1|idle|0/4/0/4|2048|0|(null)|(null)|1.70|debug*|\n"
                       "w1|idle|0/4/0/4|2048|0|(null)|(null)|1.70|long|\n"
                       "g1|mixed|8/24/0/32|256000|64000|gpu:a100:4(S:0-1)|gpu:a100:2(IDX:0-1)|N/A|gpu|\n")
    assert [(n["name"], n["cpus_used"], n["cpus"], n["gpus_used"], n["gpus"], n["partitions"]) for n in slurm] == [
        ("w1", 0, 4, 0, 0, ["debug", "long"]), ("g1", 8, 32, 2, 4, ["gpu"])]
    assert slurm[1]["mem_used_gb"] == 62.5 and slurm[1]["load"] is None
    sge = read_nodes("HOSTNAME  ARCH  NCPU NSOC NCOR NTHR  LOAD  MEMTOT  MEMUSE  SWAPTO  SWAPUS\n"
                     "global    -     -    -    -    -     -     -       -       -       -\n"
                     "sge       lx-amd64  16  1  8  16  1.45  47.0G  19.0G  9.3G  1.7G\n"
                     "   all.q  BIP   0/2/4\n"
                     "n2        lx-amd64  16  1  8  16  -  -  -  -  -\n")
    assert [(n["name"], n["state"], n["cpus_used"], n["cpus"], n["mem_used_gb"]) for n in sge] == [
        ("sge", "up", 2, 4, 19.0), ("n2", "down", 0, 16, 0.0)]
