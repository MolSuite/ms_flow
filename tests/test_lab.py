"""Campaigns on a real Slurm, with the faults of an HPC: ssh that drops, a job cancelled or killed, a
scratch purged, two clients, two jobs on one node, Ray over two nodes.

Runs only with `MS_FLOW_LAB=<the lab/ dir of the vHPC lab>` (a clone of github.com/exactlab/vhpc
with a `lab/` of its own: `setup.sh`, `up.sh`, `install-env.sh`): its login answers on
127.0.0.1:2222 with the key in `runtime/`, `workspace/` is its `/workspace` and `/opt/venv` has
pydantic and Ray. This checkout's ms_flow and ms_env are copied there first. Some 9 minutes: `pytest -m lab`.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import pytest

LAB = os.environ.get("MS_FLOW_LAB")
pytestmark = [pytest.mark.lab, pytest.mark.skipif(not LAB, reason="no vHPC lab: set MS_FLOW_LAB")]

from ms_env import Host, HostError  # noqa: E402
from ms_flow.engine import Engine, Flow, ThreadExecutor  # noqa: E402
from ms_flow.remote import TEMPLATES, RemoteExecutor  # noqa: E402

import lab_tasks  # noqa: E402

HERE = Path(__file__).resolve().parent
ADDR, THERE = "user@127.0.0.1", "/workspace/ms_flow_lab"
SSH = ("ssh", "-p", "2222", "-i", f"{LAB}/runtime/id_ed25519", "-o", "BatchMode=yes",
       "-o", f"UserKnownHostsFile={LAB}/runtime/known_hosts", "-o", "StrictHostKeyChecking=yes")
FAR = f"{THERE}/far"


def lab(command):
    """What an admin or the cluster does there, not the client."""
    return subprocess.run([*SSH, ADDR, command], capture_output=True, text=True).stdout.strip()


@pytest.fixture(scope="module", autouse=True)
def code_there():
    import ms_env
    import ms_flow
    here = Path(LAB, "workspace", "ms_flow_lab")
    for name, package in (("flow", ms_flow), ("env", ms_env)):
        shutil.rmtree(here / name / package.__name__, ignore_errors=True)
        shutil.copytree(Path(package.__file__).parent, here / name / package.__name__,
                        ignore=shutil.ignore_patterns("__pycache__"))
    (here / "tasks").mkdir(exist_ok=True)
    shutil.copy(HERE / "lab_tasks.py", here / "tasks")


@pytest.fixture(autouse=True)
def said(tmp_path, monkeypatch):
    (tmp_path / "said.txt").write_text("from here")
    monkeypatch.setenv("CAMPAIGN_FILE", str(tmp_path / "said.txt"))
    lab_tasks.GOT.clear()
    return tmp_path


class Faulty(Host):
    """The lab's ssh, doing `action` on the `nth` call whose command has `pattern`: "fail" before
    it runs there, "ack" after (it ran, this side never heard back), or call it first."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rules, self.down_until = [], 0.0

    def fault(self, pattern, nth=1, action="fail"):
        self.rules.append([pattern, nth, action, 0])

    def run(self, command, data=b"", timeout=600):
        if time.monotonic() < self.down_until:
            raise HostError("ssh: connect to host 127.0.0.1 port 2222: Connection timed out")
        hit = None
        for rule in self.rules:
            if rule[0] in command:
                rule[3] += 1
                hit = hit or (rule[2] if rule[3] == rule[1] else None)
        if hit == "fail":
            raise HostError("ssh: connect to host 127.0.0.1 port 2222: Connection refused")
        if callable(hit):
            hit()
        out = super().run(command, data, timeout)
        if hit == "ack":
            raise HostError("ssh: Connection closed by 127.0.0.1 port 2222")
        return out


def far(setup="", **resources):
    """The lab's queue, asking for `resources` (one node with two CPUs by default) as a queue executor's fields do."""
    ex = RemoteExecutor(ADDR, workdir=FAR, scheduler="slurm", template=TEMPLATES["slurm"], env="/opt/venv",
                        poll_s=2, part_seconds=1, keep_after=7,
                        asks=resources or {"nodes": 1, "cpus": 2},
                        setup=f"#SBATCH --no-requeue\n{setup}\nexport PYTHONPATH={THERE}/flow:{THERE}/env:{THERE}/tasks")
    ex.remote = Faulty(ADDR, ssh=SSH)
    return ex


def lab_engine(tmp, ex):
    engine = Engine(cpus=4)
    engine.register("thread", ThreadExecutor(4))  # where a campaign follows its job
    engine.register("far", ex)
    engine.attach("j", Path(tmp) / "j.db")
    return engine


def run(params, tmp, ex=None, seconds=600, during=None, **options):
    with lab_engine(tmp, ex or far()) as engine:
        job = Flow(engine, None).submit(lab_tasks.screen, params, executor="far", journal="j", **options)
        if during:
            threading.Thread(target=during, args=(engine, job), daemon=True).start()
        return engine.wait(job, seconds)


def got():
    return sorted(r["n"] for r in lab_tasks.GOT)


def until(check, seconds=300):
    end = time.monotonic() + seconds
    while not check() and time.monotonic() < end:
        time.sleep(1)
    return check()


def params(request, **more):
    return {"n": 8, "nap": 2, "tag": f"{request.node.name}-{time.time()}", **more}


def test_a_campaign_runs_there(request, said):
    assert run(params(request), said).status == "completed"
    assert got() == list(range(8))
    assert {r["said"] for r in lab_tasks.GOT} == {"from here"}


def test_a_campaign_asks_the_queue_for_itself(request, said):
    ex = far()
    assert run(params(request), said, ex, resources={"cpus": 1, "time": "0:17:00"}).status == "completed"
    asked = ex.remote.run(f"grep -l -- --time=0:17:00 {FAR}/c*/job.sh | xargs grep -c -- --cpus-per-task=1").decode()
    assert asked.split() == ["1"]


@pytest.mark.parametrize("cpus", [1, 2])
def test_any_job_with_a_key_asks_the_queue_for_itself(said, cpus):
    ex, got = far(), []  # the executor asks for 2 CPUs
    with lab_engine(said, ex) as engine:
        key = f"granted-{cpus}-{time.time()}"
        ex.ask(key, {"cpus": cpus})
        job = engine.submit("far", lab_tasks.granted, range(2), key=key, journal="j", on_results=got.extend)
        assert engine.wait(job, 600).status == "completed"
    assert {result["cpus"] for _, result in got} == {str(cpus)}  # what Slurm gave, read on the node


@pytest.mark.parametrize("pattern, nth, action", [
    ("tar xf -", 1, "fail"), ("sha256sum -c", 1, "fail"), ("sbatch", 1, "fail"), (".poll.", 2, "fail"),
    (".poll.", 6, "fail"), ("tar xf -", 1, "ack"), ("sbatch", 1, "ack")])
def test_one_ssh_call_that_fails_does_not_fail_the_campaign(request, said, pattern, nth, action):
    ex = far()
    ex.remote.fault(pattern, nth, action)
    assert run(params(request), said, ex).status == "completed"
    assert got() == list(range(8))


def test_a_campaign_waits_out_a_network_that_goes_and_says_so(request, said):
    seen = []

    def cut(engine, job):
        until(lambda: lab_tasks.GOT)
        ex.remote.down_until = time.monotonic() + 50
        until(lambda: engine.status(job).unreachable, 60)
        seen.append(engine.status(job).unreachable)

    ex = far()
    assert run(params(request, n=20), said, ex, during=cut).status == "completed"
    assert got() == list(range(20))
    assert seen and "has not answered since" in seen[0]


def test_a_job_cancelled_there_fails_and_goes_again_when_sent_again(request, said):
    def cancel(engine, job):
        until(lambda: lab_tasks.GOT)
        lab(f"scancel $(cat $(ls -td {FAR}/c*-* | head -1)/queue.id)")

    p = params(request, n=20)
    assert run(p, said, during=cancel).status == "failed"
    lab_tasks.GOT.clear()
    assert run(p, said).status == "completed"
    assert got() == list(range(20))


def test_a_runner_killed_there_is_resumed_from_its_journal(request, said):
    def kill(engine, job):
        until(lambda: len(lab_tasks.GOT) >= 2)
        lab("for h in slurm-worker1 slurm-worker2; do ssh -o StrictHostKeyChecking=no $h "
            "pkill -9 -f ms_flow.runner; done")

    assert run(params(request, n=20), said, during=kill).status == "completed"
    assert set(got()) == set(range(20))


def test_a_campaign_whose_directory_is_purged_mid_run_goes_again(request, said):
    def purge(engine, job):
        until(lambda: len(lab_tasks.GOT) >= 2)
        lab(f"d=$(ls -td {FAR}/c*-* | head -1); scancel $(cat $d/queue.id); rm -rf $d")

    assert run(params(request, n=20), said, during=purge).status == "completed"
    assert set(got()) == set(range(20))


def test_a_campaign_whose_results_were_purged_after_its_end_goes_again(request, said):
    p = params(request)
    assert run(p, said).status == "completed"
    d = lab(f"ls -td {FAR}/c*-* | head -1")
    lab(f"rm -rf {d}/files {d}/stages/*/results")
    lab_tasks.GOT.clear()
    assert run({**p, "run_id": "again"}, said, seconds=300).status == "completed"
    assert got() == list(range(8))


def test_a_file_spoiled_there_is_sent_again(request, said):
    """The files of a job that never got queued are spoiled there (a half transfer, a bad disk)
    before it is sent again."""
    ex, p = far(), params(request)

    def gone():
        ex.remote.down_until = float("inf")
        raise HostError("ssh: connect to host 127.0.0.1 port 2222: Connection refused")

    ex.remote.fault("sbatch", 1, gone)
    with lab_engine(said, ex) as engine:
        Flow(engine, None).submit(lab_tasks.screen, p, executor="far", journal="j")
        until(lambda: ex.remote.down_until, 120)
    lab(f"for f in {FAR}/c*-*/files/*/said.txt; do echo garbage > $f; done")
    assert run(p, said).status == "completed"
    assert {r["said"] for r in lab_tasks.GOT} == {"from here"}


def test_a_campaign_closed_mid_run_is_taken_up_again(request, said):
    p = params(request, n=20)
    with lab_engine(said, far()) as engine:
        Flow(engine, None).submit(lab_tasks.screen, p, executor="far", journal="j")
        until(lambda: len(lab_tasks.GOT) >= 3)
    time.sleep(10)
    assert run(p, said).status == "completed"
    assert set(got()) == set(range(20))


def test_two_clients_of_one_campaign_queue_one_job(request, said):
    tag, dirs = params(request)["tag"], lab(f"ls -d {FAR}/c*-* 2>/dev/null | wc -l")
    clients = [subprocess.Popen([sys.executable, __file__, tag, str(said)], stdout=subprocess.PIPE, text=True,
                                env={**os.environ, "PYTHONHASHSEED": str(seed)}) for seed in (1, 2)]
    for client in clients:
        out = json.loads(client.communicate(timeout=600)[0].strip().splitlines()[-1])
        assert out["status"] == "completed", out
        assert out["got"] == list(range(8)) or out["got"] == [], out
    assert int(lab(f"ls -d {FAR}/c*-* | wc -l")) == int(dirs or 0) + 1  # one campaign, whatever its hash seed


@pytest.mark.parametrize("jobs, setup, nodes", [
    (2, "#SBATCH --nodelist=slurm-worker1", 0),  # two allocations on one node
    (1, "", 2),                                  # Ray over two nodes
    (2, "", 2)])                                 # two of those at once
def test_jobs_that_share_nodes_do_not_meet(request, said, jobs, setup, nodes):
    snaps = [None] * jobs

    def go(i):
        snaps[i] = run(params(request, tag=f"{request.node.name}-{time.time()}-{i}", nap=3), tempfile.mkdtemp(),
                       far(setup, nodes=nodes, cpus=1), seconds=400)

    threads = [threading.Thread(target=go, args=(i,)) for i in range(jobs)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert [s.status for s in snaps] == ["completed"] * jobs, [s.error for s in snaps]
    time.sleep(5)
    assert lab("for h in slurm-worker1 slurm-worker2; do ssh -o StrictHostKeyChecking=no $h "
               "'pgrep -fc /tmp/ray_' 2>/dev/null || echo 0; done").split() == ["0", "0"]


def test_the_files_of_a_project_go_once_to_its_folder_there(request, said, monkeypatch):
    """With rsync over there or, without it, tar; a second campaign sends nothing."""
    from ms_flow import locations
    from ms_flow.core.database.project import ProjectStore

    project = ProjectStore()
    project.connect(said / "p")
    (said / "p" / "said.txt").write_text("from here")
    monkeypatch.setenv("CAMPAIGN_FILE", str(said / "p" / "said.txt"))
    ex, there = far(), None
    with lab_engine(said, ex) as engine:
        flow = Flow(engine, project)
        there = locations.root(ex, project)
        ex.remote.fault(f"mkdir -p {there}", 10 ** 9)  # never fails: counts the sendings to the project's folder
        assert engine.wait(flow.submit(lab_tasks.screen, params(request), executor="far"), 600).status == "completed"
        assert {r["said"] for r in lab_tasks.GOT} == {"from here"}
        assert getattr(ex, "_no_rsync", False) == (lab("command -v rsync || :") == "")
        assert lab(f"cat {there}/said.txt") == "from here"
        sendings = ex.remote.rules[0][3]  # 2 without rsync there: the try, and tar
        assert engine.wait(flow.submit(lab_tasks.screen, params(request), executor="far"), 600).status == "completed"
        assert ex.remote.rules[0][3] == sendings  # another campaign found it there
        lab(f"rm {there}/said.txt")
        assert locations.reconcile(project, ex) == ["said.txt"]


if __name__ == "__main__":  # one client of test_two_clients_of_one_campaign_queue_one_job
    os.environ["CAMPAIGN_FILE"] = str(Path(sys.argv[2]) / "said.txt")
    snap = run({"n": 8, "nap": 2, "tag": sys.argv[1]}, tempfile.mkdtemp())
    print(json.dumps({"status": snap.status, "error": snap.error, "got": got()}))
