"""Ray without a shared disk (M9a), on a real Ray of several nodes on this machine: each node keeps
its files in a temporary directory of its own, as if it were another machine."""
import gc
import os
import threading
import time
from collections import Counter

import pytest

from benchmarks.refactor_optimization import workers
from ms_flow.engine import Dir, Engine, File

ray = pytest.importorskip("ray")
pytestmark = [pytest.mark.slow, pytest.mark.xdist_group("ray_cluster")]

_MiB = 1 << 20


@pytest.fixture(scope="module")
def cluster(tmp_path_factory):
    from ray.cluster_utils import Cluster

    nodes = tmp_path_factory.mktemp("nodes")
    cluster = Cluster(initialize_head=True, head_node_args={"num_cpus": 0})
    cluster.disks = []
    for name in ("a", "b"):
        disk = nodes / name
        disk.mkdir()
        cluster.add_node(num_cpus=2, env_vars={"TMPDIR": str(disk)})
        cluster.disks.append(str(disk))
    cluster.nodes_dir = nodes
    yield cluster
    cluster.shutdown()


def _executor(cluster):
    from ms_flow.engine import RayExecutor

    return RayExecutor(address=cluster.address, shared=False)


def _rss_mib() -> int:
    with open("/proc/self/status") as status:
        return next(int(line.split()[1]) for line in status if line.startswith("VmRSS")) // 1024


def _copies(disk) -> list:
    return [os.path.join(folder, name) for folder, _, names in os.walk(os.path.join(disk, "msflow-files"))
            for name in names]


def test_bytes_arrive_unchanged(cluster, tmp_path):
    (tmp_path / "in.arrows").write_bytes(os.urandom(3 * _MiB + 5))
    (tmp_path / "in.arrows.index").write_bytes(b"")  # empty files go and come back too
    every, top, got = tmp_path / "every", tmp_path / "top", ([], [], [])
    with Engine(cpus=1) as engine:
        engine.register("ray", _executor(cluster))
        jobs = [engine.submit("ray", workers.leave_bytes, [{"out": Dir(every), "size": 5 * _MiB + 3}],
                              on_results=got[0].extend),
                engine.submit("ray", workers.leave_bytes, [{"out": Dir(top), "size": 70 * _MiB, "top_only": True}],
                              on_results=got[1].extend),
                engine.submit("ray", workers.landed, [{"container": File(tmp_path / "in.arrows")}],
                              on_results=got[2].extend)]
        assert [engine.wait(job, 180).status for job in jobs] == ["completed"] * 3
    [(_, made_every)], [(_, made_top)], [(_, read)] = got
    assert made_every["named"] == str(every)
    assert {name: workers._digest(str(every / name)) for name in os.listdir(every)} == made_every["made"]
    assert (every / "empty.bin").stat().st_size == 0
    # Top-N comes back alone: the large part it was written beside stays where it was made.
    assert made_top["named"] == str(top / "top.arrows")
    assert sorted(os.listdir(top)) == ["top.arrows", "top.arrows.index"]
    assert workers._digest(str(top / "top.arrows")) == made_top["made"]["top.arrows"]
    assert (read["data"], read["index"]) == (workers._digest(str(tmp_path / "in.arrows")),
                                             workers._digest(str(tmp_path / "in.arrows.index")))
    assert not [path for path in tmp_path.rglob("*") if ".tmp" in path.name]


def test_inputs_land_once_per_node(cluster, tmp_path, monkeypatch):
    from ms_flow.engine import executors

    monkeypatch.setattr(executors, "_CHUNK", _MiB)  # what this machine sends goes in pieces
    (tmp_path / "in.arrows").write_bytes(os.urandom(3 * _MiB + 17))
    (tmp_path / "in.arrows.index").write_bytes(os.urandom(1000))
    got = []
    with Engine(cpus=1) as engine:
        ray_executor = _executor(cluster)
        engine.register("ray", ray_executor)
        job = engine.submit("ray", workers.landed, [{"container": File(tmp_path / "in.arrows"), "n": i}
                                                    for i in range(16)], batch=1, on_results=got.extend)
        assert engine.wait(job, 180).status == "completed"
        gc.collect()
        assert not ray_executor._carried
        kept = {disk: _copies(disk) for disk in cluster.disks}
    want = workers._digest(str(tmp_path / "in.arrows")), workers._digest(str(tmp_path / "in.arrows.index"))
    assert len(got) == 16 and {(result["data"], result["index"]) for _, result in got} == {want}
    inodes = {}
    for _, result in got:
        inodes.setdefault(result["tmp"], set()).add(result["ino"])
    assert set(inodes) <= set(cluster.disks)
    assert all(len(seen) == 1 for seen in inodes.values()), f"landed more than once on a node: {inodes}"
    assert all(len([path for path in kept[disk] if not path.endswith(".lock")]) == 2 for disk in inodes), kept
    assert not any(_copies(disk) for disk in cluster.disks), "the node copies go at shutdown"


def test_a_2_gib_part_returns_with_bounded_memory(cluster, tmp_path):
    out, got, peak = tmp_path / "out", [], []
    with Engine(cpus=1) as engine:
        engine.register("ray", _executor(cluster))
        warm = engine.submit("ray", workers.leave_bytes, [{"out": Dir(tmp_path / "warm"), "size": 70 * _MiB}])
        assert engine.wait(warm, 180).status == "completed"
        gc.collect()
        before, over = _rss_mib(), threading.Event()

        def watch():
            while not over.is_set():
                peak.append(_rss_mib())
                time.sleep(0.02)

        watcher = threading.Thread(target=watch)
        watcher.start()
        cpu = time.process_time()
        job = engine.submit("ray", workers.leave_bytes, [{"out": Dir(out), "size": 2048 * _MiB}], on_results=got.extend)
        try:
            assert engine.wait(job, 600).status == "completed"
        finally:
            over.set()
            watcher.join()
        cpu = time.process_time() - cpu
    rise = max(peak) - before
    print(f"\n2 GiB part: parent RSS {before} MiB -> peak {max(peak)} MiB (+{rise}), parent CPU {cpu:.2f} s")
    assert (out / "part.arrows").stat().st_size == 2048 * _MiB
    assert workers._digest(str(out / "part.arrows")) == got[0][1]["made"]["part.arrows"]
    assert rise < 256, f"parent RSS rose {rise} MiB"


def test_a_node_lost_before_the_receipt_reruns_only_its_tasks(cluster, tmp_path):
    disk = cluster.nodes_dir / "lost"
    disk.mkdir()
    victim = cluster.add_node(num_cpus=2, env_vars={"TMPDIR": str(disk)})
    log, out, got = tmp_path / "log", tmp_path / "out", []
    payloads = [{"n": i, "log": str(log), "victim": str(disk), "out": Dir(out)} for i in range(6)]
    with Engine(cpus=1) as engine:
        engine.register("ray", _executor(cluster))
        job = engine.submit("ray", workers.held_on, payloads, batch=1, retries=0, on_results=got.extend)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            began = [line.split() for line in log.read_text().splitlines()] if log.exists() else []
            on_victim = {n for n, where in began if where == str(disk)}
            if len(began) >= 6 and on_victim:
                break
            time.sleep(0.2)
        assert on_victim, "no task went to the node that is taken away"
        cluster.remove_node(victim)
        assert engine.wait(job, 300).status == "completed"
    runs = Counter(line.split()[0] for line in log.read_text().splitlines())
    assert {n for n, times in runs.items() if times > 1} == on_victim, runs
    assert all(times <= 2 for times in runs.values()), runs
    assert sorted(path.name for path in out.iterdir()) == [f"{i}.txt" for i in range(6)]


def test_canceled_while_its_part_comes_back_leaves_nothing_half_written(cluster, tmp_path):
    out, again = tmp_path / "out", tmp_path / "again"
    with Engine(cpus=1) as engine:
        engine.register("ray", _executor(cluster))
        job = engine.submit("ray", workers.leave_bytes, [{"out": Dir(out), "size": 1536 * _MiB}])
        deadline = time.monotonic() + 300
        while not (out / "part.arrows.tmp").exists():
            assert time.monotonic() < deadline, "the part never started to come back"
            time.sleep(0.005)
        engine.cancel(job)
        assert engine.wait(job, 60).status == "canceled"
        # the executor is still good for the next one
        nxt = engine.submit("ray", workers.leave_bytes, [{"out": Dir(again), "size": 5 * _MiB}])
        assert engine.wait(nxt, 120).status == "completed"
        deadline = time.monotonic() + 30
        while (out / "part.arrows.tmp").exists() and time.monotonic() < deadline:
            time.sleep(0.05)
    assert not (out / "part.arrows").exists() and not (out / "part.arrows.tmp").exists()
    assert (again / "part.arrows").stat().st_size == 5 * _MiB
