"""The new engine core (`ms_flow.engine`): counters, admission by tokens, feeders, keys, worker death, cancel."""
import os
import queue
import sqlite3
import subprocess
import sys
import threading
import time
from contextlib import closing

import pytest

from benchmarks.refactor_optimization import workers
from ms_flow.engine import Engine, ProcessExecutor, ThreadExecutor, content_key, report


def _boom_on_3(payload):
    if payload == 3:
        raise ValueError("three")
    return payload * 2


def _echo(payload):
    if "fail" in payload:
        raise ValueError("asked to")
    return payload


def _sizes(payloads):
    """A batched function: every payload learns how many travelled with it."""
    if "short" in payloads:
        return []
    if "fail" in payloads:
        raise ValueError("asked to")
    return [len(payloads)] * len(payloads)


def _half_then_held(payload):
    """Says it is halfway and, if held, stays there until the gate file is there."""
    report(0.5)
    open(payload["gate"] + ".said", "w").close()
    while payload["held"] and not os.path.exists(payload["gate"]):
        time.sleep(0.01)
    return payload["idx"]


class _Trickle:
    """An executor that runs nothing: the test plays the remote side and says how each chunk ended."""

    capacity, uses_tokens = 1, False

    def __init__(self):
        self.tasks = queue.Queue()

    def bind(self, on_done):
        self.on_done = on_done

    def bind_parts(self, on_part):
        self.on_part = on_part

    def submit(self, fn, payload, tag):
        self.tasks.put(([payload], tag))

    def submit_many(self, fn, payloads, tag, whole=False):
        self.tasks.put((payloads, tag))

    def cancel(self, handle):
        pass

    def shutdown(self):
        pass


class _Gauge:
    """A chunk function that records how many of itself ran at once."""

    def __init__(self):
        self.now = self.peak = 0
        self.lock = threading.Lock()

    def __call__(self, payload):
        with self.lock:
            self.now += 1
            self.peak = max(self.peak, self.now)
        time.sleep(0.02)
        with self.lock:
            self.now -= 1


def test_results_errors_and_terminal_state(monkeypatch):
    results, errors, terminal = {}, {}, []
    with Engine(cpus=4, mem_mb=0) as engine:
        engine.register("thread", ThreadExecutor(4))
        job = engine.submit("thread", _boom_on_3, range(500), on_result=results.__setitem__,
                            on_error=errors.__setitem__, on_terminal=terminal.append)
        snap = engine.wait(job, 30)
        assert (snap.status, snap.chunks_total, snap.chunks_done, snap.chunks_failed) == \
            ("completed_with_errors", 500, 499, 1)
        assert snap.chunks_inflight == 0 and "ValueError: three" in snap.error
        assert len(results) == 499 and sorted(results.values())[:3] == [0, 2, 4]
        assert list(errors) == [content_key(3)] and terminal == [snap]
        assert engine.wait(engine.submit("thread", _boom_on_3, [])).status == "completed"
        assert engine.wait(engine.submit("thread", _boom_on_3, [3])).status == "failed"
        # an ended job is let go of; its last snapshot answers for it, and for the jobs that come after it
        assert not engine._jobs and engine.status(job) == snap and len(engine.jobs()) == 3
        assert engine.wait(engine.submit("thread", _boom_on_3, [1], after=[job]), 30).status == "completed"
        monkeypatch.setattr("ms_flow.engine.engine._REMEMBERED", 2)
        engine.wait(engine.submit("thread", _boom_on_3, [1]), 30)
        with pytest.raises(KeyError):
            engine.status(job)


def test_failing_source_and_failing_callback():
    def source():
        yield 1
        raise RuntimeError("no more rows")

    with Engine(cpus=2, mem_mb=0) as engine:
        engine.register("thread", ThreadExecutor(2))
        snap = engine.wait(engine.submit("thread", _boom_on_3, source()), 30)
        assert snap.status == "failed" and snap.chunks_done == 1 and "no more rows" in snap.error
        snap = engine.wait(engine.submit("thread", _boom_on_3, [1, 2], on_result=lambda k, r: 1 / 0), 30)
        assert snap.status == "failed" and snap.chunks_failed == 2 and "ZeroDivisionError" in snap.error


def test_results_are_taken_in_groups_and_done_only_once_taken():
    written, sizes, errors, go = {}, [], {}, threading.Event()

    def write(pairs):  # all or nothing, as a transaction is
        sizes.append(len(pairs))
        if len(sizes) == 1:
            go.wait(10)  # a slow first write: what ends meanwhile queues up behind it
        if any(result == 8 for _, result in pairs):
            raise ValueError("cannot store 8")
        written.update(pairs)

    with Engine(cpus=4, mem_mb=0) as engine:
        engine.register("thread", ThreadExecutor(4))
        with pytest.raises(ValueError):
            engine.submit("thread", _boom_on_3, [1], on_result=print, on_results=write)
        job = engine.submit("thread", _boom_on_3, range(200), window=200, on_results=write,
                            on_error=errors.__setitem__)
        while engine.status(job).chunks_inflight < 199:  # 3 raised; the rest ended or is being written
            time.sleep(0.01)
        time.sleep(0.1)
        assert engine.status(job).chunks_done == 0  # ended is not done: nothing has been taken yet
        go.set()
        snap = engine.wait(job, 30)
        assert (snap.status, snap.chunks_done, snap.chunks_failed) == ("completed_with_errors", 198, 2)
        assert max(sizes) > 100  # one call for what had queued up, then one each to find the bad one
        assert len(written) == 198 and content_key(4) not in written
        assert "cannot store 8" in errors[content_key(4)] and "three" in errors[content_key(3)]


@pytest.mark.parametrize("cost, expected", [
    ({"cpu": 2}, 4),                # 8 CPUs / 2
    ({"mem_mb": 400}, 2),           # 1000 MB / 400
    ({"gpu": 1}, 1),                # one card
    ({"cpu": 1, "max_cpu": 3}, 3),  # the job's own cap
    ({"window": 5}, 5),
])
def test_admission_by_tokens(cost, expected):
    gauge = _Gauge()
    with Engine(cpus=8, gpus=1, mem_mb=1000) as engine:
        engine.register("thread", ThreadExecutor(16, uses_tokens=True))
        snap = engine.wait(engine.submit("thread", gauge, range(60), **cost), 30)
        assert snap.status == "completed" and gauge.peak == expected
        assert engine.tokens.free == [8, 1, 1000]


def test_a_chunk_can_carry_its_own_cost_and_key():
    gauge, seen, errors = _Gauge(), [], {}

    def fn(payload):
        seen.append(payload)
        gauge(payload)

    heavy = [{"n": i, "_cpu_required": 4, "_key": f"heavy-{i}"} for i in range(10)]
    with Engine(cpus=8, mem_mb=1000) as engine:
        engine.register("thread", ThreadExecutor(16, uses_tokens=True))
        snap = engine.wait(engine.submit("thread", fn, heavy + [{"n": 99, "_mem_mb": 5000}],
                                         on_error=errors.__setitem__), 30)
        assert (snap.status, snap.chunks_done, snap.chunks_failed) == ("completed_with_errors", 10, 1)
        assert gauge.peak == 2 and seen[0] == {"n": 0} and heavy[0]["_cpu_required"] == 4
        assert list(errors) == [content_key({"n": 99})] and "can never run" in snap.error
        assert engine.tokens.free == [8, 0, 1000]


def test_content_key_is_the_same_in_another_process():
    payload = {"ligand": "CCO", "receptor": ["1abc", 7], "box": (1.5, 2.0)}
    code = f"from ms_flow.engine import content_key; print(content_key({payload!r}).hex())"
    other = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True,
                           env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)})
    assert other.stdout.strip() == content_key(payload).hex() and len(content_key(payload)) == 16
    assert content_key({"ligand": "CCN"}) != content_key({"ligand": "CCO"})
    assert content_key(threading.Lock()) != content_key(threading.Lock())  # unknown content: never equal


def test_the_source_runs_off_the_loop_and_only_a_window_ahead():
    read, gate = [], threading.Event()

    def slow():
        for i in range(3):
            time.sleep(0.3)
            yield i

    def counted():
        for i in range(1000):
            read.append(i)
            yield i

    with Engine(cpus=4, mem_mb=0) as engine:
        engine.register("thread", ThreadExecutor(4))
        slow_job = engine.submit("thread", _boom_on_3, slow())
        quick = engine.wait(engine.submit("thread", _boom_on_3, range(4, 200)), 30)
        assert quick.finished_at - quick.started_at < 0.25  # a slow source holds only its own job
        assert engine.wait(slow_job, 30).chunks_done == 3

        held = engine.submit("thread", lambda payload: gate.wait(10), counted(), window=5)
        time.sleep(0.2)
        assert len(read) <= 5 + 5 + 1  # in flight, read ahead, and the one waiting for room
        gate.set()
        assert engine.wait(held, 30).chunks_done == 1000


def test_chunk_that_can_never_run_is_refused():
    with Engine(cpus=2, mem_mb=100) as engine:
        engine.register("thread", ThreadExecutor(2, uses_tokens=True))
        with pytest.raises(ValueError):
            engine.submit("thread", _boom_on_3, [1], mem_mb=200)
        with pytest.raises(ValueError):
            engine.submit("nope", _boom_on_3, [1])


def test_cancel_and_close_release_everything():
    with Engine(cpus=2, mem_mb=0) as engine:
        engine.register("thread", ThreadExecutor(2, uses_tokens=True))
        job = engine.submit("thread", workers.nap, [{"s": 0.05}] * 200)
        engine.cancel(job)
        assert engine.wait(job, 10).status == "canceled"
        assert engine.wait(engine.submit("thread", _boom_on_3, range(3)), 10).chunks_done == 3
        for _ in range(100):  # a canceled chunk that was already running keeps its CPU until it ends
            if engine.tokens.free[0] == 2:
                break
            time.sleep(0.01)
        assert engine.tokens.free[0] == 2
        left = engine.submit("thread", workers.nap, [{"s": 0.05}] * 200)
    assert engine.status(left).status == "interrupted"
    with pytest.raises(RuntimeError):
        engine.submit("thread", _boom_on_3, [1])


def test_a_job_waits_for_the_jobs_it_comes_after():
    produced, gate = [], threading.Event()

    def first(payload):
        gate.wait(10)
        return payload

    def from_first():  # read only once the first job is over
        yield from list(produced)

    with Engine(cpus=4, mem_mb=0) as engine:
        engine.register("thread", ThreadExecutor(4))
        a = engine.submit("thread", first, range(20), on_result=lambda key, result: produced.append(result))
        b = engine.submit("thread", _boom_on_3, from_first(), after=[a])
        c = engine.submit("thread", _boom_on_3, [1], after=[b])
        bad = engine.submit("thread", _boom_on_3, [3])
        d = engine.submit("thread", _boom_on_3, [1], after=[a, bad])
        e = engine.submit("thread", _boom_on_3, [1], after=[d])
        assert engine.wait(e, 30).status == "failed" and f"job {d}" in engine.status(e).error
        assert "ended as failed" in engine.status(d).error and engine.status(b).status == "waiting"
        engine.cancel(c)
        assert engine.wait(c, 30).status == "canceled"
        gate.set()
        done = engine.wait(b, 30)
        assert (done.status, done.chunks_done, done.chunks_failed) == ("completed_with_errors", 19, 1)
        with pytest.raises(KeyError):
            engine.submit("thread", _boom_on_3, [1], after=[999])


@pytest.mark.slow
def test_worker_death_costs_one_chunk():
    with Engine(cpus=4, mem_mb=0) as engine:
        engine.register("process", ProcessExecutor(4))
        first = engine.wait(engine.submit("process", workers.die_on_7, [{"idx": i} for i in range(40)]), 120)
        assert (first.status, first.chunks_done, first.chunks_failed) == ("completed_with_errors", 39, 1)
        assert "worker process died" in first.error
        second = engine.wait(engine.submit("process", workers.noop, [{"idx": i} for i in range(20)]), 60)
        assert (second.status, second.chunks_done, second.peak_mem_mb > 0) == ("completed", 20, True)
        assert engine.tokens.free[0] == 4


class _CountingPool(ProcessExecutor):
    tasks = 0

    def submit(self, *args):
        self.tasks += 1
        self.sizes.append(len(args[1]) if len(args) > 3 else 1)  # a fourth argument: a shared task
        return super().submit(*args)


@pytest.mark.slow
def test_short_chunks_share_a_task_and_long_ones_do_not():
    results, errors = {}, {}
    with Engine(cpus=4, mem_mb=0) as engine:
        pool = _CountingPool(4)
        pool.sizes = []
        engine.register("process", pool)
        payloads = [{"idx": i} for i in range(5000)]
        payloads[7]["fail"] = True
        short = engine.wait(engine.submit("process", _echo, payloads, on_result=results.__setitem__,
                                          on_error=errors.__setitem__), 120)
        assert (short.status, short.chunks_done, short.chunks_failed) == ("completed_with_errors", 4999, 1)
        assert list(errors) == [content_key(payloads[7])] and "ValueError" in short.error
        assert len(results) == 4999 and results[content_key(payloads[8])] == {"idx": 8}
        assert pool.tasks < 1000 and engine.tokens.free[0] == 4

        pool.tasks = 0  # a chunk worth a task of its own gets one
        long = engine.wait(engine.submit("process", workers.nap, [{"idx": i, "s": 0.2} for i in range(40)]), 120)
        assert (long.status, long.chunks_done, pool.tasks) == ("completed", 40, 40)

        pool.sizes = []  # the last chunks of a job are shared out among the workers, not packed into one
        tail = engine.wait(engine.submit("process", workers.nap, [{"idx": i, "s": 0.02} for i in range(80)]), 120)
        assert tail.chunks_done == 80 and max(pool.sizes) > 1 and pool.sizes[-3:] == [1, 1, 1]

        # a dead worker costs the chunk that killed it, not the ones that shared its task
        pool.tasks = 0
        payloads = [{"idx": i} for i in range(8, 3008)]
        payloads[2000] = {"idx": 7}
        died = engine.wait(engine.submit("process", workers.die_on_7, payloads), 120)
        assert (died.status, died.chunks_done, died.chunks_failed) == ("completed_with_errors", 2999, 1)
        assert "worker process died" in died.error and pool.tasks < 1500 and engine.tokens.free[0] == 4


@pytest.mark.slow
def test_a_batch_of_a_set_size_and_a_function_that_takes_it_whole():
    sizes, errors = {}, {}
    with Engine(cpus=4, mem_mb=0) as engine:
        engine.register("process", ProcessExecutor(4))
        engine.register("thread", ThreadExecutor(4))
        with pytest.raises(ValueError, match="one chunk to a task"):
            engine.submit("thread", _echo, [1], batch=2)

        def slowly():  # the source trickles in: a task still waits for its ten
            for i in range(95):
                time.sleep(0.001)
                yield i

        snap = engine.wait(engine.submit("process", _sizes, slowly(), batch=10, batched=True,
                                         on_result=sizes.__setitem__), 60)
        assert (snap.status, snap.chunks_done) == ("completed", 95)
        assert sorted(sizes.values()) == [5] * 5 + [10] * 90
        # left to the engine a batched function still gets a list, of one to begin with
        sizes.clear()
        assert engine.wait(engine.submit("process", _sizes, ["a"], batched=True, on_result=sizes.__setitem__), 60).chunks_done == 1
        assert list(sizes.values()) == [1]
        # set size, one payload at a time
        assert engine.wait(engine.submit("process", _echo, [f"p{i}" for i in range(25)], batch=10), 60).chunks_done == 25
        # raising, or answering short, fails the batch it happened in and no other
        payloads = [f"p{i}" for i in range(8)] + ["fail", "p9"] + ["short"] + [f"q{i}" for i in range(9)]
        snap = engine.wait(engine.submit("process", _sizes, payloads + [f"r{i}" for i in range(10)], batch=10, batched=True,
                                         on_error=errors.__setitem__), 60)
        assert (snap.status, snap.chunks_done, snap.chunks_failed) == ("completed_with_errors", 10, 20)
        assert "asked to" in errors[content_key("p9")] and "0 results for 10 payloads" in errors[content_key("q0")]


def test_retries_time_limit_and_cancel_that_kills(tmp_path):
    with Engine(cpus=4, mem_mb=0) as engine:
        engine.register("process", ProcessExecutor(4))
        engine.register("thread", ThreadExecutor(2))
        engine.attach("project", tmp_path / "executor.db")

        # fails twice, works the third time: done, and nobody is told of an error
        errors = []
        flaky = [{"idx": 0, "path": str(tmp_path / "a"), "fails": 2}, {"idx": 1, "path": str(tmp_path / "b"), "fails": 9}]
        snap = engine.wait(engine.submit("process", workers.flaky, flaky, retries=2, retry_wait=0.01, journal="project",
                                         on_error=lambda key, error: errors.append(key)), 60)
        assert (snap.status, snap.chunks_done, snap.chunks_failed, snap.chunks_retried) == \
            ("completed_with_errors", 1, 1, 4)
        assert errors == [content_key(flaky[1])] and (tmp_path / "b").read_text() == "xxx"
        with closing(sqlite3.connect(tmp_path / "executor.db")) as db:
            assert db.execute("SELECT attempt FROM chunks").fetchall() == [(3,)]  # the one that failed for good

        # a chunk that hangs is cut at the limit; the ones that fell with it are not charged
        naps = [{"idx": i, "s": 60 if i == 5 else 0.2} for i in range(12)]
        began = time.perf_counter()
        snap = engine.wait(engine.submit("process", workers.nap, naps, timeout=1.0), 60)
        assert (snap.status, snap.chunks_done, snap.chunks_failed, snap.chunks_retried) == \
            ("completed_with_errors", 11, 1, 0)
        assert "timed out" in snap.error and time.perf_counter() - began < 15

        with pytest.raises(ValueError, match="cannot stop a running chunk"):
            engine.submit("thread", _echo, [{}], timeout=1.0)

        # canceling stops what is running, and a job sharing the executor still ends well
        hung = engine.submit("process", workers.nap, [{"idx": i, "s": 60} for i in range(2)])
        other = engine.submit("process", workers.nap, [{"idx": i, "s": 0.3} for i in range(6)])
        time.sleep(1.0)
        engine.cancel(hung)
        assert engine.wait(hung, 10).status == "canceled"
        assert (engine.wait(other, 30).status, engine.status(other).chunks_done) == ("completed", 6)
        deadline = time.monotonic() + 5
        while engine.tokens.free[0] != 4 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert engine.tokens.free[0] == 4


def test_progress_counts_how_far_the_running_chunks_are(tmp_path):
    def held(n, which, gate):
        return [{"idx": i, "held": which(i), "gate": str(tmp_path / gate)} for i in range(n)]

    def reaches(job_id, progress):
        deadline = time.monotonic() + 30
        while (snap := engine.status(job_id)).progress != progress:
            assert time.monotonic() < deadline, snap
            time.sleep(0.01)
        return snap

    with Engine(cpus=2, mem_mb=0) as engine:
        engine.register("process", ProcessExecutor(2))
        engine.register("thread", ThreadExecutor(2))

        # few chunks: each one is watched, and what it reports counts
        few = engine.submit("process", _half_then_held, held(2, lambda i: True, "few"), total=2)
        assert reaches(few, 0.5).chunks_done == 0
        (tmp_path / "few").touch()
        assert engine.wait(few, 30).progress == 1.0

        # chunks sharing a task: the worker counts the ones it ends, whatever they report
        packed = engine.submit("process", _half_then_held, held(8, lambda i: i % 4 == 3, "packed"), total=8, batch=4)
        assert reaches(packed, 0.875).chunks_done == 0  # three ended and one halfway, in each of the two tasks
        (tmp_path / "packed").touch()
        assert engine.wait(packed, 30).progress == 1.0

        # not watched (many chunks, or an executor that cannot tell): the chunks that ended, no more
        for executor, n in (("process", 6), ("thread", 2)):
            job_id = engine.submit(executor, _half_then_held, held(n, lambda i: True, executor), total=n)
            while not (tmp_path / f"{executor}.said").exists():
                time.sleep(0.01)
            assert engine.status(job_id).progress == 0.0
            (tmp_path / executor).touch()
            assert engine.wait(job_id, 30).progress == 1.0

        # unless the job asks for it
        asked = engine.submit("process", _half_then_held, held(6, lambda i: True, "asked"), total=6, progress=True)
        reaches(asked, 0.5 * 2 / 6)
        (tmp_path / "asked").touch()
        assert engine.wait(asked, 30).progress == 1.0

        # no total until the source ends: nothing to be a part of
        ended = threading.Event()

        def source():
            yield {}
            ended.wait(30)

        unknown = engine.submit("thread", _echo, source())
        while engine.status(unknown).chunks_done != 1:
            time.sleep(0.01)
        assert engine.status(unknown).progress is None
        ended.set()
        assert engine.wait(unknown, 30).progress == 1.0


def test_the_chunks_of_a_task_can_end_ahead_of_it():
    remote, got = _Trickle(), []
    with Engine(cpus=1) as engine:
        engine.register("remote", remote)
        job = engine.submit("remote", _echo, ["a", "b", "c"], batch=3, retries=1, retry_wait=0,
                            on_result=lambda key, result: got.append(result))
        payloads, task = remote.tasks.get(timeout=5)
        assert payloads == ["a", "b", "c"]
        remote.on_part(task, 0, "error", None, "no luck")  # the one the task goes by: it runs again
        remote.on_part(task, 1, "done", "B", None)
        while engine.status(job).chunks_done < 1:  # counted and delivered with its task still running
            time.sleep(0.005)
        assert got == ["B"] and remote.tasks.empty()
        remote.on_done(task, "done", [("never", None), None, ("C", None)], None, 0)
        payloads, again = remote.tasks.get(timeout=5)
        assert payloads == ["a"] and again is not task
        remote.on_done(again, "done", "A", None, 0)
        end = engine.wait(job, timeout=5)
    assert (end.status, end.chunks_done, end.chunks_failed, end.chunks_retried) == ("completed", 3, 0, 1)
    assert sorted(got) == ["A", "B", "C"]


@pytest.mark.slow
def test_the_same_jobs_on_ray(tmp_path):
    pytest.importorskip("ray")
    from ms_flow.engine import RayExecutor

    # Ray's workers import what they run: nothing from this module, which only pytest can find.
    with Engine(cpus=1) as engine:  # a job on Ray spends none of the engine's own CPUs
        engine.register("ray", RayExecutor(num_cpus=4))
        got = []
        job = engine.submit("ray", workers.row, [{"idx": i} for i in range(6)], cpu=2,
                            on_result=lambda key, result: got.append(result["idx"]))
        end = engine.wait(job, 120)
        assert (end.status, end.chunks_done, sorted(got)) == ("completed", 6, list(range(6)))
        once = {"path": str(tmp_path / "tries"), "fails": 1}
        failed = engine.wait(engine.submit("ray", workers.flaky, [once]), 60)
        assert failed.status == "failed" and "not yet" in failed.error
        shared = engine.wait(engine.submit("ray", list, ["a", "b", "c", "d"], batch=2, batched=True), 60)
        assert (shared.status, shared.chunks_done) == ("completed", 4)
        dead = engine.wait(engine.submit("ray", workers.die_on_7, [{"idx": i} for i in range(12)]), 120)
        assert (dead.status, dead.chunks_done, dead.chunks_failed) == ("completed_with_errors", 11, 1)
        assert "worker process died" in dead.error

        # a chunk that hangs is cut at the limit; the ones that fell with it are not charged
        naps = [{"idx": i, "s": 60 if i == 5 else 0.2} for i in range(12)]
        cut = engine.wait(engine.submit("ray", workers.nap, naps, timeout=2.0), 120)
        assert (cut.status, cut.chunks_done, cut.chunks_failed, cut.chunks_retried) == ("completed_with_errors", 11, 1, 0)
        assert "timed out" in cut.error

        # what a running chunk says of itself comes back from the nodes
        held = engine.submit("ray", workers.half_then_held, [{"idx": i, "gate": str(tmp_path / "gate")} for i in range(2)])
        deadline = time.monotonic() + 30
        while (snap := engine.status(held)).progress != 0.5:
            assert time.monotonic() < deadline, snap
            time.sleep(0.05)
        (tmp_path / "gate").touch()
        assert engine.wait(held, 30).progress == 1.0
