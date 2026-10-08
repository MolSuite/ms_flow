"""The new engine core (`ms_flow.engine`): counters, admission by tokens, worker death, cancel."""
import threading
import time

import pytest

from benchmarks.refactor_optimization import workers
from ms_flow.engine import Engine, ProcessExecutor, ThreadExecutor


def _boom_on_3(payload):
    if payload == 3:
        raise ValueError("three")
    return payload * 2


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


def test_results_errors_and_terminal_state():
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
        assert list(errors) == [3] and terminal == [snap]
        assert engine.wait(engine.submit("thread", _boom_on_3, [])).status == "completed"
        assert engine.wait(engine.submit("thread", _boom_on_3, [3])).status == "failed"


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
        assert engine.tokens.free[0] == 2
        left = engine.submit("thread", workers.nap, [{"s": 0.05}] * 200)
    assert engine.status(left).status == "interrupted"
    with pytest.raises(RuntimeError):
        engine.submit("thread", _boom_on_3, [1])


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
