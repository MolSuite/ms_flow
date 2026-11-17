"""Autobatch: task sizes timed give the fixed and per-chunk cost; the size kept follows."""
import heapq
import time

from ms_flow.engine import Engine, ProcessExecutor
from ms_flow.engine.engine import _Autobatch


def _settle(auto, cost, noise=(1.0, 1.0), tasks=200):
    """Feed `auto` tasks that take `cost(n)` seconds until it settles; the size it keeps."""
    for index in range(tasks):
        if auto.settled:
            return auto.size
        n = auto.size
        auto.seen(n, cost(n) * noise[index % 2])
    raise AssertionError("never settled")


def test_a_fixed_cost_per_task_is_spread_to_near_the_most():
    # A program that takes 1 s to start and 10 ms an element: 19·a/b = 1900 gets 95 % of the most.
    assert _settle(_Autobatch(8), lambda n: 1 + 0.01 * n) == 1896


def test_without_a_fixed_cost_the_jobs_size_stays():
    assert _settle(_Autobatch(8, start=64), lambda n: 0.01 * n) == 64


def test_it_measures_one_task_of_the_jobs_size_then_twice_and_three_times_it():
    auto, sizes = _Autobatch(32, start=128), []
    while not auto.settled:
        sizes.append(auto.size)
        auto.seen(auto.size, 1 + 0.01 * auto.size)
    assert sizes == [128, 256, 384] and auto.size == 1888  # 19·a/b = 1900


def test_a_slow_task_of_one_grows_only_as_far_as_the_limit_allows():
    # 60 s a task of one, 50 s of it fixed. Under 130 s: two (120 s in proportion, the safe guess
    # while one size is known), three (80 s along the line), then (130 - 50) / 10 = 8.
    assert _settle(_Autobatch(1, seconds=130), lambda n: 50 + 10 * n) == 8
    assert _settle(_Autobatch(1, seconds=100), lambda n: 50 + 10 * n) == 1  # two might pass it


def test_two_cards_of_different_speed_settle_where_both_get_through_near_the_most():
    """One size for both (as on a Ray cluster), not the slow card's. The cards are the GTX 1660
    SUPER and 1050 Ti of the M0 runs (Uni-Dock): t(n) = a + b·n each."""
    cards = [lambda n: (n + 31) / 11.4, lambda n: (n + 19) / 4.8]  # seconds for a task of n

    def together(size):
        return sum(size / card(size) for card in cards)

    auto, ends = _Autobatch(32, start=128), []
    for index, card in enumerate(cards):  # each card starts a task; the first to end goes first
        heapq.heappush(ends, (card(auto.size), index, auto.size, card(auto.size)))
    while not auto.settled:
        now, index, n, took = heapq.heappop(ends)
        auto.seen(n, took, f"card{index}")
        heapq.heappush(ends, (now + cards[index](auto.size), index, auto.size, cards[index](auto.size)))
    most = 11.4 + 4.8
    assert 256 <= auto.size <= 1024 and together(auto.size) >= 0.93 * most


def test_the_ceiling_and_the_floor_unit():
    assert _settle(_Autobatch(100, ceiling=1000), lambda n: 1 + 0.0001 * n) == 1000
    assert _settle(_Autobatch(100, ceiling=950), lambda n: 1 + 0.0001 * n) == 900  # in whole floors


def test_a_task_limit_holds_the_size_and_never_stops_a_task():
    # 1 s + 10 ms an element under 6 s: at most 500 elements, 496 in whole floors of 8.
    assert _settle(_Autobatch(8, seconds=6), lambda n: 1 + 0.01 * n) == 496
    auto = _Autobatch(8, seconds=6)
    auto.size = 512
    auto.seen(512, 12.0)  # it ran twice the limit, to the end: the next ones are half as big
    assert (auto.size, auto.settled) == (256, True)


def test_a_lost_task_halves_it_down_to_the_floor():
    auto = _Autobatch(8)
    auto.size = 64
    auto.lost()
    assert (auto.size, auto.settled) == (32, True)
    auto.size = 8
    auto.lost()
    assert auto.size == 8


def test_a_task_tells_what_it_used():
    from ms_flow.engine.executors import _call_whole

    def busy(payloads):
        end = time.perf_counter() + 0.5
        while time.perf_counter() < end:
            pass
        return payloads

    outcomes, used = _call_whole(busy, [1, 2])
    assert outcomes == [(1, None), (2, None)]
    assert used["where"] and used["seconds"] >= 0.5 and 0.5 < used["cpu"] <= 1.5 and used["mb"] > 0


def _costly(payloads):
    """A batched function with a fixed cost per task, like a program that starts."""
    time.sleep(0.05 + 0.0002 * len(payloads))
    return [len(payloads)] * len(payloads)


def test_the_engine_grows_the_tasks_of_a_job_with_a_floor():
    sizes, fixed = {}, {}
    with Engine(cpus=2, mem_mb=0) as engine:
        engine.register("process", ProcessExecutor(2))
        snap = engine.wait(engine.submit("process", _costly, range(3000), batch=4, batched=True,
                                         on_result=sizes.__setitem__), 120)
        assert (snap.status, snap.chunks_done) == ("completed", 3000)
        assert max(sizes.values()) >= 64
        assert all(size % 4 == 0 for size in sizes.values())  # whole floors, the tail shares too
        # the job says no: every task takes its floor
        engine.wait(engine.submit("process", _costly, range(40), batch=4, batched=True, autobatch=False,
                                  on_result=fixed.__setitem__), 60)
        assert set(fixed.values()) == {4}
        # the job's guess of a good size is where it starts
        start = {}
        engine.wait(engine.submit("process", _costly, range(1000, 1400), batch=4, batched=True, autobatch=40,
                                  on_result=start.__setitem__), 60)
        assert next(iter(start.values())) == 40


def test_the_tail_is_shared_by_the_tasks_that_run_at_once():
    from types import SimpleNamespace
    from ms_flow.engine.engine import _at_once
    ray = SimpleNamespace(impl=SimpleNamespace(capacity=16, total=(16.0, 2.0, 0)))
    assert _at_once(ray, (1, 1, 0)) == 2  # two GPUs, however many CPUs
    assert _at_once(ray, (1, 0, 0)) == 16
    assert _at_once(SimpleNamespace(impl=SimpleNamespace(capacity=4)), (1, 1, 0)) == 4
    assert _at_once(SimpleNamespace(impl=SimpleNamespace(capacity=14)), (1, 1, 0), (14, 1, 0)) == 1  # one card here
