"""Autobatch: two task sizes timed give the fixed and per-chunk cost; the size kept follows."""
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


def test_without_a_fixed_cost_it_stays_at_the_floor():
    assert _settle(_Autobatch(8, start=64), lambda n: 0.01 * n) == 8


def test_it_starts_where_the_job_says_and_tries_three_times_that():
    auto = _Autobatch(32, start=128)
    assert auto.size == 128
    for _ in range(2):
        auto.seen(128, 1 + 0.01 * 128)
    assert (auto.size, auto.settled) == (384, False)


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
        auto.seen(n, took, width=2)
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
