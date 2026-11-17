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


def _card(vram, held):
    """What `_Usage` tells of a task holding `held` MB of a card with `vram` MB, 500 MB of it others'."""
    return {"where": "node:0", "vram": held, "vram_used": 500 + held, "vram_total": vram, "gpu": 60, "gpu_top": 90}


def test_the_next_size_goes_on_only_if_it_fits_in_the_card():
    # 128 holds 1000 MB of 6000: 2x and 3x fit; 19·a/b = 1900 would need 14.8 GB, so it stops
    # at what fits in 90 % of the card: (5400 - 500) MB at 7.8 MB a chunk = 627, 608 in floors of 32.
    auto, sizes = _Autobatch(32, start=128), []
    while not auto.settled:
        n = auto.size
        sizes.append(n)
        auto.seen(n, 1 + 0.01 * n, used=_card(6000, 1000 * n / 128))
    assert sizes == [128, 256, 384] and auto.size == 608


def test_with_two_sizes_held_the_card_fits_along_their_line():
    # The 1050 Ti of the Ray runs: 128 held 1474 MB, 256 held 2422 MB of 4096 (a fixed ~530 MB).
    # In proportion 128 allows 288; along the line through both, 256 allows (3686 - 526) / 7.4 = 427.
    auto = _Autobatch(32, start=128)
    laptop = lambda held: {"where": "laptop:0", "vram": held, "vram_used": held + 4, "vram_total": 4096}
    auto.seen(128, 20.0, used=laptop(1474))  # times with a large fixed part: only memory bounds it
    assert auto.size == 256
    auto.seen(256, 30.0, used=laptop(2422))
    assert auto.size == 384  # 3x fits now; in proportion it would have stopped at 288


def test_a_card_known_from_one_size_holds_the_size_back_until_it_tells_more():
    # The run 8 order: the 1660 ends 128 and 256 before the 1050 Ti ends its second task.
    auto = _Autobatch(32, start=128)
    fast = lambda held: {"where": "pc:0", "vram": held, "vram_used": held + 500, "vram_total": 6144}
    slow = lambda held: {"where": "laptop:0", "vram": held, "vram_used": held + 4, "vram_total": 4096}
    auto.seen(128, 20.0, width=2, used=fast(1389))
    auto.seen(128, 31.5, width=2, used=slow(1474))  # 288 in proportion
    auto.seen(256, 30.8, width=2, used=fast(2711))
    assert (auto.size, auto.settled) == (288, False)
    auto.seen(256, 57.5, width=2, used=slow(2422))  # its line allows 427
    auto.seen(288, 34.3, width=2, used=fast(2961))
    assert auto.size == 384


def test_a_task_that_fills_the_card_halves_the_size():
    auto = _Autobatch(32, start=128)
    auto.seen(128, 2.0, used=_card(4000, 3500))  # 4000 MB in use of 4000
    assert (auto.size, auto.settled) == (64, True)
    auto = _Autobatch(1)
    auto.seen(1, 60.0, used=_card(4000, 3500))
    assert auto.size == 1  # never under the floor
    # two tasks to a card: each grows, and both have to fit
    auto = _Autobatch(32, start=128)
    auto.seen(128, 2.0, used=_card(6000, 1000), sharing=2)  # 2500 of 6000: room for 2.3 times
    assert auto.size == 256


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
        auto.seen(n, took, width=2, used={"where": f"card{index}"})
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
    assert used["ram"] > 0 and used["ram_used"] <= used["ram_total"]  # psutil: read while it ran


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
