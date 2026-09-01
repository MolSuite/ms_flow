"""The extracted local scheduler is a self-contained component: resource accounting
+ admission, usable and testable without the manager god-object. Occupancy is
sampled via an injected provider (running chunks + in-flight submits), not a
manual reserve/release counter."""
from pathlib import Path

from ms_flow.core.executor.local_compute_scheduler import LocalComputeScheduler
from ms_flow.core.executor.local_scheduler import LocalReadyBatch


class _Job:
    queue_policy = "fifo"
    priority = 0
    created_at = 0


class _Chunk:
    def __init__(self, cpu=1, gpu=0):
        self.cpu_required = cpu
        self.gpu_required = gpu
        self.created_at = 0


def _candidates(specs):
    return [(_Chunk(*s), _Job()) for s in specs]


def test_occupancy_provider_subtracts_from_available():
    occ = {"cpu": 0, "gpu": 0}
    sched = LocalComputeScheduler(
        total_cpu=8, total_gpu=2,
        occupancy_provider=lambda: (occ["cpu"], occ["gpu"]),
    )
    assert sched.available_cpu({}) == 8 and sched.available_gpu({}) == 2
    assert sched.used_cpu == 0 and sched.used_gpu == 0
    occ["cpu"], occ["gpu"] = 3, 1
    assert sched.available_cpu({}) == 5 and sched.available_gpu({}) == 1
    assert sched.used_cpu == 3 and sched.used_gpu == 1
    occ["cpu"], occ["gpu"] = 0, 0
    assert sched.available_cpu({}) == 8 and sched.available_gpu({}) == 2


def test_admission_respects_gpu_budget():
    # two chunks each need 1 GPU; only 1 GPU total. The generator re-checks the
    # snapshot each step, and the real dispatch grows occupancy as it admits (the
    # chunk enters the dispatch pool) — so mimic that by bumping the sampled value.
    occ = {"cpu": 0, "gpu": 0}
    sched = LocalComputeScheduler(
        total_cpu=16, total_gpu=1,
        occupancy_provider=lambda: (occ["cpu"], occ["gpu"]),
    )
    admitted = []
    for cand in sched.iter_admissible(_candidates([(1, 1), (1, 1)]), cpu_limited=True, executors={}):
        admitted.append(cand)
        occ["gpu"] += cand.gpu_required
    assert len(admitted) == 1

    # GPU still occupied -> nothing admissible until it frees up
    assert list(sched.iter_admissible(_candidates([(1, 1)]), cpu_limited=True, executors={})) == []
    occ["gpu"] = 0
    assert len(list(sched.iter_admissible(_candidates([(1, 1)]), cpu_limited=True, executors={}))) == 1


def test_not_cpu_limited_admits_everything():
    # a self-scheduling backend (ray/dask) is cpu_limited=False -> mf-core doesn't gate
    sched = LocalComputeScheduler(total_cpu=1, total_gpu=0)
    cands = _candidates([(4, 2), (8, 3)])  # way over budget
    admitted = list(sched.iter_admissible(cands, cpu_limited=False, executors={}))
    assert len(admitted) == 2
