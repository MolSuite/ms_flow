from datetime import datetime, timedelta
from types import SimpleNamespace
from pathlib import Path

from ms_flow.core.executor.local_scheduler import (
    LocalAdmissionDecision,
    LocalDispatchPolicy,
    LocalReadyBatch,
    LocalResourceSnapshot,
    LocalScheduler,
)


def _candidate(
    *,
    cpu_required: int,
    queue_policy: str = "fifo",
    priority: int = 0,
    job_offset_s: int = 0,
    chunk_offset_s: int = 0,
):
    base = datetime(2026, 1, 1, 12, 0, 0)
    chunk = SimpleNamespace(
        cpu_required=cpu_required,
        created_at=base + timedelta(seconds=chunk_offset_s),
    )
    job = SimpleNamespace(
        queue_policy=queue_policy,
        priority=priority,
        created_at=base + timedelta(seconds=job_offset_s),
    )
    return chunk, job


def test_local_scheduler_sorts_priority_before_fifo():
    scheduler = LocalScheduler()
    low_fifo = _candidate(cpu_required=1, queue_policy="fifo", job_offset_s=0, chunk_offset_s=0)
    high_priority = _candidate(cpu_required=1, queue_policy="priority", priority=10, job_offset_s=10, chunk_offset_s=10)
    ordered = scheduler.sort_candidates([low_fifo, high_priority])

    assert isinstance(ordered[0], LocalReadyBatch)
    assert ordered[0].priority == 10
    assert ordered[0].queue_policy == "priority"
    assert ordered[0].job is high_priority[1]
    assert ordered[1].job is low_fifo[1]


def test_local_scheduler_skips_oversized_candidate_and_admits_later_fit():
    scheduler = LocalScheduler()
    blocked = _candidate(cpu_required=4, queue_policy="priority", priority=5)
    later_fit = _candidate(cpu_required=1, queue_policy="fifo", job_offset_s=1, chunk_offset_s=1)

    admitted = list(
        scheduler.iter_admissible(
            [blocked, later_fit],
            resources=LocalResourceSnapshot(cpu_limited=True, available_cpu=2),
        )
    )

    assert [item.job for item in admitted] == [later_fit[1]]


def test_local_scheduler_returns_all_sorted_candidates_when_cpu_is_not_limited():
    scheduler = LocalScheduler()
    first = _candidate(cpu_required=4, queue_policy="fifo", job_offset_s=0, chunk_offset_s=0)
    second = _candidate(cpu_required=1, queue_policy="fifo", job_offset_s=1, chunk_offset_s=1)

    admitted = list(
        scheduler.iter_admissible(
            [second, first],
            resources=LocalResourceSnapshot(cpu_limited=False, available_cpu=0),
        )
    )

    assert [item.job for item in admitted] == [first[1], second[1]]


def test_local_scheduler_builds_explicit_ready_batch_contract():
    scheduler = LocalScheduler()
    chunk, job = _candidate(cpu_required=3, queue_policy="priority", priority=7)

    ready = scheduler.build_ready_batch(chunk, job)

    assert isinstance(ready, LocalReadyBatch)
    assert ready.chunk is chunk
    assert ready.job is job
    assert ready.cpu_required == 3
    assert ready.queue_policy == "priority"
    assert ready.priority == 7


def test_local_scheduler_returns_explicit_admission_decision():
    scheduler = LocalScheduler()
    ready = scheduler.build_ready_batch(*_candidate(cpu_required=5))

    decision = scheduler.decide_admission(
        ready,
        resources=LocalResourceSnapshot(cpu_limited=True, available_cpu=2),
    )

    assert isinstance(decision, LocalAdmissionDecision)
    assert decision.admit is False
    assert decision.stop_cycle is False
    assert "cpu_required=5" in decision.reason


def test_local_scheduler_stop_cycle_policy_can_preserve_legacy_break_behavior():
    scheduler = LocalScheduler(policy=LocalDispatchPolicy(stop_cycle_on_unavailable_cpu=True))
    blocked = _candidate(cpu_required=4, queue_policy="priority", priority=10)
    later_fit = _candidate(cpu_required=1, queue_policy="fifo", job_offset_s=1, chunk_offset_s=1)

    admitted = list(
        scheduler.iter_admissible(
            [blocked, later_fit],
            resources=LocalResourceSnapshot(cpu_limited=True, available_cpu=2),
        )
    )

    assert admitted == []


def test_local_scheduler_rechecks_callable_resources_between_candidates():
    scheduler = LocalScheduler()
    first = _candidate(cpu_required=1, queue_policy="fifo", job_offset_s=0, chunk_offset_s=0)
    second = _candidate(cpu_required=1, queue_policy="fifo", job_offset_s=1, chunk_offset_s=1)
    snapshots = iter(
        [
            LocalResourceSnapshot(cpu_limited=True, available_cpu=1),
            LocalResourceSnapshot(cpu_limited=True, available_cpu=0),
        ]
    )

    admitted = list(
        scheduler.iter_admissible(
            [first, second],
            resources=lambda: next(snapshots),
        )
    )

    assert [item.job for item in admitted] == [first[1]]


def test_local_scheduler_orders_priority_candidates_by_job_then_chunk_age():
    scheduler = LocalScheduler()
    older_job_newer_chunk = _candidate(
        cpu_required=1,
        queue_policy="priority",
        priority=5,
        job_offset_s=0,
        chunk_offset_s=10,
    )
    newer_job_older_chunk = _candidate(
        cpu_required=1,
        queue_policy="priority",
        priority=5,
        job_offset_s=5,
        chunk_offset_s=0,
    )

    ordered = scheduler.sort_candidates([newer_job_older_chunk, older_job_newer_chunk])

    assert [item.job for item in ordered] == [older_job_newer_chunk[1], newer_job_older_chunk[1]]
