from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterator, Sequence


DispatchPair = tuple[Any, Any]


@dataclass(frozen=True)
class LocalResourceSnapshot:
    """Observed local capacity at a dispatch decision point."""

    cpu_limited: bool
    available_cpu: int
    gpu_limited: bool = False
    available_gpu: int = 0
    available_slots: int | None = None
    available_memory_bytes: int | None = None


@dataclass(frozen=True)
class LocalReadyBatch:
    """
    Explicit local scheduling candidate.

    It keeps references to the original chunk/job objects while materializing the
    scheduling attributes that the local admission controller needs.
    """

    chunk: Any
    job: Any
    cpu_required: int
    gpu_required: int
    queue_policy: str
    priority: int
    job_created_at: Any
    batch_created_at: Any


@dataclass(frozen=True)
class LocalAdmissionDecision:
    """Decision returned by the local admission controller."""

    admit: bool
    stop_cycle: bool = False
    reason: str = ""


@dataclass(frozen=True)
class LocalDispatchPolicy:
    """
    Local admission policy for ready work.

    The default favors fairness:
    when running against the local process CPU pool, skip candidates that do
    not currently fit and keep scanning the rest of the ready queue.
    """

    stop_cycle_on_unavailable_cpu: bool = False


class LocalScheduler:
    """Small local admission controller used by ExecutorManager."""

    def __init__(self, policy: LocalDispatchPolicy | None = None):
        self._policy = policy or LocalDispatchPolicy()

    @staticmethod
    def build_ready_batch(chunk: Any, job: Any) -> LocalReadyBatch:
        return LocalReadyBatch(
            chunk=chunk,
            job=job,
            cpu_required=max(1, int(getattr(chunk, "cpu_required", 1) or 1)),
            gpu_required=max(0, int(getattr(chunk, "gpu_required", 0) or 0)),
            queue_policy=str(getattr(job, "queue_policy", "fifo") or "fifo"),
            priority=int(getattr(job, "priority", 0) or 0),
            job_created_at=getattr(job, "created_at", None),
            batch_created_at=getattr(chunk, "created_at", None),
        )

    def normalize_candidates(
        self,
        candidates: Sequence[LocalReadyBatch | DispatchPair],
    ) -> list[LocalReadyBatch]:
        rows: list[LocalReadyBatch] = []
        for item in candidates:
            if isinstance(item, LocalReadyBatch):
                rows.append(item)
                continue
            chunk, job = item
            rows.append(self.build_ready_batch(chunk, job))
        return rows

    def sort_candidates(
        self,
        candidates: Sequence[LocalReadyBatch | DispatchPair],
    ) -> list[LocalReadyBatch]:
        rows = self.normalize_candidates(candidates)

        def sort_key(candidate: LocalReadyBatch):
            if candidate.queue_policy == "priority":
                return (0, -candidate.priority, candidate.job_created_at, candidate.batch_created_at)
            return (1, candidate.job_created_at, candidate.batch_created_at)

        rows.sort(key=sort_key)
        return rows

    def decide_admission(
        self,
        candidate: LocalReadyBatch,
        *,
        resources: LocalResourceSnapshot,
    ) -> LocalAdmissionDecision:
        if resources.cpu_limited and candidate.cpu_required > max(0, int(resources.available_cpu)):
            if self._policy.stop_cycle_on_unavailable_cpu:
                return LocalAdmissionDecision(
                    admit=False,
                    stop_cycle=True,
                    reason=(
                        f"candidate cpu_required={candidate.cpu_required} exceeds "
                        f"available_cpu={resources.available_cpu}"
                    ),
                )
            return LocalAdmissionDecision(
                admit=False,
                stop_cycle=False,
                reason=(
                    f"candidate cpu_required={candidate.cpu_required} exceeds "
                    f"available_cpu={resources.available_cpu}"
                ),
            )
        # Second token axis: GPU. A candidate that needs more GPU than is free
        # is skipped (never stops the cycle — a CPU-only chunk behind it can
        # still be admitted), mirroring the fairness policy for CPU.
        if resources.gpu_limited and candidate.gpu_required > max(0, int(resources.available_gpu)):
            return LocalAdmissionDecision(
                admit=False,
                stop_cycle=False,
                reason=(
                    f"candidate gpu_required={candidate.gpu_required} exceeds "
                    f"available_gpu={resources.available_gpu}"
                ),
            )
        return LocalAdmissionDecision(admit=True)

    def iter_admissible(
        self,
        candidates: Sequence[LocalReadyBatch | DispatchPair],
        *,
        resources: LocalResourceSnapshot | Callable[[], LocalResourceSnapshot],
    ) -> Iterator[LocalReadyBatch]:
        for candidate in self.sort_candidates(candidates):
            current_resources = resources() if callable(resources) else resources
            decision = self.decide_admission(candidate, resources=current_resources)
            if decision.admit:
                yield candidate
                continue
            if decision.stop_cycle:
                break


__all__ = [
    "LocalAdmissionDecision",
    "LocalDispatchPolicy",
    "LocalReadyBatch",
    "LocalResourceSnapshot",
    "LocalScheduler",
]
