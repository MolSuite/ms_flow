"""Cohesive local (loky / in-process) compute scheduler.

Bundles the three things that were scattered across ``manager.py`` for scheduling
work on local process/thread executors:

  * resource accounting  (LocalResourceManager: cpu + gpu tokens)
  * admission policy      (LocalScheduler: fairness/priority + fit checks)
  * the reserve/release lifecycle + the in-flight-aware "available" view

mf-core owns exactly ONE of these (its local scheduler) and delegates to it for
loky/thread work. Self-scheduling backends (ray, dask) bring their own scheduler
and bypass this entirely via ``consumes_local_cpu_tokens=False`` — mf-core just
forwards the chunk's cpu/gpu requirement to them as native annotations.

Extracting this here keeps resources + fairness + admission *together* (moving only
resources into an adapter would tear them apart) while pulling the logic out of the
manager god-object, so the boundary is: manager = orchestration, this = scheduling.
"""

from __future__ import annotations

from typing import Callable, Mapping, Optional

from ms_flow.core.executor.local_scheduler import LocalResourceSnapshot, LocalScheduler
from ms_flow.core.executor.resource_manager import LocalResourceManager


class LocalComputeScheduler:
    def __init__(
        self,
        *,
        total_cpu: int,
        total_gpu: int = 0,
        occupancy_provider: Optional[Callable[[], tuple[int, int]]] = None,
    ):
        self._resources = LocalResourceManager(total_cpu=total_cpu, total_gpu=total_gpu)
        self._policy = LocalScheduler()
        # SAMPLED occupancy: (cpu, gpu) currently occupied = running chunks
        # (authoritative registry) + in-flight submits (dispatch pool). There is
        # no manual reserve/release counter, so a chunk that is both "reserved"
        # and "in flight" can no longer be double-counted (the old bug that made
        # jobs wait for global CPU while cores sat idle). Injected so the
        # scheduler never imports the manager.
        self._occupancy = occupancy_provider or (lambda: (0, 0))

    # -- exposed for the registry's registration-time headroom checks --
    @property
    def resources(self) -> LocalResourceManager:
        return self._resources

    @property
    def total_cpu(self) -> int:
        return self._resources.total_cpu

    @property
    def total_gpu(self) -> int:
        return self._resources.total_gpu

    @property
    def used_cpu(self) -> int:
        return int(self._occupancy()[0])

    @property
    def used_gpu(self) -> int:
        return int(self._occupancy()[1])

    def reserved_cpu(self, executors: Mapping[str, object]) -> int:
        return self._resources.reserved_cpu(executors)

    # -- headroom = total − adapter-declared reservation − sampled occupancy --
    def available_cpu(self, executors: Mapping[str, object]) -> int:
        return max(
            0,
            self._resources.total_cpu
            - self._resources.reserved_cpu(executors)
            - int(self._occupancy()[0]),
        )

    def available_gpu(self, executors: Mapping[str, object]) -> int:
        return max(
            0,
            self._resources.total_gpu
            - self._resources.reserved_gpu(executors)
            - int(self._occupancy()[1]),
        )

    # -- adapter classification (delegates to the accounting layer) --
    def accounting_mode(self, adapter: object) -> str:
        return self._resources.accounting_mode(adapter)

    def participates_in_local_accounting(self, adapter: object) -> bool:
        return self._resources.participates_in_local_accounting(adapter)

    # -- admission policy --
    def sort_candidates(self, candidates):
        return self._policy.sort_candidates(candidates)

    def iter_admissible(self, candidates, *, cpu_limited: bool, executors: Mapping[str, object]):
        """Yield the candidates that fit the current cpu+gpu budget, in fairness order.

        ``cpu_limited`` reflects whether the target adapter consumes local tokens; it
        also gates the GPU axis (a local process backend that consumes CPU tokens
        consumes GPU tokens too).
        """
        return self._policy.iter_admissible(
            candidates,
            resources=lambda: LocalResourceSnapshot(
                cpu_limited=cpu_limited,
                available_cpu=self.available_cpu(executors),
                gpu_limited=cpu_limited,
                available_gpu=self.available_gpu(executors),
            ),
        )


__all__ = ["LocalComputeScheduler"]
