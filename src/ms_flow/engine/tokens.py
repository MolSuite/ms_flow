"""CPU, GPU and memory tokens: the one place that says whether a chunk fits."""
from __future__ import annotations

import os
import sys


def usable_cpus() -> int:
    """The CPUs this process may use: in a job of a queue, the ones it was given and not the node's."""
    return len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else os.cpu_count() or 1


def physical_memory_mb() -> int:
    if sys.platform == "win32":
        import ctypes

        class _Status(ctypes.Structure):
            _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong)] + [
                (name, ctypes.c_ulonglong)
                for name in ("ullTotalPhys", "ullAvailPhys", "ullTotalPageFile", "ullAvailPageFile",
                             "ullTotalVirtual", "ullAvailVirtual", "ullAvailExtendedVirtual")
            ]

        status = _Status(dwLength=ctypes.sizeof(_Status))
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status))
        return status.ullTotalPhys // 2**20
    return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") // 2**20


def memory_budget_mb(reserve: float = 0.2, min_reserve_mb: int = 2048) -> int:
    """Physical RAM minus what is left to the system and to the application itself."""
    total = physical_memory_mb()
    return max(0, total - max(int(total * reserve), min_reserve_mb))


class Tokens:
    """Free CPU, GPU and memory. A cost is `(cpu, gpu, mem_mb)`.

    Owned by the engine loop, so nothing here locks.
    """

    __slots__ = ("total", "free")

    def __init__(self, cpu: float, gpu: float, mem_mb: float):
        self.total = (cpu, gpu, mem_mb)
        self.free = [cpu, gpu, mem_mb]

    def ever_fits(self, cost) -> bool:
        return all(c <= t for c, t in zip(cost, self.total))

    def fits(self, cost) -> bool:
        free = self.free
        return cost[0] <= free[0] and cost[1] <= free[1] and cost[2] <= free[2]

    def take(self, cost) -> None:
        free = self.free
        free[0] -= cost[0]
        free[1] -= cost[1]
        free[2] -= cost[2]

    def give(self, cost) -> None:
        free = self.free
        free[0] += cost[0]
        free[1] += cost[1]
        free[2] += cost[2]
