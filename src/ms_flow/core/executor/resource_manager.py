from __future__ import annotations

from typing import Mapping


class LocalResourceManager:
    """Tracks adapter-declared local reservations and accounting metadata."""

    def __init__(self, total_cpu: int, total_gpu: int = 0):
        self.total_cpu = max(1, int(total_cpu))
        self.total_gpu = max(0, int(total_gpu))

    def reserved_cpu(self, executors: Mapping[str, object]) -> int:
        return sum(max(0, int(getattr(adapter, "reserved_cpu", 0) or 0)) for adapter in executors.values())

    # ------------------------------------------------------------------
    # GPU reservations are adapter-declared like CPU reservations. Dynamic
    # occupancy belongs to LocalComputeScheduler, which samples runtime state.
    # ------------------------------------------------------------------

    def reserved_gpu(self, executors: Mapping[str, object]) -> int:
        return sum(max(0, int(getattr(adapter, "reserved_gpu", 0) or 0)) for adapter in executors.values())

    @staticmethod
    def _adapter_metadata(adapter: object) -> object:
        return getattr(adapter, "metadata", None)

    @classmethod
    def accounting_mode(cls, adapter: object) -> str:
        metadata = cls._adapter_metadata(adapter)
        if bool(getattr(metadata, "consumes_local_cpu_tokens", getattr(adapter, "consumes_local_cpu_tokens", False))):
            return "dynamic"
        if (
            str(getattr(metadata, "mode", getattr(adapter, "execution_mode", "external")) or "external").strip().lower() == "local"
            and max(0, int(getattr(adapter, "reserved_cpu", 0) or 0)) > 0
        ):
            return "reserved"
        return "none"

    def participates_in_local_accounting(self, adapter: object) -> bool:
        return self.accounting_mode(adapter) != "none"


def detect_local_gpus() -> int:
    """Best-effort local GPU count. Returns 0 when it can't tell (never raises).

    Honors CUDA_VISIBLE_DEVICES if set (respects masking), else asks nvidia-smi.
    """
    import os

    env = os.environ.get("CUDA_VISIBLE_DEVICES")
    if env is not None:
        return len([d for d in env.split(",") if d.strip() != ""])
    try:
        import subprocess

        out = subprocess.run(["nvidia-smi", "-L"], capture_output=True, text=True, timeout=3)
        if out.returncode == 0:
            return sum(1 for line in out.stdout.splitlines() if line.strip().startswith("GPU "))
    except Exception:
        pass
    return 0


__all__ = ["LocalResourceManager", "detect_local_gpus"]
