from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class DispatchPolicy:
    """
    Internal dispatch contract for feed/batch/inflight control.

    The current runtime still feeds already-materialized batch payloads, but this
    policy makes the distinction explicit and prepares the move from chunk/window
    semantics to batch/inflight semantics.
    """

    batch_size: int | str = 1
    max_inflight_tasks: int = 16
    max_inflight_items: int | None = None
    prefetch_factor: float = 1.0
    refill_threshold: int = 1

    def __post_init__(self):
        batch_size = self.batch_size
        if isinstance(batch_size, str):
            normalized_batch_size: int | str = str(batch_size).strip().lower() or "auto"
            if normalized_batch_size != "auto":
                normalized_batch_size = max(1, int(normalized_batch_size))
        else:
            normalized_batch_size = max(1, int(batch_size))

        max_inflight_tasks = max(1, int(self.max_inflight_tasks))
        max_inflight_items = (
            None if self.max_inflight_items is None else max(max_inflight_tasks, int(self.max_inflight_items))
        )
        prefetch_factor = max(0.0, float(self.prefetch_factor))
        refill_threshold = max(1, min(max_inflight_tasks, int(self.refill_threshold)))

        object.__setattr__(self, "batch_size", normalized_batch_size)
        object.__setattr__(self, "max_inflight_tasks", max_inflight_tasks)
        object.__setattr__(self, "max_inflight_items", max_inflight_items)
        object.__setattr__(self, "prefetch_factor", prefetch_factor)
        object.__setattr__(self, "refill_threshold", refill_threshold)

    @classmethod
    def from_mapping(
        cls,
        raw: Mapping[str, Any] | None,
    ) -> "DispatchPolicy":
        payload = dict(raw or {})
        if not payload:
            return cls()
        return cls(
            batch_size=payload.get("batch_size", 1),
            max_inflight_tasks=payload.get("max_inflight_tasks", 16),
            max_inflight_items=payload.get("max_inflight_items"),
            prefetch_factor=payload.get("prefetch_factor", 1.0),
            refill_threshold=payload.get("refill_threshold", 1),
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "batch_size": self.batch_size,
            "max_inflight_tasks": self.max_inflight_tasks,
            "max_inflight_items": self.max_inflight_items,
            "prefetch_factor": self.prefetch_factor,
            "refill_threshold": self.refill_threshold,
        }


__all__ = ["DispatchPolicy"]
