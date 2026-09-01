from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class ProcessorSpec:
    fn: Callable[..., Any]
    executor: str = "compute"
    supported_executors: tuple[str, ...] = ()
    cpu_required: int = 1


def processor(
    fn: Callable[..., Any],
    *,
    executor: str = "compute",
    supported_executors: tuple[str, ...] = (),
    cpu_required: int = 1,
) -> ProcessorSpec:
    return ProcessorSpec(
        fn=fn,
        executor=executor,
        supported_executors=tuple(supported_executors),
        cpu_required=cpu_required,
    )


__all__ = ["ProcessorSpec", "processor"]
