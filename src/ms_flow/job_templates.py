from __future__ import annotations

from typing import Any, Callable, Iterable, Optional

from pydantic import BaseModel

from ms_flow.tasking import JobDefinition, _build_job_definition


def build_streaming_job_definition(
    *,
    name: str,
    run_chunk: Callable[..., Any],
    chunker: Callable[..., Any],
    task_name: str | None = None,
    description: str = "",
    params_model: Optional[type[BaseModel]] = None,
    input_model: Optional[type[BaseModel]] = None,
    setup: Optional[Callable[..., Any]] = None,
    stage_chunk: Optional[Callable[..., Any]] = None,
    finalize: Optional[Callable[..., Any]] = None,
    result_handler_factory: Optional[Callable[..., Any]] = None,
    output_spec: Any = None,
    output_flush_every: int = 500,
    executor: str = "compute",
    supported_executors: Iterable[str] = (),
    cpu_required: int = 1,
    stage_fail_policy: str = "fail_fast",
    max_stage_failures: int = 0,
    store_results: bool = False,
    tags: Iterable[str] = (),
) -> JobDefinition:
    """
    Build a reusable streaming JobDefinition with minimal boilerplate.

    Intended for app-level jobs where each chunk is produced lazily and
    persisted incrementally through output specs.
    """
    return _build_job_definition(
        name=name,
        run_chunk=run_chunk,
        task_name=task_name,
        description=description,
        params_model=params_model,
        input_model=input_model,
        chunker=chunker,
        setup=setup,
        stage_chunk=stage_chunk,
        finalize=finalize,
        result_handler_factory=result_handler_factory,
        output_spec=output_spec,
        output_flush_every=output_flush_every,
        executor=executor,
        supported_executors=tuple(supported_executors),
        cpu_required=cpu_required,
        stage_fail_policy=stage_fail_policy,
        max_stage_failures=max_stage_failures,
        store_results=store_results,
        tags=tuple(tags),
    )

def streaming_job(
    *,
    name: str,
    run_chunk: Callable[..., Any],
    chunker: Callable[..., Any],
    task_name: str | None = None,
    description: str = "",
    params_model: Optional[type[BaseModel]] = None,
    input_model: Optional[type[BaseModel]] = None,
    setup: Optional[Callable[..., Any]] = None,
    stage: Optional[Callable[..., Any]] = None,
    finalize: Optional[Callable[..., Any]] = None,
    result_handler: Optional[Callable[..., Any]] = None,
    output: Any = None,
    flush_every: int = 500,
    executor: str = "compute",
    supported_executors: Iterable[str] | None = None,
    cpu_required: int = 1,
    stage_fail_policy: str = "fail_fast",
    max_stage_failures: int = 0,
    store_results: bool = False,
    tags: Iterable[str] = (),
) -> JobDefinition:
    """Simple public helper to compose a reusable streaming job."""
    normalized_executor = str(executor).strip() or "compute"
    normalized_supported_executors = tuple(
        dict.fromkeys((normalized_executor, *(tuple(supported_executors or ()))))
    )
    return build_streaming_job_definition(
        name=name,
        run_chunk=run_chunk,
        chunker=chunker,
        task_name=task_name,
        description=description,
        params_model=params_model,
        input_model=input_model,
        setup=setup,
        stage_chunk=stage,
        finalize=finalize,
        result_handler_factory=result_handler,
        output_spec=output,
        output_flush_every=flush_every,
        executor=normalized_executor,
        supported_executors=normalized_supported_executors,
        cpu_required=cpu_required,
        stage_fail_policy=stage_fail_policy,
        max_stage_failures=max_stage_failures,
        store_results=store_results,
        tags=tags,
    )


def batch_job(
    *,
    name: str,
    run_batch: Callable[..., Any],
    batcher: Callable[..., Any],
    task_name: str | None = None,
    description: str = "",
    params_model: Optional[type[BaseModel]] = None,
    input_model: Optional[type[BaseModel]] = None,
    setup: Optional[Callable[..., Any]] = None,
    stage: Optional[Callable[..., Any]] = None,
    finalize: Optional[Callable[..., Any]] = None,
    result_handler: Optional[Callable[..., Any]] = None,
    output: Any = None,
    flush_every: int = 500,
    executor: str = "compute",
    supported_executors: Iterable[str] | None = None,
    cpu_required: int = 1,
    stage_fail_policy: str = "fail_fast",
    max_stage_failures: int = 0,
    store_results: bool = False,
    tags: Iterable[str] = (),
) -> JobDefinition:
    """
    Simple public helper for jobs where each chunk represents a batch.

    `batcher` must produce already-grouped payloads, usually shaped like
    `{"items": [...]}`, so `run_batch` processes the whole batch.
    """
    normalized_executor = str(executor).strip() or "compute"
    normalized_supported_executors = tuple(
        dict.fromkeys((normalized_executor, *(tuple(supported_executors or ()))))
    )
    return build_streaming_job_definition(
        name=name,
        run_chunk=run_batch,
        chunker=batcher,
        task_name=task_name,
        description=description,
        params_model=params_model,
        input_model=input_model,
        setup=setup,
        stage_chunk=stage,
        finalize=finalize,
        result_handler_factory=result_handler,
        output_spec=output,
        output_flush_every=flush_every,
        executor=normalized_executor,
        supported_executors=normalized_supported_executors,
        cpu_required=cpu_required,
        stage_fail_policy=stage_fail_policy,
        max_stage_failures=max_stage_failures,
        store_results=store_results,
        tags=tags,
    )


__all__ = ["batch_job", "build_streaming_job_definition", "streaming_job"]
