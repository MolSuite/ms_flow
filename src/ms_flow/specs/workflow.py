from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Optional

from ms_flow.specs.input import InputSource
from ms_flow.specs.output import OutputSink
from ms_flow.specs.processor import ProcessorSpec, processor
from ms_flow.tasking import JobDefinition, _build_job_definition


def _normalize_processor(value: Callable[..., Any] | ProcessorSpec) -> ProcessorSpec:
    if isinstance(value, ProcessorSpec):
        return value
    if callable(value):
        return processor(value)
    raise TypeError("workflow(process=...) requires an importable callable or a ProcessorSpec.")


@dataclass(frozen=True)
class WorkflowSpec:
    name: str
    input: InputSource
    process: Callable[..., Any] | ProcessorSpec
    output: OutputSink | None = None
    description: str = ""
    task_name: str | None = None
    setup: Optional[Callable[..., Any]] = None
    stage: Optional[Callable[..., Any]] = None
    finalize: Optional[Callable[..., Any]] = None
    result_handler: Optional[Callable[..., Any]] = None
    flush_every: int = 500
    executor: str = ""
    supported_executors: tuple[str, ...] = ()
    cpu_required: int | None = None
    stage_fail_policy: str = "fail_fast"
    max_stage_failures: int = 0
    store_results: bool = False
    tags: tuple[str, ...] = ()

    def build_chunks(
        self,
        *,
        params: Mapping[str, Any] | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> Iterable[dict[str, Any]]:
        return self.input.iter_chunks(params=params, config=config)

    def build_job_definition(self, *, config: Mapping[str, Any] | None = None) -> JobDefinition:
        normalized = _normalize_processor(self.process)
        resolved_executor = str(self.executor or normalized.executor or "compute").strip() or "compute"
        resolved_supported = tuple(
            dict.fromkeys(
                item
                for item in (resolved_executor, *(self.supported_executors or normalized.supported_executors))
                if str(item).strip()
            )
        )
        output_spec = None
        if self.output is not None:
            if hasattr(self.output, "get_output_spec"):
                output_spec = self.output.get_output_spec(dict(config or {}))
            else:
                # If it's already an OutputSpec or similar, use it directly
                output_spec = self.output
        return _build_job_definition(
            name=self.name,
            run_chunk=normalized.fn,
            task_name=self.task_name,
            description=self.description,
            chunker=None,
            setup=self.setup,
            stage_chunk=self.stage,
            finalize=self.finalize,
            stage_fail_policy=self.stage_fail_policy,
            max_stage_failures=self.max_stage_failures,
            output_spec=output_spec,
            output_flush_every=self.flush_every,
            result_handler_factory=self.result_handler,
            executor=resolved_executor,
            supported_executors=resolved_supported,
            cpu_required=self.cpu_required if self.cpu_required is not None else normalized.cpu_required,
            store_results=bool(self.store_results),
            tags=tuple(self.tags),
        )


class WorkflowLauncher:
    def __init__(self, molsuite):
        self._molsuite = molsuite

    def submit(
        self,
        spec: WorkflowSpec,
        *,
        params: Mapping[str, Any] | None = None,
        config: Mapping[str, Any] | None = None,
        queue_policy: str = "fifo",
        priority: int = 0,
    ) -> str:
        effective_config = dict(config or {})
        if "project_db" not in effective_config:
            effective_config["project_db"] = self._molsuite.project_db
        if "project_store" not in effective_config:
            effective_config["project_store"] = self._molsuite.project_store
        if "project_context" not in effective_config:
            effective_config["project_context"] = self._molsuite.active_context
        if "project_resources" not in effective_config:
            resource_map = self._molsuite.get_project_resource_map()
            if resource_map:
                effective_config["project_resources"] = resource_map

        job_def = spec.build_job_definition(config=effective_config)
        return self._molsuite.submit_job(
            job_def,
            chunks=spec.build_chunks(params=params, config=effective_config),
            config=effective_config,
            queue_policy=queue_policy,
            priority=priority,
        )


from ms_flow.specs.input import InputSource, simple_items

def _normalize_input(value: Any) -> InputSource:
    if isinstance(value, InputSource):
        return value
    if hasattr(value, "__iter__"):
        return simple_items(value)
    raise TypeError(f"workflow(input=...) requires an InputSource or an iterable. Got: {type(value)}")


def workflow(
    *,
    name: str,
    input: Any,
    process: Callable[..., Any] | ProcessorSpec,
    output: OutputSink | None = None,
    description: str = "",
    task_name: str | None = None,
    setup: Optional[Callable[..., Any]] = None,
    stage: Optional[Callable[..., Any]] = None,
    finalize: Optional[Callable[..., Any]] = None,
    result_handler: Optional[Callable[..., Any]] = None,
    flush_every: int = 500,
    executor: str = "",
    supported_executors: tuple[str, ...] = (),
    cpu_required: int | None = None,
    stage_fail_policy: str = "fail_fast",
    max_stage_failures: int = 0,
    store_results: bool = False,
    tags: tuple[str, ...] = (),
) -> WorkflowSpec:
    return WorkflowSpec(
        name=name,
        input=_normalize_input(input),
        process=process,
        output=output,
        description=description,
        task_name=task_name,
        setup=setup,
        stage=stage,
        finalize=finalize,
        result_handler=result_handler,
        flush_every=flush_every,
        executor=executor,
        supported_executors=tuple(supported_executors),
        cpu_required=cpu_required,
        stage_fail_policy=stage_fail_policy,
        max_stage_failures=max_stage_failures,
        store_results=store_results,
        tags=tuple(tags),
    )


__all__ = ["WorkflowLauncher", "WorkflowSpec", "workflow"]
