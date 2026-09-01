from __future__ import annotations

import inspect
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Iterable, Iterator, Optional

from pydantic import BaseModel

from ms_flow.core.callable_refs import (
    callable_ref,
    resolve_callable_ref,
    validate_importable_callable,
    validate_importable_ref,
)
from ms_flow.core.data import DbOutputSpec, OutputSpec


def _call_with_optional_config(fn: Callable[..., Any], params: dict, config: dict) -> Any:
    try:
        n_params = len(inspect.signature(fn).parameters)
    except (TypeError, ValueError):
        n_params = 1
    return fn(params, config) if n_params >= 2 else fn(params)


_MIB = 1024 * 1024


@dataclass(frozen=True)
class GuardrailWarning:
    code: str
    message: str
    severity: str = "warning"
    details: dict[str, Any] = field(default_factory=dict)

    def to_mapping(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "severity": self.severity,
            "details": dict(self.details),
        }


def build_scalability_guardrails(
    *,
    executor: str,
    output_spec: OutputSpec | None,
    output_flush_every: int,
    store_results: bool,
    total_chunks: int | None,
    max_inflight_tasks: int | None,
    sink_max_buffer_factor: int | None = None,
    sink_max_buffer_bytes: int | None = None,
    sink_max_payload_bytes: int | None = None,
) -> tuple[GuardrailWarning, ...]:
    warnings: list[GuardrailWarning] = []
    normalized_flush_every = max(1, int(output_flush_every or 1))

    if normalized_flush_every == 1 and output_spec is not None:
        if isinstance(output_spec, DbOutputSpec) and output_spec.mode == "graph":
            warnings.append(
                GuardrailWarning(
                    code="graph_flush_every_1",
                    message=(
                        "`output_flush_every=1` with `DbOutputSpec(mode='graph')` forces a per-chunk commit "
                        "and tends to degrade persistence throughput."
                    ),
                    details={
                        "output_kind": output_spec.kind,
                        "output_mode": output_spec.mode,
                        "db_role": output_spec.db_role,
                    },
                )
            )
        elif sink_max_payload_bytes is not None and int(sink_max_payload_bytes) >= 1 * _MIB:
            warnings.append(
                GuardrailWarning(
                    code="flush_every_1_large_payload_budget",
                    message=(
                        "`output_flush_every=1` combined with a high payload budget can amplify small, "
                        "expensive flushes in the sink."
                    ),
                    details={
                        "output_kind": output_spec.kind,
                        "output_flush_every": normalized_flush_every,
                        "sink_max_payload_bytes": int(sink_max_payload_bytes),
                    },
                )
            )

    if output_spec is not None and store_results:
        warnings.append(
            GuardrailWarning(
                code="store_results_with_sink",
                message=(
                    "The job uses `output_spec` together with `store_results=True`. At high volume this usually "
                    "retains more state than needed in `executor.db`."
                ),
                details={
                    "output_kind": output_spec.kind,
                    "store_results": True,
                },
            )
        )

    quotas_large = (
        (sink_max_buffer_factor is not None and int(sink_max_buffer_factor) > 50)
        or (sink_max_buffer_bytes is not None and int(sink_max_buffer_bytes) > 256 * _MIB)
        or (sink_max_payload_bytes is not None and int(sink_max_payload_bytes) > 64 * _MIB)
    )
    quotas_inconsistent = (
        sink_max_buffer_bytes is not None
        and sink_max_payload_bytes is not None
        and int(sink_max_buffer_bytes) < int(sink_max_payload_bytes)
    )
    if quotas_large or quotas_inconsistent:
        warnings.append(
            GuardrailWarning(
                code="sink_quotas_unreasonable",
                message=(
                    "The sink quotas look unscalable for desktop use. Review `max_buffer_factor`, "
                    "`max_buffer_bytes` and `max_payload_bytes`."
                ),
                details={
                    "max_buffer_factor": None if sink_max_buffer_factor is None else int(sink_max_buffer_factor),
                    "max_buffer_bytes": None if sink_max_buffer_bytes is None else int(sink_max_buffer_bytes),
                    "max_payload_bytes": None if sink_max_payload_bytes is None else int(sink_max_payload_bytes),
                },
            )
        )

    return tuple(warnings)


def _build_job_definition(
    *,
    name: str,
    run_chunk: Callable[..., Any],
    task_name: str | None = None,
    description: str = "",
    params_model: Optional[type[BaseModel]] = None,
    input_model: Optional[type[BaseModel]] = None,
    chunker: Optional[Callable[..., Any]] = None,
    setup: Optional[Callable[..., Any]] = None,
    stage_chunk: Optional[Callable[..., Any]] = None,
    finalize: Optional[Callable[..., Any]] = None,
    result_handler_factory: Optional[Callable[..., Any]] = None,
    output_spec: Any = None,
    output_flush_every: int = 500,
    executor: str = "compute",
    supported_executors: Iterable[str] = (),
    cpu_required: int | None = 1,
    stage_fail_policy: str = "fail_fast",
    max_stage_failures: int = 0,
    store_results: bool = False,
    tags: Iterable[str] = (),
) -> "JobDefinition":
    normalized_name = str(name).strip()
    if not normalized_name:
        raise ValueError("Job name must not be empty.")

    resolved_executor = str(executor).strip() or "compute"
    resolved_supported = tuple(
        dict.fromkeys(
            item
            for item in (resolved_executor, *(tuple(supported_executors or ())))
            if str(item).strip()
        )
    )
    resolved_task_name = str(task_name or f"{normalized_name}_task").strip() or f"{normalized_name}_task"
    handler_ref = validate_importable_callable(run_chunk)

    task_def = TaskDefinition(
        name=resolved_task_name,
        fn=run_chunk,
        handler_ref=handler_ref,
        description=description,
        executor=resolved_executor,
        supported_executors=resolved_supported,
        cpu_required=1 if cpu_required is None else cpu_required,
        input_model=input_model,
        tags=tuple(tags),
    )

    return JobDefinition(
        name=normalized_name,
        description=description,
        task=task_def,
        params_model=params_model,
        chunker=chunker,
        setup=setup,
        stage_chunk=stage_chunk,
        finalize=finalize,
        stage_fail_policy=stage_fail_policy,
        max_stage_failures=max_stage_failures,
        output_spec=output_spec,
        output_flush_every=output_flush_every,
        result_handler_factory=result_handler_factory,
        executor=resolved_executor,
        supported_executors=resolved_supported,
        cpu_required=cpu_required,
        store_results=store_results,
        tags=tuple(tags),
    )


@dataclass(frozen=True)
class TaskDefinition:
    name: str
    fn: Callable[..., Any]
    handler_ref: str
    description: str = ""
    executor: str = "compute"
    supported_executors: tuple[str, ...] = ()
    cpu_required: int = 1
    input_model: Optional[type[BaseModel]] = None
    tags: tuple[str, ...] = ()

    def __post_init__(self):
        supported = tuple(
            dict.fromkeys(
                str(item).strip()
                for item in (self.supported_executors or ())
                if str(item).strip()
            ).keys()
        )
        if not supported:
            supported = (self.executor,)
        if self.executor not in supported:
            raise ValueError(
                f"default executor '{self.executor}' is not in supported_executors={supported}."
            )
        object.__setattr__(self, "supported_executors", supported)
        object.__setattr__(self, "cpu_required", max(1, int(self.cpu_required)))
        object.__setattr__(self, "handler_ref", validate_importable_ref(self.handler_ref, label="handler_ref"))

    @property
    def params_model(self) -> Optional[type[BaseModel]]:
        return self.input_model

    def __call__(self, payload: dict, progress=None) -> Any:
        if progress is None:
            return self.fn(payload)
        try:
            n_params = len(inspect.signature(self.fn).parameters)
        except (TypeError, ValueError):
            n_params = 1
        return self.fn(payload, progress) if n_params >= 2 else self.fn(payload)

    def validate_payload(self, payload: dict | BaseModel) -> dict:
        if self.input_model is None:
            if isinstance(payload, BaseModel):
                return payload.model_dump(mode="python")
            return dict(payload)
        if isinstance(payload, self.input_model):
            validated = payload
        else:
            validated = self.input_model.model_validate(payload)
        return validated.model_dump(mode="python")

    def with_options(self, **changes) -> "TaskDefinition":
        data = {**changes}
        if "fn" not in data:
            data["fn"] = self.fn
        if "handler_ref" not in data:
            data["handler_ref"] = self.handler_ref
        return replace(self, **data)


@dataclass(frozen=True)
class RequirementSpec:
    """Declarative capability required before a job should run."""

    entity_kind: str
    capability: str
    role: str = ""
    value: Any = None
    required: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        object.__setattr__(self, "entity_kind", str(self.entity_kind or "").strip())
        object.__setattr__(self, "capability", str(self.capability or "").strip())
        object.__setattr__(self, "role", str(self.role or "").strip())
        object.__setattr__(self, "required", bool(self.required))
        object.__setattr__(self, "metadata", dict(self.metadata or {}))
        if not self.entity_kind:
            raise ValueError("RequirementSpec.entity_kind must not be empty.")
        if not self.capability:
            raise ValueError("RequirementSpec.capability must not be empty.")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "entity_kind": self.entity_kind,
            "capability": self.capability,
            "role": self.role,
            "value": self.value,
            "required": self.required,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class CapabilitySpec:
    """Declarative capability produced by a completed job."""

    entity_kind: str
    capability: str
    role: str = ""
    value: Any = None
    artifact_kind: str = ""
    format: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        object.__setattr__(self, "entity_kind", str(self.entity_kind or "").strip())
        object.__setattr__(self, "capability", str(self.capability or "").strip())
        object.__setattr__(self, "role", str(self.role or "").strip())
        object.__setattr__(self, "artifact_kind", str(self.artifact_kind or "").strip())
        object.__setattr__(self, "format", str(self.format or "").strip())
        object.__setattr__(self, "metadata", dict(self.metadata or {}))
        if not self.entity_kind:
            raise ValueError("CapabilitySpec.entity_kind must not be empty.")
        if not self.capability:
            raise ValueError("CapabilitySpec.capability must not be empty.")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "entity_kind": self.entity_kind,
            "capability": self.capability,
            "role": self.role,
            "value": self.value,
            "artifact_kind": self.artifact_kind,
            "format": self.format,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class JobDefinition:
    name: str
    task: TaskDefinition
    description: str = ""
    params_model: Optional[type[BaseModel]] = None
    chunker: Optional[Callable[..., Any]] = None
    chunker_ref: str = ""
    setup: Optional[Callable[..., Any]] = None
    setup_ref: str = ""
    stage_chunk: Optional[Callable[..., Any]] = None
    stage_chunk_ref: str = ""
    finalize: Optional[Callable[..., Any]] = None
    finalize_ref: str = ""
    stage_fail_policy: str = "fail_fast"
    max_stage_failures: int = 0
    output_spec: Any = None
    output_flush_every: int = 500
    result_handler_factory: Optional[Callable[..., Any]] = None
    result_handler_ref: str = ""
    executor: str = ""
    supported_executors: tuple[str, ...] = ()
    cpu_required: Optional[int] = None
    store_results: bool = True
    tags: tuple[str, ...] = ()
    required: tuple[RequirementSpec, ...] = ()
    produces: tuple[CapabilitySpec, ...] = ()

    def __post_init__(self):
        executor = self.executor or self.task.executor
        supported = tuple(
            dict.fromkeys(
                str(item).strip()
                for item in (self.supported_executors or self.task.supported_executors)
                if str(item).strip()
            ).keys()
        )
        if not supported:
            supported = (executor,)
        if executor not in supported:
            raise ValueError(
                f"default executor '{executor}' is not in supported_executors={supported}."
            )
        object.__setattr__(self, "executor", executor)
        object.__setattr__(self, "supported_executors", supported)
        object.__setattr__(self, "cpu_required", max(1, int(self.cpu_required or self.task.cpu_required)))

        chunker_ref = self.chunker_ref or (callable_ref(self.chunker) if self.chunker is not None else "")
        setup_ref = self.setup_ref or (callable_ref(self.setup) if self.setup is not None else "")
        stage_chunk_ref = self.stage_chunk_ref or (callable_ref(self.stage_chunk) if self.stage_chunk is not None else "")
        finalize_ref = self.finalize_ref or (callable_ref(self.finalize) if self.finalize is not None else "")
        result_handler_ref = self.result_handler_ref or (
            callable_ref(self.result_handler_factory) if self.result_handler_factory is not None else ""
        )
        fail_policy = str(self.stage_fail_policy or "fail_fast").strip().lower()
        if fail_policy not in {"fail_fast", "continue_with_threshold"}:
            raise ValueError(
                f"invalid stage_fail_policy in JobDefinition '{self.name}': "
                f"stage_fail_policy='{self.stage_fail_policy}'. "
                "Use 'fail_fast' or 'continue_with_threshold'."
            )
        object.__setattr__(self, "chunker_ref", validate_importable_ref(chunker_ref, label="chunker_ref"))
        object.__setattr__(self, "setup_ref", validate_importable_ref(setup_ref, label="setup_ref"))
        object.__setattr__(self, "stage_chunk_ref", validate_importable_ref(stage_chunk_ref, label="stage_chunk_ref"))
        object.__setattr__(self, "finalize_ref", validate_importable_ref(finalize_ref, label="finalize_ref"))
        object.__setattr__(self, "stage_fail_policy", fail_policy)
        object.__setattr__(self, "max_stage_failures", max(0, int(self.max_stage_failures)))
        object.__setattr__(self, "output_flush_every", max(1, int(self.output_flush_every)))
        object.__setattr__(
            self,
            "result_handler_ref",
            validate_importable_ref(result_handler_ref, label="result_handler_ref"),
        )
        object.__setattr__(self, "required", tuple(self.required or ()))
        object.__setattr__(self, "produces", tuple(self.produces or ()))

    def with_options(self, **changes) -> "JobDefinition":
        data = {**changes}
        if "task" not in data:
            data["task"] = self.task
        if "chunker" not in data:
            data["chunker"] = self.chunker
        if "setup" not in data:
            data["setup"] = self.setup
        if "stage_chunk" not in data:
            data["stage_chunk"] = self.stage_chunk
        if "finalize" not in data:
            data["finalize"] = self.finalize
        if "output_spec" not in data:
            data["output_spec"] = self.output_spec
        if "result_handler_factory" not in data:
            data["result_handler_factory"] = self.result_handler_factory
        return replace(self, **data)

    def __call__(self, params: dict | BaseModel, config: Optional[dict] = None):
        if self.chunker is None:
            return list(self.build_chunks(params, config=config))
        return _call_with_optional_config(self.chunker, self.validate_params(params), dict(config or {}))

    def validate_params(self, params: dict | BaseModel) -> dict:
        if self.params_model is None:
            return self.task.validate_payload(params)
        if isinstance(params, self.params_model):
            validated = params
        else:
            validated = self.params_model.model_validate(params)
        return validated.model_dump(mode="python")

    def build_chunks(self, params: dict | BaseModel, *, config: Optional[dict] = None) -> Iterable[dict]:
        validated = self.validate_params(params)
        config_map = dict(config or {})

        if self.chunker is not None:
            produced = _call_with_optional_config(self.chunker, validated, config_map)
        elif self.chunker_ref:
            produced = _call_with_optional_config(resolve_callable_ref(self.chunker_ref), validated, config_map)
        else:
            produced = [validated]

        if isinstance(produced, dict):
            produced = [produced]

        def _iter() -> Iterator[dict]:
            for item in produced:
                yield self.task.validate_payload(item)

        return _iter()

    def build_result_handler(self, *args, **kwargs):
        if self.result_handler_factory is not None:
            return self.result_handler_factory(*args, **kwargs)
        if self.result_handler_ref:
            factory = resolve_callable_ref(self.result_handler_ref)
            return factory(*args, **kwargs)
        return None

    def scalability_warnings(
        self,
        *,
        total_chunks: int | None = None,
        max_inflight_tasks: int | None = None,
        sink_max_buffer_factor: int | None = None,
        sink_max_buffer_bytes: int | None = None,
        sink_max_payload_bytes: int | None = None,
    ) -> tuple[GuardrailWarning, ...]:
        return build_scalability_guardrails(
            executor=self.executor,
            output_spec=self.output_spec if isinstance(self.output_spec, OutputSpec) else None,
            output_flush_every=self.output_flush_every,
            store_results=bool(self.store_results),
            total_chunks=total_chunks,
            max_inflight_tasks=max_inflight_tasks,
            sink_max_buffer_factor=sink_max_buffer_factor,
            sink_max_buffer_bytes=sink_max_buffer_bytes,
            sink_max_payload_bytes=sink_max_payload_bytes,
        )

    def submit(self, molsuite, *, params: dict | BaseModel, config: Optional[dict] = None, **kwargs) -> str:
        return molsuite.submit_job(self, params=params, config=config, **kwargs)

    def submit_with_options(
        self,
        molsuite,
        *,
        params: dict | BaseModel,
        config: Optional[dict] = None,
        job_options: Optional[dict[str, Any]] = None,
        **kwargs,
    ) -> str:
        job_def = self.with_options(**dict(job_options or {})) if job_options else self
        return job_def.submit(molsuite, params=params, config=config, **kwargs)

    def run(
        self,
        molsuite,
        *,
        params: dict | BaseModel,
        config: Optional[dict] = None,
        poll_s: float = 0.25,
        progress_cb=None,
        **kwargs,
    ):
        job_id = self.submit(molsuite, params=params, config=config, **kwargs)
        if progress_cb is None:
            return molsuite.wait_for_job(job_id, poll_s=poll_s)
        return molsuite.wait_for_job(job_id, poll_s=poll_s, progress_cb=progress_cb)

    def run_with_options(
        self,
        molsuite,
        *,
        params: dict | BaseModel,
        config: Optional[dict] = None,
        job_options: Optional[dict[str, Any]] = None,
        poll_s: float = 0.25,
        progress_cb=None,
        **kwargs,
    ):
        job_def = self.with_options(**dict(job_options or {})) if job_options else self
        return job_def.run(
            molsuite,
            params=params,
            config=config,
            poll_s=poll_s,
            progress_cb=progress_cb,
            **kwargs,
        )


class JobSpec:
    """
    Class-based declaration for larger jobs.

    `JobSpec` is intentionally a declaration layer only. It compiles to the
    existing `JobDefinition`, so executors, staging, result handlers, and
    re-importable callable references keep the current behavior.
    """

    name: str = ""
    task_name: str = ""
    description: str = ""
    params_model: Optional[type[BaseModel]] = None
    input_model: Optional[type[BaseModel]] = None
    executor: str = "compute"
    supported_executors: tuple[str, ...] = ()
    cpu_required: int | None = 1
    stage_fail_policy: str = "fail_fast"
    max_stage_failures: int = 0
    output_spec: Any = None
    output_flush_every: int = 500
    store_results: bool = True
    tags: tuple[str, ...] = ()
    required: tuple[RequirementSpec, ...] = ()
    produces: tuple[CapabilitySpec, ...] = ()

    @staticmethod
    def build_chunks(params: dict, config: dict | None = None) -> Iterable[dict]:
        del config
        yield params

    @staticmethod
    def run_chunk(payload: dict, progress=None) -> Any:
        del progress
        raise NotImplementedError("JobSpec subclasses must implement run_chunk().")

    @classmethod
    def _optional_callable(cls, name: str) -> Callable[..., Any] | None:
        if name not in cls.__dict__:
            return None
        value = getattr(cls, name)
        return value if callable(value) else None

    @classmethod
    def to_task_definition(cls) -> TaskDefinition:
        normalized_name = str(cls.name or cls.__name__).strip() or cls.__name__
        task_name = str(cls.task_name or f"{normalized_name}_task").strip() or f"{normalized_name}_task"
        return TaskDefinition(
            name=task_name,
            fn=cls.run_chunk,
            handler_ref=validate_importable_callable(cls.run_chunk),
            description=str(cls.description or ""),
            executor=str(cls.executor or "compute"),
            supported_executors=tuple(cls.supported_executors or (cls.executor or "compute",)),
            cpu_required=1 if cls.cpu_required is None else int(cls.cpu_required),
            input_model=cls.input_model,
            tags=tuple(cls.tags or ()),
        )

    @classmethod
    def scalability_warnings(
        cls,
        *,
        total_chunks: int | None = None,
        max_inflight_tasks: int | None = None,
        sink_max_buffer_factor: int | None = None,
        sink_max_buffer_bytes: int | None = None,
        sink_max_payload_bytes: int | None = None,
    ) -> tuple[GuardrailWarning, ...]:
        return cls.to_job_definition().scalability_warnings(
            total_chunks=total_chunks,
            max_inflight_tasks=max_inflight_tasks,
            sink_max_buffer_factor=sink_max_buffer_factor,
            sink_max_buffer_bytes=sink_max_buffer_bytes,
            sink_max_payload_bytes=sink_max_payload_bytes,
        )

    @classmethod
    def to_job_definition(cls) -> JobDefinition:
        normalized_name = str(cls.name or cls.__name__).strip() or cls.__name__
        setup = cls._optional_callable("setup")
        stage_chunk = cls._optional_callable("stage_chunk")
        finalize = cls._optional_callable("finalize")
        result_handler_factory = cls._optional_callable("result_handler_factory")
        return JobDefinition(
            name=normalized_name,
            task=cls.to_task_definition(),
            description=str(cls.description or ""),
            params_model=cls.params_model,
            chunker=cls.build_chunks,
            setup=setup,
            stage_chunk=stage_chunk,
            finalize=finalize,
            stage_fail_policy=str(cls.stage_fail_policy or "fail_fast"),
            max_stage_failures=int(cls.max_stage_failures or 0),
            output_spec=cls.output_spec,
            output_flush_every=int(cls.output_flush_every or 500),
            result_handler_factory=result_handler_factory,
            executor=str(cls.executor or "compute"),
            supported_executors=tuple(cls.supported_executors or (cls.executor or "compute",)),
            cpu_required=cls.cpu_required,
            store_results=bool(cls.store_results),
            tags=tuple(cls.tags or ()),
            required=tuple(cls.required or ()),
            produces=tuple(cls.produces or ()),
        )

def task(
    *,
    name: Optional[str] = None,
    description: str = "",
    executor: str = "compute",
    supported_executors: Iterable[str] = (),
    cpu_required: int = 1,
    input_model: Optional[type[BaseModel]] = None,
    tags: Iterable[str] = (),
):
    def _decorator(fn: Callable[..., Any]) -> TaskDefinition:
        handler_ref = validate_importable_callable(fn)
        return TaskDefinition(
            name=(name or fn.__name__).strip() or fn.__name__,
            fn=fn,
            handler_ref=handler_ref,
            description=description,
            executor=executor,
            supported_executors=tuple(supported_executors),
            cpu_required=cpu_required,
            input_model=input_model,
            tags=tuple(tags),
        )

    return _decorator


def job(
    *,
    task: TaskDefinition,
    name: Optional[str] = None,
    description: str = "",
    params_model: Optional[type[BaseModel]] = None,
    executor: str = "",
    supported_executors: Iterable[str] = (),
    cpu_required: Optional[int] = None,
    setup: Optional[Callable[..., Any]] = None,
    stage_chunk: Optional[Callable[..., Any]] = None,
    finalize: Optional[Callable[..., Any]] = None,
    stage_fail_policy: str = "fail_fast",
    max_stage_failures: int = 0,
    output_spec: Any = None,
    output_flush_every: int = 500,
    result_handler_factory: Optional[Callable[..., Any]] = None,
    store_results: bool = True,
    tags: Iterable[str] = (),
):
    def _decorator(chunker: Callable[..., Any]) -> JobDefinition:
        validate_importable_callable(chunker)
        if setup is not None:
            validate_importable_callable(setup)
        if stage_chunk is not None:
            validate_importable_callable(stage_chunk)
        if finalize is not None:
            validate_importable_callable(finalize)
        if result_handler_factory is not None:
            validate_importable_callable(result_handler_factory)
        return JobDefinition(
            name=(name or chunker.__name__).strip() or chunker.__name__,
            task=task,
            description=description,
            params_model=params_model,
            chunker=chunker,
            result_handler_factory=result_handler_factory,
            executor=executor,
            supported_executors=tuple(supported_executors),
            cpu_required=cpu_required,
            setup=setup,
            stage_chunk=stage_chunk,
            finalize=finalize,
            stage_fail_policy=stage_fail_policy,
            max_stage_failures=max_stage_failures,
            output_spec=output_spec,
            output_flush_every=output_flush_every,
            store_results=store_results,
            tags=tuple(tags),
        )

    return _decorator


__all__ = [
    "build_scalability_guardrails",
    "CapabilitySpec",
    "GuardrailWarning",
    "JobDefinition",
    "JobSpec",
    "RequirementSpec",
    "TaskDefinition",
    "job",
    "task",
]
