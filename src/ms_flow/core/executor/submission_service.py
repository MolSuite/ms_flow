from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Iterator, List, Optional, Union
from uuid import UUID

from sqlmodel import select

from ms_flow.core.data import DataContext, OutputSpec, from_wire_value, to_wire_value
from ms_flow.core.database import ProjectStore
from ms_flow.core.database.executor_models import ExecutorJob
from ms_flow.core.executor.dispatch_model import DispatchPolicy
from ms_flow.core.executor.result_handlers import OutputSpecResultHandler, ResultHandler
from ms_flow.core.executor.runtime_state import JobFeed, JobLifecycle
from ms_flow.core.executor.runner_refs import (
    RunnerRef,
    call_with_optional_context,
    normalize_runner,
    normalize_uuid,
    ref_to_str,
    resolve_runner,
    str_to_ref,
)
from ms_flow.core.executor.utils import _safe_json_dumps, _safe_json_loads
from ms_flow.tasking import GuardrailWarning, build_scalability_guardrails

if TYPE_CHECKING:
    from ms_flow.core.executor.manager import ExecutorManager


def _normalize_optional_ref(value: Optional[Union[Callable, str, RunnerRef]]) -> str:
    if value in (None, ""):
        return ""
    ref = normalize_runner(value)
    return ref_to_str(ref)


def _normalize_output_spec(value: Any) -> Optional[OutputSpec]:
    if value is None:
        return None
    decoded = from_wire_value(to_wire_value(value))
    if isinstance(decoded, OutputSpec):
        return decoded
    raise ValueError("output_spec must be an OutputSpec instance (or wire-encoded OutputSpec).")


@dataclass(frozen=True)
class PreparedSubmit:
    job_id: str
    normalized_project_id: UUID | None
    queue_policy: str
    lifecycle: JobLifecycle
    payload: dict[str, Any]
    dispatch_policy: DispatchPolicy
    feed_total_chunks: int | None
    default_cpu_required: int
    default_gpu_required: int
    output_spec: OutputSpec | None
    output_flush_every: int
    store_results: bool
    handler_instance: ResultHandler | None
    guardrail_warnings: tuple[GuardrailWarning, ...]


class SubmissionService:
    def __init__(self, manager: "ExecutorManager"):
        self.manager = manager

    def _prepare_submit(
        self,
        *,
        executor_name: str,
        chunks: List[dict] | Iterator[dict],
        run_chunk: Callable | str | RunnerRef,
        project_id: UUID | str | None,
        priority: int,
        queue_policy: str,
        default_cpu_required: int,
        default_gpu_required: int = 0,
        max_job_cpu: int | None,
        job_payload: dict[str, Any] | None,
        job_id: str | None,
        batch_size: int | str,
        max_inflight_tasks: int,
        max_inflight_items: int | None,
        prefetch_factor: float,
        refill_threshold: int,
        result_handler: ResultHandler | None,
        store_results: bool,
        setup_ref: Callable | str | RunnerRef | None,
        stage_ref: Callable | str | RunnerRef | None,
        finalize_ref: Callable | str | RunnerRef | None,
        stage_fail_policy: str,
        max_stage_failures: int,
        output_spec: Any,
        output_flush_every: int,
        total_chunks: int | None,
    ) -> PreparedSubmit:
        if self.manager.executor_db is None:
            raise RuntimeError("ExecutorManager has no executor_db bound. Open a project first.")
        if executor_name not in self.manager._executors:
            raise ValueError(f"Unknown executor: {executor_name}")

        try:
            runner_ref = normalize_runner(run_chunk)
        except (ValueError, TypeError) as exc:
            raise ValueError(f"Invalid run_chunk: {exc}") from exc

        normalized_project_id = normalize_uuid(project_id)
        normalized_queue_policy = queue_policy if queue_policy in {"fifo", "priority"} else "fifo"
        normalized_job_id = job_id or uuid.uuid4().hex
        normalized_setup_ref = _normalize_optional_ref(setup_ref)
        normalized_stage_ref = _normalize_optional_ref(stage_ref)
        normalized_finalize_ref = _normalize_optional_ref(finalize_ref)
        normalized_output_spec = _normalize_output_spec(output_spec)
        normalized_output_flush_every = max(1, int(output_flush_every))
        normalized_stage_fail_policy = str(stage_fail_policy or "fail_fast").strip().lower()
        if normalized_stage_fail_policy not in {"fail_fast", "continue_with_threshold"}:
            raise ValueError(
                f"Invalid stage_fail_policy='{stage_fail_policy}' for submit_job(). "
                "Use 'fail_fast' or 'continue_with_threshold'."
            )
        if total_chunks is None and isinstance(chunks, (list, tuple)):
            total_chunks = len(chunks)

        lifecycle = JobLifecycle(
            setup_ref=normalized_setup_ref,
            stage_ref=normalized_stage_ref,
            finalize_ref=normalized_finalize_ref,
            stage_fail_policy=normalized_stage_fail_policy,
            max_stage_failures=max(0, int(max_stage_failures)),
        )
        dispatch_policy = DispatchPolicy(
            batch_size=batch_size,
            max_inflight_tasks=max_inflight_tasks,
            max_inflight_items=max_inflight_items,
            prefetch_factor=prefetch_factor,
            refill_threshold=refill_threshold,
        )
        normalized_default_cpu_required = max(1, int(default_cpu_required))
        normalized_default_gpu_required = max(0, int(default_gpu_required))
        guardrail_warnings = build_scalability_guardrails(
            executor=executor_name,
            output_spec=normalized_output_spec,
            output_flush_every=normalized_output_flush_every,
            store_results=bool(store_results),
            total_chunks=total_chunks,
            max_inflight_tasks=max_inflight_tasks,
            sink_max_buffer_factor=self.manager._output_sink_max_buffer_factor,
            sink_max_buffer_bytes=self.manager._output_sink_max_buffer_bytes,
            sink_max_payload_bytes=self.manager._output_sink_max_payload_bytes,
        )

        full_payload = dict(job_payload or {})
        full_payload["_runner_ref"] = runner_ref
        full_payload["_lifecycle"] = {
            "setup_ref": lifecycle.setup_ref,
            "stage_ref": lifecycle.stage_ref,
            "finalize_ref": lifecycle.finalize_ref,
            "stage_fail_policy": lifecycle.stage_fail_policy,
            "max_stage_failures": lifecycle.max_stage_failures,
        }
        full_payload["_dispatch_policy"] = dispatch_policy.to_mapping()
        full_payload["_output_spec"] = (
            to_wire_value(normalized_output_spec) if normalized_output_spec is not None else None
        )
        full_payload["_output_flush_every"] = normalized_output_flush_every
        full_payload["_default_cpu_required"] = normalized_default_cpu_required
        full_payload["_default_gpu_required"] = normalized_default_gpu_required
        full_payload["_max_job_cpu"] = None if max_job_cpu is None else max(1, int(max_job_cpu))
        full_payload["_store_results"] = bool(store_results)
        full_payload["_total_chunks"] = total_chunks
        full_payload["_guardrail_warnings"] = [item.to_mapping() for item in guardrail_warnings]
        handler_instance = result_handler
        if handler_instance is None and normalized_output_spec is not None:
            handler_instance = self.build_output_handler(
                job_id=normalized_job_id,
                output_spec=normalized_output_spec,
                data_context_mapping=full_payload.get("_data_context") or {},
                flush_every=normalized_output_flush_every,
            )

        return PreparedSubmit(
            job_id=normalized_job_id,
            normalized_project_id=normalized_project_id,
            queue_policy=normalized_queue_policy,
            lifecycle=lifecycle,
            payload=full_payload,
            dispatch_policy=dispatch_policy,
            feed_total_chunks=total_chunks,
            default_cpu_required=normalized_default_cpu_required,
            default_gpu_required=normalized_default_gpu_required,
            output_spec=normalized_output_spec,
            output_flush_every=normalized_output_flush_every,
            store_results=bool(store_results),
            handler_instance=handler_instance,
            guardrail_warnings=guardrail_warnings,
        )

    def submit_job(
        self,
        *,
        executor_name: str,
        chunks: List[dict] | Iterator[dict],
        run_chunk: Callable | str | RunnerRef,
        project_id: UUID | str | None = None,
        origin_id: str = "",
        task_type: str = "",
        priority: int = 0,
        queue_policy: str = "fifo",
        default_cpu_required: int = 1,
        default_gpu_required: int = 0,
        max_job_cpu: int | None = None,
        job_payload: dict[str, Any] | None = None,
        job_id: str | None = None,
        batch_size: int | str = 1,
        max_inflight_tasks: int = 16,
        max_inflight_items: int | None = None,
        prefetch_factor: float = 1.0,
        refill_threshold: int = 1,
        result_handler: ResultHandler | None = None,
        store_results: bool = True,
        setup_ref: Callable | str | RunnerRef | None = None,
        stage_ref: Callable | str | RunnerRef | None = None,
        finalize_ref: Callable | str | RunnerRef | None = None,
        stage_fail_policy: str = "fail_fast",
        max_stage_failures: int = 0,
        output_spec: Any = None,
        output_flush_every: int = 500,
        total_chunks: int | None = None,
        depends_on: list[str] | None = None,
        chunk_fail_fast_min_processed: int | None = None,
        chunk_fail_fast_max_failed_ratio: float | None = None,
        chunk_fail_fast_max_consecutive_failures: int | None = None,
        attached_resources: list[Any] | None = None,
    ) -> str:
        payload_with_fail_fast = dict(job_payload or {})
        fail_fast_payload: dict[str, Any] = {}
        if chunk_fail_fast_min_processed is not None:
            fail_fast_payload["min_processed"] = max(1, int(chunk_fail_fast_min_processed))
        if chunk_fail_fast_max_failed_ratio is not None:
            ratio = float(chunk_fail_fast_max_failed_ratio)
            if not 0.0 <= ratio <= 1.0:
                raise ValueError("chunk_fail_fast_max_failed_ratio must be between 0.0 and 1.0.")
            fail_fast_payload["max_failed_ratio"] = ratio
        if chunk_fail_fast_max_consecutive_failures is not None:
            fail_fast_payload["max_consecutive_failures"] = max(1, int(chunk_fail_fast_max_consecutive_failures))
        if fail_fast_payload:
            payload_with_fail_fast["_chunk_fail_fast"] = fail_fast_payload

        deferred_chunk_build_pending = bool((job_payload or {}).get("_deferred_chunk_build_pending"))
        prepared = self._prepare_submit(
            executor_name=executor_name,
            chunks=chunks,
            run_chunk=run_chunk,
            project_id=project_id,
            priority=priority,
            queue_policy=queue_policy,
            default_cpu_required=default_cpu_required,
            default_gpu_required=default_gpu_required,
            max_job_cpu=max_job_cpu,
            job_payload=payload_with_fail_fast,
            job_id=job_id,
            batch_size=batch_size,
            max_inflight_tasks=max_inflight_tasks,
            max_inflight_items=max_inflight_items,
            prefetch_factor=prefetch_factor,
            refill_threshold=refill_threshold,
            result_handler=result_handler,
            store_results=store_results,
            setup_ref=setup_ref,
            stage_ref=stage_ref,
            finalize_ref=finalize_ref,
            stage_fail_policy=stage_fail_policy,
            max_stage_failures=max_stage_failures,
            output_spec=output_spec,
            output_flush_every=output_flush_every,
            total_chunks=total_chunks,
        )

        self.manager.job_store.create_job(
            job_id=prepared.job_id,
            project_id=prepared.normalized_project_id,
            origin_id=origin_id,
            task_type=task_type,
            executor_name=executor_name,
            queue_policy=prepared.queue_policy,
            priority=int(priority),
            payload_json=_safe_json_dumps(prepared.payload),
            depends_on_json=_safe_json_dumps(depends_on or []),
            total_chunks=prepared.feed_total_chunks,
        )
        feed = JobFeed(
            job_id=prepared.job_id,
            executor_name=executor_name,
            item_source=None if deferred_chunk_build_pending else iter(chunks),
            dispatch_policy=prepared.dispatch_policy,
            default_cpu_required=prepared.default_cpu_required,
            default_gpu_required=prepared.default_gpu_required,
            source_ready=not deferred_chunk_build_pending,
            total_chunks=prepared.feed_total_chunks,
            attached_resources=list(attached_resources or []),
        )

        self.manager.register_job_runtime(
            job_id=prepared.job_id,
            runner_ref=prepared.payload["_runner_ref"],
            feed=feed,
            lifecycle=prepared.lifecycle,
            store_results=prepared.store_results,
            handler=prepared.handler_instance,
        )

        if self.manager._thread is None or not self.manager._thread.is_alive():
            self.manager._feed_all_windows()

        self.manager._add_event(
            prepared.job_id,
            level="INFO",
            event_type="job_submitted",
            message=(
                f"Job submitted to '{executor_name}' "
                f"(runner={ref_to_str(prepared.payload['_runner_ref'])} "
                f"inflight={prepared.dispatch_policy.max_inflight_tasks})"
            ),
        )
        for warning in prepared.guardrail_warnings:
            self.manager._add_event(
                prepared.job_id,
                level="WARNING",
                event_type="job_configuration_warning",
                message=warning.message,
                payload=warning.to_mapping(),
            )
            self.manager._log_executor(
                logging.WARNING,
                "Job %s configuration warning code=%s executor=%s message=%s",
                prepared.job_id,
                warning.code,
                executor_name,
                warning.message,
                extra={
                    "job_id": prepared.job_id,
                    "project_id": str(prepared.normalized_project_id) if prepared.normalized_project_id else "",
                    "guardrail_code": warning.code,
                },
            )
        self.manager._log_executor(
            logging.INFO,
            "Submitted job %s executor=%s runner=%s inflight=%s priority=%s",
            prepared.job_id,
            executor_name,
            ref_to_str(prepared.payload["_runner_ref"]),
            prepared.dispatch_policy.max_inflight_tasks,
            priority,
            extra={
                "job_id": prepared.job_id,
                "project_id": str(prepared.normalized_project_id) if prepared.normalized_project_id else "",
            },
        )
        self.manager.signal_runtime_work()
        return prepared.job_id

    def resubmit_job(
        self,
        source_job_id: str,
        *,
        executor_name: str | None = None,
        cpu_required: int | None = None,
        queue_policy: str | None = None,
        priority: int | None = None,
        project_id: UUID | str | None = None,
        store_results: bool | None = None,
        output_spec: Any = None,
        output_flush_every: int | None = None,
    ) -> str:
        if self.manager.executor_db is None:
            raise RuntimeError("ExecutorManager has no executor_db bound. Open a project first.")

        with self.manager.executor_db.get_session() as session:
            source_job = session.exec(select(ExecutorJob).where(ExecutorJob.job_id == source_job_id)).first()
        if source_job is None:
            raise RuntimeError(f"No job found to resubmit: {source_job_id}")

        payload = _safe_json_loads(source_job.payload_json)
        chunker_ref = str(payload.get("_chunker_ref") or "").strip()
        if not chunker_ref:
            raise ValueError(
                f"Job '{source_job_id}' cannot be resubmitted automatically because it has no "
                "persisted _chunker_ref. Use declarative jobs (`submit_job(job_def, ...)`, "
                "`workflow(...)`, `run(...)`), or rebuild the chunks manually."
            )

        lifecycle_meta = dict(payload.get("_lifecycle") or {})
        chunker_fn = resolve_runner(str_to_ref(chunker_ref))
        config, resources = self.build_chunk_source_config(
            job=source_job,
            payload=payload,
            lifecycle_meta=lifecycle_meta,
            cursor_position=0,
        )
        produced = call_with_optional_context(chunker_fn, payload.get("_chunker_params") or {}, config)
        if isinstance(produced, dict):
            produced = [produced]

        dispatch_policy = DispatchPolicy.from_mapping(payload.get("_dispatch_policy"))
        selected_executor = str(executor_name or source_job.executor_name).strip() or source_job.executor_name
        supported_executors = tuple(
            str(item).strip()
            for item in (payload.get("_supported_executors") or ())
            if str(item).strip()
        )
        if supported_executors and selected_executor not in supported_executors:
            raise ValueError(
                f"Executor '{selected_executor}' no soportado al reenviar job '{source_job_id}'. "
                f"Compatibles: {supported_executors}."
            )
        selected_queue_policy = (
            queue_policy if queue_policy in {"fifo", "priority"} else (source_job.queue_policy or "fifo")
        )
        selected_priority = int(source_job.priority if priority is None else priority)
        selected_project_id = normalize_uuid(project_id if project_id is not None else source_job.project_id)
        selected_output_spec = output_spec if output_spec is not None else payload.get("_output_spec")
        selected_output_flush_every = (
            int(output_flush_every)
            if output_flush_every is not None
            else int(payload.get("_output_flush_every", 500) or 500)
        )
        selected_cpu_required = max(
            1,
            int(cpu_required if cpu_required is not None else payload.get("_default_cpu_required", 1) or 1),
        )
        selected_store_results = bool(
            payload.get("_store_results", True) if store_results is None else store_results
        )

        cloned_payload = dict(payload)
        cloned_payload["_default_cpu_required"] = selected_cpu_required
        cloned_payload["_store_results"] = selected_store_results

        return self.submit_job(
            executor_name=selected_executor,
            chunks=iter(produced or ()),
            run_chunk=payload.get("_runner_ref"),
            project_id=selected_project_id,
            origin_id=source_job.origin_id,
            task_type=source_job.task_type,
            priority=selected_priority,
            queue_policy=selected_queue_policy,
            default_cpu_required=selected_cpu_required,
            job_payload=cloned_payload,
            batch_size=dispatch_policy.batch_size,
            max_inflight_tasks=dispatch_policy.max_inflight_tasks,
            max_inflight_items=dispatch_policy.max_inflight_items,
            prefetch_factor=dispatch_policy.prefetch_factor,
            refill_threshold=dispatch_policy.refill_threshold,
            store_results=selected_store_results,
            setup_ref=str(lifecycle_meta.get("setup_ref", "") or ""),
            stage_ref=str(lifecycle_meta.get("stage_ref", "") or ""),
            finalize_ref=str(lifecycle_meta.get("finalize_ref", "") or ""),
            stage_fail_policy=str(lifecycle_meta.get("stage_fail_policy", "fail_fast") or "fail_fast"),
            max_stage_failures=max(0, int(lifecycle_meta.get("max_stage_failures", 0) or 0)),
            output_spec=selected_output_spec,
            output_flush_every=selected_output_flush_every,
            attached_resources=resources,
        )

    def build_chunk_source_config(
        self,
        *,
        job: ExecutorJob,
        payload: dict[str, Any],
        lifecycle_meta: dict[str, Any],
        cursor_position: int,
    ) -> tuple[dict[str, Any], list[Any]]:
        config: dict[str, Any] = {}
        resources: list[Any] = []
        data_context = payload.get("_data_context") or {}
        if isinstance(data_context, dict):
            config.update(dict(data_context))

        project_path = str(config.get("project_path") or "").strip()
        project_db_path = str(config.get("project_db_path") or "").strip()
        if not project_db_path and project_path:
            project_db_path = str(Path(project_path).expanduser().resolve() / "project.db")
        if project_db_path:
            project_db = ProjectStore()
            project_db.set_db_path(project_db_path)
            project_db.setup()
            project_db.project_dir = Path(project_path).expanduser().resolve() if project_path else None
            config["project_db"] = project_db
            config["project_store"] = project_db
            resources.append(project_db)

        config["job_id"] = job.job_id
        config["project_id"] = str(job.project_id) if job.project_id else ""
        config["setup_data"] = dict(lifecycle_meta.get("setup_data") or {})
        return config, resources

    def restore_chunk_source(
        self,
        *,
        job: ExecutorJob,
        payload: dict[str, Any],
        lifecycle_meta: dict[str, Any],
        cursor_position: int,
    ) -> tuple[Iterator[dict], list[Any]]:
        chunker_ref = payload.get("_chunker_ref")
        chunker_params = payload.get("_chunker_params")
        if not chunker_ref:
            raise RuntimeError("deferred chunk build requires a persisted _chunker_ref.")

        chunker_fn = resolve_runner(str_to_ref(str(chunker_ref)))
        config, resources = self.build_chunk_source_config(
            job=job,
            payload=payload,
            lifecycle_meta=lifecycle_meta,
            cursor_position=cursor_position,
        )
        produced = call_with_optional_context(chunker_fn, chunker_params or {}, config)
        if isinstance(produced, dict):
            produced = [produced]
        return iter(produced or ()), resources

    def build_output_handler(
        self,
        *,
        job_id: str,
        output_spec: OutputSpec,
        data_context_mapping: dict[str, Any],
        flush_every: int,
    ) -> ResultHandler:
        flush_size = max(1, int(flush_every))
        max_buffer_size = max(flush_size, flush_size * self.manager._output_sink_max_buffer_factor)
        data_context = DataContext.from_mapping(data_context_mapping)
        return OutputSpecResultHandler(
            executor_db=self.manager.executor_db,
            job_id=job_id,
            bridge=self.manager._data_bridge,
            output_spec=output_spec,
            data_context=data_context,
            flush_every=flush_size,
            max_buffer_size=max_buffer_size,
            max_buffer_bytes=self.manager._output_sink_max_buffer_bytes,
            max_payload_bytes=self.manager._output_sink_max_payload_bytes,
            max_pending_chunks=self.manager._output_sink_max_pending_chunks,
            max_pending_bytes=self.manager._output_sink_max_pending_bytes,
            flush_retries=self.manager._output_sink_flush_retries,
            retry_backoff_s=self.manager._output_sink_retry_backoff_s,
        )
