from __future__ import annotations

import functools
import time
from typing import TYPE_CHECKING, Any, Callable, Optional
from uuid import UUID

from ms_flow.core.data import TableOutputSpec
from ms_flow.core.executor.dispatch_model import DispatchPolicy
from ms_flow.core.executor.job_snapshot import JobSnapshot
from ms_flow.job_templates import batch_job, streaming_job

if TYPE_CHECKING:
    from ms_flow.tasking import JobDefinition, TaskDefinition


_OPERATIONAL_PROFILE_PRESETS: dict[str, dict[str, Any]] = {
    "strict": {
        "max_inflight_tasks": 8,
        "max_inflight_items": 128,
        "output_flush_every": 100,
    },
    "balanced": {
        "max_inflight_tasks": None,
        "max_inflight_items": None,
        "output_flush_every": None,
    },
    "throughput": {
        "max_inflight_tasks": 64,
        "max_inflight_items": 2048,
        "output_flush_every": 1000,
    },
}


def _normalize_operational_profile(value: Any) -> str:
    profile = str(value or "balanced").strip().lower()
    if profile not in _OPERATIONAL_PROFILE_PRESETS:
        raise ValueError("operational_profile must be 'strict', 'balanced' or 'throughput'.")
    return profile


def _has_inline_workflow_arguments(
    *,
    name: str | None,
    input: Any,
    process: Callable[..., Any] | Any | None,
    output: Any,
    description: str,
    task_name: str | None,
    setup: Optional[Callable[..., Any]],
    stage: Optional[Callable[..., Any]],
    finalize: Optional[Callable[..., Any]],
    result_handler: Optional[Callable[..., Any]],
    flush_every: int,
    executor: str,
    supported_executors,
    cpu_required: int | None,
    stage_fail_policy: str,
    max_stage_failures: int,
    store_results: bool,
    tags,
) -> bool:
    return any(
        (
            name is not None,
            input is not None,
            process is not None,
            output is not None,
            bool(description),
            task_name is not None,
            setup is not None,
            stage is not None,
            finalize is not None,
            result_handler is not None,
            flush_every != 500,
            bool(str(executor or "").strip()),
            bool(tuple(supported_executors or ())),
            cpu_required is not None,
            stage_fail_policy != "fail_fast",
            int(max_stage_failures) != 0,
            bool(store_results),
            bool(tuple(tags or ())),
        )
    )


class MolSuiteJobApiMixin:
    def run(
        self,
        spec=None,
        *,
        name: str | None = None,
        input=None,
        process: Callable[..., Any] | Any | None = None,
        output=None,
        description: str = "",
        task_name: str | None = None,
        setup: Optional[Callable[..., Any]] = None,
        stage: Optional[Callable[..., Any]] = None,
        finalize: Optional[Callable[..., Any]] = None,
        result_handler: Optional[Callable[..., Any]] = None,
        flush_every: int = 500,
        executor: str = "",
        supported_executors=(),
        cpu_required: int | None = None,
        stage_fail_policy: str = "fail_fast",
        max_stage_failures: int = 0,
        store_results: bool = False,
        tags=(),
        params: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
        queue_policy: str = "fifo",
        priority: int = 0,
    ) -> str:
        from ms_flow.specs.workflow import WorkflowLauncher, WorkflowSpec, workflow

        if spec is None:
            if input is None or process is None:
                raise ValueError(
                    "run(...) requires spec=... or the triple input=..., process=..., output=...."
                )
            resolved_name = str(name or "").strip()
            if not resolved_name:
                raise ValueError("run(name=...) cannot be empty when building the workflow inline.")
            spec = workflow(
                name=resolved_name,
                input=input,
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
                supported_executors=tuple(supported_executors or ()),
                cpu_required=cpu_required,
                stage_fail_policy=stage_fail_policy,
                max_stage_failures=max_stage_failures,
                store_results=store_results,
                tags=tuple(tags or ()),
            )
        else:
            if _has_inline_workflow_arguments(
                name=name,
                input=input,
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
                supported_executors=supported_executors,
                cpu_required=cpu_required,
                stage_fail_policy=stage_fail_policy,
                max_stage_failures=max_stage_failures,
                store_results=store_results,
                tags=tags,
            ):
                raise ValueError(
                    "run(spec=...) does not accept inline workflow parameters. "
                    "Use spec=... alone, or the simple path name=..., input=..., process=..., output=...."
                )
            if not isinstance(spec, WorkflowSpec):
                raise TypeError(
                    "run(spec=...) requires a WorkflowSpec. "
                    "For a JobDefinition use job_def.submit(...), job_def.run(...) or submit_job(job_def, ...)."
                )

        return WorkflowLauncher(self).submit(
            spec,
            params=params,
            config=config,
            queue_policy=queue_policy,
            priority=priority,
        )

    def task(self, name: str, **kwargs):
        """Decorator to define a task."""

        def decorator(func):
            from ms_flow.tasking import task

            t = task(name=name, handler=func, **kwargs)
            return t

        return decorator

    def job(self, name: str, task: Any, **kwargs):
        """Decorator to define a job."""

        def decorator(func):
            from ms_flow.tasking import job

            j = job(name=name, task=task, chunker=func, **kwargs)
            return j

        return decorator

    def _resolve_sink(self, sink: Any) -> Any:
        """Resolve sink object. If string, convert to TableOutputSpec."""
        if isinstance(sink, str):
            return TableOutputSpec(table_name=sink)
        return sink

    def submit_job(
        self,
        job: "JobDefinition",
        *,
        params: dict[str, Any] | Any | None = None,
        chunks=None,
        config: dict[str, Any] | None = None,
        project_id: str | UUID | None = None,
        depends_on: list[str] | None = None,
        queue_policy: str = "fifo",
        priority: int = 0,
        executor_name: str | None = None,
        cpu_required: int | None = None,
        max_job_cpu: int | None = None,
        max_inflight_tasks: int | None = None,
        max_inflight_items: int | None = None,
        total_chunks: int | None = None,
        chunk_fail_fast_min_processed: int | None = None,
        chunk_fail_fast_max_failed_ratio: float | None = None,
        chunk_fail_fast_max_consecutive_failures: int | None = None,
        store_results: bool | None = None,
        output_spec: Any = None,
        output_flush_every: int | None = None,
        operational_profile: str | None = None,
        result_handler=None,
        result_handler_args: tuple[Any, ...] = (),
        result_handler_kwargs: dict[str, Any] | None = None,
    ) -> str:
        if self.active_context is None:
            if project_id is None:
                raise RuntimeError(
                    f"There is no active project to run job '{job.name}'. "
                    "Use create_or_open_project()/open_project(), or pass project_id."
                )
            self.open_project(project_id)

        self._require_runtime()
        if self.executor_manager is None:
            raise RuntimeError("ExecutorManager no inicializado.")

        submit_config = dict(config or {})
        effective_project_id = project_id
        if effective_project_id is None and self.active_context is not None:
            effective_project_id = str(self.active_context.id)

        selected_executor = str(executor_name or job.executor).strip() or job.executor
        if selected_executor not in job.supported_executors:
            raise ValueError(
                f"Executor '{selected_executor}' is not supported by job '{job.name}'. "
                f"Compatible: {job.supported_executors}."
            )

        prepared_chunks = chunks
        deferred_chunk_build = False
        if prepared_chunks is None:
            if params is None:
                raise ValueError(
                    f"Job '{job.name}' requires params or chunks. "
                    "Pass params=... so MolSuite builds the chunks, or chunks=... if you already materialised them."
                )

            job_config = dict(submit_config)
            if "project_store" not in job_config:
                job_config["project_store"] = self.project_store
            if "project_db" not in job_config:
                job_config["project_db"] = self.project_db
            if "project_context" not in job_config:
                job_config["project_context"] = self.active_context
            if "project_resources" not in job_config:
                resource_map = self.get_project_resource_map(project_id=effective_project_id)
                if resource_map:
                    job_config["project_resources"] = resource_map

            if depends_on and job.chunker_ref:
                prepared_chunks = iter(())
                deferred_chunk_build = True
            else:
                prepared_chunks = job.build_chunks(params, config=job_config)

        handler_instance = result_handler
        if handler_instance is None:
            handler_instance = job.build_result_handler(
                *result_handler_args,
                **(result_handler_kwargs or {}),
            )

        effective_output_spec = self._resolve_sink(output_spec if output_spec is not None else job.output_spec)
        limits = self.settings_manager.settings.operational_limits
        profile = _normalize_operational_profile(
            operational_profile
            if operational_profile is not None
            else submit_config.get("operational_profile", limits.operational_profile)
        )
        preset = _OPERATIONAL_PROFILE_PRESETS[profile]
        effective_max_inflight_tasks = (
            max(1, int(max_inflight_tasks))
            if max_inflight_tasks is not None
            else int(preset["max_inflight_tasks"] or limits.default_max_inflight_tasks)
        )
        effective_max_inflight_items = (
            max(1, int(max_inflight_items))
            if max_inflight_items is not None
            else int(preset["max_inflight_items"] or limits.default_max_inflight_items)
        )
        effective_output_flush_every = (
            int(output_flush_every)
            if output_flush_every is not None
            else int(preset["output_flush_every"] or job.output_flush_every)
        )
        operational_policy = {
            "profile": profile,
            "max_inflight_tasks": effective_max_inflight_tasks,
            "max_inflight_items": effective_max_inflight_items,
            "output_flush_every": effective_output_flush_every,
        }

        return self.executor_manager.submit_job(
            executor_name=selected_executor,
            chunks=prepared_chunks,
            run_chunk=job.task.handler_ref,
            project_id=effective_project_id,
            origin_id=self.app_id,
            task_type=job.name,
            priority=priority,
            depends_on=depends_on,
            queue_policy=queue_policy,
            default_cpu_required=cpu_required or job.cpu_required,
            max_job_cpu=max_job_cpu,
            max_inflight_tasks=effective_max_inflight_tasks,
            max_inflight_items=effective_max_inflight_items,
            job_payload={
                "job_name": job.name,
                "task_name": job.task.name,
                "_operational_profile": operational_policy,
                "_supported_executors": list(job.supported_executors),
                "_chunker_ref": job.chunker_ref,
                "_chunker_params": params,
                "_dispatch_policy": DispatchPolicy(
                    max_inflight_tasks=effective_max_inflight_tasks,
                    max_inflight_items=effective_max_inflight_items,
                ).to_mapping(),
                "_default_cpu_required": cpu_required or job.cpu_required,
                "_deferred_chunk_build_pending": deferred_chunk_build,
                "_data_context": {
                    "project_path": str(self.active_context.path) if self.active_context is not None else "",
                    "project_db_path": (
                        str(self.project_db.db_path)
                        if self.project_db is not None and self.project_db.db_path is not None
                        else ""
                    ),
                    "executor_db_path": (
                        str(self.executor_db.db_path)
                        if self.executor_db is not None and self.executor_db.db_path is not None
                        else ""
                    ),
                    # Deferred chunk builds (depends_on + chunker) rebuild config from this
                    # context, so persist the JSON-safe resource map too — otherwise jobs that
                    # resolve a project resource at chunk-build time (e.g. chemistry's 'molecules'
                    # storage dir) fail with "Missing project resource" once they're deferred.
                    "project_resources": self.get_project_resource_map(project_id=effective_project_id),
                },
            },
            result_handler=handler_instance,
            store_results=job.store_results if store_results is None else bool(store_results),
            setup_ref=job.setup_ref,
            stage_ref=job.stage_chunk_ref,
            finalize_ref=job.finalize_ref,
            stage_fail_policy=job.stage_fail_policy,
            max_stage_failures=job.max_stage_failures,
            output_spec=effective_output_spec,
            output_flush_every=effective_output_flush_every,
            total_chunks=(
                max(0, int(total_chunks))
                if total_chunks is not None
                else len(prepared_chunks) if isinstance(prepared_chunks, list) else None
            ),
            chunk_fail_fast_min_processed=chunk_fail_fast_min_processed,
            chunk_fail_fast_max_failed_ratio=chunk_fail_fast_max_failed_ratio,
            chunk_fail_fast_max_consecutive_failures=chunk_fail_fast_max_consecutive_failures,
        )

    def wait_for_job(
        self,
        job_id: str,
        *,
        poll_s: float = 0.25,
        progress_cb: Callable[[JobSnapshot], None] | None = None,
    ) -> JobSnapshot:
        self._require_runtime()
        if self.executor_manager is None:
            raise RuntimeError("ExecutorManager no inicializado.")

        terminal = {"completed", "failed", "canceled"}
        while True:
            row = self.executor_manager.get_job(job_id)
            if row is None:
                raise RuntimeError(f"No se encontro job: {job_id}")
            if progress_cb is not None:
                progress_cb(row)
            if row.status in terminal:
                return row
            time.sleep(max(0.05, float(poll_s)))

    def purge_job_history(self, *, older_than_days: float = 30.0) -> dict[str, int]:
        """Prune chunks and events of finished jobs. Runs by itself when a project opens."""
        self._require_runtime()
        access = getattr(self, "executor_access", None)
        if access is None:
            self._require_executor_db()
            raise RuntimeError("ExecutorStore is not available for the active project.")
        return access.purge_finished_jobs(older_than_days=older_than_days)

    def get_job_outputs(self, job_id: str) -> list[dict[str, Any]]:
        self._require_runtime()
        access = getattr(self, "executor_access", None)
        if access is None:
            self._require_executor_db()
            raise RuntimeError("ExecutorStore is not available for the active project.")
        return access.list_job_outputs(job_id)

    def get_job_chunks(
        self,
        *,
        job_id: str | None = None,
        job_ids: list[str] | None = None,
        statuses: tuple[str, ...] | list[str] | None = None,
        limit: int | None = None,
        include_payload: bool = False,
    ) -> list[dict[str, Any]]:
        self._require_runtime()
        access = getattr(self, "executor_access", None)
        if access is None:
            self._require_executor_db()
            raise RuntimeError("ExecutorStore is not available for the active project.")
        return access.list_job_chunks(
            job_id=job_id,
            job_ids=job_ids,
            statuses=statuses,
            limit=limit,
            include_payload=include_payload,
        )

    def get_job_event_records(
        self,
        *,
        after_event_id: int = 0,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        self._require_runtime()
        access = getattr(self, "executor_access", None)
        if access is None:
            self._require_executor_db()
            raise RuntimeError("ExecutorStore is not available for the active project.")
        return access.list_job_events(
            after_event_id=after_event_id,
            limit=limit,
            ascending=True,
        )

    def get_job_events(self, job_id: str, limit: int | None = None) -> list[dict[str, Any]]:
        self._require_runtime()
        access = getattr(self, "executor_access", None)
        if access is None:
            self._require_executor_db()
            raise RuntimeError("ExecutorStore is not available for the active project.")
        rows = access.list_job_events(job_id=job_id, limit=limit, ascending=True)
        return [
            {
                "level": row["level"],
                "type": row["event_type"],
                "message": row["message"],
                "created_at": row["created_at"].isoformat(),
            }
            for row in rows
        ]

    def cancel_job(self, job_id: str) -> None:
        self._require_runtime()
        if self.executor_manager is None:
            raise RuntimeError("ExecutorManager no inicializado.")
        self.executor_manager.cancel_job(job_id)

    def resubmit_job(
        self,
        job_id: str,
        *,
        executor_name: str | None = None,
        cpu_required: int | None = None,
        queue_policy: str | None = None,
        priority: int | None = None,
        project_id: str | UUID | None = None,
        store_results: bool | None = None,
        output_spec: Any = None,
        output_flush_every: int | None = None,
    ) -> str:
        self._require_runtime()
        if self.executor_manager is None:
            raise RuntimeError("ExecutorManager no inicializado.")
        return self.executor_manager.resubmit_job(
            job_id,
            executor_name=executor_name,
            cpu_required=cpu_required,
            queue_policy=queue_policy,
            priority=priority,
            project_id=project_id,
            store_results=store_results,
            output_spec=output_spec,
            output_flush_every=output_flush_every,
        )

    def get_executor_status(self) -> dict[str, Any]:
        self._require_runtime()
        if self.executor_manager is None:
            raise RuntimeError("ExecutorManager no inicializado.")
        return self.executor_manager.get_status()

    def get_executor_capability_matrix(self) -> dict[str, dict[str, Any]]:
        self._require_runtime()
        if self.executor_manager is None:
            raise RuntimeError("ExecutorManager no inicializado.")
        return self.executor_manager.get_executor_capability_matrix()

    def activate_compute_backend(self, backend: str, **kwargs) -> dict[str, Any]:
        """Switch the logical ``compute`` executor between ``loky`` (local) and
        ``ray`` (cluster). Returns the resulting backend status dict."""
        self._require_runtime()
        if self.executor_manager is None:
            raise RuntimeError("ExecutorManager no inicializado.")
        return self.executor_manager.activate_compute_backend(backend, **kwargs)

    def compute_backend_status(self) -> dict[str, Any]:
        self._require_runtime()
        if self.executor_manager is None:
            raise RuntimeError("ExecutorManager no inicializado.")
        status = dict(self.executor_manager.compute_backend_status())
        # Why the configured backend may not be the live one (unreachable ray, etc).
        status["fallback_reason"] = getattr(self, "compute_backend_fallback", "")
        return status

    def get_project_store(self):
        return self.advanced.project_store


__all__ = ["MolSuiteJobApiMixin", "batch_job", "streaming_job"]
