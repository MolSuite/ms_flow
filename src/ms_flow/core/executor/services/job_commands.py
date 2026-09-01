from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Callable, Iterator, List, Optional, Union
from uuid import UUID

from sqlmodel import select

from ms_flow.core.database.executor_models import ExecutorJob, ExecutorJobChunk
from ms_flow.core.executor.result_handlers import ResultHandler
from ms_flow.core.executor.runner_refs import RunnerRef
from ms_flow.core.executor.utils import TERMINAL_JOB_STATUSES


class JobCommandService:
    """Public job lifecycle for ExecutorManager: submit / resubmit / cancel.

    This service owns the user-facing job mutation commands. It is composed into
    ExecutorManager instead of inherited as a mixin, keeping the manager API stable
    without making job control part of the manager class hierarchy.
    """

    def __init__(self, manager):
        self._manager = manager

    def __getattr__(self, name):
        return getattr(self._manager, name)

    def submit_job(
        self,
        executor_name: str,
        chunks: Union[List[dict], Iterator[dict]],
        run_chunk: Union[Callable, str, RunnerRef],
        project_id: Optional[Union[UUID, str]] = None,
        origin_id: str = "",
        task_type: str = "",
        priority: int = 0,
        queue_policy: str = "fifo",
        default_cpu_required: int = 1,
        default_gpu_required: int = 0,
        max_job_cpu: int | None = None,
        job_payload: Optional[dict] = None,
        job_id: Optional[str] = None,
        batch_size: int | str = 1,
        max_inflight_tasks: int = 16,
        max_inflight_items: int | None = None,
        prefetch_factor: float = 1.0,
        refill_threshold: int = 1,
        result_handler: Optional[ResultHandler] = None,
        store_results: bool = True,
        setup_ref: Optional[Union[Callable, str, RunnerRef]] = None,
        stage_ref: Optional[Union[Callable, str, RunnerRef]] = None,
        finalize_ref: Optional[Union[Callable, str, RunnerRef]] = None,
        stage_fail_policy: str = "fail_fast",
        max_stage_failures: int = 0,
        output_spec: Any = None,
        output_flush_every: int = 500,
        total_chunks: Optional[int] = None,
        depends_on: Optional[List[str]] = None,
        chunk_fail_fast_min_processed: int | None = None,
        chunk_fail_fast_max_failed_ratio: float | None = None,
        chunk_fail_fast_max_consecutive_failures: int | None = None,
        attached_resources: Optional[list[Any]] = None,
    ) -> str:
        """
        Submit a job.

        Parameters
        ----------
        chunks : list or generator of dicts
            Each dict is the payload for one chunk. Generators are consumed
            lazily — only `max_inflight_tasks` payloads are materialized at any time.
            Each dict may contain special keys:
                _cpu_required : int  (default: default_cpu_required)
        run_chunk : callable, "module:function" string, or RunnerRef dict
            The function to execute for each chunk. Stored as a serializable
            reference so jobs can be recovered after a manager restart.
            Must be importable from an installed module (no lambdas/__main__).

        max_inflight_tasks : int
            Maximum number of payloads alive (pending + running) in DB at once.
            Controls memory and executor saturation for large jobs.

        result_handler : ResultHandler or None
            If provided, handle() is called for each completed chunk.
            flush() is called periodically and at job completion.

        store_results : bool
            Whether to persist chunk output_json in executor DB.
            Set False for large jobs where results are handled externally.
        """
        return self._request_engine_command(
            "submit",
            {
                "executor_name": executor_name,
                "chunks": chunks,
                "run_chunk": run_chunk,
                "project_id": project_id,
                "origin_id": origin_id,
                "task_type": task_type,
                "priority": priority,
                "queue_policy": queue_policy,
                "default_cpu_required": default_cpu_required,
                "default_gpu_required": default_gpu_required,
                "max_job_cpu": max_job_cpu,
                "job_payload": job_payload,
                "job_id": job_id,
                "batch_size": batch_size,
                "max_inflight_tasks": max_inflight_tasks,
                "max_inflight_items": max_inflight_items,
                "prefetch_factor": prefetch_factor,
                "refill_threshold": refill_threshold,
                "result_handler": result_handler,
                "store_results": store_results,
                "setup_ref": setup_ref,
                "stage_ref": stage_ref,
                "finalize_ref": finalize_ref,
                "stage_fail_policy": stage_fail_policy,
                "max_stage_failures": max_stage_failures,
                "output_spec": output_spec,
                "output_flush_every": output_flush_every,
                "total_chunks": total_chunks,
                "depends_on": depends_on,
                "chunk_fail_fast_min_processed": chunk_fail_fast_min_processed,
                "chunk_fail_fast_max_failed_ratio": chunk_fail_fast_max_failed_ratio,
                "chunk_fail_fast_max_consecutive_failures": chunk_fail_fast_max_consecutive_failures,
                "attached_resources": attached_resources,
            },
        )

    def resubmit_job(
        self,
        source_job_id: str,
        *,
        executor_name: str | None = None,
        cpu_required: int | None = None,
        queue_policy: str | None = None,
        priority: int | None = None,
        project_id: Optional[Union[UUID, str]] = None,
        store_results: bool | None = None,
        output_spec: Any = None,
        output_flush_every: int | None = None,
    ) -> str:
        return self._request_engine_command(
            "resubmit",
            {
                "source_job_id": source_job_id,
                "executor_name": executor_name,
                "cpu_required": cpu_required,
                "queue_policy": queue_policy,
                "priority": priority,
                "project_id": project_id,
                "store_results": store_results,
                "output_spec": output_spec,
                "output_flush_every": output_flush_every,
            },
        )

    def cancel_job(self, job_id: str):
        return self._request_engine_command("cancel", {"job_id": job_id})

    def _cancel_job_now(self, job_id: str):
        self._assert_engine_thread()
        with self.executor_db.get_session() as session:
            job = session.exec(select(ExecutorJob).where(ExecutorJob.job_id == job_id)).first()
            if job is None:
                raise RuntimeError(f"Job no encontrado: {job_id}")
            if job.status in TERMINAL_JOB_STATUSES:
                return

        feed = None
        staging_futures = self._staging.cancel_job(job_id)
        with self._lock:
            self._cancel_requested_jobs.add(job_id)
            feed = self._job_feeds.get(job_id)
        if feed is not None:
            with feed.lock:
                feed.exhausted = True
        for future in staging_futures:
            if future is not None:
                future.cancel()

        running_for_job = []
        with self._lock:
            for chunk_id, item in self._running_chunks.items():
                if item.job_id == job_id:
                    running_for_job.append((chunk_id, item))

        for chunk_id, item in running_for_job:
            adapter = self._executors.get(item.executor_name)
            cancel_accepted = False
            if adapter:
                cancel_accepted = bool(adapter.cancel(item.handle_id))
            self._log_executor(
                logging.WARNING,
                "Cancellation requested for running chunk (accepted=%s)",
                cancel_accepted,
                extra={"job_id": job_id, "chunk_id": chunk_id},
            )
            self._add_event(
                job_id,
                chunk_id=chunk_id,
                level="WARNING",
                event_type="chunk_cancel_requested",
                message=f"Cancellation requested for running chunk (accepted={cancel_accepted}).",
            )
        
        # Also cancel from dispatch pool if it's currently being submitted
        self._dispatch_pool.cancel_job(job_id)

        now = datetime.now()
        canceled_before_execution: list[str] = []
        payload_refs: dict[str, Optional[str]] = {}
        with self.executor_db.get_session() as session:
            job = session.exec(select(ExecutorJob).where(ExecutorJob.job_id == job_id)).one()
            job.status = "cancel_requested"
            job.finished_at = None
            job.updated_at = now
            session.add(job)

            pre_dispatch = session.exec(
                select(ExecutorJobChunk).where(
                    ExecutorJobChunk.job_id == job_id,
                    ExecutorJobChunk.status.in_(("pending", "staging")),
                )
            ).all()
            for chunk in pre_dispatch:
                payload_refs[chunk.chunk_id] = chunk.checkpoint_ref
                chunk.status = "canceled"
                chunk.progress = 100.0
                chunk.updated_at = now
                chunk.finished_at = now
                session.add(chunk)
                canceled_before_execution.append(chunk.chunk_id)
            session.commit()

        for chunk_id in canceled_before_execution:
            self.remove_chunk_payload_file(payload_refs.get(chunk_id))
            self._add_event(
                job_id,
                chunk_id=chunk_id,
                level="WARNING",
                event_type="chunk_canceled",
                message="Chunk canceled before execution.",
            )
        self._log_executor(
            logging.WARNING,
            "Job cancellation requested by user",
            extra={"job_id": job_id},
        )
        self._add_event(job_id, level="WARNING", event_type="job_cancel_requested",
                        message="Job cancellation requested by user")
        # Ensure the event is visible to tests immediately
        self.event_recorder.flush()
        self.refresh_job_status(job_id)
        self.signal_runtime_work()
