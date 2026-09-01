from __future__ import annotations

import json
import logging
import functools
import threading
import time
from concurrent.futures import Future
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union
from uuid import UUID

from sqlmodel import select

from ms_flow.core.data import (
    DataBridge,
    DataContractError,
    DataContext,
    ExecutorTransportProfile,
)
from ms_flow.core.database.executor_models import (
    ExecutorHeartbeat,
    ExecutorJob,
    ExecutorJobChunk,
    ExecutorJobEvent,
    ExecutorJobFeedState,
)
from ms_flow.core.database.master_models import (
    ProjectJobIndex,
)
from ms_flow.core.executor.dispatch_model import DispatchPolicy
from ms_flow.core.executor.backend_selector import ComputeBackendController
from ms_flow.core.executor.job_monitoring import (
    JobRefreshPlan,
    JobRuntimeMetrics,
    SchedulerNoteSnapshot,
    build_job_runtime_metrics,
    evaluate_job_refresh,
    resolve_scheduler_block_reason,
)
from ms_flow.core.executor.job_snapshot import JobSnapshot
from ms_flow.core.executor.lifecycle_controller import LifecycleController
from ms_flow.core.executor.dispatch_service import DispatchService
from ms_flow.core.executor.local_adapters import (
    ExecutorAdapterBase,
    ExternalExecutorAdapter,
    LokyProcessExecutorAdapter,
    ThreadExecutorAdapter,
)
from ms_flow.core.executor.executor_registry import ExecutorRegistry
from ms_flow.core.executor.local_compute_scheduler import LocalComputeScheduler
from ms_flow.core.executor.command_inbox import CommandInbox, EngineCommand
from ms_flow.core.executor.result_handlers import (
    BufferedResultHandler,
    CallbackResultHandler,
    OutputSpecResultHandler,
    ResultHandler,
    SimpleResultHandler,
)
from ms_flow.core.executor.resource_manager import detect_local_gpus
from ms_flow.core.executor.runtime_state import (
    ExecutorRuntimeState,
    JobFeed,
    JobLifecycle,
    RunningChunk,
)
from ms_flow.core.executor.runner_refs import (
    RunnerRef,
    call_with_optional_context,
    normalize_runner,
    normalize_uuid,
    ref_to_str,
    resolve_runner,
    str_to_ref,
)
from ms_flow.core.executor.staging_manager import StagingManager
from ms_flow.core.executor.services.dispatch_pool import DispatchPool
from ms_flow.core.executor.services.event_recorder import EventRecorder
from ms_flow.core.executor.services.feeding import FeedingService
from ms_flow.core.executor.services.job_store import JobStore, JobStoreDeps
from ms_flow.core.executor.services.job_commands import JobCommandService
from ms_flow.core.executor.services.job_status import JobStatusService
from ms_flow.core.executor.services.payload_store import ChunkPayloadStore
from ms_flow.core.executor.services.persistence_coordinator import (
    INTENT_CANCELED,
    INTENT_COMPLETED,
    INTENT_FAILED,
    TerminalTransition,
)
from ms_flow.core.executor.services.persistence_coordinator import PersistenceCoordinator
from ms_flow.core.executor.services.sink_writer_pool import SinkWriteTask, SinkWriterPool
from ms_flow.core.executor.submission_service import SubmissionService
from ms_flow.core.executor.runtime_status_service import RuntimeStatusService


from ms_flow.core.executor.utils import (
    _safe_json_dumps,
    _safe_json_loads,
    CHUNK_ACTIVE_STATUSES,
    JOB_RECOVERABLE_STATUSES,
    TERMINAL_JOB_STATUSES,
)
# ---------------------------------------------------------------------------
# ExecutorManager
# ---------------------------------------------------------------------------

class ExecutorManager:
    """
    Schedules chunks from ExecutorJob rows onto registered executor adapters.

    Key design principles
    ---------------------
    1. Lazy input   — submit_job() accepts a generator; chunks are inserted
                      into DB in small batches as the window drains (JobFeed).
    2. Lazy output  — results delivered via ResultHandler.handle() as each
                      chunk completes; never accumulated in memory.
    3. Two-cycle dispatch — running jobs fill their window first; only then
                      are pending (not yet started) jobs considered.
    4. CPU accounting — local work is sampled from running chunks and pending
                       dispatches; adapter reservations reduce that budget.
    5. No max_jobs  — concurrency controlled by cpu_required + inflight policy,
                      not by an arbitrary job-count cap.

    CPU accounting
    --------------
    total_cpu      : logical CPUs on this machine
    _reserved_cpu  : sum of adapter.reserved_cpu (e.g. Ray local blocks N)
    _used_cpu      : sampled CPUs of local running chunks and pending dispatches
    _available_cpu : headroom after `_reserved_cpu` and sampled occupancy
    """

    def __init__(
        self,
        executor_db,
        total_cpu: int,
        total_gpu: int | None = None,
        master_db=None,
        poll_interval: float = 0.1,
        active_poll_interval: float = 0.02,
        progress_flush_interval: float = 2.0,
        staging_max_workers: int | None = None,
        max_inline_chunk_payload_bytes: int = 512 * 1024,
        max_spool_payload_bytes: int = 64 * 1024 * 1024,
        logger: Optional[logging.Logger] = None,
    ):
        self.executor_db = executor_db
        self.master_db = master_db
        self.total_cpu = max(1, int(total_cpu))
        self.total_gpu = int(total_gpu) if total_gpu is not None else detect_local_gpus()
        self.poll_interval = float(max(0.02, poll_interval))
        self.active_poll_interval = float(
            max(0.005, min(active_poll_interval, self.poll_interval))
        )
        self.logger = logger or logging.getLogger("molsuite.executor.manager")

        self._executors: Dict[str, ExecutorAdapterBase] = {}
        self.runtime_state = ExecutorRuntimeState()
        self._resolved_callable_cache: Dict[str, Callable[..., Any]] = {}
        self._data_bridge = DataBridge()
        # Local (loky/thread) scheduling — resources + admission + reserve/release —
        # lives in one component; the manager delegates to it. Ray/dask self-schedule.
        self._scheduler = LocalComputeScheduler(
            total_cpu=self.total_cpu,
            total_gpu=self.total_gpu,
            occupancy_provider=self._sampled_occupancy,
        )
        self._lifecycle_controller = LifecycleController(self)
        self._call_with_optional_context = call_with_optional_context

        self._last_progress_flush: float = 0.0
        self._last_handler_flush: float = 0.0

        self._stop_event = threading.Event()
        self._loop_wake_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._engine_thread_id: int | None = None
        self._command_inbox = CommandInbox(maxsize=256)
        self._lock = threading.RLock()
        self.registry = ExecutorRegistry(
            executors=self._executors,
            available_cpu=lambda: self._scheduler.available_cpu(self._executors),
            total_cpu=self.total_cpu,
            lock=self._lock,
            on_heartbeat=self._upsert_heartbeat,
            running_executor_names=lambda: [c.executor_name for c in self._running_chunks.values()],
            logger=self.logger,
        )
        self._staging_max_workers = max(1, int(staging_max_workers)) if staging_max_workers is not None else None
        self._staging = StagingManager(total_cpu=self.total_cpu, max_workers=self._staging_max_workers)
        self._output_sink_flush_retries: int = 3
        self._output_sink_retry_backoff_s: float = 0.05
        self._output_sink_max_buffer_factor: int = 10
        self._output_sink_max_buffer_bytes: int = 16 * 1024 * 1024
        self._output_sink_max_payload_bytes: int = 4 * 1024 * 1024
        self._output_sink_max_pending_chunks: int = 1024
        self._output_sink_max_pending_bytes: int = 256 * 1024 * 1024
        self._max_inline_chunk_payload_bytes: int = max(1024, int(max_inline_chunk_payload_bytes))
        self._max_spool_payload_bytes: int = max(self._max_inline_chunk_payload_bytes, int(max_spool_payload_bytes))
        
        self._last_loop_latency_ms: float = 0.0
        self._loop_iteration_count: int = 0
        self._consecutive_loop_errors: int = 0
        self._last_loop_error: str = ""
        self._last_loop_error_at: str | None = None
        self._MAX_LOOP_BACKOFF_S: float = 30.0
        self._heartbeat_sync_interval_s: float = 0.5
        self._last_heartbeat_sync_at: float = 0.0
        self._dispatch_pool = DispatchPool(
            max_workers=max(2, min(8, self.total_cpu)),
            timeout_s=30.0,
        )

        self.payload_store = ChunkPayloadStore(
            self,
            max_inline_bytes=self._max_inline_chunk_payload_bytes,
            max_spool_bytes=self._max_spool_payload_bytes,
        )
        self.event_recorder = EventRecorder(
            self,
            progress_flush_interval=progress_flush_interval,
        )
        self.job_store = JobStore(
            JobStoreDeps(
                executor_db_provider=lambda: self.executor_db,
                master_db_provider=lambda: self.master_db,
                runtime_state=self.runtime_state,
                record_scheduler_reason=self.record_scheduler_reason,
                get_feed_state_row=self.get_feed_state_row,
                job_max_cpu_limit=self.job_max_cpu_limit,
                resolve_scheduler_block_reason=self.resolve_scheduler_block_reason,
                scheduler_note_snapshot=self.scheduler_note_snapshot,
            )
        )
        self.submission_service = SubmissionService(self)
        self.runtime_status_service = RuntimeStatusService(self)
        self.feeding_service = FeedingService(self)
        self.job_commands = JobCommandService(self)
        self.job_status = JobStatusService(self)
        self.compute_backend = ComputeBackendController(self)
        self.persistence_coordinator = PersistenceCoordinator(self)
        self._sink_writer_pool = SinkWriterPool(
            max_pending=self._output_sink_max_pending_chunks,
            on_completion=self.signal_runtime_work,
        )
        self.dispatch_service = DispatchService(self)

    def configure_output_sink_limits(
        self,
        *,
        flush_retries: int | None = None,
        retry_backoff_s: float | None = None,
        max_buffer_factor: int | None = None,
        max_buffer_bytes: int | None = None,
        max_payload_bytes: int | None = None,
        max_pending_chunks: int | None = None,
        max_pending_bytes: int | None = None,
    ):
        if flush_retries is not None:
            self._output_sink_flush_retries = max(0, int(flush_retries))
        if retry_backoff_s is not None:
            self._output_sink_retry_backoff_s = max(0.0, float(retry_backoff_s))
        if max_buffer_factor is not None:
            self._output_sink_max_buffer_factor = max(1, int(max_buffer_factor))
        if max_buffer_bytes is not None:
            self._output_sink_max_buffer_bytes = max(1024, int(max_buffer_bytes))
        if max_payload_bytes is not None:
            self._output_sink_max_payload_bytes = max(1024, int(max_payload_bytes))
        if max_pending_chunks is not None:
            new_capacity = max(1, int(max_pending_chunks))
            if new_capacity != self._output_sink_max_pending_chunks:
                if self.manager_thread_alive():
                    raise RuntimeError(
                        "Sink queue capacity cannot change while the executor manager is running."
                    )
                self._sink_writer_pool.shutdown()
                self._output_sink_max_pending_chunks = new_capacity
                self._sink_writer_pool = SinkWriterPool(
                    max_pending=new_capacity,
                    on_completion=self.signal_runtime_work,
                )
        if max_pending_bytes is not None:
            self._output_sink_max_pending_bytes = max(1024, int(max_pending_bytes))

    def has_active_jobs(self) -> bool:
        return self.job_store.has_active_jobs()

    def _assert_no_active_work_for_db_transition(self, action: str) -> None:
        with self._lock:
            if self._running_chunks:
                raise RuntimeError(
                    f"Cannot {action} executor_db while chunks are running."
                )
        if self.has_active_jobs():
            raise RuntimeError(
                f"Cannot {action} executor_db while jobs are active or cancelling."
            )

    def rebind_executor_db(self, executor_db) -> None:
        if executor_db is None:
            raise ValueError("executor_db cannot be None.")
        self._assert_no_active_work_for_db_transition("swap")
        self.persistence_coordinator.flush()
        self.event_recorder.flush_progress(force=True)
        self.event_recorder.flush()
        self.executor_db = executor_db
        for name in self._executors:
            self._upsert_heartbeat(name)
        self.logger.info("Executor manager rebound to db=%s", getattr(executor_db, "db_path", None))

    def unbind_executor_db(self):
        self._assert_no_active_work_for_db_transition("desasociar")
        self.persistence_coordinator.flush()
        self.event_recorder.flush_progress(force=True)
        self.event_recorder.flush()
        executor_db = self.executor_db
        self.executor_db = None
        self.logger.info("Executor manager unbound from executor db")
        return executor_db

    def configure_runtime_limits(
        self,
        *,
        max_inline_chunk_payload_bytes: int | None = None,
        max_spool_payload_bytes: int | None = None,
        staging_max_workers: int | None = None,
    ):
        self.payload_store.configure(
            max_inline=max_inline_chunk_payload_bytes,
            max_spool=max_spool_payload_bytes,
        )
        if staging_max_workers is not None:
            desired_workers = max(1, int(staging_max_workers))
            if desired_workers != self._staging_max_workers:
                self._staging_max_workers = desired_workers
                self._staging.configure(max_workers=desired_workers)

    def _ensure_staging_pool(self):
        self._staging.ensure_pool()

    def _log_executor(self, level: int, message: str, *args, **kwargs):
        extra = kwargs.pop("extra", None) or {}
        self.logger.log(level, message, *args, extra=extra, **kwargs)

    # ------------------------------------------------------------------
    # CPU accounting
    # ------------------------------------------------------------------

    @property
    def _reserved_cpu(self) -> int:
        return self._scheduler.reserved_cpu(self._executors)

    @property
    def _available_cpu(self) -> int:
        return self._scheduler.available_cpu(self._executors)

    @property
    def _available_gpu(self) -> int:
        return self._scheduler.available_gpu(self._executors)

    # ------------------------------------------------------------------
    # Executor registration (delegated to ExecutorRegistry)
    # ------------------------------------------------------------------

    def register_thread_executor(self, name: str = "thread", max_workers: int = 8):
        self.registry.register_thread(name=name, max_workers=max_workers)

    def register_process_pool_executor(
        self,
        name: str = "process_pool",
        *,
        max_workers: int | None = None,
        timeout_s: float = 10.0,
        kill_workers_on_shutdown: bool = True,
    ):
        self.registry.register_process_pool(
            name=name,
            max_workers=max_workers,
            timeout_s=timeout_s,
            kill_workers_on_shutdown=kill_workers_on_shutdown,
        )

    def register_ray_executor(
        self,
        name: str = "ray",
        mode: str = "external",
        cpus: int = 0,
        shared_fs: Optional[bool] = None,
        native: bool = False,
        address: Optional[str] = None,
        namespace: Optional[str] = None,
        runtime_env: Optional[dict[str, Any]] = None,
        gpu_slots_per_device: int = 1,
    ):
        self.registry.register_ray(
            name=name,
            mode=mode,
            cpus=cpus,
            shared_fs=shared_fs,
            native=native,
            address=address,
            namespace=namespace,
            runtime_env=runtime_env,
            gpu_slots_per_device=gpu_slots_per_device,
        )


    def register_hpc_executor(
        self,
        name: str = "hpc",
        shared_fs: bool = False,
        submit_command: str | list[str] | tuple[str, ...] | None = None,
        poll_command: str | list[str] | tuple[str, ...] | None = None,
        cancel_command: str | list[str] | tuple[str, ...] | None = None,
        poll_interval_s: float = 2.0,
        command_context: Optional[dict[str, Any]] = None,
        command_env: Optional[dict[str, str]] = None,
        python_executable: Optional[str] = None,
    ):
        self.registry.register_hpc(
            name=name,
            shared_fs=shared_fs,
            submit_command=submit_command,
            poll_command=poll_command,
            cancel_command=cancel_command,
            poll_interval_s=poll_interval_s,
            command_context=command_context,
            command_env=command_env,
            python_executable=python_executable,
        )

    def unregister_executor(self, name: str):
        self.registry.unregister(name)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._ensure_staging_pool()
            self._stop_event.clear()
        with self._lock:
            self._thread = threading.Thread(
                target=self._run_loop,
                name="molsuite-executor-manager",
                daemon=True,
            )
            self._thread.start()
        self.logger.info("Executor manager started (total_cpu=%s)", self.total_cpu)

    def _mark_job_failed(self, job_id: str, reason: str):
        self._fail_job(job_id, reason, flush_events=True)

    def _mark_job_canceled(self, job_id: str, reason: str):
        self._cancel_job(job_id, reason, flush_events=True)

    @staticmethod
    def _normalize_chunk_fail_fast_config(payload: dict[str, Any] | None) -> dict[str, Any]:
        raw = dict((payload or {}).get("_chunk_fail_fast") or {})
        config: dict[str, Any] = {}
        if raw.get("min_processed") is not None:
            config["min_processed"] = max(1, int(raw["min_processed"]))
        if raw.get("max_failed_ratio") is not None:
            ratio = float(raw["max_failed_ratio"])
            if 0.0 <= ratio <= 1.0:
                config["max_failed_ratio"] = ratio
        if raw.get("max_consecutive_failures") is not None:
            config["max_consecutive_failures"] = max(1, int(raw["max_consecutive_failures"]))
        return config

    def register_chunk_success_for_fail_fast(self, job_id: str) -> None:
        lifecycle = self.get_job_lifecycle(job_id)
        if lifecycle is not None:
            lifecycle.consecutive_chunk_failures = 0

    def register_chunk_failure_for_fail_fast(self, job_id: str) -> bool:
        lifecycle = self.get_job_lifecycle(job_id)
        if lifecycle is not None:
            lifecycle.consecutive_chunk_failures += 1
            consecutive_failures = int(lifecycle.consecutive_chunk_failures)
        else:
            consecutive_failures = 0

        with self.executor_db.get_session() as session:
            job = session.exec(select(ExecutorJob).where(ExecutorJob.job_id == job_id)).first()
            if job is None or job.status in {"failed", "canceled", "cancel_requested"}:
                return False
            config = self._normalize_chunk_fail_fast_config(_safe_json_loads(job.payload_json))
            if not config:
                return False
            feed_state = self.get_feed_state_row(session, job_id)
            metrics = self._build_job_runtime_metrics(session, job, feed_state)

        reasons: list[str] = []
        max_consecutive_failures = config.get("max_consecutive_failures")
        if max_consecutive_failures is not None and consecutive_failures >= int(max_consecutive_failures):
            reasons.append(
                f"consecutive chunk failures reached {consecutive_failures}/{int(max_consecutive_failures)}"
            )

        max_failed_ratio = config.get("max_failed_ratio")
        min_processed = int(config.get("min_processed") or 1)
        processed_for_ratio = int(metrics.done) + int(metrics.failed)
        if (
            max_failed_ratio is not None
            and processed_for_ratio >= min_processed
            and processed_for_ratio > 0
        ):
            failed_ratio = float(metrics.failed) / float(processed_for_ratio)
            if failed_ratio >= float(max_failed_ratio):
                reasons.append(
                    f"failed ratio reached {failed_ratio:.3f} ({int(metrics.failed)}/{processed_for_ratio})"
                )

        if not reasons:
            return False

        reason = (
            "Job aborted by chunk fail-fast policy: "
            + "; ".join(reasons)
            + "."
        )
        self._add_event(
            job_id,
            level="ERROR",
            event_type="job_chunk_fail_fast_triggered",
            message=reason,
            payload={
                "consecutive_chunk_failures": consecutive_failures,
                "chunks_done": int(metrics.done),
                "chunks_failed": int(metrics.failed),
                "processed_for_ratio": processed_for_ratio,
                "config": config,
            },
        )
        self.event_recorder.flush()
        self._mark_job_failed(job_id, reason)
        return True

    def _update_project_job_index(
        self,
        *,
        job_id: str,
        project_id: Optional[UUID],
        status: str,
        progress: float,
        updated_at: datetime,
    ):
        if project_id is None or self.master_db is None:
            return
        with self.master_db.get_session() as session:
            row = session.exec(
                select(ProjectJobIndex).where(ProjectJobIndex.job_id == job_id)
            ).first()
            if row is None:
                return
            row.status = status
            row.progress = progress
            row.updated_at = updated_at
            session.add(row)
            session.commit()

    def _cleanup_job_runtime(self, job_id: str, *, flush_handler: bool):
        cleanup_state = self.runtime_state.pop_job_runtime(job_id)
        for future in self._staging.cancel_job(job_id):
            if future is not None:
                future.cancel()

        if flush_handler and cleanup_state.handler is not None:
            try:
                cleanup_state.handler.flush()
            except Exception as exc:
                self.logger.exception("Final ResultHandler flush error: %s", exc)
        if cleanup_state.handler is not None and hasattr(cleanup_state.handler, "close"):
            cleanup_state.handler.close()

        if cleanup_state.feed is not None:
            self._lifecycle_controller.close_job_resources(cleanup_state.feed.attached_resources)

        self._cleanup_job_payload_spool(job_id)

    def _cancel_active_job_chunks(self, job_id: str, reason: str) -> None:
        for item in self._running_chunks_for_job(job_id):
            adapter = self.registered_executors().get(item.executor_name)
            if adapter is not None:
                adapter.cancel(item.handle_id)
            claimed, _ = self._claim_running_chunk(item)
            if claimed is not None:
                self._mark_chunk_canceled(claimed.chunk_id, job_id=claimed.job_id)

        now = datetime.now()
        with self.executor_db.get_session() as session:
            chunks = session.exec(
                select(ExecutorJobChunk).where(
                    ExecutorJobChunk.job_id == job_id,
                    ExecutorJobChunk.status.in_(CHUNK_ACTIVE_STATUSES),
                )
            ).all()
            for chunk in chunks:
                chunk.status = "canceled"
                chunk.updated_at = now
                chunk.finished_at = now
                if reason and not chunk.error:
                    chunk.error = reason
                session.add(chunk)
            session.commit()

    def _cancel_job(
        self,
        job_id: str,
        reason: str,
        *,
        cancel_active_chunks: bool = False,
        flush_events: bool = False,
    ) -> None:
        if cancel_active_chunks:
            self._cancel_active_job_chunks(job_id, reason)

        now = datetime.now()
        project_id = None
        with self.executor_db.get_session() as session:
            job = session.exec(select(ExecutorJob).where(ExecutorJob.job_id == job_id)).first()
            if job is not None:
                job.status = "canceled"
                job.error = reason
                job.updated_at = now
                job.finished_at = now
                project_id = job.project_id
                session.add(job)
                session.add(
                    ExecutorJobEvent(
                        job_id=job_id,
                        level="WARNING",
                        event_type="job_canceled",
                        message=reason or "Job canceled.",
                        created_at=now,
                    )
                )
            session.commit()

        self._update_project_job_index(
            job_id=job_id,
            project_id=project_id,
            status="canceled",
            progress=100.0,
            updated_at=now,
        )
        self._cleanup_job_runtime(job_id, flush_handler=True)
        if flush_events:
            self.event_recorder.flush()

    def _fail_job(
        self,
        job_id: str,
        reason: str,
        *,
        cancel_active_chunks: bool = False,
        flush_events: bool = False,
    ) -> None:
        if cancel_active_chunks:
            self._cancel_active_job_chunks(job_id, reason)

        now = datetime.now()
        project_id = None
        failed_written = False
        with self.executor_db.get_session() as session:
            job = session.exec(select(ExecutorJob).where(ExecutorJob.job_id == job_id)).first()
            if job is not None and job.status == "failed":
                failed_written = True
            elif job is not None and job.status not in {"cancel_requested", "canceled", "completed"}:
                job.status = "failed"
                job.error = reason
                job.updated_at = now
                job.finished_at = now
                session.add(job)
                failed_written = True
            project_id = job.project_id if job is not None else None
            session.commit()

        if failed_written:
            self._update_project_job_index(
                job_id=job_id,
                project_id=project_id,
                status="failed",
                progress=100.0,
                updated_at=now,
            )
        self._cleanup_job_runtime(job_id, flush_handler=True)
        if failed_written:
            self._add_event(job_id, level="ERROR", event_type="job_failed", message=reason)
        if flush_events:
            self.event_recorder.flush()

    def get_feed_state_row(
        self,
        session,
        job_id: str,
    ) -> Optional[ExecutorJobFeedState]:
        return session.exec(
            select(ExecutorJobFeedState).where(ExecutorJobFeedState.job_id == job_id)
        ).first()

    def ensure_feed_state_row(
        self,
        session,
        *,
        job_id: str,
        now: Optional[datetime] = None,
    ) -> ExecutorJobFeedState:
        row = self.get_feed_state_row(session, job_id)
        if row is None:
            row = ExecutorJobFeedState(
                job_id=job_id,
                created_at=now or datetime.now(),
                updated_at=now or datetime.now(),
            )
        return row

    def mark_job_failed_from_stage(self, job_id: str, reason: str):
        self._fail_job(
            job_id,
            reason,
            cancel_active_chunks=True,
            flush_events=True,
        )

    def stop(self):
        self._stop_event.set()
        self._loop_wake_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        self._thread = None

        self._sink_writer_pool.shutdown()
        self._flush_all_handlers()
        self.event_recorder.flush()
        self._staging.shutdown()
        self._dispatch_pool.shutdown()

        self.runtime_state.clear_running_chunks()
        with self._lock:
            self.runtime_state.clear()
            self.event_recorder.clear()
            self._resolved_callable_cache.clear()
            self.payload_store.cleanup_all()
        for adapter in self._executors.values():
            try:
                adapter.shutdown()
            except Exception:
                pass
        for name in list(self._executors.keys()):
            self._upsert_heartbeat(name, status="offline")
        self.logger.info("Executor manager stopped")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @functools.wraps(JobCommandService.submit_job)
    def submit_job(self, *args, **kwargs) -> str:
        return self.job_commands.submit_job(*args, **kwargs)

    @functools.wraps(JobCommandService.resubmit_job)
    def resubmit_job(self, *args, **kwargs) -> str:
        return self.job_commands.resubmit_job(*args, **kwargs)

    @functools.wraps(JobCommandService.cancel_job)
    def cancel_job(self, job_id: str):
        return self.job_commands.cancel_job(job_id)

    def _cancel_job_now(self, job_id: str):
        return self.job_commands._cancel_job_now(job_id)

    def activate_compute_backend(self, backend: str, **kwargs) -> dict[str, Any]:
        return self.compute_backend.activate(backend, **kwargs)

    def compute_backend_status(self) -> dict[str, Any]:
        return self.compute_backend.status()

    def _on_chunk_done(self, item: RunningChunk, payload: dict):
        return self.job_status._on_chunk_done(item, payload)

    def _poll_sink_completions(self) -> None:
        return self.job_status._poll_sink_completions()

    def _on_chunk_failed(self, item: RunningChunk, error_msg: str):
        return self.job_status._on_chunk_failed(item, error_msg)

    def _mark_chunk_failed(self, chunk_id: str, job_id: str, error_msg: str):
        return self.job_status._mark_chunk_failed(chunk_id, job_id, error_msg)

    def _mark_chunk_canceled(self, chunk_id: str, job_id: str = ""):
        return self.job_status._mark_chunk_canceled(chunk_id, job_id=job_id)

    def refresh_job_status(self, job_id: str):
        return self.job_status.refresh_job_status(job_id)

    def _add_event(
        self,
        job_id: str,
        chunk_id: str = "",
        level: str = "INFO",
        event_type: str = "log",
        message: str = "",
        payload: Optional[dict] = None,
    ):
        return self.job_status._add_event(
            job_id=job_id,
            chunk_id=chunk_id,
            level=level,
            event_type=event_type,
            message=message,
            payload=payload,
        )

    def _flush_events(self):
        return self.job_status._flush_events()

    def _upsert_heartbeat(self, executor_name: str, status: str = "online"):
        return self.job_status._upsert_heartbeat(executor_name, status=status)

    def _sync_heartbeats(self):
        return self.job_status._sync_heartbeats()

    def _assert_engine_thread(self) -> None:
        if threading.get_ident() != self._engine_thread_id:
            raise RuntimeError("Control-plane mutation must run on the engine thread.")

    def _is_engine_thread(self) -> bool:
        return threading.get_ident() == self._engine_thread_id

    def _request_engine_command(self, kind: str, payload: dict[str, Any]):
        if self._is_engine_thread():
            return self._execute_engine_command(EngineCommand(kind=kind, payload=payload, future=Future()))
        if not self.manager_thread_alive():
            raise RuntimeError("ExecutorManager must be started before mutating the control plane.")
        future = self._command_inbox.publish(kind, payload)
        self.signal_runtime_work()
        return future.result()

    def _execute_engine_command(self, command: EngineCommand):
        self._assert_engine_thread()
        if command.kind == "submit":
            return self.submission_service.submit_job(**command.payload)
        if command.kind == "resubmit":
            return self.submission_service.resubmit_job(**command.payload)
        if command.kind == "cancel":
            return self._cancel_job_now(**command.payload)
        raise RuntimeError(f"Unknown engine command: {command.kind}")

    def _drain_engine_commands(self) -> bool:
        self._assert_engine_thread()
        submitted = False
        for command in self._command_inbox.drain():
            if command.future.cancelled():
                continue
            submitted = submitted or command.kind in {"submit", "resubmit"}
            try:
                result = self._execute_engine_command(command)
            except BaseException as exc:
                command.future.set_exception(exc)
            else:
                command.future.set_result(result)
        return submitted

    def _sampled_occupancy(self) -> tuple[int, int]:
        """(cpu, gpu) currently occupied = running chunks (registry) + in-flight
        submits (dispatch pool). This is what admission subtracts from total —
        sampled from real state instead of a manual reserve/release counter."""
        pool = getattr(self, "_dispatch_pool", None)
        pending_cpu = pool.get_total_cpu_required() if pool is not None else 0
        pending_gpu = pool.get_total_gpu_required() if pool is not None else 0
        return (
            self.runtime_state.running_cpu() + pending_cpu,
            self.runtime_state.running_gpu() + pending_gpu,
        )

    def _register_running_chunk(
        self,
        *,
        job_id: str,
        chunk_id: str,
        executor_name: str,
        handle_id: str,
        cpu_required: int,
        gpu_required: int = 0,
    ) -> RunningChunk:
        self._assert_engine_thread()
        item = RunningChunk(
            job_id=job_id,
            chunk_id=chunk_id,
            executor_name=executor_name,
            handle_id=handle_id or "",
            cpu_required=int(cpu_required or 0),
            gpu_required=int(gpu_required or 0),
        )
        with self._lock:
            self.runtime_state.register_running_chunk(item)
        return item

    def _claim_running_chunk(
        self,
        item: RunningChunk,
        *,
        check_cancel: bool = False,
    ) -> tuple[RunningChunk | None, bool]:
        self._assert_engine_thread()
        with self._lock:
            cancel_requested = check_cancel and self.runtime_state.has_cancel_request(item.job_id)
            owned = self.runtime_state.pop_running_chunk(item.chunk_id)
        return owned, bool(cancel_requested)

    def _running_chunks_for_job(self, job_id: str) -> list[RunningChunk]:
        with self._lock:
            return self.runtime_state.snapshot_running_chunks_for_job(job_id)

    def _build_job_runtime_metrics(
        self,
        session,
        job: ExecutorJob,
        feed_state: Optional[ExecutorJobFeedState],
    ) -> JobRuntimeMetrics:
        return build_job_runtime_metrics(session, job, feed_state)

    def _evaluate_job_refresh(
        self,
        *,
        job: ExecutorJob,
        lifecycle: Optional[JobLifecycle],
        metrics: JobRuntimeMetrics,
        feed_exhausted: bool,
    ) -> JobRefreshPlan:
        return evaluate_job_refresh(
            job=job,
            lifecycle=lifecycle,
            metrics=metrics,
            feed_exhausted=feed_exhausted,
        )

    def resolve_scheduler_block_reason(
        self,
        *,
        job: ExecutorJob,
        status: str,
        metrics: JobRuntimeMetrics,
    ) -> str:
        executor = self._executors.get(job.executor_name)
        return resolve_scheduler_block_reason(
            job=job,
            status=status,
            metrics=metrics,
            max_job_cpu=self.job_max_cpu_limit(job),
            available_cpu=self._available_cpu,
            executor_consumes_local_cpu_tokens=bool(
                executor is not None and executor.metadata.consumes_local_cpu_tokens
            ),
        )

    @staticmethod
    def _scheduler_reason_event_type(reason: str) -> str:
        mapping = {
            "waiting_for_feed": "job_waiting_for_feed",
            "waiting_for_dispatch": "job_waiting_for_dispatch",
            "waiting_for_global_cpu": "job_waiting_for_global_cpu",
            "waiting_for_job_cpu_cap": "job_waiting_for_job_cpu_cap",
            "waiting_for_stage": "job_waiting_for_stage",
            "waiting_for_dependencies": "job_waiting_for_dependencies",
            "waiting_for_sink": "job_waiting_for_sink",
            "waiting_for_output_sink_quota": "job_waiting_for_output_sink_quota",
        }
        return mapping.get(reason, "")

    @staticmethod
    def _scheduler_reason_message(reason: str) -> str:
        mapping = {
            "waiting_for_feed": "Job waiting for feed materialization.",
            "waiting_for_dispatch": "Job waiting for chunk dispatch.",
            "waiting_for_global_cpu": "Job waiting for global CPU availability.",
            "waiting_for_job_cpu_cap": "Job waiting for available CPU within the job CPU cap.",
            "waiting_for_stage": "Job waiting for staging completion.",
            "waiting_for_dependencies": "Job waiting for dependency completion.",
            "waiting_for_sink": "Job waiting for output sink confirmation.",
            "waiting_for_output_sink_quota": "Job waiting for output sink quota to drain.",
        }
        return mapping.get(reason, "")

    def _persist_scheduler_reason(self, job_id: str, reason: str) -> None:
        if self.executor_db is None:
            return
        with self.executor_db.get_session() as session:
            job = session.exec(select(ExecutorJob).where(ExecutorJob.job_id == job_id)).first()
            if job is None:
                return
            job.scheduler_reason = reason
            session.add(job)
            session.commit()
            project_id = job.project_id
        if project_id is None or self.master_db is None:
            return
        with self.master_db.get_session() as session:
            row = session.exec(
                select(ProjectJobIndex).where(ProjectJobIndex.job_id == job_id)
            ).first()
            if row is None:
                return
            row.scheduler_reason = reason
            session.add(row)
            session.commit()

    def record_scheduler_reason(self, job_id: str, reason: str, *, payload: Optional[dict[str, Any]] = None):
        now = datetime.now()
        event_type = self._scheduler_reason_event_type(reason)
        changed = False
        with self._lock:
            state = self.runtime_state.get_scheduler_note(job_id)
            previous_reason = str(state.current_reason or "")
            changed = previous_reason != reason
            if reason:
                state.current_reason = reason
                state.last_scheduler_reason = reason
                state.last_scheduler_reason_at = now
                state.last_scheduler_payload = dict(payload or {})
            else:
                state.current_reason = ""
        if changed:
            self._persist_scheduler_reason(job_id, reason)
        if reason and reason != previous_reason and event_type:
            self._add_event(
                job_id,
                level="INFO",
                event_type=event_type,
                message=self._scheduler_reason_message(reason),
                payload={"reason": reason, **dict(payload or {})},
            )

    def _record_dispatch_attempt(self, job_id: str, *, chunk_id: str):
        now = datetime.now()
        with self._lock:
            state = self.runtime_state.get_scheduler_note(job_id)
            state.last_dispatch_attempt_at = now

    def scheduler_note_snapshot(self, job_id: str) -> SchedulerNoteSnapshot:
        with self._lock:
            state = self.runtime_state.snapshot_scheduler_note(job_id)
        return SchedulerNoteSnapshot(
            current_scheduler_reason=str(state.current_reason or ""),
            last_dispatch_attempt_at=state.last_dispatch_attempt_at,
            last_scheduler_reason_at=state.last_scheduler_reason_at,
            last_scheduler_reason=str(state.last_scheduler_reason or ""),
            last_scheduler_payload=dict(state.last_scheduler_payload or {}),
        )

    def get_job(self, job_id: str) -> Optional[JobSnapshot]:
        """Fetch job snapshot."""
        return self.job_store.get_job(job_id)

    def list_jobs(self, status: Optional[str] = None) -> list[JobSnapshot]:
        """List job snapshots."""
        return self.job_store.list_jobs(status=status)

    @property
    def _job_runners(self):
        return self.runtime_state.job_runners

    @property
    def _job_feeds(self):
        return self.runtime_state.job_feeds

    @property
    def _job_lifecycles(self):
        return self.runtime_state.job_lifecycles

    @property
    def _job_result_handlers(self):
        return self.runtime_state.job_result_handlers

    @property
    def _job_store_results(self):
        return self.runtime_state.job_store_results

    @property
    def _cancel_requested_jobs(self):
        return self.runtime_state.cancel_requested_jobs

    @property
    def _job_scheduler_notes(self):
        return self.runtime_state.job_scheduler_notes

    @property
    def _running_chunks(self):
        return self.runtime_state.running_chunks

    # ------------------------------------------------------------------
    # Runtime facade for collaborating services
    # ------------------------------------------------------------------

    def registered_executors(self) -> dict[str, ExecutorAdapterBase]:
        return dict(self._executors)

    def running_chunks_snapshot(self) -> list[RunningChunk]:
        with self._lock:
            return self.runtime_state.snapshot_running_chunks()

    def job_feeds_snapshot(self) -> list[JobFeed]:
        with self._lock:
            return self.runtime_state.snapshot_job_feeds()

    def job_result_handlers_snapshot(self) -> dict[str, ResultHandler]:
        with self._lock:
            return self.runtime_state.snapshot_result_handlers()

    def get_job_feed(self, job_id: str) -> Optional[JobFeed]:
        with self._lock:
            return self.runtime_state.get_job_feed(job_id)

    def get_runner_ref(self, job_id: str) -> RunnerRef | str | None:
        with self._lock:
            return self.runtime_state.get_runner_ref(job_id)

    def get_job_lifecycle(self, job_id: str) -> Optional[JobLifecycle]:
        with self._lock:
            return self.runtime_state.get_job_lifecycle(job_id)

    def get_job_result_handler(self, job_id: str) -> Optional[ResultHandler]:
        with self._lock:
            return self.runtime_state.get_result_handler(job_id)

    def stores_job_results(self, job_id: str, *, default: bool = True) -> bool:
        with self._lock:
            return self.runtime_state.stores_job_results(job_id, default=default)

    def register_job_runtime(
        self,
        *,
        job_id: str,
        runner_ref: RunnerRef | str,
        feed: JobFeed,
        lifecycle: JobLifecycle,
        store_results: bool,
        handler: ResultHandler | None = None,
        cancel_requested: bool = False,
    ) -> None:
        with self._lock:
            # RLA-300 submit guard: a cancellation may have landed durably while
            # this submission was still mid-flight (before the feed existed). The
            # cancel sweep could not exhaust a feed that had not been registered
            # yet, so close that window here under the same lock that serializes
            # cancel_job — exhaust the freshly registered feed so the loop
            # converges the job to canceled instead of feeding a canceled job.
            already_canceled = (
                cancel_requested
                or job_id in self.runtime_state.cancel_requested_jobs
                or self._job_cancellation_landed(job_id)
            )
            self.runtime_state.register_job_runtime(
                job_id=job_id,
                runner_ref=runner_ref,
                feed=feed,
                lifecycle=lifecycle,
                store_results=store_results,
                handler=handler,
                cancel_requested=already_canceled,
            )
            if already_canceled:
                # Exhaust under the same lock so the feeding cycle never observes
                # this feed in a live state — otherwise it could emit a chunk for
                # an already-canceled job in the gap before we close the feed.
                with feed.lock:
                    feed.exhausted = True
        if already_canceled:
            self.refresh_job_status(job_id)
            self.signal_runtime_work()

    def _job_cancellation_landed(self, job_id: str) -> bool:
        """Whether a cancellation already reached the durable job row.

        Closes the submit/cancel race where cancel_job ran (and the loop already
        finalized + cleaned the in-memory cancel set) before this submission
        registered its feed: the in-memory signal is gone, but the durable row is
        cancel_requested/canceled, so a late feed must not be fed.
        """
        with self.executor_db.get_session() as session:
            status = session.exec(
                select(ExecutorJob.status).where(ExecutorJob.job_id == job_id)
            ).first()
        return str(status or "") in ("cancel_requested", "canceled", "failed", "completed")

    def signal_runtime_work(self) -> None:
        self._loop_wake_event.set()

    def available_cpu(self) -> int:
        return self._available_cpu

    def reserved_cpu(self) -> int:
        return self._reserved_cpu

    def used_local_cpu(self) -> int:
        return self._scheduler.used_cpu

    def available_gpu(self) -> int:
        return self._available_gpu

    def used_gpu(self) -> int:
        return self._scheduler.used_gpu

    def local_budget_policy_snapshot(self) -> dict[str, Any]:
        executors: dict[str, dict[str, Any]] = {}
        for name, adapter in self.registered_executors().items():
            accounting_mode = self.executor_local_accounting_mode(adapter)
            executors[name] = {
                "backend": adapter.metadata.backend,
                "mode": adapter.metadata.mode,
                "budgeting": accounting_mode,
                "reserved_cpu": int(adapter.reserved_cpu or 0),
                "participates_in_local_budget": self.executor_participates_in_local_accounting(adapter),
            }
        return {
            "total_cpu": self.total_cpu,
            "fixed_reserved_cpu": self.reserved_cpu(),
            "dynamic_used_cpu": self.used_local_cpu(),
            "dynamic_available_cpu": self.available_cpu(),
            "distributed_runtime_policy": {
                "applies_to_backends": ["ray"],
                "local_mode_budgeting": "reserved",
                "managed_mode_budgeting": "none",
                "external_mode_budgeting": "none",
                "reusable_local_process_budgeting": "dynamic",
            },
            "executors": executors,
        }

    def manager_thread_alive(self) -> bool:
        return bool(self._thread is not None and self._thread.is_alive())

    def dispatch_pool_snapshot(self) -> dict[str, Any]:
        return self._dispatch_pool.snapshot()

    def loop_runtime_snapshot(self) -> dict[str, Any]:
        return {
            "last_latency_ms": self._last_loop_latency_ms,
            "iterations": self._loop_iteration_count,
            "poll_interval_s": self.poll_interval,
            "active_poll_interval_s": self.active_poll_interval,
            "adaptive_polling": True,
            "latency_sensitive_work": self._has_latency_sensitive_work(),
            "consecutive_errors": self._consecutive_loop_errors,
            "backoff_s": self._current_loop_backoff_s(),
            "last_error": self._last_loop_error,
            "last_error_at": self._last_loop_error_at,
        }

    def staging_runtime_snapshot(self) -> dict[str, Any]:
        return {
            "ready": self._staging.ready(),
            "active_tasks": self._staging.active_count(),
            "capacity": self._staging.capacity(),
            "available_slots": self._current_staging_slots(),
        }

    def runtime_limits_snapshot(self) -> dict[str, Any]:
        return {
            "max_inline_chunk_payload_bytes": self._max_inline_chunk_payload_bytes,
            "max_spool_payload_bytes": self._max_spool_payload_bytes,
            "staging_max_workers": self._staging.configured_max_workers or max(2, min(8, self.total_cpu)),
            "output_sink_flush_retries": self._output_sink_flush_retries,
            "output_sink_retry_backoff_s": self._output_sink_retry_backoff_s,
            "output_sink_max_buffer_factor": self._output_sink_max_buffer_factor,
            "output_sink_max_buffer_bytes": self._output_sink_max_buffer_bytes,
            "output_sink_max_payload_bytes": self._output_sink_max_payload_bytes,
            "output_sink_max_pending_chunks": self._output_sink_max_pending_chunks,
            "output_sink_max_pending_bytes": self._output_sink_max_pending_bytes,
        }

    def executor_local_accounting_mode(self, adapter: ExecutorAdapterBase) -> str:
        return self._scheduler.accounting_mode(adapter)

    def executor_participates_in_local_accounting(self, adapter: ExecutorAdapterBase) -> bool:
        return self._scheduler.participates_in_local_accounting(adapter)

    def clear_scheduler_reason_if_matches(self, job_id: str, reason: str) -> None:
        snapshot = self.scheduler_note_snapshot(job_id)
        if snapshot.current_scheduler_reason == reason:
            self.record_scheduler_reason(job_id, "")

    def output_sink_pressure_snapshot(self, job_id: str) -> dict[str, Any]:
        handler = self.get_job_result_handler(job_id)
        if handler is None or not hasattr(handler, "snapshot"):
            return {}
        snapshot = handler.snapshot()
        if not isinstance(snapshot, dict):
            return {}
        pending_chunks = int(snapshot.get("buffered_items", 0) or 0)
        pending_bytes = int(snapshot.get("buffered_bytes", 0) or 0)
        return {
            "pending_chunks": pending_chunks,
            "pending_bytes": pending_bytes,
            "max_pending_chunks": int(self._output_sink_max_pending_chunks),
            "max_pending_bytes": int(self._output_sink_max_pending_bytes),
            "soft_payload_bytes": int(self._output_sink_max_payload_bytes),
            "sink_type": str(snapshot.get("type", "") or ""),
        }

    def output_sink_quota_blocked(self, job_id: str) -> tuple[bool, dict[str, Any]]:
        pressure = self.output_sink_pressure_snapshot(job_id)
        if not pressure:
            return False, {}
        blocked = (
            int(pressure["pending_chunks"]) >= int(pressure["max_pending_chunks"])
            or int(pressure["pending_bytes"]) >= int(pressure["max_pending_bytes"])
        )
        return blocked, pressure

    def add_job_event(
        self,
        job_id: str,
        chunk_id: str = "",
        level: str = "INFO",
        event_type: str = "log",
        message: str = "",
        payload: Optional[dict] = None,
    ) -> None:
        self._add_event(
            job_id,
            chunk_id=chunk_id,
            level=level,
            event_type=event_type,
            message=message,
            payload=payload,
        )

    def get_status(self) -> dict:
        return self.runtime_status_service.get_status()

    def get_operational_snapshot(self) -> dict:
        return self.runtime_status_service.get_operational_snapshot()

    def get_healthcheck(self) -> dict[str, Any]:
        return self.runtime_status_service.get_healthcheck()

    def _current_loop_backoff_s(self) -> float:
        base_interval = (
            self.active_poll_interval
            if self._has_latency_sensitive_work()
            else self.poll_interval
        )
        if self._consecutive_loop_errors <= 0:
            return float(base_interval)
        return min(
            self._MAX_LOOP_BACKOFF_S,
            self.poll_interval * (2 ** min(self._consecutive_loop_errors, 8)),
        )

    def _has_latency_sensitive_work(self) -> bool:
        if self._staging.active_count() > 0:
            return True

        with self._lock:
            executor_names = {
                item.executor_name
                for item in self.runtime_state.running_chunks.values()
            }
            executor_names.update(
                feed.executor_name
                for feed in self.runtime_state.job_feeds.values()
            )

        return any(
            adapter is not None and adapter.metadata.mode == "local"
            for name in executor_names
            if (adapter := self._executors.get(name)) is not None
        )

    def _wait_for_loop_tick(self) -> None:
        self._loop_wake_event.wait(self._current_loop_backoff_s())
        self._loop_wake_event.clear()

    def _terminalize_interrupted_jobs(self) -> None:
        self._assert_engine_thread()
        self._terminalize_active_jobs(
            reason="runtime_interrupted",
            message="Job failed because a previous MolSuite runtime ended before completion.",
        )

    def _terminalize_active_jobs_on_shutdown(self) -> None:
        self._assert_engine_thread()
        for item in self.running_chunks_snapshot():
            adapter = self._executors.get(item.executor_name)
            if adapter is not None:
                try:
                    adapter.cancel(item.handle_id)
                except Exception:
                    self.logger.debug(
                        "Best-effort cancel failed during shutdown for chunk=%s",
                        item.chunk_id,
                    )
        for job_id in tuple(self.runtime_state.job_feeds):
            self._dispatch_pool.cancel_job(job_id)
            for future in self._staging.cancel_job(job_id):
                future.cancel()
        self.persistence_coordinator.flush()
        self._terminalize_active_jobs(
            reason="runtime_interrupted",
            message="Job failed because the MolSuite runtime was stopped before completion.",
        )

    def _terminalize_active_jobs(self, *, reason: str, message: str) -> None:
        if self.executor_db is None:
            return
        now = datetime.now()
        project_updates: list[tuple[str, UUID | None]] = []
        payload_refs: list[str] = []
        with self.executor_db.get_session() as session:
            jobs = session.exec(
                select(ExecutorJob).where(ExecutorJob.status.in_(JOB_RECOVERABLE_STATUSES))
            ).all()
            job_ids = [job.job_id for job in jobs]
            if not job_ids:
                return
            chunks = session.exec(
                select(ExecutorJobChunk).where(
                    ExecutorJobChunk.job_id.in_(job_ids),
                    ExecutorJobChunk.status.in_(CHUNK_ACTIVE_STATUSES),
                )
            ).all()
            for chunk in chunks:
                chunk.status = "failed"
                chunk.progress = 100.0
                chunk.error = reason
                chunk.updated_at = now
                chunk.finished_at = now
                if chunk.checkpoint_ref:
                    payload_refs.append(chunk.checkpoint_ref)
                session.add(chunk)
            for job in jobs:
                job.status = "failed"
                job.progress = 100.0
                job.error = reason
                job.updated_at = now
                job.finished_at = now
                project_updates.append((job.job_id, job.project_id))
                session.add(job)
                session.add(
                    ExecutorJobEvent(
                        job_id=job.job_id,
                        level="ERROR",
                        event_type="job_interrupted",
                        message=message,
                        payload_json=json.dumps({"reason": reason}),
                        created_at=now,
                    )
                )
            session.commit()

        for payload_ref in payload_refs:
            self.remove_chunk_payload_file(payload_ref)
        for job_id, project_id in project_updates:
            self._update_project_job_index(
                job_id=job_id,
                project_id=project_id,
                status="failed",
                progress=100.0,
                updated_at=now,
            )

    def get_executor_capability_matrix(self) -> dict[str, dict[str, Any]]:
        return self.runtime_status_service.get_executor_capability_matrix()

    # ------------------------------------------------------------------
    # Window feed
    # ------------------------------------------------------------------

    def feed_window(self, feed, max_chunks=None):
        return self.feeding_service.feed_window(feed, max_chunks=max_chunks)

    def _feed_all_windows(self):
        """Refill windows for all active feeds."""
        self.feeding_service.feed_all_windows()

    # ------------------------------------------------------------------
    def _poll_dispatch_completions(self):
        self.dispatch_service.poll_completions()

    def _finalize_dispatch_success(self, dispatch_info, handle_id: str):
        self.dispatch_service.finalize_dispatch_success(dispatch_info, handle_id)

    def _run_loop(self):
        self._engine_thread_id = threading.get_ident()
        try:
            self._terminalize_interrupted_jobs()
            while not self._stop_event.is_set():
                try:
                    submitted = self._drain_engine_commands()
                    if submitted:
                        # Give sequential callers a brief chance to enqueue the
                        # next submit before a slow feed starts materializing.
                        self._loop_wake_event.clear()
                        self._loop_wake_event.wait(self.active_poll_interval)
                        self._loop_wake_event.clear()
                        self._drain_engine_commands()
                    loop_start = time.perf_counter()
                    if self.executor_db is None:
                        self._last_loop_latency_ms = (time.perf_counter() - loop_start) * 1000.0
                        self._loop_iteration_count += 1
                        self._wait_for_loop_tick()
                        continue

                    self._poll_running_chunks()
                    self._poll_dispatch_completions()
                    self._poll_staging_tasks()
                    self._poll_sink_completions()
                    # Flush terminal transitions before feeding/dispatch so freed
                    # window slots and retry-pending chunks are visible this tick.
                    self.persistence_coordinator.flush()
                    self._feed_all_windows()
                    self._run_staging_cycle()
                    self._dispatch_pending_chunks()

                    self._flush_progress_if_due()
                    self._flush_handlers_if_due()
                    self._flush_events()
                    self._sync_heartbeats()
                    self._last_loop_latency_ms = (time.perf_counter() - loop_start) * 1000.0
                    self._loop_iteration_count += 1
                    self._consecutive_loop_errors = 0
                except Exception as exc:
                    self.logger.exception("Executor manager loop error: %s", exc)
                    self._consecutive_loop_errors += 1
                    self._last_loop_error = str(exc)
                    self._last_loop_error_at = datetime.now().isoformat()

                self._wait_for_loop_tick()
        finally:
            self._terminalize_active_jobs_on_shutdown()
            self._command_inbox.reject_pending(RuntimeError("ExecutorManager detenido."))
            self._engine_thread_id = None

    def _poll_running_chunks(self):
        for item in self.running_chunks_snapshot():
            adapter = self._executors.get(item.executor_name)
            if adapter is None:
                self._on_chunk_failed(item, f"Executor no disponible: {item.executor_name}")
                continue

            if adapter.metadata.consumes_local_cpu_tokens:
                latest = adapter.drain_progress(item.handle_id)
                if latest is not None:
                    self.event_recorder.record_chunk_progress(item.job_id, item.chunk_id, min(99.0, float(latest)))

            state, payload, error = adapter.poll(item.handle_id)
            if state == "RUNNING":
                continue
            if state == "DONE":
                self._on_chunk_done(item, payload or {})
            else:
                self._on_chunk_failed(item, error or "Unknown execution error")

    def is_cancel_requested(self, job_id: str) -> bool:
        with self._lock:
            if job_id in self._cancel_requested_jobs:
                return True
            # The in-memory set is authoritative for registered runtimes:
            # cancel_job populates it in-process and recovery repopulates it
            # via register_job_runtime(cancel_requested=...).
            if job_id in self.runtime_state.job_runners:
                return False
        if self.executor_db is None:
            return False
        with self.executor_db.get_session() as session:
            job = session.exec(select(ExecutorJob).where(ExecutorJob.job_id == job_id)).first()
        # "canceled" (terminal) is included so late lifecycle completions —
        # e.g. a setup future polled after the job was already fully canceled
        # and its in-memory runtime state cleaned up — are still suppressed
        # instead of emitting a spurious *_completed event for a dead job.
        return bool(job is not None and job.status in ("cancel_requested", "canceled"))

    def finalize_running_chunk_as_canceled(self, item: RunningChunk, message: str):
        item, _ = self._claim_running_chunk(item)
        if item is None:
            return
        self.persistence_coordinator.enqueue(
            TerminalTransition(
                job_id=item.job_id,
                chunk_id=item.chunk_id,
                intent=INTENT_CANCELED,
                error=message,
                event_message=message,
            )
        )

    def _resolve_cached_callable(self, ref: str) -> Callable[..., Any]:
        cached = self._resolved_callable_cache.get(ref)
        if cached is not None:
            return cached
        fn = resolve_runner(str_to_ref(ref))
        self._resolved_callable_cache[ref] = fn
        return fn

    def _current_staging_slots(self) -> int:
        return self._staging.available_slots()

    def _promote_staging_chunk_to_pending(self, chunk_id: str):
        now = datetime.now()
        with self.executor_db.get_session() as session:
            chunk = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.chunk_id == chunk_id)
            ).first()
            if chunk is None or chunk.status != "staging":
                return
            chunk.status = "pending"
            chunk.updated_at = now
            session.add(chunk)
            session.commit()

    def _build_data_context_mapping(self, job: ExecutorJob) -> dict[str, Any]:
        payload = _safe_json_loads(job.payload_json)
        raw = payload.get("_data_context") or {}
        if not isinstance(raw, dict):
            raw = {}
        mapping = dict(raw)
        raw_params = payload.get("_chunker_params")
        mapping["job_params"] = dict(raw_params) if isinstance(raw_params, dict) else {}
        mapping["job_name"] = str(payload.get("job_name") or job.task_type or "")
        if "project_id" not in mapping:
            mapping["project_id"] = str(job.project_id) if job.project_id else ""
        if str(mapping.get("hpc_wdir", "")).strip() == "":
            project_root = mapping.get("project_path") or mapping.get("project_dir")
            if project_root:
                mapping["hpc_wdir"] = str(Path(str(project_root)).expanduser().resolve() / "tmp" / "hpc_wdir")
        return mapping

    def _get_or_create_job_payload_dir(self, job_id: str) -> Path:
        return self.payload_store.get_or_create_job_payload_dir(job_id)

    def encode_chunk_payload_for_storage(
        self,
        *,
        job_id: str,
        chunk_id: str,
        payload_obj: dict[str, Any],
    ) -> tuple[str, str]:
        return self.payload_store.encode_payload(job_id, chunk_id, payload_obj)

    def decode_chunk_payload_from_storage(self, payload_json: str) -> dict[str, Any]:
        return self.payload_store.decode_payload(payload_json)

    def remove_chunk_payload_file(self, path_str: str):
        self.payload_store.remove_payload_file(path_str)

    def _cleanup_job_payload_spool(self, job_id: str):
        self.payload_store.cleanup_job(job_id)

    def _build_executor_transport_mapping(self, job: ExecutorJob) -> dict[str, Any]:
        adapter = self._executors.get(job.executor_name)
        mapping: dict[str, Any] = {}
        if adapter is not None:
            metadata = adapter.metadata
            mapping.update(
                {
                    "executor_backend": metadata.backend,
                    "executor_mode": metadata.mode,
                    "executor_shared_fs": metadata.shared_filesystem,
                }
            )

        data_context = self._build_data_context_mapping(job)
        for key in ("executor_backend", "executor_mode", "executor_shared_fs", "hpc_wdir"):
            value = data_context.get(key)
            if value not in ("", None):
                mapping[key] = value
        return mapping

    def _default_stage_materialize_payload(self, payload: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
        data_context = DataContext.from_mapping(context)
        profile = ExecutorTransportProfile.from_mapping(context)
        materialized = self._data_bridge.materialize_payload(payload, data_context, executor_profile=profile)
        if not isinstance(materialized, dict):
            raise DataContractError("Materialized chunk payload must be a dict.")
        return materialized

    def _run_staging_cycle(self):
        self._lifecycle_controller.run_staging_cycle()

    def _schedule_setup(self, job: ExecutorJob, lifecycle: JobLifecycle):
        self._lifecycle_controller.schedule_setup(job, lifecycle)

    def _schedule_finalize(
        self,
        job: ExecutorJob,
        lifecycle: JobLifecycle,
        terminal_status: str = "completed",
    ):
        return self._lifecycle_controller.schedule_finalize(
            job,
            lifecycle,
            terminal_status=terminal_status,
        )

    def _poll_staging_tasks(self):
        self._lifecycle_controller.poll_staging_tasks()

    def _mark_chunk_staging_completed(self, job_id: str, chunk_id: str, payload: Optional[dict]):
        self._lifecycle_controller.mark_chunk_staging_completed(job_id, chunk_id, payload)

    def _mark_chunk_stage_failed(self, job_id: str, chunk_id: str, error_msg: str):
        self._lifecycle_controller.mark_chunk_stage_failed(job_id, chunk_id, error_msg)

    def _dispatch_pending_chunks(self):
        self.dispatch_service.dispatch_pending()

    @staticmethod
    def job_max_cpu_limit(job: ExecutorJob) -> int | None:
        payload = _safe_json_loads(job.payload_json)
        raw = payload.get("_max_job_cpu")
        if raw in (None, ""):
            return None
        return max(1, int(raw))

    def _flush_progress_if_due(self):
        """Delegate progress flushing to the EventRecorder service."""
        self.event_recorder.flush_progress()

    # ------------------------------------------------------------------
    # Result handler flush
    # ------------------------------------------------------------------

    def _flush_handlers_if_due(self):
        now = time.monotonic()
        if now - self._last_handler_flush < self.event_recorder.progress_flush_interval:
            return
        self._last_handler_flush = now
        self._flush_all_handlers()

    def _flush_all_handlers(self):
        with self._lock:
            handlers = dict(self._job_result_handlers)
        for job_id, handler in handlers.items():
            try:
                handler.flush()
                self.refresh_job_status(job_id)
            except Exception as exc:
                self.logger.exception("ResultHandler flush error: %s", exc)
                self._add_event(
                    job_id,
                    level="ERROR",
                    event_type="result_sink_failed",
                    message=f"Result handler flush failed: {exc}",
                )
                self.mark_job_failed_from_stage(job_id, f"Result handler flush failed: {exc}")
