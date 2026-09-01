from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Dict, Optional

from sqlmodel import select

from ms_flow.core.database.executor_models import ExecutorHeartbeat, ExecutorJob
from ms_flow.core.executor.runtime_state import JobLifecycle, RunningChunk
from ms_flow.core.executor.result_handlers import OutputSpecResultHandler, ResultHandler
from ms_flow.core.executor.services.persistence_coordinator import (
    INTENT_CANCELED,
    INTENT_COMPLETED,
    INTENT_FAILED,
    TerminalTransition,
)
from ms_flow.core.executor.services.sink_writer_pool import SinkWriteTask
from ms_flow.core.executor.utils import _safe_json_dumps


class JobStatusService:
    """Chunk/job status writes, event flushing and heartbeats for ExecutorManager.

    This service owns DB status transitions, terminal chunk handling, event writes
    and executor heartbeats. It is composed into ExecutorManager instead of
    inherited as a mixin.
    """

    def __init__(self, manager):
        self._manager = manager

    def __getattr__(self, name):
        return getattr(self._manager, name)

    # ------------------------------------------------------------------
    # DB status updates
    # ------------------------------------------------------------------

    def _on_chunk_done(self, item: RunningChunk, payload: dict):
        item, cancel_won = self._claim_running_chunk(item, check_cancel=True)
        if item is None:
            return
        if cancel_won:
            self.persistence_coordinator.enqueue(
                TerminalTransition(
                    job_id=item.job_id,
                    chunk_id=item.chunk_id,
                    intent=INTENT_CANCELED,
                    error="Chunk completed after cancellation request; result discarded.",
                    event_message="Chunk completed after cancellation request; result discarded.",
                )
            )
            return

        store = self.stores_job_results(item.job_id)
        handler = self.get_job_result_handler(item.job_id)
        output_json = _safe_json_dumps(payload.get("result")) if store else "{}"
        if isinstance(handler, OutputSpecResultHandler):
            staged = None
            try:
                staged = handler.stage(item.chunk_id, payload.get("result"))
                self._sink_writer_pool.submit(
                    SinkWriteTask(
                        job_id=item.job_id,
                        chunk_id=item.chunk_id,
                        handler=handler,
                        staged=staged,
                        output_json=output_json,
                    )
                )
            except Exception as exc:
                if staged is not None:
                    handler.reject(staged, str(exc))
                handler.on_error(item.chunk_id, str(exc))
                self._add_event(
                    item.job_id,
                    chunk_id=item.chunk_id,
                    level="ERROR",
                    event_type="result_sink_failed",
                    message=f"Result sink enqueue failed: {exc}",
                )
                self.persistence_coordinator.enqueue(
                    TerminalTransition(
                        job_id=item.job_id,
                        chunk_id=item.chunk_id,
                        intent=INTENT_FAILED,
                        error=f"Result sink enqueue failed: {exc}",
                    )
                )
            return
        if handler is not None:
            try:
                handler.handle(item.chunk_id, payload.get("result"))
            except Exception as exc:
                self.logger.exception("ResultHandler.handle error chunk=%s: %s", item.chunk_id, exc)
                self.add_job_event(
                    item.job_id,
                    chunk_id=item.chunk_id,
                    level="ERROR",
                    event_type="result_sink_failed",
                    message=f"Result handler write failed: {exc}",
                )
                self.persistence_coordinator.enqueue(
                    TerminalTransition(
                        job_id=item.job_id,
                        chunk_id=item.chunk_id,
                        intent=INTENT_FAILED,
                        error=f"Result handler write failed: {exc}",
                    )
                )
                self.event_recorder.flush()
                return

        self.persistence_coordinator.enqueue(
            TerminalTransition(
                job_id=item.job_id,
                chunk_id=item.chunk_id,
                intent=INTENT_COMPLETED,
                output_json=output_json,
            )
        )

    def _poll_sink_completions(self) -> None:
        completions = self._sink_writer_pool.drain_completions()
        if not completions:
            return

        confirmations: list[tuple[SinkWriteTask, dict[str, Any]]] = []
        rejections: list[tuple[SinkWriteTask, str, str]] = []

        for completion in completions:
            task = completion.task
            if completion.error:
                self._add_event(
                    task.job_id,
                    chunk_id=task.chunk_id,
                    level="ERROR",
                    event_type="result_sink_failed",
                    message=completion.error,
                )
                rejections.append((task, completion.error, INTENT_FAILED))
                continue
            if self.is_cancel_requested(task.job_id):
                rejections.append(
                    (
                        task,
                        "Sink write finished after cancellation request; result discarded.",
                        INTENT_CANCELED,
                    )
                )
                continue
            confirmations.append((task, completion.receipt or {}))

        for handler, items in self._group_sink_items(confirmations):
            try:
                self._confirm_sink_items(handler, items)
            except Exception as exc:
                failed_items = [
                    (task, f"Failed to confirm sink write: {exc}", INTENT_FAILED)
                    for task, _receipt in items
                ]
                self._reject_sink_items(handler, failed_items)
                for task, error, intent in failed_items:
                    self.persistence_coordinator.enqueue(
                        TerminalTransition(
                            job_id=task.job_id,
                            chunk_id=task.chunk_id,
                            intent=intent,
                            error=error,
                        )
                    )
                continue

            for task, _receipt in items:
                self.persistence_coordinator.enqueue(
                    TerminalTransition(
                        job_id=task.job_id,
                        chunk_id=task.chunk_id,
                        intent=INTENT_COMPLETED,
                        output_json=task.output_json,
                    )
                )

        for handler, items in self._group_sink_items(rejections):
            self._reject_sink_items(handler, items)
            for task, error, intent in items:
                self.persistence_coordinator.enqueue(
                    TerminalTransition(
                        job_id=task.job_id,
                        chunk_id=task.chunk_id,
                        intent=intent,
                        error=error,
                    )
                )

    @staticmethod
    def _group_sink_items(items: list[tuple]) -> list[tuple[Any, list[tuple]]]:
        grouped: dict[int, tuple[Any, list[tuple]]] = {}
        for item in items:
            handler = item[0].handler
            key = id(handler)
            if key not in grouped:
                grouped[key] = (handler, [])
            grouped[key][1].append(item)
        return list(grouped.values())

    @staticmethod
    def _confirm_sink_items(handler: Any, items: list[tuple[SinkWriteTask, dict[str, Any]]]) -> None:
        confirm_batch = getattr(handler, "confirm_batch", None)
        if callable(confirm_batch):
            confirm_batch([(task.staged, receipt) for task, receipt in items])
            return
        for task, receipt in items:
            handler.confirm(task.staged, receipt)

    @staticmethod
    def _reject_sink_items(handler: Any, items: list[tuple[SinkWriteTask, str, str]]) -> None:
        reject_batch = getattr(handler, "reject_batch", None)
        if callable(reject_batch):
            reject_batch([(task.staged, error) for task, error, _intent in items])
            return
        for task, error, _intent in items:
            handler.reject(task.staged, error)

    def _on_chunk_failed(self, item: RunningChunk, error_msg: str):
        if self.is_cancel_requested(item.job_id):
            self.finalize_running_chunk_as_canceled(
                item,
                f"Chunk terminated after cancellation request: {error_msg}",
            )
            return

        item, _ = self._claim_running_chunk(item)
        if item is None:
            return

        self.persistence_coordinator.enqueue(
            TerminalTransition(
                job_id=item.job_id,
                chunk_id=item.chunk_id,
                intent=INTENT_FAILED,
                error=error_msg,
                notify_handler_error=True,
            )
        )

    def _mark_chunk_failed(self, chunk_id: str, job_id: str, error_msg: str):
        self.persistence_coordinator.enqueue(
            TerminalTransition(
                job_id=job_id,
                chunk_id=chunk_id,
                intent=INTENT_FAILED,
                error=error_msg,
                event_message=error_msg,
            )
        )

    def _mark_chunk_canceled(self, chunk_id: str, job_id: str = ""):
        self.persistence_coordinator.enqueue(
            TerminalTransition(
                job_id=job_id,
                chunk_id=chunk_id,
                intent=INTENT_CANCELED,
                emit_event=False,
            )
        )

    def refresh_job_status(self, job_id: str):
        finalize_to_schedule: tuple[ExecutorJob, JobLifecycle, str] | None = None
        final_flush_error = ""
        cleanup = False

        with self.executor_db.get_session() as session:
            job = session.exec(select(ExecutorJob).where(ExecutorJob.job_id == job_id)).first()
            if job is None:
                return

            feed_state = self.get_feed_state_row(session, job_id)
            feed = self.get_job_feed(job_id)
            lifecycle = self.get_job_lifecycle(job_id)
            if feed is not None:
                persisted_exhausted = bool(feed_state.exhausted) if feed_state is not None else False
                feed_exhausted = bool(feed.exhausted) or persisted_exhausted
            elif feed_state is not None:
                feed_exhausted = bool(feed_state.exhausted)
            else:
                feed_exhausted = True

            metrics = self._build_job_runtime_metrics(session, job, feed_state)
            refresh_plan = self._evaluate_job_refresh(
                job=job,
                lifecycle=lifecycle,
                metrics=metrics,
                feed_exhausted=feed_exhausted,
            )

            now = datetime.now()
            job_values: dict[str, Any] = {
                "status": refresh_plan.persisted_status,
                "progress": refresh_plan.persisted_progress,
                "updated_at": now,
                "loop_latency_ms": float(self._last_loop_latency_ms),
            }
            if job.started_at is not None and int(metrics.processed) > 0:
                elapsed_s = max(0.001, (now - job.started_at).total_seconds())
                job_values["throughput_eps"] = round(int(metrics.processed) / elapsed_s, 6)
            else:
                job_values["throughput_eps"] = 0.0

            if refresh_plan.schedule_finalize and lifecycle is not None:
                finalize_to_schedule = (
                    job,
                    lifecycle,
                    refresh_plan.derived_status,
                )
            elif refresh_plan.cleanup_terminal:
                if refresh_plan.derived_status == "completed" and lifecycle is not None and lifecycle.finalize_ref:
                    self._add_event(
                        job_id,
                        level="INFO",
                        event_type="job_finalize_completed",
                        message="Finalization completed.",
                    )
                    self.event_recorder.flush()
                job_values["finished_at"] = now
                cleanup = True

            if cleanup:
                handler = self.get_job_result_handler(job_id)
                if handler is not None:
                    try:
                        handler.flush()
                    except Exception as exc:
                        final_flush_error = f"Final result flush failed: {exc}"
                        self.logger.exception(final_flush_error)
                        job_values["status"] = "failed"
                        job_values["error"] = final_flush_error
                        job_values["progress"] = 100.0

            if feed_state is not None:
                feed_state.items_acked = int(refresh_plan.processed)
                feed_state.cursor_position = max(
                    int(feed_state.cursor_position or 0),
                    int(feed.total_emitted or 0) if feed is not None else 0,
                )
                feed_state.exhausted = bool(refresh_plan.feed_exhausted)
                feed_state.updated_at = now
                session.add(feed_state)

            for key, value in job_values.items():
                setattr(job, key, value)
            session.add(job)
            session.commit()

            project_id = job.project_id
            status = job_values["status"]
            progress = job_values["progress"]
            updated_at = now

        self.job_store.update_job_metadata(
            job_id=job_id,
            project_id=project_id,
            status=status,
            progress=progress,
            updated_at=updated_at,
        )
        self.record_scheduler_reason(
            job_id,
            self.resolve_scheduler_block_reason(
                job=job,
                status=status,
                metrics=metrics,
            ),
        )

        if finalize_to_schedule is not None:
            self._schedule_finalize(*finalize_to_schedule)
            return
        if not cleanup:
            return
        if status == "canceled":
            self._add_event(job_id, level="WARNING", event_type="job_canceled", message="Job canceled.")
            self._log_executor(
                logging.WARNING,
                "Job %s canceled and aborted.",
                job_id,
                extra={"job_id": job_id},
            )
            self.record_scheduler_reason(job_id, "")
            self.event_recorder.flush()
        if final_flush_error:
            self._add_event(
                job_id,
                level="ERROR",
                event_type="result_sink_failed",
                message=final_flush_error,
            )
            self.event_recorder.flush()
        self._cleanup_job_runtime(job_id, flush_handler=False)

    def _add_event(
        self,
        job_id: str,
        chunk_id: str = "",
        level: str = "INFO",
        event_type: str = "log",
        message: str = "",
        payload: Optional[dict] = None,
    ):
        """Buffer a job event locally to be flushed in batch later."""
        self.event_recorder.add_event(
            job_id=job_id,
            chunk_id=chunk_id,
            level=level,
            event_type=event_type,
            message=message,
            payload=payload,
        )

    def _flush_events(self):
        """Write all buffered events to DB in a single transaction."""
        self.event_recorder.flush()

    def _upsert_heartbeat(self, executor_name: str, status: str = "online"):
        if self.executor_db is None:
            return
        with self.executor_db.get_session() as session:
            row = session.exec(
                select(ExecutorHeartbeat).where(ExecutorHeartbeat.executor_name == executor_name)
            ).first()
            if row is None:
                row = ExecutorHeartbeat(
                    executor_name=executor_name,
                    status=status,
                    total_cpu=self.total_cpu,
                    used_cpu=0,
                    running_jobs=0,
                    updated_at=datetime.now(),
                )
            else:
                row.status = status
                row.updated_at = datetime.now()
            session.add(row)
            session.commit()

    def _sync_heartbeats(self):
        if self.executor_db is None:
            return
        now = time.monotonic()
        if (
            self._last_heartbeat_sync_at > 0.0
            and now - self._last_heartbeat_sync_at < self._heartbeat_sync_interval_s
        ):
            return
        with self._lock:
            running_items = list(self._running_chunks.values())
        used_cpu = self._scheduler.used_cpu

        running_by_executor: Dict[str, set] = {}
        for item in running_items:
            running_by_executor.setdefault(item.executor_name, set()).add(item.job_id)

        with self.executor_db.get_session() as session:
            for executor_name in self._executors.keys():
                row = session.exec(
                    select(ExecutorHeartbeat).where(ExecutorHeartbeat.executor_name == executor_name)
                ).first()
                if row is None:
                    row = ExecutorHeartbeat(executor_name=executor_name)
                row.status = "online"
                row.total_cpu = self.total_cpu
                row.used_cpu = used_cpu
                row.running_jobs = len(running_by_executor.get(executor_name, set()))
                row.running_chunks = len([i for i in running_items if i.executor_name == executor_name])
                row.loop_latency_ms = self._last_loop_latency_ms
                row.updated_at = datetime.now()
                session.add(row)
            session.commit()
        self._last_heartbeat_sync_at = now
