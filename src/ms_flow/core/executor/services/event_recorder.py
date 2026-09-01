from __future__ import annotations

import logging
import threading
from datetime import datetime
from typing import Dict, List, Optional, TYPE_CHECKING

from sqlalchemy import update
from ms_flow.core.executor.utils import _safe_json_dumps
from ms_flow.core.database.executor_models import ExecutorJobChunk, ExecutorJobEvent

if TYPE_CHECKING:
    from ms_flow.core.executor.manager import ExecutorManager


# Event taxonomy (MRF-061 contract):
#   Structural events  — persisted unconditionally; drive trazabilidad.
#     job_submitted, job_canceled, job_failed, job_interrupted,
#     job_setup_*/job_finalize_*, job_configuration_warning,
#     chunk_failed, chunk_stage_failed, chunk_canceled,
#     result_sink_failed, dispatch_failed, dispatch_timeout.
#   Operational events — high-frequency; suppress in hot path to bound log growth.
#     chunk_dispatched, chunk_completed (emitted via PersistenceCoordinator),
#     scheduler_reason_* (gated on reason change via _record_scheduler_reason).
#
# Progress updates are NOT persisted as events.  They flow through the
# progress_buffer (chunk_id → (job_id, progress)) and are flushed in a
# single batched UPDATE per cadence tick — no SELECT per chunk.


class EventRecorder:
    """
    Buffers and persists job events and progress updates.

    Two independent buffers:
      event_buffer   — append-only rows (ExecutorJobEvent), flushed via flush().
      progress_buffer — latest progress per running chunk (job_id, progress),
                        flushed via flush_progress() with no per-chunk SELECT.
    """

    def __init__(
        self,
        executor_manager: "ExecutorManager",
        progress_flush_interval: float = 2.0,
    ):
        self.manager = executor_manager
        self.progress_flush_interval = float(progress_flush_interval)
        self.logger = logging.getLogger("molsuite.executor.event_recorder")

        self.event_buffer: List[ExecutorJobEvent] = []
        # chunk_id → (job_id, progress)  — avoids per-chunk SELECT in flush_progress
        self.progress_buffer: Dict[str, tuple[str, float]] = {}
        self.last_progress_flush: float = 0.0
        self.lock = threading.RLock()

    def add_event(
        self,
        job_id: str,
        chunk_id: str = "",
        level: str = "INFO",
        event_type: str = "log",
        message: str = "",
        payload: Optional[dict] = None,
    ):
        """Append a job event to the in-memory buffer."""
        with self.lock:
            self.event_buffer.append(
                ExecutorJobEvent(
                    job_id=job_id,
                    chunk_id=chunk_id,
                    level=level,
                    event_type=event_type,
                    message=message,
                    payload_json=_safe_json_dumps(payload) if payload else "{}",
                    created_at=datetime.now(),
                )
            )

    def record_chunk_progress(self, job_id: str, chunk_id: str, progress: float):
        """Buffer the latest progress for a running chunk.

        Stores (job_id, progress) so flush_progress can issue a bulk UPDATE
        without a SELECT to resolve the owning job.
        """
        with self.lock:
            self.progress_buffer[chunk_id] = (str(job_id), float(progress))

    def flush(self):
        """Persist all buffered events to DB in one transaction."""
        self._flush_events()

    def _flush_events(self):
        with self.lock:
            executor_db = self.manager.executor_db
            if not self.event_buffer or executor_db is None:
                return
            batch = list(self.event_buffer)
            self.event_buffer.clear()

        try:
            with executor_db.get_session() as session:
                for event in batch:
                    session.add(event)
                session.commit()
        except Exception as exc:
            self.logger.error("Failed to flush executor events: %s", exc)
            # Batch discarded on persistent DB error to prevent unbounded growth.

    def flush_progress(self, *, force: bool = False):
        """Persist buffered progress updates.

        Uses column-scoped UPDATE (no SELECT per chunk) — one transaction for
        the entire snapshot, then one refresh_job_status per dirty job.
        """
        now_ts = datetime.now().timestamp()
        if not force and now_ts - self.last_progress_flush < self.progress_flush_interval:
            return

        with self.lock:
            executor_db = self.manager.executor_db
            if not self.progress_buffer or executor_db is None:
                return
            snapshot = dict(self.progress_buffer)
            self.progress_buffer.clear()
            self.last_progress_flush = now_ts

        db_now = datetime.now()
        dirty_jobs: set[str] = set()

        try:
            with executor_db.get_session() as session:
                for chunk_id, (job_id, progress) in snapshot.items():
                    session.exec(
                        update(ExecutorJobChunk)
                        .where(ExecutorJobChunk.chunk_id == chunk_id)
                        .where(ExecutorJobChunk.status == "running")
                        .values(progress=progress, updated_at=db_now)
                    )
                    dirty_jobs.add(job_id)
                session.commit()
        except Exception as exc:
            self.logger.error("Failed to flush chunk progress: %s", exc)
            return

        for job_id in dirty_jobs:
            try:
                self.manager.refresh_job_status(job_id)
            except Exception as exc:
                self.logger.error("Failed to refresh job status after progress flush job=%s: %s", job_id, exc)

    def clear(self):
        """Wipe all buffers (called on stop)."""
        with self.lock:
            self.event_buffer.clear()
            self.progress_buffer.clear()
