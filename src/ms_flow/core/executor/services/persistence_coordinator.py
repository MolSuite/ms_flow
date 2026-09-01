from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlmodel import select

from ms_flow.core.database.executor_models import ExecutorJobChunk

if TYPE_CHECKING:
    from ms_flow.core.executor.manager import ExecutorManager

logger = logging.getLogger("molsuite.executor.persistence")

INTENT_COMPLETED = "completed"
INTENT_FAILED = "failed"
INTENT_CANCELED = "canceled"

_TERMINAL_STATUSES = {"completed", "failed", "canceled"}

# Side-effect chains triggered during a flush (e.g. terminal cleanup canceling
# leftover running chunks) may enqueue new transitions; they are re-drained in
# the same flush up to this many passes, then deferred to the next cadence.
_MAX_FLUSH_PASSES = 8


@dataclass
class TerminalTransition:
    job_id: str
    chunk_id: str
    intent: str
    error: str = ""
    output_json: Optional[str] = None
    event_message: str = ""
    emit_event: bool = True
    notify_handler_error: bool = False


@dataclass
class _ResolvedEffect:
    transition: TerminalTransition
    status: str
    payload_ref: Optional[str]


class PersistenceCoordinator:
    """Single batched write path for terminal chunk transitions.

    Producers enqueue transitions (memory only); ``flush`` applies the whole
    batch to ``executor.db`` in one transaction per pass and refreshes each
    affected job's status once per flush instead of once per chunk.
    """

    def __init__(self, manager: "ExecutorManager"):
        self.manager = manager
        self._lock = threading.Lock()
        self._pending: list[TerminalTransition] = []
        self._dirty_jobs: set[str] = set()
        self._flushing = False
        self._last_flush_batch = 0
        self._total_flushed = 0
        self._total_flushes = 0

    # ------------------------------------------------------------------
    # Producer API
    # ------------------------------------------------------------------

    def enqueue(self, transition: TerminalTransition) -> None:
        with self._lock:
            self._pending.append(transition)
            if transition.job_id:
                self._dirty_jobs.add(transition.job_id)
        self.flush_if_unmanaged()

    def mark_job_dirty(self, job_id: str) -> None:
        with self._lock:
            self._dirty_jobs.add(job_id)
        self.flush_if_unmanaged()

    def has_pending(self) -> bool:
        with self._lock:
            return bool(self._pending or self._dirty_jobs)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "pending_transitions": len(self._pending),
                "dirty_jobs": len(self._dirty_jobs),
                "last_flush_batch": self._last_flush_batch,
                "total_flushed": self._total_flushed,
                "total_flushes": self._total_flushes,
            }

    # ------------------------------------------------------------------
    # Flush
    # ------------------------------------------------------------------

    def flush_if_unmanaged(self) -> None:
        """Preserve synchronous semantics when the manager loop is not running."""
        if not self.manager.manager_thread_alive():
            self.flush()

    def flush(self) -> None:
        if self._flushing:
            return
        self._flushing = True
        try:
            for _ in range(_MAX_FLUSH_PASSES):
                with self._lock:
                    transitions = self._pending
                    dirty = self._dirty_jobs
                    self._pending = []
                    self._dirty_jobs = set()
                if not transitions and not dirty:
                    return
                self._flush_pass(transitions, dirty)
        finally:
            self._flushing = False

    def _flush_pass(self, transitions: list[TerminalTransition], dirty: set[str]) -> None:
        if self.manager.executor_db is None:
            return
        effects: list[_ResolvedEffect] = []
        if transitions:
            now = datetime.now()
            chunk_ids = [t.chunk_id for t in transitions]
            with self.manager.executor_db.get_session() as session:
                rows = session.exec(
                    select(ExecutorJobChunk).where(ExecutorJobChunk.chunk_id.in_(chunk_ids))
                ).all()
                by_id = {row.chunk_id: row for row in rows}
                for transition in transitions:
                    row = by_id.get(transition.chunk_id)
                    if row is None:
                        continue
                    effect = self._resolve_transition(row, transition)
                    if effect is None:
                        continue
                    values = self._transition_values(effect, transition, now)
                    for key, value in values.items():
                        setattr(row, key, value)
                    session.add(row)
                    effects.append(effect)
                session.commit()

            self._last_flush_batch = len(effects)
            self._total_flushed += len(effects)
            self._run_side_effects(effects)
        self._total_flushes += 1

        for job_id in sorted(dirty):
            try:
                self.manager.refresh_job_status(job_id)
            except Exception as exc:
                logger.exception("Job status refresh failed for job=%s: %s", job_id, exc)

    def _resolve_transition(self, row, transition: TerminalTransition) -> Optional[_ResolvedEffect]:
        """Resolve one already-serialized terminal transition."""
        current_status = row.status
        checkpoint_ref = row.checkpoint_ref
        if current_status in _TERMINAL_STATUSES:
            return None

        if transition.intent == INTENT_COMPLETED:
            return _ResolvedEffect(transition, "completed", checkpoint_ref)

        if transition.intent == INTENT_FAILED:
            if self.manager.is_cancel_requested(transition.job_id):
                return _ResolvedEffect(transition, "canceled", checkpoint_ref)
            return _ResolvedEffect(transition, "failed", checkpoint_ref)

        if transition.intent == INTENT_CANCELED:
            return _ResolvedEffect(transition, "canceled", checkpoint_ref)

        logger.error("Unknown terminal transition intent: %s", transition.intent)
        return None

    @staticmethod
    def _transition_values(effect: _ResolvedEffect, transition: TerminalTransition, now: datetime) -> dict:
        if effect.status == "completed":
            return {
                "status": "completed",
                "progress": 100.0,
                "output_json": transition.output_json if transition.output_json is not None else "{}",
                "updated_at": now,
                "finished_at": now,
            }
        values = {
            "status": effect.status,
            "progress": 100.0,
            "updated_at": now,
            "finished_at": now,
        }
        if transition.error or effect.status == "failed":
            values["error"] = transition.error
        return values

    def _run_side_effects(self, effects: list[_ResolvedEffect]) -> None:
        for effect in effects:
            transition = effect.transition
            if effect.status in _TERMINAL_STATUSES:
                self.manager.remove_chunk_payload_file(effect.payload_ref)

            if effect.status == "completed":
                if transition.emit_event:
                    self.manager.add_job_event(
                        transition.job_id,
                        chunk_id=transition.chunk_id,
                        level="INFO",
                        event_type="chunk_completed",
                        message=transition.event_message or "Chunk completed",
                    )
                self.manager.register_chunk_success_for_fail_fast(transition.job_id)
            elif effect.status == "failed":
                self.manager.add_job_event(
                    transition.job_id,
                    chunk_id=transition.chunk_id,
                    level="ERROR",
                    event_type="chunk_failed",
                    message=transition.event_message or f"Chunk failed permanently: {transition.error}",
                )
                if transition.notify_handler_error:
                    handler = self.manager.get_job_result_handler(transition.job_id)
                    if handler is not None:
                        try:
                            handler.on_error(transition.chunk_id, transition.error)
                        except Exception as exc:
                            logger.exception("ResultHandler.on_error error: %s", exc)
                self.manager.register_chunk_failure_for_fail_fast(transition.job_id)
            elif effect.status == "canceled" and transition.emit_event:
                self.manager.add_job_event(
                    transition.job_id,
                    chunk_id=transition.chunk_id,
                    level="WARNING",
                    event_type="chunk_canceled",
                    message=transition.event_message or "Chunk canceled.",
                )
