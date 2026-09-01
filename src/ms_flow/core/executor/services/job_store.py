from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, List, Optional, Union
from uuid import UUID

from ms_flow.core.executor.utils import (
    _safe_json_dumps,
    _safe_json_loads,
    JOB_RECOVERABLE_STATUSES,
)
from ms_flow.core.database.executor_models import ExecutorJob, ExecutorJobFeedState
from ms_flow.core.database.master_models import ProjectJobIndex
from ms_flow.core.executor.job_snapshot import JobSnapshot
from ms_flow.core.executor.runtime_state import ExecutorRuntimeState, JobLifecycle
from sqlmodel import select


# Sentinel that means "fetch feed_state from DB" vs an explicit None meaning
# "this job has no feed_state row".  Using object() avoids any accidental
# equality match with real values.
_AUTO = object()


@dataclass(frozen=True)
class JobStoreDeps:
    # Providers keep DB rebind/unbind visible without owning either store.
    executor_db_provider: Callable[[], Any]
    master_db_provider: Callable[[], Any]
    runtime_state: ExecutorRuntimeState
    record_scheduler_reason: Callable[..., None]
    get_feed_state_row: Callable[[Any, str], Any]
    job_max_cpu_limit: Callable[[ExecutorJob], int | None]
    resolve_scheduler_block_reason: Callable[..., str]
    scheduler_note_snapshot: Callable[[str], Any]


class JobStore:
    """
    Service for CRUD and synchronization of ExecutorJob and ProjectJobIndex.
    Centralizes all job metadata persistence.
    """

    def __init__(self, deps: JobStoreDeps):
        self.deps = deps
        self.logger = logging.getLogger("molsuite.executor.job_store")

    @property
    def executor_db(self):
        return self.deps.executor_db_provider()

    @property
    def master_db(self):
        return self.deps.master_db_provider()

    def get_job(self, job_id: str) -> Optional[JobSnapshot]:
        """Fetch job snapshot with full metrics and derived status."""
        if self.executor_db is None:
            return None
            
        with self.executor_db.get_session() as session:
            job = session.exec(select(ExecutorJob).where(ExecutorJob.job_id == job_id)).first()
            if job is None:
                return None
            snapshot = self.build_snapshot(session, job)
        self.deps.record_scheduler_reason(job_id, snapshot.scheduler_block_reason)
        return snapshot

    def list_jobs(
        self,
        *,
        project_id: Optional[Union[UUID, str]] = None,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[JobSnapshot]:
        """List job snapshots with batching to avoid N+1 queries."""
        if self.executor_db is None:
            return []

        with self.executor_db.get_session() as session:
            stmt = select(ExecutorJob)
            if project_id:
                stmt = stmt.where(ExecutorJob.project_id == project_id)
            if status:
                stmt = stmt.where(ExecutorJob.status == status)
                
            stmt = stmt.order_by(ExecutorJob.created_at.desc()).limit(limit).offset(offset)
            jobs = session.exec(stmt).all()
            if not jobs:
                return []
            snapshots = self.build_snapshot_batch(session, jobs)

        for snapshot in snapshots:
            self.deps.record_scheduler_reason(snapshot.job_id, snapshot.scheduler_block_reason)
        return snapshots

    def build_snapshot(
        self,
        session,
        job: ExecutorJob,
        feed_state: Any = _AUTO,
    ) -> JobSnapshot:
        """Build a single snapshot, orchestrating all monitoring components.

        Parameters
        ----------
        feed_state:
            Supply a pre-loaded ``ExecutorJobFeedState`` row (or ``None`` when
            the job has no feed state row) to skip the per-job DB query.
            Leave as the default ``_AUTO`` sentinel to fetch automatically.
            ``build_snapshot_batch`` always passes a value so the batch
            bulk-load is actually used.
        """
        from ms_flow.core.executor.job_monitoring import (
            build_job_runtime_metrics,
            derive_job_status,
            build_job_snapshot,
        )

        if feed_state is _AUTO:
            feed_state = self.deps.get_feed_state_row(session, job.job_id)
        metrics = build_job_runtime_metrics(session, job, feed_state)

        feed = self.deps.runtime_state.get_job_feed(job.job_id)
        lifecycle = self.deps.runtime_state.get_job_lifecycle(job.job_id)
        if lifecycle is None:
            lifecycle = self._load_persisted_lifecycle(job)
        if feed is not None:
            feed_exhausted = bool(feed.exhausted)
        elif feed_state is not None:
            feed_exhausted = bool(feed_state.exhausted)
        elif job.status in JOB_RECOVERABLE_STATUSES and job.finished_at is None:
            feed_exhausted = False
        else:
            feed_exhausted = True
        
        derived_status, derived_progress = derive_job_status(
            job=job,
            lifecycle=lifecycle,
            metrics=metrics,
            feed_exhausted=feed_exhausted,
        )

        handler = self.deps.runtime_state.get_result_handler(job.job_id)
        sink_snapshot = handler.snapshot() if hasattr(handler, "snapshot") else None
        buffered_items = int((sink_snapshot or {}).get("buffered_items", 0) or 0)
        ack_lagging = (
            feed_state is not None
            and int(metrics.processed) > int(feed_state.items_acked or 0)
        )

        # Snapshots must not advertise a terminal state while output flush or
        # finalize work is still pending, otherwise waiters can observe a false
        # "completed" before artifacts are durable.
        if derived_status in {"completed", "failed", "canceled"}:
            if (
                derived_status == "completed"
                and lifecycle is not None
                and lifecycle.finalize_ref
                and not lifecycle.finalize_done
            ):
                derived_status = "running"
                derived_progress = min(float(derived_progress), 99.0)
            elif derived_status == "completed" and ack_lagging:
                derived_status = "running"
                derived_progress = min(float(derived_progress), 99.0)
            elif derived_status == "completed" and buffered_items > 0:
                derived_status = "running"
                derived_progress = min(float(derived_progress), 99.0)
            elif derived_status == "completed" and int(metrics.sink_lag_chunks) > 0:
                derived_status = "running"
                derived_progress = min(float(derived_progress), 99.0)
            elif derived_status == "completed" and job.finished_at is None:
                derived_status = "running"
                derived_progress = min(float(derived_progress), 99.0)
            elif (
                derived_status == "canceled"
                and job.status == "cancel_requested"
                and job.finished_at is None
                and self.deps.runtime_state.has_cancel_request(job.job_id)
            ):
                derived_status = "cancel_requested"
                derived_progress = min(float(derived_progress), 99.0)

        max_job_cpu = self.deps.job_max_cpu_limit(job)
        scheduler_block_reason = self.deps.resolve_scheduler_block_reason(
            job=job,
            status=derived_status,
            metrics=metrics,
        )
        scheduler_notes = self.deps.scheduler_note_snapshot(job.job_id)
        persisted_reason = str(job.scheduler_reason or "")
        preferred_reason = scheduler_notes.current_scheduler_reason or persisted_reason
        if preferred_reason and derived_status not in {"completed", "failed", "canceled", "cancel_requested"} and (
            not scheduler_block_reason
            or scheduler_block_reason in {"waiting_for_feed", "waiting_for_dispatch"}
        ):
            scheduler_block_reason = preferred_reason
        snapshot_status = "pending" if derived_status == "pending_feed" else derived_status

        return build_job_snapshot(
            job=job,
            status=snapshot_status,
            progress=derived_progress,
            metrics=metrics,
            feed_exhausted=feed_exhausted,
            max_job_cpu=max_job_cpu,
            scheduler_block_reason=scheduler_block_reason,
            scheduler_notes=scheduler_notes,
            output_sink=sink_snapshot,
            live_chunks_emitted=int(feed.total_emitted) if feed else None,
        )

    def _load_persisted_lifecycle(self, job: ExecutorJob) -> Optional[JobLifecycle]:
        payload = _safe_json_loads(job.payload_json)
        lifecycle_meta = payload.get("_lifecycle") or {}
        if not isinstance(lifecycle_meta, dict) or not lifecycle_meta:
            return None
        return JobLifecycle(
            setup_ref=str(lifecycle_meta.get("setup_ref", "") or ""),
            stage_ref=str(lifecycle_meta.get("stage_ref", "") or ""),
            finalize_ref=str(lifecycle_meta.get("finalize_ref", "") or ""),
            stage_fail_policy=str(lifecycle_meta.get("stage_fail_policy", "fail_fast") or "fail_fast"),
            max_stage_failures=max(0, int(lifecycle_meta.get("max_stage_failures", 0) or 0)),
            setup_done=True,
            finalize_done=True,
        )

    def build_snapshot_batch(self, session, jobs: List[ExecutorJob]) -> List[JobSnapshot]:
        """Build snapshots for a batch of jobs efficiently.

        Bulk-loads feed states in a single query and passes them to
        ``build_snapshot`` to avoid one extra DB round-trip per job.
        """
        if not jobs:
            return []

        # Single query for all feed states in the batch.
        job_ids = [j.job_id for j in jobs]
        feed_states: dict[str, ExecutorJobFeedState] = {
            row.job_id: row
            for row in session.exec(
                select(ExecutorJobFeedState).where(ExecutorJobFeedState.job_id.in_(job_ids))
            ).all()
        }

        # Pass the pre-loaded feed_state (or None when absent) so
        # build_snapshot skips the per-job feed_state query.
        return [
            self.build_snapshot(session, job, feed_states.get(job.job_id))
            for job in jobs
        ]

    def create_job(
        self,
        job_id: str,
        project_id: Optional[Union[UUID, str]],
        origin_id: str,
        task_type: str,
        executor_name: str,
        queue_policy: str,
        priority: int,
        payload_json: str,
        depends_on_json: str,
        total_chunks: Optional[int] = None,
    ):
        """Initial insertion of job and feed state into DB."""
        if self.executor_db is None:
            raise RuntimeError("No executor_db bound.")

        now = datetime.now()
        with self.executor_db.get_session() as session:
            session.add(
                ExecutorJob(
                    job_id=job_id,
                    project_id=project_id,
                    origin_id=origin_id,
                    task_type=task_type,
                    executor_name=executor_name,
                    queue_policy=queue_policy,
                    priority=priority,
                    status="pending",
                    progress=0.0,
                    total_emitted=0,
                    total_chunks=total_chunks,
                    payload_json=payload_json,
                    depends_on=depends_on_json,
                    created_at=now,
                    updated_at=now,
                )
            )
            session.commit()

        # Update project index in master DB
        if project_id and self.master_db:
            with self.master_db.get_session() as session:
                session.add(
                    ProjectJobIndex(
                        project_id=project_id,
                        job_id=job_id,
                        origin_id=origin_id,
                        task_type=task_type,
                        status="pending",
                        progress=0.0,
                        created_at=now,
                        updated_at=now,
                    )
                )
                session.commit()

    def update_job_metadata(
        self,
        job_id: str,
        project_id: Optional[Union[UUID, str]],
        status: str,
        progress: float,
        updated_at: datetime,
    ):
        """Synchronize job status and progress back to the master DB index."""
        if not project_id or not self.master_db:
            return

        try:
            with self.master_db.get_session() as session:
                idx = session.exec(
                    select(ProjectJobIndex).where(ProjectJobIndex.job_id == job_id)
                ).first()
                if idx:
                    idx.status = status
                    idx.progress = progress
                    idx.updated_at = updated_at
                    session.add(idx)
                    session.commit()
        except Exception as exc:
            self.logger.warning("Failed to update ProjectJobIndex for job %s: %s", job_id, exc)

    def has_active_jobs(self) -> bool:
        """Check if any jobs are currently in a recoverable (non-terminal) status."""
        if self.executor_db is None:
            return False
        with self.executor_db.get_session() as session:
            row = session.exec(
                select(ExecutorJob.job_id)
                .where(ExecutorJob.status.in_(JOB_RECOVERABLE_STATUSES))
                .limit(1)
            ).first()
        return row is not None
