from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import func
from sqlmodel import select

from ms_flow.core.database.executor_models import ExecutorJob, ExecutorJobChunk, ExecutorJobFeedState
from ms_flow.core.executor.job_snapshot import JobSnapshot
from ms_flow.core.executor.runtime_state import JobLifecycle

TERMINAL_JOB_STATUSES = {"completed", "failed", "canceled"}


@dataclass(frozen=True)
class JobRuntimeMetrics:
    done: int
    failed: int
    canceled: int
    stage_failed: int
    staging: int
    running: int
    pending: int
    total: int
    processed: int
    chunks_dispatched: int
    chunks_ready_not_dispatched: int
    chunk_queue_wait_avg_s: float
    chunk_queue_wait_max_s: float
    first_chunk_emitted_at: Optional[datetime]
    first_chunk_dispatched_at: Optional[datetime]
    last_dispatch_attempt_at: Optional[datetime]
    last_progress_at: Optional[datetime]
    chunks_emitted: int
    feed_cursor_position: int
    feed_items_acked: int
    running_cpu: int
    running_progress_sum: float
    running_progress_avg: float
    progress_structural: float
    progress_operational: float
    backlog_chunks: int
    backlog_dispatch_chunks: int
    backlog_stage_chunks: int
    sink_lag_chunks: int
    sink_lag_bytes: int
    sink_oldest_lag_s: Optional[float]
    job_age_s: float
    active_work_age_s: Optional[float]


@dataclass(frozen=True)
class SchedulerNoteSnapshot:
    current_scheduler_reason: str
    last_dispatch_attempt_at: Optional[datetime]
    last_scheduler_reason_at: Optional[datetime]
    last_scheduler_reason: str
    last_scheduler_payload: dict[str, Any] | None = None


@dataclass(frozen=True)
class JobRefreshPlan:
    derived_status: str
    derived_progress: float
    persisted_status: str
    persisted_progress: float
    processed: int
    feed_exhausted: bool
    schedule_finalize: bool
    cleanup_terminal: bool


@dataclass(frozen=True)
class JobStatusDecision:
    status: str
    progress: float


@dataclass(frozen=True)
class RuntimeOverviewMetrics:
    active_by_executor: dict[str, int]
    total_active: int
    backlog_chunks: int
    sink_lag_chunks: int
    sink_lag_bytes: int
    oldest_active_work_age_s: Optional[float]
    oldest_sink_lag_age_s: Optional[float]
    blocked_jobs: int
    blocked_by_reason: dict[str, int]
    quota_blocked_jobs: int
    writer_blocked_jobs: int
    cpu_blocked_jobs: int


@dataclass(frozen=True)
class RuntimeHealthDbSnapshot:
    active_jobs: int
    heartbeat_age_s: float
    heartbeat_stale: bool


def build_job_runtime_metrics(session, job: ExecutorJob, feed_state: Optional[ExecutorJobFeedState]) -> JobRuntimeMetrics:
    from sqlalchemy import case, func
    now = datetime.now()

    counts = session.exec(
        select(ExecutorJobChunk.status, func.count(ExecutorJobChunk.id))
        .where(ExecutorJobChunk.job_id == job.job_id)
        .group_by(ExecutorJobChunk.status)
    ).all()
    status_map = dict(counts)
    done = int(status_map.get("completed", 0) or 0)
    failed = int(status_map.get("failed", 0) or 0)
    canceled = int(status_map.get("canceled", 0) or 0)
    stage_failed = int(status_map.get("stage_failed", 0) or 0)
    staging = int(status_map.get("staging", 0) or 0)
    running = int(status_map.get("running", 0) or 0)
    pending = int(status_map.get("pending", 0) or 0)
    materialized_total = done + failed + canceled + stage_failed + running + pending + staging
    processed = done + failed + canceled + stage_failed

    wait_expr = (
        (func.julianday(ExecutorJobChunk.started_at) - func.julianday(ExecutorJobChunk.created_at))
        * 86400.0
    )
    wait_row = session.exec(
        select(
            func.count(ExecutorJobChunk.id),
            func.avg(wait_expr),
            func.max(wait_expr),
        ).where(
            ExecutorJobChunk.job_id == job.job_id,
            ExecutorJobChunk.started_at.is_not(None),
        )
    ).one()
    chunks_dispatched = int(wait_row[0] or 0)
    chunk_queue_wait_avg_s = float(wait_row[1] or 0.0)
    chunk_queue_wait_max_s = float(wait_row[2] or 0.0)
    first_chunk_emitted_at = session.exec(
        select(func.min(ExecutorJobChunk.created_at)).where(ExecutorJobChunk.job_id == job.job_id)
    ).one()
    first_chunk_dispatched_at = session.exec(
        select(func.min(ExecutorJobChunk.started_at)).where(
            ExecutorJobChunk.job_id == job.job_id,
            ExecutorJobChunk.started_at.is_not(None),
        )
    ).one()
    last_dispatch_attempt_at = session.exec(
        select(func.max(ExecutorJobChunk.started_at)).where(
            ExecutorJobChunk.job_id == job.job_id,
            ExecutorJobChunk.started_at.is_not(None),
        )
    ).one()
    last_progress_at = session.exec(
        select(func.max(ExecutorJobChunk.updated_at)).where(
            ExecutorJobChunk.job_id == job.job_id,
            (
                (ExecutorJobChunk.progress > 0.0)
                | ExecutorJobChunk.finished_at.is_not(None)
            ),
        )
    ).one()

    emitted_from_feed = int(feed_state.cursor_position or 0) if feed_state is not None else int(job.total_emitted or 0)
    declared_total = max(0, int(job.total_chunks or 0))
    total = max(materialized_total, emitted_from_feed, declared_total)
    chunks_emitted = max(materialized_total, emitted_from_feed)
    running_cpu = int(
        session.exec(
            select(func.coalesce(func.sum(ExecutorJobChunk.cpu_required), 0)).where(
                ExecutorJobChunk.job_id == job.job_id,
                ExecutorJobChunk.status == "running",
            )
        ).one()
        or 0
    )
    progress_row = session.exec(
        select(
            func.coalesce(
                func.sum(
                    case(
                        (ExecutorJobChunk.status == "running", ExecutorJobChunk.progress),
                        else_=0.0,
                    )
                ),
                0.0,
            ),
            func.avg(
                case(
                    (ExecutorJobChunk.status == "running", ExecutorJobChunk.progress),
                    else_=None,
                )
            ),
        ).where(ExecutorJobChunk.job_id == job.job_id)
    ).one()
    running_progress_sum = float(progress_row[0] or 0.0)
    running_progress_avg = float(progress_row[1] or 0.0)
    if last_progress_at is None and running_progress_sum > 0.0:
        last_progress_at = job.updated_at or now
    sink_lag_row = session.exec(
        select(
            func.count(ExecutorJobChunk.id),
            func.coalesce(func.sum(func.json_extract(ExecutorJobChunk.output_sink_info_json, "$.payload_bytes")), 0),
            func.min(ExecutorJobChunk.output_produced_at),
        ).where(
            ExecutorJobChunk.job_id == job.job_id,
            ExecutorJobChunk.output_state.in_(("produced", "persisted")),
        )
    ).one()
    sink_lag_chunks = int(sink_lag_row[0] or 0)
    sink_lag_bytes = int(sink_lag_row[1] or 0)
    oldest_sink_at = sink_lag_row[2]
    sink_oldest_lag_s = (
        max(0.0, (now - oldest_sink_at).total_seconds())
        if oldest_sink_at is not None
        else None
    )
    active_work_since = session.exec(
        select(func.min(ExecutorJobChunk.created_at)).where(
            ExecutorJobChunk.job_id == job.job_id,
            ExecutorJobChunk.status.in_(("pending", "running", "staging")),
        )
    ).one()
    active_work_age_s = (
        max(0.0, (now - active_work_since).total_seconds())
        if active_work_since is not None
        else None
    )
    backlog_dispatch_chunks = pending
    backlog_stage_chunks = staging
    backlog_chunks = backlog_dispatch_chunks + backlog_stage_chunks
    job_age_s = max(0.0, (now - job.created_at).total_seconds())
    progress_structural = round((processed / total) * 100.0, 2) if total > 0 else 0.0
    progress_operational = round((((processed * 100.0) + running_progress_sum) / total), 2) if total > 0 else 0.0

    return JobRuntimeMetrics(
        done=done,
        failed=failed,
        canceled=canceled,
        stage_failed=stage_failed,
        staging=staging,
        running=running,
        pending=pending,
        total=total,
        processed=processed,
        chunks_dispatched=chunks_dispatched,
        chunks_ready_not_dispatched=pending,
        chunk_queue_wait_avg_s=max(0.0, chunk_queue_wait_avg_s),
        chunk_queue_wait_max_s=max(0.0, chunk_queue_wait_max_s),
        first_chunk_emitted_at=first_chunk_emitted_at,
        first_chunk_dispatched_at=first_chunk_dispatched_at,
        last_dispatch_attempt_at=last_dispatch_attempt_at,
        last_progress_at=last_progress_at,
        chunks_emitted=chunks_emitted,
        feed_cursor_position=emitted_from_feed,
        feed_items_acked=int(feed_state.items_acked or 0) if feed_state is not None else processed,
        running_cpu=running_cpu,
        running_progress_sum=running_progress_sum,
        running_progress_avg=max(0.0, running_progress_avg),
        progress_structural=progress_structural,
        progress_operational=max(0.0, min(100.0, progress_operational)),
        backlog_chunks=backlog_chunks,
        backlog_dispatch_chunks=backlog_dispatch_chunks,
        backlog_stage_chunks=backlog_stage_chunks,
        sink_lag_chunks=sink_lag_chunks,
        sink_lag_bytes=sink_lag_bytes,
        sink_oldest_lag_s=sink_oldest_lag_s,
        job_age_s=job_age_s,
        active_work_age_s=active_work_age_s,
    )


def _resolve_job_progress(
    *,
    metrics: JobRuntimeMetrics,
    feed_exhausted: bool,
) -> float:
    progress = float(metrics.progress_operational)
    if not feed_exhausted:
        progress = min(progress, 99.0)
    return progress


def _resolve_terminal_job_status(
    *,
    job: ExecutorJob,
    lifecycle: Optional[JobLifecycle],
    metrics: JobRuntimeMetrics,
    feed_exhausted: bool,
) -> str:
    if not (feed_exhausted and metrics.running == 0 and metrics.pending == 0 and metrics.staging == 0):
        return ""
    if metrics.total <= 0:
        # Exhausted feed emitted no chunks: there is no work to do, so the job is
        # complete (not eternally "pending"). Checked before the canceled==total
        # branch so an empty feed is not mislabeled as canceled.
        return "completed"
    if metrics.processed < metrics.total:
        # Exhausted source with no active chunks but outstanding work (e.g.
        # spooled-but-unmaterialized durable items): not terminal yet.
        return ""
    if metrics.failed > 0:
        return "failed"
    if metrics.stage_failed > 0:
        if lifecycle is None or lifecycle.stage_fail_policy == "fail_fast":
            return "failed"
        if metrics.stage_failed > (lifecycle.max_stage_failures or 0):
            return "failed"
        return "completed"
    if metrics.canceled == metrics.total:
        return "canceled"
    return "completed"


def derive_job_status(
    *,
    job: ExecutorJob,
    lifecycle: Optional[JobLifecycle],
    metrics: JobRuntimeMetrics,
    feed_exhausted: bool,
) -> tuple[str, float]:
    progress = _resolve_job_progress(metrics=metrics, feed_exhausted=feed_exhausted)

    forced_terminal = (
        job.status
        if job.status in {"failed", "canceled"} and job.finished_at is not None
        else ""
    )
    if forced_terminal:
        terminal_progress = 100.0 if forced_terminal == "completed" else progress
        return forced_terminal, terminal_progress
    if job.status == "cancel_requested":
        if metrics.running == 0 and metrics.pending == 0 and metrics.staging == 0:
            return "canceled", progress
        return "cancel_requested", min(progress, 99.0)
    terminal_status = _resolve_terminal_job_status(
        job=job,
        lifecycle=lifecycle,
        metrics=metrics,
        feed_exhausted=feed_exhausted,
    )
    if terminal_status:
        terminal_progress = 100.0 if terminal_status == "completed" else progress
        return terminal_status, terminal_progress
    if metrics.running > 0 or metrics.processed > 0 or metrics.stage_failed > 0:
        return "running", progress
    if metrics.staging > 0:
        return "staging", progress
    if metrics.pending > 0:
        return "queued", progress
    if not feed_exhausted:
        return "pending_feed", progress
    return "pending", progress


def evaluate_job_refresh(
    *,
    job: ExecutorJob,
    lifecycle: Optional[JobLifecycle],
    metrics: JobRuntimeMetrics,
    feed_exhausted: bool,
) -> JobRefreshPlan:
    derived_status, derived_progress = derive_job_status(
        job=job,
        lifecycle=lifecycle,
        metrics=metrics,
        feed_exhausted=feed_exhausted,
    )
    persisted_status = derived_status
    persisted_progress = derived_progress
    schedule_finalize = False
    cleanup_terminal = False

    if derived_status in TERMINAL_JOB_STATUSES and lifecycle is not None:
        if derived_status != "canceled" and lifecycle.finalize_ref and not lifecycle.finalize_done:
            schedule_finalize = True
            persisted_status = "running"
            persisted_progress = min(float(derived_progress), 99.0)
        else:
            cleanup_terminal = True
    elif derived_status in TERMINAL_JOB_STATUSES:
        cleanup_terminal = True

    return JobRefreshPlan(
        derived_status=derived_status,
        derived_progress=float(derived_progress),
        persisted_status=persisted_status,
        persisted_progress=float(persisted_progress),
        processed=int(metrics.processed),
        feed_exhausted=bool(feed_exhausted),
        schedule_finalize=bool(schedule_finalize),
        cleanup_terminal=bool(cleanup_terminal),
    )


def resolve_scheduler_block_reason(
    *,
    job: ExecutorJob,
    status: str,
    metrics: JobRuntimeMetrics,
    max_job_cpu: Optional[int],
    available_cpu: int,
    executor_consumes_local_cpu_tokens: bool,
) -> str:
    pending = metrics.pending
    running = metrics.running
    staging = metrics.staging
    if status in TERMINAL_JOB_STATUSES or status == "cancel_requested":
        return ""
    if status == "pending_feed":
        return "waiting_for_feed"
    if status == "staging":
        return "waiting_for_stage"
    if status == "running" and metrics.sink_lag_chunks > 0 and pending == 0 and staging == 0 and running == 0:
        return "waiting_for_sink"
    if status not in {"queued", "running"}:
        return ""

    running_cpu = metrics.running_cpu
    if max_job_cpu is not None and pending > 0 and running_cpu >= max_job_cpu:
        return "waiting_for_job_cpu_cap"
    # "Blocked" means making no progress. A job that already has chunks running is
    # at capacity, not blocked — its pending backlog is just pipelined behind the
    # CPUs it is already using. Only flag CPU starvation when NONE of this job's
    # chunks are running (something else holds every CPU, or it can't get a slot).
    if executor_consumes_local_cpu_tokens and pending > 0 and available_cpu <= 0 and running == 0:
        return "waiting_for_global_cpu"
    if pending > 0 and running == 0:
        return "waiting_for_dispatch"
    return ""


def classify_scheduler_block_reason(reason: str) -> str:
    normalized = str(reason or "").strip()
    if not normalized:
        return ""
    if normalized == "waiting_for_output_sink_quota":
        return "quota"
    if normalized in {"waiting_for_sink", "waiting_for_dispatch"}:
        return "backend"
    if normalized in {"waiting_for_global_cpu", "waiting_for_job_cpu_cap"}:
        return "cpu"
    if normalized == "waiting_for_stage":
        return "staging"
    if normalized == "waiting_for_feed":
        return "feed"
    if normalized == "waiting_for_dependencies":
        return "dependency"
    return "other"


def build_runtime_overview_metrics(session, *, executor_names: tuple[str, ...], now: Optional[datetime] = None) -> RuntimeOverviewMetrics:
    from sqlalchemy import func

    active_by_executor = {name: 0 for name in executor_names}
    total_active = 0
    blocked_jobs = 0
    blocked_by_reason: dict[str, int] = {}
    for job in session.exec(
        select(ExecutorJob).where(ExecutorJob.status.in_(("pending", "pending_feed", "queued", "staging", "running", "cancel_requested")))
    ).all():
        if job.executor_name in active_by_executor:
            active_by_executor[job.executor_name] += 1
        total_active += 1
        reason = str(job.scheduler_reason or "").strip()
        if reason:
            blocked_jobs += 1
            blocked_by_reason[reason] = int(blocked_by_reason.get(reason, 0) or 0) + 1

    backlog_chunks = int(
        session.exec(
            select(func.count(ExecutorJobChunk.id)).where(
                ExecutorJobChunk.status.in_(("pending", "staging"))
            )
        ).one()
        or 0
    )
    sink_lag_row = session.exec(
        select(
            func.count(ExecutorJobChunk.id),
            func.coalesce(func.sum(func.json_extract(ExecutorJobChunk.output_sink_info_json, "$.payload_bytes")), 0),
            func.min(ExecutorJobChunk.output_produced_at),
        ).where(
            ExecutorJobChunk.output_state.in_(("produced", "persisted"))
        )
    ).one()
    oldest_active_at = session.exec(
        select(func.min(ExecutorJobChunk.created_at)).where(
            ExecutorJobChunk.status.in_(("pending", "running", "staging"))
        )
    ).one()
    sink_lag_chunks = int(sink_lag_row[0] or 0)
    sink_lag_bytes = int(sink_lag_row[1] or 0)
    oldest_sink_at = sink_lag_row[2]
    observed_now = now or datetime.now()
    return RuntimeOverviewMetrics(
        active_by_executor=active_by_executor,
        total_active=total_active,
        backlog_chunks=backlog_chunks,
        sink_lag_chunks=sink_lag_chunks,
        sink_lag_bytes=sink_lag_bytes,
        oldest_active_work_age_s=(
            max(0.0, (observed_now - oldest_active_at).total_seconds())
            if oldest_active_at is not None
            else None
        ),
        oldest_sink_lag_age_s=(
            max(0.0, (observed_now - oldest_sink_at).total_seconds())
            if oldest_sink_at is not None
            else None
        ),
        blocked_jobs=blocked_jobs,
        blocked_by_reason=blocked_by_reason,
        quota_blocked_jobs=int(blocked_by_reason.get("waiting_for_output_sink_quota", 0) or 0),
        writer_blocked_jobs=int(blocked_by_reason.get("waiting_for_sink", 0) or 0),
        cpu_blocked_jobs=int(
            (blocked_by_reason.get("waiting_for_global_cpu", 0) or 0)
            + (blocked_by_reason.get("waiting_for_job_cpu_cap", 0) or 0)
        ),
    )


def build_runtime_health_db_snapshot(session, *, poll_interval: float, now: Optional[datetime] = None) -> RuntimeHealthDbSnapshot:
    active_jobs = int(
        session.exec(
            select(ExecutorJob).where(
                ExecutorJob.status.in_(("pending", "pending_feed", "queued", "staging", "running", "cancel_requested"))
            )
        ).all().__len__()
    )
    from ms_flow.core.database.executor_models import ExecutorHeartbeat

    heartbeats = session.exec(select(ExecutorHeartbeat)).all()
    observed_now = now or datetime.now()
    heartbeat_age_s = 0.0
    heartbeat_stale = False
    if heartbeats:
        latest = max(row.updated_at for row in heartbeats if row.updated_at is not None)
        heartbeat_age_s = max(0.0, (observed_now - latest).total_seconds())
        heartbeat_stale = heartbeat_age_s > max(1.0, poll_interval * 10.0)
    return RuntimeHealthDbSnapshot(
        active_jobs=active_jobs,
        heartbeat_age_s=heartbeat_age_s,
        heartbeat_stale=heartbeat_stale,
    )


def build_job_snapshot(
    *,
    job: ExecutorJob,
    status: str,
    progress: float,
    metrics: JobRuntimeMetrics,
    feed_exhausted: bool,
    max_job_cpu: Optional[int],
    scheduler_block_reason: str,
    scheduler_notes: SchedulerNoteSnapshot,
    output_sink: Any,
    live_chunks_emitted: Optional[int] = None,
) -> JobSnapshot:
    progress_structural = float(metrics.progress_structural)
    progress_operational = float(metrics.progress_operational)
    if not feed_exhausted:
        progress_structural = min(progress_structural, 99.0)
        progress_operational = min(progress_operational, 99.0)
    job_queue_wait_s = None
    if job.started_at is not None:
        job_queue_wait_s = max(0.0, (job.started_at - job.created_at).total_seconds())
    throughput_eps = float(job.throughput_eps or 0.0)
    loop_latency_ms = float(job.loop_latency_ms or 0.0)
    chunks_emitted = int(live_chunks_emitted) if live_chunks_emitted is not None else int(metrics.chunks_emitted)
    sink_snapshot = output_sink if isinstance(output_sink, dict) else {}
    scheduler_block_category = classify_scheduler_block_reason(scheduler_block_reason)
    scheduler_block_details = (
        dict(scheduler_notes.last_scheduler_payload or {})
        if scheduler_block_reason and scheduler_block_reason == scheduler_notes.current_scheduler_reason
        else {}
    )
    sink_buffered_items = int(sink_snapshot.get("buffered_items", 0) or 0)
    sink_buffered_bytes = int(sink_snapshot.get("buffered_bytes", 0) or 0)
    sink_pending_chunks_quota = int(sink_snapshot.get("max_pending_chunks", 0) or 0)
    sink_pending_bytes_quota = int(sink_snapshot.get("max_pending_bytes", 0) or 0)
    sink_pending_chunks_pressure = (
        round(sink_buffered_items / sink_pending_chunks_quota, 6)
        if sink_pending_chunks_quota > 0
        else 0.0
    )
    sink_pending_bytes_pressure = (
        round(sink_buffered_bytes / sink_pending_bytes_quota, 6)
        if sink_pending_bytes_quota > 0
        else 0.0
    )
    return JobSnapshot(
        job_id=job.job_id,
        project_id=str(job.project_id) if job.project_id else None,
        origin_id=job.origin_id,
        task_type=job.task_type,
        status=status,
        cancel_requested=status == "cancel_requested",
        error=job.error or "",
        executor_name=job.executor_name,
        progress=progress,
        progress_structural=progress_structural,
        progress_operational=progress_operational,
        progress_running_chunks_avg=float(metrics.running_progress_avg),
        priority=job.priority,
        queue_policy=job.queue_policy,
        chunks_total=int(metrics.total),
        chunks_emitted=chunks_emitted,
        chunks_dispatched=int(metrics.chunks_dispatched),
        chunks_done=int(metrics.done),
        chunks_failed=int(metrics.failed),
        chunks_stage_failed=int(metrics.stage_failed),
        chunks_running=int(metrics.running),
        chunks_pending=int(metrics.pending),
        chunks_staging=int(metrics.staging),
        chunks_ready_not_dispatched=int(metrics.chunks_ready_not_dispatched),
        backlog_chunks=int(metrics.backlog_chunks),
        backlog_dispatch_chunks=int(metrics.backlog_dispatch_chunks),
        backlog_stage_chunks=int(metrics.backlog_stage_chunks),
        feed_exhausted=feed_exhausted,
        feed_cursor_position=int(metrics.feed_cursor_position),
        feed_items_acked=int(metrics.feed_items_acked),
        output_sink=output_sink,
        loop_latency_ms=loop_latency_ms,
        throughput_eps=throughput_eps,
        job_queue_wait_s=job_queue_wait_s,
        chunks_started=int(metrics.chunks_dispatched),
        chunk_queue_wait_avg_s=float(metrics.chunk_queue_wait_avg_s),
        chunk_queue_wait_max_s=float(metrics.chunk_queue_wait_max_s),
        running_cpu=int(metrics.running_cpu),
        max_job_cpu=max_job_cpu,
        sink_lag_chunks=int(metrics.sink_lag_chunks),
        sink_lag_bytes=int(metrics.sink_lag_bytes),
        sink_oldest_lag_s=metrics.sink_oldest_lag_s,
        sink_buffered_items=sink_buffered_items,
        sink_buffered_bytes=sink_buffered_bytes,
        sink_pending_chunks_quota=sink_pending_chunks_quota,
        sink_pending_bytes_quota=sink_pending_bytes_quota,
        sink_pending_chunks_pressure=sink_pending_chunks_pressure,
        sink_pending_bytes_pressure=sink_pending_bytes_pressure,
        sink_writer_flush_count=int(sink_snapshot.get("flush_count", 0) or 0),
        sink_writer_retry_count=int(sink_snapshot.get("retry_count", 0) or 0),
        sink_writer_flush_failures=int(sink_snapshot.get("flush_failures", 0) or 0),
        sink_writer_last_flush_duration_ms=float(sink_snapshot.get("last_flush_duration_ms", 0.0) or 0.0),
        sink_writer_total_bytes_written=int(sink_snapshot.get("total_bytes_written", 0) or 0),
        sink_writer_total_items_written=int(sink_snapshot.get("total_items_written", 0) or 0),
        sink_writer_oversized_items=int(sink_snapshot.get("oversized_items", 0) or 0),
        job_age_s=float(metrics.job_age_s),
        active_work_age_s=metrics.active_work_age_s,
        scheduler_block_reason=scheduler_block_reason,
        scheduler_block_category=scheduler_block_category,
        scheduler_block_details=scheduler_block_details,
        last_dispatch_attempt_at=metrics.last_dispatch_attempt_at or scheduler_notes.last_dispatch_attempt_at,
        last_scheduler_reason_at=scheduler_notes.last_scheduler_reason_at,
        last_scheduler_reason=scheduler_block_reason or scheduler_notes.current_scheduler_reason or scheduler_notes.last_scheduler_reason,
        first_chunk_emitted_at=metrics.first_chunk_emitted_at,
        first_chunk_dispatched_at=metrics.first_chunk_dispatched_at,
        last_progress_at=metrics.last_progress_at,
        created_at=job.created_at,
        updated_at=job.updated_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
    )
