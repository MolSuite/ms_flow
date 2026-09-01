from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any, Iterator, Optional

from sqlmodel import select

from ms_flow.core.data import payload_has_input_specs, to_wire_value
from ms_flow.core.database.executor_models import ExecutorJob, ExecutorJobChunk, ExecutorJobFeedState
from ms_flow.core.executor.utils import _safe_json_dumps, _safe_json_loads

if TYPE_CHECKING:
    from ms_flow.core.executor.manager import ExecutorManager, JobFeed

logger = logging.getLogger("molsuite.executor.feeding")

class FeedingService:
    def __init__(self, manager: "ExecutorManager"):
        self.manager = manager

    def feed_all_windows(self):
        feeds = self.manager.job_feeds_snapshot()

        active_feeds = [f for f in feeds if not f.exhausted]
        if not active_feeds:
            return

        for feed in active_feeds:
            if not self._check_dependencies(feed.job_id):
                continue
            self.feed_window(feed, max_chunks=1)

        for feed in active_feeds:
            # Check dependencies before feeding
            if not self._check_dependencies(feed.job_id):
                continue
            
            self.feed_window(feed)

    def _activate_deferred_source(self, job: ExecutorJob) -> bool:
        feed = self.manager.get_job_feed(job.job_id)
        if feed is None or feed.source_ready:
            return True

        payload = _safe_json_loads(job.payload_json)
        lifecycle_meta = payload.get("_lifecycle") or {}
        try:
            item_source, resources = self.manager.submission_service.restore_chunk_source(
                job=job,
                payload=payload,
                lifecycle_meta=lifecycle_meta,
                cursor_position=0,
            )
        except Exception as exc:
            logger.exception("Failed to activate deferred chunk source for job=%s: %s", job.job_id, exc)
            self.manager._mark_job_failed(job.job_id, f"Failed to activate deferred chunk source: {exc}")
            return False

        payload["_deferred_chunk_build_pending"] = False
        with feed.lock:
            feed.item_source = item_source
            feed.source_ready = True
            feed.attached_resources.extend(resources)

        if self.manager.executor_db is not None:
            with self.manager.executor_db.get_session() as session:
                job_row = session.exec(select(ExecutorJob).where(ExecutorJob.job_id == job.job_id)).first()
                if job_row is not None:
                    job_row.payload_json = _safe_json_dumps(payload)
                    job_row.updated_at = datetime.now()
                    session.add(job_row)
                    session.commit()
        return True

    def _check_dependencies(self, job_id: str) -> bool:
        """Returns True if dependencies are satisfied and deferred source is ready."""
        if self.manager.executor_db is None:
            return False

        dependency_failure_reason = ""
        resolved_job: ExecutorJob | None = None
        dependencies_ready = False
        with self.manager.executor_db.get_session() as session:
            job = session.exec(select(ExecutorJob).where(ExecutorJob.job_id == job_id)).first()
            if not job:
                return False
            resolved_job = job
            if not job.depends_on:
                self.manager.record_scheduler_reason(job_id, "")
                dependencies_ready = True
            else:
                try:
                    deps = json.loads(job.depends_on)
                except (json.JSONDecodeError, TypeError, ValueError):
                    logger.warning(
                        "Invalid depends_on JSON for job=%s, treating as no dependencies: %s",
                        job_id, job.depends_on,
                    )
                    deps = []

                if not deps:
                    self.manager.record_scheduler_reason(job_id, "")
                    dependencies_ready = True
                else:
                    # Check status of each dependency
                    unfinished = []
                    failed_or_canceled: list[str] = []
                    for dep_id in deps:
                        dep_job = session.exec(select(ExecutorJob).where(ExecutorJob.job_id == dep_id)).first()
                        if dep_job is None:
                            unfinished.append(dep_id)
                            continue
                        if dep_job.status == "completed":
                            continue
                        if dep_job.status in {"failed", "canceled"}:
                            failed_or_canceled.append(dep_id)
                            continue
                        unfinished.append(dep_id)

                    if failed_or_canceled:
                        dependency_failure_reason = (
                            f"Canceled because dependency failed or was canceled: {', '.join(failed_or_canceled)}"
                        )
                    elif unfinished:
                        self.manager.record_scheduler_reason(
                            job_id,
                            "waiting_for_dependencies",
                            payload={"dependencies": unfinished},
                        )
                        return False
                    else:
                        self.manager.record_scheduler_reason(job_id, "")
                        dependencies_ready = True

        if dependency_failure_reason:
            self.manager._mark_job_canceled(job_id, dependency_failure_reason)
            return False

        if not dependencies_ready:
            return False

        if resolved_job is None:
            return False
        return self._activate_deferred_source(resolved_job)

    def feed_window(self, feed: "JobFeed", *, max_chunks: int | None = None) -> int:
        if self.manager.executor_db is None or not feed.source_ready:
            return 0
        if feed.exhausted:
            return 0
        sink_blocked, sink_pressure = self.manager.output_sink_quota_blocked(feed.job_id)
        if sink_blocked:
            self.manager.record_scheduler_reason(
                feed.job_id,
                "waiting_for_output_sink_quota",
                payload=sink_pressure,
            )
            return 0
        self.manager.clear_scheduler_reason_if_matches(feed.job_id, "waiting_for_output_sink_quota")

        chunk_active_statuses = ("pending", "running", "staging")
        
        with self.manager.executor_db.get_session() as session:
            durable_active_ids = set(session.exec(
                select(ExecutorJobChunk.chunk_id).where(
                    ExecutorJobChunk.job_id == feed.job_id,
                    ExecutorJobChunk.status.in_(chunk_active_statuses),
                )
            ).all())
            submit_active_ids = self.manager._dispatch_pool.active_chunk_ids(feed.job_id)
            feed.live_count = len(durable_active_ids | submit_active_ids)

            slots = feed.dispatch_policy.max_inflight_tasks - feed.live_count
            if max_chunks is not None:
                slots = min(slots, max(1, int(max_chunks)))
            
            if slots <= 0:
                return 0

            now = datetime.now()
            inserted = 0
            feed_error = ""
            lifecycle = self.manager.get_job_lifecycle(feed.job_id)
            
            with feed.lock:
                for _ in range(slots):
                    try:
                        if feed.item_source is None:
                            feed_error = "Chunk source is not ready."
                            break
                        chunk_payload = next(feed.item_source)
                    except StopIteration:
                        feed.exhausted = True
                        break
                    except Exception as exc:
                        logger.exception("Error in job generator job=%s: %s", feed.job_id, exc)
                        feed.exhausted = True
                        feed_error = str(exc)
                        break

                    payload_copy = dict(chunk_payload)
                    cpu_required = int(payload_copy.pop("_cpu_required", feed.default_cpu_required))
                    gpu_required = int(payload_copy.pop("_gpu_required", feed.default_gpu_required))

                    payload_wire = to_wire_value(payload_copy)
                    requires_staging = bool(lifecycle and (lifecycle.setup_ref or lifecycle.stage_ref)) or payload_has_input_specs(payload_wire)

                    chunk_id = uuid.uuid4().hex
                    payload_json, payload_ref = self.manager.encode_chunk_payload_for_storage(
                        job_id=feed.job_id,
                        chunk_id=chunk_id,
                        payload_obj=payload_wire,
                    )

                    session.add(ExecutorJobChunk(
                        job_id=feed.job_id,
                        chunk_id=chunk_id,
                        executor_name=feed.executor_name,
                        payload_json=payload_json,
                        output_json="{}",
                        cpu_required=max(1, cpu_required),
                        gpu_required=max(0, gpu_required),
                        status="staging" if requires_staging else "pending",
                        progress=0.0,
                        checkpoint_ref=payload_ref,
                        created_at=now,
                        updated_at=now,
                    ))
                    inserted += 1
                    feed.total_emitted += 1

                # Update job row
                job_row = session.exec(select(ExecutorJob).where(ExecutorJob.job_id == feed.job_id)).first()
                if job_row:
                    job_row.total_emitted = feed.total_emitted
                    if feed.exhausted and job_row.total_chunks is None:
                        job_row.total_chunks = feed.total_emitted
                    job_row.updated_at = now
                    session.add(job_row)

                feed_state = self.manager.ensure_feed_state_row(
                    session, job_id=feed.job_id, now=now
                )
                feed_state.cursor_position = feed.total_emitted
                feed_state.exhausted = feed.exhausted
                feed_state.updated_at = now
                if feed_error:
                    feed_state.last_error = feed_error
                session.add(feed_state)

                if inserted > 0 or feed.exhausted or feed_error:
                    session.commit()
                    feed.live_count += inserted

        if feed.exhausted and feed.total_chunks is None:
            feed.total_chunks = feed.total_emitted

        if inserted > 0:
            logger.debug("Feed job=%s: emitted %s chunks (total=%s exhausted=%s)", 
                        feed.job_id, inserted, feed.total_emitted, feed.exhausted)
            self.manager.refresh_job_status(feed.job_id)
        elif feed.exhausted or feed_error:
            self.manager.refresh_job_status(feed.job_id)
            
        return inserted
