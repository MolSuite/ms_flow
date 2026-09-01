from __future__ import annotations

from concurrent.futures import Future
from datetime import datetime
from typing import Any, Callable, Optional

from sqlmodel import select

from ms_flow.core.data import payload_has_input_specs
from ms_flow.core.database import ProjectStore
from ms_flow.core.database.executor_models import ExecutorJob, ExecutorJobChunk
from ms_flow.core.executor.runtime_state import JobLifecycle


CHUNK_ACTIVE_STATUSES = ("pending", "running", "staging")
JOB_ACTIVE_STATUSES = ("pending", "running", "staging")

class LifecycleController:
    def __init__(self, manager: Any):
        self._manager = manager

    def mark_job_failed_from_stage(self, job_id: str, reason: str):
        self._manager.mark_job_failed_from_stage(job_id, reason)

    def run_staging_cycle(self):
        manager = self._manager
        if not manager._staging.ready():
            return
        slots = manager._staging.available_slots()
        if slots <= 0:
            return

        # "cancel_requested" is included so staging chunks orphaned by a
        # cancellation race (a chunk born "staging" by the feed around the same
        # time cancel_job runs its sweep, whose staging future was canceled and
        # will never complete) are still seen here and finalized via the
        # is_cancel_requested guard below. Without it, such a chunk is never
        # re-examined and the job is stuck at cancel_requested forever.
        staging_job_statuses = (*JOB_ACTIVE_STATUSES, "cancel_requested")
        with manager.executor_db.get_session() as session:
            rows = session.exec(
                select(ExecutorJobChunk, ExecutorJob)
                .join(ExecutorJob, ExecutorJob.job_id == ExecutorJobChunk.job_id)
                .where(
                    ExecutorJobChunk.status == "staging",
                    ExecutorJob.status.in_(staging_job_statuses),
                )
                .limit(300)
            ).all()

        def sort_key(pair):
            chunk, job = pair
            if job.queue_policy == "priority":
                return (0, -job.priority, job.created_at, chunk.created_at)
            return (1, job.created_at, chunk.created_at)

        rows.sort(key=sort_key)
        running_chunk_ids = {meta.chunk_id for meta in manager._staging.snapshot()}

        for chunk, job in rows:
            if slots <= 0:
                break
            if chunk.chunk_id in running_chunk_ids:
                continue
            if manager.is_cancel_requested(job.job_id):
                manager._mark_chunk_canceled(chunk.chunk_id, job_id=job.job_id)
                continue

            lifecycle = manager.get_job_lifecycle(job.job_id)
            try:
                payload = manager.decode_chunk_payload_from_storage(chunk.payload_json)
            except Exception as exc:
                self.mark_chunk_stage_failed(job.job_id, chunk.chunk_id, f"Invalid chunk payload: {exc}")
                continue
            has_input_specs = payload_has_input_specs(payload)
            stage_fn: Optional[Callable[..., Any]] = None
            stage_context = {
                "job_id": job.job_id,
                "chunk_id": chunk.chunk_id,
                "executor_name": job.executor_name,
                "project_id": str(job.project_id) if job.project_id else "",
            }

            if lifecycle and lifecycle.setup_ref:
                if lifecycle.setup_failed:
                    self.mark_chunk_stage_failed(job.job_id, chunk.chunk_id, "Setup failed previously.")
                    continue
                if not lifecycle.setup_done:
                    if not lifecycle.setup_started:
                        self.schedule_setup(job, lifecycle)
                        slots -= 1
                    continue

            if lifecycle and lifecycle.stage_ref:
                try:
                    stage_fn = manager._resolve_cached_callable(lifecycle.stage_ref)
                except Exception as exc:
                    self.mark_chunk_stage_failed(job.job_id, chunk.chunk_id, f"Cannot resolve stage callable: {exc}")
                    continue
                stage_context["setup_data"] = dict(lifecycle.setup_data)
            elif has_input_specs:
                stage_fn = manager._default_stage_materialize_payload
                stage_context.update(manager._build_data_context_mapping(job))
                stage_context.update(manager._build_executor_transport_mapping(job))
            else:
                manager._promote_staging_chunk_to_pending(chunk.chunk_id)
                continue

            token = f"chunk:{chunk.chunk_id}"
            manager._staging.submit(
                token=token,
                kind="chunk",
                job_id=job.job_id,
                chunk_id=chunk.chunk_id,
                call_with_optional_context=manager._call_with_optional_context,
                fn=stage_fn,
                payload=payload,
                context=stage_context,
                on_done=manager.signal_runtime_work,
            )
            manager._add_event(
                job.job_id,
                chunk_id=chunk.chunk_id,
                level="INFO",
                event_type="chunk_staging_started",
                message="Chunk entered staging.",
            )
            running_chunk_ids.add(chunk.chunk_id)
            slots -= 1

    def schedule_setup(self, job: ExecutorJob, lifecycle: JobLifecycle):
        manager = self._manager
        if not manager._staging.ready() or not lifecycle.setup_ref or lifecycle.setup_started:
            return
        try:
            setup_fn = manager._resolve_cached_callable(lifecycle.setup_ref)
        except Exception as exc:
            lifecycle.setup_failed = True
            self.mark_job_failed_from_stage(job.job_id, f"Cannot resolve setup callable: {exc}")
            return

        lifecycle.setup_started = True
        context = {
            "job_id": job.job_id,
            "executor_name": job.executor_name,
            "project_id": str(job.project_id) if job.project_id else "",
        }
        context.update(manager._build_data_context_mapping(job))
        token = f"setup:{job.job_id}"
        manager._staging.submit(
            token=token,
            kind="setup",
            job_id=job.job_id,
            chunk_id="",
            call_with_optional_context=manager._call_with_optional_context,
            fn=setup_fn,
            payload={"job_id": job.job_id},
            context=context,
            on_done=manager.signal_runtime_work,
        )
        manager._add_event(job.job_id, level="INFO", event_type="job_setup_started", message="Job setup started.")

    def schedule_finalize(
        self,
        job: ExecutorJob,
        lifecycle: JobLifecycle,
        *,
        terminal_status: str = "completed",
    ):
        manager = self._manager
        if not manager._staging.ready():
            return False
        if not lifecycle.finalize_ref or lifecycle.finalize_done or lifecycle.finalize_started:
            return False
        try:
            finalize_fn = manager._resolve_cached_callable(lifecycle.finalize_ref)
        except Exception as exc:
            self.mark_job_failed_from_stage(job.job_id, f"Cannot resolve finalize callable: {exc}")
            return False

        lifecycle.finalize_started = True
        context = {
            "job_id": job.job_id,
            "executor_name": job.executor_name,
            "project_id": str(job.project_id) if job.project_id else "",
            "setup_data": dict(lifecycle.setup_data),
            "terminal_status": str(terminal_status or ""),
        }
        context.update(manager._build_data_context_mapping(job))
        token = f"finalize:{job.job_id}"
        manager._staging.submit(
            token=token,
            kind="finalize",
            job_id=job.job_id,
            chunk_id="",
            call_with_optional_context=manager._call_with_optional_context,
            fn=finalize_fn,
            payload={"job_id": job.job_id},
            context=context,
            on_done=manager.signal_runtime_work,
        )
        manager._add_event(job.job_id, level="INFO", event_type="job_finalize_started", message="Job finalization started.")
        return True

    def poll_staging_tasks(self):
        manager = self._manager
        snapshot = manager._staging.pop_completed()
        if not snapshot:
            return

        for meta in snapshot:
            future: Future = meta.future
            kind = str(meta.kind or "chunk")
            job_id = str(meta.job_id or "")
            chunk_id = str(meta.chunk_id or "")
            lifecycle = manager.get_job_lifecycle(job_id)
            try:
                result = future.result()
            except Exception as exc:
                error_msg = f"{exc}"
                if kind == "setup":
                    if lifecycle is not None:
                        lifecycle.setup_failed = True
                        lifecycle.setup_started = False
                    self.mark_job_failed_from_stage(job_id, f"Setup failed: {error_msg}")
                elif kind == "finalize":
                    if lifecycle is not None:
                        lifecycle.finalize_started = False
                    self.mark_job_failed_from_stage(job_id, f"Finalize failed: {error_msg}")
                else:
                    self.mark_chunk_stage_failed(job_id, chunk_id, f"Staging failed: {error_msg}")
                continue

            if kind == "setup":
                if manager.is_cancel_requested(job_id):
                    manager.refresh_job_status(job_id)
                    continue
                if lifecycle is not None:
                    lifecycle.setup_done = True
                    lifecycle.setup_started = False
                    lifecycle.setup_data = result if isinstance(result, dict) else {}
                manager._add_event(job_id, level="INFO", event_type="job_setup_completed", message="Job setup completed.")
                manager.refresh_job_status(job_id)
                continue

            if kind == "finalize":
                if manager.is_cancel_requested(job_id):
                    manager.refresh_job_status(job_id)
                    continue
                if lifecycle is not None:
                    lifecycle.finalize_done = True
                    lifecycle.finalize_started = False
                manager._add_event(job_id, level="INFO", event_type="job_finalize_completed", message="Job finalization completed.")
                manager.refresh_job_status(job_id)
                continue

            payload = result if isinstance(result, dict) else None
            if manager.is_cancel_requested(job_id):
                manager._mark_chunk_canceled(chunk_id, job_id=job_id)
                manager._add_event(
                    job_id,
                    chunk_id=chunk_id,
                    level="WARNING",
                    event_type="chunk_canceled",
                    message="Chunk canceled while awaiting staging completion.",
                )
                manager.refresh_job_status(job_id)
                continue
            self.mark_chunk_staging_completed(job_id, chunk_id, payload)

    def mark_chunk_staging_completed(self, job_id: str, chunk_id: str, payload: Optional[dict]):
        manager = self._manager
        if manager.is_cancel_requested(job_id):
            manager._mark_chunk_canceled(chunk_id, job_id=job_id)
            return
        now = datetime.now()
        old_ref: Optional[str] = None
        new_ref = ""
        payload_json = ""
        if payload is not None:
            payload_json, payload_ref = manager.encode_chunk_payload_for_storage(
                job_id=job_id,
                chunk_id=chunk_id,
                payload_obj=payload,
            )
            new_ref = payload_ref

        with manager.executor_db.get_session() as session:
            chunk = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.chunk_id == chunk_id)
            ).first()
            if chunk is None or chunk.status != "staging":
                if new_ref:
                    manager.remove_chunk_payload_file(new_ref)
                return
            old_ref = chunk.checkpoint_ref
            if payload is not None:
                chunk.payload_json = payload_json
                chunk.checkpoint_ref = new_ref
            chunk.status = "pending"
            chunk.error = ""
            chunk.updated_at = now
            session.add(chunk)
            session.commit()

        if payload is not None and old_ref and old_ref != new_ref:
            manager.remove_chunk_payload_file(old_ref)

        manager._add_event(
            job_id,
            chunk_id=chunk_id,
            level="INFO",
            event_type="chunk_staging_completed",
            message="Chunk staging completed.",
        )
        manager.refresh_job_status(job_id)

    def mark_chunk_stage_failed(self, job_id: str, chunk_id: str, error_msg: str):
        manager = self._manager
        now = datetime.now()
        with manager.executor_db.get_session() as session:
            chunk = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.chunk_id == chunk_id)
            ).first()
            if chunk is None or chunk.status != "staging":
                return
            payload_ref = chunk.checkpoint_ref
            chunk.status = "stage_failed"
            chunk.progress = 100.0
            chunk.error = error_msg
            chunk.updated_at = now
            chunk.finished_at = now
            session.add(chunk)
            session.commit()
        manager.remove_chunk_payload_file(payload_ref)

        lifecycle = manager.get_job_lifecycle(job_id)
        manager._add_event(
            job_id,
            chunk_id=chunk_id,
            level="ERROR",
            event_type="chunk_stage_failed",
            message=error_msg,
        )
        if lifecycle is not None:
            lifecycle.stage_failures += 1
            if lifecycle.stage_fail_policy == "fail_fast":
                self.mark_job_failed_from_stage(job_id, f"Stage failure in chunk {chunk_id}: {error_msg}")
                return
            if lifecycle.stage_failures > lifecycle.max_stage_failures:
                self.mark_job_failed_from_stage(
                    job_id,
                    (
                        f"Stage failures exceeded threshold "
                        f"({lifecycle.stage_failures}>{lifecycle.max_stage_failures}). "
                        f"Last error: {error_msg}"
                    ),
                )
                return

        manager.refresh_job_status(job_id)

    def close_job_resources(self, attached_resources: list[Any]):
        for resource in attached_resources:
            if isinstance(resource, ProjectStore):
                try:
                    resource.disconnect()
                except Exception:
                    pass
