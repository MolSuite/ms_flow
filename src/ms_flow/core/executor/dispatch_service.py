from __future__ import annotations

from datetime import datetime
from typing import Any, List, Tuple

from sqlmodel import select

from ms_flow.core.database.executor_models import ExecutorJob, ExecutorJobChunk
from ms_flow.core.executor.local_adapters import ExecutorAdapterBase, ThreadExecutorAdapter


class DispatchService:
    """Dispatch policy: candidate selection, per-job CPU limits, admission,
    and the chunk submit/finalize handshake with the DispatchPool.

    Holds a back-reference to the ExecutorManager (same pattern as
    LifecycleController). The actual async execution lives in the manager's
    DispatchPool; this service owns only the *policy* of what to dispatch and
    when, plus finalizing successful submissions into running chunks.
    """

    def __init__(self, manager: Any):
        self._manager = manager

    # ------------------------------------------------------------------
    # Admission / candidate selection
    # ------------------------------------------------------------------

    def active_dispatch_executors(self) -> set[str]:
        # Pending chunks can only belong to jobs with a registered runtime
        # (dispatch needs their runner_ref anyway), so executors without an
        # active feed have nothing to dispatch and skip the candidates query.
        manager = self._manager
        with manager._lock:
            return {feed.executor_name for feed in manager.runtime_state.job_feeds.values()}

    def dispatch_pending(self):
        manager = self._manager
        active_executors = self.active_dispatch_executors()
        if not active_executors:
            return
        for executor_name, adapter in manager._executors.items():
            if executor_name not in active_executors:
                continue
            self._candidates_for_executor(adapter, executor_name, ("running",))
            self._candidates_for_executor(
                adapter,
                executor_name,
                ("pending", "pending_feed", "queued", "staging"),
            )

    def _candidates_for_executor(
        self,
        adapter: ExecutorAdapterBase,
        executor_name: str,
        job_statuses: tuple[str, ...],
    ) -> None:
        # Dynamic local CPU admission is backend-agnostic for local process
        # executors: reusable pools still participate through
        # consumes_local_cpu_tokens, so cpu_required remains enforced here.
        manager = self._manager
        cpu_limited = adapter.metadata.consumes_local_cpu_tokens
        candidates = self._pending_candidates(executor_name, job_statuses=job_statuses, limit=200)
        sink_quota_cache: dict[str, tuple[bool, dict[str, Any]]] = {}
        for candidate in manager._scheduler.iter_admissible(
            candidates,
            cpu_limited=cpu_limited,
            executors=manager._executors,
        ):
            sink_state = sink_quota_cache.get(candidate.job.job_id)
            if sink_state is None:
                sink_state = manager.output_sink_quota_blocked(candidate.job.job_id)
                sink_quota_cache[candidate.job.job_id] = sink_state
            sink_blocked, sink_pressure = sink_state
            if sink_blocked:
                manager.record_scheduler_reason(
                    candidate.job.job_id,
                    "waiting_for_output_sink_quota",
                    payload=sink_pressure,
                )
                continue
            manager.clear_scheduler_reason_if_matches(candidate.job.job_id, "waiting_for_output_sink_quota")
            if not self._job_can_dispatch_more_cpu(
                candidate.job,
                candidate_cpu=candidate.cpu_required,
                cpu_limited=cpu_limited,
            ):
                continue
            self._dispatch_chunk(adapter, candidate.job, candidate.chunk)

    def _pending_candidates(
        self,
        executor_name: str,
        job_statuses: tuple[str, ...] = ("pending",),
        limit: int = 500,
    ) -> List[Tuple[ExecutorJobChunk, ExecutorJob]]:
        manager = self._manager
        if manager.executor_db is None:
            return []
        with manager.executor_db.get_session() as session:
            rows = session.exec(
                select(ExecutorJobChunk, ExecutorJob)
                .join(ExecutorJob, ExecutorJob.job_id == ExecutorJobChunk.job_id)
                .where(
                    ExecutorJobChunk.executor_name == executor_name,
                    ExecutorJobChunk.status == "pending",
                    ExecutorJob.status.in_(job_statuses),
                )
                .limit(limit)
            ).all()

        # Filter out chunks that are already in the dispatch pool
        filtered = [
            (chunk, job) for chunk, job in rows
            if not manager._dispatch_pool.is_dispatching(chunk.chunk_id)
        ]
        return manager._scheduler.sort_candidates(filtered)

    # ------------------------------------------------------------------
    # Per-job CPU gating
    # ------------------------------------------------------------------

    def _job_running_cpu_usage(self, job_id: str) -> int:
        manager = self._manager
        with manager._lock:
            running = sum(
                int(item.cpu_required or 0)
                for item in manager._running_chunks.values()
                if item.job_id == job_id
            )
            pending_dispatch = manager._dispatch_pool.get_job_cpu_usage(job_id)
            return running + pending_dispatch

    def _job_can_dispatch_more_cpu(self, job: ExecutorJob, *, candidate_cpu: int, cpu_limited: bool) -> bool:
        if not cpu_limited:
            return True
        max_job_cpu = self._manager.job_max_cpu_limit(job)
        if max_job_cpu is None:
            return True
        running_cpu = self._job_running_cpu_usage(job.job_id)
        return running_cpu + max(1, int(candidate_cpu)) <= max_job_cpu

    # ------------------------------------------------------------------
    # Submit + finalize handshake
    # ------------------------------------------------------------------

    def _dispatch_chunk(
        self,
        adapter: ExecutorAdapterBase,
        job: ExecutorJob,
        chunk: ExecutorJobChunk,
    ) -> bool:
        manager = self._manager
        runner_ref = manager.get_runner_ref(job.job_id)
        if runner_ref is None:
            manager._mark_chunk_failed(
                chunk_id=chunk.chunk_id,
                job_id=job.job_id,
                error_msg="Runner reference not found (job not rehydrated after restart).",
            )
            return False

        try:
            payload = manager.decode_chunk_payload_from_storage(chunk.payload_json)
        except Exception as exc:
            manager._mark_chunk_failed(
                chunk_id=chunk.chunk_id,
                job_id=job.job_id,
                error_msg=f"Invalid chunk payload: {exc}",
            )
            return False
        chunk_id = chunk.chunk_id
        job_id_for_progress = job.job_id

        def _progress_cb(value: float):
            with manager._lock:
                manager.event_recorder.record_chunk_progress(job_id_for_progress, chunk_id, value)

        if manager.is_cancel_requested(job.job_id):
            return False

        try:
            manager._record_dispatch_attempt(job.job_id, chunk_id=chunk.chunk_id)
            submit_context = manager._build_data_context_mapping(job)
            submit_context.update(manager._build_executor_transport_mapping(job))
            submit_context.update(
                {
                    "job_id": job.job_id,
                    "chunk_id": chunk.chunk_id,
                    "cpu_required": int(chunk.cpu_required),
                    "gpu_required": int(chunk.gpu_required),
                    "executor_name": job.executor_name,
                }
            )

            def _submit():
                return adapter.submit(
                    job.job_id,
                    chunk.chunk_id,
                    payload,
                    runner_ref,
                    _progress_cb,
                    submit_context,
                )

            # Thread submissions are already non-blocking because the adapter
            # only enqueues work into its own ThreadPoolExecutor and returns a
            # handle immediately. Routing them through DispatchPool adds an
            # unnecessary async layer and, in practice, can leave the chunk
            # stuck in queued/pending without ever finalizing the dispatch.
            if isinstance(adapter, ThreadExecutorAdapter):
                handle_id = _submit()
                self.finalize_dispatch_success(
                    type(
                        "DirectDispatchInfo",
                        (),
                        {
                            "chunk_id": chunk.chunk_id,
                            "job_id": job.job_id,
                            "executor_name": adapter.name,
                            "cpu_required": 0,
                            "gpu_required": 0,
                        },
                    )(),
                    handle_id,
                )
                return True

            # DispatchPool contributes to sampled occupancy until this submit
            # becomes a registered running chunk.
            local_tokens = adapter.metadata.consumes_local_cpu_tokens
            manager._dispatch_pool.submit(
                chunk_id=chunk.chunk_id,
                job_id=job.job_id,
                executor_name=adapter.name,
                cpu_required=chunk.cpu_required if local_tokens else 0,
                gpu_required=chunk.gpu_required if local_tokens else 0,
                fn=_submit,
                args=(),
                on_done=manager.signal_runtime_work,
            )
            return True  # Dispatch accepted (async)
        except Exception as exc:
            manager._mark_chunk_failed(chunk.chunk_id, job.job_id, f"Dispatch submission failed: {exc}")
            return False

    # ------------------------------------------------------------------
    # Completion polling
    # ------------------------------------------------------------------

    def poll_completions(self):
        """Poll and process results of asynchronous adapter.submit() calls."""
        manager = self._manager
        for completion in manager._dispatch_pool.poll_completed():
            dispatch_info = completion.dispatch
            handle_id = completion.handle_id
            error = completion.error
            if completion.abandoned:
                if handle_id:
                    adapter = manager._executors.get(dispatch_info.executor_name)
                    self._drop_dispatch_handle(adapter, handle_id, dispatch_info.chunk_id)
                continue
            if error:
                manager.logger.error(
                    "Async dispatch failed for job=%s chunk=%s: %s",
                    dispatch_info.job_id,
                    dispatch_info.chunk_id,
                    error,
                )
                error_payload = {
                    "executor_name": dispatch_info.executor_name,
                    "cpu_required": int(dispatch_info.cpu_required or 0),
                    "error": error,
                }
                if "timed out" in str(error).lower():
                    error_payload["timeout_s"] = manager._dispatch_pool.snapshot().get("timeout_s")
                    manager._add_event(
                        dispatch_info.job_id,
                        chunk_id=dispatch_info.chunk_id,
                        level="ERROR",
                        event_type="dispatch_timeout",
                        message=error,
                        payload=error_payload,
                    )
                else:
                    manager._add_event(
                        dispatch_info.job_id,
                        chunk_id=dispatch_info.chunk_id,
                        level="ERROR",
                        event_type="dispatch_failed",
                        message=error,
                        payload=error_payload,
                    )

                manager._mark_chunk_failed(
                    dispatch_info.chunk_id, dispatch_info.job_id, error
                )
                continue

            # Success: register in running_chunks + update DB
            self.finalize_dispatch_success(dispatch_info, handle_id)

    def finalize_dispatch_success(self, dispatch_info, handle_id: str):
        """Persist a successful submit as running, then register runtime ownership."""
        manager = self._manager
        chunk_id = dispatch_info.chunk_id
        job_id = dispatch_info.job_id
        executor_name = dispatch_info.executor_name
        cpu_required = dispatch_info.cpu_required
        gpu_required = getattr(dispatch_info, "gpu_required", 0)
        adapter = manager._executors.get(executor_name)
        now = datetime.now()

        if manager.is_cancel_requested(job_id):
            self._drop_dispatch_handle(adapter, handle_id, chunk_id)
            with manager.executor_db.get_session() as session:
                chunk = session.exec(
                    select(ExecutorJobChunk).where(ExecutorJobChunk.chunk_id == chunk_id)
                ).first()
                if chunk is not None and chunk.status in {"pending", "staging"}:
                    chunk.status = "canceled"
                    chunk.progress = 100.0
                    chunk.updated_at = now
                    chunk.finished_at = now
                    session.add(chunk)
                    session.commit()
            manager._add_event(
                job_id,
                chunk_id=chunk_id,
                level="WARNING",
                event_type="chunk_canceled",
                message="Chunk canceled after dispatch completed during cancellation race.",
            )
            manager.refresh_job_status(job_id)
            return

        queue_wait_s = 0.0
        with manager.executor_db.get_session() as session:
            chunk = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.chunk_id == chunk_id)
            ).first()
            if chunk is None or chunk.status != "pending":
                chunk = None
            else:
                queue_wait_s = max(0.0, (now - chunk.created_at).total_seconds())
                chunk.status = "running"
                chunk.started_at = now
                chunk.updated_at = now
                session.add(chunk)
                session.commit()

        if chunk is None:
            self._drop_dispatch_handle(adapter, handle_id, chunk_id)
            manager.refresh_job_status(job_id)
            return

        manager._register_running_chunk(
            job_id=job_id,
            chunk_id=chunk_id,
            executor_name=executor_name,
            handle_id=handle_id,
            cpu_required=cpu_required,
            gpu_required=gpu_required,
        )

        admittable = ("pending", "staging", "pending_feed", "queued")
        with manager.executor_db.get_session() as session:
            job = session.exec(select(ExecutorJob).where(ExecutorJob.job_id == job_id)).first()
            if job is not None and job.status in admittable:
                job.status = "running"
                job.started_at = job.started_at or now
                job.updated_at = now
                session.add(job)
            session.commit()
        manager._add_event(
            job_id,
            chunk_id=chunk_id,
            level="INFO",
            event_type="chunk_dispatched",
            message=f"Chunk dispatched to {executor_name}",
            payload={"queue_wait_s": queue_wait_s},
        )
        manager.logger.debug(
            "Finalized dispatch for job=%s chunk=%s on executor=%s",
            job_id, chunk_id, executor_name
        )

    @staticmethod
    def _drop_dispatch_handle(adapter, handle_id: str, chunk_id: str) -> None:
        if adapter is not None and handle_id:
            try:
                adapter.cancel(handle_id)
            except Exception:
                pass
