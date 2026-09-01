from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Iterator, Optional

from ms_flow.core.executor.dispatch_model import DispatchPolicy
from ms_flow.core.executor.result_handlers import ResultHandler
from ms_flow.core.executor.runner_refs import RunnerRef


@dataclass
class RunningChunk:
    job_id: str
    chunk_id: str
    executor_name: str
    handle_id: str
    cpu_required: int
    gpu_required: int = 0


@dataclass
class JobLifecycle:
    setup_ref: str = ""
    stage_ref: str = ""
    finalize_ref: str = ""
    stage_fail_policy: str = "fail_fast"
    max_stage_failures: int = 0
    setup_done: bool = False
    setup_started: bool = False
    setup_failed: bool = False
    finalize_started: bool = False
    finalize_done: bool = False
    setup_data: dict[str, Any] = field(default_factory=dict)
    stage_failures: int = 0
    consecutive_chunk_failures: int = 0


@dataclass
class JobFeed:
    """
    Lazy chunk feed for a single job.

    Maintains an inflight task window over the item/batch source:
    - Only `max_inflight_tasks` payloads are materialized in DB/executor at any time.
    - As chunks complete, more are pulled from the generator and inserted.
    - When the generator is exhausted, `exhausted` is set to True.
    """

    job_id: str
    executor_name: str
    item_source: Optional[Iterator[dict]]
    dispatch_policy: DispatchPolicy
    default_cpu_required: int
    default_gpu_required: int = 0
    source_ready: bool = True
    exhausted: bool = False
    live_count: int = 0
    total_emitted: int = 0
    total_chunks: Optional[int] = None
    attached_resources: list[Any] = field(default_factory=list, compare=False, repr=False)
    lock: threading.Lock = field(default_factory=threading.Lock, init=False, compare=False, repr=False)


@dataclass
class SchedulerNoteState:
    current_reason: str = ""
    last_dispatch_attempt_at: Optional[datetime] = None
    last_scheduler_reason_at: Optional[datetime] = None
    last_scheduler_reason: str = ""
    last_scheduler_payload: dict[str, Any] = field(default_factory=dict)


@dataclass
class RuntimeCleanupState:
    feed: JobFeed | None
    handler: ResultHandler | None


class ExecutorRuntimeState:
    """In-memory owner for ephemeral executor runtime state."""

    def __init__(self):
        self.job_runners: dict[str, RunnerRef | str] = {}
        self.job_feeds: dict[str, JobFeed] = {}
        self.job_lifecycles: dict[str, JobLifecycle] = {}
        self.job_result_handlers: dict[str, ResultHandler] = {}
        self.job_store_results: dict[str, bool] = {}
        self.cancel_requested_jobs: set[str] = set()
        self.job_scheduler_notes: dict[str, SchedulerNoteState] = {}
        self.running_chunks: dict[str, RunningChunk] = {}

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
        self.job_runners[job_id] = runner_ref
        self.job_feeds[job_id] = feed
        self.job_lifecycles[job_id] = lifecycle
        self.job_store_results[job_id] = bool(store_results)
        if handler is not None:
            self.job_result_handlers[job_id] = handler
        else:
            self.job_result_handlers.pop(job_id, None)
        if cancel_requested:
            self.cancel_requested_jobs.add(job_id)
        else:
            self.cancel_requested_jobs.discard(job_id)

    def snapshot_job_feeds(self) -> list[JobFeed]:
        return list(self.job_feeds.values())

    def get_job_feed(self, job_id: str) -> JobFeed | None:
        return self.job_feeds.get(job_id)

    def get_runner_ref(self, job_id: str) -> RunnerRef | str | None:
        return self.job_runners.get(job_id)

    def get_job_lifecycle(self, job_id: str) -> JobLifecycle | None:
        return self.job_lifecycles.get(job_id)

    def get_result_handler(self, job_id: str) -> ResultHandler | None:
        return self.job_result_handlers.get(job_id)

    def snapshot_result_handlers(self) -> dict[str, ResultHandler]:
        return dict(self.job_result_handlers)

    def stores_job_results(self, job_id: str, *, default: bool = True) -> bool:
        return bool(self.job_store_results.get(job_id, default))

    def has_cancel_request(self, job_id: str) -> bool:
        return job_id in self.cancel_requested_jobs

    def get_scheduler_note(self, job_id: str) -> SchedulerNoteState:
        state = self.job_scheduler_notes.get(job_id)
        if state is None:
            state = SchedulerNoteState()
            self.job_scheduler_notes[job_id] = state
        return state

    def snapshot_scheduler_note(self, job_id: str) -> SchedulerNoteState:
        state = self.job_scheduler_notes.get(job_id)
        if state is None:
            return SchedulerNoteState()
        return SchedulerNoteState(
            current_reason=str(state.current_reason or ""),
            last_dispatch_attempt_at=state.last_dispatch_attempt_at,
            last_scheduler_reason_at=state.last_scheduler_reason_at,
            last_scheduler_reason=str(state.last_scheduler_reason or ""),
            last_scheduler_payload=dict(state.last_scheduler_payload or {}),
        )

    def register_running_chunk(self, item: RunningChunk) -> None:
        if item.chunk_id in self.running_chunks:
            raise RuntimeError(f"Chunk '{item.chunk_id}' is already registered as running.")
        self.running_chunks[item.chunk_id] = item

    def pop_running_chunk(self, chunk_id: str) -> RunningChunk | None:
        return self.running_chunks.pop(chunk_id, None)

    def snapshot_running_chunks(self) -> list[RunningChunk]:
        return list(self.running_chunks.values())

    def running_cpu(self) -> int:
        """Sampled CPU occupancy = sum of cpu over currently-running chunks.

        Authoritative and self-correcting: a chunk contributes exactly while it
        is registered running, so there is no manual counter to drift or
        double-count. The set is small (bounded by total_cpu)."""
        return sum(max(0, int(item.cpu_required or 0)) for item in self.running_chunks.values())

    def running_gpu(self) -> int:
        return sum(max(0, int(item.gpu_required or 0)) for item in self.running_chunks.values())

    def snapshot_running_chunks_for_job(self, job_id: str) -> list[RunningChunk]:
        return [item for item in self.running_chunks.values() if item.job_id == job_id]

    def clear_running_chunks(self) -> list[RunningChunk]:
        items = list(self.running_chunks.values())
        self.running_chunks.clear()
        return items

    def pop_job_runtime(self, job_id: str) -> RuntimeCleanupState:
        feed = self.job_feeds.pop(job_id, None)
        handler = self.job_result_handlers.pop(job_id, None)
        self.job_runners.pop(job_id, None)
        self.job_lifecycles.pop(job_id, None)
        self.job_store_results.pop(job_id, None)
        self.cancel_requested_jobs.discard(job_id)
        self.job_scheduler_notes.pop(job_id, None)
        return RuntimeCleanupState(feed=feed, handler=handler)

    def clear(self) -> None:
        self.job_runners.clear()
        self.job_feeds.clear()
        self.job_lifecycles.clear()
        self.job_result_handlers.clear()
        self.job_store_results.clear()
        self.cancel_requested_jobs.clear()
        self.job_scheduler_notes.clear()
        self.running_chunks.clear()
