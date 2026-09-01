from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class JobSnapshot(BaseModel):
    """
    Typed snapshot of an ExecutorJob's current state, metrics, and metadata.
    Replaces the legacy dictionary-based snapshot for better type safety and DX.

    Residual compatibility:
    - `snapshot["field"]` and `snapshot.get("field")` still work
    - a full mapping interface is no longer exposed
    """
    model_config = ConfigDict(from_attributes=True)

    job_id: str
    project_id: Optional[str] = Field(default=None)
    origin_id: str
    task_type: str
    status: str
    cancel_requested: bool = Field(default=False)
    executor_name: str
    error: str = Field(default="")
    progress: float = Field(default=0.0)
    progress_structural: float = Field(default=0.0)
    progress_operational: float = Field(default=0.0)
    progress_running_chunks_avg: float = Field(default=0.0)
    priority: int = Field(default=0)
    queue_policy: str = Field(default="fifo")
    
    chunks_total: int = Field(default=0)
    chunks_emitted: int = Field(default=0)
    chunks_dispatched: int = Field(default=0)
    chunks_done: int = Field(default=0)
    chunks_failed: int = Field(default=0)
    chunks_stage_failed: int = Field(default=0)
    chunks_running: int = Field(default=0)
    chunks_pending: int = Field(default=0)
    chunks_staging: int = Field(default=0)
    chunks_ready_not_dispatched: int = Field(default=0)
    backlog_chunks: int = Field(default=0)
    backlog_dispatch_chunks: int = Field(default=0)
    backlog_stage_chunks: int = Field(default=0)
    
    feed_exhausted: bool = Field(default=True)
    feed_cursor_position: int = Field(default=0)
    feed_items_acked: int = Field(default=0)
    
    output_sink: Optional[Any] = Field(default=None)
    loop_latency_ms: float = Field(default=0.0)
    throughput_eps: float = Field(default=0.0)
    job_queue_wait_s: Optional[float] = Field(default=None)
    
    chunks_started: int = Field(default=0)
    chunk_queue_wait_avg_s: float = Field(default=0.0)
    chunk_queue_wait_max_s: float = Field(default=0.0)
    running_cpu: int = Field(default=0)
    max_job_cpu: Optional[int] = Field(default=None)
    sink_lag_chunks: int = Field(default=0)
    sink_lag_bytes: int = Field(default=0)
    sink_oldest_lag_s: Optional[float] = Field(default=None)
    sink_buffered_items: int = Field(default=0)
    sink_buffered_bytes: int = Field(default=0)
    sink_pending_chunks_quota: int = Field(default=0)
    sink_pending_bytes_quota: int = Field(default=0)
    sink_pending_chunks_pressure: float = Field(default=0.0)
    sink_pending_bytes_pressure: float = Field(default=0.0)
    sink_writer_flush_count: int = Field(default=0)
    sink_writer_retry_count: int = Field(default=0)
    sink_writer_flush_failures: int = Field(default=0)
    sink_writer_last_flush_duration_ms: float = Field(default=0.0)
    sink_writer_total_bytes_written: int = Field(default=0)
    sink_writer_total_items_written: int = Field(default=0)
    sink_writer_oversized_items: int = Field(default=0)
    job_age_s: float = Field(default=0.0)
    active_work_age_s: Optional[float] = Field(default=None)
    
    scheduler_block_reason: str = Field(default="")
    scheduler_block_category: str = Field(default="")
    scheduler_block_details: dict[str, Any] = Field(default_factory=dict)
    last_dispatch_attempt_at: Optional[datetime] = Field(default=None)
    last_scheduler_reason_at: Optional[datetime] = Field(default=None)
    last_scheduler_reason: str = Field(default="")
    first_chunk_emitted_at: Optional[datetime] = Field(default=None)
    first_chunk_dispatched_at: Optional[datetime] = Field(default=None)
    last_progress_at: Optional[datetime] = Field(default=None)
    
    created_at: datetime
    updated_at: Optional[datetime] = Field(default=None)
    started_at: Optional[datetime] = Field(default=None)
    finished_at: Optional[datetime] = Field(default=None)

    @property
    def is_terminal(self) -> bool:
        """Returns True if the job is in a terminal state."""
        return self.status in {"completed", "failed", "canceled"}

    @property
    def is_running(self) -> bool:
        """Returns True if the job is actively running or staging."""
        return self.status in {"running", "staging"}

    def __getitem__(self, item: str) -> Any:
        """Minimal compatibility with legacy dict-style callers."""
        try:
            return getattr(self, item)
        except AttributeError:
            raise KeyError(item)

    def __contains__(self, item: str) -> bool:
        """Minimal compatibility with legacy `"field" in snapshot` checks."""
        return hasattr(self, item)

    def get(self, item: str, default: Any = None) -> Any:
        """Minimal compatibility with `.get(...)` for legacy callers."""
        return getattr(self, item, default)

    def to_mapping(self) -> dict[str, Any]:
        """Serialise the snapshot explicitly when a dict really is needed."""
        return self.model_dump(mode="python")
