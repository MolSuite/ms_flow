from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4

from sqlmodel import Field, SQLModel


class ExecutorJob(SQLModel, table=True):
    """Global state of each job inside the executor runtime."""

    __tablename__ = "executor_jobs"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    job_id: str = Field(index=True, unique=True)
    project_id: Optional[UUID] = Field(default=None, index=True)
    origin_id: str = ""
    task_type: str = ""
    executor_name: str = Field(default="thread", index=True)
    queue_policy: str = Field(default="fifo", index=True)
    priority: int = Field(default=0, index=True)
    status: str = Field(default="pending", index=True)
    progress: float = 0.0
    payload_json: str = "{}"
    depends_on: str = "[]"
    result_ref: str = ""
    error: str = ""
    scheduler_reason: str = ""
    total_chunks: Optional[int] = None
    total_emitted: int = Field(default=0, index=True)
    loop_latency_ms: float = 0.0
    throughput_eps: float = 0.0
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now, index=True)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None


class ExecutorJobFeedState(SQLModel, table=True):
    """Compact progress of the lazy feed while the job is active."""

    __tablename__ = "executor_job_feeds"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    job_id: str = Field(index=True, unique=True)
    cursor_position: int = Field(default=0, index=True)
    items_acked: int = 0
    exhausted: bool = False
    last_error: str = ""
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now, index=True)


class ExecutorJobChunk(SQLModel, table=True):
    """Per-chunk / executable-unit tracking of a job."""

    __tablename__ = "executor_job_chunks"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    job_id: str = Field(index=True)
    chunk_id: str = Field(index=True, unique=True)
    executor_name: str = Field(default="thread", index=True)
    payload_json: str = "{}"
    output_json: str = "{}"
    output_state: str = Field(default="none", index=True)
    output_payload_json: str = "{}"
    output_sink_info_json: str = "{}"
    cpu_required: int = Field(default=1, index=True)
    gpu_required: int = Field(default=0, index=True)
    status: str = Field(default="pending", index=True)
    progress: float = 0.0
    checkpoint_ref: str = ""
    error: str = ""
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now, index=True)
    started_at: Optional[datetime] = None
    output_produced_at: Optional[datetime] = None
    output_persisted_at: Optional[datetime] = None
    output_confirmed_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None


class ExecutorJobEvent(SQLModel, table=True):
    """Append-only events for executor streaming/log/progress."""

    __tablename__ = "executor_job_events"

    id: Optional[int] = Field(default=None, primary_key=True)
    job_id: str = Field(index=True)
    chunk_id: str = Field(default="", index=True)
    level: str = Field(default="INFO", index=True)
    event_type: str = Field(default="log", index=True)
    message: str = ""
    payload_json: str = "{}"
    created_at: datetime = Field(default_factory=datetime.now, index=True)


class ExecutorHeartbeat(SQLModel, table=True):
    """Availability state/telemetry per executor."""

    __tablename__ = "executor_heartbeats"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    executor_name: str = Field(index=True, unique=True)
    status: str = Field(default="online", index=True)
    total_cpu: int = 0
    used_cpu: int = 0
    running_jobs: int = 0
    running_chunks: int = 0
    loop_latency_ms: float = 0.0
    updated_at: datetime = Field(default_factory=datetime.now, index=True)
