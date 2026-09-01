from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlmodel import Field, SQLModel


class Project(SQLModel, table=True):
    """Strict representation of the row in the MasterDB."""

    __tablename__ = "projects"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    name: str
    path: str
    app_id: str = ""
    description: str = ""
    scope: str = "full"
    tags: str = ""
    favorite: bool = False
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)


class ProjectJobIndex(SQLModel, table=True):
    """Lightweight per-project job summary for fast global queries."""

    __tablename__ = "project_job_index"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    project_id: UUID = Field(index=True)
    job_id: str = Field(index=True, unique=True)
    origin_id: str = ""
    task_type: str = ""
    status: str = Field(default="pending", index=True)
    progress: float = 0.0
    scheduler_reason: str = ""
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now, index=True)
