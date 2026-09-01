from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlmodel import Field, SQLModel


class ProjectArtifact(SQLModel, table=True):
    """Persisted project artifact produced or imported by an app."""

    __tablename__ = "project_artifacts"

    id: Optional[int] = Field(default=None, primary_key=True)
    app_id: str = Field(default="", index=True)
    entity_kind: str = Field(default="", index=True)
    entity_id: Optional[int] = Field(default=None, index=True)
    artifact_kind: str = Field(default="", index=True)
    role: str = Field(default="", index=True)
    path: str = ""
    ref: str = ""
    format: str = Field(default="", index=True)
    status: str = Field(default="available", index=True)
    producer_job_id: str = Field(default="", index=True)
    metadata_json: str = "{}"
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now, index=True)


class ProjectArtifactCapability(SQLModel, table=True):
    """Capability attached to a project artifact."""

    __tablename__ = "project_artifact_capabilities"

    id: Optional[int] = Field(default=None, primary_key=True)
    artifact_id: int = Field(index=True)
    capability: str = Field(index=True)
    value_json: str = "{}"
    method: str = Field(default="", index=True)
    params_json: str = "{}"
    metadata_json: str = "{}"
    created_at: datetime = Field(default_factory=datetime.now, index=True)


class ProjectOperationRun(SQLModel, table=True):
    """Thin app-level operation run index linked to one or more jobs."""

    __tablename__ = "project_operation_runs"

    id: Optional[int] = Field(default=None, primary_key=True)
    app_id: str = Field(default="", index=True)
    operation_name: str = Field(default="", index=True)
    job_id: str = Field(default="", index=True)
    status: str = Field(default="pending", index=True)
    input_ref_json: str = "{}"
    output_ref_json: str = "{}"
    params_json: str = "{}"
    created_at: datetime = Field(default_factory=datetime.now)
    finished_at: Optional[datetime] = None
