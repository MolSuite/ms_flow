from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime
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
    created_at: datetime = Field(default_factory=datetime.now, sa_type=DateTime)
    finished_at: Optional[datetime] = Field(default=None, sa_type=DateTime)


class PayloadVolume(SQLModel, table=True):
    """One append-only volume file, `volumes/<id:09d>.vol` (see `ms_flow.payloads`)."""

    __tablename__ = "ms_volumes"
    # Ids are never reused: a file name must never point to two volumes over time.
    __table_args__ = {"sqlite_autoincrement": True}

    id: Optional[int] = Field(default=None, primary_key=True)
    series: str = Field(default="", index=True)
    state: str = Field(default="active", index=True)  # active | sealed | trash
    size: int = 0


class PayloadRecord(SQLModel, table=True):
    """Where the bytes of one payload live. App tables reference `id`, never the location."""

    __tablename__ = "ms_payloads"
    __table_args__ = {"sqlite_autoincrement": True}

    id: Optional[int] = Field(default=None, primary_key=True)
    volume_id: int = Field(foreign_key="ms_volumes.id", index=True)
    offset: int = 0  # of the needle header
    length: int = 0  # stored bytes, without the needle header
    raw_length: int = 0
    codec: int = 0
    crc32: int = 0
    kind: str = ""
    part_offset: Optional[int] = None  # a part of a group: where its raw bytes start in the needle's
    part_length: Optional[int] = None  # NULL: the whole needle


class PayloadDictionary(SQLModel, table=True):
    """A zstd dictionary of one series, `volumes/dicts/<id>.zdict`; `id` is the dictionary id in its frames."""

    __tablename__ = "ms_payload_dicts"
    __table_args__ = {"sqlite_autoincrement": True}

    id: Optional[int] = Field(default=None, primary_key=True)
    series: str = Field(default="", index=True)
    samples: int = 0
