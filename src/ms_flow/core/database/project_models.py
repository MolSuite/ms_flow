from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, UniqueConstraint
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


class ProjectMeta(SQLModel, table=True):
    """What ms_flow keeps of the project itself: `uid`, the name of its folder on other machines."""

    __tablename__ = "ms_project_meta"

    key: str = Field(primary_key=True)
    value: str = ""


class RemoteLocation(SQLModel, table=True):
    """A file of the project that is on another machine too, at the same path under the project's
    folder there (see `ms_flow.locations`): as it was when it was last verified there."""

    __tablename__ = "ms_locations"
    __table_args__ = (UniqueConstraint("host", "path"),)

    id: Optional[int] = Field(default=None, primary_key=True)
    host: str = Field(index=True)  # "<address>:<work directory>", this machine's address being ""
    path: str = ""  # in the project, with "/"
    size: int = 0
    sha256: str = ""
    verified_at: datetime = Field(default_factory=datetime.now, sa_type=DateTime)


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


class ShardGeneration(SQLModel, table=True):
    """One rewrite of the sharded dataset (see `ms_flow.core.data.catalog`). Only one is active."""

    __tablename__ = "ms_shard_generations"
    __table_args__ = {"sqlite_autoincrement": True}  # an id is never given twice: states name it

    id: Optional[int] = Field(default=None, primary_key=True)
    parent_id: Optional[int] = Field(default=None, foreign_key="ms_shard_generations.id", index=True)
    step: str = ""  # what produced it: "import", or the step that rewrote the dataset
    folder: str = ""
    job_id: str = ""
    is_active: bool = Field(default=False, index=True)
    kept: bool = False  # activation never deletes it; deleting it is an explicit action
    created_at: datetime = Field(default_factory=datetime.now, sa_type=DateTime)


class CatalogFile(SQLModel, table=True):
    """One file of a sharded dataset (or of what a step made of it): the catalog keeps a row per
    file, never per shard. Its shards are listed in the file's own index
    (`ms_flow.core.data.shard.file_groups`); the counts here are what a view or a progress bar
    needs without opening it. App tables reference ``id``."""

    __tablename__ = "ms_shard_files"
    __table_args__ = {"sqlite_autoincrement": True}  # a shard id holds its file id: never reused

    id: Optional[int] = Field(default=None, primary_key=True)
    generation_id: Optional[int] = Field(default=None, foreign_key="ms_shard_generations.id", index=True)
    # What the file is: "source" (the dataset itself), or what a step made of it (a role the app names).
    role: str = Field(default="source", index=True)
    path: str = ""
    # "writing" while its writer has it open (its counts grow; nothing reads it), then "ready"
    # (read, appended to), or "sealed" (read, never appended again).
    status: str = Field(default="ready", index=True)
    n_shards: int = 0
    n_records: int = 0
    n_active: int = 0  # n_records minus the active exclusions
    n_shards_excluded: int = 0
    # What the index had committed, retired rows included: these only grow. An index below any
    # of them is stale or foreign, and is refused before an appender cuts its tail.
    n_rows: int = 0
    n_retired: int = 0
    end: int = 0
    # The scopes (the batches that wrote them) its shards belong to, comma-separated: what a view names.
    scopes: str = ""
    # Where the file is: "" here, else the host that wrote it and keeps it (a step that ran there
    # left its output there; only its name and counts came back).
    location: str = Field(default="", index=True)
    error: str = ""
    created_at: datetime = Field(default_factory=datetime.now, sa_type=DateTime)
