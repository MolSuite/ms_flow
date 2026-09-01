from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from ms_flow.core.database.sqlite_utils import (
    normalize_fields,
    normalize_identifier,
    normalize_order,
)


@dataclass(frozen=True)
class ProjectReadQuery:
    table: str = ""
    fields: tuple[str, ...] = ()
    filters: dict[str, Any] = field(default_factory=dict)
    order: tuple[str, ...] = ()
    limit: int | None = None
    offset: int = 0
    query: str = ""
    params: tuple[Any, ...] = ()
    batch_size: int | None = None

    def __post_init__(self):
        raw_query = str(self.query or "").strip()
        raw_table = str(self.table or "").strip()
        if not raw_query and not raw_table:
            raise ValueError("ProjectReadQuery requires either table or query.")
        if raw_query and raw_table:
            raise ValueError("ProjectReadQuery accepts table or query, not both.")
        object.__setattr__(self, "table", normalize_identifier(raw_table, label="table") if raw_table else "")
        object.__setattr__(self, "query", raw_query)
        object.__setattr__(self, "fields", normalize_fields(self.fields))
        object.__setattr__(self, "filters", dict(self.filters or {}))
        object.__setattr__(self, "order", normalize_order(self.order))
        object.__setattr__(self, "limit", None if self.limit is None else max(0, int(self.limit)))
        object.__setattr__(self, "offset", max(0, int(self.offset or 0)))
        object.__setattr__(self, "params", tuple(self.params or ()))
        object.__setattr__(self, "batch_size", None if self.batch_size is None else max(1, int(self.batch_size)))


@dataclass(frozen=True)
class ProjectCommitKey:
    sink_key: str
    commit_key: str


@dataclass(frozen=True)
class ProjectCommitReceipt:
    sink_key: str
    commit_key: str
    target_name: str
    row_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProjectBulkInsertRequest:
    table_name: str
    rows: tuple[Mapping[str, Any], ...] = ()
    columns: tuple[str, ...] = ()
    model_path: str = ""
    write_mode: str = "bulk"
    conflict_keys: tuple[str, ...] = ()
    validate_model: bool = False
    commit_key: ProjectCommitKey | None = None


@dataclass(frozen=True)
class ProjectBulkInsertResult:
    target_name: str
    rows_written: int = 0
    deduplicated: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProjectGraphInsertRequest:
    graph_meta: Mapping[str, Any]
    payload: Mapping[str, Sequence[Mapping[str, Any]]]
    commit_key: ProjectCommitKey | None = None


@dataclass(frozen=True)
class ProjectGraphInsertResult:
    rows_written: int = 0
    nodes_written: dict[str, int] = field(default_factory=dict)
    deduplicated: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProjectStoreCapabilities:
    store_name: str
    supports_concurrent_writers: bool
    supports_returning_inserted_ids: bool
    requires_local_path: bool
    is_sqlite_family: bool = False
    notes: tuple[str, ...] = ()
