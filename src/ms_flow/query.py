from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, Iterable, Iterator, Mapping, Optional, Sequence

from ms_flow.core.data import DbInputSpec
from ms_flow.core.database import ProjectReadQuery, resolve_project_store
from ms_flow.core.database.sqlite_utils import (
    compile_select,
    normalize_fields,
    normalize_identifier,
    normalize_order,
)


@dataclass(frozen=True)
class QuerySpec:
    """Declarative query specification for project paths and DB transport."""

    table: str = ""
    fields: tuple[str, ...] = ()
    filters: dict[str, Any] = field(default_factory=dict)
    order: tuple[str, ...] = ()
    limit: Optional[int] = None
    offset: int = 0
    query: str = ""
    params: tuple[Any, ...] = ()
    db_role: str = "project"
    db_path: str = ""
    schema: str = ""
    meta: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        raw_query = str(self.query or "").strip()
        raw_table = str(self.table or "").strip()
        if not raw_query and not raw_table:
            raise ValueError("QuerySpec requires either table or query.")
        if raw_query and raw_table:
            raise ValueError("QuerySpec accepts table or query, not both.")
        if raw_table:
            object.__setattr__(self, "table", normalize_identifier(raw_table, label="table"))
        else:
            object.__setattr__(self, "table", "")
        object.__setattr__(self, "query", raw_query)
        object.__setattr__(self, "fields", normalize_fields(self.fields))
        object.__setattr__(self, "order", normalize_order(self.order))
        object.__setattr__(self, "filters", dict(self.filters or {}))
        object.__setattr__(self, "params", tuple(self.params or ()))
        object.__setattr__(self, "db_role", str(self.db_role or "project").strip().lower() or "project")
        object.__setattr__(self, "db_path", str(self.db_path or ""))
        object.__setattr__(self, "schema", str(self.schema or ""))
        object.__setattr__(self, "meta", dict(self.meta or {}))
        object.__setattr__(self, "limit", None if self.limit is None else max(0, int(self.limit)))
        object.__setattr__(self, "offset", max(0, int(self.offset or 0)))

    def compile(self) -> tuple[str, tuple[Any, ...]]:
        return compile_select(
            table=self.table,
            fields=self.fields,
            filters=self.filters,
            order=self.order,
            limit=self.limit,
            offset=self.offset,
            query=self.query,
            params=self.params,
        )

    def to_input_spec(self) -> DbInputSpec:
        sql, params = self.compile()
        return DbInputSpec(
            query=sql,
            params=params,
            db_role=self.db_role,
            db_path=self.db_path,
            schema=self.schema,
            meta=self.meta,
        )


def db_input_for(
    table: str | QuerySpec = "",
    *,
    fields: Sequence[str] | None = None,
    filters: Mapping[str, Any] | None = None,
    order: Sequence[str] | None = None,
    limit: int | None = None,
    offset: int = 0,
    query: str = "",
    params: Iterable[Any] = (),
    db_role: str = "project",
    db_path: str | Path = "",
    schema: str = "",
    meta: Optional[dict[str, Any]] = None,
) -> DbInputSpec:
    if isinstance(table, QuerySpec):
        return table.to_input_spec()
    spec = QuerySpec(
        table=str(table or ""),
        fields=tuple(fields or ()),
        filters=dict(filters or {}),
        order=tuple(order or ()),
        limit=limit,
        offset=offset,
        query=query,
        params=tuple(params or ()),
        db_role=db_role,
        db_path=str(db_path or ""),
        schema=schema,
        meta=dict(meta or {}),
    )
    return spec.to_input_spec()


def _as_project_read_query(spec: QuerySpec, *, batch_size: int | None = None) -> ProjectReadQuery:
    return ProjectReadQuery(
        table=spec.table,
        fields=spec.fields,
        filters=spec.filters,
        order=spec.order,
        limit=spec.limit,
        offset=spec.offset,
        query=spec.query,
        params=spec.params,
        batch_size=batch_size,
    )


def db_rows(
    source: Any,
    table: str | QuerySpec = "",
    *,
    fields: Sequence[str] | None = None,
    filters: Mapping[str, Any] | None = None,
    order: Sequence[str] | None = None,
    limit: int | None = None,
    offset: int = 0,
    query: str = "",
    params: Iterable[Any] = (),
) -> list[dict[str, Any]]:
    spec = table if isinstance(table, QuerySpec) else QuerySpec(
        table=str(table or ""),
        fields=tuple(fields or ()),
        filters=dict(filters or {}),
        order=tuple(order or ()),
        limit=limit,
        offset=offset,
        query=query,
        params=tuple(params or ()),
    )
    backend = resolve_project_store(source)
    return backend.read_rows(_as_project_read_query(spec))


def db_count(
    source: Any,
    table: str | QuerySpec = "",
    *,
    filters: Mapping[str, Any] | None = None,
    limit: int | None = None,
    offset: int = 0,
    query: str = "",
    params: Iterable[Any] = (),
) -> int:
    spec = table if isinstance(table, QuerySpec) else QuerySpec(
        table=str(table or ""),
        filters=dict(filters or {}),
        limit=limit,
        offset=offset,
        query=query,
        params=tuple(params or ()),
    )
    backend = resolve_project_store(source)
    return backend.count_rows(_as_project_read_query(spec))


def db_stream(
    source: Any,
    table: str | QuerySpec = "",
    *,
    fields: Sequence[str] | None = None,
    filters: Mapping[str, Any] | None = None,
    order: Sequence[str] | None = None,
    limit: int | None = None,
    offset: int = 0,
    query: str = "",
    params: Iterable[Any] = (),
    batch_size: int = 500,
) -> Iterator[dict[str, Any]]:
    spec = table if isinstance(table, QuerySpec) else QuerySpec(
        table=str(table or ""),
        fields=tuple(fields or ()),
        filters=dict(filters or {}),
        order=tuple(order or ()),
        limit=limit,
        offset=offset,
        query=query,
        params=tuple(params or ()),
    )
    backend = resolve_project_store(source)
    yield from backend.stream_rows(_as_project_read_query(spec, batch_size=batch_size))


def db_pages(
    source: Any,
    spec: QuerySpec,
    *,
    key: str = "id",
    page_size: int = 500,
) -> Iterator[dict[str, Any]]:
    """The same rows as `db_stream`, but in keyset pages.

    Each page is its own short read (`WHERE key > last ORDER BY key LIMIT n`) instead of a single
    cursor held open for the whole feed. Feeding a million-ligand screen takes hours, and a read
    snapshot held open that long prevents SQLite from checkpointing the WAL: `project.db-wal`
    grows without bound while the job itself writes its results.

    Without a usable key (raw SQL, `offset`, or an order other than `key`) no keyset is possible:
    it falls back to `db_stream`, which returns exactly the same rows.
    """
    key_name = str(key or "").strip()
    pageable = (
        bool(spec.table)
        and bool(key_name)
        and not spec.offset
        and (not spec.fields or key_name in spec.fields)
        and (not spec.order or tuple(spec.order) == (key_name,))
    )
    if not pageable:
        yield from db_stream(source, spec, batch_size=page_size)
        return

    backend = resolve_project_store(source)
    size = max(1, int(page_size))
    remaining = None if spec.limit is None else int(spec.limit)
    cursor: Any = None
    while remaining is None or remaining > 0:
        take = size if remaining is None else min(size, remaining)
        filters = dict(spec.filters)
        if cursor is not None:
            filters[f"{key_name}__gt"] = cursor
        page = replace(spec, filters=filters, order=(key_name,), limit=take, offset=0)
        rows = backend.read_rows(_as_project_read_query(page))
        if not rows:
            return
        yield from rows
        if len(rows) < take:
            return
        cursor = rows[-1].get(key_name)
        if cursor is None:
            return  # no cursor, no progress; stopping beats repeating the page forever
        if remaining is not None:
            remaining -= len(rows)


__all__ = ["QuerySpec", "db_input_for", "db_count", "db_pages", "db_rows", "db_stream"]
