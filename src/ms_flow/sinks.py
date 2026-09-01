from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

from sqlmodel import SQLModel

from ms_flow.core.data import DbOutputSpec, FileOutputSpec


def _model_table_name(model: type[SQLModel]) -> str:
    table = getattr(model, "__table__", None)
    if table is None or not getattr(table, "name", ""):
        raise ValueError(f"Model {model!r} does not define a SQL table.")
    return str(table.name)


def _model_output_columns(model: type[SQLModel]) -> tuple[str, ...]:
    table = getattr(model, "__table__", None)
    if table is None:
        raise ValueError(f"Model {model!r} does not define a SQL table.")
    return tuple(column.name for column in table.columns if not column.primary_key)


def _model_path(model: type[SQLModel]) -> str:
    return f"{model.__module__}:{model.__qualname__}"


def _coerce_graph_node(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("graph_sink nodes must be mapping values.")
    model = value.get("model")
    table = str(value.get("table", "") or "")
    columns = value.get("columns") or ()
    model_path = str(value.get("model_path", "") or "")
    if model is not None:
        table = table or _model_table_name(model)
        if not columns:
            columns = _model_output_columns(model)
        model_path = model_path or _model_path(model)
    write_mode = str(value.get("write_mode", "") or "insert").strip().lower()
    if write_mode not in {"insert", "bulk", "upsert"}:
        raise ValueError(f"Invalid graph node write_mode '{write_mode}'. Use insert/upsert.")
    conflict_keys = tuple(str(key).strip() for key in (value.get("conflict_keys") or ()) if str(key).strip())
    if write_mode == "upsert" and not conflict_keys:
        conflict_keys = ("id",)
    return {
        "name": str(value.get("name", "")),
        "table": table,
        "columns": [str(column) for column in columns],
        "model_path": model_path,
        "pk_field": str(value.get("pk_field", "id")),
        "ref_field": str(value.get("ref_field", "$ref")),
        "validate_model": bool(value.get("validate_model", False)),
        "write_mode": write_mode,
        "conflict_keys": list(conflict_keys),
    }


def _coerce_graph_relation(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("graph_sink relations must be mapping values.")
    return {
        "source_node": str(value.get("source_node", "")),
        "source_ref_field": str(value.get("source_ref_field", "")),
        "target_node": str(value.get("target_node", "")),
        "fk_field": str(value.get("fk_field", "")),
        "target_ref_field": str(value.get("target_ref_field", "")),
        # deferred=True: the FK points "backwards" (the parent references a child inserted
        # later). It stays out of the topological order — it is resolved with an UPDATE at
        # the end, once the ids on both sides exist. Without this it would be a cycle.
        "deferred": bool(value.get("deferred", False)),
    }


def table_sink(
    table: str = "",
    *,
    model: type[SQLModel] | None = None,
    columns: Iterable[str] = (),
    write_mode: str = "bulk",
    conflict_keys: Iterable[str] = (),
    validate_model: bool = False,
    db_role: str = "project",
    db_path: str | Path = "",
    schema: str = "",
    meta: Optional[dict[str, Any]] = None,
) -> DbOutputSpec:
    """Simple helper to persist results into a SQLite table.

    `write_mode="upsert"` updates existing rows via `INSERT ... ON CONFLICT
    (conflict_keys) DO UPDATE`. Each emitted row only updates the columns it carries
    (SET is built per payload), so it needs `model` and `conflict_keys` to be the PK
    or a UNIQUE constraint of the table.
    """
    resolved_table = _model_table_name(model) if model is not None else str(table)
    # For upsert we keep explicit columns when given so the SET clause is scoped;
    # otherwise default to the model's writable columns.
    if columns:
        resolved_columns: tuple[str, ...] = tuple(str(col) for col in columns)
    elif model is not None:
        resolved_columns = _model_output_columns(model)
    else:
        resolved_columns = ()
    return DbOutputSpec(
        table=resolved_table,
        columns=resolved_columns,
        model_path=_model_path(model) if model is not None else "",
        write_mode=str(write_mode or "bulk"),
        conflict_keys=tuple(str(key) for key in conflict_keys),
        validate_model=bool(validate_model),
        db_role=str(db_role or "project"),
        db_path=str(db_path or ""),
        schema=str(schema or ""),
        meta=dict(meta or {}),
    )


def graph_sink(
    *,
    nodes: Iterable[Mapping[str, Any]],
    relations: Iterable[Mapping[str, Any]] = (),
    db_role: str = "project",
    db_path: str | Path = "",
    schema: str = "",
    meta: Optional[dict[str, Any]] = None,
) -> DbOutputSpec:
    """Helper to persist a multi-table relational graph into SQLite."""
    resolved_meta = dict(meta or {})
    resolved_meta["graph"] = {
        "nodes": [_coerce_graph_node(node) for node in nodes],
        "relations": [_coerce_graph_relation(relation) for relation in relations],
    }
    return DbOutputSpec(
        table="",
        columns=(),
        write_mode="bulk",
        db_role=str(db_role or "project"),
        db_path=str(db_path or ""),
        mode="graph",
        schema=str(schema or ""),
        meta=resolved_meta,
    )


def file_sink(
    path: str | Path,
    *,
    root: str = "project",
    fmt: str = "json",
    encoding: str = "utf-8",
    ensure_parent: bool = True,
    append: bool = False,
    per_batch: bool = False,
    schema: str = "",
    meta: Optional[dict[str, Any]] = None,
) -> FileOutputSpec:
    """Simple helper to persist results to a file.

    `append=True` (text/binary fmt only) is what a long job needs: each flush writes only its
    own batch. Without it the sink keeps everything produced in memory and rewrites the whole
    file on every flush — O(N) RAM and O(N^2) IO over the job's total.

    `per_batch=True` is the other bounded option: one numbered file per batch, keeping only the
    path manifest in memory (which grows with the number of flushes, not of items).
    """
    return FileOutputSpec(
        path=str(path),
        root=str(root or "project"),
        fmt=str(fmt or "json"),
        encoding=str(encoding or "utf-8"),
        ensure_parent=bool(ensure_parent),
        append=bool(append),
        per_batch=bool(per_batch),
        schema=str(schema or ""),
        meta=dict(meta or {}),
    )


__all__ = ["file_sink", "graph_sink", "table_sink"]
