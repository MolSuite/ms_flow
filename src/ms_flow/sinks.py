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
    return tuple(column.name for column in table.columns if not column.primary_key and column.computed is None)


def _with_conflict_keys(columns: Iterable[str], conflict_keys: Iterable[str]) -> tuple[str, ...]:
    """An upsert's default columns: the model's writable ones plus its conflict keys.

    The writable columns leave primary keys out, so a composite-PK upsert would emit rows
    without the very keys its ON CONFLICT names.
    """
    columns = tuple(columns)
    return tuple(key for key in conflict_keys if key not in columns) + columns


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
    if write_mode == "upsert" and model is not None and not value.get("columns"):
        columns = _with_conflict_keys(columns, conflict_keys)
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
        if str(write_mode or "").strip().lower() == "upsert":
            resolved_columns = _with_conflict_keys(resolved_columns, (str(key) for key in conflict_keys))
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
    """Simple helper to persist results to a file. A relative `path` is under the project.

    What a task returns is written as it comes when it is text or bytes, and as a JSON line
    otherwise; with `fmt="json"` the file is one JSON list of what the tasks returned.

    `append=True` (text/binary fmt only) is what a long job needs: each group of results is
    added to the file as it arrives. `per_batch=True` is the other bounded option: one numbered
    file (`name_00001.ext`) for each group. With neither, everything is kept in memory and the
    file is written once, when the job completes.
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


class CatalogWriter:
    """The writer `catalog_sink` makes: each group of receipts (`catalog.file_row(path, part=...)`)
    is committed into the containers of its ``(generation, role)``, beside its parts, by one
    `catalog.ContainerWriter` each, kept until the job ends (it holds their lock). A part that
    could not be committed stays out: its origin is still to derive, and the job fails at the end.
    ``tables`` is what the jobs reading the catalog wait for."""

    tables = ("ms_shard_files",)

    def __init__(self, project_db) -> None:
        self.project_db, self._writers, self.failed = project_db, {}, []

    def write(self, results: list) -> None:
        from ms_flow.core.data.catalog import ContainerWriter

        groups: dict[tuple[int, str], list[dict]] = {}
        for result in results:
            for receipt in (result if isinstance(result, list) else [result]):
                if not receipt.get("part"):
                    raise ValueError(f"{receipt.get('path')}: a receipt names its part (catalog.file_row(part=))")
                groups.setdefault((int(receipt["generation_id"]), str(receipt["role"])), []).append(receipt)
        for (generation_id, role), receipts in groups.items():
            writer = self._writers.get((generation_id, role))
            if writer is None:  # ponytail: the containers of a role live beside its first part
                writer = self._writers[(generation_id, role)] = ContainerWriter(
                    self.project_db, Path(receipts[0]["path"]).parent, generation_id=generation_id, role=role)
            self.failed += [receipt["error"] for receipt in writer.commit_parts(receipts) if receipt.get("error")]

    def finish(self) -> None:
        self.close()
        if self.failed:
            raise RuntimeError(f"{len(self.failed)} part(s) not committed, first: {self.failed[0]}")

    def ended(self, _snapshot) -> None:
        self.close()

    def close(self) -> None:
        for writer in self._writers.values():
            writer.close()
        self._writers.clear()


def catalog_sink():
    """The output of a step whose tasks each write a part and return its receipt,
    ``catalog.file_row(path, role=..., generation_id=..., part=...)``: the parts are appended to
    the containers of their role as the results arrive (`CatalogWriter`), no merge pass. ``part``
    names the task's unit of work in this submission: a receipt delivered twice commits once, a
    redo is a new submission. `catalog.sweep` at the job's start deletes what a crash left, and
    `catalog.underived` is the resume."""
    def make(_params, project) -> CatalogWriter:
        return CatalogWriter(project)

    return make


__all__ = ["CatalogWriter", "catalog_sink", "file_sink", "graph_sink", "table_sink"]
