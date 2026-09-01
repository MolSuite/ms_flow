from __future__ import annotations

import base64
import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Optional

INPUT_WIRE_KEY = "__molsuite_input_spec__"
OUTPUT_WIRE_KEY = "__molsuite_output_spec__"
RAY_FILE_INPUT_KEY = "__molsuite_ray_file_input__"
RAY_FILE_ARTIFACT_KEY = "__molsuite_ray_file_artifact__"
RAY_OUTPUT_DIR_KEY = "__molsuite_ray_output_dir__"
_DB_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_\\.]*$")


class DataContractError(RuntimeError):
    pass


def _normalize_db_identifier(value: str, *, label: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{label} must not be empty.")
    if not _DB_IDENTIFIER_RE.match(normalized):
        raise ValueError(f"Invalid {label} '{value}'.")
    return normalized


def _normalize_db_order(order: Iterable[str] = ()) -> tuple[str, ...]:
    normalized: list[str] = []
    for raw_item in order or ():
        item = str(raw_item or "").strip()
        if not item:
            continue
        desc = item.startswith("-")
        field = item[1:] if desc else item
        normalized_field = _normalize_db_identifier(field, label="order field")
        normalized.append(f"-{normalized_field}" if desc else normalized_field)
    return tuple(normalized)


@dataclass(frozen=True)
class DataSpecBase:
    kind: str
    schema: str = ""
    meta: dict[str, Any] = field(default_factory=dict)

    def _base_payload(self) -> dict[str, Any]:
        return {"kind": self.kind, "schema": self.schema, "meta": dict(self.meta)}


@dataclass(frozen=True)
class InputSpec(DataSpecBase):
    def to_wire(self) -> dict[str, Any]:
        raise NotImplementedError


@dataclass(frozen=True)
class InlineInputSpec(InputSpec):
    payload: Any = None

    def __init__(self, payload: Any, *, schema: str = "", meta: Optional[dict[str, Any]] = None):
        super().__init__(kind="inline", schema=schema, meta=dict(meta or {}))
        object.__setattr__(self, "payload", payload)

    def to_wire(self) -> dict[str, Any]:
        return {INPUT_WIRE_KEY: {**self._base_payload(), "payload": to_wire_value(self.payload)}}


@dataclass(frozen=True)
class BytesInputSpec(InputSpec):
    payload_b64: str = ""

    def __init__(self, payload: bytes, *, schema: str = "", meta: Optional[dict[str, Any]] = None):
        encoded = base64.b64encode(payload).decode("ascii")
        super().__init__(kind="bytes", schema=schema, meta=dict(meta or {}))
        object.__setattr__(self, "payload_b64", encoded)

    @property
    def payload(self) -> bytes:
        return base64.b64decode(self.payload_b64.encode("ascii"))

    def to_wire(self) -> dict[str, Any]:
        return {INPUT_WIRE_KEY: {**self._base_payload(), "payload_b64": self.payload_b64}}


@dataclass(frozen=True)
class FileInputSpec(InputSpec):
    path: str = ""
    root: str = ""
    fmt: str = "binary"
    encoding: str = "utf-8"
    delivery: str = "content"
    cache: bool = False

    def __init__(
        self,
        path: str,
        *,
        root: str = "",
        fmt: str = "binary",
        encoding: str = "utf-8",
        delivery: str = "content",
        cache: bool = False,
        schema: str = "",
        meta: Optional[dict[str, Any]] = None,
    ):
        fmt_value = str(fmt).strip().lower() or "binary"
        if fmt_value not in {"binary", "text", "json"}:
            raise ValueError(f"Invalid file format '{fmt}'. Use binary/text/json.")
        delivery_value = str(delivery or "content").strip().lower()
        if delivery_value not in {"content", "path"}:
            raise ValueError("FileInputSpec delivery must be 'content' or 'path'.")
        super().__init__(kind="file", schema=schema, meta=dict(meta or {}))
        object.__setattr__(self, "path", str(path))
        object.__setattr__(self, "root", str(root or ""))
        object.__setattr__(self, "fmt", fmt_value)
        object.__setattr__(self, "encoding", str(encoding or "utf-8"))
        object.__setattr__(self, "delivery", delivery_value)
        object.__setattr__(self, "cache", bool(cache))

    def to_wire(self) -> dict[str, Any]:
        return {
            INPUT_WIRE_KEY: {
                **self._base_payload(),
                "path": self.path,
                "root": self.root,
                "fmt": self.fmt,
                "encoding": self.encoding,
                "delivery": self.delivery,
                "cache": self.cache,
            }
        }


@dataclass(frozen=True)
class ProjectOutputDirSpec(InputSpec):
    """A project directory where a task may create one or more artifacts."""

    path: str = ""

    def __init__(self, path: str, *, schema: str = "", meta: Optional[dict[str, Any]] = None):
        normalized = str(path or "").strip()
        if not normalized:
            raise ValueError("ProjectOutputDirSpec path must not be empty.")
        super().__init__(kind="project_output_dir", schema=schema, meta=dict(meta or {}))
        object.__setattr__(self, "path", normalized)

    def to_wire(self) -> dict[str, Any]:
        return {INPUT_WIRE_KEY: {**self._base_payload(), "path": self.path}}


@dataclass(frozen=True)
class FileArtifact:
    """A file produced by a worker that MF must retain in the project.

    ``destination`` is project-relative. Ray returns only the path when the
    filesystem is shared; otherwise it transfers the bytes and MF writes them.
    """

    path: str
    destination: str


@dataclass(frozen=True)
class DbInputSpec(InputSpec):
    table: str = ""
    columns: tuple[str, ...] = ()
    where: dict[str, Any] = field(default_factory=dict)
    order: tuple[str, ...] = ()
    limit: Optional[int] = None
    offset: int = 0
    query: str = ""
    params: tuple[Any, ...] = ()
    db_role: str = "project"
    db_path: str = ""

    def __init__(
        self,
        *,
        table: str = "",
        columns: Iterable[str] = (),
        fields: Iterable[str] = (),
        where: Optional[dict[str, Any]] = None,
        filters: Optional[dict[str, Any]] = None,
        order: Iterable[str] = (),
        limit: Optional[int] = None,
        offset: int = 0,
        query: str = "",
        params: Iterable[Any] = (),
        db_role: str = "project",
        db_path: str = "",
        schema: str = "",
        meta: Optional[dict[str, Any]] = None,
    ):
        if not table and not query:
            raise ValueError("DbInputSpec requires either table or query.")
        role = str(db_role or "project").strip().lower()
        if role not in {"project", "executor", "custom"}:
            raise ValueError(f"Invalid db_role '{db_role}'. Use project/executor/custom.")
        normalized_columns = tuple(str(col) for col in (columns or ()))
        normalized_fields = tuple(str(col) for col in (fields or ()))
        if normalized_columns and normalized_fields and normalized_columns != normalized_fields:
            raise ValueError("DbInputSpec received both columns and fields with different values.")
        normalized_where = dict(where or {})
        normalized_filters = dict(filters or {})
        if normalized_where and normalized_filters and normalized_where != normalized_filters:
            raise ValueError("DbInputSpec received both where and filters with different values.")
        super().__init__(kind="db", schema=schema, meta=dict(meta or {}))
        object.__setattr__(self, "table", str(table or ""))
        object.__setattr__(
            self,
            "columns",
            tuple(_normalize_db_identifier(col, label="field") for col in (normalized_columns or normalized_fields)),
        )
        object.__setattr__(self, "where", dict(normalized_where or normalized_filters))
        object.__setattr__(self, "order", _normalize_db_order(order))
        object.__setattr__(self, "limit", None if limit is None else max(0, int(limit)))
        object.__setattr__(self, "offset", max(0, int(offset or 0)))
        object.__setattr__(self, "query", str(query or ""))
        object.__setattr__(self, "params", tuple(params))
        object.__setattr__(self, "db_role", role)
        object.__setattr__(self, "db_path", str(db_path or ""))

    @property
    def fields(self) -> tuple[str, ...]:
        return self.columns

    @property
    def filters(self) -> dict[str, Any]:
        return dict(self.where)

    def to_wire(self) -> dict[str, Any]:
        return {
            INPUT_WIRE_KEY: {
                **self._base_payload(),
                "table": self.table,
                "columns": list(self.columns),
                "where": to_wire_value(self.where),
                "order": list(self.order),
                "limit": self.limit,
                "offset": self.offset,
                "query": self.query,
                "params": [to_wire_value(item) for item in self.params],
                "db_role": self.db_role,
                "db_path": self.db_path,
            }
        }


@dataclass(frozen=True)
class OutputSpec(DataSpecBase):
    def to_wire(self) -> dict[str, Any]:
        raise NotImplementedError


@dataclass(frozen=True)
class FileOutputSpec(OutputSpec):
    path: str = ""
    root: str = ""
    fmt: str = "binary"
    encoding: str = "utf-8"
    ensure_parent: bool = True
    append: bool = False
    per_batch: bool = False

    def __init__(
        self,
        path: str,
        *,
        root: str = "",
        fmt: str = "binary",
        encoding: str = "utf-8",
        ensure_parent: bool = True,
        append: bool = False,
        per_batch: bool = False,
        schema: str = "",
        meta: Optional[dict[str, Any]] = None,
    ):
        fmt_value = str(fmt).strip().lower() or "binary"
        if fmt_value not in {"binary", "text", "json"}:
            raise ValueError(f"Invalid file format '{fmt}'. Use binary/text/json.")
        if append and fmt_value == "json":
            # Concatenating JSON arrays does not yield valid JSON. For streaming, use
            # fmt="text" with one JSON line per item (JSON Lines).
            raise ValueError("append=True is not supported with fmt='json'. Use fmt='text' (JSON Lines).")
        if append and per_batch:
            raise ValueError("append and per_batch are exclusive: one file appended, or one file per batch.")
        super().__init__(kind="file", schema=schema, meta=dict(meta or {}))
        object.__setattr__(self, "path", str(path))
        object.__setattr__(self, "root", str(root or ""))
        object.__setattr__(self, "fmt", fmt_value)
        object.__setattr__(self, "encoding", str(encoding or "utf-8"))
        object.__setattr__(self, "ensure_parent", bool(ensure_parent))
        object.__setattr__(self, "append", bool(append))
        object.__setattr__(self, "per_batch", bool(per_batch))

    def to_wire(self) -> dict[str, Any]:
        return {
            OUTPUT_WIRE_KEY: {
                **self._base_payload(),
                "path": self.path,
                "root": self.root,
                "fmt": self.fmt,
                "encoding": self.encoding,
                "ensure_parent": self.ensure_parent,
                "append": self.append,
                "per_batch": self.per_batch,
            }
        }


@dataclass(frozen=True)
class DbOutputSpec(OutputSpec):
    table: str = ""
    columns: tuple[str, ...] = ()
    model_path: str = ""
    write_mode: str = "bulk"
    conflict_keys: tuple[str, ...] = ()
    validate_model: bool = False
    db_role: str = "project"
    db_path: str = ""
    mode: str = "insert"

    def __init__(
        self,
        *,
        table: str,
        columns: Iterable[str] = (),
        model_path: str = "",
        write_mode: str = "bulk",
        conflict_keys: Iterable[str] = (),
        validate_model: bool = False,
        db_role: str = "project",
        db_path: str = "",
        mode: str = "insert",
        schema: str = "",
        meta: Optional[dict[str, Any]] = None,
    ):
        role = str(db_role or "project").strip().lower()
        if role not in {"project", "executor", "custom"}:
            raise ValueError(f"Invalid db_role '{db_role}'. Use project/executor/custom.")
        mode_value = str(mode or "insert").strip().lower()
        if mode_value not in {"insert", "graph"}:
            raise ValueError("DbOutputSpec currently supports only mode='insert' or mode='graph'.")
        write_mode_value = str(write_mode or "bulk").strip().lower()
        if write_mode_value not in {"bulk", "row", "upsert"}:
            raise ValueError(f"Invalid write_mode '{write_mode}'. Use bulk/row/upsert.")
        conflict_keys_value = tuple(str(key).strip() for key in conflict_keys if str(key).strip())
        if write_mode_value == "upsert":
            if not str(model_path or "").strip():
                raise ValueError("write_mode='upsert' requires model_path.")
            if not conflict_keys_value:
                conflict_keys_value = ("id",)
        if validate_model and not str(model_path or "").strip():
            raise ValueError("validate_model=True requires model_path.")
        super().__init__(kind="db", schema=schema, meta=dict(meta or {}))
        object.__setattr__(self, "table", str(table))
        object.__setattr__(self, "columns", tuple(str(col) for col in columns))
        object.__setattr__(self, "model_path", str(model_path or ""))
        object.__setattr__(self, "write_mode", write_mode_value)
        object.__setattr__(self, "conflict_keys", conflict_keys_value)
        object.__setattr__(self, "validate_model", bool(validate_model))
        object.__setattr__(self, "db_role", role)
        object.__setattr__(self, "db_path", str(db_path or ""))
        object.__setattr__(self, "mode", mode_value)

    def to_wire(self) -> dict[str, Any]:
        return {
            OUTPUT_WIRE_KEY: {
                **self._base_payload(),
                "table": self.table,
                "columns": list(self.columns),
                "model_path": self.model_path,
                "write_mode": self.write_mode,
                "conflict_keys": list(self.conflict_keys),
                "validate_model": self.validate_model,
                "db_role": self.db_role,
                "db_path": self.db_path,
                "mode": self.mode,
            }
        }


@dataclass(frozen=True)
class TableOutputSpec(DbOutputSpec):
    def __init__(
        self,
        table_name: str,
        *,
        columns: Iterable[str] = (),
        schema: str = "",
        meta: Optional[dict[str, Any]] = None,
    ):
        super().__init__(
            table=str(table_name),
            columns=columns,
            db_role="project",
            schema=schema,
            meta=meta,
        )


def wire_to_input_spec(payload: Mapping[str, Any]) -> InputSpec:
    kind = str(payload.get("kind", "")).strip().lower()
    schema = str(payload.get("schema", "") or "")
    meta = dict(payload.get("meta") or {})
    if kind == "inline":
        return InlineInputSpec(payload=from_wire_value(payload.get("payload")), schema=schema, meta=meta)
    if kind == "bytes":
        raw = str(payload.get("payload_b64", "") or "")
        return BytesInputSpec(payload=base64.b64decode(raw.encode("ascii")), schema=schema, meta=meta)
    if kind == "file":
        return FileInputSpec(
            path=str(payload.get("path", "")),
            root=str(payload.get("root", "")),
            fmt=str(payload.get("fmt", "binary")),
            encoding=str(payload.get("encoding", "utf-8")),
            delivery=str(payload.get("delivery", "content")),
            cache=bool(payload.get("cache", False)),
            schema=schema,
            meta=meta,
        )
    if kind == "project_output_dir":
        return ProjectOutputDirSpec(path=str(payload.get("path", "")), schema=schema, meta=meta)
    if kind == "db":
        return DbInputSpec(
            table=str(payload.get("table", "")),
            columns=payload.get("columns") or (),
            where=from_wire_value(payload.get("where") or {}),
            order=payload.get("order") or (),
            limit=payload.get("limit"),
            offset=payload.get("offset", 0),
            query=str(payload.get("query", "")),
            params=from_wire_value(payload.get("params") or []),
            db_role=str(payload.get("db_role", "project")),
            db_path=str(payload.get("db_path", "")),
            schema=schema,
            meta=meta,
        )
    raise DataContractError(f"Unsupported input spec kind '{kind}'.")


def wire_to_output_spec(payload: Mapping[str, Any]) -> OutputSpec:
    kind = str(payload.get("kind", "")).strip().lower()
    schema = str(payload.get("schema", "") or "")
    meta = dict(payload.get("meta") or {})
    if kind == "file":
        return FileOutputSpec(
            path=str(payload.get("path", "")),
            root=str(payload.get("root", "")),
            fmt=str(payload.get("fmt", "binary")),
            encoding=str(payload.get("encoding", "utf-8")),
            ensure_parent=bool(payload.get("ensure_parent", True)),
            append=bool(payload.get("append", False)),
            per_batch=bool(payload.get("per_batch", False)),
            schema=schema,
            meta=meta,
        )
    if kind == "db":
        return DbOutputSpec(
            table=str(payload.get("table", "")),
            columns=payload.get("columns") or (),
            model_path=str(payload.get("model_path", "")),
            write_mode=str(payload.get("write_mode", "bulk")),
            conflict_keys=payload.get("conflict_keys") or (),
            validate_model=bool(payload.get("validate_model", False)),
            db_role=str(payload.get("db_role", "project")),
            db_path=str(payload.get("db_path", "")),
            mode=str(payload.get("mode", "insert")),
            schema=schema,
            meta=meta,
        )
    raise DataContractError(f"Unsupported output spec kind '{kind}'.")


def to_wire_value(value: Any) -> Any:
    if isinstance(value, InputSpec):
        return value.to_wire()
    if isinstance(value, OutputSpec):
        return value.to_wire()
    if isinstance(value, dict):
        return {str(key): to_wire_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_wire_value(item) for item in value]
    return value


def from_wire_value(value: Any) -> Any:
    if isinstance(value, dict):
        if INPUT_WIRE_KEY in value:
            return wire_to_input_spec(dict(value[INPUT_WIRE_KEY] or {}))
        if OUTPUT_WIRE_KEY in value:
            return wire_to_output_spec(dict(value[OUTPUT_WIRE_KEY] or {}))
        return {str(key): from_wire_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [from_wire_value(item) for item in value]
    return value


def payload_has_input_specs(value: Any) -> bool:
    if isinstance(value, InputSpec):
        return True
    if isinstance(value, dict):
        if INPUT_WIRE_KEY in value:
            return True
        return any(payload_has_input_specs(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(payload_has_input_specs(item) for item in value)
    return False


__all__ = [
    "BytesInputSpec",
    "DataContractError",
    "DbInputSpec",
    "DbOutputSpec",
    "FileInputSpec",
    "FileArtifact",
    "FileOutputSpec",
    "INPUT_WIRE_KEY",
    "InlineInputSpec",
    "InputSpec",
    "OUTPUT_WIRE_KEY",
    "RAY_FILE_INPUT_KEY",
    "RAY_FILE_ARTIFACT_KEY",
    "RAY_OUTPUT_DIR_KEY",
    "ProjectOutputDirSpec",
    "OutputSpec",
    "TableOutputSpec",
    "from_wire_value",
    "payload_has_input_specs",
    "to_wire_value",
]
