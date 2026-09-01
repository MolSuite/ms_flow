from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence, TypeVar
from collections import OrderedDict
import threading

from sqlalchemy import column as sql_column, text as sql_text
from sqlmodel import SQLModel, func, select as sql_select

from ms_flow.core.database.base import BaseSQLiteDB, create_project_tables
from ms_flow.core.database.project_records import (
    ProjectBulkInsertRequest,
    ProjectBulkInsertResult,
    ProjectCommitKey,
    ProjectCommitReceipt,
    ProjectGraphInsertRequest,
    ProjectGraphInsertResult,
    ProjectReadQuery,
    ProjectStoreCapabilities,
)
from ms_flow.core.database.project_store_ops import _ProjectStoreSqliteOps
from ms_flow.core.database.sqlite_utils import compile_subquery_sql


_PROJECT_STORE_CACHE: "OrderedDict[str, ProjectStore]" = OrderedDict()
_PROJECT_STORE_CACHE_LOCK = threading.Lock()
_PROJECT_STORE_CACHE_MAX = 8
ModelT = TypeVar("ModelT", bound=SQLModel)


def subquery_clause(spec: Any, *, null_safe: bool):
    """QuerySpec -> SQLAlchemy clause reusable by the model compiler.

    QuerySpec compiles with positional `?` parameters; SQLAlchemy needs named ones. The
    conversion is literal because QuerySpec never emits text literals: every value is bound
    and identifiers are normalised.
    """
    sql, params = compile_subquery_sql(spec, null_safe=null_safe)
    col = str((getattr(spec, "fields", None) or ("",))[0]).split(".")[-1]
    chunks = sql.split("?")
    if len(chunks) - 1 != len(params):
        raise ValueError("Subquery placeholder count does not match its parameters.")
    binds: dict[str, Any] = {}
    rendered = chunks[0]
    for index, chunk in enumerate(chunks[1:]):
        name = f"__sq_{index}"
        binds[name] = params[index]
        rendered += f":{name}{chunk}"
    # `.columns()` turns the text into a typed SELECT: SQLAlchemy renders it as a
    # subquery (`IN (SELECT ...)`), not as a list of expressions.
    return sql_text(rendered).bindparams(**binds).columns(sql_column(col))


class ProjectStore:
    """SQLite store for project data and artifacts.

    This object owns the `project.db` connection lifecycle and exposes the
    optimized project write/read operations.
    """

    _OUTPUT_COMMIT_TABLE = "molsuite_output_commits"

    def __init__(self, db_path: Path | str | "ProjectStore" | None = None, *, auto_setup: bool = False):
        if isinstance(db_path, ProjectStore):
            self.__dict__ = db_path.__dict__
            return
        self.project_dir: Path | None = None
        self._sqlite = _ProjectSQLiteHandle(db_path=None, auto_setup=False)
        self._ops = _ProjectStoreSqliteOps(self)
        if db_path is not None:
            self.set_db_path(db_path)
            if auto_setup:
                self.setup()

    @classmethod
    def open_at(cls, db_path: Path | str) -> "ProjectStore":
        store = cls(db_path)
        store.setup()
        return store

    @classmethod
    def open_cached(cls, db_path: Path | str) -> "ProjectStore":
        resolved = Path(db_path).expanduser().resolve()
        key = str(resolved)
        with _PROJECT_STORE_CACHE_LOCK:
            cached = _PROJECT_STORE_CACHE.get(key)
            if cached is not None:
                _PROJECT_STORE_CACHE.move_to_end(key)
                return cached
            store = cls.open_at(resolved)
            _PROJECT_STORE_CACHE[key] = store
            while len(_PROJECT_STORE_CACHE) > _PROJECT_STORE_CACHE_MAX:
                _, evicted = _PROJECT_STORE_CACHE.popitem(last=False)
                evicted.dispose()
            return store

    @staticmethod
    def clear_cached_stores() -> None:
        with _PROJECT_STORE_CACHE_LOCK:
            for store in _PROJECT_STORE_CACHE.values():
                store.dispose()
            _PROJECT_STORE_CACHE.clear()

    @staticmethod
    def resolve_db_path(path: Path | str) -> Path:
        resolved = Path(path).expanduser().resolve()
        if resolved.name == "project.db":
            return resolved
        return resolved / "project.db"

    @staticmethod
    def commit_key_from_extras(extras: dict[str, Any]) -> ProjectCommitKey | None:
        sink_key = str(extras.get("molsuite_output_sink_key") or "").strip()
        commit_key = str(extras.get("molsuite_output_commit_key") or "").strip()
        if not sink_key or not commit_key:
            return None
        return ProjectCommitKey(sink_key=sink_key, commit_key=commit_key)

    def output_commits_exist(self, sink_key: str, commit_keys: Sequence[str]) -> set[str]:
        normalized_sink_key = str(sink_key or "").strip()
        keys = [str(key) for key in commit_keys if str(key or "").strip()]
        if not normalized_sink_key or not keys:
            return set()
        placeholders = ", ".join("?" for _ in keys)
        with self.get_session() as session:
            conn = session.connection()
            conn.exec_driver_sql(
                f"""
                CREATE TABLE IF NOT EXISTS {self._OUTPUT_COMMIT_TABLE} (
                    sink_key TEXT NOT NULL,
                    commit_key TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    row_count INTEGER NOT NULL DEFAULT 0,
                    committed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (sink_key, commit_key)
                )
                """
            )
            rows = conn.exec_driver_sql(
                f"""
                SELECT commit_key
                FROM {self._OUTPUT_COMMIT_TABLE}
                WHERE sink_key = ? AND commit_key IN ({placeholders})
                """,
                (normalized_sink_key, *keys),
            ).all()
        return {str(row[0]) for row in rows}

    @property
    def capabilities(self) -> ProjectStoreCapabilities:
        return self._ops.capabilities

    def read_rows(self, query: ProjectReadQuery) -> list[dict[str, Any]]:
        return self._ops.read_rows(query)

    def count_rows(self, query: ProjectReadQuery) -> int:
        return self._ops.count_rows(query)

    def bulk_insert_rows(self, request: ProjectBulkInsertRequest) -> ProjectBulkInsertResult:
        return self._ops.bulk_insert_rows(request)

    def insert_graph(self, request: ProjectGraphInsertRequest) -> ProjectGraphInsertResult:
        return self._ops.insert_graph(request)

    def has_commit_receipt(self, commit_key: ProjectCommitKey) -> bool:
        return self._ops.has_commit_receipt(commit_key)

    def record_commit_receipt(self, receipt: ProjectCommitReceipt) -> None:
        self._ops.record_commit_receipt(receipt)

    def record_commit_receipts(self, receipts: Sequence[ProjectCommitReceipt]) -> None:
        rows = [
            (
                str(receipt.sink_key or "").strip(),
                str(receipt.commit_key or "").strip(),
                str(receipt.target_name or "").strip() or "__unknown__",
                int(receipt.row_count or 0),
            )
            for receipt in receipts
            if str(receipt.sink_key or "").strip() and str(receipt.commit_key or "").strip()
        ]
        if not rows:
            return
        with self.get_session() as session:
            conn = session.connection()
            conn.exec_driver_sql(
                f"""
                CREATE TABLE IF NOT EXISTS {self._OUTPUT_COMMIT_TABLE} (
                    sink_key TEXT NOT NULL,
                    commit_key TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    row_count INTEGER NOT NULL DEFAULT 0,
                    committed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (sink_key, commit_key)
                )
                """
            )
            conn.exec_driver_sql(
                f"""
                INSERT OR IGNORE INTO {self._OUTPUT_COMMIT_TABLE} (
                    sink_key,
                    commit_key,
                    table_name,
                    row_count
                ) VALUES (?, ?, ?, ?)
                """,
                rows,
            )
            session.commit()

    def persist_output_spec(self, spec: Any, data: Any, *, commit_key: ProjectCommitKey | None = None) -> dict[str, Any]:
        from collections.abc import Mapping
        from ms_flow.core.data.contracts import DataContractError

        if getattr(spec, "mode", "insert") == "graph":
            if not isinstance(data, Mapping):
                raise DataContractError("Graph project output requires a mapping payload keyed by node name.")
            payload: dict[str, list[dict[str, Any]]] = {}
            for node_name, rows in dict(data).items():
                if isinstance(rows, Mapping):
                    payload[str(node_name)] = [dict(rows)]
                elif isinstance(rows, list):
                    payload[str(node_name)] = [dict(item) for item in rows]
                elif rows in (None, ()):
                    payload[str(node_name)] = []
                else:
                    raise DataContractError(
                        f"Graph project output node '{node_name}' requires dict or list[dict] payload."
                    )
            result = self.insert_graph(
                ProjectGraphInsertRequest(
                    graph_meta=dict(getattr(spec, "meta", {}) or {}),
                    payload=payload,
                    commit_key=commit_key,
                )
            )
            return dict(result.metadata)

        if isinstance(data, dict):
            rows = (dict(data),)
        elif isinstance(data, list):
            rows = tuple(dict(item) for item in data)
        else:
            raise DataContractError("Project DbOutputSpec writes require dict or list[dict] payload.")
        result = self.bulk_insert_rows(
            ProjectBulkInsertRequest(
                table_name=spec.table,
                rows=rows,
                columns=tuple(spec.columns),
                model_path=spec.model_path,
                write_mode=spec.write_mode,
                conflict_keys=tuple(spec.conflict_keys),
                validate_model=spec.validate_model,
                commit_key=commit_key,
            )
        )
        return dict(result.metadata)

    @property
    def engine(self) -> Any:
        return self._sqlite.engine

    @property
    def db_path(self) -> Path | None:
        return self._sqlite.db_path

    def connect(self, project_dir: Path):
        normalized_dir = Path(project_dir).expanduser().resolve()
        normalized_dir.mkdir(parents=True, exist_ok=True)
        self.connect_path(normalized_dir / "project.db", project_dir=normalized_dir)

    def connect_path(self, db_path: Path | str, *, project_dir: Path | None = None):
        normalized_dir = Path(project_dir).expanduser().resolve() if project_dir is not None else None
        resolved_db_path = ProjectStore.resolve_db_path(db_path)
        if self.engine is not None and self.db_path == resolved_db_path:
            self.project_dir = normalized_dir
            return
        self.disconnect()
        self.project_dir = normalized_dir
        self._sqlite.set_db_path(resolved_db_path)
        self._sqlite.setup()

    def disconnect(self):
        self._sqlite.dispose()
        self._sqlite.db_path = None
        self.project_dir = None

    def get_session(self):
        return self._sqlite.get_session()

    def reconnect(self):
        self._sqlite.reconnect()

    def dispose(self):
        self._sqlite.dispose()

    def setup(self):
        self._sqlite.setup()

    def set_db_path(self, db_path: Path | str):
        self._sqlite.set_db_path(ProjectStore.resolve_db_path(db_path))

    # Transitional CRUD helpers. Hot write paths should use bulk_insert_rows().
    @staticmethod
    def _resolve_model_field(model: type[ModelT], field_name: str):
        name = str(field_name or "").strip()
        if not name:
            raise ValueError("Field name cannot be empty.")
        if not hasattr(model, name):
            raise ValueError(f"Unknown field '{name}' for model {model.__name__}.")
        return getattr(model, name)

    @classmethod
    def _model_filter_expressions(
        cls,
        model: type[ModelT],
        filters: Mapping[str, Any] | None = None,
    ) -> list[Any]:
        expressions: list[Any] = []
        for raw_key, raw_value in (filters or {}).items():
            key = str(raw_key)
            field_name, op = key.split("__", 1) if "__" in key else (key, "eq")
            field = cls._resolve_model_field(model, field_name)
            operator = op.strip().lower()
            value = raw_value

            if operator == "eq":
                expressions.append(field == value)
            elif operator == "ne":
                expressions.append(field != value)
            elif operator == "gt":
                expressions.append(field > value)
            elif operator == "gte":
                expressions.append(field >= value)
            elif operator == "lt":
                expressions.append(field < value)
            elif operator == "lte":
                expressions.append(field <= value)
            elif operator == "in":
                values = [] if value is None else list(value) if isinstance(value, (list, tuple, set)) else [value]
                expressions.append(field.in_(values))
            elif operator == "not_in":
                if value is None:
                    continue
                values = list(value) if isinstance(value, (list, tuple, set)) else [value]
                expressions.append(~field.in_(values))
            elif operator in ("in_subquery", "not_in_subquery"):
                # Membership/state live in other tables: they are declared as a single-column
                # QuerySpec and resolved by the database. Ids are never materialised in Python.
                specs = value if isinstance(value, (list, tuple)) else [value]
                for spec in specs:
                    clause = subquery_clause(spec, null_safe=operator == "not_in_subquery")
                    expressions.append(
                        field.in_(clause) if operator == "in_subquery" else field.not_in(clause)
                    )
            elif operator == "contains":
                expressions.append(field.contains(value))
            elif operator == "startswith":
                expressions.append(field.startswith(value))
            elif operator == "endswith":
                expressions.append(field.endswith(value))
            elif operator == "is_null":
                flag = True if value is None else bool(value)
                expressions.append(field.is_(None) if flag else field.is_not(None))
            elif operator == "is_not_null":
                flag = True if value is None else bool(value)
                expressions.append(field.is_not(None) if flag else field.is_(None))
            else:
                raise ValueError(f"Unsupported filter operator '{operator}' in '{key}'.")
        return expressions

    @classmethod
    def _model_order_expressions(
        cls,
        model: type[ModelT],
        order: Sequence[Any] | None = None,
    ) -> list[Any]:
        expressions: list[Any] = []
        for raw_item in order or ():
            if not isinstance(raw_item, str):
                expressions.append(raw_item)
                continue
            item = raw_item.strip()
            if not item:
                continue
            desc = item.startswith("-")
            field_name = item[1:] if desc else item
            field = cls._resolve_model_field(model, field_name)
            expressions.append(field.desc() if desc else field.asc())
        return expressions

    @staticmethod
    def _model_row_dict(row: Any, fields: Sequence[str] | None = None) -> dict[str, Any]:
        if isinstance(row, dict):
            return dict(row) if fields is None else {name: row.get(name) for name in fields}
        if isinstance(row, SQLModel):
            dumped = row.model_dump(mode="python")
            return dumped if fields is None else {name: dumped.get(name) for name in fields}
        if fields is None:
            if hasattr(row, "_mapping"):
                return dict(row._mapping)
            return {"value": row}
        return {name: getattr(row, name, None) for name in fields}

    def insert(self, row: ModelT, *, commit: bool = True, refresh: bool = True) -> ModelT:
        with self.get_session() as session:
            session.add(row)
            if commit:
                session.commit()
            if refresh:
                session.refresh(row)
            return row

    def insert_batch(self, rows: Iterable[ModelT], *, commit: bool = True):
        rows_list = list(rows)
        if not rows_list:
            return []
        with self.get_session() as session:
            session.add_all(rows_list)
            if commit:
                session.commit()
            return rows_list

    def select(
        self,
        model: type[ModelT],
        *,
        filters: Mapping[str, Any] | None = None,
        order: Sequence[Any] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[ModelT]:
        statement = sql_select(model)
        for condition in self._model_filter_expressions(model, filters):
            statement = statement.where(condition)
        order_exprs = self._model_order_expressions(model, order)
        if order_exprs:
            statement = statement.order_by(*order_exprs)
        if offset is not None and offset > 0:
            statement = statement.offset(offset)
        if limit is not None and limit >= 0:
            statement = statement.limit(limit)
        with self.get_session() as session:
            return list(session.exec(statement).all())

    def first(
        self,
        model: type[ModelT],
        *,
        filters: Mapping[str, Any] | None = None,
        order: Sequence[Any] | None = None,
    ) -> ModelT | None:
        rows = self.select(model, filters=filters, order=order, limit=1)
        return rows[0] if rows else None

    def count(self, model: type[ModelT], *, filters: Mapping[str, Any] | None = None) -> int:
        statement = sql_select(func.count()).select_from(model)
        for condition in self._model_filter_expressions(model, filters):
            statement = statement.where(condition)
        with self.get_session() as session:
            return int(session.exec(statement).one())

    def delete(self, model: type[ModelT], *, filters: Mapping[str, Any] | None = None) -> int:
        rows = self.select(model, filters=filters)
        if not rows:
            return 0
        with self.get_session() as session:
            for row in rows:
                attached = session.merge(row)
                session.delete(attached)
            session.commit()
        return len(rows)

    def execute(self, statement):
        with self.get_session() as session:
            result = session.exec(statement)
            try:
                return result.all()
            except Exception:
                return result

    def stream(
        self,
        model: type[ModelT],
        *,
        filters: Mapping[str, Any] | None = None,
        order: Sequence[Any] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        yield_per: int = 1000,
    ) -> Iterable[ModelT]:
        statement = sql_select(model)
        for condition in self._model_filter_expressions(model, filters):
            statement = statement.where(condition)
        order_exprs = self._model_order_expressions(model, order)
        if order_exprs:
            statement = statement.order_by(*order_exprs)
        if offset is not None and offset > 0:
            statement = statement.offset(offset)
        if limit is not None and limit >= 0:
            statement = statement.limit(limit)
        statement = statement.execution_options(yield_per=yield_per)
        with self.get_session() as session:
            for row in session.exec(statement):
                yield row

    def select_rows(
        self,
        model: type[ModelT],
        *,
        fields: Sequence[str] | None = None,
        filters: Mapping[str, Any] | None = None,
        order: Sequence[Any] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict[str, Any]]:
        rows = self.select(model, filters=filters, order=order, limit=limit, offset=offset)
        return [self._model_row_dict(row, fields=fields) for row in rows]

    def stream_rows(self, *args, **kwargs):
        if args and args[0].__class__.__name__ == "ProjectReadQuery":
            return self._ops.stream_rows(*args, **kwargs)
        return self.stream_model_rows(*args, **kwargs)

    def stream_model_rows(
        self,
        model: type[ModelT],
        *,
        fields: Sequence[str] | None = None,
        filters: Mapping[str, Any] | None = None,
        order: Sequence[Any] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        yield_per: int = 1000,
    ) -> Iterable[dict[str, Any]]:
        for row in self.stream(
            model,
            filters=filters,
            order=order,
            limit=limit,
            offset=offset,
            yield_per=yield_per,
        ):
            yield self._model_row_dict(row, fields=fields)

    def page_after(
        self,
        model: type[ModelT],
        *,
        cursor_field: str = "id",
        after: Any = None,
        limit: int = 1000,
        filters: Mapping[str, Any] | None = None,
        fields: Sequence[str] | None = None,
        ascending: bool = True,
    ) -> tuple[list[dict[str, Any]], Any]:
        self._resolve_model_field(model, cursor_field)
        merged_filters = dict(filters or {})
        if after is not None:
            op = "gt" if ascending else "lt"
            merged_filters[f"{cursor_field}__{op}"] = after
        order = [cursor_field if ascending else f"-{cursor_field}"]
        effective_fields = list(fields or [])
        include_cursor = cursor_field not in effective_fields
        if include_cursor:
            effective_fields.append(cursor_field)
        rows = self.select_rows(
            model,
            fields=effective_fields,
            filters=merged_filters or None,
            order=order,
            limit=max(1, int(limit)),
        )
        if not rows:
            return [], after
        next_cursor = rows[-1].get(cursor_field)
        if include_cursor:
            for row in rows:
                row.pop(cursor_field, None)
        return rows, next_cursor

    def stream_keyset(
        self,
        model: type[ModelT],
        *,
        cursor_field: str = "id",
        after: Any = None,
        batch_size: int = 1000,
        filters: Mapping[str, Any] | None = None,
        fields: Sequence[str] | None = None,
        ascending: bool = True,
        max_rows: int | None = None,
    ) -> Iterable[dict[str, Any]]:
        emitted = 0
        cursor = after
        while True:
            rows, cursor = self.page_after(
                model,
                cursor_field=cursor_field,
                after=cursor,
                limit=batch_size,
                filters=filters,
                fields=fields,
                ascending=ascending,
            )
            if not rows:
                break
            for row in rows:
                yield row
                emitted += 1
                if max_rows is not None and emitted >= max_rows:
                    return

    def update_rows(
        self,
        model: type[ModelT],
        *,
        values: Mapping[str, Any],
        filters: Mapping[str, Any] | None = None,
        limit: int | None = None,
    ) -> int:
        if not values:
            return 0
        rows = self.select(model, filters=filters, limit=limit)
        if not rows:
            return 0
        with self.get_session() as session:
            updated = 0
            for row in rows:
                attached = session.merge(row)
                for key, value in values.items():
                    if hasattr(attached, key):
                        setattr(attached, key, value)
                session.add(attached)
                updated += 1
            session.commit()
            return updated


class _ProjectSQLiteHandle(BaseSQLiteDB):
    def _create_tables(self):
        create_project_tables(self.engine)

    def _setup_path_error_message(self) -> str:
        return "No project is connected."

    def _session_error_message(self) -> str:
        return "No project is connected."


def resolve_project_store(source: Any) -> ProjectStore:
    def _store_from_candidate(candidate: Any) -> ProjectStore | None:
        if candidate is None:
            return None
        if isinstance(candidate, ProjectStore):
            return candidate
        project_store = getattr(candidate, "project_store", None)
        if project_store is not None and project_store is not candidate:
            store = _store_from_candidate(project_store)
            if store is not None:
                return store
        project_db = getattr(candidate, "project_db", None)
        if project_db is not None and project_db is not candidate:
            store = _store_from_candidate(project_db)
            if store is not None:
                return store
        molsuite = getattr(candidate, "molsuite", None)
        if molsuite is not None and molsuite is not candidate:
            store = _store_from_candidate(molsuite)
            if store is not None:
                return store
        return None

    store = _store_from_candidate(source)
    if store is None:
        raise ValueError("Could not resolve project store from source.")
    return store
