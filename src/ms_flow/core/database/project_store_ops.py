from __future__ import annotations

import importlib
import json
from collections import OrderedDict
from pathlib import Path
from typing import Any, Iterator, Mapping, Protocol, Sequence

from sqlalchemy import MetaData, bindparam
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlmodel import Session

from ms_flow.core.data.contracts import DataContractError, DbOutputSpec
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
from ms_flow.core.database.sqlite_utils import compile_count, compile_select, normalize_identifier


class SessionProvider(Protocol):
    def get_session(self) -> Session: ...

    @property
    def db_path(self) -> Path | None: ...

    @property
    def engine(self) -> Any: ...


def _normalize_identifier(value: str, *, label: str) -> str:
    return normalize_identifier(value, label=label)


def resolve_project_store_ops(source: Any) -> "_ProjectStoreSqliteOps":
    def _ops_from_candidate(candidate: Any) -> "_ProjectStoreSqliteOps | None":
        if candidate is None:
            return None
        if isinstance(candidate, _ProjectStoreSqliteOps):
            return candidate
        ops = getattr(candidate, "_ops", None)
        if isinstance(ops, _ProjectStoreSqliteOps):
            return ops
        project_store = getattr(candidate, "project_store", None)
        if project_store is not None and project_store is not candidate:
            ops = _ops_from_candidate(project_store)
            if ops is not None:
                return ops
        project_db = getattr(candidate, "project_db", None)
        if project_db is not None and project_db is not candidate:
            ops = _ops_from_candidate(project_db)
            if ops is not None:
                return ops
        molsuite = getattr(candidate, "molsuite", None)
        if molsuite is not None and molsuite is not candidate:
            ops = _ops_from_candidate(molsuite)
            if ops is not None:
                return ops
        return None

    ops = _ops_from_candidate(source)
    if ops is None:
        raise ValueError(
            "Could not resolve project store SQLite operations from source. Usa MolSuite, ProjectDataContext o ProjectStore."
        )
    return ops


class _StandaloneProjectSQLiteDB(BaseSQLiteDB):
    def _create_tables(self):
        create_project_tables(self.engine)

    def _setup_path_error_message(self) -> str:
        return "project.db no configurada."

    def _session_error_message(self) -> str:
        return "project.db no configurada."


def _column_defaults(model, columns: list[str]) -> dict[str, Any]:
    """The Python defaults of the requested columns, by name.

    A sink writes dicts, not model instances: `default_factory` never runs. And when each row
    is flattened column by column, a missing key became an explicit NULL that also overrode the
    column's own default — or blew up if it was NOT NULL. This fills in only what is **missing**;
    a deliberate None is respected.
    """
    table = getattr(model, "__table__", None)
    if table is None:
        return {}
    defaults: dict[str, Any] = {}
    wanted = set(columns)
    for column in table.columns:
        if column.name not in wanted or column.default is None:
            continue
        default = column.default
        if getattr(default, "is_callable", False):
            defaults[column.name] = lambda arg=default.arg: arg(None)
        elif getattr(default, "is_scalar", False):
            defaults[column.name] = lambda value=default.arg: value
    return defaults


def _fill_column_defaults(rows: list[dict[str, Any]], defaults: dict[str, Any]) -> list[dict[str, Any]]:
    if not defaults:
        return rows
    return [
        row
        if defaults.keys() <= row.keys()
        else {**{name: factory() for name, factory in defaults.items() if name not in row}, **row}
        for row in rows
    ]


class _ProjectStoreSqliteOps:
    _OUTPUT_COMMIT_TABLE = "molsuite_output_commits"

    def __init__(self, db: SessionProvider):
        self._db = db
        self._graph_model_cache: dict[str, Any] = {}
        self._graph_table_cache: dict[tuple[int, str], Any] = {}
        self._graph_spec_cache: dict[str, tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]] = {}

    @property
    def capabilities(self) -> ProjectStoreCapabilities:
        return ProjectStoreCapabilities(
            store_name="sqlite",
            supports_concurrent_writers=False,
            supports_returning_inserted_ids=True,
            requires_local_path=True,
            is_sqlite_family=True,
            notes=(
                "single-writer effective semantics",
                "local file-backed store",
                "prefer batching and controlled writer concurrency",
            ),
        )

    @classmethod
    def open_at(cls, db_path: Path | str) -> "_ProjectStoreSqliteOps":
        db = _StandaloneProjectSQLiteDB(Path(db_path).expanduser().resolve(), auto_setup=True)
        return cls(db)

    @property
    def db_path(self) -> Path | None:
        raw_path = getattr(self._db, "db_path", None)
        if raw_path is None:
            return None
        return Path(raw_path).expanduser().resolve()

    def is_connected(self) -> bool:
        return getattr(self._db, "engine", None) is not None and self.db_path is not None

    def get_session(self) -> Session:
        return self._db.get_session()

    def dispose(self) -> None:
        dispose = getattr(self._db, "dispose", None)
        if callable(dispose):
            dispose()

    @staticmethod
    def _resolve_model(model_path: str):
        module_name, _, qualname = str(model_path or "").partition(":")
        if not module_name or not qualname:
            raise DataContractError(f"Invalid model_path '{model_path}'.")
        model = importlib.import_module(module_name)
        for part in qualname.split("."):
            model = getattr(model, part)
        return model

    @staticmethod
    def _normalize_write_rows(
        *,
        rows: list[dict[str, Any]],
        columns: list[str],
        model=None,
        validate_model: bool = False,
    ) -> list[dict[str, Any]]:
        defaults = {} if model is None else _column_defaults(model, columns)
        normalized: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            if model is not None and validate_model:
                payload = model.model_validate(payload).model_dump(mode="python")
            payload = _fill_column_defaults([payload], defaults)[0]
            normalized.append({column: payload.get(column) for column in columns})
        return normalized

    @staticmethod
    def _rows_to_tuples(
        *,
        rows: list[dict[str, Any]],
        columns: list[str],
        model=None,
        validate_model: bool = False,
        dialect=None,
    ) -> list[tuple[Any, ...]]:
        """Column-ordered tuples in a single pass, for the executemany insert path.

        When validate_model is False (the default all sinks use) this is just a
        column pluck per row — no intermediate normalized dict. When validating,
        it validates+dumps per row and then plucks columns, still one pass.

        Applies each column's SQLAlchemy bind processor (JSON dump, custom
        TypeDecorators) so this raw executemany path matches Core's typed insert.
        Leaf graph nodes and bulk inserts skip Core's RETURNING path, so without
        this a dict/list bound to a JSON column reaches the sqlite3 DBAPI
        unserialized -> "type 'dict' is not supported". Processors are looked up
        once (not per row); when none apply it stays the plain single-pass pluck.
        """
        if model is not None and validate_model:
            rows = [model.model_validate(row).model_dump(mode="python") for row in rows]
        if model is not None:
            rows = _fill_column_defaults(rows, _column_defaults(model, columns))
        processors = None
        if dialect is not None and model is not None and hasattr(model, "__table__"):
            columns_by_name = {column.name: column for column in model.__table__.columns}
            processors = [
                (columns_by_name[name].type.bind_processor(dialect) if name in columns_by_name else None)
                for name in columns
            ]
        if not processors or not any(processors):
            return [tuple(row.get(column) for column in columns) for row in rows]
        return [
            tuple(
                processor(row.get(column)) if processor is not None else row.get(column)
                for column, processor in zip(columns, processors)
            )
            for row in rows
        ]

    @staticmethod
    def _execute_upsert(conn, table, rows: list[dict[str, Any]], *, conflict_keys: list[str], allowed_columns=None) -> int:
        """INSERT ... ON CONFLICT(conflict_keys) DO UPDATE, grouped by payload shape."""
        valid_columns = {column.name for column in table.columns}
        groups: "OrderedDict[tuple[str, ...], list[dict[str, Any]]]" = OrderedDict()
        for row in rows:
            if allowed_columns:
                present = tuple(column for column in allowed_columns if column in valid_columns and column in row)
            else:
                present = tuple(key for key in row.keys() if key in valid_columns)
            for key in conflict_keys:
                if key not in present:
                    raise DataContractError(
                        f"upsert payload is missing conflict key '{key}' for table '{table.name}'."
                    )
            groups.setdefault(present, []).append(row)

        written = 0
        for insert_columns, group_rows in groups.items():
            update_columns = [column for column in insert_columns if column not in conflict_keys]
            statement = sqlite_insert(table)
            if update_columns:
                statement = statement.on_conflict_do_update(
                    index_elements=list(conflict_keys),
                    set_={column: statement.excluded[column] for column in update_columns},
                )
            else:
                statement = statement.on_conflict_do_nothing(index_elements=list(conflict_keys))
            conn.execute(statement, [{column: row.get(column) for column in insert_columns} for row in group_rows])
            written += len(group_rows)
        return written

    @staticmethod
    def _graph_model_columns(model) -> list[str]:
        if not hasattr(model, "__table__"):
            raise DataContractError(f"Graph model {model!r} does not define a SQL table.")
        columns = [column.name for column in model.__table__.columns if not column.primary_key]
        if not columns:
            raise DataContractError(f"Graph model {model!r} does not expose writable columns.")
        return columns

    @staticmethod
    def _resolve_graph_table(conn, node: Mapping[str, Any], model):
        if model is not None:
            table = model.__table__
            if node.get("table") and str(table.name) != str(node.get("table")):
                raise DataContractError(
                    f"Graph node '{node.get('name')}' model table '{table.name}' does not match '{node.get('table')}'."
                )
            return table
        metadata = MetaData()
        table_name = str(node.get("table") or "").strip()
        metadata.reflect(bind=conn, only=[table_name])
        if table_name not in metadata.tables:
            raise DataContractError(f"Graph node '{node.get('name')}' table '{table_name}' was not found in target DB.")
        return metadata.tables[table_name]

    @staticmethod
    def _graph_topological_order(
        node_names: Sequence[str] | Any,
        relations: Sequence[Mapping[str, Any]] | Any,
    ) -> list[str]:
        pending = {name: 0 for name in node_names}
        outgoing: dict[str, list[str]] = {name: [] for name in pending}
        for relation in relations:
            source_node = str(relation.get("source_node") or "")
            target_node = str(relation.get("target_node") or "")
            pending[source_node] += 1
            outgoing[target_node].append(source_node)
        ready = sorted(name for name, degree in pending.items() if degree == 0)
        ordered: list[str] = []
        while ready:
            current = ready.pop(0)
            ordered.append(current)
            for target in outgoing[current]:
                pending[target] -= 1
                if pending[target] == 0:
                    ready.append(target)
                    ready.sort()
        if len(ordered) != len(pending):
            raise DataContractError("GraphOutputSpec relations contain a cycle.")
        return ordered

    def _apply_deferred_relations(
        self,
        conn,
        pending: Sequence[tuple[Mapping[str, Any], str, str]],
        nodes_by_name: Mapping[str, Mapping[str, Any]],
        ref_maps: Mapping[str, Mapping[str, Any]],
    ) -> None:
        """Close the "backwards" FKs with one UPDATE per relation, once both sides have ids.

        A parent pointing at a child (molecules.active_binding_site_id -> binding_sites.id) does not
        fit the topological order: the child needs the parent's id for its own FK. Rather than asking
        the caller to invent a stable index to point at the row, everything is inserted and then
        fixed up, inside the same transaction.
        """
        if not pending:
            return
        grouped: dict[int, list[dict[str, Any]]] = {}
        relations: dict[int, Mapping[str, Any]] = {}
        for relation, source_ref, target_ref in pending:
            key = id(relation)
            target_node = str(relation["target_node"])
            target_id = ref_maps[target_node].get(target_ref)
            if target_id is None:
                raise DataContractError(
                    f"Deferred relation on node '{relation['source_node']}' references unknown ref "
                    f"'{target_ref}' from node '{target_node}'."
                )
            source_id = ref_maps[str(relation["source_node"])].get(source_ref)
            if source_id is None:
                raise DataContractError(
                    f"Deferred relation could not locate row '{source_ref}' in node "
                    f"'{relation['source_node']}': its id was not captured."
                )
            relations[key] = relation
            grouped.setdefault(key, []).append({"_deferred_pk": source_id, "_deferred_fk": target_id})

        for key, params in grouped.items():
            relation = relations[key]
            node = nodes_by_name[str(relation["source_node"])]
            model = self._resolve_cached_model(str(node.get("model_path") or ""))
            table = self._resolve_cached_graph_table(conn, node, model)
            pk_field = str(node.get("pk_field") or "id")
            statement = (
                table.update()
                .where(table.c[pk_field] == bindparam("_deferred_pk"))
                .values({str(relation["fk_field"]): bindparam("_deferred_fk")})
            )
            conn.execute(statement, params)

    def _graph_spec_parts(self, spec: DbOutputSpec) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
        if spec.mode != "graph":
            raise DataContractError("Graph helpers require DbOutputSpec(mode='graph').")
        raw_graph = spec.meta.get("graph")
        if not isinstance(raw_graph, Mapping):
            raise DataContractError("DbOutputSpec(mode='graph') requires meta['graph'].")
        raw_nodes = raw_graph.get("nodes") or ()
        raw_relations = raw_graph.get("relations") or ()
        nodes_by_name: dict[str, dict[str, Any]] = {}
        for raw_node in raw_nodes:
            if not isinstance(raw_node, Mapping):
                raise DataContractError("Graph node definitions must be mapping values.")
            node = dict(raw_node)
            name = str(node.get("name") or "").strip()
            table = str(node.get("table") or "").strip()
            model_path = str(node.get("model_path") or "").strip()
            if not name:
                raise DataContractError("Graph node name must not be empty.")
            if not table and not model_path:
                raise DataContractError(f"Graph node '{name}' requires table or model_path.")
            if name in nodes_by_name:
                raise DataContractError(f"Graph node '{name}' is duplicated.")
            node["name"] = name
            node["table"] = table
            node["model_path"] = model_path
            node["columns"] = [str(column) for column in node.get("columns") or ()]
            node["pk_field"] = str(node.get("pk_field") or "id")
            node["ref_field"] = str(node.get("ref_field") or "$ref")
            node["validate_model"] = bool(node.get("validate_model", False))
            write_mode = str(node.get("write_mode") or "insert").strip().lower()
            if write_mode not in {"insert", "bulk", "upsert"}:
                raise DataContractError(
                    f"Graph node '{name}' has invalid write_mode '{write_mode}'. Use insert/upsert."
                )
            node["write_mode"] = write_mode
            conflict_keys = tuple(str(key).strip() for key in (node.get("conflict_keys") or ()) if str(key).strip())
            if write_mode == "upsert" and not conflict_keys:
                conflict_keys = ("id",)
            node["conflict_keys"] = list(conflict_keys)
            nodes_by_name[name] = node
        if not nodes_by_name:
            raise DataContractError("Graph output requires at least one node.")

        relations: list[dict[str, Any]] = []
        for raw_relation in raw_relations:
            if not isinstance(raw_relation, Mapping):
                raise DataContractError("Graph relation definitions must be mapping values.")
            relation = dict(raw_relation)
            source_node = str(relation.get("source_node") or "").strip()
            source_ref_field = str(relation.get("source_ref_field") or "").strip()
            target_node = str(relation.get("target_node") or "").strip()
            fk_field = str(relation.get("fk_field") or "").strip()
            target_ref_field = str(relation.get("target_ref_field") or "").strip()
            if not source_node or not target_node or not source_ref_field or not fk_field:
                raise DataContractError("Graph relations require source_node, source_ref_field, target_node and fk_field.")
            if source_node not in nodes_by_name:
                raise DataContractError(f"Unknown source node '{source_node}' in graph relation.")
            if target_node not in nodes_by_name:
                raise DataContractError(f"Unknown target node '{target_node}' in graph relation.")
            deferred = bool(relation.get("deferred", False))
            if deferred and not str(nodes_by_name[source_node].get("ref_field") or "$ref"):
                raise DataContractError(
                    f"Deferred relation on node '{source_node}' needs a ref_field: the UPDATE "
                    "has to find the row it already inserted."
                )
            relations.append(
                {
                    "source_node": source_node,
                    "source_ref_field": source_ref_field,
                    "target_node": target_node,
                    "fk_field": fk_field,
                    "target_ref_field": target_ref_field,
                    "deferred": deferred,
                }
            )
        return nodes_by_name, relations

    @staticmethod
    def _normalize_graph_payload(
        data: Any,
        nodes_by_name: Mapping[str, Mapping[str, Any]],
    ) -> dict[str, list[dict[str, Any]]]:
        if not isinstance(data, Mapping):
            raise DataContractError("Graph db output writes require a mapping payload keyed by node name.")
        node_names = set(nodes_by_name)
        unknown_nodes = set(data) - node_names
        if unknown_nodes:
            raise DataContractError(f"Graph db output received unknown nodes: {sorted(unknown_nodes)}")
        payload: dict[str, list[dict[str, Any]]] = {}
        for node_name in nodes_by_name:
            raw_rows = data.get(node_name, [])
            if isinstance(raw_rows, Mapping):
                rows = [dict(raw_rows)]
            elif isinstance(raw_rows, list):
                rows = []
                for item in raw_rows:
                    if not isinstance(item, Mapping):
                        raise DataContractError(
                            f"Graph db output node '{node_name}' requires list[dict] payload."
                        )
                    rows.append(dict(item))
            elif raw_rows in (None, ()):
                rows = []
            else:
                raise DataContractError(
                    f"Graph db output node '{node_name}' requires dict or list[dict] payload."
                )
            payload[node_name] = rows
        return payload

    def _check_output_commit(self, conn, *, sink_key: str, commit_key: str) -> bool:
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
        existing = conn.exec_driver_sql(
            f"SELECT 1 FROM {self._OUTPUT_COMMIT_TABLE} WHERE sink_key = ? AND commit_key = ?",
            (sink_key, commit_key),
        ).first()
        return existing is not None

    def _record_output_commit(self, conn, *, sink_key: str, commit_key: str, table_name: str, row_count: int) -> None:
        conn.exec_driver_sql(
            f"""
            INSERT INTO {self._OUTPUT_COMMIT_TABLE} (
                sink_key,
                commit_key,
                table_name,
                row_count
            ) VALUES (?, ?, ?, ?)
            """,
            (sink_key, commit_key, table_name, row_count),
        )

    @classmethod
    def _compile_query(cls, query: ProjectReadQuery) -> tuple[str, tuple[Any, ...]]:
        return compile_select(
            table=query.table,
            fields=query.fields,
            filters=query.filters,
            order=query.order,
            limit=query.limit,
            offset=query.offset,
            query=query.query,
            params=query.params,
        )

    @staticmethod
    def _rows_from_result(result, *, batch_size: int | None = None) -> Iterator[dict[str, Any]]:
        if batch_size is None:
            for row in result.mappings().all():
                yield dict(row)
            return
        fetch_size = max(1, int(batch_size))
        while True:
            rows = result.mappings().fetchmany(fetch_size)
            if not rows:
                break
            for row in rows:
                yield dict(row)

    def count_rows(self, query: ProjectReadQuery) -> int:
        sql, params = compile_count(
            table=query.table,
            fields=query.fields,
            filters=query.filters,
            order=(),
            limit=query.limit,
            offset=query.offset,
            query=query.query,
            params=query.params,
        )
        with self.get_session() as session:
            row = session.connection().exec_driver_sql(sql, params).first()
        return int(row[0]) if row is not None else 0

    def read_rows(self, query: ProjectReadQuery) -> list[dict[str, Any]]:
        sql, params = self._compile_query(query)
        with self.get_session() as session:
            result = session.connection().exec_driver_sql(sql, params)
            return list(self._rows_from_result(result, batch_size=None))

    def stream_rows(self, query: ProjectReadQuery) -> Iterator[dict[str, Any]]:
        sql, params = self._compile_query(query)
        with self.get_session() as session:
            result = session.connection().exec_driver_sql(sql, params)
            yield from self._rows_from_result(result, batch_size=query.batch_size)

    @staticmethod
    def _graph_cache_key(graph_meta: Mapping[str, Any]) -> str:
        return json.dumps(dict(graph_meta or {}), ensure_ascii=False, sort_keys=True, default=str)

    def _resolve_cached_graph_spec(
        self,
        spec: DbOutputSpec,
    ) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
        cache_key = self._graph_cache_key(spec.meta.get("graph") or {})
        cached = self._graph_spec_cache.get(cache_key)
        if cached is None:
            cached = self._graph_spec_parts(spec)
            self._graph_spec_cache[cache_key] = cached
        return cached

    def _resolve_cached_model(self, model_path: str) -> Any:
        normalized_path = str(model_path or "").strip()
        if not normalized_path:
            return None
        cached = self._graph_model_cache.get(normalized_path)
        if cached is None:
            cached = self._resolve_model(normalized_path)
            self._graph_model_cache[normalized_path] = cached
        return cached

    def _resolve_cached_graph_table(self, conn, node: Mapping[str, Any], model: Any):
        if model is not None:
            return model.__table__
        table_name = str(node.get("table") or "").strip()
        cache_key = (id(conn.engine), table_name)
        cached = self._graph_table_cache.get(cache_key)
        if cached is None:
            cached = self._resolve_graph_table(conn, node, model)
            self._graph_table_cache[cache_key] = cached
        return cached

    def bulk_insert_rows(self, request: ProjectBulkInsertRequest) -> ProjectBulkInsertResult:
        rows = [dict(row) for row in request.rows]
        if not rows:
            return ProjectBulkInsertResult(target_name=request.table_name, rows_written=0)
        table_name = _normalize_identifier(request.table_name, label="table")
        model = self._resolve_cached_model(request.model_path)
        columns = list(request.columns) if request.columns else list(rows[0].keys())
        columns = [_normalize_identifier(column, label="column") for column in columns]
        commit_key = request.commit_key

        with self.get_session() as session:
            conn = session.connection()
            if commit_key is not None and self._check_output_commit(
                conn,
                sink_key=commit_key.sink_key,
                commit_key=commit_key.commit_key,
            ):
                return ProjectBulkInsertResult(
                    target_name=table_name,
                    rows_written=len(rows),
                    deduplicated=True,
                    metadata={
                        "db_path": str(self.db_path) if self.db_path is not None else "",
                        "table": table_name,
                        "rows": len(rows),
                        "deduplicated": True,
                    },
                )

            try:
                if request.write_mode == "upsert":
                    if model is None:
                        raise DataContractError("write_mode='upsert' requires a model_path.")
                    self._execute_upsert(
                        conn,
                        model.__table__,
                        rows,
                        conflict_keys=list(request.conflict_keys) or ["id"],
                        allowed_columns=columns if request.columns else None,
                    )
                else:
                    # Single-pass shaping straight to column-ordered tuples: the
                    # old path built a normalized dict per row and then re-tupled
                    # it (two Python passes over every row for zero gain when
                    # validate_model is False, which is the default and what all
                    # sinks use). _rows_to_tuples does it once.
                    tuple_rows = self._rows_to_tuples(
                        rows=rows,
                        columns=columns,
                        model=model,
                        validate_model=request.validate_model,
                        dialect=conn.dialect,
                    )
                    placeholders = ", ".join("?" for _ in columns)
                    columns_expr = ", ".join(columns)
                    sql = f"INSERT INTO {table_name} ({columns_expr}) VALUES ({placeholders})"
                    conn.exec_driver_sql(sql, tuple_rows)
                if commit_key is not None:
                    self._record_output_commit(
                        conn,
                        sink_key=commit_key.sink_key,
                        commit_key=commit_key.commit_key,
                        table_name=table_name,
                        row_count=len(rows),
                    )
                session.commit()
            except Exception:
                session.rollback()
                raise
        result = {
            "db_path": str(self.db_path) if self.db_path is not None else "",
            "table": table_name,
            "rows": len(rows),
        }
        return ProjectBulkInsertResult(
            target_name=table_name,
            rows_written=int(result.get("rows", 0) or 0),
            deduplicated=bool(result.get("deduplicated", False)),
            metadata=dict(result),
        )

    def insert_graph(self, request: ProjectGraphInsertRequest) -> ProjectGraphInsertResult:
        # Graph inserts need RETURNING to resolve generated ids across relations;
        # flat table writes should use bulk_insert_rows for maximum throughput.
        spec = DbOutputSpec(
            table="",
            db_role="project",
            mode="graph",
            meta=dict(request.graph_meta),
        )
        # Normalize the container to a list per node (cheap: reference copy), but
        # DON'T copy each row dict here — _normalize_graph_payload already makes
        # the one defensive per-row copy right after. Copying twice was pure waste.
        payload = {str(node_name): list(rows) for node_name, rows in request.payload.items()}
        nodes_by_name, relations = self._resolve_cached_graph_spec(spec)
        graph_payload = self._normalize_graph_payload(payload, nodes_by_name)
        total_rows = sum(len(rows) for rows in graph_payload.values())
        if total_rows == 0:
            return ProjectGraphInsertResult(
                rows_written=0,
                nodes_written={},
                metadata={"db_path": str(self.db_path) if self.db_path is not None else ""},
            )

        deferred_relations = [relation for relation in relations if relation.get("deferred")]
        direct_relations = [relation for relation in relations if not relation.get("deferred")]
        relations_by_source: dict[str, list[dict[str, Any]]] = {node_name: [] for node_name in nodes_by_name}
        for relation in direct_relations:
            relations_by_source[str(relation["source_node"])].append(relation)
        deferred_by_source: dict[str, list[dict[str, Any]]] = {node_name: [] for node_name in nodes_by_name}
        for relation in deferred_relations:
            deferred_by_source[str(relation["source_node"])].append(relation)
        # A node only needs its generated ids captured (via RETURNING) when some
        # relation points at it — i.e. a child will resolve its FK from this
        # node's id. Nodes nobody references are leaves: RETURNING there is pure
        # cost (SQLite degrades executemany to per-row when RETURNING is present).
        # The target node of a deferred relation needs ids too: the final UPDATE reads them.
        fk_target_nodes = {str(relation["target_node"]) for relation in relations}
        fk_target_nodes |= {str(relation["source_node"]) for relation in deferred_relations}
        ordered_nodes = self._graph_topological_order(nodes_by_name.keys(), direct_relations)
        commit_key = request.commit_key

        with self.get_session() as session:
            conn = session.connection()
            if (
                commit_key is not None
                and self._check_output_commit(
                    conn,
                    sink_key=commit_key.sink_key,
                    commit_key=commit_key.commit_key,
                )
            ):
                return ProjectGraphInsertResult(
                    rows_written=total_rows,
                    nodes_written={name: len(rows) for name, rows in graph_payload.items()},
                    deduplicated=True,
                    metadata={
                        "db_path": str(self.db_path) if self.db_path is not None else "",
                        "deduplicated": True,
                    },
                )

            try:
                ref_maps: dict[str, dict[str, Any]] = {node_name: {} for node_name in nodes_by_name}
                inserted_counts: dict[str, int] = {}
                pending_deferred: list[tuple[dict[str, Any], str, str]] = []

                for node_name in ordered_nodes:
                    node = nodes_by_name[node_name]
                    rows = graph_payload.get(node_name, [])
                    if not rows:
                        inserted_counts[node_name] = 0
                        continue

                    model_path = str(node.get("model_path") or "")
                    model = self._resolve_cached_model(model_path)
                    table = self._resolve_cached_graph_table(conn, node, model)
                    pk_field = str(node.get("pk_field") or "id")
                    if pk_field not in table.c:
                        raise DataContractError(
                            f"Graph node '{node_name}' primary key field '{pk_field}' not found in table '{table.name}'."
                        )
                    columns = (
                        [str(column) for column in node.get("columns") or ()]
                        if node.get("columns")
                        else self._graph_model_columns(model)
                    )
                    if not columns:
                        raise DataContractError(
                            f"Graph node '{node_name}' requires explicit columns or a model with writable columns."
                        )

                    node_write_mode = str(node.get("write_mode") or "insert").strip().lower()
                    resolved_rows: list[dict[str, Any]] = []
                    row_refs: list[str | None] = []
                    ref_field = str(node.get("ref_field") or "$ref")

                    for raw_row in rows:
                        row_payload = dict(raw_row)
                        row_ref = row_payload.get(ref_field)
                        row_refs.append(None if row_ref is None else str(row_ref))
                        if row_ref is not None and str(row_ref) in ref_maps[node_name]:
                            raise DataContractError(
                                f"Graph node '{node_name}' contains duplicate ref '{row_ref}'."
                            )

                        for relation in relations_by_source[node_name]:
                            fk_field = str(relation["fk_field"])
                            if row_payload.get(fk_field) is not None:
                                continue
                            target_node = str(relation["target_node"])
                            target_ref_field = str(
                                relation.get("target_ref_field")
                                or nodes_by_name[target_node].get("ref_field")
                                or "$ref"
                            )
                            source_ref_field = str(relation["source_ref_field"])
                            target_ref = row_payload.get(source_ref_field)
                            if target_ref is None:
                                continue
                            target_id = ref_maps[target_node].get(str(target_ref))
                            if target_id is None:
                                raise DataContractError(
                                    f"Graph node '{node_name}' references unknown ref '{target_ref}' "
                                    f"from node '{target_node}' via field '{source_ref_field}'."
                                )
                            row_payload[fk_field] = target_id
                            if target_ref_field != source_ref_field and target_ref_field in row_payload:
                                row_payload.setdefault(target_ref_field, target_ref)

                        for relation in deferred_by_source[node_name]:
                            target_ref = row_payload.pop(str(relation["source_ref_field"]), None)
                            if target_ref is not None and row_ref is not None:
                                pending_deferred.append((relation, str(row_ref), str(target_ref)))

                        row_payload.pop(ref_field, None)
                        resolved_rows.append(row_payload)

                    if node_write_mode == "upsert":
                        # Upsert rows carry their own conflict key, so resolve refs to the
                        # key value instead of an auto-assigned id.
                        self._execute_upsert(
                            conn,
                            table,
                            resolved_rows,
                            conflict_keys=list(node.get("conflict_keys") or [pk_field]),
                            allowed_columns=columns,
                        )
                        for row_ref, row_payload in zip(row_refs, resolved_rows):
                            if row_ref is not None and row_payload.get(pk_field) is not None:
                                ref_maps[node_name][row_ref] = row_payload.get(pk_field)
                        inserted_counts[node_name] = len(resolved_rows)
                    elif node_name in fk_target_nodes:
                        # A child resolves its FK from this node's generated id, so
                        # capture ids with RETURNING (slower per-row path on SQLite).
                        write_rows = self._normalize_write_rows(
                            rows=resolved_rows,
                            columns=columns,
                            model=model,
                            validate_model=bool(node.get("validate_model", False)),
                        )
                        insert_stmt = table.insert().returning(table.c[pk_field])
                        inserted_ids = [row[0] for row in conn.execute(insert_stmt, write_rows).all()]
                        if len(inserted_ids) != len(write_rows):
                            raise DataContractError(
                                f"Graph node '{node_name}' insert returned {len(inserted_ids)} ids for {len(write_rows)} rows."
                            )
                        for row_ref, inserted_id in zip(row_refs, inserted_ids):
                            if row_ref is not None:
                                ref_maps[node_name][row_ref] = inserted_id
                        inserted_counts[node_name] = len(write_rows)
                    else:
                        # Leaf node: nobody references its ids, so skip RETURNING
                        # and use the flat executemany path (single-pass tuples).
                        tuple_rows = self._rows_to_tuples(
                            rows=resolved_rows,
                            columns=columns,
                            model=model,
                            validate_model=bool(node.get("validate_model", False)),
                            dialect=conn.dialect,
                        )
                        safe_table = _normalize_identifier(str(table.name), label="table")
                        safe_columns = [_normalize_identifier(str(column), label="column") for column in columns]
                        placeholders = ", ".join("?" for _ in safe_columns)
                        columns_expr = ", ".join(safe_columns)
                        conn.exec_driver_sql(
                            f"INSERT INTO {safe_table} ({columns_expr}) VALUES ({placeholders})",
                            tuple_rows,
                        )
                        inserted_counts[node_name] = len(tuple_rows)

                self._apply_deferred_relations(conn, pending_deferred, nodes_by_name, ref_maps)

                if commit_key is not None:
                    self._record_output_commit(
                        conn,
                        sink_key=commit_key.sink_key,
                        commit_key=commit_key.commit_key,
                        table_name="__graph__",
                        row_count=total_rows,
                    )
                session.commit()
            except Exception:
                session.rollback()
                raise

        result = {
            "db_path": str(self.db_path) if self.db_path is not None else "",
            "nodes": inserted_counts,
            "rows": total_rows,
        }
        return ProjectGraphInsertResult(
            rows_written=int(result.get("rows", 0) or 0),
            nodes_written={str(k): int(v) for k, v in dict(result.get("nodes") or {}).items()},
            deduplicated=bool(result.get("deduplicated", False)),
            metadata=dict(result),
        )

    def has_commit_receipt(self, commit_key: ProjectCommitKey) -> bool:
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
            row = conn.exec_driver_sql(
                f"""
                SELECT 1
                FROM {self._OUTPUT_COMMIT_TABLE}
                WHERE sink_key = ? AND commit_key = ?
                """,
                (commit_key.sink_key, commit_key.commit_key),
            ).first()
            return row is not None

    def record_commit_receipt(self, receipt: ProjectCommitReceipt) -> None:
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
                (
                    receipt.sink_key,
                    receipt.commit_key,
                    receipt.target_name,
                    int(receipt.row_count),
                ),
            )
            session.commit()
