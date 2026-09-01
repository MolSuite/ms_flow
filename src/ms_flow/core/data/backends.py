from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from sqlalchemy import MetaData

from ms_flow.core.data.contracts import (
    BytesInputSpec,
    DataContractError,
    DbInputSpec,
    DbOutputSpec,
    FileInputSpec,
    FileOutputSpec,
    InlineInputSpec,
    InputSpec,
    OutputSpec,
)
from ms_flow.core.data.runtime import DataContext
from ms_flow.core.database.base import get_sqlite_engine
from ms_flow.core.database.sqlite_utils import compile_select


class InputBackendBase:
    source: str = ""

    def read(self, spec: InputSpec, context: DataContext) -> Any:
        raise NotImplementedError


class OutputBackendBase:
    target: str = ""

    def write(self, spec: OutputSpec, data: Any, context: DataContext) -> dict[str, Any]:
        raise NotImplementedError


class InlineInputBackend(InputBackendBase):
    source = "inline"

    def read(self, spec: InputSpec, context: DataContext) -> Any:
        del context
        if not isinstance(spec, InlineInputSpec):
            raise DataContractError("InlineInputBackend requires InlineInputSpec.")
        return spec.payload


class BytesInputBackend(InputBackendBase):
    source = "bytes"

    def read(self, spec: InputSpec, context: DataContext) -> Any:
        del context
        if not isinstance(spec, BytesInputSpec):
            raise DataContractError("BytesInputBackend requires BytesInputSpec.")
        return spec.payload


class LocalFileBackend(InputBackendBase, OutputBackendBase):
    source = "file"
    target = "file"

    @staticmethod
    def resolve_path(path: str, root: str, context: DataContext) -> Path:
        raw = Path(path).expanduser()
        if raw.is_absolute():
            return raw.resolve()

        root_key = str(root or "").strip().lower()
        if root_key == "project":
            if context.project_dir is None:
                raise DataContractError("FileInputSpec root='project' requires project_dir in DataContext.")
            return (context.project_dir / raw).resolve()
        if root_key == "executor":
            if context.executor_db_path is None:
                raise DataContractError("FileInputSpec root='executor' requires executor_db_path in DataContext.")
            return (context.executor_db_path.parent / raw).resolve()
        if context.project_dir is not None:
            return (context.project_dir / raw).resolve()
        return raw.resolve()

    def read(self, spec: InputSpec, context: DataContext) -> Any:
        if not isinstance(spec, FileInputSpec):
            raise DataContractError("LocalFileBackend requires FileInputSpec for reads.")
        target = self.resolve_path(spec.path, spec.root, context)
        if spec.fmt == "binary":
            return target.read_bytes()
        if spec.fmt == "text":
            return target.read_text(encoding=spec.encoding)
        return json.loads(target.read_text(encoding=spec.encoding))

    def write(self, spec: OutputSpec, data: Any, context: DataContext) -> dict[str, Any]:
        if not isinstance(spec, FileOutputSpec):
            raise DataContractError("LocalFileBackend requires FileOutputSpec for writes.")
        target = self.resolve_path(spec.path, spec.root, context)
        if spec.ensure_parent:
            target.parent.mkdir(parents=True, exist_ok=True)

        append = bool(getattr(spec, "append", False))
        if spec.fmt == "binary":
            if isinstance(data, str):
                payload = data.encode(spec.encoding)
            elif isinstance(data, bytes):
                payload = data
            else:
                payload = json.dumps(data, ensure_ascii=False).encode(spec.encoding)
            with open(target, "ab" if append else "wb") as handle:
                handle.write(payload)
            written = len(payload)
        elif spec.fmt == "text":
            text = data if isinstance(data, str) else json.dumps(data, ensure_ascii=False)
            with open(target, "a" if append else "w", encoding=spec.encoding) as handle:
                handle.write(text)
            written = len(text.encode(spec.encoding))
        else:
            target.write_text(json.dumps(data, ensure_ascii=False), encoding=spec.encoding)
            written = target.stat().st_size if target.exists() else 0

        return {"path": str(target), "bytes": int(written)}


class SQLiteBackend(InputBackendBase, OutputBackendBase):
    source = "db"
    target = "db"
    _OUTPUT_COMMIT_TABLE = "molsuite_output_commits"

    @staticmethod
    def _resolve_db_path(role: str, db_path: str, context: DataContext) -> Path:
        role_value = str(role or "project").strip().lower()
        if role_value == "custom":
            if not db_path:
                raise DataContractError("db_role='custom' requires db_path.")
            return Path(db_path).expanduser().resolve()
        if role_value == "project":
            if context.project_db_path is None:
                raise DataContractError("db_role='project' requires project_db_path in DataContext.")
            return context.project_db_path
        if role_value == "executor":
            if context.executor_db_path is None:
                raise DataContractError("db_role='executor' requires executor_db_path in DataContext.")
            return context.executor_db_path
        raise DataContractError(f"Unsupported db_role '{role}'.")

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
        normalized: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            if model is not None and validate_model:
                payload = model.model_validate(payload).model_dump(mode="python")
            normalized.append({column: payload.get(column) for column in columns})
        return normalized

    @staticmethod
    def _execute_upsert(conn, table, rows: list[dict[str, Any]], *, conflict_keys: list[str], allowed_columns=None) -> int:
        """INSERT ... ON CONFLICT(conflict_keys) DO UPDATE, with a dynamic SET per payload.

        Rows are grouped by the columns they actually carry so each row only updates
        the columns it provides (no NULL clobber of columns it omits). `allowed_columns`,
        when given, restricts which columns are written. Requires `conflict_keys` to be a
        PK or UNIQUE constraint on the table.
        """
        from collections import OrderedDict
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

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

    def _graph_model_columns(self, model) -> list[str]:
        if not hasattr(model, "__table__"):
            raise DataContractError(f"Graph model {model!r} does not define a SQL table.")
        columns = [column.name for column in model.__table__.columns if not column.primary_key]
        if not columns:
            raise DataContractError(f"Graph model {model!r} does not expose writable columns.")
        return columns

    def _resolve_graph_table(self, conn, node: Mapping[str, Any], model):
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
        node_names: Iterable[str],
        relations: Iterable[Mapping[str, Any]],
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
            relations.append(
                {
                    "source_node": source_node,
                    "source_ref_field": source_ref_field,
                    "target_node": target_node,
                    "fk_field": fk_field,
                    "target_ref_field": target_ref_field,
                    "deferred": bool(relation.get("deferred", False)),
                }
            )
        return nodes_by_name, relations

    def _normalize_graph_payload(
        self,
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

    def _write_graph(self, spec: DbOutputSpec, data: Any, context: DataContext, db_path: Path) -> dict[str, Any]:
        nodes_by_name, relations = self._graph_spec_parts(spec)
        if any(relation.get("deferred") for relation in relations):
            # Only the ProjectStore resolves deferred relations (UPDATE at the end of the commit).
            # Here they would be silently ignored and the FK would stay NULL: better to fail.
            raise DataContractError(
                "Deferred graph relations are only supported by project-role graph sinks."
            )
        graph_payload = self._normalize_graph_payload(data, nodes_by_name)
        relations_by_source: dict[str, list[dict[str, Any]]] = {node_name: [] for node_name in nodes_by_name}
        for relation in relations:
            relations_by_source[str(relation["source_node"])].append(relation)
        ordered_nodes = self._graph_topological_order(nodes_by_name.keys(), relations)
        commit_key = str(context.extras.get("molsuite_output_commit_key") or "").strip()
        sink_key = str(context.extras.get("molsuite_output_sink_key") or "").strip()
        total_rows = sum(len(rows) for rows in graph_payload.values())
        if total_rows == 0:
            return {"db_path": str(db_path), "nodes": {}, "rows": 0}

        engine = get_sqlite_engine(db_path)
        with engine.begin() as conn:
            if commit_key and sink_key and self._check_output_commit(
                conn,
                sink_key=sink_key,
                commit_key=commit_key,
            ):
                return {
                    "db_path": str(db_path),
                    "nodes": {name: len(rows) for name, rows in graph_payload.items()},
                    "rows": total_rows,
                    "deduplicated": True,
                }

            ref_maps: dict[str, dict[str, Any]] = {node_name: {} for node_name in nodes_by_name}
            inserted_counts: dict[str, int] = {}
            for node_name in ordered_nodes:
                node = nodes_by_name[node_name]
                rows = graph_payload.get(node_name, [])
                if not rows:
                    inserted_counts[node_name] = 0
                    continue

                model_path = str(node.get("model_path") or "")
                model = self._resolve_model(model_path) if model_path else None
                table = self._resolve_graph_table(conn, node, model)
                pk_field = str(node.get("pk_field") or "id")
                if pk_field not in table.c:
                    raise DataContractError(
                        f"Graph node '{node_name}' primary key field '{pk_field}' not found in table '{table.name}'."
                    )
                columns = [str(column) for column in node.get("columns") or ()] if node.get("columns") else self._graph_model_columns(model)
                if not columns:
                    raise DataContractError(
                        f"Graph node '{node_name}' requires explicit columns or a model with writable columns."
                    )
                node_write_mode = str(node.get("write_mode") or "insert").strip().lower()
                resolved_rows: list[dict[str, Any]] = []
                row_refs: list[str | None] = []
                for raw_row in rows:
                    payload = dict(raw_row)
                    ref_field = str(node.get("ref_field") or "$ref")
                    row_ref = payload.get(ref_field)
                    row_refs.append(None if row_ref is None else str(row_ref))
                    if row_ref is not None and str(row_ref) in ref_maps[node_name]:
                        raise DataContractError(
                            f"Graph node '{node_name}' contains duplicate ref '{row_ref}'."
                        )
                    for relation in relations_by_source[node_name]:
                        fk_field = str(relation["fk_field"])
                        if payload.get(fk_field) is not None:
                            continue
                        target_node = str(relation["target_node"])
                        target_ref = payload.get(str(relation["source_ref_field"]))
                        if target_ref is None:
                            continue
                        target_id = ref_maps[target_node].get(str(target_ref))
                        if target_id is None:
                            raise DataContractError(
                                f"Graph node '{node_name}' references unknown ref '{target_ref}' "
                                f"from node '{target_node}' via field '{relation['source_ref_field']}'."
                            )
                        payload[fk_field] = target_id
                    payload.pop(ref_field, None)
                    resolved_rows.append(payload)

                if node_write_mode == "upsert":
                    # Upsert rows carry their own primary/conflict key, so resolve refs
                    # to the key value instead of an auto-assigned id.
                    self._execute_upsert(
                        conn,
                        table,
                        resolved_rows,
                        conflict_keys=list(node.get("conflict_keys") or [pk_field]),
                        allowed_columns=columns,
                    )
                    for row_ref, payload in zip(row_refs, resolved_rows):
                        if row_ref is not None and payload.get(pk_field) is not None:
                            ref_maps[node_name][row_ref] = payload.get(pk_field)
                    inserted_counts[node_name] = len(resolved_rows)
                else:
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
                        if row_ref is None:
                            continue
                        ref_maps[node_name][row_ref] = inserted_id
                    inserted_counts[node_name] = len(write_rows)

            if commit_key and sink_key:
                self._record_output_commit(
                    conn,
                    sink_key=sink_key,
                    commit_key=commit_key,
                    table_name="__graph__",
                    row_count=total_rows,
                )
        return {
            "db_path": str(db_path),
            "nodes": inserted_counts,
            "rows": total_rows,
        }

    def output_commits_exist(
        self, spec: DbOutputSpec, context: DataContext, commit_keys: list[str]
    ) -> set[str]:
        """Return which of the given commit keys are already recorded for this sink.

        Recovery probe: lets a result handler skip re-persisting chunks that
        were already committed (e.g. under a legacy per-chunk commit key)
        before a restart. Best-effort: any failure reports no matches.
        """
        sink_key = str(context.extras.get("molsuite_output_sink_key") or "").strip()
        keys = [str(key) for key in commit_keys if str(key or "").strip()]
        if not sink_key or not keys:
            return set()
        if str(spec.db_role or "").strip().lower() == "project":
            try:
                from ms_flow.core.database import ProjectStore

                if context.project_db_path is None:
                    return set()
                store = ProjectStore.open_cached(context.project_db_path)
                return store.output_commits_exist(sink_key, keys)
            except Exception:
                return set()
        try:
            db_path = self._resolve_db_path(spec.db_role, spec.db_path, context)
        except DataContractError:
            return set()
        if not Path(db_path).exists():
            return set()
        try:
            with get_sqlite_engine(db_path).begin() as conn:
                placeholders = ",".join("?" for _ in keys)
                rows = conn.exec_driver_sql(
                    f"SELECT commit_key FROM {self._OUTPUT_COMMIT_TABLE} "
                    f"WHERE sink_key = ? AND commit_key IN ({placeholders})",
                    (sink_key, *keys),
                ).all()
            return {str(row[0]) for row in rows}
        except Exception:
            return set()

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

    def read(self, spec: InputSpec, context: DataContext) -> Any:
        if not isinstance(spec, DbInputSpec):
            raise DataContractError("SQLiteBackend requires DbInputSpec for reads.")
        db_path = self._resolve_db_path(spec.db_role, spec.db_path, context)
        sql, params = compile_select(
            table=spec.table,
            fields=spec.columns,
            filters=spec.where,
            order=spec.order,
            limit=spec.limit,
            offset=spec.offset,
            query=spec.query,
            params=spec.params,
        )
        with get_sqlite_engine(db_path).connect() as conn:
            result = conn.exec_driver_sql(sql, params)
            return [dict(row) for row in result.mappings()]

    def write(self, spec: OutputSpec, data: Any, context: DataContext) -> dict[str, Any]:
        if isinstance(spec, DbOutputSpec) and str(spec.db_role or "").strip().lower() == "project":
            from ms_flow.core.data.project_output import persist_project_output

            return persist_project_output(spec, data, context)
        if isinstance(spec, DbOutputSpec) and spec.mode == "graph":
            db_path = self._resolve_db_path(spec.db_role, spec.db_path, context)
            return self._write_graph(spec, data, context, db_path)
        if not isinstance(spec, DbOutputSpec):
            raise DataContractError("SQLiteBackend requires DbOutputSpec for writes.")
        db_path = self._resolve_db_path(spec.db_role, spec.db_path, context)
        if isinstance(data, dict):
            rows = [data]
        elif isinstance(data, list):
            rows = [dict(item) for item in data]
        else:
            raise DataContractError("DbOutputSpec writes require dict or list[dict] payload.")
        if not rows:
            return {"db_path": str(db_path), "table": spec.table, "rows": 0}

        columns = list(spec.columns) if spec.columns else list(rows[0].keys())
        commit_key = str(context.extras.get("molsuite_output_commit_key") or "").strip()
        sink_key = str(context.extras.get("molsuite_output_sink_key") or "").strip()
        model = self._resolve_model(spec.model_path) if spec.model_path else None
        write_rows = self._normalize_write_rows(
            rows=rows,
            columns=columns,
            model=model,
            validate_model=spec.validate_model,
        )

        if spec.write_mode == "upsert":
            if model is None:
                raise DataContractError("write_mode='upsert' requires a model (model_path).")
            conflict_keys = list(spec.conflict_keys) or ["id"]
            engine = get_sqlite_engine(db_path)
            with engine.begin() as conn:
                self._execute_upsert(
                    conn,
                    model.__table__,
                    rows,
                    conflict_keys=conflict_keys,
                    allowed_columns=list(columns) if spec.columns else None,
                )
            return {"db_path": str(db_path), "table": spec.table, "rows": len(rows)}

        if spec.write_mode == "bulk" and model is not None:
            engine = get_sqlite_engine(db_path)
            with engine.begin() as conn:
                if commit_key and sink_key and self._check_output_commit(
                    conn,
                    sink_key=sink_key,
                    commit_key=commit_key,
                ):
                    return {
                        "db_path": str(db_path),
                        "table": spec.table,
                        "rows": len(write_rows),
                        "deduplicated": True,
                    }
                conn.execute(model.__table__.insert(), write_rows)
                if commit_key and sink_key:
                    self._record_output_commit(
                        conn,
                        sink_key=sink_key,
                        commit_key=commit_key,
                        table_name=spec.table,
                        row_count=len(write_rows),
                    )
            return {"db_path": str(db_path), "table": spec.table, "rows": len(write_rows)}

        placeholders = ", ".join(["?"] * len(columns))
        columns_expr = ", ".join(columns)
        sql = f"INSERT INTO {spec.table} ({columns_expr}) VALUES ({placeholders})"
        with get_sqlite_engine(db_path).begin() as conn:
            if commit_key and sink_key and self._check_output_commit(
                conn,
                sink_key=sink_key,
                commit_key=commit_key,
            ):
                return {
                    "db_path": str(db_path),
                    "table": spec.table,
                    "rows": len(write_rows),
                    "deduplicated": True,
                }
            values = [tuple(row.get(col) for col in columns) for row in write_rows]
            if spec.write_mode == "row":
                for value in values:
                    conn.exec_driver_sql(sql, value)
            elif values:
                conn.exec_driver_sql(sql, values)
            if commit_key and sink_key:
                self._record_output_commit(
                    conn,
                    sink_key=sink_key,
                    commit_key=commit_key,
                    table_name=spec.table,
                    row_count=len(write_rows),
                )
        return {"db_path": str(db_path), "table": spec.table, "rows": len(write_rows)}


def build_default_input_backends() -> dict[str, InputBackendBase]:
    file_backend = LocalFileBackend()
    sqlite_backend = SQLiteBackend()
    return {
        "inline": InlineInputBackend(),
        "bytes": BytesInputBackend(),
        "file": file_backend,
        "db": sqlite_backend,
    }


def build_default_output_backends() -> dict[str, OutputBackendBase]:
    file_backend = LocalFileBackend()
    sqlite_backend = SQLiteBackend()
    return {
        "file": file_backend,
        "db": sqlite_backend,
        "graph": sqlite_backend,
    }
