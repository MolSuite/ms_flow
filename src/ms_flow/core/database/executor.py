from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Sequence

from sqlalchemy import inspect
from sqlmodel import SQLModel, delete, select

from ms_flow.core.database.base import BaseSQLiteDB, EXECUTOR_TABLES
from ms_flow.core.database.executor_models import ExecutorJob, ExecutorJobChunk, ExecutorJobEvent

# Local copy of ms_flow.core.executor.utils.TERMINAL_JOB_STATUSES: importing it from here would
# close a cycle (executor imports database).
TERMINAL_JOB_STATUSES = ("canceled", "completed", "failed")
DEFAULT_HISTORY_RETENTION_DAYS = 30.0


class ExecutorStore(BaseSQLiteDB):
    backend_name = "sqlite"

    def _schema_namespace(self) -> str:
        return "executor"

    def _schema_version(self) -> int:
        return 11

    def __init__(self, db_path: Path):
        super().__init__(db_path=db_path, auto_setup=True)

    def _create_tables(self):
        SQLModel.metadata.create_all(self.engine, tables=list(EXECUTOR_TABLES))

    def _migrate_schema(self, current_version: int):
        if current_version not in (0, self._schema_version()):
            raise RuntimeError(
                "Unsupported legacy operational schema. Create a new operational store "
                "for this MolSuite runtime."
            )
        inspector = inspect(self.engine)
        for table in EXECUTOR_TABLES:
            actual_columns = {
                str(column["name"])
                for column in inspector.get_columns(table.name)
            }
            expected_columns = set(table.columns.keys())
            if not expected_columns.issubset(actual_columns):
                raise RuntimeError(
                    "Unsupported legacy operational schema. Create a new operational store "
                    "for this MolSuite runtime."
                )
        return current_version

    def _setup_path_error_message(self) -> str:
        return "Executor DB no configurada."

    def _session_error_message(self) -> str:
        return "Executor DB no configurada."

    @staticmethod
    def _decode_json(raw: str) -> dict[str, Any]:
        payload = (raw or "").strip()
        if not payload or payload == "{}":
            return {}
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            return {"raw": payload}
        return parsed if isinstance(parsed, dict) else {"value": parsed}

    def list_job_events(
        self,
        *,
        job_id: str | None = None,
        after_event_id: int | None = None,
        limit: int | None = None,
        ascending: bool = True,
    ) -> list[dict[str, Any]]:
        statement = select(ExecutorJobEvent)
        if job_id:
            statement = statement.where(ExecutorJobEvent.job_id == job_id)
        if after_event_id is not None:
            statement = statement.where(ExecutorJobEvent.id > int(after_event_id))
        order_expr = ExecutorJobEvent.id.asc() if ascending else ExecutorJobEvent.id.desc()
        statement = statement.order_by(order_expr)
        if limit is not None and limit > 0:
            statement = statement.limit(int(limit))
        with self.get_session() as session:
            rows = session.exec(statement).all()
        if not ascending:
            rows = list(reversed(rows))
        return [
            {
                "event_id": int(row.id or 0),
                "job_id": row.job_id,
                "chunk_id": row.chunk_id,
                "level": row.level,
                "event_type": row.event_type,
                "message": row.message,
                "payload": self._decode_json(row.payload_json),
                "created_at": row.created_at,
            }
            for row in rows
        ]

    def list_job_chunks(
        self,
        *,
        job_id: str | None = None,
        job_ids: Sequence[str] | None = None,
        statuses: Iterable[str] | None = None,
        limit: int | None = None,
        include_payload: bool = False,
    ) -> list[dict[str, Any]]:
        statement = select(ExecutorJobChunk)
        if job_id:
            statement = statement.where(ExecutorJobChunk.job_id == job_id)
        elif job_ids:
            statement = statement.where(ExecutorJobChunk.job_id.in_(list(job_ids)))
        if statuses:
            statement = statement.where(ExecutorJobChunk.status.in_(list(statuses)))
        statement = statement.order_by(ExecutorJobChunk.updated_at.desc(), ExecutorJobChunk.chunk_id.asc())
        if limit is not None and limit > 0:
            statement = statement.limit(int(limit))
        with self.get_session() as session:
            rows = session.exec(statement).all()

        def _to_dict(row: ExecutorJobChunk) -> dict[str, Any]:
            data = {
                "job_id": row.job_id,
                "chunk_id": row.chunk_id,
                "executor_name": row.executor_name,
                "status": row.status,
                "progress": float(row.progress or 0.0),
                "cpu_required": int(row.cpu_required or 0),
                "created_at": row.created_at,
                "updated_at": row.updated_at,
                "started_at": row.started_at,
                "finished_at": row.finished_at,
                "error": row.error or "",
            }
            # payload_json (the chunk input) is only pulled on demand — it can be large, and
            # the hot live-polling path doesn't need it. Job-detail views ask for it to show
            # which input a failed chunk was processing.
            if include_payload:
                data["payload_json"] = row.payload_json or ""
            return data

        return [_to_dict(row) for row in rows]

    def list_job_outputs(
        self,
        job_id: str,
        *,
        limit: int | None = None,
        descending: bool = False,
    ) -> list[dict[str, Any]]:
        statement = (
            select(ExecutorJobChunk)
            .where(
                ExecutorJobChunk.job_id == job_id,
                ExecutorJobChunk.output_json != "",
                ExecutorJobChunk.output_json != "{}",
            )
            .order_by(
                ExecutorJobChunk.finished_at.desc() if descending else ExecutorJobChunk.finished_at.asc(),
                ExecutorJobChunk.updated_at.desc() if descending else ExecutorJobChunk.updated_at.asc(),
            )
        )
        if limit is not None and limit > 0:
            statement = statement.limit(int(limit))
        with self.get_session() as session:
            rows = session.exec(statement).all()

        outputs: list[dict[str, Any]] = []
        for row in rows:
            parsed = self._decode_json(row.output_json)
            if parsed:
                outputs.append(parsed)
        return outputs

    def purge_finished_jobs(
        self, *, older_than_days: float = DEFAULT_HISTORY_RETENTION_DAYS
    ) -> dict[str, int]:
        """Delete chunks and events of jobs that finished more than N days ago.

        Without this executor.db grows without bound: there is one chunk row per unit of work,
        i.e. millions in a screen. The `ExecutorJob` row stays — it is one per job, and it is
        what the user sees as history. `older_than_days <= 0` deletes nothing.
        """
        if older_than_days <= 0:
            return {"chunks": 0, "events": 0}
        cutoff = datetime.now() - timedelta(days=float(older_than_days))
        stale_jobs = (
            select(ExecutorJob.job_id)
            .where(ExecutorJob.status.in_(TERMINAL_JOB_STATUSES))
            .where(ExecutorJob.updated_at < cutoff)
        )
        with self.get_session() as session:
            deleted = {}
            for name, model in (("chunks", ExecutorJobChunk), ("events", ExecutorJobEvent)):
                result = session.exec(
                    delete(model).where(model.job_id.in_(stale_jobs))
                )
                deleted[name] = int(result.rowcount or 0)
            session.commit()
        return deleted


def open_executor_store(location: str | Path, **_options: Any) -> ExecutorStore:
    raw = str(location).strip()
    if raw.startswith(("postgresql://", "postgresql+")):
        raise ValueError("Executor store supports SQLite only.")
    return ExecutorStore(Path(location))


ExecutorDB = ExecutorStore
