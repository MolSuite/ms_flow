from __future__ import annotations

from pathlib import Path

from sqlmodel import SQLModel

from ms_flow.core.database.base import BaseSQLiteDB, MASTER_TABLES
from ms_flow.core.database.master_models import ProjectJobIndex


class MasterDB(BaseSQLiteDB):
    def _schema_namespace(self) -> str:
        return "master"

    def _schema_version(self) -> int:
        return 6

    def __init__(self, db_path: Path):
        super().__init__(db_path=db_path, auto_setup=True)

    def _create_tables(self):
        SQLModel.metadata.create_all(self.engine, tables=list(MASTER_TABLES))

    def _migrate_schema(self, current_version: int):
        with self.engine.begin() as conn:
            if current_version < 2:
                self._ensure_columns(
                    conn,
                    "projects",
                    {
                        "tags": "VARCHAR DEFAULT ''",
                        "favorite": "BOOLEAN DEFAULT 0",
                        "scope": "VARCHAR DEFAULT 'full'",
                        "app_id": "VARCHAR DEFAULT ''",
                    },
                )
                conn.exec_driver_sql(
                    "UPDATE projects SET scope='full' WHERE scope IS NULL OR TRIM(scope)='';"
                )
                current_version = 2
            if current_version < 3:
                has_project_job_index = conn.exec_driver_sql(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='project_job_index';"
                ).fetchone()
                if has_project_job_index is not None:
                    self._ensure_columns(
                        conn,
                        "project_job_index",
                        {
                            "origin_id": "VARCHAR DEFAULT ''",
                        },
                    )
                    columns = self._existing_columns(conn, "project_job_index")
                    if "plugin_id" in columns:
                        # Migration compatibility: old builds persisted `plugin_id`
                        # where the modern model uses `origin_id`.
                        conn.exec_driver_sql(
                            """
                            UPDATE project_job_index
                            SET origin_id = COALESCE(NULLIF(TRIM(plugin_id), ''), origin_id, '')
                            WHERE origin_id IS NULL OR TRIM(origin_id) = '';
                            """
                        )
                current_version = 3
            if current_version < 4:
                has_project_job_index = conn.exec_driver_sql(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='project_job_index';"
                ).fetchone()
                if has_project_job_index is not None:
                    self._ensure_columns(
                        conn,
                        "project_job_index",
                        {
                            "origin_id": "VARCHAR DEFAULT ''",
                        },
                    )
                current_version = 4
            if current_version < 5:
                def _table_exists(table_name: str) -> bool:
                    return conn.exec_driver_sql(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
                        (table_name,),
                    ).fetchone() is not None

                rows_by_job_id: dict[str, dict] = {}

                if _table_exists("project_job_index"):
                    for row in conn.exec_driver_sql(
                        """
                        SELECT id, project_id, job_id, origin_id, task_type, status, progress, created_at, updated_at
                        FROM project_job_index;
                        """
                    ).mappings():
                        rows_by_job_id[str(row["job_id"])] = dict(row)

                if _table_exists("project_job_index_legacy"):
                    legacy_columns = self._existing_columns(conn, "project_job_index_legacy")
                    # Migration compatibility: accept both `origin_id` and `plugin_id`
                    # while the pre-v5 legacy table still exists.
                    origin_expr = (
                        "COALESCE(NULLIF(TRIM(origin_id), ''), NULLIF(TRIM(plugin_id), ''), '')"
                        if "origin_id" in legacy_columns and "plugin_id" in legacy_columns
                        else "COALESCE(NULLIF(TRIM(plugin_id), ''), '')"
                        if "plugin_id" in legacy_columns
                        else "COALESCE(NULLIF(TRIM(origin_id), ''), '')"
                    )
                    for row in conn.exec_driver_sql(
                        f"""
                        SELECT id, project_id, job_id, {origin_expr} AS origin_id, task_type, status, progress, created_at, updated_at
                        FROM project_job_index_legacy;
                        """
                    ).mappings():
                        rows_by_job_id.setdefault(str(row["job_id"]), dict(row))

                if _table_exists("project_job_index"):
                    conn.exec_driver_sql("DROP TABLE project_job_index;")
                if _table_exists("project_job_index_legacy"):
                    conn.exec_driver_sql("DROP TABLE project_job_index_legacy;")

                ProjectJobIndex.__table__.create(conn)
                for row in rows_by_job_id.values():
                    conn.exec_driver_sql(
                        """
                        INSERT INTO project_job_index (
                            id,
                            project_id,
                            job_id,
                            origin_id,
                            task_type,
                            status,
                            progress,
                            created_at,
                            updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        (
                            row["id"],
                            row["project_id"],
                            row["job_id"],
                            row["origin_id"],
                            row["task_type"],
                            row["status"],
                            row["progress"],
                            row["created_at"],
                            row["updated_at"],
                        ),
                    )
                current_version = 5
            if current_version < 6:
                has_project_job_index = conn.exec_driver_sql(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='project_job_index';"
                ).fetchone()
                if has_project_job_index is not None:
                    self._ensure_columns(
                        conn,
                        "project_job_index",
                        {
                            "scheduler_reason": "VARCHAR DEFAULT ''",
                        },
                    )
                current_version = 6
        return current_version

    def _setup_path_error_message(self) -> str:
        return "Master DB no configurada."

    def _session_error_message(self) -> str:
        return "Master DB no configurada."
