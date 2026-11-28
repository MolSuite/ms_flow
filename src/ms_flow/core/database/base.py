from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from sqlalchemy import event
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel, Session

from ms_flow.core.database import project_models  # noqa: F401  registers ms_volumes/ms_payloads
from ms_flow.core.database.master_models import (
    Project,
    ProjectJobIndex,
)

MASTER_TABLES = (
    Project.__table__,
    ProjectJobIndex.__table__,
)
MASTER_TABLE_NAMES = {table.name for table in MASTER_TABLES}
SCHEMA_VERSION_TABLE = "_schema_versions"


def create_sqlite_engine(db_path: Path):
    normalized = Path(db_path).expanduser().resolve()
    normalized.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(
        f"sqlite:///{normalized.as_posix()}",
        connect_args={
            "check_same_thread": False,
            "timeout": 30.0,
        },
    )

    @event.listens_for(engine, "connect")
    def _apply_sqlite_pragmas(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA journal_mode=WAL;")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA busy_timeout=30000")
        finally:
            cursor.close()

    return normalized, engine


_ENGINE_CACHE: dict[str, Any] = {}
_ENGINE_CACHE_LOCK = threading.Lock()


def get_sqlite_engine(db_path: Path):
    """Engine shared per path, with the pragmas of `create_sqlite_engine`.

    For consumers without their own lifecycle (data backends). `BaseSQLiteDB`
    objects still create and dispose of their own via `create_sqlite_engine`.
    """
    key = str(Path(db_path).expanduser().resolve())
    with _ENGINE_CACHE_LOCK:
        engine = _ENGINE_CACHE.get(key)
        if engine is None:
            _, engine = create_sqlite_engine(Path(key))
            _ENGINE_CACHE[key] = engine
        return engine


def dispose_cached_engines() -> None:
    with _ENGINE_CACHE_LOCK:
        for engine in _ENGINE_CACHE.values():
            engine.dispose()
        _ENGINE_CACHE.clear()


def create_session_factory(engine):
    return sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine,
        class_=Session,
    )


PROJECT_SCHEMA = 2  # the project tables as this version writes them; checked on every open


class ProjectSchemaError(RuntimeError):
    """A project database this version cannot open. It was left as it was."""


def create_project_tables(engine, db_path: Path | str | None = None):
    """The project tables, after checking the project's schema version, in one transaction (D5).

    A database without project tables is new: created at `PROJECT_SCHEMA`. One at that version
    gets the tables it lacks. Any other version is refused before anything is written: no
    migration, no table recreated (D5a). SQLite DDL is atomic with this driver only inside a
    `BEGIN` of our own.
    """
    tables = [table for table in SQLModel.metadata.sorted_tables if table.name not in MASTER_TABLE_NAMES]
    with engine.connect() as conn:
        raw = conn.connection.driver_connection
        level = raw.isolation_level
        raw.isolation_level = None  # our BEGIN, so the DDL (and its after_create seeds) is one transaction
        try:
            conn.exec_driver_sql("BEGIN IMMEDIATE")
            try:
                _check_project_schema(conn, db_path)
                SQLModel.metadata.create_all(conn, tables=tables)
                conn.exec_driver_sql(f"CREATE TABLE IF NOT EXISTS {SCHEMA_VERSION_TABLE} (schema_name TEXT PRIMARY KEY, "
                                     "version INTEGER NOT NULL, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")
                conn.exec_driver_sql(f"INSERT INTO {SCHEMA_VERSION_TABLE} (schema_name, version) VALUES ('project', ?) "
                                     "ON CONFLICT(schema_name) DO NOTHING", (PROJECT_SCHEMA,))
            except BaseException:
                raw.execute("ROLLBACK")
                raise
            raw.execute("COMMIT")
        finally:
            raw.isolation_level = level


def _check_project_schema(conn, db_path) -> None:
    names = {row[0] for row in conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE type = 'table'")}
    version = None
    if SCHEMA_VERSION_TABLE in names:
        found = conn.exec_driver_sql(
            f"SELECT version FROM {SCHEMA_VERSION_TABLE} WHERE schema_name = 'project'").fetchone()
        version = int(found[0]) if found is not None else None
    existing = {name for name in names if not name.startswith("sqlite_") and name != SCHEMA_VERSION_TABLE}
    where = Path(db_path).parent if db_path is not None else "this project"
    if version is not None and version > PROJECT_SCHEMA:
        raise ProjectSchemaError(f"{where}: made by a newer version (project schema {version}, "
                                 f"this one reads {PROJECT_SCHEMA}). Open it with that version.")
    if version != PROJECT_SCHEMA and existing:
        raise ProjectSchemaError(f"{where}: made by an older version (project schema {version or 1}, "
                                 f"this one reads {PROJECT_SCHEMA}) and cannot be opened. "
                                 "Make a new project and import its data again.")


class BaseSQLiteDB(ABC):
    """Technical base for SQLite DBs: engine, sessions and lifecycle."""

    def __init__(self, db_path: Path | None = None, *, auto_setup: bool = True):
        self.db_path: Path | None = None
        if db_path is not None:
            self.set_db_path(db_path)
        self.engine = None
        self._session_factory = None
        if auto_setup:
            self.setup()

    def set_db_path(self, db_path: Path | str):
        self.db_path = Path(db_path).expanduser().resolve()

    def setup(self):
        if self.db_path is None:
            raise RuntimeError(self._setup_path_error_message())
        self.db_path, self.engine = create_sqlite_engine(self.db_path)
        self._create_tables()
        with self.engine.begin() as conn:
            self._ensure_schema_version_table(conn)
            current_version = self._get_schema_version(conn, self._schema_namespace())
            applied_version = int(self._migrate_schema(current_version) or current_version)
            target_version = max(applied_version, self._schema_version())
            self._set_schema_version(conn, self._schema_namespace(), target_version)
        self._session_factory = create_session_factory(self.engine)

    @abstractmethod
    def _create_tables(self):
        """Must create the base tables of the concrete DB."""

    def _migrate_schema(self, current_version: int):
        """Optional hook for additive migrations."""

    def _schema_namespace(self) -> str:
        return self.__class__.__name__.lower()

    def _schema_version(self) -> int:
        return 1

    @staticmethod
    def _ensure_schema_version_table(conn):
        conn.exec_driver_sql(
            f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_VERSION_TABLE} (
                schema_name TEXT PRIMARY KEY,
                version INTEGER NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """
        )

    @staticmethod
    def _get_schema_version(conn, schema_name: str) -> int:
        row = conn.exec_driver_sql(
            f"SELECT version FROM {SCHEMA_VERSION_TABLE} WHERE schema_name = ?;",
            (schema_name,),
        ).fetchone()
        return int(row[0]) if row is not None else 0

    @staticmethod
    def _set_schema_version(conn, schema_name: str, version: int):
        conn.exec_driver_sql(
            f"""
            INSERT INTO {SCHEMA_VERSION_TABLE} (schema_name, version, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(schema_name) DO UPDATE SET
                version=excluded.version,
                updated_at=CURRENT_TIMESTAMP;
            """,
            (schema_name, int(version)),
        )

    @staticmethod
    def _existing_columns(conn, table_name: str) -> set[str]:
        return {row[1] for row in conn.exec_driver_sql(f"PRAGMA table_info({table_name});").fetchall()}

    @staticmethod
    def _table_exists(conn, table_name: str) -> bool:
        row = conn.exec_driver_sql(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1;",
            (str(table_name),),
        ).fetchone()
        return row is not None

    @staticmethod
    def _ensure_columns(conn, table_name: str, columns: dict[str, str]):
        existing_columns = BaseSQLiteDB._existing_columns(conn, table_name)
        for column_name, column_type in columns.items():
            if column_name in existing_columns:
                continue
            conn.exec_driver_sql(
                f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type};"
            )

    def _setup_path_error_message(self) -> str:
        return "DB not configured: path missing."

    def _session_error_message(self) -> str:
        return "DB not configured."

    def get_session(self) -> Session:
        if self._session_factory is None:
            raise RuntimeError(self._session_error_message())
        return self._session_factory()

    def reconnect(self):
        if self.db_path is None:
            raise RuntimeError(self._setup_path_error_message())
        self.dispose()
        self.setup()

    def dispose(self):
        if self.engine is not None:
            self.engine.dispose()
            self.engine = None
        self._session_factory = None


class ProjectTablesDB(BaseSQLiteDB):
    """A handle on a project database. Every one shares the namespace and the version: a class
    of its own would read version 0 of the same file."""

    def _create_tables(self):
        try:
            create_project_tables(self.engine, self.db_path)
        except ProjectSchemaError:
            self.engine.dispose()
            raise

    def _schema_namespace(self) -> str:
        return "project"

    def _schema_version(self) -> int:
        return PROJECT_SCHEMA
