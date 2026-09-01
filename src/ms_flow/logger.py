import logging
import queue
import threading
import time
from collections import deque
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from pathlib import Path
from typing import Deque, Optional, Sequence, Union

DEFAULT_MAX_LOG_SIZE = 10 * 1024 * 1024  # 10 MB
DEFAULT_BACKUP_COUNT = 10
DEFAULT_RETENTION_DAYS = 30


class ContextAwareFormatter(logging.Formatter):
    """Formatter that appends correlation identifiers when present."""

    def format(self, record: logging.LogRecord) -> str:
        rendered = super().format(record)
        parts = []
        for field in ("project_id", "job_id", "chunk_id"):
            value = getattr(record, field, "")
            if value not in ("", None):
                parts.append(f"{field}={value}")
        if not parts:
            return rendered
        return f"{rendered} [{' '.join(parts)}]"


class PrefixFilter(logging.Filter):
    """Accept records whose logger name starts with any configured prefix."""

    def __init__(self, prefixes: Sequence[str]):
        super().__init__()
        self._prefixes = tuple(prefixes)

    def filter(self, record: logging.LogRecord) -> bool:
        return any(record.name.startswith(prefix) for prefix in self._prefixes)


class ExcludePrefixFilter(logging.Filter):
    """Reject records whose logger name starts with any configured prefix."""

    def __init__(self, excluded_prefixes: Sequence[str]):
        super().__init__()
        self._excluded_prefixes = tuple(excluded_prefixes)

    def filter(self, record: logging.LogRecord) -> bool:
        return not any(record.name.startswith(prefix) for prefix in self._excluded_prefixes)


class CircularBufferLogHandler(logging.Handler):
    def __init__(self, buffer: Deque[str]):
        super().__init__()
        self.buffer = buffer

    def emit(self, record: logging.LogRecord) -> None:
        self.buffer.append(self.format(record))


class LoggingManager:
    """Async logging manager with app/executor/project channels."""

    def __init__(
        self,
        global_log_dir: Path,
        max_bytes: int = DEFAULT_MAX_LOG_SIZE,
        backup_count: int = DEFAULT_BACKUP_COUNT,
        retention_days: int = DEFAULT_RETENTION_DAYS,
        queue_size: int = 10000,
        app_level: Union[int, str] = logging.INFO,
        executor_level: Union[int, str] = logging.INFO,
        project_level: Union[int, str] = logging.INFO,
        console_level: Union[int, str] = logging.INFO,
        root_namespace: str = "molsuite",
    ):
        self.global_log_dir = Path(global_log_dir).expanduser().resolve()
        self.root_namespace = str(root_namespace).strip() or "molsuite"
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.retention_days = retention_days
        self.app_level = self._resolve_level(app_level)
        self.executor_level = self._resolve_level(executor_level)
        self.project_level = self._resolve_level(project_level)
        self.console_level = self._resolve_level(console_level)

        if queue_size <= 0:
            self._queue = queue.Queue()
        else:
            self._queue = queue.Queue(maxsize=queue_size)
        self._queue_handler = QueueHandler(self._queue)
        self._listener: Optional[QueueListener] = None

        self.log_buffer: Deque[str] = deque(maxlen=200)
        self._buffer_handler = CircularBufferLogHandler(self.log_buffer)
        self._console_handler: Optional[logging.Handler] = None
        self._app_handler: Optional[logging.Handler] = None
        self._executor_handler: Optional[logging.Handler] = None
        self._project_handler: Optional[logging.Handler] = None
        self._project_log_path: Optional[Path] = None

        self._started = False
        self._lock = threading.RLock()

    @staticmethod
    def _resolve_level(level: Union[int, str]) -> int:
        if isinstance(level, int):
            return level
        normalized = str(level).upper().strip()
        if normalized not in logging._nameToLevel:
            raise ValueError(f"Invalid log level: {level}")
        return int(logging._nameToLevel[normalized])

    @staticmethod
    def _build_formatter() -> logging.Formatter:
        return ContextAwareFormatter(
            "[{asctime}] [{levelname:^8s}] {name}: {message}",
            style="{",
        )

    def _create_rotating_handler(self, path: Path, level: int) -> RotatingFileHandler:
        path.parent.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(path, maxBytes=self.max_bytes, backupCount=self.backup_count)
        handler.setLevel(level)
        handler.setFormatter(self._build_formatter())
        return handler

    def _base_logger(self) -> logging.Logger:
        logger = logging.getLogger(self.root_namespace)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        return logger

    def _configure_core_handlers(self):
        self.global_log_dir.mkdir(parents=True, exist_ok=True)

        app_log_path = self.global_log_dir / "app.log"
        executor_log_path = self.global_log_dir / "executor.log"

        self._app_handler = self._create_rotating_handler(app_log_path, self.app_level)
        self._app_handler.addFilter(
            ExcludePrefixFilter(
                (
                    f"{self.root_namespace}.executor",
                    f"{self.root_namespace}.project",
                )
            )
        )

        self._executor_handler = self._create_rotating_handler(executor_log_path, self.executor_level)
        self._executor_handler.addFilter(PrefixFilter((f"{self.root_namespace}.executor",)))

        self._console_handler = logging.StreamHandler()
        self._console_handler.setLevel(self.console_level)
        self._console_handler.setFormatter(self._build_formatter())

        self._buffer_handler.setLevel(logging.DEBUG)
        self._buffer_handler.setFormatter(self._build_formatter())

    def _active_handlers(self) -> list[logging.Handler]:
        handlers = []
        if self._app_handler is not None:
            handlers.append(self._app_handler)
        if self._executor_handler is not None:
            handlers.append(self._executor_handler)
        if self._project_handler is not None:
            handlers.append(self._project_handler)
        if self._console_handler is not None:
            handlers.append(self._console_handler)
        handlers.append(self._buffer_handler)
        return handlers

    def _restart_listener(self):
        if self._listener is not None:
            self._listener.stop()

        handlers = self._active_handlers()
        self._listener = QueueListener(self._queue, *handlers, respect_handler_level=True)
        if self._started:
            self._listener.start()

    def start(self):
        with self._lock:
            if self._started:
                return

            self._configure_core_handlers()
            base = self._base_logger()
            if self._queue_handler not in base.handlers:
                base.addHandler(self._queue_handler)

            self._started = True
            self._restart_listener()
            self.cleanup_old_logs()
            self.get_app_logger("lifecycle").info("Logging manager started")

    def stop(self):
        with self._lock:
            if not self._started:
                return

            self.get_app_logger("lifecycle").info("Logging manager stopping")
            self._started = False
            if self._listener is not None:
                self._listener.stop()
                self._listener = None

            base = self._base_logger()
            if self._queue_handler in base.handlers:
                base.removeHandler(self._queue_handler)

            for handler in self._active_handlers():
                try:
                    handler.flush()
                except Exception:
                    pass
                try:
                    handler.close()
                except Exception:
                    pass

            self._app_handler = None
            self._executor_handler = None
            self._project_handler = None
            self._console_handler = None
            self._project_log_path = None

    def set_project_logging(self, project_dir: Path):
        with self._lock:
            project_path = Path(project_dir).expanduser().resolve()
            logs_dir = project_path / "logs"
            log_path = logs_dir / "project.log"

            if self._project_handler is not None:
                try:
                    self._project_handler.close()
                except Exception:
                    pass
                self._project_handler = None

            self._project_handler = self._create_rotating_handler(log_path, self.project_level)
            self._project_handler.addFilter(PrefixFilter((f"{self.root_namespace}.project",)))
            self._project_log_path = log_path

            if self._started:
                self._restart_listener()
            self.cleanup_old_logs()

    def clear_project_logging(self):
        with self._lock:
            if self._project_handler is None:
                self._project_log_path = None
                return

            try:
                self._project_handler.close()
            except Exception:
                pass
            self._project_handler = None
            self._project_log_path = None

            if self._started:
                self._restart_listener()

    def cleanup_old_logs(self, retention_days: Optional[int] = None) -> int:
        days = self.retention_days if retention_days is None else retention_days
        if days <= 0:
            return 0

        cutoff_ts = time.time() - (days * 24 * 60 * 60)
        removed = 0

        candidate_dirs = [self.global_log_dir]
        if self._project_log_path is not None:
            candidate_dirs.append(self._project_log_path.parent)

        visited_dirs = set()
        for directory in candidate_dirs:
            if directory in visited_dirs:
                continue
            visited_dirs.add(directory)
            if not directory.exists():
                continue

            for entry in directory.glob("*.log*"):
                if not entry.is_file():
                    continue
                try:
                    if entry.stat().st_mtime < cutoff_ts:
                        entry.unlink()
                        removed += 1
                except FileNotFoundError:
                    continue
                except OSError:
                    continue
        return removed

    def get_app_logger(self, name: str = "app") -> logging.Logger:
        if name.startswith(f"{self.root_namespace}."):
            return logging.getLogger(name)
        return logging.getLogger(f"{self.root_namespace}.app.{name}")

    def get_executor_logger(self, name: str = "executor") -> logging.Logger:
        if name.startswith(f"{self.root_namespace}."):
            return logging.getLogger(name)
        return logging.getLogger(f"{self.root_namespace}.executor.{name}")

    def get_project_logger(self, name: str = "project") -> logging.Logger:
        if name.startswith(f"{self.root_namespace}."):
            return logging.getLogger(name)
        return logging.getLogger(f"{self.root_namespace}.project.{name}")

    @property
    def project_log_path(self) -> Optional[Path]:
        return self._project_log_path

