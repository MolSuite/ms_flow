from __future__ import annotations

import contextlib
import json
import logging
import os
import subprocess
import uuid
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union
from uuid import UUID

import toml
from pydantic import BaseModel, Field, IPvAnyAddress, field_serializer, field_validator, model_validator

_HARDCODED_LOCAL_CPU_LIMIT = 14

# Categorical values are declared, not just validated: the settings UI turns any
# Literal into a combo box instead of a free-text field.
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
_LEVEL_ALIASES = {"WARN": "WARNING", "FATAL": "CRITICAL", "NOTSET": "DEBUG"}


class SSHHost(BaseModel):
    address: Union[IPvAnyAddress, str]
    port: int = Field(default=22, ge=1, le=65535)
    wdir: Path = Field(default="~/.molsuite_temp")


class LocalResourcesConfig(BaseModel):
    """Local machine resources that can be shared by multiple workers."""

    cpus: int = -1
    gpus: int = -1
    memory_gb: Optional[int] = None
    max_threads: int = -1
    max_processes: int = -1

    @field_validator("cpus", mode="before")
    @classmethod
    def set_default_cpus(cls, v: Optional[int]) -> int:
        if v is None or v == -1:
            return min(os.cpu_count() or 1, _HARDCODED_LOCAL_CPU_LIMIT)
        return min(int(v), _HARDCODED_LOCAL_CPU_LIMIT)

    @field_validator("gpus", mode="before")
    @classmethod
    def set_default_gpus(cls, v: Optional[int]) -> int:
        if v is None or v == -1:
            with contextlib.suppress(FileNotFoundError, subprocess.SubprocessError):
                # Try to detect NVIDIA GPUs
                result = subprocess.run(
                    ["nvidia-smi", "--list-gpus"],
                    capture_output=True,
                    text=True,
                )
                if result.returncode == 0:
                    return len(result.stdout.strip().split("\n"))
            return 0
        return v

    @model_validator(mode="after")
    def validate_resources(self) -> "LocalResourcesConfig":
        cpu_count = self.cpus if self.cpus and self.cpus > 0 else (os.cpu_count() or 1)
        cpu_count = max(1, min(int(cpu_count), _HARDCODED_LOCAL_CPU_LIMIT))
        self.cpus = cpu_count

        if self.max_threads is None or self.max_threads == -1:
            self.max_threads = cpu_count * 2
        if self.max_processes is None or self.max_processes == -1:
            self.max_processes = cpu_count

        # Ensure max_threads and max_processes are at least 1
        self.max_threads = max(1, self.max_threads)
        self.max_processes = max(1, self.max_processes)

        # Ensure we don't have more threads/processes than CPUs
        if cpu_count > 0:
            self.max_threads = min(self.max_threads, cpu_count * 2)  # Account for hyperthreading
            self.max_processes = min(self.max_processes, cpu_count)

        return self


class WorkerConfig(BaseModel):
    """Base configuration for all workers."""

    wid: UUID = Field(default_factory=uuid.uuid4)
    name: str
    type: str  # process, thread, ray, hpc
    enabled: bool = True
    cpus: Optional[int] = None
    memory_gb: Optional[int] = None
    gpus: Optional[int] = None

    @field_serializer("wid")
    def serialize_uuid(self, v: Union[UUID, str]) -> str:
        return str(v)


class ProcessPoolWorkerConfig(WorkerConfig):
    """Configuration for pooled process-based workers."""

    type: str = "process_pool"
    max_workers: int = 4
    cpus: int = 2
    memory_gb: int = 4
    gpus: int = 0
    timeout_s: float = 10.0
    kill_workers_on_shutdown: bool = True


class ThreadWorkerConfig(WorkerConfig):
    """Configuration for thread-based workers."""

    type: str = "thread"
    max_workers: int = 8
    cpus: int = 1
    memory_gb: int = 2
    gpus: int = 0


class RayWorkerConfig(WorkerConfig):
    """Configuration for Ray cluster workers.

    ``local`` starts a single-node runtime, ``external`` attaches to an
    existing cluster, and ``managed`` lets MF launch the configured head and
    workers through Ray's on-premise launcher.
    """

    type: str = "ray"
    native: bool = False
    address: str = "127.0.0.1:6379"
    mode: Literal["local", "external", "managed"] = "external"
    shared_fs: Optional[bool] = None
    cpus: int = 4
    memory_gb: int = 8
    gpus: int = 0
    gpu_slots_per_device: int = Field(default=1, ge=1)
    head_ip: str = ""
    worker_ips: List[str] = Field(default_factory=list)
    ssh_user: str = ""
    conda_env: str = ""
    python_version: str = ""
    setup_commands: List[str] = Field(default_factory=list)




class HPCWorkerConfig(WorkerConfig):
    """Configuration for HPC workers."""

    type: str = "hpc"
    scheduler: str = "slurm"
    host: str = ""
    user: str = ""
    queue: str = "default"
    max_slots: int = 10
    priority: int = 0
    shared_fs: bool = False
    submit_command: Optional[Union[str, List[str]]] = None
    poll_command: Optional[Union[str, List[str]]] = None
    cancel_command: Optional[Union[str, List[str]]] = None
    poll_interval_s: float = Field(default=2.0, ge=0.1)
    command_env: Dict[str, str] = Field(default_factory=dict)
    python_executable: Optional[str] = None
    cpus: int = 4
    memory_gb: int = 16
    gpus: int = 0
    ssh_user: str = ""
    conda_env: str = ""
    python_version: str = ""
    setup_commands: List[str] = Field(default_factory=list)


class ResourcesConfig(BaseModel):
    """Local machine resources configuration."""

    local: LocalResourcesConfig = Field(default_factory=LocalResourcesConfig)


class WorkersConfig(BaseModel):
    """
    Workers configuration - directly maps worker names to configs.
    Each worker is a dynamic attribute of the object.
    """

    # Config that allows extra attributes (dynamic workers)
    model_config = {"extra": "allow"}

    def __init__(self, **data):
        processed = {}
        for name, worker_data in data.items():
            if isinstance(worker_data, dict):
                worker_type = worker_data.get("type", "process_pool")
                if worker_type == "process_pool":
                    processed[name] = ProcessPoolWorkerConfig(**worker_data)
                elif worker_type == "thread":
                    processed[name] = ThreadWorkerConfig(**worker_data)
                elif worker_type == "ray":
                    processed[name] = RayWorkerConfig(**worker_data)
                elif worker_type == "hpc":
                    processed[name] = HPCWorkerConfig(**worker_data)
                else:
                    worker_data["type"] = "process_pool"
                    processed[name] = ProcessPoolWorkerConfig(**worker_data)
            else:
                processed[name] = worker_data
        super().__init__(**processed)


class DatabaseConfig(BaseModel):
    _db: str = "molsuite_executor.db"
    connection_timeout: int = 30


class GeneralConfig(BaseModel):
    poll_interval: float = 0.1
    log_level: LogLevel = "INFO"

    @field_validator("log_level", mode="before")
    @classmethod
    def normalize_level_name(cls, v: str) -> str:
        level_name = str(v).upper().strip()
        return _LEVEL_ALIASES.get(level_name, level_name)


class OperationalLimitsConfig(BaseModel):
    operational_profile: Literal["strict", "balanced", "throughput"] = "balanced"
    max_inline_chunk_payload_bytes: int = Field(default=512 * 1024, ge=1024)
    max_spool_payload_bytes: int = Field(default=64 * 1024 * 1024, ge=1024)
    staging_max_workers: int = Field(default=8, ge=1)
    default_max_inflight_tasks: int = Field(default=16, ge=1)
    default_max_inflight_items: int = Field(default=256, ge=1)
    progress_flush_interval_s: float = Field(default=2.0, gt=0.0)
    output_sink_flush_retries: int = Field(default=3, ge=0)
    output_sink_retry_backoff_s: float = Field(default=0.05, ge=0.0)
    output_sink_max_buffer_factor: int = Field(default=10, ge=1)
    output_sink_max_buffer_bytes: int = Field(default=16 * 1024 * 1024, ge=1024)
    output_sink_max_payload_bytes: int = Field(default=4 * 1024 * 1024, ge=1024)
    output_sink_max_pending_chunks: int = Field(default=1024, ge=1)
    output_sink_max_pending_bytes: int = Field(default=256 * 1024 * 1024, ge=1024)

    @field_validator("operational_profile", mode="before")
    @classmethod
    def validate_operational_profile(cls, v: str) -> str:
        profile = str(v or "balanced").strip().lower()
        if profile not in {"strict", "balanced", "throughput"}:
            raise ValueError("operational_profile must be one of: strict, balanced, throughput")
        return profile


class LoggingConfig(BaseModel):
    max_file_size_mb: int = Field(default=10, ge=1)
    backup_count: int = Field(default=10, ge=1)
    retention_days: int = Field(default=30, ge=1)
    queue_size: int = Field(default=10000, ge=0)
    app_level: LogLevel = "INFO"
    executor_level: LogLevel = "INFO"
    project_level: LogLevel = "INFO"
    console_level: LogLevel = "INFO"

    @field_validator("app_level", "executor_level", "project_level", "console_level", mode="before")
    @classmethod
    def validate_level_name(cls, v: str) -> str:
        level_name = str(v).upper().strip()
        return _LEVEL_ALIASES.get(level_name, level_name)


class Settings(BaseModel):
    """
    Configuration for the orchestration engine.
    Can be loaded from TOML or constructed programmatically.
    """

    data_dir: str = "data"
    projects_db: Path = Field(default_factory=lambda: Path.home().joinpath(".molsuite", "projects.db"))
    executor_db: Optional[Path] = None
    general: GeneralConfig = Field(default_factory=GeneralConfig)
    operational_limits: OperationalLimitsConfig = Field(default_factory=OperationalLimitsConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    resources: ResourcesConfig = Field(default_factory=ResourcesConfig)
    workers: WorkersConfig = Field(default_factory=WorkersConfig)
    applications: Dict[str, Dict[str, Any]] = Field(default_factory=dict)

    _config_path: Optional[Path] = None

    @classmethod
    def load(cls, config_path: Optional[str] = None) -> "Settings":
        """
        Load config from TOML file or return defaults.

        Args:
            config_path: Optional path to the config file. If None, searches in standard locations.
        """
        path = Path(config_path) if config_path else cls._find_config()
        logging.info(f"Reading config from: {path.as_posix()}")

        if path and path.exists():
            with open(path, "r") as f:
                data = toml.load(f)
            config = cls.model_validate(data)
            config._config_path = path
            return config

        return cls()

    @model_validator(mode="after")
    def ensure_executor_db(self) -> "Settings":
        if self.executor_db is None:
            self.executor_db = self.projects_db.parent / "executor.db"
        return self

    @classmethod
    def _find_config(cls) -> Path:
        search_paths = [
            Path.cwd() / "molsuite.toml",
            Path.home() / ".config" / "molsuite" / "config.toml",
        ]

        for path in search_paths:
            if path.exists():
                return path

        return Path.home() / ".config" / "molsuite" / "config.toml"

    @property
    def data_path(self) -> Path:
        return Path(self.data_dir).resolve()


    def ensure_dirs(self) -> None:
        self.data_path.mkdir(parents=True, exist_ok=True)
        if self._config_path:
            self._config_path.parent.mkdir(parents=True, exist_ok=True)

    def add_worker(self, worker: WorkerConfig) -> None:
        workers_dict = self.workers.model_dump()
        workers_dict[worker.name] = worker.model_dump()
        self.workers = WorkersConfig(**workers_dict)

    def get_worker(self, name: str) -> Optional[WorkerConfig]:
        return getattr(self.workers, name, None)

    def get_all_workers(self) -> Dict[str, WorkerConfig]:
        workers = {}
        workers_dict = self.workers.model_dump()
        for name in workers_dict:
            worker = getattr(self.workers, name)
            if isinstance(worker, WorkerConfig):
                workers[name] = worker
        return workers

    def get_workers_by_type(self, worker_type: str) -> List[WorkerConfig]:
        return [worker for worker in self.get_all_workers().values() if worker.type == worker_type]

    def get_enabled_workers(self) -> List[WorkerConfig]:
        return [worker for worker in self.get_all_workers().values() if worker.enabled]

    def remove_worker(self, name: str) -> bool:
        workers_dict = self.workers.model_dump()
        if name in workers_dict:
            del workers_dict[name]
            self.workers = WorkersConfig(**workers_dict)
            return True
        return False

    def disable_worker(self, name: str) -> bool:
        worker = self.get_worker(name)
        if worker:
            worker.enabled = False
            return True
        return False

    def enable_worker(self, name: str) -> bool:
        worker = self.get_worker(name)
        if worker:
            worker.enabled = True
            return True
        return False

    def save(self, path: Optional[Path] = None, local: bool = False) -> None:
        if path is None:
            if local:
                path = Path.cwd() / "molsuite.toml"
            else:
                path = Path.home() / ".config" / "molsuite" / "config.toml"

        path.parent.mkdir(parents=True, exist_ok=True)

        config_dict = self.model_dump(
            mode="python",
            exclude_none=True,
            exclude={"_config_path"},
        )

        def default_serializer(obj):
            if isinstance(obj, Path):
                return str(obj)
            if hasattr(obj, "model_dump"):
                return obj.model_dump(mode="json")
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

        serializable_dict = json.loads(
            json.dumps(config_dict, default=default_serializer, indent=2)
        )

        with open(path, "w") as f:
            toml.dump(serializable_dict, f)

        self._config_path = path
