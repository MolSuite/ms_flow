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

from ms_flow.engine.tokens import usable_cpus

_HARDCODED_LOCAL_CPU_LIMIT = 14


def molsuite_home() -> Path:
    """The one per-user folder of the ``ms_*`` packages: registry, settings, logs, engine lock.

    ``$MOLSUITE_HOME`` moves it (a cluster whose home is small, a test); an app keeps what
    is its own in its own folder, never here.
    """
    configured = str(os.environ.get("MOLSUITE_HOME") or "").strip()
    return Path(configured).expanduser() if configured else Path.home() / ".molsuite"

# Categorical values are declared, not just validated: the settings UI turns any
# Literal into a combo box instead of a free-text field.
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
_LEVEL_ALIASES = {"WARN": "WARNING", "FATAL": "CRITICAL", "NOTSET": "DEBUG"}


class SSHHost(BaseModel):
    address: Union[IPvAnyAddress, str]
    port: int = Field(default=22, ge=1, le=65535)
    wdir: Path = Field(default="~/.molsuite/tmp")


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
            return min(usable_cpus(), _HARDCODED_LOCAL_CPU_LIMIT)
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
        cpu_count = self.cpus if self.cpus and self.cpus > 0 else usable_cpus()
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
    # Absolute path on the remote where apps install the external tools they manage.
    tools_home: str = ""




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
    # Absolute path on the remote where apps install the external tools they manage.
    tools_home: str = ""


class RayRoleConfig(BaseModel):
    address: str = Field(default="", description="Empty starts a cluster on this machine; `auto` or `host:port` joins one that is running.")
    shared_disk: Optional[bool] = Field(
        default=None, description="Whether every node sees the disk of this machine. Unset: only the cluster started here does.")


class QueueRoleConfig(BaseModel):
    host: str = Field(default="", description="The login node, as `ssh` names it. Empty is the queue of the machine this runs on.")
    scheduler: Literal["slurm", "none"] = "slurm"
    workdir: str = Field(default="ms_flow_jobs", description="Where the jobs live over there; a relative one is under the home of the account.")
    python: str = "python"
    setup: str = Field(default="", description="The shell lines that prepare the environment before the runner starts.")
    runner: str = Field(default="", description="Arguments for the runner, like `--ray auto` or `--cpus 16`.")
    template: str = Field(default="", description="A file with the script the queue runs. Empty is the one built in.")
    elements_per_job: int = Field(default=0, ge=0, description="How many elements go in each job of the queue. 0 is all of them.")
    max_jobs: int = Field(default=0, ge=0, description="How many jobs it has in the queue at once. 0 is any number.")
    poll_seconds: float = Field(default=30, gt=0)
    shared_disk: Optional[bool] = Field(
        default=None, description="Whether the other side sees the disk of this machine. Unset: only without a host.")


class ProcessRoleConfig(BaseModel):
    """Who runs what is submitted to `process`: one of them, never two that share this machine."""

    kind: Literal["process", "ray", "queue"] = Field(
        default="process", description="`process` is the pool of this machine, `ray` a Ray cluster, `queue` the queue of an HPC.")
    on_login_node: bool = Field(
        default=False, description="Let the pool of this machine run on the login node of a cluster.")
    ray: RayRoleConfig = Field(default_factory=RayRoleConfig)
    queue: QueueRoleConfig = Field(default_factory=QueueRoleConfig)


class ResourcesConfig(BaseModel):
    """Local machine resources configuration."""

    local: LocalResourcesConfig = Field(default_factory=LocalResourcesConfig)
    process: ProcessRoleConfig = Field(default_factory=ProcessRoleConfig)


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
    prune_missing_projects: bool = Field(
        default=True,
        description="On startup, drop registry entries whose project folder no longer exists (e.g. tmp test projects).",
    )

    @field_validator("log_level", mode="before")
    @classmethod
    def normalize_level_name(cls, v: str) -> str:
        level_name = str(v).upper().strip()
        return _LEVEL_ALIASES.get(level_name, level_name)


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

    # The MolSuite home: moving it orphans every registered project, so it is not a UI
    # setting. Each project carries its own executor.db.
    projects_db: Path = Field(
        default_factory=lambda: molsuite_home() / "projects.db",
        json_schema_extra={"settings_hidden": True},
    )
    general: GeneralConfig = Field(default_factory=GeneralConfig)
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

    @classmethod
    def _find_config(cls) -> Path:
        search_paths = [
            Path.cwd() / "molsuite.toml",
            molsuite_home() / "config.toml",
        ]

        for path in search_paths:
            if path.exists():
                return path

        return molsuite_home() / "config.toml"

    def ensure_dirs(self) -> None:
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
                path = molsuite_home() / "config.toml"

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
