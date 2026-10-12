from __future__ import annotations

import contextlib
import json
import logging
import os
import re
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import toml
from pydantic import BaseModel, Field, field_validator, model_validator


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


NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_-]*")
_SHARE = "0 is all it has, a negative number leaves that many free, a positive one is how many."


def share(asked: int, has: int, least: int = 0) -> int:
    """How much of the `has` a machine has is for jobs: all of it for 0, all but that many for a
    negative number, that many for a positive one. Never over what it has nor under `least`."""
    return max(least, min(has, asked if asked > 0 else has + asked))


def gpus_here() -> int:
    # ponytail: the NVIDIA ones are the ones counted; another vendor's when a job asks for them.
    with contextlib.suppress(FileNotFoundError, subprocess.SubprocessError):
        result = subprocess.run(["nvidia-smi", "--list-gpus"], capture_output=True, text=True)
        if result.returncode == 0:
            return len(result.stdout.strip().splitlines())
    return 0


class HostConfig(BaseModel):
    """A machine that runs jobs, kept by name. The one named `local` is this machine."""

    address: str = Field(default="", description="How `ssh` names it. Empty is this machine.")
    env: str = Field(default="", description="The environment there that holds the app. Empty: the app prepares one.")
    workdir: str = Field(
        default=".molsuite/jobs",
        description="Where what runs there keeps its files; a relative one is under the home of the account.")
    cpus: int = Field(default=0, description=_SHARE)
    gpus: int = Field(default=0, description=_SHARE)
    memory_gb: int = Field(default=0, description=_SHARE)
    tools: Dict[str, str] = Field(
        default_factory=dict, description="Where a program is there, by its name, when it is not the one the app installs.")


class ExecutorConfig(BaseModel):
    """Who runs jobs, kept by name. `hosts` are names in `Settings.hosts`.

    `loky` is the pool of processes of this machine. `ray` is a Ray cluster started from this
    machine over its hosts: with `local` among them this machine works in it too. `queue` is
    the queue of an HPC, and its one host the login node (`local` when this runs on it).
    """

    kind: Literal["loky", "ray", "queue"]
    hosts: List[str] = Field(default_factory=lambda: ["local"])
    on_login_node: bool = Field(default=False, description="`loky`: let the pool run on the login node of a cluster.")
    scheduler: Literal["slurm", "none"] = Field(default="slurm", description="`queue`: who keeps the queue.")
    setup: str = Field(default="", description="`queue`: the shell lines that prepare the environment before the runner starts.")
    runner: str = Field(default="", description="`queue`: arguments for the runner, like `--ray auto` or `--cpus 16`.")
    template: str = Field(default="", description="`queue`: a file with the script the queue runs. Empty is the one built in.")
    elements_per_job: int = Field(
        default=0, ge=0, description="`queue`: how many elements go in each job of the queue. 0 is all of them.")
    max_jobs: int = Field(default=0, ge=0, description="`queue`: how many jobs it has in the queue at once. 0 is any number.")
    poll_seconds: float = Field(default=30, gt=0, description="`queue`: how often the queue is asked.")
    shared_disk: Optional[bool] = Field(
        default=None, description="`queue`: whether the other side sees the disk of this machine. Unset: only on `local`.")


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

    # Chosen where the executors are started and stopped, not typed into a grid.
    default_executor: str = Field(
        default="local", description="Who runs a job that names nobody.", json_schema_extra={"settings_hidden": True})

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
    hosts: Dict[str, HostConfig] = Field(default_factory=dict)
    executors: Dict[str, ExecutorConfig] = Field(default_factory=dict)
    applications: Dict[str, Dict[str, Any]] = Field(default_factory=dict)

    _config_path: Optional[Path] = None

    @model_validator(mode="after")
    def _hosts_and_executors(self) -> "Settings":
        """`local` is always there, in both; an executor names hosts that are kept."""
        self.hosts.setdefault("local", HostConfig())
        self.executors.setdefault("local", ExecutorConfig(kind="loky"))
        for name in (*self.hosts, *self.executors):
            if not NAME.fullmatch(name):
                raise ValueError(f"`{name}` is not a name: letters, digits, `_` and `-`, and no digit first")
        if "thread" in self.executors:
            raise ValueError("`thread` is the name of the threads of this process")
        if self.general.default_executor not in self.executors:
            self.general.default_executor = "local"  # the one it named is not kept any more
        for name, host in self.hosts.items():
            if bool(host.address) == (name == "local"):
                raise ValueError("`hosts.local` is this machine: it has no address" if name == "local"
                                 else f"`hosts.{name}` has no address")
        addresses = [host.address for host in self.hosts.values()]
        for name, host in self.hosts.items():
            if addresses.count(host.address) > 1:
                raise ValueError(f"`hosts.{name}` has the address of another host: {host.address}")
        for name, executor in self.executors.items():
            unknown = [host for host in executor.hosts if host not in self.hosts]
            if unknown:
                raise ValueError(f"`executors.{name}` names hosts that are not kept: {', '.join(unknown)}")
            if (executor.kind == "loky") != (name == "local"):
                raise ValueError("`executors.local` is the pool of this machine, and the only one")
            if not executor.hosts or len(set(executor.hosts)) != len(executor.hosts):
                raise ValueError(f"`executors.{name}` takes each of its hosts once, and at least one")
            if executor.kind == "loky" and executor.hosts != ["local"]:
                raise ValueError("`executors.local` runs on this machine: its only host is `local`")
            if executor.kind == "queue" and len(executor.hosts) != 1:
                raise ValueError(f"`executors.{name}` takes one host: the login node of its queue")
        return self

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
