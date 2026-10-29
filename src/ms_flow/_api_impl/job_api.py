from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

_LOGIN_NODE = ("this is the login node of a cluster, where nothing is to compute: send the job to a queue "
               "(`executors.local.on_login_node` forces the pool of this machine)")


def _login_node() -> bool:
    # ponytail: Slurm is the one told apart; the commands of another scheduler when a cluster has one.
    return shutil.which("sbatch") is not None and "SLURM_JOB_ID" not in os.environ


def _jobs_in_queue(address: str, workdir: str, scheduler: str) -> str:
    """Why the environment on the host of a queue is not to be changed now, or nothing."""
    from ms_flow.remote import RemoteExecutor

    there = RemoteExecutor(address, workdir=workdir, scheduler=scheduler)
    try:
        alive = sum(row["in_queue"] for row in there.jobs_there())
    finally:
        there.shutdown()
    return f"{alive} of its jobs are in the queue there" if alive else ""


def blocked(name: str, settings, active) -> str:
    """Why the executor kept as `name` is not to be started while those in `active` run, or nothing.

    A machine works for one executor at a time, and this process joins one Ray. The pool of
    this machine is not in the way of a Ray that takes this machine: it is put aside for it
    (see `Workspace.activate`). A queue computes nothing on its host, so it shares it, and it
    is the only one started on the login node of a cluster: a Ray has its head there even
    when none of its tasks run on it.
    """
    asked = settings.executors[name]
    if _login_node() and (asked.kind == "ray" or asked.kind == "loky" and not asked.on_login_node):
        return _LOGIN_NODE
    for other in active:
        config = settings.executors.get(other)  # `thread` is nobody's
        if other == name or config is None or "queue" in (asked.kind, config.kind):
            continue
        if asked.kind == config.kind == "ray":
            return f"one Ray at a time, and `{other}` is active"
        shared = [host for host in asked.hosts if host in config.hosts]
        if shared and config.kind == "ray":
            return f"`{shared[0]}` works for `{other}`, which is active"
    return ""


def _here(local) -> tuple[int, int, int | None]:
    """The CPUs, the GPUs and the memory in MB (None: the engine's own budget) that
    `hosts.local` gives to jobs."""
    from ms_flow.core.settings.models import gpus_here, share
    from ms_flow.engine.tokens import physical_memory_mb, usable_cpus

    return (share(local.cpus, usable_cpus(), 1), share(local.gpus, gpus_here()),
            share(local.memory_gb * 1024, physical_memory_mb(), 1024) if local.memory_gb else None)


def _spec_executor(spec: str | None) -> str | None:
    try:
        return json.loads(spec).get("executor")
    except (TypeError, ValueError, AttributeError):
        return None


def build_executor(name: str, settings, host_env=None):
    """The executor `settings.executors[name]` describes, or None for the pool on a login node.

    A host with no environment named gets it from `host_env` (see `Workspace.host_env`).
    """
    config = settings.executors[name]
    hosts = [settings.hosts[host] for host in config.hosts]
    if config.kind == "ray":
        from ms_flow.engine import RayExecutor

        # Without `local` among its hosts this machine holds the head and runs none of its tasks.
        cpus, gpus, _ = _here(settings.hosts["local"]) if "local" in config.hosts else (0, 0, None)
        # ponytail: memory is left to Ray, which takes its share of what the machine has.
        others = [host for host in hosts if host.address]
        if not others:
            return RayExecutor(None, num_cpus=cpus, num_gpus=gpus)
        from ms_flow.ray_hosts import RayOverHosts

        if host_env is None and not all(host.env for host in others):
            raise ValueError(f"a host of `executors.{name}` has no `env` and nothing prepares it: see `Workspace.host_env`")
        # Nothing of a Ray started here outlives its session: nobody to ask whether it is busy.
        return RayOverHosts([host.address for host in others],
                            [host.env or host_env(host.address, None) for host in others],
                            limits=[(host.cpus, host.gpus, host.memory_gb) for host in others],
                            num_cpus=cpus, num_gpus=gpus)
    if config.kind == "queue":
        from ms_flow.remote import JOB_SH, RemoteExecutor

        host, env = hosts[0], hosts[0].env
        if host.address and not env and host_env is not None:
            env = host_env(host.address, lambda: _jobs_in_queue(host.address, host.workdir, config.scheduler))
        return RemoteExecutor(host.address or None, workdir=host.workdir, scheduler=config.scheduler,
                              template=Path(config.template).read_text() if config.template else JOB_SH,
                              setup=config.setup, runner=config.runner,
                              elements_per_job=config.elements_per_job or None, max_jobs=config.max_jobs or None,
                              poll_s=config.poll_seconds, shared=config.shared_disk, env=env)
    if _login_node() and not config.on_login_node:
        return None
    from ms_flow.engine import ProcessExecutor

    return ProcessExecutor(_here(hosts[0])[0])


class WorkspaceJobApiMixin:
    #: Set by the app: `host_env(address, busy, on_line) -> path` makes sure the host has the
    #: environment that runs its jobs and says where. It is for a host with no `env` named,
    #: before any job runs there. `busy`, when given, returns why that environment is not to
    #: be changed now (see `ms_env.ensure`); `on_line`, when given, is told each line of what
    #: is done there.
    host_env = None

    @property
    def engine(self):
        """The engine behind `submit`, built at first use: the one that shares out this machine,
        with its threads as `thread` and its pool of processes as `local`.

        Raises `ms_flow.engine.Busy` when another process of this user already runs one.
        """
        if self._engine is None:
            from ms_flow.engine import Engine, ThreadExecutor

            cpus, gpus, mem_mb = _here(self.settings_manager.settings.hosts["local"])
            engine = Engine(cpus=cpus, gpus=gpus, mem_mb=mem_mb, state_dir=self._master_db_path.parent)
            engine.register("thread", ThreadExecutor(cpus * 2))
            self._engine = engine
            self._local_back()
        return self._engine

    def _local_back(self) -> None:
        executor = build_executor("local", self.settings_manager.settings)
        if executor is not None:
            self._engine.register("local", executor)

    def _active(self) -> set[str]:
        """The executors that take jobs now. Never builds the engine: before its first use
        nothing does but the pool of this machine."""
        if self._engine is not None:
            return {name for name in self._engine._executors if name != "thread"}
        pool = self.settings_manager.settings.executors["local"]
        return {"local"} if pool.on_login_node or not _login_node() else set()

    def executors(self) -> list[dict]:
        """One row for each executor kept: `name`, `kind`, `hosts`, `default`, and `state`:
        `on`, `starting`, `off`, or `blocked` when it is off and not to be started now, with
        `why`. Safe to poll."""
        settings, active = self.settings_manager.settings, self._active()
        rows = []
        for name, config in settings.executors.items():
            why = "" if name in active or name in self._starting else blocked(name, settings, active)
            state = "starting" if name in self._starting else "on" if name in active else "blocked" if why else "off"
            rows.append({"name": name, "kind": config.kind, "hosts": list(config.hosts), "state": state, "why": why,
                         "default": name == settings.general.default_executor})
        return rows

    def activate(self, name: str, on_line=None) -> None:
        """Start the executor kept as `name`, which takes jobs under that name from then on.

        Refused with `ValueError` while another active one has one of its machines or is a Ray
        too (see `blocked`). A Ray that takes this machine puts the pool of it aside until it
        is deactivated, so the pool must have no job that has not ended. A host that is not
        reached or not prepared raises, and leaves everything as it was. `on_line` hears what
        is done on a host that is being prepared. It waits for its hosts: not from a thread
        that cannot.
        """
        settings, engine = self.settings_manager.settings, self.engine
        if name not in settings.executors:
            raise ValueError(f"no executor is kept as `{name}`")
        self._starting.add(name)
        try:
            with self._activation:
                if name in engine._executors:
                    return
                why = blocked(name, settings, self._active())
                if why:
                    raise ValueError(f"`{name}` is not started: {why}")
                config = settings.executors[name]
                aside = config.kind == "ray" and "local" in config.hosts and "local" in engine._executors
                if aside:
                    try:
                        engine.unregister("local")
                    except ValueError:
                        raise ValueError(f"`{name}` takes this machine, and its pool has jobs that have not ended") from None
                try:
                    host_env = self.host_env and (lambda address, busy: self.host_env(address, busy, on_line))
                    executor = build_executor(name, settings, host_env)
                    if executor is None:
                        raise RuntimeError(_LOGIN_NODE)
                    engine.register(name, executor)
                except BaseException:
                    if aside:
                        self._local_back()
                    raise
                if aside:
                    self._aside = name
        finally:
            self._starting.discard(name)

    def deactivate(self, name: str) -> None:
        """Stop the executor active as `name`. Refused with `ValueError` while a job submitted
        to it has not ended. The pool of this machine comes back if it was aside for this one."""
        if self._engine is None or name not in self._active():
            return
        with self._activation:
            self._engine.unregister(name)
            if self._aside == name:
                self._aside = None
                self._local_back()

    def _change(self, table: str, name: str, change) -> None:
        """Run `change` on the entry `name` of `hosts` or `executors`, unless an active executor
        is made of it. The pool of this machine is the one made again with what is kept."""
        executors = self.settings_manager.settings.executors
        users = [key for key in self._active() & set(executors) if self._engine is not None and
                 (key == name if table == "executors" else name in executors[key].hosts)]
        if users and users != ["local"]:
            raise ValueError(f"`{table}.{name}` is in use by `{users[0]}`, which is active: deactivate it first")
        with self._activation:
            if users:
                self._engine.unregister("local")  # refused while it has jobs, and then nothing is changed
            try:
                change()
            finally:
                if users:
                    self._local_back()

    def save_host(self, name: str, **fields) -> None:
        """`SettingsManager.save_host`, refused while an active executor works on that machine."""
        self._change("hosts", name, lambda: self.settings_manager.save_host(name, **fields))

    def remove_host(self, name: str) -> None:
        self._change("hosts", name, lambda: self.settings_manager.remove_host(name))

    def save_executor(self, name: str, **fields) -> None:
        """`SettingsManager.save_executor`, refused while that executor is active."""
        self._change("executors", name, lambda: self.settings_manager.save_executor(name, **fields))

    def remove_executor(self, name: str) -> None:
        self._change("executors", name, lambda: self.settings_manager.remove_executor(name))

    def _project_flow(self):
        project = self._require_active_project_runtime()
        if self._flow is None:
            from ms_flow.engine import Flow

            self.engine.attach("project", project.context.path / "executor.db")
            self._flow = Flow(self.engine, project.project_db)
        return self._flow

    def submit(self, job, params=None, *, executor: str | None = None, **options) -> int:
        """Run a `ms_flow.engine.job` in the active project and return its id, for `engine.status`
        and `engine.wait`.

        Its results go to the project's store, and the job is recorded in the project's
        `executor.db`: closing the project interrupts it, and submitting it again with the same
        `key=` continues it. `executor` is `thread` or the name of an active one (see
        `activate`), by default the one the settings give (`general.default_executor`);
        `options` are those of `ms_flow.engine.Flow.submit`.
        """
        flow = self._project_flow()
        name = executor or self.settings_manager.settings.general.default_executor
        self._ready(name)
        return flow.submit(job, params, journal="project", executor=name, **options)

    def _ready(self, name: str) -> None:
        if name not in self._engine._executors:
            raise RuntimeError(
                f"the pool of this machine is aside while `{self._aside}` has it" if name == "local" and self._aside
                else _LOGIN_NODE if name == "local" and _login_node()
                else f"`{name}` is not active: see `Workspace.activate`")

    def origin(self, key: str) -> dict:
        """What the job recorded under `key` in the active project was submitted with: `job` (its
        `module:qualname`), `params` and `options`. Raises for one `resume` cannot submit again."""
        return self._project_flow().origin(key, journal="project")

    def resume(self, key: str, **options) -> int:
        """Submit again the job recorded under `key` (`JobSnapshot.key`) in the active project,
        with the params and options it was submitted with: one that was interrupted (the project
        or the application was closed) or canceled continues where it was. `options` say
        otherwise for this run, and bring what the record cannot keep (`on_terminal`)."""
        from ms_flow.engine.flow import named

        origin = self.origin(key)
        return self.submit(named(origin["job"]), origin["params"], key=key, **{**origin["options"], **options})

    def move(self, key: str, executor: str, **options) -> int:
        """Continue on `executor` the job recorded under `key`: `resume`, for one that may be running.
        A job this session runs is canceled first and waited for, so not from a thread that cannot
        wait: what its tasks in flight had done is done again there."""
        self.origin(key)  # nothing is canceled for a job that cannot be submitted again,
        self._ready(executor)  # or that has nowhere to go
        for job in self._engine.jobs():
            if job.key == key and job.finished_at is None:
                self._engine.cancel(job.id)
                self._engine.wait(job.id)
        return self.resume(key, executor=executor, **options)

    def remote_jobs(self, executor: str) -> list[dict]:
        """What the executor of that name has on another machine, whichever session or project
        sent it and with or without a project open: see `RemoteExecutor.jobs_there`. Nothing
        when it runs its jobs here. It asks that machine, so not from a thread that cannot wait."""
        slot = self.engine._executors.get(executor)
        return slot.impl.jobs_there() if hasattr(getattr(slot, "impl", None), "jobs_there") else []

    def remove_remote_job(self, executor: str, dir: str) -> None:
        """Take one of `remote_jobs` out of the queue, with what it has there. One that a job of
        this session waits for is refused: cancel the job."""
        self.engine.executor(executor).remove_there(dir)

    def jobs(self, limit: int | None = None) -> list:
        """The jobs of the active project, oldest first; with `limit`, the last ones.

        What a monitor shows: every job on record in the project's journal, the ones of earlier
        sessions too, each as the engine has it now when it ran it in this session. Reads the
        journal without owning it and never builds the engine, so it is safe to poll.
        """
        project = self._runtime_state.active_project
        if project is None:
            return []
        from ms_flow.engine import JobSnapshot
        from ms_flow.engine.journal import recorded

        live = {(job.key, job.started_at): job for job in (self._engine.jobs() if self._flow is not None else ())}
        # ponytail: a job of a process that died reads as running until this session's first
        # submit opens the journal, which is what marks it interrupted.
        listed = [live.pop((row["job_id"], row["started_at"]), None)
                  or JobSnapshot(None, row["name"], row["status"], row["total"], row["done"], row["failed"], 0,
                                 row["error"], 0, row["started_at"], row["finished_at"], row["job_id"], 0,
                                 min(1.0, (row["done"] + row["failed"]) / row["total"]) if row["total"] else None,
                                 executor=_spec_executor(row["spec"]))
                  for row in recorded(project.context.path / "executor.db", limit)]
        # One just submitted is not in the file until the journal's next write. Only the active
        # project has jobs that have not ended: closing a project interrupts its own.
        return listed + [job for job in live.values() if job.finished_at is None]

    def job_tasks(self, job_id: int | None) -> list:
        """The tasks in flight of a job this session runs, by its `JobSnapshot.id`: see
        `Engine.tasks`. Nothing for a job of another session. Never builds the engine."""
        return self._engine.tasks(job_id) if self._engine is not None and job_id is not None else []

    def job_failures(self, key: str, limit: int = 200) -> list[dict]:
        """`attempt`, `error` (the traceback) and `input` (a short repr of the element) of the
        chunks that failed in the job recorded under `key` (`JobSnapshot.key`) in the active
        project, at most `limit`."""
        from ms_flow.engine.journal import failures

        return failures(self._require_active_project_runtime().context.path / "executor.db", key, limit)

    def get_project_store(self):
        return self.advanced.project_store


__all__ = ["WorkspaceJobApiMixin"]
