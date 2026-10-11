from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

log = logging.getLogger(__name__)

_LOGIN_NODE = ("this is the login node of a cluster, where the pool of processes does not run: set "
               "`resources.process.kind` to `queue` or `ray`, or `resources.process.on_login_node` to force it")


def _login_node() -> bool:
    # ponytail: Slurm is the one told apart; the commands of another scheduler when a cluster has one.
    return shutil.which("sbatch") is not None and "SLURM_JOB_ID" not in os.environ


def _jobs_in_queue(queue) -> str:
    """Why the environment on the host of `queue` is not to be changed now, or nothing."""
    from ms_flow.remote import RemoteExecutor

    there = RemoteExecutor(queue.host, workdir=queue.workdir, scheduler=queue.scheduler)
    try:
        alive = sum(row["in_queue"] for row in there.jobs_there())
    finally:
        there.shutdown()
    return f"{alive} of its jobs are in the queue there" if alive else ""


def _process_executor(resources, host_env=None):
    """The executor `resources.process` names, or None for the pool of a login node.

    A host with no environment named gets it from `host_env` (see `Workspace.host_env`).
    """
    role = resources.process
    if role.kind == "ray":
        if role.ray.hosts and not role.ray.address:
            from ms_flow.ray_hosts import RayOverHosts

            if not role.ray.env and host_env is None:
                raise ValueError("`resources.process.ray.env` is empty and nothing prepares the hosts: see `Workspace.host_env`")
            # Nothing of a Ray started here outlives its session: nobody to ask whether it is busy.
            return RayOverHosts(role.ray.hosts, role.ray.env or [host_env(host, None) for host in role.ray.hosts])
        from ms_flow.engine import RayExecutor

        return RayExecutor(role.ray.address or None, shared=role.ray.shared_disk)
    if role.kind == "queue":
        from ms_flow.remote import JOB_SH, RemoteExecutor

        queue, env = role.queue, role.queue.env
        if queue.host and not env and queue.python == "python" and host_env is not None:
            env = host_env(queue.host, lambda: _jobs_in_queue(queue))
        return RemoteExecutor(queue.host or None, workdir=queue.workdir, scheduler=queue.scheduler,
                              template=Path(queue.template).read_text() if queue.template else JOB_SH,
                              python=queue.python, setup=queue.setup, runner=queue.runner,
                              elements_per_job=queue.elements_per_job or None, max_jobs=queue.max_jobs or None,
                              poll_s=queue.poll_seconds, shared=queue.shared_disk, env=env)
    if _login_node() and not role.on_login_node:
        return None
    from ms_flow.engine import ProcessExecutor

    return ProcessExecutor(resources.local.max_processes)


class WorkspaceJobApiMixin:
    #: Set by the app: `host_env(address, busy, on_line) -> path` makes sure the host has the
    #: environment that runs its jobs and says where. It is called when `process` is given to a
    #: host with no environment named, so before any job runs there. `busy`, when given, returns
    #: why that environment is not to be changed now (see `ms_env.ensure`); `on_line`, when given,
    #: is told each line of what is done there.
    host_env = None
    _host_line = None  # who hears those lines while `run_process_on` runs

    @property
    def engine(self):
        """The engine behind `submit`, built at first use: the one that shares out this machine.

        Raises `ms_flow.engine.Busy` when another process of this user already runs one.
        """
        if self._engine is None:
            from ms_flow.engine import Engine, ThreadExecutor

            local = self.settings_manager.settings.resources.local
            engine = Engine(cpus=local.cpus, gpus=local.gpus, state_dir=self._master_db_path.parent,
                            mem_mb=local.memory_gb * 1024 if local.memory_gb else None)
            engine.register("thread", ThreadExecutor(local.max_threads))
            self._engine = engine
            self.reload_executors()
        return self._engine

    def reload_executors(self) -> None:
        """Put in `process` whoever `resources.process` names now. Call it after changing that setting.

        Raises `ValueError` while a job submitted to `process` has not ended, and leaves it as it was.
        """
        if self._engine is None:
            return  # built from the settings at first use
        self._engine.unregister("process")
        host_env = self.host_env and (lambda address, busy: self.host_env(address, busy, self._host_line))
        executor = _process_executor(self.settings_manager.settings.resources, host_env)
        if executor is not None:
            self._engine.register("process", executor)

    def run_process_on(self, kind: str = "process", on_line=None, **fields) -> None:
        """Say who runs what is submitted to `process`, keep it for this user and put it in place.

        `fields` are those of that kind in `resources.process`, the rest of them as they come:
        `run_process_on("ray", hosts=["pc2"])`, `run_process_on("queue", host="hpc")`,
        `run_process_on()` for the pool of this machine. A value that is not valid, a host that
        is not reached or not prepared, and a job on `process` that has not ended raise, and
        leave the setting and the executor as they were. `on_line` hears what is done on a
        host that is being prepared. It waits for those hosts: not from a thread that cannot.
        """
        manager, path = self.settings_manager, "resources.process"
        before = manager.get_global_value(path).model_dump(mode="json")
        # ponytail: going back writes the whole section as it was, defaults included.
        manager.set_global_value(path, {**before, **fields, "kind": kind} if kind == "process"
                                 else {**before, "kind": kind, kind: fields})
        self._host_line = on_line
        try:
            if self._engine is None:
                self.engine
            else:
                self.reload_executors()
        except BaseException:
            manager.set_global_value(path, before)
            if self._engine is not None and "process" not in self._engine._executors:
                try:
                    self.reload_executors()
                except Exception:  # the one that was there does not come back either: said, not raised over the cause
                    log.exception("`process` was left with no executor")
            raise
        finally:
            self._host_line = None

    def _project_flow(self):
        project = self._require_active_project_runtime()
        if self._flow is None:
            from ms_flow.engine import Flow

            self.engine.attach("project", project.context.path / "executor.db")
            self._flow = Flow(self.engine, project.project_db)
        return self._flow

    def submit(self, job, params=None, **options) -> int:
        """Run a `ms_flow.engine.job` in the active project and return its id, for `engine.status`
        and `engine.wait`.

        Its results go to the project's store, and the job is recorded in the project's
        `executor.db`: closing the project interrupts it, and submitting it again with the same
        `key=` continues it. `options` are those of `ms_flow.engine.Flow.submit`.
        """
        flow = self._project_flow()
        try:
            self._engine.executor(options.get("executor", "process"))
        except KeyError:
            if options.get("executor", "process") != "process":
                raise
            raise RuntimeError(_LOGIN_NODE if _login_node() else
                               "nothing is registered as `process`: see `reload_executors`") from None
        return flow.submit(job, params, journal="project", **options)

    def resume(self, key: str, **options) -> int:
        """Submit again the job recorded under `key` (`JobSnapshot.key`) in the active project,
        with the params and options it was submitted with: one that was interrupted (the project
        or the application was closed) or canceled continues where it was. `options` say
        otherwise for this run, and bring what the record cannot keep (`on_terminal`)."""
        from ms_flow.engine.flow import named

        origin = self._project_flow().origin(key, journal="project")
        return self.submit(named(origin["job"]), origin["params"], key=key, **{**origin["options"], **options})

    def remote_jobs(self) -> list[dict]:
        """What the executor that plays `process` has on another machine, whichever session or
        project sent it and with or without a project open: see `RemoteExecutor.jobs_there`.
        Nothing when it runs its jobs here. It asks that machine, so not from a thread that
        cannot wait."""
        slot = self.engine._executors.get("process")
        return slot.impl.jobs_there() if hasattr(getattr(slot, "impl", None), "jobs_there") else []

    def remove_remote_job(self, dir: str) -> None:
        """Take one of `remote_jobs` out of the queue, with what it has there. One that a job of
        this session waits for is refused: cancel the job."""
        self.engine.executor("process").remove_there(dir)

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
                                 min(1.0, (row["done"] + row["failed"]) / row["total"]) if row["total"] else None)
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
