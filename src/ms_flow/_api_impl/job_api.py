from __future__ import annotations

import os
import shutil
from pathlib import Path

_LOGIN_NODE = ("this is the login node of a cluster, where the pool of processes does not run: set "
               "`resources.process.kind` to `queue` or `ray`, or `resources.process.on_login_node` to force it")


def _login_node() -> bool:
    # ponytail: Slurm is the one told apart; the commands of another scheduler when a cluster has one.
    return shutil.which("sbatch") is not None and "SLURM_JOB_ID" not in os.environ


def _process_executor(resources):
    """The executor `resources.process` names, or None for the pool of a login node."""
    role = resources.process
    if role.kind == "ray":
        from ms_flow.engine import RayExecutor

        return RayExecutor(role.ray.address or None, shared=role.ray.shared_disk)
    if role.kind == "queue":
        from ms_flow.remote import JOB_SH, RemoteExecutor

        queue = role.queue
        return RemoteExecutor(queue.host or None, workdir=queue.workdir, scheduler=queue.scheduler,
                              template=Path(queue.template).read_text() if queue.template else JOB_SH,
                              python=queue.python, setup=queue.setup, runner=queue.runner,
                              elements_per_job=queue.elements_per_job or None, max_jobs=queue.max_jobs or None,
                              poll_s=queue.poll_seconds, shared=queue.shared_disk)
    if _login_node() and not role.on_login_node:
        return None
    from ms_flow.engine import ProcessExecutor

    return ProcessExecutor(resources.local.max_processes)


class WorkspaceJobApiMixin:
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
        executor = _process_executor(self.settings_manager.settings.resources)
        if executor is not None:
            self._engine.register("process", executor)

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

    def job_failures(self, key: str, limit: int = 200) -> list[dict]:
        """`attempt`, `error` (the traceback) and `input` (a short repr of the element) of the
        chunks that failed in the job recorded under `key` (`JobSnapshot.key`) in the active
        project, at most `limit`."""
        from ms_flow.engine.journal import failures

        return failures(self._require_active_project_runtime().context.path / "executor.db", key, limit)

    def get_project_store(self):
        return self.advanced.project_store


__all__ = ["WorkspaceJobApiMixin"]
