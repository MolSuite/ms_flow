from __future__ import annotations


class MolSuiteJobApiMixin:
    @property
    def engine(self):
        """The engine behind `submit`, built at first use: the one that shares out this machine.

        Raises `ms_flow.engine.Busy` when another process of this user already runs one.
        """
        if self._engine is None:
            from ms_flow.engine import Engine, ProcessExecutor, ThreadExecutor

            local = self.settings_manager.settings.resources.local
            engine = Engine(cpus=local.cpus, gpus=local.gpus, state_dir=self._master_db_path.parent,
                            mem_mb=local.memory_gb * 1024 if local.memory_gb else None)
            engine.register("process", ProcessExecutor(local.max_processes))
            engine.register("thread", ThreadExecutor(local.max_threads))
            self._engine = engine
        return self._engine

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
        return self._project_flow().submit(job, params, journal="project", **options)

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


__all__ = ["MolSuiteJobApiMixin"]
