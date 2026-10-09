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
            engine = Engine(cpus=local.cpus, gpus=local.gpus, state_dir=self._master_db_path.parent)
            engine.register("process", ProcessExecutor(local.cpus))
            engine.register("thread", ThreadExecutor(local.cpus))
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

    def get_project_store(self):
        return self.advanced.project_store


__all__ = ["MolSuiteJobApiMixin"]
