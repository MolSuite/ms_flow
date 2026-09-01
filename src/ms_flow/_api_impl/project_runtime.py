from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Callable
from uuid import UUID

from ms_flow.core.database import ExecutorDB, ProjectStore
from ms_flow.core.database.master_models import Project
from ms_flow.core.executor.job_snapshot import JobSnapshot
from ms_flow.core.executor.manager import ExecutorManager
from ms_flow.core.executor.provisioning import cluster_address, cluster_launch_needed, ray_preflight
from ms_flow.core.project import ActiveProjectRuntime, ProjectDataContext, ProjectRuntimeState
from ms_flow.core.project.context import ProjectContext
from ms_flow.core.project.resources import ProjectResource
from ms_flow.core.project.manager import SettingsProfile

ACTIVE_PROJECT_JOB_STATUSES = ("pending", "pending_feed", "queued", "running", "staging", "cancel_requested")


class MolSuiteProjectRuntimeMixin:
    def get_project_resource_contract(self):
        return self._project_resource_contract

    def project_resource_specs(self):
        return self._project_resource_contract.specs

    def _resolve_project_root(self, project_id: str | UUID | None = None) -> Path | None:
        if project_id is not None:
            project = self._validate_project_access(self._normalize_id(project_id))
            return Path(project.path).expanduser().resolve()
        if self.active_context is None:
            return None
        return Path(self.active_context.path).expanduser().resolve()

    def list_project_resources(self, project_id: str | UUID | None = None) -> dict[str, ProjectResource]:
        project_root = self._resolve_project_root(project_id=project_id)
        if project_root is None:
            return {}
        return self._project_resource_contract.resolve(project_root)

    def get_project_resource(self, key: str, project_id: str | UUID | None = None) -> ProjectResource:
        project_root = self._resolve_project_root(project_id=project_id)
        if project_root is None:
            raise RuntimeError(f"There is no active project to resolve resource '{key}'.")
        return self._project_resource_contract.resolve_one(project_root, key)

    def get_project_resource_path(
        self,
        key: str,
        *parts: str | Path,
        project_id: str | UUID | None = None,
        create_parent: bool = False,
    ) -> Path:
        resource = self.get_project_resource(key, project_id=project_id)
        path = resource.path.joinpath(*(str(part) for part in parts))
        if create_parent:
            path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def get_project_resource_map(self, project_id: str | UUID | None = None) -> dict[str, dict[str, str]]:
        return {
            key: resource.to_mapping()
            for key, resource in self.list_project_resources(project_id=project_id).items()
        }

    def _merge_project_dirs(self, extra_dirs: list[str] | None = None) -> list[str]:
        names: list[str] = []
        for item in (*self._project_resource_contract.required_dirs(), *(tuple(extra_dirs or ()))):
            normalized = str(item).strip()
            if normalized and normalized not in names:
                names.append(normalized)
        return names

    def _resolve_executor_db_path(self, project_path: Path) -> Path:
        return project_path.expanduser().resolve() / self._executor_db_relative

    def _resolve_project_db_path(self, project_path: Path) -> Path:
        return Path(project_path).expanduser().resolve() / "project.db"

    def _register_configured_executors(
        self,
        executor_manager: ExecutorManager,
        resources,
        *,
        allow_remote_compute: bool = True,
    ) -> None:
        registered_names: set[str] = set()
        compute_backend = "loky"
        compute_options: dict[str, Any] = {"max_workers": resources.max_processes}
        for worker in self.settings_manager.settings.get_enabled_workers():
            worker_type = str(getattr(worker, "type", "")).strip().lower()
            worker_name = str(getattr(worker, "name", "")).strip()
            if not worker_name:
                continue
            if worker_name in registered_names:
                raise ValueError(f"Executor configurado duplicado: '{worker_name}'.")

            if worker_type == "thread":
                executor_manager.register_thread_executor(
                    name=worker_name,
                    max_workers=int(getattr(worker, "max_workers", resources.max_threads)),
                )
            elif worker_type == "process_pool":
                compute_backend = "loky"
                compute_options = {
                    "max_workers": int(getattr(worker, "max_workers", resources.max_processes)),
                }
            elif worker_type == "ray":
                ray_mode = str(getattr(worker, "mode", "external"))
                if ray_mode == "local":
                    ray_address = None
                elif ray_mode == "managed":
                    ray_address = cluster_address(worker)
                else:
                    ray_address = str(getattr(worker, "address", "")).strip() or None
                compute_backend = "ray"
                compute_options = {
                    "ray_mode": ray_mode,
                    "cpus": int(getattr(worker, "cpus", resources.cpus) or resources.cpus),
                    "address": ray_address,
                    "shared_fs": getattr(worker, "shared_fs", None),
                    "gpu_slots_per_device": int(getattr(worker, "gpu_slots_per_device", 1) or 1),
                    "needs_launch": cluster_launch_needed(worker),
                }
            elif worker_type == "hpc":
                executor_manager.register_hpc_executor(
                    name=worker_name,
                    shared_fs=bool(getattr(worker, "shared_fs", False)),
                    submit_command=getattr(worker, "submit_command", None),
                    poll_command=getattr(worker, "poll_command", None),
                    cancel_command=getattr(worker, "cancel_command", None),
                    poll_interval_s=float(getattr(worker, "poll_interval_s", 2.0)),
                    command_context=getattr(worker, "command_context", None),
                    command_env=getattr(worker, "command_env", None),
                    python_executable=getattr(worker, "python_executable", None),
                )
                self.app_logger.info(
                    "HPC worker '%s' registered as external executor; local orchestration remains thread-backed.",
                    worker_name,
                )
            else:
                self.app_logger.warning(
                    "Ignoring unsupported worker type '%s' for worker '%s'.",
                    worker_type,
                    worker_name,
                )
                continue
            registered_names.add(worker_name)

        if "thread" not in registered_names:
            executor_manager.register_thread_executor(
                name="thread",
                max_workers=resources.max_threads,
            )
        # Opening a project must never block on the network. Ray is only activated
        # here when it can be reached in ~2s; a cluster we would have to launch
        # ourselves (ray up) or one that is down falls back to loky and is left for
        # the user to activate from the monitor. Without this guard a stale
        # [workers.ray] address froze the whole app at startup for ~60s inside
        # ray.init's GCS reconnect loop.
        self.compute_backend_fallback = ""
        if compute_backend == "ray":
            if not allow_remote_compute:
                # Re-registering after a settings save must not start (nor tear down)
                # a cluster backend: that is an explicit action in the monitor.
                self.compute_backend_fallback = "Ray changes apply from the monitor (Executors tab)."
                return
            reason = self._ray_autostart_blocker(compute_options)
            if reason:
                self.compute_backend_fallback = reason
                self.app_logger.warning("Ray not auto-activated: %s Falling back to loky.", reason)
                compute_backend = "loky"
        if compute_backend == "ray":
            try:
                executor_manager.activate_compute_backend(
                    "ray",
                    ray_mode=str(compute_options.get("ray_mode", "external")),
                    cpus=int(compute_options.get("cpus", resources.cpus) or resources.cpus),
                    address=compute_options.get("address"),
                    shared_fs=compute_options.get("shared_fs"),
                    gpu_slots_per_device=int(compute_options.get("gpu_slots_per_device", 1) or 1),
                )
                return
            except Exception as exc:
                self.compute_backend_fallback = f"Ray activation failed: {exc}"
                self.app_logger.warning("%s Falling back to loky.", self.compute_backend_fallback)
        executor_manager.activate_compute_backend(
            "loky",
            max_workers=int(compute_options.get("max_workers", resources.max_processes)),
        )

    @staticmethod
    def _ray_autostart_blocker(compute_options: dict[str, Any]) -> str:
        """Return an empty string when Ray may be activated inline, else the reason."""
        address = compute_options.get("address")
        if compute_options.get("needs_launch"):
            error = ray_preflight(address)
            return "" if not error else f"managed cluster is not running ({error})."
        if str(compute_options.get("ray_mode", "external")).lower() == "local" and not address:
            return ""
        return ray_preflight(address)

    def reload_configured_executors(self, *, allow_remote_compute: bool = False) -> dict[str, Any]:
        """Re-apply the workers settings to the live manager (after saving settings).

        ``register_*`` replaces an executor of the same name, so this is just the
        registration pass again. Executors with running chunks make it fail loudly
        rather than silently keeping the old configuration.
        """
        manager = self._require_executor_manager()
        resources = self.settings_manager.settings.resources.local
        self._register_configured_executors(manager, resources, allow_remote_compute=allow_remote_compute)
        return {
            "executors": sorted(manager.registered_executors()),
            "fallback_reason": getattr(self, "compute_backend_fallback", ""),
        }

    def unregister_executor(self, name: str) -> None:
        self._require_executor_manager().unregister_executor(name)

    def _require_executor_manager(self) -> ExecutorManager:
        manager = self.executor_manager
        if manager is None:
            raise RuntimeError("ExecutorManager is not initialised. Activate a project first.")
        return manager

    def _create_executor_manager(self, executor_db: ExecutorDB) -> ExecutorManager:
        self.executor_logger = self.logging_manager.get_executor_logger("core")
        resources = self.settings_manager.settings.resources.local
        general = self.settings_manager.settings.general
        limits = self.settings_manager.settings.operational_limits
        executor_manager = ExecutorManager(
            executor_db=executor_db,
            master_db=self.master_db,
            total_cpu=resources.cpus,
            total_gpu=resources.gpus,
            poll_interval=float(general.poll_interval),
            progress_flush_interval=float(limits.progress_flush_interval_s),
            staging_max_workers=int(limits.staging_max_workers),
            max_inline_chunk_payload_bytes=int(limits.max_inline_chunk_payload_bytes),
            max_spool_payload_bytes=int(limits.max_spool_payload_bytes),
            logger=self.executor_logger,
        )
        executor_manager.configure_output_sink_limits(
            flush_retries=int(limits.output_sink_flush_retries),
            retry_backoff_s=float(limits.output_sink_retry_backoff_s),
            max_buffer_factor=int(limits.output_sink_max_buffer_factor),
            max_buffer_bytes=int(limits.output_sink_max_buffer_bytes),
            max_payload_bytes=int(limits.output_sink_max_payload_bytes),
            max_pending_chunks=int(limits.output_sink_max_pending_chunks),
            max_pending_bytes=int(limits.output_sink_max_pending_bytes),
        )
        self._register_configured_executors(executor_manager, resources)
        executor_manager.start()
        return executor_manager

    def _initialize_runtime_for_project(self, project_path: Path):
        desired_executor_db = self._resolve_executor_db_path(project_path)
        runtime = self._runtime_state
        active_project = runtime.active_project
        if (
            runtime.initialized
            and active_project is not None
            and runtime.executor_manager is not None
            and active_project.project_db is not None
            and active_project.executor_db is not None
            and active_project.executor_db.db_path == desired_executor_db
        ):
            return

        executor_db = ExecutorDB(desired_executor_db)
        try:
            executor_db.purge_finished_jobs()
        except Exception:
            pass  # retention is disk hygiene: it must never block opening the project
        project_db = ProjectStore()
        executor_manager = runtime.executor_manager
        previous_executor_db = active_project.executor_db if active_project is not None else None

        if executor_manager is None:
            executor_manager = self._create_executor_manager(executor_db)
        else:
            try:
                executor_manager.rebind_executor_db(executor_db)
            except Exception:
                try:
                    executor_db.dispose()
                except Exception:
                    pass
                raise
            if previous_executor_db is not None and previous_executor_db is not executor_db:
                try:
                    previous_executor_db.dispose()
                except Exception:
                    pass

        self._runtime_state = ProjectRuntimeState(
            executor_manager=executor_manager,
            active_project=ActiveProjectRuntime(
                context=self.active_context,
                executor_db=executor_db,
                project_db=project_db,
                project_store=None,
                executor_db_path=desired_executor_db,
            ),
        )
        self.app_logger.info("MolSuite runtime listo: executor=%s", desired_executor_db)

    def _shutdown_runtime_stack(self):
        runtime = self._runtime_state
        active_project = runtime.active_project
        try:
            if runtime.executor_manager is not None:
                runtime.executor_manager.stop()
        except Exception:
            pass
        self.executor_logger = None

        try:
            if active_project is not None and active_project.project_db is not None:
                active_project.project_db.disconnect()
        except Exception:
            pass

        try:
            if active_project is not None and active_project.executor_db is not None:
                active_project.executor_db.dispose()
        except Exception:
            pass

        self._runtime_state = ProjectRuntimeState()

    def _close_project_runtime(self):
        runtime = self._runtime_state
        manager = runtime.executor_manager
        active_project = runtime.active_project
        executor_db = active_project.executor_db if active_project is not None else None

        try:
            if active_project is not None and active_project.project_db is not None:
                active_project.project_db.disconnect()
        except Exception:
            pass

        if manager is not None:
            manager.unbind_executor_db()

        try:
            if executor_db is not None:
                executor_db.dispose()
        except Exception:
            pass

        self.logging_manager.clear_project_logging()
        self.project_logger = None
        self._runtime_state = ProjectRuntimeState(
            executor_manager=manager,
        )

    def _require_runtime(self):
        if not self._runtime_state.initialized:
            raise RuntimeError("Runtime is not initialised. Activate a project first.")

    def _require_active_project_runtime(self) -> ActiveProjectRuntime:
        runtime = self._runtime_state.active_project
        if runtime is None or runtime.context is None:
            raise RuntimeError("There is no active project.")
        return runtime

    def _require_executor_db(self) -> ExecutorDB:
        executor_db = self.executor_db
        if executor_db is None:
            raise RuntimeError("ExecutorDB no inicializada.")
        return executor_db

    def list_projects(self, page: int = 1, items_per_page: int = 20) -> list[Project]:
        return self.project_manager.repository.get_projects_paginated(page, items_per_page)

    def open_project(
        self,
        project_id: str | UUID,
        *,
        cancel_running_tasks_on_switch: bool = True,
        touch_master: bool = True,
        extra_dirs: list[str] | None = None,
    ) -> ProjectContext:
        normalized_id = self._normalize_id(project_id)
        self._validate_project_access(normalized_id)

        if self.active_context:
            if self.active_context.id == normalized_id:
                return self.active_context
            self.close_project(cancel_running_tasks=cancel_running_tasks_on_switch)

        context = self.project_manager.load_project_by_id(
            normalized_id,
            sm=self.settings_manager,
            extra_dirs=self._merge_project_dirs(extra_dirs),
        )
        self._activate_context(context, touch_master=touch_master)
        self.app_logger.info(
            "Project activated: id=%s name=%s",
            context.id,
            context.name,
            extra={"project_id": str(context.id)},
        )
        return context

    def create_project(
        self,
        name: str,
        folder: Path | str,
        description: str = "",
        tags: list[str] | None = None,
        base_settings: SettingsProfile = "global",
        scope: str = "full",
        activate: bool = True,
        extra_dirs: list[str] | None = None,
    ) -> ProjectContext:
        folder = Path(folder).expanduser().resolve()
        normalized_scope = str(scope).strip()
        effective_scope = normalized_scope or "full"
        if effective_scope == "full":
            effective_scope = self.app_id
        context = self.project_manager.create_project(
            name=name,
            folder=folder,
            sm=self.settings_manager,
            base=base_settings,
            description=description,
            scope=effective_scope,
            app_id=self.app_id,
            tags=tags,
            extra_dirs=self._merge_project_dirs(extra_dirs),
        )
        if not activate:
            return context

        if self.active_context and self.active_context.id != context.id:
            self.close_project(cancel_running_tasks=True)
        self._activate_context(context, touch_master=True)
        self.app_logger.info(
            "Project created and activated: id=%s name=%s",
            context.id,
            context.name,
            extra={"project_id": str(context.id)},
        )
        return context

    def find_project(
        self,
        *,
        name: str | None = None,
        folder: Path | str | None = None,
    ) -> Project | None:
        return self.project_manager.find_project(name=name, folder=folder)

    def create_or_open_project(
        self,
        *,
        name: str,
        folder: Path | str,
        description: str = "",
        tags: list[str] | None = None,
        base_settings: SettingsProfile = "global",
        scope: str = "full",
        cancel_running_tasks_on_switch: bool = True,
        activate: bool = True,
        extra_dirs: list[str] | None = None,
    ) -> ProjectContext:
        folder_path = Path(folder).expanduser().resolve()
        existing_at_path = self.project_manager.find_project_global(folder=folder_path)
        if existing_at_path is not None:
            existing_app_id = (existing_at_path.app_id or "").strip()
            if existing_app_id != self.app_id:
                raise ValueError(
                    f"Path '{folder_path}' already belongs to project '{existing_at_path.name}' "
                    f"of app_id='{existing_app_id or '<empty>'}'."
                )
            if activate:
                return self.open_project(
                    existing_at_path.id,
                    cancel_running_tasks_on_switch=cancel_running_tasks_on_switch,
                    touch_master=True,
                    extra_dirs=self._merge_project_dirs(extra_dirs),
                )
            return self.project_manager.load_project(
                project=existing_at_path,
                sm=self.settings_manager,
                extra_dirs=self._merge_project_dirs(extra_dirs),
            )
        existing = self.project_manager.find_project(name=name)
        if existing is not None:
            if activate:
                return self.open_project(
                    existing.id,
                    cancel_running_tasks_on_switch=cancel_running_tasks_on_switch,
                    touch_master=True,
                    extra_dirs=self._merge_project_dirs(extra_dirs),
                )
            return self.project_manager.load_project(
                project=existing,
                sm=self.settings_manager,
                extra_dirs=self._merge_project_dirs(extra_dirs),
            )
        return self.create_project(
            name=name,
            folder=folder_path,
            description=description,
            tags=tags,
            base_settings=base_settings,
            scope=scope,
            activate=activate,
            extra_dirs=self._merge_project_dirs(extra_dirs),
        )

    def register_task_canceller(self, project_id: str | UUID, canceller: Callable[[], None]):
        normalized_id = self._normalize_id(project_id)
        runtime = self._require_active_project_runtime()
        if runtime.context.id != normalized_id:
            raise RuntimeError("Cancellers can only be registered for the active project.")
        runtime.task_cancellers.append(canceller)

    def get_app_logger(self, name: str = "core"):
        return self.logging_manager.get_app_logger(name)

    def get_executor_logger(self, name: str = "core"):
        return self.logging_manager.get_executor_logger(name)

    def get_project_logger(self, name: str = "core"):
        return self.logging_manager.get_project_logger(name)

    def get_entity_loader_context(self) -> ProjectDataContext:
        return self.advanced.entity_loader_context()

    def get_project_data_context(self) -> ProjectDataContext:
        return self.advanced.project_data_context()

    def get_runtime_healthcheck(self) -> dict[str, Any]:
        if self.executor_manager is None:
            inactive_check = {
                "ok": False,
                "reason": "no_active_project",
            }
            return {
                "status": "inactive",
                "checks": {
                    "runtime": inactive_check,
                },
                "core_health": {
                    "status": "inactive",
                    "ok": False,
                    "checks": {"runtime": inactive_check},
                },
                "persistence_health": {
                    "status": "inactive",
                    "ok": False,
                    "checks": {"executor_db": {"ok": False, "bound": False}},
                },
                "sink_health": {
                    "status": "inactive",
                    "ok": True,
                    "checks": {},
                },
            }

        health = dict(self.executor_manager.get_healthcheck())
        if self.active_context is not None:
            health["project_id"] = str(self.active_context.id)
        return health

    def register_hpc_executor(
        self,
        *,
        name: str = "hpc",
        shared_fs: bool = False,
        submit_command: str | list[str] | tuple[str, ...] | None = None,
        poll_command: str | list[str] | tuple[str, ...] | None = None,
        cancel_command: str | list[str] | tuple[str, ...] | None = None,
        poll_interval_s: float = 2.0,
        command_env: dict[str, str] | None = None,
        python_executable: str | None = None,
    ) -> None:
        manager = self._require_executor_manager()
        manager.register_hpc_executor(
            name=name,
            shared_fs=shared_fs,
            submit_command=submit_command,
            poll_command=poll_command,
            cancel_command=cancel_command,
            poll_interval_s=poll_interval_s,
            command_env=command_env,
            python_executable=python_executable,
        )

    def register_ray_executor(
        self,
        *,
        name: str = "ray",
        mode: str = "external",
        cpus: int = 0,
        shared_fs: bool | None = None,
        native: bool = False,
        address: str | None = None,
        namespace: str | None = None,
        runtime_env: dict[str, Any] | None = None,
        gpu_slots_per_device: int = 1,
    ) -> None:
        manager = self._require_executor_manager()
        manager.register_ray_executor(
            name=name,
            mode=mode,
            cpus=cpus,
            shared_fs=shared_fs,
            native=native,
            address=address,
            namespace=namespace,
            runtime_env=runtime_env,
            gpu_slots_per_device=gpu_slots_per_device,
        )


    def refresh_master_db(self):
        self.master_db.reconnect()
        self.app_logger.debug("Master DB reconnected")

    def close_project(self, cancel_running_tasks: bool = True):
        if not self.active_context:
            self.settings_manager.clear_project()
            self.logging_manager.clear_project_logging()
            return

        context = self.active_context
        cancel_error = None
        if cancel_running_tasks:
            try:
                self._cancel_project_tasks(context.id)
                self.cancel_executor_jobs(
                    project_id=context.id,
                    statuses=ACTIVE_PROJECT_JOB_STATUSES,
                )
                self._wait_for_project_jobs_to_drain(context.id)
            except Exception as exc:
                cancel_error = exc
        else:
            remaining_jobs = self._list_active_project_jobs(context.id)
            if remaining_jobs:
                raise RuntimeError("The project cannot be closed while active jobs remain.")

        if cancel_error is not None:
            raise cancel_error

        self.project_manager.close_project(context)
        self._close_project_runtime()
        self.settings_manager.clear_project()
        self.app_logger.info(
            "Project closed: id=%s name=%s",
            context.id,
            context.name,
            extra={"project_id": str(context.id)},
        )

    def list_executor_jobs(
        self,
        statuses: tuple[str, ...] = ("pending", "running"),
        project_id: str | UUID | None = None,
    ) -> list[JobSnapshot]:
        if self.executor_manager is None:
            return []

        jobs: list[JobSnapshot] = []
        for status in statuses:
            jobs.extend(self.executor_manager.list_jobs(status=status))

        if project_id is None:
            return jobs

        normalized_id = str(self._normalize_id(project_id))
        return [job for job in jobs if job.project_id == normalized_id]

    def cancel_executor_jobs(
        self,
        *,
        project_id: str | UUID | None = None,
        statuses: tuple[str, ...] = ACTIVE_PROJECT_JOB_STATUSES,
    ) -> int:
        if self.executor_manager is None:
            return 0

        jobs = self.list_executor_jobs(statuses=statuses, project_id=project_id)
        canceled = 0
        for job in jobs:
            try:
                self.executor_manager.cancel_job(job.job_id)
                canceled += 1
            except Exception:
                continue
        if canceled:
            self.app_logger.warning(
                "Canceled executor jobs: count=%s project_id=%s",
                canceled,
                project_id,
                extra={"project_id": str(project_id) if project_id is not None else ""},
            )
        return canceled

    def _list_active_project_jobs(self, project_id: str | UUID | None) -> list[JobSnapshot]:
        active_jobs = self.list_executor_jobs(statuses=ACTIVE_PROJECT_JOB_STATUSES, project_id=project_id)
        seen = {job.job_id for job in active_jobs}
        for job in self._list_project_jobs_for_activity(project_id):
            if job.job_id in seen:
                continue
            if self._job_has_pending_storage(job):
                active_jobs.append(job)
                seen.add(job.job_id)
        return active_jobs

    def _list_project_jobs_for_activity(self, project_id: str | UUID | None) -> list[JobSnapshot]:
        if self.executor_manager is None:
            return []
        jobs = self.executor_manager.list_jobs(status=None)
        if project_id is None:
            return jobs
        normalized_id = str(self._normalize_id(project_id))
        return [job for job in jobs if job.project_id == normalized_id]

    @staticmethod
    def _job_has_pending_storage(job: JobSnapshot) -> bool:
        return (
            int(job.sink_lag_chunks or 0) > 0
            or int(job.sink_lag_bytes or 0) > 0
            or int(job.sink_buffered_items or 0) > 0
            or int(job.sink_buffered_bytes or 0) > 0
        )

    def _wait_for_project_jobs_to_drain(
        self,
        project_id: str | UUID,
        *,
        timeout_s: float = 30.0,
        poll_s: float | None = None,
    ) -> None:
        deadline = time.monotonic() + max(0.1, float(timeout_s))
        sleep_s = poll_s
        if sleep_s is None:
            manager = self.executor_manager
            sleep_s = manager.poll_interval if manager is not None else 0.1
        sleep_s = max(0.02, float(sleep_s))

        while time.monotonic() < deadline:
            jobs = self._list_active_project_jobs(project_id)
            if not jobs:
                return
            time.sleep(sleep_s)

        remaining = self._list_active_project_jobs(project_id)
        raise RuntimeError(
            f"Could not drain the project before the timeout; active jobs remaining={len(remaining)}."
        )

    def get_project_activity(self, project_id: str | UUID | None = None) -> dict[str, Any]:
        normalized_project_id = None
        if project_id is not None:
            normalized_project_id = self._normalize_id(project_id)
        elif self.active_context is not None:
            normalized_project_id = self.active_context.id

        jobs = self._list_active_project_jobs(normalized_project_id if normalized_project_id is not None else None)
        runtime = self._runtime_state.active_project
        cancellers = 0
        if runtime is not None and runtime.context is not None:
            if normalized_project_id is None or runtime.context.id == normalized_project_id:
                cancellers = len(runtime.task_cancellers or [])
        storage_pending_jobs = [job for job in jobs if self._job_has_pending_storage(job)]
        sink_lag_chunks = sum(int(job.sink_lag_chunks or 0) for job in jobs)
        sink_lag_bytes = sum(int(job.sink_lag_bytes or 0) for job in jobs)
        sink_buffered_items = sum(int(job.sink_buffered_items or 0) for job in jobs)
        sink_buffered_bytes = sum(int(job.sink_buffered_bytes or 0) for job in jobs)
        active = bool(jobs or cancellers)
        return {
            "project_id": str(normalized_project_id) if normalized_project_id is not None else None,
            "active": active,
            "can_switch_project": not active,
            "jobs_active": len(jobs),
            "job_ids": [job.job_id for job in jobs],
            "statuses": sorted({job.status for job in jobs if job.status}),
            "storage_pending_jobs": len(storage_pending_jobs),
            "storage_pending_job_ids": [job.job_id for job in storage_pending_jobs],
            "sink_lag_chunks": sink_lag_chunks,
            "sink_lag_bytes": sink_lag_bytes,
            "sink_buffered_items": sink_buffered_items,
            "sink_buffered_bytes": sink_buffered_bytes,
            "external_cancellers": cancellers,
        }

    def get_project_switch_status(self, project_id: str | UUID | None = None) -> dict[str, Any]:
        activity = self.get_project_activity(project_id=project_id)
        reasons: list[str] = []
        if int(activity.get("jobs_active", 0) or 0) > 0:
            reasons.append("jobs_active")
        if int(activity.get("storage_pending_jobs", 0) or 0) > 0:
            reasons.append("storage_pending")
        if int(activity.get("external_cancellers", 0) or 0) > 0:
            reasons.append("external_cancellers")
        return {
            **activity,
            "can_switch": not bool(activity.get("active", False)),
            "block_reasons": reasons,
        }

    def can_switch_project(self, project_id: str | UUID | None = None) -> bool:
        return bool(self.get_project_switch_status(project_id=project_id)["can_switch"])

    def _cancel_project_tasks(self, project_id: UUID):
        runtime = self._runtime_state.active_project
        if runtime is None or runtime.context is None or runtime.context.id != project_id:
            return
        cancellers = list(runtime.task_cancellers or [])
        runtime.task_cancellers = []
        errors = []
        for cancel in cancellers:
            try:
                cancel()
            except Exception as exc:
                errors.append(exc)
        if errors:
            raise RuntimeError(f"Cancellation failed for {len(errors)} task(s).")

    def delete_projects(self, project_ids: list[str | UUID], delete_files: bool = True):
        normalized_ids = []
        for project_id in project_ids:
            try:
                normalized_ids.append(self._normalize_id(project_id))
            except ValueError:
                continue

        if not normalized_ids:
            return

        if self.active_context and self.active_context.id in normalized_ids:
            self.close_project()

        self.project_manager.delete_projects(normalized_ids, delete_files=delete_files)
        self.app_logger.info("Projects deleted: count=%s delete_files=%s", len(normalized_ids), delete_files)

    def _activate_context(self, context: ProjectContext, touch_master: bool):
        self._initialize_runtime_for_project(context.path)
        runtime = self._runtime_state
        active_project = runtime.active_project
        if active_project is None or active_project.project_db is None:
            raise RuntimeError("Runtime is incomplete for activating a project context.")

        active_project.project_db.connect(context.path)
        active_project.project_store = active_project.project_db
        active_project.context = context
        active_project.task_cancellers.clear()
        self.logging_manager.set_project_logging(context.path)
        active_project.project_logger = self.logging_manager.get_project_logger("core")
        self.project_logger.info(
            "Project context active: id=%s path=%s",
            context.id,
            context.path,
            extra={"project_id": str(context.id)},
        )
        self.app_logger.info(
            "Project runtime active for project=%s",
            context.id,
            extra={"project_id": str(context.id)},
        )
        if touch_master:
            self.project_manager.touch_project(context.id)

    def shutdown(self):
        try:
            self.close_project(cancel_running_tasks=True)
        except Exception:
            pass
        self._shutdown_runtime_stack()
        try:
            self.logging_manager.stop()
        except Exception:
            pass


__all__ = ["ACTIVE_PROJECT_JOB_STATUSES", "MolSuiteProjectRuntimeMixin"]
