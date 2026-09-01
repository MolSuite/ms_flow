from __future__ import annotations

import logging
from typing import Any, Callable, Iterable, Optional

from ms_flow.core.executor.local_adapters import (
    ExecutorAdapterBase,
    ExternalExecutorAdapter,
    LokyProcessExecutorAdapter,
    ThreadExecutorAdapter,
)


class ExecutorRegistry:
    """Owns the registration lifecycle of executor adapters.

    Encapsulates the ``register_*``/``unregister`` logic over the shared
    ``_executors`` dict that the :class:`ExecutorManager` exposes to its readers
    (dispatch, health, CPU accounting). The dict is shared by reference — the
    registry mutates it; the manager keeps reading it directly.

    Runtime concerns are NOT owned here: heartbeat upserts and the
    running-chunk guard are injected as callbacks so the registry stays
    decoupled from the manager's full surface (forward-compatible with the
    RuntimeContext extraction).
    """

    def __init__(
        self,
        *,
        executors: dict[str, ExecutorAdapterBase],
        available_cpu: Callable[[], int],
        total_cpu: int,
        lock,
        on_heartbeat: Callable[..., None],
        running_executor_names: Callable[[], Iterable[str]],
        logger: Optional[logging.Logger] = None,
    ):
        self._executors = executors
        self._available_cpu = available_cpu
        self.total_cpu = int(total_cpu)
        self._lock = lock
        self._on_heartbeat = on_heartbeat
        self._running_executor_names = running_executor_names
        self.logger = logger or logging.getLogger("molsuite.executor.registry")

    # ------------------------------------------------------------------
    # CPU headroom (registration-time only)
    # ------------------------------------------------------------------

    def _check_cpu_headroom(self, requested_cpu: int, executor_name: str):
        headroom = self._available_cpu()
        if int(requested_cpu) > headroom:
            raise ValueError(
                f"Cannot register executor '{executor_name}': requested {requested_cpu} CPUs "
                f"but only {headroom} available."
            )

    # ------------------------------------------------------------------
    # Local executors
    # ------------------------------------------------------------------

    def register_thread(self, name: str = "thread", max_workers: int = 8):
        """Thread executor — always available, never reserves CPUs."""
        if name in self._executors:
            self.unregister(name)
        adapter = ThreadExecutorAdapter(name=name, max_workers=max_workers)
        self._executors[name] = adapter
        self._on_heartbeat(name)
        self.logger.info("Registered thread executor '%s' (max_workers=%s)", name, max_workers)

    def register_process_pool(
        self,
        name: str = "process_pool",
        *,
        max_workers: int | None = None,
        timeout_s: float = 10.0,
        kill_workers_on_shutdown: bool = True,
    ):
        """
        Reusable local loky process pool — the local-throughput compute backend.
        No CPUs are reserved at registration time; chunk dispatch consumes
        cpu_required units from _available_cpu (loky is a hard dependency).
        """
        if name in self._executors:
            self.unregister(name)
        resolved_max_workers = self.total_cpu if max_workers is None else max_workers
        adapter = LokyProcessExecutorAdapter(
            name=name,
            max_workers=resolved_max_workers,
            timeout_s=timeout_s,
            kill_workers_on_shutdown=kill_workers_on_shutdown,
        )
        self._executors[name] = adapter
        self._on_heartbeat(name)
        self.logger.info(
            "Registered loky process pool executor '%s' (max_workers=%s)",
            name,
            resolved_max_workers,
        )

    # ------------------------------------------------------------------
    # Distributed / external executors
    # ------------------------------------------------------------------

    def register_ray(
        self,
        name: str = "ray",
        mode: str = "external",
        cpus: int = 0,
        shared_fs: Optional[bool] = None,
        native: bool = False,
        address: Optional[str] = None,
        namespace: Optional[str] = None,
        runtime_env: Optional[dict[str, Any]] = None,
        gpu_slots_per_device: int = 1,
    ):
        """
        Ray executor.

        mode : "local" | "managed" | "external"
            local    — scheduler + workers on this machine. Reserves `cpus`.
            managed  — MF owns cluster startup; adapter attaches to its head.
            external — everything external. No reservation.
        cpus : required when mode="local".
        """
        if name in self._executors:
            self.unregister(name)
        if mode not in ("local", "managed", "external"):
            raise ValueError(f"Invalid Ray mode '{mode}'. Use 'local', 'managed', or 'external'.")
        reserved_cpu = 0
        if mode == "local":
            if cpus <= 0:
                raise ValueError("Ray mode='local' requires cpus > 0.")
            self._check_cpu_headroom(cpus, name)
            reserved_cpu = cpus
        if native:
            from ms_flow.core.executor.ray_adapter import RayExecutorAdapter

            adapter = RayExecutorAdapter(
                name=name,
                reserved_cpu=reserved_cpu,
                mode=mode,
                shared_fs=shared_fs,
                address=address,
                namespace=namespace,
                runtime_env=runtime_env,
                gpu_slots_per_device=gpu_slots_per_device,
            )
            self._executors[name] = adapter
            self._on_heartbeat(name)
            self.logger.info(
                "Registered Ray executor '%s' (native mode=%s reserved_cpu=%s)",
                name,
                mode,
                reserved_cpu,
            )
            return
        adapter = ExternalExecutorAdapter(
            name=name,
            reserved_cpu=reserved_cpu,
            backend="ray",
            mode=mode,
            shared_fs=shared_fs,
        )
        self._executors[name] = adapter
        self._on_heartbeat(name)
        self.logger.info("Registered Ray executor '%s' (mode=%s reserved_cpu=%s)", name, mode, reserved_cpu)


    def register_hpc(
        self,
        name: str = "hpc",
        shared_fs: bool = False,
        submit_command: str | list[str] | tuple[str, ...] | None = None,
        poll_command: str | list[str] | tuple[str, ...] | None = None,
        cancel_command: str | list[str] | tuple[str, ...] | None = None,
        poll_interval_s: float = 2.0,
        command_context: Optional[dict[str, Any]] = None,
        command_env: Optional[dict[str, str]] = None,
        python_executable: Optional[str] = None,
    ):
        """HPC executor — fully external. Supports command-based submit/poll/cancel."""
        if name in self._executors:
            self.unregister(name)
        if submit_command is None or poll_command is None:
            adapter = ExternalExecutorAdapter(
                name=name,
                reserved_cpu=0,
                backend="hpc",
                mode="external",
                shared_fs=shared_fs,
            )
            self._executors[name] = adapter
            self._on_heartbeat(name)
            self.logger.info("Registered HPC executor '%s' (stub external)", name)
            return

        from ms_flow.core.executor.hpc_adapter import HPCCommandExecutorAdapter

        adapter = HPCCommandExecutorAdapter(
            name=name,
            submit_command=submit_command,
            poll_command=poll_command,
            cancel_command=cancel_command,
            poll_interval_s=poll_interval_s,
            shared_fs=shared_fs,
            command_context=command_context,
            command_env=command_env,
            python_executable=python_executable,
        )
        self._executors[name] = adapter
        self._on_heartbeat(name)
        self.logger.info("Registered HPC executor '%s' (command-based external)", name)

    # ------------------------------------------------------------------
    # Removal
    # ------------------------------------------------------------------

    def unregister(self, name: str):
        """Remove an executor, releasing its reserved_cpu. Fails if chunks are running."""
        with self._lock:
            active = [n for n in self._running_executor_names() if n == name]
            if active:
                raise RuntimeError(
                    f"Cannot unregister executor '{name}': {len(active)} chunk(s) still running."
                )
            adapter = self._executors.pop(name, None)
        if adapter is None:
            return
        try:
            adapter.shutdown()
        except Exception:
            pass
        self._on_heartbeat(name, status="offline")
        self.logger.info("Unregistered executor '%s' (released reserved_cpu=%s)", name, adapter.reserved_cpu)
