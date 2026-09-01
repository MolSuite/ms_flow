"""Dynamic compute-backend control: loky (single machine) <-> ray (multi machine).

mf can run heterogeneous executors at once, but for the real workflow you want
exactly ONE *compute* backend live at a time: loky for a single PC (fast start,
in-process pool) or ray for distributed work across machines. This coordinates the
register/unregister lifecycle behind a single stable executor name, so submitters
always target ``"compute"`` regardless of which backend is active underneath.

Switching while work is still running is a policy decision (kill vs refuse), exposed
as ``kill_running`` — a UI would surface that as a "kill running jobs?" prompt.
After a switch the new backend is health-probed (a no-op round-trip) before returning,
so callers know it actually executes.
"""

from __future__ import annotations

import time
import uuid
from typing import Any, Optional

from .runner_refs import normalize_runner



def _healthcheck_noop(payload: dict) -> dict:
    """Trivial runnable used to confirm a freshly-activated backend executes."""
    return {"ok": True, "probe": payload.get("probe")}


class ComputeBackendController:
    """Owns the logical ``compute`` executor slot.

    The public operation is intentionally one verb: activate the desired backend.
    If no backend is active, activation registers it. If another backend is active,
    activation applies the requested transition policy and switches the logical
    ``compute`` executor to the new implementation.
    """

    LOKY = "loky"
    RAY = "ray"
    _BACKENDS = (LOKY, RAY)
    _POLICIES = ("refuse_if_busy", "cancel_and_wait", "drain_and_wait")

    def __init__(self, manager: Any, *, executor_name: str = "compute"):
        self._manager = manager
        self._name = executor_name
        self._active: Optional[str] = None
        self._state = "inactive"
        self._last_error = ""

    @property
    def active(self) -> Optional[str]:
        return self._active

    @property
    def executor_name(self) -> str:
        return self._name

    @property
    def state(self) -> str:
        return self._state

    def status(self) -> dict[str, Any]:
        return {
            "executor_name": self._name,
            "backend": self._active,
            "state": self._state,
            "last_error": self._last_error,
            "active_jobs": len(self._active_jobs()) if self._active else 0,
            "running_chunks": self._running_chunks() if self._active else 0,
        }

    # ------------------------------------------------------------------

    def _active_jobs(self) -> list:
        return [
            job
            for job in self._manager.list_jobs()
            if job.executor_name == self._name and not job.is_terminal
        ]

    def _running_chunks(self) -> int:
        return sum(
            1 for c in self._manager.running_chunks_snapshot() if c.executor_name == self._name
        )

    def _wait_drained(self, timeout_s: float) -> None:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if not self._active_jobs() and self._running_chunks() == 0:
                return
            time.sleep(0.05)
        raise RuntimeError(
            f"Backend '{self._active}' did not drain within {timeout_s}s "
            f"(active_jobs={len(self._active_jobs())}, running_chunks={self._running_chunks()})."
        )

    def _probe_health(self, timeout_s: float) -> bool:
        # Drive the adapter directly (its own submit/poll future registry) instead
        # of submit_job, so a backend switch never leaves a technical row in the
        # job DB / metrics / UI. No scheduler admission, no CPU tokens consumed.
        if not self._manager.manager_thread_alive():
            return False
        adapter = self._manager.registered_executors().get(self._name)
        if adapter is None:
            return False
        probe_id = uuid.uuid4().hex
        try:
            handle = adapter.submit(
                job_id=f"__healthcheck__{probe_id}",
                chunk_id=probe_id,
                payload={"probe": probe_id},
                fn_ref=normalize_runner(_healthcheck_noop),
                progress_cb=None,
            )
            deadline = time.time() + timeout_s
            while time.time() < deadline:
                state, _result, _error = adapter.poll(handle)
                if state == "DONE":
                    return True
                if state == "FAILED":
                    return False
                time.sleep(0.05)
            adapter.cancel(handle)
            return False
        except Exception:
            return False

    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_policy(policy: str | None, kill_running: bool) -> str:
        if policy is None:
            return "cancel_and_wait" if kill_running else "refuse_if_busy"
        policy = str(policy).strip().lower()
        if policy not in ComputeBackendController._POLICIES:
            raise ValueError(
                f"Unknown compute backend transition policy '{policy}'. "
                f"Use one of {ComputeBackendController._POLICIES}."
            )
        return policy

    def _prepare_existing_backend(self, *, policy: str, timeout_s: float) -> None:
        if self._active is None:
            return
        running = self._active_jobs()
        if running and policy == "refuse_if_busy":
            raise RuntimeError(
                f"{len(running)} job(s) still running on backend '{self._active}'. "
                "Use policy='cancel_and_wait' or policy='drain_and_wait' to switch."
            )
        if running and policy == "cancel_and_wait":
            for job in running:
                self._manager.cancel_job(job.job_id)
        self._state = "draining"
        self._wait_drained(timeout_s)
        self._manager.unregister_executor(self._name)
        self._active = None

    def _register_backend(
        self,
        backend: str,
        *,
        max_workers: int | None,
        cpus: int | None,
        ray_mode: str,
        address: str | None,
        ray_opts: dict[str, Any],
    ) -> None:
        if backend == self.LOKY:
            self._manager.register_process_pool_executor(
                name=self._name,
                max_workers=max_workers if max_workers is not None else self._manager.total_cpu,
            )
            return
        resolved_cpus = cpus if cpus is not None else self._manager.total_cpu
        self._manager.register_ray_executor(
            name=self._name,
            mode=ray_mode,
            cpus=resolved_cpus if ray_mode == "local" else 0,
            native=True,
            address=address,
            **ray_opts,
        )

    def activate(
        self,
        backend: str,
        *,
        policy: str | None = None,
        kill_running: bool = False,
        wait_healthy_s: float = 30.0,
        max_workers: int | None = None,
        cpus: int | None = None,
        ray_mode: str = "local",
        address: str | None = None,
        **ray_opts: Any,
    ) -> dict:
        """Make ``backend`` the live compute backend. Returns a small status dict.

        ``policy`` controls transition when another backend is active:
        ``refuse_if_busy`` (default), ``cancel_and_wait`` or ``drain_and_wait``.
        ``kill_running`` is retained as compatibility shorthand for
        ``policy='cancel_and_wait'``.
        """
        backend = str(backend).strip().lower()
        if backend not in self._BACKENDS:
            raise ValueError(f"Unknown backend '{backend}'. Use one of {self._BACKENDS}.")
        resolved_policy = self._resolve_policy(policy, kill_running)

        if backend == self._active:
            if self._manager.manager_thread_alive():
                self._state = "checking"
                healthy = self._probe_health(wait_healthy_s)
            else:
                healthy = False
                self._state = "registered"
            self._state = "healthy" if healthy else "unhealthy"
            if not self._manager.manager_thread_alive():
                self._state = "registered"
            self._last_error = "" if healthy or self._state == "registered" else f"Healthcheck failed for compute backend '{backend}'."
            return {**self.status(), "changed": False, "healthy": healthy}

        previous = self._active
        try:
            self._state = "switching" if previous is not None else "starting"
            self._prepare_existing_backend(policy=resolved_policy, timeout_s=wait_healthy_s)
            self._register_backend(
                backend,
                max_workers=max_workers,
                cpus=cpus,
                ray_mode=ray_mode,
                address=address,
                ray_opts=ray_opts,
            )
        except Exception as exc:
            self._state = "failed"
            self._last_error = str(exc)
            raise

        self._active = backend

        if self._manager.manager_thread_alive():
            healthy = self._probe_health(wait_healthy_s)
            self._state = "healthy" if healthy else "unhealthy"
        else:
            healthy = False
            self._state = "registered"
        self._last_error = "" if healthy or self._state == "registered" else f"Healthcheck failed for compute backend '{backend}'."
        return {**self.status(), "changed": True, "healthy": healthy}


# Compatibility alias while callers migrate to the controller name.
ComputeBackendSelector = ComputeBackendController
