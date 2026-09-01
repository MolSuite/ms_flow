from __future__ import annotations

import multiprocessing as mp
import queue
import threading
import traceback
import uuid
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional, Tuple

from ms_flow.core.executor.runner_refs import (
    ProgressCallback,
    RunnerRef,
    drain_progress_queue,
    make_runner_call,
    process_worker_entry,
    resolve_runner,
)


def _failure_detail(exc: Exception) -> str:
    """Chunk error string carrying the semantic message on the first line and the full
    (remote) traceback below it. ProcessPool/loky re-raise with the worker traceback chained,
    so format_exc() captures where the task actually failed — not just this poll frame."""
    tb = traceback.format_exc()
    message = str(exc)
    return f"{message}\n\n{tb}" if tb and "NoneType: None" not in tb else message

try:
    from loky import ProcessPoolExecutor as LokyProcessPoolExecutor
except Exception:  # pragma: no cover - optional dependency
    LokyProcessPoolExecutor = None




def pooled_process_worker_entry(
    fn_ref: RunnerRef,
    payload: dict,
    progress_queue=None,
    handle_id: str = "",
    chunk_id: str = "",
) -> Any:
    def _pooled_progress_cb(value: float) -> None:
        if progress_queue is None or not handle_id:
            return
        try:
            progress_queue.put_nowait(
                {
                    "handle_id": str(handle_id),
                    "chunk_id": str(chunk_id),
                    "progress": float(value),
                }
            )
        except Exception:
            return None

    fn = resolve_runner(fn_ref)
    return make_runner_call(fn, payload, _pooled_progress_cb)


@dataclass(frozen=True)
class ExecutorAdapterMetadata:
    backend: str
    mode: str
    support_level: str
    shared_filesystem: bool
    consumes_local_cpu_tokens: bool
    supports_inline: bool
    supports_bytes: bool
    supports_file_input: bool
    supports_db_input: bool

    def to_mapping(self) -> dict[str, object]:
        return asdict(self)


class ExecutorAdapterBase:
    def __init__(self, name: str, reserved_cpu: int = 0):
        self.name = name
        self.reserved_cpu = max(0, reserved_cpu)

    @property
    def backend_name(self) -> str:
        return "generic"

    @property
    def execution_mode(self) -> str:
        return "external"

    @property
    def has_shared_filesystem(self) -> bool:
        return False

    @property
    def consumes_local_cpu_tokens(self) -> bool:
        return False

    @property
    def support_level(self) -> str:
        return "stable"

    @property
    def metadata(self) -> ExecutorAdapterMetadata:
        backend = self.backend_name
        mode = self.execution_mode
        shared_fs = self.has_shared_filesystem
        return ExecutorAdapterMetadata(
            backend=backend,
            mode=mode,
            support_level=self.support_level,
            shared_filesystem=shared_fs,
            consumes_local_cpu_tokens=self.consumes_local_cpu_tokens,
            supports_inline=True,
            supports_bytes=True,
            supports_file_input=shared_fs or backend in {"hpc", "ray"} or mode == "local",
            supports_db_input=True,
        )

    @property
    def integration_kind(self) -> str:
        return "builtin"

    def health_snapshot(self) -> dict[str, Any]:
        return {"ok": True, "integration": self.integration_kind}

    def submit(
        self,
        job_id: str,
        chunk_id: str,
        payload: dict,
        fn_ref: RunnerRef,
        progress_cb: ProgressCallback,
        submit_context: Optional[dict[str, Any]] = None,
    ) -> str:
        raise NotImplementedError

    def poll(self, handle_id: str) -> Tuple[str, Optional[dict], Optional[str]]:
        raise NotImplementedError

    def cancel(self, handle_id: str) -> bool:
        raise NotImplementedError

    def shutdown(self):
        raise NotImplementedError


class ThreadExecutorAdapter(ExecutorAdapterBase):
    def __init__(self, name: str, max_workers: int):
        super().__init__(name=name, reserved_cpu=0)
        self._pool = ThreadPoolExecutor(
            max_workers=max(1, int(max_workers)),
            thread_name_prefix=f"{name}-",
        )
        self._futures: Dict[str, Future] = {}
        self._lock = threading.Lock()

    @property
    def backend_name(self) -> str:
        return "thread"

    @property
    def execution_mode(self) -> str:
        return "local"

    @property
    def has_shared_filesystem(self) -> bool:
        return True

    def submit(self, job_id, chunk_id, payload, fn_ref: RunnerRef, progress_cb, submit_context=None) -> str:
        del job_id, chunk_id, submit_context
        handle_id = uuid.uuid4().hex
        fn = resolve_runner(fn_ref)
        future = self._pool.submit(make_runner_call, fn, payload, progress_cb)
        with self._lock:
            self._futures[handle_id] = future
        return handle_id

    def poll(self, handle_id: str) -> Tuple[str, Optional[dict], Optional[str]]:
        with self._lock:
            future = self._futures.get(handle_id)
        if future is None:
            return "FAILED", None, "Unknown handle"
        if not future.done():
            return "RUNNING", None, None
        try:
            result = future.result()
            with self._lock:
                self._futures.pop(handle_id, None)
            return "DONE", {"result": result}, None
        except Exception as exc:
            detail = _failure_detail(exc)
            with self._lock:
                self._futures.pop(handle_id, None)
            return "FAILED", None, detail

    def cancel(self, handle_id: str) -> bool:
        with self._lock:
            future = self._futures.get(handle_id)
        return bool(future and future.cancel())

    def shutdown(self):
        self._pool.shutdown(wait=False, cancel_futures=True)




class LokyProcessExecutorAdapter(ExecutorAdapterBase):
    def __init__(
        self,
        name: str,
        *,
        max_workers: int,
        timeout_s: float = 10.0,
        kill_workers_on_shutdown: bool = True,
    ):
        if LokyProcessPoolExecutor is None:
            raise RuntimeError("loky is not installed. Install 'loky' to use LokyProcessExecutorAdapter.")
        super().__init__(name=name, reserved_cpu=0)
        self._max_workers = max(1, int(max_workers))
        self._timeout_s = max(0.0, float(timeout_s))
        self._kill_workers_on_shutdown = bool(kill_workers_on_shutdown)
        self._manager = None
        self._progress_queue = None
        self._pool = None
        self._futures: Dict[str, Future] = {}
        self._handle_job_ids: Dict[str, str] = {}
        self._latest_progress: Dict[str, float] = {}
        self._lock = threading.Lock()

    @property
    def backend_name(self) -> str:
        return "process_pool_loky"

    @property
    def execution_mode(self) -> str:
        return "local"

    @property
    def has_shared_filesystem(self) -> bool:
        return True

    @property
    def consumes_local_cpu_tokens(self) -> bool:
        return True

    @property
    def support_level(self) -> str:
        return "experimental"

    def health_snapshot(self) -> dict[str, Any]:
        snapshot = super().health_snapshot()
        snapshot.update(
            {
                "backend": self.backend_name,
                "max_workers": self._max_workers,
                "timeout_s": self._timeout_s,
                "kill_workers_on_shutdown": self._kill_workers_on_shutdown,
                "initialized": self._pool is not None,
                "inflight_handles": len(self._futures),
            }
        )
        return snapshot

    def _ensure_runtime_objects_locked(self) -> None:
        if self._manager is None:
            self._manager = mp.Manager()
        if self._progress_queue is None:
            self._progress_queue = self._manager.Queue()
        if self._pool is None:
            self._pool = self._build_pool()

    def _build_pool(self):
        return LokyProcessPoolExecutor(
            max_workers=self._max_workers,
            timeout=self._timeout_s,
        )

    def _shutdown_pool(self, pool) -> None:
        if pool is None:
            return
        try:
            pool.shutdown(wait=True, kill_workers=self._kill_workers_on_shutdown)
        except Exception:
            try:
                pool.shutdown(wait=False, kill_workers=self._kill_workers_on_shutdown)
            except Exception:
                pass

    def _shutdown_manager(self) -> None:
        manager = self._manager
        self._manager = None
        self._progress_queue = None
        if manager is None:
            return
        try:
            manager.shutdown()
        except Exception:
            pass

    def _drain_shared_progress_queue(self) -> None:
        if self._progress_queue is None:
            return
        while True:
            try:
                msg = self._progress_queue.get_nowait()
            except queue.Empty:
                break
            except Exception:
                break
            handle_id = str(msg.get("handle_id") or "").strip()
            if not handle_id:
                continue
            try:
                self._latest_progress[handle_id] = float(msg.get("progress"))
            except Exception:
                continue

    def _all_inflight_handles_belong_to(self, job_id: str) -> bool:
        job_id_text = str(job_id or "").strip()
        if not job_id_text:
            return False
        return bool(self._handle_job_ids) and all(value == job_id_text for value in self._handle_job_ids.values())

    def _reset_pool_locked(self) -> None:
        old_pool = self._pool
        self._pool = None
        self._futures.clear()
        self._handle_job_ids.clear()
        self._latest_progress.clear()
        self._shutdown_pool(old_pool)
        self._ensure_runtime_objects_locked()

    def submit(self, job_id, chunk_id, payload, fn_ref: RunnerRef, progress_cb, submit_context=None) -> str:
        del progress_cb, submit_context
        handle_id = uuid.uuid4().hex
        with self._lock:
            self._ensure_runtime_objects_locked()
            future = self._pool.submit(
                pooled_process_worker_entry,
                fn_ref,
                payload,
                self._progress_queue,
                handle_id,
                str(chunk_id or ""),
            )
            self._futures[handle_id] = future
            self._handle_job_ids[handle_id] = str(job_id or "")
        return handle_id

    def drain_progress(self, handle_id: str) -> Optional[float]:
        with self._lock:
            self._drain_shared_progress_queue()
            return self._latest_progress.get(handle_id)

    def poll(self, handle_id: str) -> Tuple[str, Optional[dict], Optional[str]]:
        with self._lock:
            self._drain_shared_progress_queue()
            future = self._futures.get(handle_id)
        if future is None:
            return "FAILED", None, "Unknown handle"
        if not future.done():
            return "RUNNING", None, None
        if future.cancelled():
            with self._lock:
                self._futures.pop(handle_id, None)
                self._handle_job_ids.pop(handle_id, None)
                self._latest_progress.pop(handle_id, None)
            return "FAILED", None, "Task canceled before execution"
        try:
            result = future.result()
            with self._lock:
                self._futures.pop(handle_id, None)
                self._handle_job_ids.pop(handle_id, None)
                self._latest_progress.pop(handle_id, None)
            return "DONE", {"result": result}, None
        except Exception as exc:
            detail = _failure_detail(exc)
            with self._lock:
                self._futures.pop(handle_id, None)
                self._handle_job_ids.pop(handle_id, None)
                self._latest_progress.pop(handle_id, None)
            return "FAILED", None, detail

    def cancel(self, handle_id: str) -> bool:
        with self._lock:
            future = self._futures.get(handle_id)
            job_id = self._handle_job_ids.get(handle_id, "")
        if future is None:
            return False
        if future.cancel():
            with self._lock:
                self._futures.pop(handle_id, None)
                self._handle_job_ids.pop(handle_id, None)
                self._latest_progress.pop(handle_id, None)
            return True
        with self._lock:
            if future.done():
                return False
            if not self._all_inflight_handles_belong_to(job_id):
                return False
            self._reset_pool_locked()
        return True

    def shutdown(self):
        with self._lock:
            old_pool = self._pool
            self._pool = None
            self._futures.clear()
            self._handle_job_ids.clear()
            self._latest_progress.clear()
        self._shutdown_pool(old_pool)
        self._shutdown_manager()


class ExternalExecutorAdapter(ExecutorAdapterBase):
    def __init__(
        self,
        name: str,
        reserved_cpu: int = 0,
        backend: str = "generic",
        mode: str = "external",
        shared_fs: Optional[bool] = None,
    ):
        super().__init__(name=name, reserved_cpu=reserved_cpu)
        self.backend = backend
        self.mode = mode
        self.shared_fs = bool(shared_fs) if shared_fs is not None else (mode == "local")

    @property
    def backend_name(self) -> str:
        return str(self.backend or "external").strip().lower()

    @property
    def execution_mode(self) -> str:
        return str(self.mode or "external").strip().lower()

    @property
    def has_shared_filesystem(self) -> bool:
        return bool(self.shared_fs)

    @property
    def integration_kind(self) -> str:
        return "stub"

    @property
    def support_level(self) -> str:
        return "experimental"

    def submit(self, job_id, chunk_id, payload, fn_ref: RunnerRef, progress_cb, submit_context=None) -> str:
        del job_id, chunk_id, payload, fn_ref, progress_cb, submit_context
        raise NotImplementedError(f"ExternalExecutorAdapter '{self.name}' has no submit implementation.")

    def poll(self, handle_id: str) -> Tuple[str, Optional[dict], Optional[str]]:
        raise NotImplementedError

    def cancel(self, handle_id: str) -> bool:
        return False

    def shutdown(self):
        pass


__all__ = [
    "ExecutorAdapterBase",
    "ExternalExecutorAdapter",
    "LokyProcessExecutorAdapter",
    "ThreadExecutorAdapter",
]
