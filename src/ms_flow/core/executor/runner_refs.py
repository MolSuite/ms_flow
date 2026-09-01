from __future__ import annotations

import inspect
import multiprocessing as mp
import queue
import traceback
from typing import Any, Callable, Dict, Optional, Union
from uuid import UUID

RunnerRef = Dict[str, str]
ProgressCallback = Callable[[float], None]


def normalize_uuid(value: Optional[Union[UUID, str]]) -> Optional[UUID]:
    if value is None:
        return None
    return UUID(str(value))


def callable_to_ref(fn: Callable) -> RunnerRef:
    module = getattr(fn, "__module__", None)
    qualname = getattr(fn, "__qualname__", None)

    if not module or not qualname:
        raise ValueError(f"Cannot create a runner reference for {fn!r}: missing __module__ or __qualname__.")
    if module == "__main__":
        raise ValueError(
            f"Cannot create a runner reference for '{qualname}': defined in __main__. "
            "Move the function to an installed module."
        )
    if "<" in qualname:
        raise ValueError(
            f"Cannot create a runner reference for '{qualname}': "
            "lambdas and nested functions are not supported."
        )
    return {"module": module, "fn": qualname}


def ref_to_str(ref: RunnerRef) -> str:
    return f"{ref['module']}:{ref['fn']}"


def str_to_ref(raw: str) -> RunnerRef:
    if ":" not in raw:
        raise ValueError(f"Invalid runner reference '{raw}'. Expected 'module:function'.")
    module, fn = raw.rsplit(":", 1)
    return {"module": module, "fn": fn}


def normalize_runner(run_chunk: Union[Callable, str, RunnerRef]) -> RunnerRef:
    if callable(run_chunk):
        return callable_to_ref(run_chunk)
    if isinstance(run_chunk, str):
        return str_to_ref(run_chunk)
    if isinstance(run_chunk, dict) and "module" in run_chunk and "fn" in run_chunk:
        return run_chunk
    raise ValueError(f"Invalid run_chunk: {run_chunk!r}. Pass a callable, 'module:fn' string, or RunnerRef dict.")


def resolve_runner(ref: RunnerRef) -> Callable:
    import importlib

    try:
        mod = importlib.import_module(ref["module"])
    except ImportError as exc:
        raise ImportError(f"Cannot import module '{ref['module']}': {exc}") from exc

    fn_name = ref["fn"]
    obj = mod
    for attr in fn_name.split("."):
        try:
            obj = getattr(obj, attr)
        except AttributeError as exc:
            raise AttributeError(f"Module '{ref['module']}' has no attribute '{fn_name}'.") from exc
    if not callable(obj):
        raise TypeError(f"'{ref['module']}:{fn_name}' is not callable.")
    return obj


def make_runner_call(fn: Callable, payload: dict, progress_cb: ProgressCallback) -> Any:
    try:
        n_params = len(inspect.signature(fn).parameters)
    except (ValueError, TypeError):
        n_params = 1
    return fn(payload, progress_cb) if n_params >= 2 else fn(payload)


def call_with_optional_context(fn: Callable[..., Any], payload: dict, context: dict) -> Any:
    try:
        n_params = len(inspect.signature(fn).parameters)
    except (ValueError, TypeError):
        n_params = 1
    if n_params >= 2:
        return fn(payload, context)
    return fn(payload)


def process_worker_entry(
    fn_ref: RunnerRef,
    payload: dict,
    out_q: mp.Queue,
    progress_q: mp.Queue,
    job_id: str,
    chunk_id: str,
):
    def _progress_cb(value: float):
        try:
            progress_q.put_nowait({"chunk_id": chunk_id, "progress": float(value)})
        except Exception:
            pass

    try:
        fn = resolve_runner(fn_ref)
        result = make_runner_call(fn, payload, _progress_cb)
        out_q.put({"ok": True, "result": result, "job_id": job_id, "chunk_id": chunk_id})
    except Exception as exc:
        out_q.put(
            {
                "ok": False,
                "error": str(exc),
                "tb": traceback.format_exc(),
                "job_id": job_id,
                "chunk_id": chunk_id,
            }
        )


def drain_progress_queue(progress_q: mp.Queue) -> Optional[float]:
    latest: Optional[float] = None
    while True:
        try:
            msg = progress_q.get_nowait()
            latest = float(msg.get("progress", latest or 0.0))
        except queue.Empty:
            break
    return latest


__all__ = [
    "ProgressCallback",
    "RunnerRef",
    "call_with_optional_context",
    "callable_to_ref",
    "drain_progress_queue",
    "make_runner_call",
    "normalize_runner",
    "normalize_uuid",
    "process_worker_entry",
    "ref_to_str",
    "resolve_runner",
    "str_to_ref",
]
