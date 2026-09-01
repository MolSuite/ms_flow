from __future__ import annotations

import inspect
import hashlib
import json
import os
import shutil
import sys
import tempfile
import threading
import uuid
from pathlib import Path
from typing import Any, Mapping, Optional

from ms_flow.core.callable_refs import resolve_callable_ref
from ms_flow.core.data.contracts import (
    FileArtifact,
    RAY_FILE_ARTIFACT_KEY,
    RAY_FILE_INPUT_KEY,
    RAY_OUTPUT_DIR_KEY,
)
from ms_flow.core.executor.local_adapters import ExecutorAdapterMetadata


def _make_runner_call(fn, payload: dict[str, Any]) -> Any:
    try:
        n_params = len(inspect.signature(fn).parameters)
    except (TypeError, ValueError):
        n_params = 1
    if n_params >= 2:
        return fn(payload, lambda _value: None)
    return fn(payload)


def _worker_file_payload(
    value: Any,
    cleanup_dirs: list[str],
    output_dirs: list[tuple[Path, str]],
) -> Any:
    if isinstance(value, dict) and set(value) == {RAY_FILE_INPUT_KEY}:
        spec = dict(value[RAY_FILE_INPUT_KEY] or {})
        data = spec.get("data", b"")
        if spec.get("object_ref"):
            import ray  # type: ignore

            data = ray.get(data)
        if not isinstance(data, bytes):
            raise TypeError("Ray file transfer payload must contain bytes.")
        delivery = str(spec.get("delivery") or "content")
        if delivery == "content":
            fmt = str(spec.get("fmt") or "binary")
            if fmt == "binary":
                return data
            text = data.decode(str(spec.get("encoding") or "utf-8"))
            return json.loads(text) if fmt == "json" else text

        name = Path(str(spec.get("name") or "input.bin")).name
        digest = str(spec.get("digest") or hashlib.sha256(data).hexdigest())
        if spec.get("cache"):
            # ponytail: cache lives for the Ray node lifetime; add bounded
            # eviction only if real receptor workloads create disk pressure.
            base = Path(tempfile.gettempdir()) / "ms_flow_ray_cache" / digest
        else:
            base = Path(tempfile.mkdtemp(prefix="ms-flow-ray-"))
            cleanup_dirs.append(str(base))
        base.mkdir(parents=True, exist_ok=True)
        target = base / name
        if not target.exists():
            temporary = base / f".{name}.{uuid.uuid4().hex}.tmp"
            temporary.write_bytes(data)
            os.replace(temporary, target)
        return str(target)
    if isinstance(value, dict) and set(value) == {RAY_OUTPUT_DIR_KEY}:
        destination = str(dict(value[RAY_OUTPUT_DIR_KEY] or {}).get("destination") or "").strip()
        if not destination:
            raise ValueError("Ray output directory requires a project-relative destination.")
        temporary_project = Path(tempfile.mkdtemp(prefix="ms-flow-ray-project-"))
        target = temporary_project / destination
        target.mkdir(parents=True, exist_ok=True)
        cleanup_dirs.append(str(temporary_project))
        output_dirs.append((target, destination))
        return str(target)
    if isinstance(value, dict):
        return {key: _worker_file_payload(item, cleanup_dirs, output_dirs) for key, item in value.items()}
    if isinstance(value, list):
        return [_worker_file_payload(item, cleanup_dirs, output_dirs) for item in value]
    return value


def _rewrite_output_paths(value: Any, output_dirs: list[tuple[Path, str]]) -> Any:
    if isinstance(value, str):
        source = Path(value).expanduser()
        if source.is_absolute():
            for root, destination in output_dirs:
                try:
                    relative = source.resolve().relative_to(root.resolve())
                except ValueError:
                    continue
                return (Path(destination) / relative).as_posix()
        return value
    if isinstance(value, dict):
        return {str(key): _rewrite_output_paths(item, output_dirs) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_rewrite_output_paths(item, output_dirs) for item in value]
    return value


def _output_dir_artifacts(output_dirs: list[tuple[Path, str]]) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    for root, destination in output_dirs:
        for source in root.rglob("*"):
            if not source.is_file():
                continue
            relative = source.relative_to(root)
            artifacts.append(
                {
                    RAY_FILE_ARTIFACT_KEY: {
                        "destination": (Path(destination) / relative).as_posix(),
                        "name": source.name,
                        "size": source.stat().st_size,
                        "data": source.read_bytes(),
                    }
                }
            )
    return artifacts


def _worker_artifacts(value: Any, *, shared_fs: bool) -> Any:
    if isinstance(value, FileArtifact):
        source = Path(value.path).expanduser().resolve()
        if not source.is_file():
            raise FileNotFoundError(f"Worker artifact not found: {source}")
        payload = {
            "destination": str(value.destination),
            "name": source.name,
            "size": source.stat().st_size,
        }
        if shared_fs:
            payload["shared_path"] = str(source)
        else:
            payload["data"] = source.read_bytes()
            source.unlink(missing_ok=True)
        return {RAY_FILE_ARTIFACT_KEY: payload}
    if isinstance(value, dict):
        return {str(key): _worker_artifacts(item, shared_fs=shared_fs) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_worker_artifacts(item, shared_fs=shared_fs) for item in value]
    return value


def _ray_runner_entry(
    fn_ref: str,
    payload: dict[str, Any],
    pythonpath_entries: list[str],
    shared_fs: bool,
) -> dict[str, Any]:
    cleanup_dirs: list[str] = []
    output_dirs: list[tuple[Path, str]] = []
    try:
        for entry in pythonpath_entries:
            if entry and entry not in sys.path:
                sys.path.insert(0, entry)
        fn = resolve_callable_ref(fn_ref)
        localized = _worker_file_payload(payload, cleanup_dirs, output_dirs)
        result = _make_runner_call(fn, localized)
        return {
            "ok": True,
            "result": _worker_artifacts(
                _rewrite_output_paths(result, output_dirs),
                shared_fs=shared_fs,
            ),
            "output_artifacts": _output_dir_artifacts(output_dirs),
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
    finally:
        for directory in cleanup_dirs:
            shutil.rmtree(directory, ignore_errors=True)


class RayExecutorAdapter:
    def __init__(
        self,
        *,
        name: str,
        reserved_cpu: int = 0,
        mode: str = "external",
        shared_fs: Optional[bool] = None,
        address: Optional[str] = None,
        namespace: Optional[str] = None,
        runtime_env: Optional[Mapping[str, Any]] = None,
        gpu_slots_per_device: int = 1,
        ignore_reinit_error: bool = True,
        log_to_driver: bool = False,
    ):
        self.name = name
        self.reserved_cpu = max(0, int(reserved_cpu))
        self.mode = str(mode or "external").strip().lower()
        self.shared_fs = bool(shared_fs) if shared_fs is not None else (self.mode == "local")
        self.gpu_slots_per_device = max(1, int(gpu_slots_per_device))
        try:
            import ray  # type: ignore
        except Exception as exc:
            raise RuntimeError("Ray is not installed. Install with: pip install 'ray[default]'") from exc

        self._ray = ray
        self._lock = threading.Lock()
        self._refs: dict[str, Any] = {}
        self._contexts: dict[str, dict[str, Any]] = {}
        self._file_refs: dict[tuple[str, int, int], tuple[Any, str]] = {}
        pythonpath_entries = [str(entry) for entry in sys.path if str(entry).strip()]
        self._pythonpath_entries = list(dict.fromkeys(pythonpath_entries))
        effective_runtime_env = dict(runtime_env or {})
        env_vars = dict(effective_runtime_env.get("env_vars") or {})
        pythonpath = os.pathsep.join(self._pythonpath_entries)
        if pythonpath:
            existing = str(env_vars.get("PYTHONPATH", "")).strip()
            env_vars["PYTHONPATH"] = pythonpath if not existing else f"{pythonpath}{os.pathsep}{existing}"
            effective_runtime_env["env_vars"] = env_vars
        if not ray.is_initialized():
            init_kwargs: dict[str, Any] = dict(
                address=address,
                namespace=namespace,
                runtime_env=effective_runtime_env or None,
                ignore_reinit_error=ignore_reinit_error,
                log_to_driver=log_to_driver,
            )
            # Enforce mf's outer CPU partition on the local cluster ray starts.
            # Without this ray auto-detects ALL host CPUs and ignores the slice mf
            # reserved for it (the token would be bookkeeping-only). Only applies
            # when ray boots the cluster (mode=local); never for external clusters.
            if self.mode == "local" and self.reserved_cpu > 0 and address is None:
                init_kwargs["num_cpus"] = self.reserved_cpu
            ray.init(**init_kwargs)
        self._remote_runner = ray.remote(_ray_runner_entry)

    @property
    def backend_name(self) -> str:
        return "ray"

    @property
    def execution_mode(self) -> str:
        return self.mode

    @property
    def has_shared_filesystem(self) -> bool:
        return self.shared_fs

    @property
    def consumes_local_cpu_tokens(self) -> bool:
        return False

    @property
    def support_level(self) -> str:
        return "experimental"

    @property
    def metadata(self) -> ExecutorAdapterMetadata:
        return ExecutorAdapterMetadata(
            backend="ray",
            mode=self.mode,
            support_level=self.support_level,
            shared_filesystem=self.shared_fs,
            consumes_local_cpu_tokens=False,
            supports_inline=True,
            supports_bytes=True,
            supports_file_input=True,
            supports_db_input=True,
        )

    @property
    def integration_kind(self) -> str:
        return "native"

    def health_snapshot(self) -> dict[str, Any]:
        with self._lock:
            managed_handles = len(self._refs)
        initialized = bool(self._ray.is_initialized())
        return {
            "ok": initialized,
            "integration": self.integration_kind,
            "managed_handles": managed_handles,
            "initialized": initialized,
            "shared_fs": self.shared_fs,
            "mode": self.mode,
            "reserved_cpu": self.reserved_cpu,
            "gpu_slots_per_device": self.gpu_slots_per_device,
            "local_budgeting": "reserved" if self.mode == "local" and self.reserved_cpu > 0 else "none",
        }

    def submit(
        self,
        job_id: str,
        chunk_id: str,
        payload: dict,
        fn_ref: dict[str, str],
        progress_cb,
        submit_context: Optional[dict[str, Any]] = None,
    ) -> str:
        del progress_cb
        options: dict[str, Any] = {}
        if submit_context is not None:
            cpu_required = int(submit_context.get("cpu_required", 0) or 0)
            if cpu_required > 0:
                options["num_cpus"] = cpu_required
            gpu_required = int(submit_context.get("gpu_required", 0) or 0)
            if gpu_required > 0:
                options["num_gpus"] = gpu_required / self.gpu_slots_per_device
        remote = self._remote_runner.options(**options) if options else self._remote_runner
        prepared = self._prepare_file_inputs(payload)
        ref = remote.remote(
            f"{fn_ref['module']}:{fn_ref['fn']}",
            prepared,
            self._pythonpath_entries,
            self.shared_fs,
        )
        handle_id = uuid.uuid4().hex
        with self._lock:
            self._refs[handle_id] = ref
            self._contexts[handle_id] = {
                **dict(submit_context or {}),
                "job_id": str(job_id),
                "chunk_id": str(chunk_id),
            }
        return handle_id

    def _prepare_file_inputs(self, value: Any) -> Any:
        if isinstance(value, dict) and set(value) == {RAY_FILE_INPUT_KEY}:
            spec = dict(value[RAY_FILE_INPUT_KEY] or {})
            source = Path(str(spec.pop("path", ""))).expanduser().resolve()
            stat = source.stat()
            cache_key = (str(source), int(stat.st_mtime_ns), int(stat.st_size))
            cached = None
            if spec.get("cache"):
                with self._lock:
                    cached = self._file_refs.get(cache_key)
            if cached is None:
                data = source.read_bytes()
                digest = hashlib.sha256(data).hexdigest()
                ref = self._ray.put(data) if spec.get("cache") and hasattr(self._ray, "put") else data
                cached = (ref, digest)
                if spec.get("cache"):
                    with self._lock:
                        self._file_refs[cache_key] = cached
            data, digest = cached
            spec.update(
                {
                    "data": data,
                    "digest": digest,
                    "object_ref": bool(spec.get("cache") and hasattr(self._ray, "put")),
                }
            )
            return {RAY_FILE_INPUT_KEY: spec}
        if isinstance(value, dict):
            return {key: self._prepare_file_inputs(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._prepare_file_inputs(item) for item in value]
        return value

    @staticmethod
    def _artifact_target(destination: str, context: Mapping[str, Any]) -> tuple[Path, str]:
        project_root_text = str(context.get("project_dir") or context.get("project_path") or "").strip()
        if not project_root_text:
            raise RuntimeError("Ray FileArtifact requires project_dir in the job data context.")
        project_root = Path(project_root_text).expanduser().resolve()
        relative = Path(str(destination or "").strip())
        if not str(relative) or relative.is_absolute():
            raise ValueError("FileArtifact destination must be a non-empty project-relative path.")
        target = (project_root / relative).resolve()
        try:
            target.relative_to(project_root)
        except ValueError as exc:
            raise ValueError("FileArtifact destination escapes the project directory.") from exc
        return target, relative.as_posix()

    def _materialize_artifacts(self, value: Any, context: Mapping[str, Any]) -> Any:
        if isinstance(value, dict) and set(value) == {RAY_FILE_ARTIFACT_KEY}:
            spec = dict(value[RAY_FILE_ARTIFACT_KEY] or {})
            target, relative = self._artifact_target(str(spec.get("destination") or ""), context)
            target.parent.mkdir(parents=True, exist_ok=True)
            shared_path = str(spec.get("shared_path") or "").strip()
            if shared_path:
                source = Path(shared_path).expanduser().resolve()
                if not source.is_file():
                    raise FileNotFoundError(f"Shared Ray artifact not found: {source}")
                if source != target:
                    try:
                        os.replace(source, target)
                    except OSError:
                        shutil.copy2(source, target)
                        source.unlink()
            else:
                data = spec.get("data")
                if not isinstance(data, bytes):
                    raise TypeError("Transferred Ray artifact must contain bytes.")
                temporary = target.parent / f".{target.name}.{uuid.uuid4().hex}.tmp"
                temporary.write_bytes(data)
                os.replace(temporary, target)
            return relative
        if isinstance(value, dict):
            return {key: self._materialize_artifacts(item, context) for key, item in value.items()}
        if isinstance(value, list):
            return [self._materialize_artifacts(item, context) for item in value]
        return value

    def poll(self, handle_id: str):
        with self._lock:
            ref = self._refs.get(handle_id)
        if ref is None:
            return "FAILED", None, "Unknown Ray handle."
        ready, _ = self._ray.wait([ref], timeout=0)
        if not ready:
            return "RUNNING", None, None
        try:
            payload = self._ray.get(ref)
        except Exception as exc:
            with self._lock:
                self._refs.pop(handle_id, None)
                self._contexts.pop(handle_id, None)
            return "FAILED", None, str(exc)
        with self._lock:
            self._refs.pop(handle_id, None)
            context = self._contexts.pop(handle_id, {})
        if payload.get("ok"):
            try:
                for artifact in list(payload.get("output_artifacts") or []):
                    self._materialize_artifacts(artifact, context)
                result = self._materialize_artifacts(payload.get("result"), context)
            except Exception as exc:
                return "FAILED", None, str(exc)
            return "DONE", {"result": result}, None
        return "FAILED", None, str(payload.get("error") or "Unknown Ray execution failure.")

    def cancel(self, handle_id: str) -> bool:
        with self._lock:
            ref = self._refs.get(handle_id)
        if ref is None:
            return False
        try:
            self._ray.cancel(ref, force=True)
            return True
        except Exception:
            return False

    def shutdown(self):
        with self._lock:
            self._refs.clear()
            self._contexts.clear()
            self._file_refs.clear()
        shutdown = getattr(self._ray, "shutdown", None)
        if callable(shutdown) and self._ray.is_initialized():
            shutdown()
