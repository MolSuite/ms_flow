from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from ms_flow.core.executor.local_adapters import ExecutorAdapterMetadata


def _safe_json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


def _safe_json_loads(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(f"{path.suffix}.tmp")
    tmp.write_text(_safe_json_dumps(data), encoding="utf-8")
    tmp.replace(path)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class HPCHandleState:
    scheduler_job_id: str
    control_dir: Path
    manifest_path: Path
    submit_script_path: Path
    status_path: Path
    result_path: Path
    stdout_path: Path
    stderr_path: Path
    last_remote_poll_at: float = 0.0


class HPCCommandExecutorAdapter:
    def __init__(
        self,
        *,
        name: str,
        submit_command: str | Sequence[str],
        poll_command: str | Sequence[str],
        cancel_command: str | Sequence[str] | None = None,
        poll_interval_s: float = 2.0,
        shared_fs: bool = False,
        command_context: Optional[Mapping[str, Any]] = None,
        command_env: Optional[Mapping[str, str]] = None,
        python_executable: Optional[str] = None,
    ):
        self.name = name
        self.reserved_cpu = 0
        self._submit_command = submit_command
        self._poll_command = poll_command
        self._cancel_command = cancel_command
        self._poll_interval_s = max(0.1, float(poll_interval_s))
        self._shared_fs = bool(shared_fs)
        self._command_context = dict(command_context or {})
        self._command_env = dict(command_env or {})
        self._python_executable = str(python_executable or sys.executable)
        pythonpath_entries = [str(entry) for entry in sys.path if str(entry).strip()]
        self._runtime_pythonpath = os.pathsep.join(dict.fromkeys(pythonpath_entries))
        self._handles: dict[str, HPCHandleState] = {}
        self._lock = threading.Lock()

    @property
    def backend_name(self) -> str:
        return "hpc"

    @property
    def execution_mode(self) -> str:
        return "external"

    @property
    def has_shared_filesystem(self) -> bool:
        return self._shared_fs

    @property
    def consumes_local_cpu_tokens(self) -> bool:
        return False

    @property
    def support_level(self) -> str:
        return "stable"

    @property
    def metadata(self) -> ExecutorAdapterMetadata:
        return ExecutorAdapterMetadata(
            backend="hpc",
            mode="external",
            support_level=self.support_level,
            shared_filesystem=self._shared_fs,
            consumes_local_cpu_tokens=False,
            supports_inline=True,
            supports_bytes=True,
            supports_file_input=True,
            supports_db_input=True,
        )

    @property
    def integration_kind(self) -> str:
        return "command"

    def health_snapshot(self) -> dict[str, Any]:
        with self._lock:
            managed_handles = len(self._handles)
        return {
            "ok": True,
            "integration": self.integration_kind,
            "managed_handles": managed_handles,
            "submit_configured": self._submit_command is not None,
            "poll_configured": self._poll_command is not None,
            "poll_interval_s": self._poll_interval_s,
            "cancel_configured": self._cancel_command is not None,
            "shared_fs": self._shared_fs,
        }

    def _build_control_dir(self, submit_context: Mapping[str, Any]) -> Path:
        raw_root = str(
            submit_context.get("hpc_wdir")
            or submit_context.get("project_path")
            or submit_context.get("project_dir")
            or ""
        ).strip()
        if not raw_root:
            raise ValueError("HPC executor requires 'hpc_wdir' or 'project_path' in submit context.")
        root = Path(raw_root).expanduser().resolve()
        job_id = str(submit_context.get("job_id", "")).strip()
        chunk_id = str(submit_context.get("chunk_id", "")).strip()
        if not job_id or not chunk_id:
            raise ValueError("HPC executor submit context requires job_id and chunk_id.")
        control_dir = root / "_molsuite_runtime" / job_id / chunk_id
        control_dir.mkdir(parents=True, exist_ok=True)
        return control_dir

    def _format_command(self, template: str | Sequence[str], values: Mapping[str, Any]) -> list[str]:
        if isinstance(template, str):
            return shlex.split(template.format(**values))
        return [str(part).format(**values) for part in template]

    def _run_command(
        self,
        template: str | Sequence[str],
        values: Mapping[str, Any],
    ) -> subprocess.CompletedProcess[str]:
        cmd = self._format_command(template, values)
        env = None
        if self._command_env:
            env = {**os.environ, **self._command_env}
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )

    def _parse_scheduler_id(self, output: str) -> str:
        text = output.strip()
        if not text:
            raise RuntimeError("HPC submit command returned empty output.")
        if text.startswith("{"):
            payload = json.loads(text)
            for key in ("job_id", "scheduler_job_id", "handle_id", "id"):
                value = str(payload.get(key, "")).strip()
                if value:
                    return value
        line = text.splitlines()[0].strip()
        if not line:
            raise RuntimeError("HPC submit command returned no scheduler job id.")
        return line

    def _poll_from_files(self, state: HPCHandleState) -> tuple[str, Optional[dict], Optional[str]]:
        if state.result_path.exists():
            payload = _safe_json_loads(state.result_path)
            if payload.get("ok"):
                return "DONE", {"result": payload.get("result")}, None
            return "FAILED", None, str(payload.get("error") or "Unknown HPC execution failure.")

        if state.status_path.exists():
            status = _safe_json_loads(state.status_path)
            current = str(status.get("state", "")).strip().upper()
            if current in {"RUNNING", "SUBMITTED", "QUEUED", "PENDING"}:
                return "RUNNING", None, None
            if current in {"FAILED", "ERROR"}:
                return "FAILED", None, str(status.get("error") or "HPC execution failed.")
            if current in {"CANCELED", "CANCELLED"}:
                return "FAILED", None, "HPC job canceled."
            if current == "DONE":
                return "RUNNING", None, None
        return "RUNNING", None, None

    def submit(
        self,
        job_id: str,
        chunk_id: str,
        payload: dict,
        fn_ref: dict[str, str],
        progress_cb: Callable[[float], None],
        submit_context: Optional[dict[str, Any]] = None,
    ) -> str:
        del progress_cb
        context = dict(self._command_context)
        context.update(dict(submit_context or {}))
        context["job_id"] = job_id
        context["chunk_id"] = chunk_id

        control_dir = self._build_control_dir(context)
        manifest_path = control_dir / "manifest.json"
        result_path = control_dir / "result.json"
        status_path = control_dir / "status.json"
        stdout_path = control_dir / "stdout.log"
        stderr_path = control_dir / "stderr.log"
        submit_script_path = control_dir / "submit.sh"

        manifest = {
            "job_id": job_id,
            "chunk_id": chunk_id,
            "runner_ref": f"{fn_ref['module']}:{fn_ref['fn']}",
            "payload": payload,
            "result_path": str(result_path),
            "status_path": str(status_path),
        }
        _write_json(manifest_path, manifest)
        _write_json(
            status_path,
            {
                "state": "SUBMITTED",
                "job_id": job_id,
                "chunk_id": chunk_id,
                "updated_at": _utc_now(),
            },
        )

        script = "\n".join(
            list(
                filter(
                    None,
                    [
                        "#!/usr/bin/env bash",
                        "set -euo pipefail",
                        (
                            f"export PYTHONPATH={shlex.quote(self._runtime_pythonpath)}"
                            if self._runtime_pythonpath
                            else ""
                        ),
                        f"cd {shlex.quote(str(control_dir))}",
                        (
                            f"exec {shlex.quote(self._python_executable)} -m ms_flow.core.executor.hpc_runtime "
                            f"--manifest {shlex.quote(str(manifest_path))} "
                            f">>{shlex.quote(str(stdout_path))} 2>>{shlex.quote(str(stderr_path))}"
                        ),
                        "",
                    ],
                )
            )
        )
        submit_script_path.write_text(script, encoding="utf-8")
        submit_script_path.chmod(0o755)

        values = {
            "job_id": job_id,
            "chunk_id": chunk_id,
            "control_dir": str(control_dir),
            "manifest_path": str(manifest_path),
            "result_path": str(result_path),
            "status_path": str(status_path),
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
            "submit_script_path": str(submit_script_path),
            "python_executable": self._python_executable,
            **context,
        }
        proc = self._run_command(self._submit_command, values)
        if proc.returncode != 0:
            raise RuntimeError(
                f"HPC submit command failed (rc={proc.returncode}): "
                f"{(proc.stderr or proc.stdout or '').strip()}"
            )

        scheduler_job_id = self._parse_scheduler_id(proc.stdout)
        handle_id = uuid.uuid4().hex
        with self._lock:
            self._handles[handle_id] = HPCHandleState(
                scheduler_job_id=scheduler_job_id,
                control_dir=control_dir,
                manifest_path=manifest_path,
                submit_script_path=submit_script_path,
                status_path=status_path,
                result_path=result_path,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
            )
        return handle_id

    def poll(self, handle_id: str) -> tuple[str, Optional[dict], Optional[str]]:
        with self._lock:
            state = self._handles.get(handle_id)
        if state is None:
            return "FAILED", None, "Unknown HPC handle."

        local_state, payload, error = self._poll_from_files(state)
        if local_state != "RUNNING":
            with self._lock:
                self._handles.pop(handle_id, None)
            return local_state, payload, error

        now = time.monotonic()
        with self._lock:
            current = self._handles.get(handle_id)
            if current is None:
                return "FAILED", None, "Unknown HPC handle."
            if now - current.last_remote_poll_at < self._poll_interval_s:
                return "RUNNING", None, None
            current.last_remote_poll_at = now

        values = {
            "scheduler_job_id": state.scheduler_job_id,
            "job_id": state.control_dir.parent.name,
            "chunk_id": state.control_dir.name,
            "control_dir": str(state.control_dir),
            "manifest_path": str(state.manifest_path),
            "result_path": str(state.result_path),
            "status_path": str(state.status_path),
            "stdout_path": str(state.stdout_path),
            "stderr_path": str(state.stderr_path),
            "submit_script_path": str(state.submit_script_path),
            **self._command_context,
        }
        proc = self._run_command(self._poll_command, values)
        if proc.returncode != 0:
            return "FAILED", None, f"HPC poll command failed (rc={proc.returncode}): {(proc.stderr or '').strip()}"

        text = proc.stdout.strip()
        if text.startswith("{"):
            payload_doc = json.loads(text)
            status = str(payload_doc.get("state", "")).strip().upper() or "RUNNING"
            if status == "DONE" and "result" in payload_doc:
                with self._lock:
                    self._handles.pop(handle_id, None)
                return "DONE", {"result": payload_doc.get("result")}, None
            if status in {"FAILED", "ERROR", "CANCELED", "CANCELLED"}:
                with self._lock:
                    self._handles.pop(handle_id, None)
                return "FAILED", None, str(payload_doc.get("error") or f"HPC job {status.lower()}.")
        elif text:
            status = text.splitlines()[0].strip().upper()
            if status in {"FAILED", "ERROR", "CANCELED", "CANCELLED"}:
                with self._lock:
                    self._handles.pop(handle_id, None)
                return "FAILED", None, f"HPC job {status.lower()}."

        local_state, payload, error = self._poll_from_files(state)
        if local_state != "RUNNING":
            with self._lock:
                self._handles.pop(handle_id, None)
            return local_state, payload, error
        return "RUNNING", None, None

    def cancel(self, handle_id: str) -> bool:
        with self._lock:
            state = self._handles.get(handle_id)
        if state is None or self._cancel_command is None:
            return False
        values = {
            "scheduler_job_id": state.scheduler_job_id,
            "job_id": state.control_dir.parent.name,
            "chunk_id": state.control_dir.name,
            "control_dir": str(state.control_dir),
            "manifest_path": str(state.manifest_path),
            "result_path": str(state.result_path),
            "status_path": str(state.status_path),
            "stdout_path": str(state.stdout_path),
            "stderr_path": str(state.stderr_path),
            "submit_script_path": str(state.submit_script_path),
            **self._command_context,
        }
        proc = self._run_command(self._cancel_command, values)
        return proc.returncode == 0

    def shutdown(self):
        return None
