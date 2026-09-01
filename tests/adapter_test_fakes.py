from __future__ import annotations

import sys
import types
from pathlib import Path


def write_fake_hpc_scheduler(script_path: Path) -> None:
    script_path.write_text(
        """#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import uuid
from pathlib import Path


def _load(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _save(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def main(argv: list[str]) -> int:
    command = argv[1]
    if command == "submit":
        script_path = Path(argv[2]).resolve()
        control_dir = Path(argv[3]).resolve()
        state_dir = Path(argv[4]).resolve()
        scheduler_job_id = uuid.uuid4().hex
        proc = subprocess.Popen(
            ["bash", str(script_path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        _save(
            state_dir / f"{scheduler_job_id}.json",
            {
                "pid": proc.pid,
                "control_dir": str(control_dir),
            },
        )
        print(scheduler_job_id)
        return 0

    scheduler_job_id = argv[2]
    state_dir = Path(argv[3]).resolve()
    state_path = state_dir / f"{scheduler_job_id}.json"
    state = _load(state_path)
    if not state:
        print(json.dumps({"state": "FAILED", "error": "Unknown scheduler job id"}))
        return 1

    control_dir = Path(state["control_dir"]).resolve()
    status_path = control_dir / "status.json"
    result_path = control_dir / "result.json"
    pid = int(state.get("pid", 0))

    if command == "poll":
        if result_path.exists():
            result = _load(result_path)
            if result.get("ok"):
                print(json.dumps({"state": "DONE"}))
                return 0
            print(json.dumps({"state": "FAILED", "error": result.get("error", "Execution failed")}))
            return 0
        if status_path.exists():
            status = _load(status_path)
            current = str(status.get("state", "")).upper()
            if current in {"FAILED", "ERROR", "CANCELED", "CANCELLED"}:
                print(json.dumps({"state": current, "error": status.get("error", "")}))
                return 0
        if pid and _is_alive(pid):
            print(json.dumps({"state": "RUNNING"}))
            return 0
        print(json.dumps({"state": "FAILED", "error": "Scheduler process exited without result"}))
        return 0

    if command == "cancel":
        if pid and _is_alive(pid):
            os.kill(pid, signal.SIGTERM)
        status = _load(status_path) if status_path.exists() else {}
        status.update({"state": "CANCELED"})
        _save(status_path, status)
        print("OK")
        return 0

    raise ValueError(f"Unknown command: {command}")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
""",
        encoding="utf-8",
    )
    script_path.chmod(0o755)


def install_fake_ray(monkeypatch):
    import concurrent.futures

    class _FakeRemoteFn:
        def __init__(self, ray_module, fn):
            self._ray = ray_module
            self._fn = fn
            self._options = {}

        def options(self, **kwargs):
            clone = _FakeRemoteFn(self._ray, self._fn)
            clone._options = dict(kwargs)
            return clone

        def remote(self, *args, **kwargs):
            future = self._ray._pool.submit(self._fn, *args, **kwargs)
            future._num_cpus = self._options.get("num_cpus")
            future._num_gpus = self._options.get("num_gpus")
            self._ray._futures.append(future)
            return future

    class _FakeRay(types.SimpleNamespace):
        def __init__(self):
            super().__init__()
            self._initialized = False
            self._pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)
            self._futures = []
            self._put_calls = 0

        def is_initialized(self):
            return self._initialized

        def init(self, **kwargs):
            self._initialized = True
            self._init_kwargs = kwargs

        def remote(self, fn):
            return _FakeRemoteFn(self, fn)

        def put(self, value):
            self._put_calls += 1
            future = concurrent.futures.Future()
            future.set_result(value)
            return future

        def wait(self, refs, timeout=0):
            ready = [ref for ref in refs if ref.done()]
            pending = [ref for ref in refs if not ref.done()]
            return ready, pending

        def get(self, ref):
            return ref.result()

        def cancel(self, ref, force=True):
            del force
            return ref.cancel()

        def shutdown(self):
            self._initialized = False

    fake_ray = _FakeRay()
    monkeypatch.setitem(sys.modules, "ray", fake_ray)
    return fake_ray
