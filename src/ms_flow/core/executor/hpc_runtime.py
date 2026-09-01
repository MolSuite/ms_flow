from __future__ import annotations

import argparse
import inspect
import json
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from ms_flow.core.callable_refs import resolve_callable_ref


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


def _make_runner_call(fn: Callable[..., Any], payload: dict[str, Any]) -> Any:
    try:
        n_params = len(inspect.signature(fn).parameters)
    except (TypeError, ValueError):
        n_params = 1
    if n_params >= 2:
        return fn(payload, lambda _value: None)
    return fn(payload)


def run_manifest(manifest_path: Path) -> int:
    manifest = _safe_json_loads(manifest_path)
    runner_ref = str(manifest.get("runner_ref", "")).strip()
    if not runner_ref:
        raise ValueError("Manifest missing runner_ref.")

    payload = manifest.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("Manifest payload must be a dict.")

    status_path = Path(str(manifest["status_path"])).expanduser().resolve()
    result_path = Path(str(manifest["result_path"])).expanduser().resolve()
    job_id = str(manifest.get("job_id", "")).strip()
    chunk_id = str(manifest.get("chunk_id", "")).strip()

    _write_json(
        status_path,
        {
            "state": "RUNNING",
            "job_id": job_id,
            "chunk_id": chunk_id,
            "updated_at": _utc_now(),
        },
    )

    try:
        fn = resolve_callable_ref(runner_ref)
        result = _make_runner_call(fn, payload)
        _write_json(
            result_path,
            {
                "ok": True,
                "result": result,
                "job_id": job_id,
                "chunk_id": chunk_id,
                "updated_at": _utc_now(),
            },
        )
        _write_json(
            status_path,
            {
                "state": "DONE",
                "job_id": job_id,
                "chunk_id": chunk_id,
                "updated_at": _utc_now(),
            },
        )
        return 0
    except Exception as exc:
        _write_json(
            result_path,
            {
                "ok": False,
                "error": str(exc),
                "traceback": traceback.format_exc(),
                "job_id": job_id,
                "chunk_id": chunk_id,
                "updated_at": _utc_now(),
            },
        )
        _write_json(
            status_path,
            {
                "state": "FAILED",
                "error": str(exc),
                "job_id": job_id,
                "chunk_id": chunk_id,
                "updated_at": _utc_now(),
            },
        )
        return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="MolSuite HPC runtime bootstrap.")
    parser.add_argument("--manifest", required=True, help="Path to the manifest JSON file.")
    args = parser.parse_args()
    return run_manifest(Path(args.manifest).expanduser().resolve())


if __name__ == "__main__":
    raise SystemExit(main())
