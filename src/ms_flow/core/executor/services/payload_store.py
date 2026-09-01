from __future__ import annotations

import logging
import shutil
import threading
from pathlib import Path
from typing import Any, Dict, Optional, TYPE_CHECKING

from ms_flow.core.executor.utils import (
    _safe_json_dumps,
    _safe_json_loads,
    CHUNK_PAYLOAD_REF_KEY,
)
from ms_flow.core.database.executor_models import ExecutorJob
from sqlmodel import select

if TYPE_CHECKING:
    from ms_flow.core.executor.manager import ExecutorManager


class ChunkPayloadStore:
    """
    Handles persistence and retrieval of chunk payloads.
    Decides whether to store payloads inline (in DB) or as spillover files.
    """

    def __init__(
        self,
        executor_manager: ExecutorManager,
        max_inline_bytes: int = 512 * 1024,
        max_spool_bytes: int = 64 * 1024 * 1024,
    ):
        self.manager = executor_manager
        self.max_inline_bytes = max_inline_bytes
        self.max_spool_bytes = max_spool_bytes
        self.logger = logging.getLogger("molsuite.executor.payload_store")

        self._job_payload_dirs: Dict[str, Path] = {}
        self._lock = threading.RLock()

    def configure(self, max_inline: int | None = None, max_spool: int | None = None):
        with self._lock:
            if max_inline is not None:
                self.max_inline_bytes = max(1024, int(max_inline))
            if max_spool is not None:
                self.max_spool_bytes = max(self.max_inline_bytes, int(max_spool))

    def get_or_create_job_payload_dir(self, job_id: str) -> Path:
        with self._lock:
            cached = self._job_payload_dirs.get(job_id)
            if cached is not None:
                cached.mkdir(parents=True, exist_ok=True)
                return cached

        project_dir: Optional[Path] = None
        # We need to access ExecutorJob to find the project path for spooling
        if self.manager.executor_db is not None:
            with self.manager.executor_db.get_session() as session:
                job = session.exec(select(ExecutorJob).where(ExecutorJob.job_id == job_id)).first()
                if job is not None:
                    payload = _safe_json_loads(job.payload_json)
                    raw_context = payload.get("_data_context") or {}
                    if isinstance(raw_context, dict):
                        raw_project = raw_context.get("project_path") or raw_context.get("project_dir")
                        if raw_project:
                            project_dir = Path(raw_project).expanduser().resolve()

        if project_dir is not None:
            payload_dir = project_dir / "tmp" / "chunk_payloads" / job_id
        else:
            base = (
                self.manager.executor_db.db_path.parent
                if self.manager.executor_db and self.manager.executor_db.db_path is not None
                else Path.cwd()
            )
            payload_dir = base / "chunk_payloads" / job_id

        payload_dir.mkdir(parents=True, exist_ok=True)
        with self._lock:
            self._job_payload_dirs[job_id] = payload_dir
        return payload_dir

    def encode_payload(
        self,
        job_id: str,
        chunk_id: str,
        payload_obj: dict[str, Any],
    ) -> tuple[str, str]:
        """
        Serializes payload. Returns (payload_json, checkpoint_ref).
        If payload is large, stores in file and returns reference.
        """
        serialized = _safe_json_dumps(payload_obj)
        payload_bytes = len(serialized.encode("utf-8"))

        if payload_bytes > self.max_spool_bytes:
            raise RuntimeError(
                f"Chunk payload exceeds max_spool_payload_bytes ({payload_bytes}>{self.max_spool_bytes})."
            )

        if payload_bytes <= self.max_inline_bytes:
            return serialized, ""

        payload_dir = self.get_or_create_job_payload_dir(job_id)
        payload_path = payload_dir / f"{chunk_id}.json"
        payload_path.write_text(serialized, encoding="utf-8")

        wrapper = {
            CHUNK_PAYLOAD_REF_KEY: {
                "path": str(payload_path),
                "format": "json",
            }
        }
        return _safe_json_dumps(wrapper), str(payload_path)

    def decode_payload(self, payload_json: str) -> dict[str, Any]:
        """Deserializes payload, following file references if necessary."""
        from ms_flow.core.executor.manager import DataContractError

        payload = _safe_json_loads(payload_json)
        if isinstance(payload, dict) and CHUNK_PAYLOAD_REF_KEY in payload:
            ref = payload.get(CHUNK_PAYLOAD_REF_KEY) or {}
            raw_path = ref.get("path")
            if not raw_path:
                raise DataContractError("Chunk payload reference is missing path.")
            path = Path(str(raw_path)).expanduser().resolve()
            if not path.exists():
                raise DataContractError(f"Chunk payload reference not found: {path}")
            payload = _safe_json_loads(path.read_text(encoding="utf-8"))

        if not isinstance(payload, dict):
            raise DataContractError("Chunk payload must decode to dict.")
        return payload

    def remove_payload_file(self, path_str: str):
        """Unlinks a specific payload file."""
        path = str(path_str or "").strip()
        if not path:
            return
        try:
            Path(path).expanduser().resolve().unlink(missing_ok=True)
        except Exception:
            pass

    def cleanup_job(self, job_id: str):
        """Deletes the entire payload spool directory for a job."""
        with self._lock:
            payload_dir = self._job_payload_dirs.pop(job_id, None)

        if payload_dir is not None:
            try:
                shutil.rmtree(payload_dir, ignore_errors=True)
            except Exception:
                pass

    def cleanup_all(self):
        """Removes all job payload directories and clears the cache."""
        with self._lock:
            dirs = list(self._job_payload_dirs.values())
            self._job_payload_dirs.clear()

        for d in dirs:
            try:
                shutil.rmtree(d, ignore_errors=True)
            except Exception:
                pass
