from __future__ import annotations

import base64
import hashlib
import json
import pickle
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Optional

from sqlmodel import select

from ms_flow.core.data import DataBridge, DataContext, DbOutputSpec, FileOutputSpec, OutputSpec
from ms_flow.core.database.executor_models import ExecutorJobChunk


def _safe_json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


_OUTPUT_BYTES_KEY = "__molsuite_output_bytes__"
_OUTPUT_DATETIME_KEY = "__molsuite_output_datetime__"
_OUTPUT_DATE_KEY = "__molsuite_output_date__"
_SPILL_FORMAT = "pickle"
_SPILL_SERIALIZER_VERSION = 1


@dataclass(frozen=True)
class OutputRetentionPolicy:
    store_final_result_json: bool
    store_sink_payload_json: bool
    store_sink_receipt_json: bool
    sink_payload_storage_reason: str
    final_result_storage_reason: str


@dataclass(frozen=True)
class StagedOutput:
    chunk_id: str
    payload_ref: str
    payload_bytes: int
    item_count: int
    payload: Any = None
    storage_kind: str = "file"
    payload_format: str = _SPILL_FORMAT
    serializer_version: int = _SPILL_SERIALIZER_VERSION


def resolve_output_retention_policy(output_spec: OutputSpec) -> OutputRetentionPolicy:
    if isinstance(output_spec, DbOutputSpec):
        target = "project.db sink"
    elif isinstance(output_spec, FileOutputSpec):
        target = "file sink"
    else:
        raise ValueError(f"Unsupported output spec for retention policy: {type(output_spec).__name__}")
    return OutputRetentionPolicy(
        store_final_result_json=False,
        store_sink_payload_json=False,
        store_sink_receipt_json=True,
        sink_payload_storage_reason="sink payload is spooled outside the operational store",
        final_result_storage_reason=f"final result belongs in {target}, not the operational store",
    )


def _encode_output_payload(value: Any) -> Any:
    if isinstance(value, bytes):
        return {_OUTPUT_BYTES_KEY: base64.b64encode(value).decode("ascii")}
    if isinstance(value, datetime):
        return {_OUTPUT_DATETIME_KEY: value.isoformat()}
    if isinstance(value, date):
        return {_OUTPUT_DATE_KEY: value.isoformat()}
    if isinstance(value, dict):
        return {str(key): _encode_output_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_encode_output_payload(item) for item in value]
    return value


def _decode_output_payload(value: Any) -> Any:
    if isinstance(value, dict):
        if set(value) == {_OUTPUT_BYTES_KEY}:
            return base64.b64decode(str(value[_OUTPUT_BYTES_KEY]).encode("ascii"))
        if set(value) == {_OUTPUT_DATETIME_KEY}:
            return datetime.fromisoformat(str(value[_OUTPUT_DATETIME_KEY]))
        if set(value) == {_OUTPUT_DATE_KEY}:
            return date.fromisoformat(str(value[_OUTPUT_DATE_KEY]))
        return {str(key): _decode_output_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_decode_output_payload(item) for item in value]
    return value


class ResultHandler:
    def handle(self, chunk_id: str, result: Any) -> None:
        raise NotImplementedError

    def on_error(self, chunk_id: str, error: str) -> None:
        pass

    def flush(self) -> None:
        pass


class BufferedResultHandler(ResultHandler):
    def __init__(self, flush_every: int = 500):
        self._flush_every = flush_every
        self._buffer: list = []

    def handle(self, chunk_id: str, result: Any) -> None:
        self._buffer.append(result)
        if len(self._buffer) >= self._flush_every:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        self.process_buffer(self._buffer)
        self._buffer.clear()

    def process_buffer(self, buffer: list) -> None:
        raise NotImplementedError


class SimpleResultHandler(ResultHandler):
    def __init__(self, db, model, flush_every: int = 50):
        self._db = db
        self._model = model
        self._flush_every = flush_every
        self._buffer: list = []

    def handle(self, chunk_id: str, result: Any) -> None:
        if isinstance(result, dict):
            self._buffer.append(self._model(**result))
        if len(self._buffer) >= self._flush_every:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        with self._db.get_session() as session:
            session.add_all(self._buffer)
            session.commit()
        self._buffer.clear()


class CallbackResultHandler(ResultHandler):
    def __init__(
        self,
        on_result: Callable[[str, Any], None],
        on_error: Optional[Callable[[str, str], None]] = None,
    ):
        self._on_result = on_result
        self._on_error = on_error

    def handle(self, chunk_id: str, result: Any) -> None:
        self._on_result(chunk_id, result)

    def on_error(self, chunk_id: str, error: str) -> None:
        if self._on_error:
            self._on_error(chunk_id, error)


class OutputSpecResultHandler(ResultHandler):
    """Stages outputs on the engine thread and writes only project data in workers."""

    def __init__(
        self,
        *,
        executor_db: Any,
        job_id: str,
        bridge: DataBridge,
        output_spec: OutputSpec,
        data_context: DataContext,
        flush_every: int = 500,
        max_buffer_size: int = 5000,
        max_buffer_bytes: int = 16 * 1024 * 1024,
        max_payload_bytes: int = 4 * 1024 * 1024,
        max_pending_chunks: int = 1024,
        max_pending_bytes: int = 256 * 1024 * 1024,
        flush_retries: int = 3,
        retry_backoff_s: float = 0.05,
        **_ignored: Any,
    ):
        if not isinstance(output_spec, (DbOutputSpec, FileOutputSpec)):
            raise ValueError("OutputSpecResultHandler supports DbOutputSpec and FileOutputSpec.")
        self._executor_db = executor_db
        self._job_id = str(job_id)
        self._bridge = bridge
        self._output_spec = output_spec
        self._retention_policy = resolve_output_retention_policy(output_spec)
        self._data_context = data_context
        self._sink_key = hashlib.sha1(
            json.dumps(output_spec.to_wire(), ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        self._flush_every = max(1, int(flush_every))
        self._max_buffer_size = max(1, int(max_buffer_size))
        self._max_buffer_bytes = max(1024, int(max_buffer_bytes))
        self._max_payload_bytes = max(1024, int(max_payload_bytes))
        self._max_pending_chunks = max(1, int(max_pending_chunks))
        self._max_pending_bytes = max(1024, int(max_pending_bytes))
        self._flush_retries = max(0, int(flush_retries))
        self._retry_backoff_s = max(0.0, float(retry_backoff_s))
        self._lock = threading.RLock()
        self._pending: dict[str, StagedOutput] = {}
        self._file_history: list[StagedOutput] = []
        self._file_manifest: list[str] = []  # per_batch: paths only, one per flush
        self._file_batch_index = 0
        self._total_items_written = 0
        self._total_bytes_written = 0
        self._flush_count = 0
        self._retry_count = 0
        self._flush_failures = 0
        self._last_error = ""
        self._last_flush_items = 0
        self._last_flush_bytes = 0
        self._last_flush_duration_ms = 0.0
        self._last_flush_at = ""
        self._max_observed_buffer_items = 0
        self._max_observed_buffer_bytes = 0
        self._oversized_items = 0
        self._spill_count = 0

    @property
    def _file_append(self) -> bool:
        """File sink in append mode: each flush writes only its own batch.

        Without it the handler keeps everything produced (`_file_history` plus its payloads) and
        rewrites the whole file on every flush: O(N) RAM and O(N^2) IO over the job's total.
        """
        return isinstance(self._output_spec, FileOutputSpec) and bool(self._output_spec.append)

    @property
    def _file_per_batch(self) -> bool:
        """One numbered file per batch; only the path manifest stays in memory."""
        return isinstance(self._output_spec, FileOutputSpec) and bool(self._output_spec.per_batch)

    @property
    def supports_concurrent_writers(self) -> bool:
        return False

    @property
    def write_batch_size(self) -> int:
        return min(self._flush_every, self._max_pending_chunks)

    def _output_payload_dir(self) -> Path:
        raw_project_dir = str(self._data_context.project_dir or "").strip()
        if raw_project_dir:
            base = Path(raw_project_dir).expanduser().resolve() / "tmp"
        else:
            db_path = getattr(self._executor_db, "db_path", None)
            base = Path(db_path).resolve().parent if db_path is not None else Path.cwd()
        target = base / "chunk_payloads" / self._job_id
        target.mkdir(parents=True, exist_ok=True)
        return target

    def _write_payload(self, chunk_id: str, result: Any) -> str:
        path = self._output_payload_dir() / f"output-{chunk_id}.pickle"
        with path.open("wb") as fh:
            pickle.dump(
                {
                    "serializer_version": _SPILL_SERIALIZER_VERSION,
                    "payload": result,
                },
                fh,
                protocol=pickle.HIGHEST_PROTOCOL,
            )
        return str(path)

    @staticmethod
    def _read_payload(payload_ref: str) -> Any:
        # Pickle is only used for local temporary files created by Molsuite Flow.
        # Do not use this path for user-provided or untrusted input.
        with Path(payload_ref).open("rb") as fh:
            raw = pickle.load(fh)
        if not isinstance(raw, dict) or raw.get("serializer_version") != _SPILL_SERIALIZER_VERSION:
            raise RuntimeError("Unsupported internal output payload format.")
        return raw.get("payload")

    @staticmethod
    def _remove_payload(payload_ref: str) -> None:
        if not payload_ref:
            return
        try:
            Path(payload_ref).unlink(missing_ok=True)
        except Exception:
            pass

    @staticmethod
    def _estimate_bytes(result: Any) -> int:
        if isinstance(result, bytes):
            return len(result)
        if isinstance(result, str):
            return len(result.encode("utf-8"))
        try:
            return len(pickle.dumps(result, protocol=pickle.HIGHEST_PROTOCOL))
        except Exception:
            return len(_safe_json_dumps(result).encode("utf-8"))

    def _item_count(self, result: Any) -> int:
        if isinstance(self._output_spec, DbOutputSpec) and self._output_spec.mode == "graph":
            return sum(len(rows) if isinstance(rows, list) else 1 for rows in result.values())
        return len(result) if isinstance(result, list) else 1

    def _validate_result(self, result: Any) -> None:
        if isinstance(self._output_spec, FileOutputSpec):
            return
        if self._output_spec.mode == "graph":
            if not isinstance(result, dict):
                raise TypeError("GraphOutputSpec result must be a dict keyed by node name.")
            return
        if isinstance(result, dict):
            return
        if isinstance(result, list) and all(isinstance(item, dict) for item in result):
            return
        raise TypeError("DbOutputSpec result must be dict or list[dict].")

    def _pending_pressure(self) -> tuple[int, int]:
        with self._lock:
            return len(self._pending), sum(item.payload_bytes for item in self._pending.values())

    def _should_spill(self, payload_bytes: int) -> bool:
        pending_count, pending_bytes = self._pending_pressure()
        return (
            payload_bytes > self._max_payload_bytes
            or pending_count + 1 > self._max_buffer_size
            or pending_bytes + payload_bytes > self._max_buffer_bytes
            or pending_count + 1 > self._max_pending_chunks
            or pending_bytes + payload_bytes > self._max_pending_bytes
        )

    def _make_staged_output(self, chunk_id: str, result: Any, payload_bytes: int) -> StagedOutput:
        if self._should_spill(payload_bytes):
            self._spill_count += 1
            return StagedOutput(
                chunk_id=str(chunk_id),
                payload_ref=self._write_payload(chunk_id, result),
                payload_bytes=payload_bytes,
                item_count=self._item_count(result),
                storage_kind="file",
                payload_format=_SPILL_FORMAT,
                serializer_version=_SPILL_SERIALIZER_VERSION,
            )
        return StagedOutput(
            chunk_id=str(chunk_id),
            payload_ref="",
            payload_bytes=payload_bytes,
            item_count=self._item_count(result),
            payload=result,
            storage_kind="memory",
            payload_format="python",
            serializer_version=_SPILL_SERIALIZER_VERSION,
        )

    def _payload_envelope(self, staged: StagedOutput) -> dict[str, Any]:
        envelope = {
            "kind": staged.storage_kind,
            "format": staged.payload_format,
            "bytes": staged.payload_bytes,
            "serializer_version": staged.serializer_version,
        }
        if staged.payload_ref:
            envelope["path"] = staged.payload_ref
        return envelope

    def _staged_payload(self, staged: StagedOutput) -> Any:
        if staged.storage_kind == "memory":
            return staged.payload
        return self._read_payload(staged.payload_ref)

    @staticmethod
    def _storage_metrics(items: list[StagedOutput]) -> dict[str, int]:
        memory = [item for item in items if item.storage_kind == "memory"]
        disk = [item for item in items if item.storage_kind != "memory"]
        return {
            "memory_buffered_items": len(memory),
            "memory_buffered_bytes": sum(item.payload_bytes for item in memory),
            "disk_buffered_items": len(disk),
            "disk_buffered_bytes": sum(item.payload_bytes for item in disk),
        }

    def stage(self, chunk_id: str, result: Any) -> StagedOutput:
        self._validate_result(result)
        payload_bytes = self._estimate_bytes(result)
        if payload_bytes > self._max_payload_bytes:
            self._oversized_items += 1
        staged = self._make_staged_output(chunk_id, result, payload_bytes)
        now = datetime.now()
        sink_info = {
            "sink_key": self._sink_key,
            "commit_key": staged.chunk_id,
            "phase": "produced",
            "payload_bytes": staged.payload_bytes,
            "item_count": staged.item_count,
            "payload_storage": staged.storage_kind,
            "payload_envelope": self._payload_envelope(staged),
        }
        with self._executor_db.get_session() as session:
            chunk = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.chunk_id == staged.chunk_id)
            ).first()
            if chunk is None:
                self._remove_payload(staged.payload_ref)
                raise RuntimeError(f"Chunk not found while staging output: {staged.chunk_id}")
            chunk.output_state = "produced"
            chunk.output_payload_json = "{}"
            chunk.output_sink_info_json = _safe_json_dumps(sink_info)
            chunk.output_produced_at = now
            chunk.updated_at = now
            session.add(chunk)
            session.commit()
        with self._lock:
            self._pending[staged.chunk_id] = staged
            if isinstance(self._output_spec, FileOutputSpec) and not (self._file_append or self._file_per_batch):
                self._file_history.append(staged)
            self._max_observed_buffer_items = max(
                self._max_observed_buffer_items,
                len(self._pending),
            )
            self._max_observed_buffer_bytes = max(
                self._max_observed_buffer_bytes,
                sum(item.payload_bytes for item in self._pending.values()),
            )
        return staged

    def _file_payload(self, batch: list[StagedOutput]) -> Any:
        assert isinstance(self._output_spec, FileOutputSpec)
        if self._file_append or self._file_per_batch:
            staged_items = list(batch)
        else:
            with self._lock:
                staged_items = list(self._file_history)
        values: list[Any] = []
        for item in staged_items:
            value = self._staged_payload(item)
            values.extend(value if isinstance(value, list) else [value])
        if self._output_spec.fmt == "json":
            return values
        if self._output_spec.fmt == "text":
            text = "\n".join(
                value if isinstance(value, str) else _safe_json_dumps(value)
                for value in values
            )
            # Every batch ends with a newline: otherwise the last line of one and the first
            # of the next would be glued together in the file.
            return f"{text}\n" if self._file_append and text else text
        parts: list[bytes] = []
        for value in values:
            if isinstance(value, bytes):
                parts.append(value)
            elif isinstance(value, str):
                parts.append(value.encode(self._output_spec.encoding))
            else:
                parts.append(_safe_json_dumps(value).encode(self._output_spec.encoding))
        return b"".join(parts)

    def _batch_file_spec(self) -> FileOutputSpec:
        """This batch's path: `out.sdf` -> `out.000003.sdf`."""
        spec = self._output_spec
        assert isinstance(spec, FileOutputSpec)
        raw = PurePosixPath(spec.path)
        with self._lock:
            numbered = f"{raw.stem}.{self._file_batch_index:06d}{raw.suffix}"
        return FileOutputSpec(
            path=str(raw.with_name(numbered)),
            root=spec.root,
            fmt=spec.fmt,
            encoding=spec.encoding,
            ensure_parent=spec.ensure_parent,
            schema=spec.schema,
            meta=dict(spec.meta or {}),
        )

    def _combined_db_payload(self, staged_items: list[StagedOutput]) -> Any:
        values = [self._staged_payload(item) for item in staged_items]
        assert isinstance(self._output_spec, DbOutputSpec)
        if self._output_spec.mode == "graph":
            combined: dict[str, list[dict[str, Any]]] = {}
            for value in values:
                for node_name, rows in value.items():
                    normalized = rows if isinstance(rows, list) else [rows]
                    combined.setdefault(str(node_name), []).extend(
                        dict(row) for row in normalized if row is not None
                    )
            return combined
        combined_rows: list[dict[str, Any]] = []
        for value in values:
            normalized = value if isinstance(value, list) else [value]
            combined_rows.extend(dict(row) for row in normalized)
        return combined_rows

    def write_batch(self, staged_items: list[StagedOutput]) -> dict[str, dict[str, Any]]:
        if not staged_items:
            return {}
        started = time.perf_counter()
        last_error: Exception | None = None
        for attempt in range(self._flush_retries + 1):
            try:
                if isinstance(self._output_spec, FileOutputSpec):
                    payload = self._file_payload(staged_items)
                    spec = self._batch_file_spec() if self._file_per_batch else self._output_spec
                    batch_receipt = self._bridge.persist_output(
                        spec,
                        payload,
                        self._data_context,
                    )
                    if self._file_per_batch:
                        with self._lock:
                            self._file_batch_index += 1
                            self._file_manifest.append(str(batch_receipt.get("path") or spec.path))
                            batch_receipt = {**batch_receipt, "batch_paths": list(self._file_manifest)}
                else:
                    payload = self._combined_db_payload(staged_items)
                    # ponytail: no per-chunk commit receipt. Crash-safety for
                    # "computed but not persisted" is delegated to domain
                    # idempotency (upsert on business keys / skip-existing before
                    # submit), so a re-run after a crash overwrites/skips instead
                    # of duplicating. Writing receipts in a second transaction
                    # only reintroduced a crash window (data committed, receipt
                    # not) for no consumer — output_commits_exist is app-facing
                    # API, not used by the executor. Upgrade path: if some job
                    # must avoid recompute, replay the on-disk output spill files.
                    batch_receipt = self._bridge.persist_output(
                        self._output_spec,
                        payload,
                        self._data_context,
                    )
                total_items = sum(item.item_count for item in staged_items)
                total_bytes = sum(item.payload_bytes for item in staged_items)
                with self._lock:
                    self._flush_count += 1
                    self._total_items_written += total_items
                    self._total_bytes_written += total_bytes
                    self._last_flush_items = total_items
                    self._last_flush_bytes = total_bytes
                    self._last_flush_duration_ms = (time.perf_counter() - started) * 1000.0
                    self._last_flush_at = datetime.now().isoformat()
                    self._last_error = ""
                receipt = dict(batch_receipt or {})
                receipt["batch_chunk_count"] = len(staged_items)
                return {item.chunk_id: dict(receipt) for item in staged_items}
            except Exception as exc:
                last_error = exc
                with self._lock:
                    self._retry_count += 1
                if attempt < self._flush_retries and self._retry_backoff_s > 0:
                    time.sleep(self._retry_backoff_s * (2**attempt))
        with self._lock:
            self._flush_failures += 1
            self._last_error = str(last_error or "unknown output write error")
        raise RuntimeError(f"Output sink write failed after retries: {self._last_error}")

    def write_staged(self, staged: StagedOutput) -> dict[str, Any]:
        return self.write_batch([staged])[staged.chunk_id]

    def _sink_info(self, staged: StagedOutput, phase: str, receipt: dict[str, Any] | None = None, error: str = "") -> dict[str, Any]:
        sink_info = {
            "sink_key": self._sink_key,
            "commit_key": staged.chunk_id,
            "phase": phase,
            "payload_bytes": staged.payload_bytes,
            "item_count": staged.item_count,
            "payload_storage": staged.storage_kind,
            "payload_envelope": self._payload_envelope(staged),
        }
        if receipt:
            sink_info.update(dict(receipt))
        if error:
            sink_info["error"] = str(error)
        return sink_info

    def confirm(self, staged: StagedOutput, receipt: dict[str, Any]) -> None:
        self.confirm_batch([(staged, receipt)])

    def confirm_batch(self, confirmations: list[tuple[StagedOutput, dict[str, Any]]]) -> None:
        if not confirmations:
            return
        now = datetime.now()
        staged_by_id = {staged.chunk_id: staged for staged, _ in confirmations}
        receipts_by_id = {staged.chunk_id: dict(receipt or {}) for staged, receipt in confirmations}
        with self._executor_db.get_session() as session:
            chunks = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.chunk_id.in_(list(staged_by_id)))
            ).all()
            for chunk in chunks:
                staged = staged_by_id.get(chunk.chunk_id)
                if staged is None:
                    continue
                chunk.output_state = "confirmed"
                chunk.output_sink_info_json = _safe_json_dumps(
                    self._sink_info(staged, "confirmed", receipts_by_id.get(chunk.chunk_id) or {})
                )
                chunk.output_persisted_at = now
                chunk.output_confirmed_at = now
                chunk.updated_at = now
                session.add(chunk)
            session.commit()
        with self._lock:
            for staged in staged_by_id.values():
                self._pending.pop(staged.chunk_id, None)
        if isinstance(self._output_spec, DbOutputSpec) or self._file_append or self._file_per_batch:
            for staged in staged_by_id.values():
                self._remove_payload(staged.payload_ref)

    def reject(self, staged: StagedOutput, error: str) -> None:
        self.reject_batch([(staged, error)])

    def reject_batch(self, rejections: list[tuple[StagedOutput, str]]) -> None:
        if not rejections:
            return
        now = datetime.now()
        staged_by_id = {staged.chunk_id: staged for staged, _ in rejections}
        errors_by_id = {staged.chunk_id: str(error) for staged, error in rejections}
        with self._executor_db.get_session() as session:
            chunks = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.chunk_id.in_(list(staged_by_id)))
            ).all()
            for chunk in chunks:
                staged = staged_by_id.get(chunk.chunk_id)
                if staged is None:
                    continue
                chunk.output_state = "failed"
                chunk.output_sink_info_json = _safe_json_dumps(
                    self._sink_info(staged, "failed", error=errors_by_id.get(chunk.chunk_id, ""))
                )
                chunk.updated_at = now
                session.add(chunk)
            session.commit()
        with self._lock:
            for staged in staged_by_id.values():
                self._pending.pop(staged.chunk_id, None)
            self._last_error = next(iter(errors_by_id.values()), "")
        for staged in staged_by_id.values():
            self._remove_payload(staged.payload_ref)

    def handle(self, chunk_id: str, result: Any) -> None:
        staged = self.stage(chunk_id, result)
        self.confirm(staged, self.write_staged(staged))

    def flush(self) -> None:
        return None

    def close(self) -> None:
        with self._lock:
            refs = {item.payload_ref for item in self._file_history if item.payload_ref}
            refs.update(item.payload_ref for item in self._pending.values() if item.payload_ref)
            self._file_history.clear()
            self._file_manifest.clear()
            self._pending.clear()
        for payload_ref in refs:
            self._remove_payload(payload_ref)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            pending_items = list(self._pending.values())
            buffered_items = len(pending_items)
            buffered_bytes = sum(item.payload_bytes for item in pending_items)
            storage_metrics = self._storage_metrics(pending_items)
            return {
                "type": "db_output_sink" if isinstance(self._output_spec, DbOutputSpec) else "file_output_sink",
                "retention_policy": self._retention_policy.__dict__.copy(),
                "buffered_items": buffered_items,
                "buffered_bytes": buffered_bytes,
                **storage_metrics,
                "flush_every": self._flush_every,
                "max_buffer_size": self._max_buffer_size,
                "max_buffer_bytes": self._max_buffer_bytes,
                "max_payload_bytes": self._max_payload_bytes,
                "max_pending_chunks": self._max_pending_chunks,
                "max_pending_bytes": self._max_pending_bytes,
                "flush_retries": self._flush_retries,
                "retry_backoff_s": self._retry_backoff_s,
                "total_items_written": self._total_items_written,
                "total_bytes_written": self._total_bytes_written,
                "flush_count": self._flush_count,
                "last_flush_items": self._last_flush_items,
                "last_flush_bytes": self._last_flush_bytes,
                "last_flush_duration_ms": self._last_flush_duration_ms,
                "last_flush_at": self._last_flush_at,
                "retry_count": self._retry_count,
                "flush_failures": self._flush_failures,
                "rejected_items": 0,
                "oversized_items": self._oversized_items,
                "spill_count": self._spill_count,
                "max_observed_buffer_items": self._max_observed_buffer_items,
                "max_observed_buffer_bytes": self._max_observed_buffer_bytes,
                "last_error": self._last_error,
                "sink_key": self._sink_key,
                "supports_concurrent_writers": self.supports_concurrent_writers,
            }
