import json
import sqlite3
from pathlib import Path

from sqlmodel import select

from ms_flow.core.data import DataBridge, DataContext, DbOutputSpec
from ms_flow.core.database import ExecutorDB, ProjectCommitReceipt, ProjectStore
from ms_flow.core.database.executor_models import ExecutorJobChunk
from ms_flow.core.executor.result_handlers import OutputSpecResultHandler, StagedOutput
from ms_flow.core.executor.services.job_status import JobStatusService
from ms_flow.core.executor.services.persistence_coordinator import (
    INTENT_CANCELED,
    INTENT_COMPLETED,
    INTENT_FAILED,
)
from ms_flow.core.executor.services.sink_writer_pool import SinkWriteCompletion, SinkWriteTask


class _BatchProbeHandler:
    def __init__(self):
        self.confirm_calls = []
        self.reject_calls = []

    def confirm_batch(self, items):
        self.confirm_calls.append(list(items))

    def reject_batch(self, items):
        self.reject_calls.append(list(items))


class _StaticCompletionPool:
    def __init__(self, completions):
        self._completions = list(completions)

    def drain_completions(self):
        completions = self._completions
        self._completions = []
        return completions


class _CoordinatorProbe:
    def __init__(self):
        self.transitions = []

    def enqueue(self, transition):
        self.transitions.append(transition)


class _EventRecorderProbe:
    def __init__(self, events):
        self._events = events

    def add_event(self, **event):
        self._events.append(event)


class _ManagerProbe:
    def __init__(self, completions):
        self._sink_writer_pool = _StaticCompletionPool(completions)
        self.persistence_coordinator = _CoordinatorProbe()
        self.events = []
        self.event_recorder = _EventRecorderProbe(self.events)

    @staticmethod
    def is_cancel_requested(job_id):
        return job_id == "cancel-job"


def _task(handler, chunk_id, *, job_id="job"):
    return SinkWriteTask(
        job_id=job_id,
        chunk_id=chunk_id,
        handler=handler,
        staged=chunk_id,
        output_json="{}",
    )


def _add_chunk(executor_db, chunk_id, *, job_id="job"):
    with executor_db.get_session() as session:
        session.add(
            ExecutorJobChunk(
                job_id=job_id,
                chunk_id=chunk_id,
                status="staging",
            )
        )
        session.commit()


def _staging_handler(tmp_path, executor_db, **limits):
    return OutputSpecResultHandler(
        executor_db=executor_db,
        job_id="job",
        bridge=DataBridge(),
        output_spec=DbOutputSpec(
            table="results",
            columns=("id", "blob"),
            db_role="custom",
            db_path=str(tmp_path / "custom.db"),
        ),
        data_context=DataContext(project_dir=tmp_path),
        flush_every=10,
        **limits,
    )


def test_job_status_batches_sink_confirm_and_reject():
    handler = _BatchProbeHandler()
    completions = [
        SinkWriteCompletion(task=_task(handler, "chunk-1"), receipt={"rows": 1}),
        SinkWriteCompletion(task=_task(handler, "chunk-2"), receipt={"rows": 1}),
        SinkWriteCompletion(task=_task(handler, "chunk-3"), error="sink failed"),
        SinkWriteCompletion(task=_task(handler, "chunk-4", job_id="cancel-job"), receipt={"rows": 1}),
    ]
    manager = _ManagerProbe(completions)

    JobStatusService(manager)._poll_sink_completions()

    assert handler.confirm_calls == [[("chunk-1", {"rows": 1}), ("chunk-2", {"rows": 1})]]
    assert handler.reject_calls == [
        [
            ("chunk-3", "sink failed"),
            ("chunk-4", "Sink write finished after cancellation request; result discarded."),
        ]
    ]
    intents_by_chunk = {
        transition.chunk_id: transition.intent
        for transition in manager.persistence_coordinator.transitions
    }
    assert intents_by_chunk == {
        "chunk-1": INTENT_COMPLETED,
        "chunk-2": INTENT_COMPLETED,
        "chunk-3": INTENT_FAILED,
        "chunk-4": INTENT_CANCELED,
    }
    assert [event["event_type"] for event in manager.events] == ["result_sink_failed"]


def test_output_staging_keeps_small_payload_in_memory(tmp_path):
    executor_db = ExecutorDB(tmp_path / "executor.db")
    _add_chunk(executor_db, "chunk-memory")
    handler = _staging_handler(tmp_path, executor_db, max_payload_bytes=4096)

    staged = handler.stage("chunk-memory", {"id": 1, "blob": "abc"})
    snapshot = handler.snapshot()

    try:
        assert staged.storage_kind == "memory"
        assert staged.payload_ref == ""
        assert staged.payload == {"id": 1, "blob": "abc"}
        assert snapshot["memory_buffered_items"] == 1
        assert snapshot["memory_buffered_bytes"] == staged.payload_bytes
        assert snapshot["disk_buffered_items"] == 0
        assert snapshot["spill_count"] == 0
    finally:
        handler.close()


def test_output_staging_spills_large_payload_to_disk(tmp_path):
    executor_db = ExecutorDB(tmp_path / "executor.db")
    _add_chunk(executor_db, "chunk-disk")
    handler = _staging_handler(tmp_path, executor_db, max_payload_bytes=1024)

    staged = handler.stage("chunk-disk", {"id": 1, "blob": "x" * 2048})
    snapshot = handler.snapshot()

    try:
        assert staged.storage_kind == "file"
        assert staged.payload_ref
        assert Path(staged.payload_ref).suffix == ".pickle"
        assert handler._staged_payload(staged) == {"id": 1, "blob": "x" * 2048}
        assert snapshot["memory_buffered_items"] == 0
        assert snapshot["disk_buffered_items"] == 1
        assert snapshot["disk_buffered_bytes"] == staged.payload_bytes
        assert snapshot["spill_count"] == 1
        with executor_db.get_session() as session:
            chunk = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.chunk_id == "chunk-disk")
            ).first()
        sink_info = json.loads(chunk.output_sink_info_json)
        assert sink_info["payload_envelope"]["kind"] == "file"
        assert sink_info["payload_envelope"]["format"] == "pickle"
        assert sink_info["payload_envelope"]["serializer_version"] == 1
    finally:
        payload_ref = staged.payload_ref
        handler.close()
        assert not payload_ref or not Path(payload_ref).exists()


def test_output_staging_spills_when_pending_chunk_pressure_is_high(tmp_path):
    executor_db = ExecutorDB(tmp_path / "executor.db")
    _add_chunk(executor_db, "chunk-1")
    _add_chunk(executor_db, "chunk-2")
    handler = _staging_handler(tmp_path, executor_db, max_payload_bytes=4096, max_pending_chunks=1)

    first = handler.stage("chunk-1", {"id": 1, "blob": "abc"})
    second = handler.stage("chunk-2", {"id": 2, "blob": "def"})
    snapshot = handler.snapshot()

    try:
        assert first.storage_kind == "memory"
        assert second.storage_kind == "file"
        assert snapshot["memory_buffered_items"] == 1
        assert snapshot["disk_buffered_items"] == 1
        assert snapshot["spill_count"] == 1
    finally:
        handler.close()


def test_project_batch_write_persists_rows_without_chunk_receipts(tmp_path):
    """Batched project writes land every row in one transaction and write NO
    per-chunk commit receipts. Re-run crash-safety is delegated to domain
    idempotency (upsert / skip-existing), so the executor keeps no receipt
    ledger — which is what removes the data-committed-but-receipt-missing crash
    window that a second receipt transaction reintroduced.
    """
    db_path = tmp_path / "project.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE docking_results (ligand_id INTEGER, score REAL)")
        conn.commit()
    finally:
        conn.close()

    spec = DbOutputSpec(
        table="docking_results",
        columns=("ligand_id", "score"),
        db_role="project",
    )
    handler = OutputSpecResultHandler(
        executor_db=object(),
        job_id="job",
        bridge=DataBridge(),
        output_spec=spec,
        data_context=DataContext(project_db_path=db_path),
        flush_every=2,
    )
    payload_1 = Path(handler._write_payload("chunk-1", {"ligand_id": 1, "score": 1.0}))
    payload_2 = Path(handler._write_payload("chunk-2", {"ligand_id": 2, "score": 2.0}))

    try:
        handler.write_batch(
            [
                StagedOutput("chunk-1", str(payload_1), payload_1.stat().st_size, 1),
                StagedOutput("chunk-2", str(payload_2), payload_2.stat().st_size, 1),
            ]
        )
    finally:
        payload_1.unlink(missing_ok=True)
        payload_2.unlink(missing_ok=True)

    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute("SELECT ligand_id, score FROM docking_results ORDER BY ligand_id").fetchall()
        receipt_table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='molsuite_output_commits'"
        ).fetchone()
    finally:
        conn.close()
        handler.close()

    assert rows == [(1, 1.0), (2, 2.0)]
    assert receipt_table is None


def test_data_bridge_output_commits_exist_uses_project_store(tmp_path):
    db_path = tmp_path / "project.db"
    store = ProjectStore.open_at(db_path)
    try:
        store.record_commit_receipts(
            [
                ProjectCommitReceipt(
                    sink_key="sink-a",
                    commit_key="chunk-1",
                    target_name="results",
                    row_count=1,
                )
            ]
        )
        bridge = DataBridge()
        existing = bridge.output_commits_exist(
            DbOutputSpec(table="results", columns=("id",), db_role="project"),
            DataContext(project_db_path=db_path, extras={"molsuite_output_sink_key": "sink-a"}),
            ["chunk-1", "chunk-2"],
        )
    finally:
        store.dispose()

    assert existing == {"chunk-1"}
