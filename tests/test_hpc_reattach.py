"""A scheduler job outlives the desktop app, so closing it must not kill nor lose the run.

Two halves of the same rule: the adapter writes its scheduler id beside the control files so
a later process can find the job again, and the startup sweep adopts those jobs instead of
marking them failed like it does for work that died with the previous process.
"""
import json
import sys
from datetime import datetime
from pathlib import Path

from ms_flow.core.database import ExecutorDB, MasterDB
from ms_flow.core.database.executor_models import (
    ExecutorJob,
    ExecutorJobChunk,
    ExecutorJobFeedState,
)
from ms_flow.core.executor.hpc_adapter import HPCCommandExecutorAdapter
from ms_flow.core.executor.manager import ExecutorManager

sys.path.insert(0, str(Path(__file__).parent))
from adapter_test_fakes import write_fake_hpc_scheduler  # noqa: E402


def _chunks(params: dict, config: dict) -> list[dict]:
    return [{"index": index} for index in range(4)]


def _run(payload: dict) -> dict:
    return {"index": payload["index"]}


def _adapter(tmp_path: Path, scheduler_state: Path) -> HPCCommandExecutorAdapter:
    script = tmp_path / "fake_scheduler.py"
    if not script.exists():
        write_fake_hpc_scheduler(script)
    return HPCCommandExecutorAdapter(
        name="hpc_test",
        submit_command=[
            sys.executable, str(script), "submit",
            "{submit_script_path}", "{control_dir}", str(scheduler_state),
        ],
        poll_command=[
            sys.executable, str(script), "poll", "{scheduler_job_id}", str(scheduler_state),
        ],
        cancel_command=[
            sys.executable, str(script), "cancel", "{scheduler_job_id}", str(scheduler_state),
        ],
    )


def test_a_new_adapter_finds_a_chunk_the_previous_process_submitted(tmp_path: Path):
    wdir = tmp_path / "wdir"
    context = {"hpc_wdir": str(wdir)}
    submitter = _adapter(tmp_path, tmp_path / "state")
    try:
        handle = submitter.submit("job-1", "chunk-1", {"index": 0}, {"module": __name__, "fn": "_run"}, lambda _v: None, context)
        scheduler_job_id = submitter._handles[handle].scheduler_job_id
    finally:
        submitter.shutdown()

    # A fresh process: nothing in memory, only what is on disk.
    successor = _adapter(tmp_path, tmp_path / "state")
    try:
        adopted = successor.reattach("job-1", "chunk-1", context)
        assert adopted is not None
        assert successor._handles[adopted].scheduler_job_id == scheduler_job_id
        assert successor.reattach("job-1", "never-submitted", context) is None
    finally:
        successor.shutdown()


def test_the_startup_sweep_adopts_hpc_jobs_instead_of_failing_them(tmp_path: Path):
    wdir = tmp_path / "wdir"
    manager = ExecutorManager(
        executor_db=ExecutorDB(tmp_path / "e.db"),
        master_db=MasterDB(tmp_path / "m.db"),
        total_cpu=2,
    )
    manager._executors["hpc_test"] = _adapter(tmp_path, tmp_path / "state")
    payload = {
        "_runner_ref": f"{__name__}:_run",
        "_chunker_ref": f"{__name__}:_chunks",
        "_chunker_params": {},
        "_data_context": {"hpc_wdir": str(wdir)},
        "_store_results": True,
    }
    # Two chunks emitted, one of them still running on the cluster.
    control_dir = wdir / "_molsuite_runtime" / "job-1" / "chunk-2"
    control_dir.mkdir(parents=True)
    (control_dir / "handle.json").write_text(json.dumps({"scheduler_job_id": "sched-9"}))
    (control_dir / "status.json").write_text(json.dumps({"state": "RUNNING"}))
    now = datetime.now()
    with manager.executor_db.get_session() as session:
        session.add(ExecutorJob(job_id="job-1", executor_name="hpc_test", status="running",
                                payload_json=json.dumps(payload), total_chunks=4))
        session.add(ExecutorJobChunk(job_id="job-1", chunk_id="chunk-2", executor_name="hpc_test",
                                     status="running", started_at=now))
        session.add(ExecutorJobFeedState(job_id="job-1", cursor_position=2))
        session.commit()

    try:
        assert manager._reattach_external_jobs() == {"job-1"}
        running = manager.running_chunks_snapshot()
        assert [item.chunk_id for item in running] == ["chunk-2"]
        # The feed resumes where the cursor left it: two of the four chunks are already out.
        feed = manager.get_job_feed("job-1")
        assert [item["index"] for item in feed.item_source] == [2, 3]

        manager._terminalize_active_jobs(reason="runtime_interrupted", message="x",
                                         skip_job_ids={"job-1"})
        assert manager.get_job("job-1")["status"] == "running"
    finally:
        manager._executors["hpc_test"].shutdown()
