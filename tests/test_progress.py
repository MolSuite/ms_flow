from __future__ import annotations

from ms_flow.progress import watch_job, watch_jobs


class Snapshot:
    def __init__(self, job_id, *, progress=25.0, terminal=False):
        self.job_id = job_id
        self.progress = progress
        self.task_type = "test"
        self.status = "completed" if terminal else "running"
        self.is_terminal = terminal


class Runtime:
    def __init__(self):
        self.rows = {"a": Snapshot("a", terminal=True), "b": Snapshot("b", terminal=True)}

    def get_executor_job(self, job_id):
        return self.rows.get(job_id)

    def wait_for_job(self, job_id, *, poll_s, progress_cb):
        row = self.rows[job_id]
        progress_cb(row)
        return row


def test_watch_job_snapshot_and_multiple_jobs(monkeypatch):
    class Bar:
        def set_description(self, *_args, **_kwargs): pass
        def refresh(self): pass
        def close(self): pass
        n = 0

    monkeypatch.setattr("ms_flow.progress.tqdm", lambda **_kwargs: Bar())
    runtime = Runtime()
    assert watch_job(runtime, "a", follow=False).job_id == "a"
    assert set(watch_jobs(runtime, ["a", "b"], follow=False)) == {"a", "b"}
