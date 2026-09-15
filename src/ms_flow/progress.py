"""Terminal and notebook progress rendering for MolSuite jobs."""
from __future__ import annotations

import time
from typing import Any, Iterable

from tqdm.auto import tqdm


def _label(snapshot: Any) -> str:
    name = str(getattr(snapshot, "task_type", "job") or "job")
    return f"{name} [{getattr(snapshot, 'status', '')}]"


def _update(bar, snapshot: Any) -> None:
    bar.set_description(_label(snapshot), refresh=False)
    bar.n = min(100.0, max(0.0, float(getattr(snapshot, "progress", 0.0) or 0.0)))
    bar.refresh()


def watch_job(molsuite, job_id: str, *, follow: bool = True, poll_s: float = 0.25):
    """Render one job with tqdm and return its current or final snapshot.

    ``follow=False`` is a non-blocking snapshot.  The default follows the job
    until it reaches a terminal state.
    """
    if not follow:
        snapshot = molsuite.get_executor_job(str(job_id))
        if snapshot is None:
            raise RuntimeError(f"No se encontro job: {job_id}")
        bar = tqdm(total=100.0, unit="%", leave=True)
        try:
            _update(bar, snapshot)
        finally:
            bar.close()
        return snapshot

    bar = tqdm(total=100.0, unit="%", leave=True)
    try:
        return molsuite.wait_for_job(str(job_id), poll_s=poll_s, progress_cb=lambda row: _update(bar, row))
    finally:
        bar.close()


def watch_jobs(molsuite, job_ids: Iterable[str], *, follow: bool = True, poll_s: float = 0.25) -> dict[str, Any]:
    """Render several jobs concurrently and return their current or final snapshots."""
    ordered = [str(job_id) for job_id in job_ids]
    if not ordered:
        return {}
    bars = [tqdm(total=100.0, unit="%", position=index, leave=True) for index in range(len(ordered))]
    try:
        snapshots: dict[str, Any] = {}
        while True:
            pending = False
            for job_id, bar in zip(ordered, bars):
                snapshot = molsuite.get_executor_job(job_id)
                if snapshot is None:
                    raise RuntimeError(f"No se encontro job: {job_id}")
                snapshots[job_id] = snapshot
                _update(bar, snapshot)
                pending = pending or not bool(getattr(snapshot, "is_terminal", False))
            if not follow or not pending:
                return snapshots
            time.sleep(max(0.05, float(poll_s)))
    finally:
        for bar in bars:
            bar.close()


__all__ = ["watch_job", "watch_jobs"]
