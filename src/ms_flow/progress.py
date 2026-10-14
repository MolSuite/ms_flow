"""Follow jobs from a terminal or a notebook: a tqdm bar for each."""
import time


def watch_jobs(engine, job_ids, *, poll_s: float = 0.25) -> dict:
    """Show a bar for each job until all of them have ended. Returns `{job_id: JobSnapshot}`.

    A job without a total shows its counts and no percentage.
    """
    from tqdm.auto import tqdm  # ponytail: not a dependency of ms_flow; whoever watches has it

    job_ids = list(job_ids)
    bars = [tqdm(total=100.0, unit="%", position=place) for place in range(len(job_ids))]
    try:
        while True:
            jobs = {job_id: engine.status(job_id) for job_id in job_ids}
            for bar, job in zip(bars, jobs.values()):
                bar.set_description(f"{job.name or job.id} [{job.status}]", refresh=False)
                bar.set_postfix(done=job.chunks_done, failed=job.chunks_failed, refresh=False)
                bar.n = 100.0 * (job.progress or 0.0)
                bar.refresh()
            if all(job.finished_at is not None for job in jobs.values()):
                return jobs
            time.sleep(poll_s)
    finally:
        for bar in bars:
            bar.close()


def watch_job(engine, job_id: int, *, poll_s: float = 0.25):
    """`watch_jobs` for one job. Returns its last `JobSnapshot`."""
    return watch_jobs(engine, [job_id], poll_s=poll_s)[job_id]
