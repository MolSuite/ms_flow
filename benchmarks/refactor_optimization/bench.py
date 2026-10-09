"""Orchestration cost and robustness of the engine, recorded once per refactor step.

    python -m benchmarks.refactor_optimization.bench --label baseline
    python -m benchmarks.refactor_optimization.bench --label "pool reset"
    python -m benchmarks.refactor_optimization.report

Each run writes `results/<label>.json`; `report.py` renders every recorded step against
the first one as `docs/03-impacto.html`. Nothing here rejects a change: it only shows what each one did.

Tasks are empty on purpose, so every millisecond measured is orchestration.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import resource
import shutil
import statistics
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parents[1]
_RUNS = _REPO / ".bench_runs" / "refactor_optimization"
_RUNS.mkdir(parents=True, exist_ok=True)
# Nothing here opens a project, but keep the real ~/.molsuite out of reach anyway.
os.environ["HOME"] = str(_RUNS / "home")
(_RUNS / "home").mkdir(exist_ok=True)
logging.disable(logging.CRITICAL)  # worker-death checks make the engine log by design
sys.path.insert(0, str(_REPO / "src"))

from benchmarks.refactor_optimization import workers  # noqa: E402

CPUS = max(2, (os.cpu_count() or 4) - 2)
TERMINAL = {"completed", "failed", "canceled", "cancelled", "completed_with_errors", "interrupted"}


# --- Engine adapter -----------------------------------------------------------------
# The only part that knows the ms_flow API. When the engine is rewritten, this is
# what changes; the measurements below must stay the same to remain comparable.

class _Counter:
    """Counts results as they arrive, so waiting for the job never polls the engine."""

    def __init__(self, step: int):
        self.n, self.step, self.marks = 0, step, []
        self.t0 = time.perf_counter()
        self.done = threading.Event()

    def handle(self, chunk_id, result):
        self.n += 1
        if self.step and self.n % self.step == 0:
            self.marks.append((self.n, time.perf_counter() - self.t0))

    def on_job_terminal(self, status):
        self.done.set()


class Engine:
    def __init__(self, workdir: Path, procs: int = CPUS):
        from ms_flow import engine

        self.workdir = Path(workdir)
        self.eng = engine.Engine(cpus=CPUS)
        try:
            self.eng.attach("project", self.workdir / "executor.db")
        except Exception:
            self.eng.close()
            raise
        self.eng.register("thread", engine.ThreadExecutor(2 * CPUS))
        self.eng.register("process", engine.ProcessExecutor(procs))

    def run(self, executor: str, payloads, fn=workers.noop, inflight: int | None = None,
            marks_every: int = 0, timeout: float = 1800.0, key: str | None = None, options: dict | None = None,
            store=None):
        """Run one job to its end. Returns (seconds, marks, job_snapshot).

        `store(pairs)` gets the results in groups of `(key, result)`, as a job that writes them does.
        """
        payloads = list(payloads)
        counter = _Counter(marks_every)
        counter.t0 = time.perf_counter()
        results = {"on_result": counter.handle} if store is None else {"on_results": store}
        job_id = self.eng.submit(executor, fn, iter(payloads), total=len(payloads), window=inflight, journal="project", key=key,
                                 on_terminal=counter.on_job_terminal, **results, **(options or {}))
        counter.done.wait(timeout)
        seconds = time.perf_counter() - counter.t0
        return seconds, counter.marks, self.eng.status(job_id)

    def state_bytes(self) -> int:
        return sum(f.stat().st_size for f in self.workdir.glob("executor.db*"))

    def close(self):
        self.eng.close()


# --- Measurements -------------------------------------------------------------------

def _workdir(tag: str) -> Path:
    return Path(tempfile.mkdtemp(prefix=f"{tag}_", dir=_RUNS))


def _items(n: int):
    return [{"idx": i} for i in range(n)]


def measure_import(repeats: int = 5) -> list[float]:
    code = "import time; t = time.perf_counter(); import ms_flow.api; print(time.perf_counter() - t)"
    env = dict(os.environ, PYTHONPATH=str(_REPO / "src"))
    return [1000 * float(subprocess.run([sys.executable, "-c", code], env=env, capture_output=True,
                                        text=True, check=True).stdout) for _ in range(repeats)]


def measure_per_chunk(executor: str, n: int, repeats: int = 3) -> list[float]:
    """ms of orchestration per empty chunk, default settings, warm pool."""
    d = _workdir(executor)
    eng = Engine(d)
    try:
        eng.run(executor, _items(64))
        return [1000 * eng.run(executor, _items(n))[0] / n for _ in range(repeats)]
    finally:
        eng.close()
        shutil.rmtree(d, ignore_errors=True)


def measure_write(n: int, repeats: int = 3) -> list[float]:
    """ms per empty chunk of a process job whose results go into a SQLite table as they arrive."""
    d = _workdir("write")
    eng = Engine(d)
    db = sqlite3.connect(d / "results.db", check_same_thread=False)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("CREATE TABLE results (key BLOB)")

    def store(pairs):
        with db:  # one transaction per call
            db.executemany("INSERT INTO results VALUES (?)", [(key,) for key, _ in pairs])

    try:
        eng.run("process", _items(64), store=store)
        out = [1000 * eng.run("process", _items(n), store=store)[0] / n for _ in range(repeats)]
        assert db.execute("SELECT count(*) FROM results").fetchone()[0] == 64 + n * repeats
        return out
    finally:
        eng.close()
        db.close()
        shutil.rmtree(d, ignore_errors=True)


def measure_first_job(repeats: int = 3) -> list[float]:
    """ms from a fresh engine to the end of a one-chunk process job."""
    out = []
    for _ in range(repeats):
        d = _workdir("first")
        eng = Engine(d)
        try:
            out.append(1000 * eng.run("process", _items(1))[0])
        finally:
            eng.close()
            shutil.rmtree(d, ignore_errors=True)
    return out


def measure_scale(n: int) -> dict[str, float]:
    """One large job: cost of the first and last tenth, and the state left on disk."""
    d = _workdir("scale")
    eng = Engine(d)
    try:
        _, marks, _ = eng.run("thread", _items(n), inflight=64, marks_every=n // 10)
    finally:
        eng.close()
    size = sum(f.stat().st_size for f in d.glob("executor.db*"))
    shutil.rmtree(d, ignore_errors=True)
    blocks = [1000 * (t1 - t0) / (n1 - n0) for (n0, t0), (n1, t1) in zip([(0, 0.0)] + marks, marks)]
    return {"scale_first_ms": blocks[0], "scale_last_ms": blocks[-1],
            "scale_growth": blocks[-1] / blocks[0], "state_kb_per_chunk": size / 1024 / n}


# --- Robustness checks --------------------------------------------------------------

def check_worker_death() -> dict[str, bool]:
    """One worker exits hard on one chunk of 40; then a second job runs on the same pool."""
    d = _workdir("death")
    eng = Engine(d, procs=4)
    try:
        _, _, first = eng.run("process", _items(40), fn=workers.die_on_7, timeout=60)
        _, _, second = eng.run("process", _items(20), timeout=60)
        return {"worker_death_isolated": first.chunks_done >= 39 and first.chunks_failed <= 1,
                "worker_death_pool_recovers": second.chunks_done == 20}
    finally:
        eng.close()
        shutil.rmtree(d, ignore_errors=True)


def check_retry_and_limit() -> dict[str, bool]:
    """A chunk that fails twice ends done on its third try; one that hangs is cut at its limit."""
    d = _workdir("retry")
    eng = Engine(d, procs=4)
    try:
        flaky = [{"idx": 0, "path": str(d / "tries"), "fails": 2}]
        _, _, third = eng.run("process", flaky, fn=workers.flaky, timeout=60,
                               options={"retries": 2, "retry_wait": 0.05})
        naps = [{"idx": i, "s": 120 if i == 5 else 0.2} for i in range(12)]
        seconds, _, hung = eng.run("process", naps, fn=workers.nap, timeout=60, options={"timeout": 1.0})
        return {"retry_then_done": (third.status, third.chunks_done, third.chunks_retried) == ("completed", 1, 2),
                "hung_chunk_cut": (hung.chunks_done, hung.chunks_failed) == (11, 1) and seconds < 15}
    finally:
        eng.close()
        shutil.rmtree(d, ignore_errors=True)


_VICTIM_JOB = [{"idx": i, "s": 0.2} for i in range(200)]


def check_kill9() -> dict[str, bool]:
    """kill -9 the process that owns the engine: no worker may outlive it by 5 s, and
    the same job submitted again must run only the chunks that were not done."""
    d = _workdir("kill9")
    victim = subprocess.Popen([sys.executable, "-m", "benchmarks.refactor_optimization.bench", "--victim", str(d)],
                              cwd=_REPO, stdout=subprocess.PIPE, text=True)
    victim.stdout.readline()
    time.sleep(2.5)
    kids = subprocess.run(["pgrep", "-P", str(victim.pid)], capture_output=True, text=True).stdout.split()
    victim.kill()
    victim.wait()
    time.sleep(5.0)
    alive = [k for k in kids if Path(f"/proc/{k}").exists()]
    for k in alive:
        try:
            os.kill(int(k), 9)
        except OSError:
            pass
    resumed = False
    try:
        eng = Engine(d, procs=4)
        try:
            _, marks, job = eng.run("process", _VICTIM_JOB, fn=workers.nap, marks_every=1, key="victim", timeout=120)
            resumed = job.status == "completed" and job.chunks_done == len(_VICTIM_JOB) and 0 < len(marks) < len(_VICTIM_JOB)
        finally:
            eng.close()
    except Exception as exc:
        print(f"  resume after kill -9: {exc!r}", flush=True)
    shutil.rmtree(d, ignore_errors=True)
    return {"kill9_no_orphans": bool(kids) and not alive, "resume_skips_done": resumed}


def _victim(workdir: str) -> None:
    eng = Engine(Path(workdir), procs=4)
    print("started", flush=True)
    eng.run("process", _VICTIM_JOB, fn=workers.nap, key="victim")


def _long_jobs(n: int) -> None:
    print(json.dumps({"thread_long_ms": measure_per_chunk("thread", n, repeats=5),
                      "process_long_ms": measure_per_chunk("process", n, repeats=5),
                      "scale_long_last_ms": [measure_scale(n)["scale_last_ms"] for _ in range(3)],
                      "write_long_ms": measure_write(n)}), flush=True)


def check_second_engine() -> dict[str, bool]:
    """A second engine on the same state file must be refused or must not disturb the first."""
    d = _workdir("two")
    first = Engine(d, procs=2)
    try:
        try:
            second = Engine(d, procs=2)
        except Exception:
            return {"second_engine_safe": True}
        try:
            box = {}
            t = threading.Thread(target=lambda: box.update(job=second.run("thread", _items(200), timeout=30)[2]))
            t.start()
            _, _, job1 = first.run("thread", _items(200), timeout=30)
            t.join()
            return {"second_engine_safe": job1.chunks_done == 200 and box["job"].chunks_done == 200}
        finally:
            second.close()
    finally:
        first.close()
        shutil.rmtree(d, ignore_errors=True)


# --- Record -------------------------------------------------------------------------

def _git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=_REPO, capture_output=True, text=True).stdout.strip()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--label", help="name of this step, e.g. 'baseline' or 'PR #12'")
    ap.add_argument("--quick", action="store_true", help="small sizes, to try the harness; not comparable")
    ap.add_argument("--victim", help=argparse.SUPPRESS)
    ap.add_argument("--long", type=int, help=argparse.SUPPRESS)
    args = ap.parse_args()
    if args.victim:
        return _victim(args.victim)
    if args.long:
        return _long_jobs(args.long)
    if not args.label:
        ap.error("--label is required")

    n, n_scale, n_long = (300, 2000, 5000) if args.quick else (2000, 20000, 200000)
    metrics: dict[str, dict] = {}

    def put(name: str, values, unit: str) -> None:
        values = [round(float(v), 4) for v in values]
        metrics[name] = {"values": values, "unit": unit}
        print(f"  {name:<24} {statistics.median(values):>10.3f} {unit}", flush=True)

    put("import_ms", measure_import(), "ms")
    put("first_job_ms", measure_first_job(), "ms")
    put("thread_ms_per_chunk", measure_per_chunk("thread", n), "ms")
    put("process_ms_per_chunk", measure_per_chunk("process", n), "ms")
    scale = measure_scale(n_scale)
    put("scale_first_ms", [scale["scale_first_ms"]], "ms")
    put("scale_last_ms", [scale["scale_last_ms"]], "ms")
    put("scale_growth", [scale["scale_growth"]], "x")
    put("state_kb_per_chunk", [scale["state_kb_per_chunk"]], "KB")
    put("peak_rss_mb", [resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024], "MB")
    # The jobs above end in tens of ms on the new engine, where one run differs from the next by
    # half. These last seconds, long enough to tell one step from the one before. They run in an
    # interpreter of their own: after the measures above, the same job costs a tenth more.
    long_jobs = subprocess.run([sys.executable, "-m", "benchmarks.refactor_optimization.bench", "--long", str(n_long)],
                               cwd=_REPO, capture_output=True, text=True, check=True).stdout.splitlines()[-1]
    for name, values in json.loads(long_jobs).items():
        put(name, values, "ms")

    checks: dict[str, bool] = {}
    for check in (check_worker_death, check_kill9, check_second_engine, check_retry_and_limit):
        try:
            checks.update(check())
        except Exception as exc:  # a check that cannot even run is a failed check
            checks[check.__name__.removeprefix("check_")] = False
            print(f"  {check.__name__}: {type(exc).__name__}: {exc}", flush=True)
    for name, ok in checks.items():
        print(f"  {name:<28} {'pass' if ok else 'FAIL'}", flush=True)

    record = {
        "label": args.label,
        "recorded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "commit": _git("rev-parse", "--short", "HEAD") + ("+dirty" if _git("status", "--porcelain", "-uno") else ""),
        "machine": {"cpus": os.cpu_count(), "engine_cpus": CPUS, "python": sys.version.split()[0]},
        "sizes": {"chunks_per_job": n, "chunks_scale_job": n_scale, "chunks_long_job": n_long},
        "quick": args.quick,
        "metrics": metrics,
        "checks": checks,
    }
    out = _HERE / "results" / (args.label.replace("/", "-").replace(" ", "_") + ".json")
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(record, indent=2) + "\n")
    print(f"wrote {out.relative_to(_REPO)}")


if __name__ == "__main__":
    main()
