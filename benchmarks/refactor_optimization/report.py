"""What each recorded step changed, as one self-contained HTML page.

    python -m benchmarks.refactor_optimization.report

Reads every `results/*.json` written by `bench.py`, orders them by date and writes
`docs/03-impacto.html`. Nothing here rejects a step: the page
only shows what moved. Every metric is lower-is-better.
The `results/volumes_*.json` files of `bench_volumes.py` are not steps: they fill the "Volumes" section.
"""
from __future__ import annotations

import argparse
import datetime
import json
import statistics
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_DEST = _HERE / "docs/03-impacto.html"
REPO = "https://github.com/MolSuite/ms_flow"

# One line per step, added in the PR that records it: label -> (PR number, what changed, still open).
STEPS = {
    "baseline": (None, "main before the refactor", False),
    "engine core": (None, "New engine next to the old one (ms_flow.engine): state in memory, CPU/GPU/memory tokens, "
                          "event loop, pools that recover. No journal yet, so nothing is written to disk. First job: "
                          "the workers import the whole ms_flow package (pool start alone is 75 ms).", True),
    "lazy import": (None, "The ms_flow package loads its top-level names on first use, so a worker that only runs chunks "
                          "imports the engine (16 ms) and not the whole API (358 ms).", True),
    "feeders": (None, "The source of chunks is read on a thread per job, a window ahead, so a slow generator holds only "
                      "its own job. Every chunk gets a content key (16-byte blake2b) and may carry its own CPU, GPU and "
                      "memory cost. Costs about 0.005 ms per chunk: the key and one more thread sharing the interpreter.",
                True),
    "journal": (None, "Jobs are recorded in executor.db again: raw sqlite3, one writer thread, one transaction every "
                      "50 ms, a 5-column row per finished chunk and no payload. A finished job keeps one row plus its "
                      "failures (the large job peaks at 37 bytes per chunk while it runs). One process owns each "
                      "journal through an operating system lock, so the second engine is refused. The growth figure is "
                      "the first tenth running before the first write, 50 ms in: over a million chunks every tenth "
                      "costs 0.030 to 0.045 ms. Recording a chunk adds about 0.009 ms to it, which only the "
                      "long thread job shows (0.025 to 0.034 ms).", True),
    "resume": (None, "A job has a key in its journal. Submitted again after an interruption or a kill, it skips the "
                     "chunks on record as done, recognised by their content key. Jobs can wait for other jobs "
                     "(after=), and their source is not read until those end.", True),
    "batching": (None, "Chunks measured to take well under 10 ms share a pool task with the ones behind them, up "
                       "to 64 to a task. Each still succeeds, fails, is retried alone after a worker death and is "
                       "journaled on its own. Chunks worth a task of their own are left alone.", True),
    "retries": (None, "A chunk that raises, outlives its time limit or kills its worker is tried again, up to "
                      "the job's retries and waiting longer each time. A chunk past its limit is stopped, and so "
                      "is whatever a canceled job was running; chunks that fall with them start again at no "
                      "cost to their own retries.", True),
    "memory": (None, "A job that continues looks each chunk's key up in the journal file instead of loading "
                     "every done key (232 MB down to 35 MB continuing a job with 1.8 million chunks done). The "
                     "engine lets go of a job when it ends and keeps the last snapshot of the latest 1000.", True),
    "pack target": (None, "Chunks share a task until it lasts 100 ms instead of 10 ms: handing a task out costs "
                          "about 1 ms with every core busy, so CPU-bound chunks of 1 to 5 ms ran 15 to 22 % over "
                          "the ideal and now run 2 to 3 % over. Once the source has ended, what is left is shared "
                          "out among the workers, so a small job does not end on a few long tasks with workers "
                          "idle. The bench chunks are empty and already went 64 to a task: same cost per chunk "
                          "against the previous step run side by side (0.0074 to 0.0086 ms both).", False),
    "resume count": (None, "A job that continues counts what it had done whether or not its source brings it back, "
                           "and no longer warns about it: total = done + pending. Nothing on the path of a chunk "
                           "changes. From this step on the long-job measures are recorded with the rest; for the "
                           "steps before, they were taken on 2026-10-08 from each step's own commit.", False),
    "grouped delivery": (None, "A job that writes its results takes them in groups (on_results): every result of the "
                               "job that is waiting, up to 256, in one call, and all of them count as done once it "
                               "returns, so nothing is on record as done before it is stored. When the call raises, "
                               "each result is offered again alone and only the bad ones fail. Storing a row per "
                               "result costs 0.004 ms per chunk this way against 0.02 to 0.03 ms with one "
                               "transaction per result. Jobs that take results one at a time cost the same as "
                               "before, run side by side with the previous step.", False),
    "forced batch": (None, "A job may set how many chunks go to a task (batch=N: the task waits to have N) and its "
                           "function may take the whole task (batched=True) and return one result per chunk. "
                           "Nothing changes for a job that sets neither: the long measures stay where they were. "
                           "This step and the three after it were recorded on 2026-10-09, each from its own "
                           "commit.", False),
    "task api": (None, "Jobs are written with @task and @job and submitted through Flow: the source gives the "
                       "elements, the task runs on one, the results go to a table or graph sink in one "
                       "transaction per group. New measure, api_long_ms: a job written this way that stores a row "
                       "per element costs 0.021 ms per element, against 0.017 ms for the engine alone writing "
                       "with sqlite3 by hand (write_long_ms).", False),
    "table sources": (None, "A source that reads a table (ms_flow.query.Rows) gives its job the total and the table "
                            "it reads, so the job waits for whoever is writing it. A job reports how far its "
                            "running chunks are, and a stopped job records the results its callback still took. "
                            "Same cost per chunk.", False),
    "new engine only": (None, "The old engine is gone: tasking, the handler, the executor adapters and their tables "
                              "in executor.db. The facade submits on the new engine (ms.submit) and a "
                              "monitor reads ms.jobs() and ms.job_failures(). Importing the package costs 300 ms "
                              "instead of 365 ms; a chunk costs the same.", True),
    "remote executor": (None, "An executor may end the chunks of a task one by one, be told what the job needs and "
                              "say how many chunks it wants in a task. On that: RayExecutor, ms_flow.runner (a "
                              "staged directory run to its end, continued when run again) and RemoteExecutor (a "
                              "task is a job in the queue of another machine, polled with one command). The local "
                              "path of a chunk gains one check per finished task. This record was taken with the "
                              "machine in use; run side by side with the previous step, alternating four times, "
                              "import is 294 against 291 ms, the first job 83 against 84 ms and a process chunk "
                              "0.0130 against 0.0132 ms.", False),
}

# What the design aims for once the journal is in (02-diseno-motor.md); without it, 0.05 ms per chunk.
TARGETS = {"thread_ms_per_chunk": 0.2, "process_ms_per_chunk": 0.2, "scale_last_ms": 0.2, "scale_first_ms": 0.2,
           "thread_long_ms": 0.2, "process_long_ms": 0.2, "scale_long_last_ms": 0.2, "write_long_ms": 0.2,
           "api_long_ms": 0.2,
           "scale_growth": 1.1, "state_kb_per_chunk": 0.3}

# key, short name, chart title, how it is measured. The first CHARTED get a chart panel.
METRICS = [
    ("thread_ms_per_chunk", "thread", "Thread executor, cost per chunk (ms)",
     "Warm engine, one job of empty chunks on the thread executor with default settings; wall time ÷ chunks. Median of 3."),
    ("process_ms_per_chunk", "process", "Process executor, cost per chunk (ms)",
     "The same job on the loky process pool, already started. Median of 3."),
    ("scale_last_ms", "large job", "Late in a large job, cost per chunk (ms)",
     "One large thread job with 64 chunks in flight; cost per chunk over its last tenth."),
    ("state_kb_per_chunk", "state", "State on disk per chunk (KB)",
     "Size of executor.db after the large job ÷ its chunks."),
    ("thread_long_ms", "thread, long", "Thread executor, long job, cost per chunk (ms)",
     "The thread job with 200,000 chunks instead of 2,000: it lasts seconds, so one run differs from the next by "
     "about 10 % where the short one differs by half. Median of 5. This is the one to compare engine steps by."),
    ("process_long_ms", "process, long", "Process executor, long job, cost per chunk (ms)",
     "The process job with 200,000 chunks. Median of 5."),
    ("scale_long_last_ms", "large job, long", "Late in a 200,000-chunk job, cost per chunk (ms)",
     "The large thread job with 64 chunks in flight, ten times longer; cost per chunk over its last tenth. Median of 3."),
    ("write_long_ms", "process, writing", "Process executor, results stored as they arrive, cost per chunk (ms)",
     "The long process job with every result inserted in a SQLite table from the delivery thread, one transaction "
     "per call of the job's callback. Median of 3. What it costs over the long process job is the writing."),
    ("api_long_ms", "task and job, writing", "A job written with @task and @job, cost per element (ms)",
     "The long process job as a package writes it: a task that returns one row, a source that gives the elements "
     "and a table_sink into a project table. Median of 3. What it costs over the one above is the project store."),
    ("first_job_ms", "first job", "First job on a fresh engine (ms)",
     "New engine, one-chunk job on the process executor, pool start-up included. Median of 3."),
    ("import_ms", "import", "Import time (ms)",
     "`import ms_flow.api` in a new interpreter. Median of 5."),
    ("scale_first_ms", "large job, start", "Early in a large job, cost per chunk (ms)",
     "The same large job; cost per chunk over its first tenth."),
    ("scale_growth", "growth", "Growth inside a large job (×)",
     "Last tenth ÷ first tenth of the large job. 1 means the cost does not depend on the job size."),
    ("peak_rss_mb", "peak RSS", "Peak memory of the benchmark process (MB)",
     "ru_maxrss of the main process at the end of the run; workers not included."),
]
CHARTED = 11

# key, name, what it does, task that is meant to make it pass.
CHECKS = [
    ("worker_death_isolated", "Worker death stays isolated",
     "One of 40 chunks kills its worker process; at most that chunk may fail.", "F0-1"),
    ("worker_death_pool_recovers", "Pool recovers after a worker death",
     "The next job on the same executor finishes 20 of 20.", "F0-1"),
    ("kill9_no_orphans", "No orphans after kill -9",
     "The main process is killed mid-job; no worker is alive 5 s later.", "F1-4"),
    ("resume_skips_done", "A killed job continues where it stopped",
     "The job killed above is submitted again under its key: it ends complete and runs only the chunks "
     "that were not on record.", "F2-7"),
    ("second_engine_safe", "Second engine on the same file is safe",
     "A second engine on the same executor.db is refused, or both jobs finish complete.", "F1-3"),
    ("retry_then_done", "A chunk that fails is tried again",
     "A chunk fails twice and works the third time: it counts as done, with two retries on record.", "F1-1"),
    ("hung_chunk_cut", "A chunk that hangs is cut",
     "One of 12 chunks sleeps for two minutes under a limit of 1 s: it alone fails and the job ends.", "F1-2"),
]

NOISE_FLOOR_PTS = 3.0


def classify(delta_pts: float, noise_pts: float) -> str:
    if abs(delta_pts) <= noise_pts:
        return "noise"
    return "improves" if delta_pts < 0 else "worsens"


assert [classify(-10, 3), classify(2, 3), classify(5, 3)] == ["improves", "noise", "worsens"]


def summarize(records: list[dict]) -> list[dict]:
    """Per step and metric: median, range, % against the baseline and the step from the previous column.

    A step counts as a change only beyond the run-to-run range of both columns (or 3 points).
    A metric first measured in a later step takes that step as its baseline.
    """
    base: dict[str, float] = {}
    prev: dict[str, tuple[float, float]] = {}
    steps = []
    for rec in records:
        pr, title, is_open = STEPS.get(rec["label"], (None, "", False))
        cells = {}
        for key, m in rec["metrics"].items():
            med = statistics.median(m["values"])
            b = base.setdefault(key, med)
            cum = 100 * (med / b - 1)
            spread = 100 * (max(m["values"]) - min(m["values"])) / b
            cell = {"med": med, "lo": min(m["values"]), "hi": max(m["values"]), "unit": m["unit"], "cum": cum}
            if key in prev:
                cell["step"] = cum - prev[key][0]
                cell["kind"] = classify(cell["step"], max(NOISE_FLOOR_PTS, spread, prev[key][1]))
            prev[key] = (cum, spread)
            cells[key] = cell
        steps.append({"label": rec["label"], "pr": pr, "title": title, "open": is_open, "commit": rec["commit"],
                      "date": rec["recorded_at"][:10], "m": cells, "checks": rec.get("checks", {})})
    return steps


VOL_BASE = "2026-10-15"  # the hand-run the volumes plan (08) must reproduce
VOL_MARGIN_PCT = 10.0  # "Hecho cuando" of V0
VOL_COLS = (("import_s", "import s"), ("durable_s", "import + sync s"), ("size_mb", "size MB"), ("files", "files"), ("get_mol_p50_us", "get_mol p50 µs"),
            ("get_mol_p99_us", "get_mol p99 µs"), ("stream_all_current_s", "stream all s"),
            ("stream_all_current_s_cold", "stream all s, cold"), ("random_s", "random read s"),
            ("random_s_cold", "random read s, cold"), ("update_5000_s", "update 5000 s"))
VOL_DRIFT = (("import_s", "import s"), ("stream_all_current_s", "stream all s"))


def _vol_cell(v: dict, col: str):
    if col.startswith("random"):  # parallel_<w>x<k>_random_s[_cold]: the name carries the worker count
        return next((v[k] for k in sorted(v) if k.startswith("parallel_") and k.endswith(col[len("random"):])), None)
    return v.get(col)


def volumes_view(records: list[dict]) -> dict:
    """One table per mode (a row per run and variant, oldest first) and the drift of the newest run against VOL_BASE."""
    records = sorted(records, key=lambda r: r["recorded_at"])
    modes: dict[str, list] = {}
    for rec in records:
        for key, v in sorted(rec["variants"].items()):
            mode, name = key.split("/", 1)
            info = [rec["label"], f"commit {rec.get('commit') or 'unknown'}", f"n={v.get('n')}"]
            if v.get("workers"):
                info.append(f"{v['workers']} workers")
            info += [x for x in ((rec.get("disk") or {}).get("model"), rec.get("note")) if x]
            modes.setdefault(mode, []).append(
                {"label": rec["label"], "variant": name, "n": v.get("n"), "info": " · ".join(info),
                 "cells": {c: _vol_cell(v, c) for c, _ in VOL_COLS}})
    out = {}
    for mode in sorted(modes, key=lambda m: m != "serial"):
        cols = [c for c in VOL_COLS if mode == "serial" or c[0] != "update_5000_s"]
        out[mode] = {"cols": cols, "rows": modes[mode]}
    ref = next((r for r in records if r["label"] == VOL_BASE), None)
    new = next((r for r in reversed(records) if r["label"] != VOL_BASE), None)
    drift = None
    if ref and new:
        drift = {"base": VOL_BASE, "label": new["label"], "margin": VOL_MARGIN_PCT, "cols": VOL_DRIFT, "rows": []}
        for key in sorted(ref["variants"].keys() & new["variants"].keys()):
            row = {"variant": key}
            for m, _ in VOL_DRIFT:
                a, b = ref["variants"][key].get(m), new["variants"][key].get(m)
                if a and b is not None:
                    pct = 100 * (b / a - 1)
                    row[m] = {"was": a, "now": b, "pct": round(pct, 1), "flag": abs(pct) > VOL_MARGIN_PCT}
            drift["rows"].append(row)
    return {"modes": out, "base": VOL_BASE, "drift": drift, "fsync": _fsync_cost(records), "job": _job_cost(records, ref),
            "reads": _read_cost(records)}


def _fsync_cost(records: list[dict]) -> list[dict]:
    """V1-9: per run with PS and PSN, what the group fsync adds to the import, and PS against HT of the same run."""
    out = []
    for rec in records:
        v = {k: rec["variants"].get(f"serial/{k}", {}).get("import_s") for k in ("PS", "PSN", "HT")}
        d = {k: rec["variants"].get(f"serial/{k}", {}).get("durable_s") for k in ("PS", "PSN", "HT")}
        if v["PS"] and v["PSN"]:
            pct = lambda a, b: round(100 * (a / b - 1), 1) if a and b else None
            # PSN without a sync ends partly in the page cache: on disk time when the run has it
            fsync = pct(d["PS"], d["PSN"]) if d["PS"] and d["PSN"] else pct(v["PS"], v["PSN"])
            out.append({"label": rec["label"], **v, "fsync_pct": fsync, "vs_ht_pct": pct(v["PS"], v["HT"]),
                        "vs_ht_durable_pct": pct(d["PS"], d["HT"]), "flag": fsync > VOL_MARGIN_PCT})
    return out


def _job_cost(records: list[dict], ref: dict | None) -> list[dict]:
    """V2-12: per run with PJ, the import through a real job against VP of VOL_BASE (the criterion) and of the same run."""
    pct = lambda a, b: round(100 * (a / b - 1), 1) if a and b else None
    base = ((ref or {}).get("variants") or {}).get("parallel/VP", {}).get("import_s")
    out = []
    for rec in records:
        pj, vp = (rec["variants"].get(f"parallel/{k}", {}) for k in ("PJ", "VP"))
        if pj.get("import_s"):
            vs_base = pct(pj["import_s"], base)
            out.append({"label": rec["label"], "PJ": pj["import_s"], "PJ_durable": pj.get("durable_s"), "VP_base": base,
                        "vs_base_pct": vs_base, "VP": vp.get("import_s"), "vs_vp_pct": pct(pj["import_s"], vp.get("import_s")),
                        "vs_vp_durable_pct": pct(pj.get("durable_s"), vp.get("durable_s")),
                        "flag": vs_base is not None and vs_base > VOL_MARGIN_PCT})
    return out


def _read_cost(records: list[dict]) -> list[dict]:
    """V3: per run with PJ, reading every current payload cold as a job's source and as an export, against VP's reader."""
    pct = lambda a, b: round(100 * (a / b - 1), 1) if a and b else None
    out = []
    for rec in records:
        pj, vp = (rec["variants"].get(f"parallel/{k}", {}) for k in ("PJ", "VP"))
        if pj.get("stream_all_current_s_cold"):
            src, exp, base = pj["stream_all_current_s_cold"], pj.get("export_all_current_s_cold"), vp.get("stream_all_current_s_cold")
            out.append({"label": rec["label"], "n": pj.get("n"), "source": src, "export": exp, "VP": base,
                        "source_vs_vp_pct": pct(src, base), "export_vs_vp_pct": pct(exp, base),
                        "disk_mb": pj.get("stream_all_current_cold_disk_mb")})
    return out


def _self_check() -> None:
    rec = lambda label, v: {"label": label, "commit": "x", "recorded_at": "2026-01-01", "metrics": {"a": {"values": v, "unit": "ms"}}}
    a = [s["m"]["a"] for s in summarize([rec("0", [10.0]), rec("1", [5.0]), rec("2", [5.1]), rec("3", [8.0])])]
    assert [c.get("kind") for c in a] == [None, "improves", "noise", "worsens"]
    assert round(a[1]["cum"]) == -50 and round(a[3]["step"]) == 29

    vrec = lambda label, at, variants: {"label": label, "recorded_at": at, "commit": None, "variants": variants}
    old = {"serial/HT": {"n": 9, "import_s": 100.0, "stream_all_current_s": 40.0},
           "parallel/VP": {"n": 9, "workers": 14, "parallel_14x20k_random_s": 0.6}}
    new = {"serial/HT": {"n": 9, "import_s": 120.0, "stream_all_current_s": 41.0, "update_5000_s": 0.3, "parallel_8x10k_random_s": 0.5,
                         "parallel_8x10k_random_s_cold": 0.9, "parallel_8x10k_random_cold_disk_mb": 5.0},
           "serial/PS": {"n": 9, "import_s": 1.0}}
    v = volumes_view([vrec("v1", "2026-11-01", new), vrec(VOL_BASE, "2026-10-15", old)])
    assert list(v["modes"]) == ["serial", "parallel"]
    assert [(r["label"], r["variant"]) for r in v["modes"]["serial"]["rows"]] == [(VOL_BASE, "HT"), ("v1", "HT"), ("v1", "PS")]
    assert "update_5000_s" not in [c for c, _ in v["modes"]["parallel"]["cols"]]
    cells = v["modes"]["serial"]["rows"][1]["cells"]
    assert (cells["random_s"], cells["random_s_cold"], cells["update_5000_s"]) == (0.5, 0.9, 0.3)
    assert v["modes"]["parallel"]["rows"][0]["cells"]["random_s"] == 0.6
    d = v["drift"]
    assert d["label"] == "v1" and [r["variant"] for r in d["rows"]] == ["serial/HT"]
    assert d["rows"][0]["import_s"] == {"was": 100.0, "now": 120.0, "pct": 20.0, "flag": True}
    assert d["rows"][0]["stream_all_current_s"]["pct"] == 2.5 and not d["rows"][0]["stream_all_current_s"]["flag"]
    assert volumes_view([vrec(VOL_BASE, "2026-10-15", old)])["drift"] is None
    (cost,) = volumes_view([vrec("v1", "2026-11-01", {**new, "serial/PSN": {"n": 9, "import_s": 0.8}})])["fsync"]
    assert (cost["fsync_pct"], cost["vs_ht_pct"], cost["vs_ht_durable_pct"], cost["flag"]) == (25.0, -99.2, None, True)
    durable = {**new, "serial/PS": {"import_s": 1.0, "durable_s": 1.05}, "serial/PSN": {"import_s": 0.8, "durable_s": 1.0},
               "serial/HT": {**new["serial/HT"], "durable_s": 0.5}}
    (cost,) = volumes_view([vrec("v1", "2026-11-01", durable)])["fsync"]
    assert (cost["fsync_pct"], cost["vs_ht_pct"], cost["vs_ht_durable_pct"], cost["flag"]) == (5.0, -99.2, 110.0, False)
    jobbed = {"parallel/PJ": {"import_s": 150.0, "durable_s": 160.0}, "parallel/VP": {"import_s": 125.0, "durable_s": 160.0}}
    (cost,) = volumes_view([vrec("v2", "2026-11-02", jobbed), vrec(VOL_BASE, "2026-10-15", {"parallel/VP": {"import_s": 120.0}})])["job"]
    assert (cost["vs_base_pct"], cost["vs_vp_pct"], cost["vs_vp_durable_pct"], cost["flag"]) == (25.0, 20.0, 0.0, True)
    read = {"parallel/PJ": {"stream_all_current_s_cold": 12.0, "export_all_current_s_cold": 9.0},
            "parallel/VP": {"stream_all_current_s_cold": 10.0}}
    (cost,) = volumes_view([vrec("v3", "2026-11-03", read)])["reads"]
    assert (cost["source_vs_vp_pct"], cost["export_vs_vp_pct"]) == (20.0, -10.0)


_self_check()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--results", type=Path, default=_HERE / "results")
    ap.add_argument("--out", type=Path, default=_DEST)
    args = ap.parse_args()

    paths = [p for p in args.results.glob("*.json") if not p.name.startswith("volumes_")]
    records = sorted((json.loads(p.read_text()) for p in paths), key=lambda r: r["recorded_at"])
    quick = [r["label"] for r in records if r.get("quick")]
    if quick:
        print(f"skipped --quick runs, not comparable: {', '.join(quick)}")
    records = [r for r in records if not r.get("quick")]
    if not records:
        raise SystemExit(f"no results in {args.results}")

    volumes = [json.loads(p.read_text()) for p in sorted(args.results.glob("volumes_*.json"))]
    data = {
        "date": datetime.datetime.now(datetime.timezone.utc).date().isoformat(), "repo": REPO,
        "machine": records[0]["machine"], "sizes": {**records[0]["sizes"], **records[-1]["sizes"]},
        "steps": summarize(records),
        "metrics": [dict(zip(("key", "short", "title", "what"), m)) for m in METRICS],
        "charted": CHARTED,
        "targets": TARGETS,
        "checks": [dict(zip(("key", "title", "what", "task"), c)) for c in CHECKS],
        "volumes": volumes_view(volumes) if volumes else None,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(PAGE.replace("/*DATA*/", json.dumps(data, separators=(",", ":")).replace("</", "<\\/")))
    print(args.out, f"{args.out.stat().st_size / 1024:.0f} kB,", len(records), "steps")


PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>ms_flow · what each step changed in the engine</title>
<style>
.viz-root {
  color-scheme: light;
  --surface-0: #f4f3f0; --surface-1: #fcfcfb; --border: #e2e1dc; --grid: #ecebe7;
  --text-primary: #0b0b0b; --text-secondary: #52514e; --text-muted: #7a7974;
  --series-1: #2a78d6; --ref: #7a7974; --good: #0a8a0a; --bad: #d03b3b;
}
@media (prefers-color-scheme: dark) {
  :root:where(:not([data-theme="light"])) .viz-root {
    color-scheme: dark;
    --surface-0: #111110; --surface-1: #1a1a19; --border: #33332f; --grid: #2a2a27;
    --text-primary: #ffffff; --text-secondary: #c3c2b7; --text-muted: #8f8e86;
    --series-1: #3987e5; --ref: #8f8e86; --good: #3fbf3f; --bad: #e66767;
  }
}
:root[data-theme="dark"] .viz-root {
  color-scheme: dark;
  --surface-0: #111110; --surface-1: #1a1a19; --border: #33332f; --grid: #2a2a27;
  --text-primary: #ffffff; --text-secondary: #c3c2b7; --text-muted: #8f8e86;
  --series-1: #3987e5; --ref: #8f8e86; --good: #3fbf3f; --bad: #e66767;
}
body { margin: 0; }
.viz-root { background: var(--surface-0); color: var(--text-primary);
  font: 14px/1.45 system-ui, sans-serif; padding: 24px; min-height: 100vh; box-sizing: border-box; }
h1 { font-size: 20px; margin: 0 0 4px; }
h2 { font-size: 15px; margin: 0 0 2px; }
h3 { font-size: 13px; margin: 14px 0 4px; }
a { color: var(--series-1); }
.sub { color: var(--text-secondary); margin: 0 0 16px; max-width: 1100px; }
.card { background: var(--surface-1); border: 1px solid var(--border); border-radius: 8px;
  padding: 16px; margin-bottom: 16px; max-width: 1100px; }
.tiles { display: flex; gap: 12px; flex-wrap: wrap; max-width: 1134px; margin-bottom: 16px; }
.tile { background: var(--surface-1); border: 1px solid var(--border); border-radius: 8px; padding: 12px 16px; flex: 1 1 180px; }
.tile .v { font-size: 28px; font-weight: 600; }
.tile .l { color: var(--text-secondary); font-size: 13px; }
.legend { display: flex; gap: 16px; flex-wrap: wrap; color: var(--text-secondary); font-size: 13px; margin: 6px 0 8px; }
.legend i { display: inline-block; width: 12px; height: 12px; border-radius: 3px; margin-right: 6px; vertical-align: -1px; }
svg text { fill: var(--text-secondary); font-size: 11px; paint-order: stroke; stroke: var(--surface-1); stroke-width: 4px; stroke-linejoin: round; }
svg .t { fill: var(--text-primary); font-size: 12px; font-weight: 600; }
svg .h { fill: transparent; }
svg .h:hover { fill: var(--text-primary); fill-opacity: .05; }
.note { color: var(--text-secondary); font-size: 13px; margin: 8px 0 0; max-width: 1000px; }
.scroll { overflow-x: auto; }
table { border-collapse: collapse; font-size: 13px; font-variant-numeric: tabular-nums; }
th, td { padding: 4px 8px; border-bottom: 1px solid var(--border); text-align: right; white-space: nowrap; }
th:first-child, td:first-child, td.l, th.l { text-align: left; }
th { color: var(--text-secondary); font-weight: 500; }
td.b, th.b { border-left: 1px solid var(--border); }
.good { color: var(--good); font-weight: 600; } .bad { color: var(--bad); font-weight: 600; } .muted { color: var(--text-muted); } td.w { white-space: normal; }
#tip { position: fixed; pointer-events: none; background: var(--surface-1); color: var(--text-primary);
  border: 1px solid var(--border); border-radius: 6px; padding: 6px 8px; font-size: 12px; display: none;
  box-shadow: 0 2px 8px rgba(0,0,0,.15); max-width: 460px; }
.toggle { float: right; background: none; border: 1px solid var(--border); color: var(--text-secondary);
  border-radius: 6px; padding: 4px 10px; cursor: pointer; }
</style>
</head>
<body>
<div class="viz-root">
<button class="toggle" onclick="const r=document.documentElement;r.dataset.theme=(r.dataset.theme==='dark'||(!r.dataset.theme&&matchMedia('(prefers-color-scheme: dark)').matches))?'light':'dark'">light / dark</button>
<h1>What did each step change in the engine?</h1>
<p class="sub" id="sub"></p>

<div class="tiles" id="tiles"></div>

<div class="card">
  <h2>Which step moved what</h2>
  <p class="sub">Step each change added, in points of the baseline value: −13 pts means the metric dropped by 13 % of what it was on the
    baseline. Everything is a cost, so down is better. An empty cell: the step did not measure that metric.</p>
  <div class="scroll" id="steptable"></div>
  <p class="note">A step inside the run-to-run range of the two columns it compares, or under 3 points, is noise on this machine and is shown as ≈.
    Nothing here blocks a change; the page only shows its effect.</p>
</div>

<div class="card">
  <h2>Engine cost after each step</h2>
  <p class="sub">Left to right: the baseline, then each step in the order it was recorded; every column includes the ones before it.
    Above each bar: the value, how many times lower than the baseline, and the step from the previous column.</p>
  <div class="legend"><span><i style="background:var(--ref)"></i>baseline</span>
    <span><i style="background:var(--series-1)"></i>after the step</span>
    <span><i style="background:var(--series-1);opacity:.55"></i>PR still open</span>
    <span><i style="background:none;border-top:2px dashed var(--ref);border-radius:0;height:0;vertical-align:3px"></i>baseline level</span>
    <span><b class="good">▼</b> step lowers the cost</span><span><b class="bad">▲</b> step raises it</span><span>≈ within noise</span></div>
  <div class="scroll" id="chart"></div>
</div>

<div class="card">
  <h2>Robustness checks</h2>
  <p class="sub">Each check runs a failure against the real engine on a fresh state file and looks at what is left.</p>
  <div class="scroll" id="checks"></div>
</div>

<div class="card">
  <h2>What needs a look</h2>
  <p class="sub">Every check that fails on the last step, and every step that raised a cost beyond noise.</p>
  <div class="scroll" id="findings"></div>
</div>

<div class="card">
  <h2>Table</h2>
  <p class="sub">Median of the repeats; the range is in the tooltip of each cell. Green: the value meets the design target of the last row.</p>
  <div class="scroll" id="table"></div>
</div>

<div class="card" id="volumes-card" hidden>
  <h2>Volumes</h2>
  <p class="sub"><code>bench_volumes.py</code> stores molecule payloads two ways and times the same reads on both: SQLite blobs
    (<b>RT</b>) against append-only volumes (serial: <b>HT</b> no fsync, <b>H</b> fsync per shard; parallel: <b>VP</b> the parent
    writes, <b>VC</b> one segment per chunk, <b>PJ</b> a real ms_flow job whose rows carry <code>Payload</code>s; <b>PS</b> the real <code>PayloadStore</code> with one fsync per commit, <b>PSN</b> the same
    without it). <b>Warm</b> reads run right after the import, with the data in the page cache;
    <b>cold</b> ones run after <code>fsync</code> and <code>posix_fadvise(DONTNEED)</code> on every file, so they come from the disk.
    A blank cell was not measured (the 2026-10-15 run has no cold reads). Plan: <a href="08-plan-volumenes.md">08-plan-volumenes.md</a>.</p>
  <div class="scroll" id="volumes"></div>
  <div class="scroll" id="drift"></div>
  <div class="scroll" id="fsync"></div>
  <div class="scroll" id="job"></div>
  <div class="scroll" id="reads"></div>
</div>

<div class="card">
  <h2>How this was measured</h2>
  <p class="note"><b>Workload.</b> Empty tasks, so what is timed is the orchestration alone: with real tasks the same cost is added on top
    of the task time. Every run starts from a new state file and a temporary HOME.</p>
  <div id="how"></div>
  <p class="note"><b>Reading it.</b> The numbers are from one machine and move a few percent between runs; compare columns of this page
    with each other, not with another machine.</p>
  <p class="note">Record a step: <code>python -m benchmarks.refactor_optimization.bench --label &lt;step&gt;</code>, add its line to
    <code>STEPS</code> in <code>report.py</code>, then <code>python -m benchmarks.refactor_optimization.report</code>.</p>
</div>
<div id="tip"></div>
</div>

<script>
// ponytail: data pasted in by benchmarks/refactor_optimization/report.py; rerun it instead of editing here
const D = /*DATA*/;
const NS = 'http://www.w3.org/2000/svg';
const el = (p, n, a = {}, t) => { const e = document.createElementNS(NS, n); for (const k in a) e.setAttribute(k, a[k]); if (t != null) e.textContent = t; p.appendChild(e); return e; };
const esc = s => String(s).replace(/[&<>]/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;'}[c]));
const tip = document.getElementById('tip');
function hover(e, html) {
  e.addEventListener('mousemove', ev => { tip.innerHTML = html; tip.style.display = 'block';
    tip.style.left = Math.min(ev.clientX + 14, innerWidth - tip.offsetWidth - 8) + 'px';
    tip.style.top = Math.min(ev.clientY + 14, innerHeight - tip.offsetHeight - 8) + 'px'; });
  e.addEventListener('mouseleave', () => tip.style.display = 'none');
}
const num = v => v >= 100 ? v.toFixed(0) : v >= 10 ? v.toFixed(1) : v >= 1 ? v.toFixed(2) : v.toPrecision(2);
const val = c => `${num(c.med)} ${c.unit}`;
const times = c => c.cum < -3 && c.med > 0 ? `${(100 / (100 + c.cum)).toFixed(1)}× lower` : c.cum > 3 ? `${(1 + c.cum / 100).toFixed(1)}× higher` : 'same';
const stepText = c => c.kind === 'noise' ? '≈' : `${c.step < 0 ? '▼ −' : '▲ +'}${Math.abs(c.step).toFixed(0)} pts`;
const stepClass = c => ({improves: 'good', worsens: 'bad', noise: 'muted'})[c.kind];
const stepHtml = c => c?.kind ? `<span class="${stepClass(c)}">${stepText(c)}</span>` : '';
const prHtml = s => s.pr ? `<a href="${D.repo}/pull/${s.pr}">#${s.pr}</a>${s.open ? ' <span class="muted">open</span>' : ''}` : '';
const passed = s => Object.values(s.checks).filter(Boolean).length;
const charted = D.metrics.slice(0, D.charted), last = D.steps.at(-1);

function chart(host) {
  const n = D.steps.length, L = 14, W = Math.max(1000, L + 92 * n + 6), cw = Math.min(150, (W - L - 6) / n), P = 132;
  const cx = i => L + cw * (i + .5), H = 20 + charted.length * P + 40;
  const svg = el(host, 'svg', {width: '100%', viewBox: `0 0 ${W} ${H}`, style: `min-width:${W > 1000 ? W * .85 : 0}px`, role: 'img',
    'aria-label': 'Engine cost after each step, one panel per metric'});
  charted.forEach((m, p) => {
    const y0 = 20 + p * P, floor = y0 + P - 14, h = P - 62, cells = D.steps.map(s => s.m[m.key]);
    const top = Math.max(...cells.filter(Boolean).map(c => c.med)), by = v => floor - v / top * h;
    el(svg, 'text', {x: L, y: y0 + 10, class: 't'}, m.title);
    el(svg, 'line', {x1: L, x2: W - 6, y1: floor, y2: floor, stroke: 'var(--ref)'});
    const first = cells.findIndex(Boolean);
    if (first < 0) return;
    el(svg, 'line', {x1: cx(first), x2: L + cw * n, y1: by(cells[first].med), y2: by(cells[first].med), stroke: 'var(--ref)', 'stroke-dasharray': '4 3'});
    cells.forEach((c, i) => {
      if (!c) return;
      const bw = Math.min(46, cw * .5), y = by(c.med);
      el(svg, 'rect', {x: cx(i) - bw / 2, y, width: bw, height: Math.max(1, floor - y), rx: 2,
        fill: i === first ? 'var(--ref)' : 'var(--series-1)', opacity: D.steps[i].open ? .55 : 1});
      el(svg, 'text', {x: cx(i), y: y - (c.kind ? 18 : 5), 'text-anchor': 'middle', class: 't'},
        num(c.med) + (c.cum < -3 && c.med > 0 ? ` · ${(100 / (100 + c.cum)).toFixed(1)}×` : ''));
      if (c.kind) el(svg, 'text', {x: cx(i), y: y - 5, 'text-anchor': 'middle', style: `fill:var(--${c.kind === 'noise' ? 'text-muted' : stepClass(c)})`}, stepText(c));
    });
  });
  D.steps.forEach((s, i) => {
    el(svg, 'text', {x: cx(i), y: H - 22, 'text-anchor': 'middle', class: 't'}, s.label);
    el(svg, 'text', {x: cx(i), y: H - 8, 'text-anchor': 'middle'}, s.pr ? `#${s.pr} ${s.open ? 'open' : 'merged'}` : s.commit);
    hover(el(svg, 'rect', {x: L + cw * i, y: 4, width: cw, height: H - 4, class: 'h'}),
      `<b>${esc(s.label)}</b>${s.pr ? ` · PR #${s.pr}${s.open ? ' open' : ''}` : ''} · ${esc(s.commit)} · ${s.date}<br>${esc(s.title)}<br>`
      + charted.filter(m => s.m[m.key]).map(m => { const c = s.m[m.key];
        return `${m.short}: <b>${val(c)}</b>${i ? ` · ${times(c)}${c.kind ? ` · step ${stepText(c)}` : ''}` : ''}`; }).join('<br>')
      + `<br>checks: ${passed(s)} of ${Object.keys(s.checks).length} pass`);
  });
}

function stepTable(host) {
  const rows = D.steps.slice(1);
  if (!rows.length) { host.innerHTML = '<p class="note">No step recorded yet: the only column is the baseline.</p>'; return; }
  host.innerHTML = '<table><tr><th>Step</th><th>PR</th><th class="l">Change</th>' + charted.map((m, i) => `<th${i ? '' : ' class="b"'}>${m.short}</th>`).join('')
    + '<th class="b">checks</th></tr>' + rows.map((s, i) => { const gained = passed(s) - passed(D.steps[i]);
      return `<tr><td>${esc(s.label)}</td><td>${prHtml(s)}</td><td class="l w">${esc(s.title)}</td>`
        + charted.map((m, j) => `<td${j ? '' : ' class="b"'}>${stepHtml(s.m[m.key])}</td>`).join('')
        + `<td class="b">${gained > 0 ? `<span class="good">+${gained}</span>` : gained < 0 ? `<span class="bad">−${-gained}</span>` : '<span class="muted">≈</span>'}`
        + ` <span class="muted">${passed(s)}/${Object.keys(s.checks).length}</span></td></tr>`; }).join('') + '</table>';
}

function checks(host) {
  host.innerHTML = '<table><tr><th class="l">Check</th><th class="l">What it does</th>' + D.steps.map(s => `<th>${esc(s.label)}</th>`).join('') + '</tr>'
    + D.checks.map(c => `<tr><td>${c.title}</td><td class="l w muted" style="min-width:300px">${c.what}</td>`
      + D.steps.map(s => { const v = s.checks[c.key];
        return `<td>${v == null ? '' : v ? '<span class="good">✔ pass</span>' : '<span class="bad">✖ fail</span>'}</td>`; }).join('') + '</tr>').join('')
    + '</table>';
}

function findings(host) {
  const rows = D.checks.filter(c => last.checks[c.key] === false)
    .map(c => [c.title, `<span class="bad">✖ fails</span> on ${esc(last.label)}`, `${c.what} Planned fix: task ${c.task}.`]);
  D.steps.forEach((s, i) => D.metrics.forEach(m => { const c = s.m[m.key];
    if (c?.kind === 'worsens') rows.push([m.title, `${stepHtml(c)} at ${esc(s.label)}`, `${val(D.steps[i - 1].m[m.key])} → ${val(c)}. ${esc(s.title)}`]); }));
  host.innerHTML = rows.length ? '<table><tr><th class="l">What</th><th class="l">Where</th><th class="l">What is known</th></tr>'
    + rows.map(r => `<tr><td>${r[0]}</td><td class="l">${r[1]}</td><td class="l w" style="min-width:380px">${r[2]}</td></tr>`).join('') + '</table>'
    : '<p class="note">Nothing: every check passes and no step raised a cost beyond noise.</p>';
}

function volumes(host, drift) {
  const f = v => v == null ? '' : Number.isInteger(v) ? v.toLocaleString('en') : num(v);
  host.innerHTML = Object.entries(D.volumes.modes).map(([mode, t]) => `<h3>${esc(mode)}</h3><table><tr><th class="l">Run</th><th class="l">Variant</th><th>n</th>`
    + t.cols.map(([, title], i) => `<th${i ? '' : ' class="b"'}>${title}</th>`).join('') + '</tr>'
    + t.rows.map(r => `<tr><td class="l" title="${esc(r.info)}">${esc(r.label)}</td><td class="l">${esc(r.variant)}</td><td>${f(r.n)}</td>`
      + t.cols.map(([k], i) => `<td class="${i ? '' : 'b'}" title="${r.cells[k] ?? ''}">${f(r.cells[k])}</td>`).join('') + '</tr>').join('') + '</table>').join('');
  const d = D.volumes.drift;
  drift.innerHTML = `<h3>Against ${esc(D.volumes.base)}</h3>` + (!d ? '<p class="note">No other run yet: its drift shows here once a run with another label exists.</p>'
    : `<table><tr><th class="l">Variant</th>` + d.cols.map(([, title]) => `<th class="b">${title}, ${esc(d.base)}</th><th>${esc(d.label)}</th><th>Δ</th>`).join('') + '</tr>'
      + d.rows.map(r => `<tr><td class="l">${esc(r.variant)}</td>` + d.cols.map(([k]) => { const c = r[k];
        return c ? `<td class="b">${f(c.was)}</td><td>${f(c.now)}</td><td class="${c.flag ? 'bad' : 'muted'}">${c.pct > 0 ? '+' : c.pct < 0 ? '−' : ''}${Math.abs(c.pct).toFixed(1)} %</td>`
          : '<td class="b"></td><td></td><td></td>'; }).join('') + '</tr>').join('') + '</table>'
      + `<p class="note">Warm reads. Red: more than ${d.margin} % away from ${esc(d.base)}, the margin V0 asks to reproduce; a variant only in one of the two runs is not listed.</p>`);
  const c = D.volumes.fsync, pc = v => v == null ? '' : `${v > 0 ? '+' : v < 0 ? '−' : ''}${Math.abs(v).toFixed(1)} %`;
  document.getElementById('fsync').innerHTML = !c.length ? '' : '<h3>Group fsync (D3)</h3><table><tr><th class="l">Run</th><th class="b">PS import s</th>'
    + '<th>PSN import s</th><th>fsync cost</th><th>HT import s</th><th>PS vs HT</th><th>PS vs HT, import + sync</th></tr>'
    + c.map(r => `<tr><td class="l">${esc(r.label)}</td><td class="b">${f(r.PS)}</td><td>${f(r.PSN)}</td><td class="${r.flag ? 'bad' : 'muted'}">${pc(r.fsync_pct)}</td>`
      + `<td>${f(r.HT)}</td><td>${pc(r.vs_ht_pct)}</td><td>${pc(r.vs_ht_durable_pct)}</td></tr>`).join('') + '</table>'
    + '<p class="note">fsync cost: PS against PSN of the same run, import + sync when the run has it. Red: the fsync costs more than 10 % of the import, the limit of V1 (then D3 is revisited). HT never syncs, so its import s ends in the page cache; import + sync compares both on the disk.</p>';
}

function jobs() {
  const c = D.volumes.job, f = v => v == null ? '' : num(v), pc = v => v == null ? '' : `${v > 0 ? '+' : v < 0 ? '−' : ''}${Math.abs(v).toFixed(1)} %`;
  document.getElementById('job').innerHTML = !c.length ? '' : `<h3>Through a job (V2)</h3><table><tr><th class="l">Run</th><th class="b">PJ import s</th>`
    + `<th>VP import s, ${esc(D.volumes.base)}</th><th>PJ vs it</th><th>VP import s, same run</th><th>PJ vs it</th><th>PJ import + sync s</th><th>PJ vs VP, import + sync</th></tr>`
    + c.map(r => `<tr><td class="l">${esc(r.label)}</td><td class="b">${f(r.PJ)}</td><td>${f(r.VP_base)}</td><td class="${r.flag ? 'bad' : 'muted'}">${pc(r.vs_base_pct)}</td>`
      + `<td>${f(r.VP)}</td><td>${pc(r.vs_vp_pct)}</td><td>${f(r.PJ_durable)}</td><td>${pc(r.vs_vp_durable_pct)}</td></tr>`).join('') + '</table>'
    + '<p class="note">PJ: the import as an ms_flow job (<code>Flow</code>, <code>ProcessExecutor</code>, <code>table_sink</code>), with the group fsync of every commit. Red: more than 10 % over VP of the base run, the limit of V2-12. VP never syncs: import + sync compares both on the disk.</p>';
}

function reads() {
  const c = D.volumes.reads, f = v => v == null ? '' : num(v), pc = v => v == null ? '' : `${v > 0 ? '+' : v < 0 ? '−' : ''}${Math.abs(v).toFixed(1)} %`;
  document.getElementById('reads').innerHTML = !c.length ? '' : `<h3>Reading everything cold (V3)</h3><table><tr><th class="l">Run</th><th>molecules</th>`
    + `<th class="b">PJ job source s</th><th>vs VP</th><th>PJ export s</th><th>vs VP</th><th>VP reader s</th><th>MB read from the disk</th></tr>`
    + c.map(r => `<tr><td class="l">${esc(r.label)}</td><td>${f(r.n)}</td><td class="b">${f(r.source)}</td><td>${pc(r.source_vs_vp_pct)}</td>`
      + `<td>${f(r.export)}</td><td>${pc(r.export_vs_vp_pct)}</td><td>${f(r.VP)}</td><td>${f(r.disk_mb)}</td></tr>`).join('') + '</table>'
    + '<p class="note">Every current payload after evicting the files from the page cache. Job source: <code>with_payloads</code> over <code>Rows</code>, as a job reads it; export: <code>iter_bytes</code>; VP reader: the bench\'s own reader, <code>get_many</code> in batches of 10 000, of the same run.</p>';
}

function table(host) {
  host.innerHTML = '<table style="font-size:12px"><tr><th>Step</th><th>PR</th><th class="l">Commit</th><th class="l">Date</th>'
    + D.metrics.map((m, i) => `<th${i ? '' : ' class="b"'} title="${m.title}">${m.short}</th>`).join('') + '<th class="b">checks</th></tr>'
    + D.steps.map(s => `<tr><td>${esc(s.label)}</td><td>${prHtml(s)}</td><td class="l">${esc(s.commit)}</td><td class="l">${s.date}</td>`
      + D.metrics.map((m, i) => { const c = s.m[m.key];
        return `<td class="${i ? '' : 'b'}${c && c.med <= D.targets[m.key] ? ' good' : ''}"${c ? ` title="${num(c.lo)} to ${num(c.hi)} ${c.unit}"` : ''}>${c ? val(c) : ''}</td>`; }).join('')
      + `<td class="b">${passed(s)}/${Object.keys(s.checks).length}</td></tr>`).join('')
    + '<tr><td class="muted">target</td><td></td><td></td><td></td>'
    + D.metrics.map((m, i) => `<td class="muted${i ? '' : ' b'}">${D.targets[m.key] ? '≤ ' + D.targets[m.key] : ''}</td>`).join('')
    + `<td class="muted b">${D.checks.length}/${D.checks.length}</td></tr></table>`;
}

document.getElementById('sub').innerHTML = `Report of ${D.date} · ${D.steps.length} column${D.steps.length > 1 ? 's' : ''}, baseline ${esc(D.steps[0].commit)} `
  + `of ${D.steps[0].date} · ${D.machine.cpus} logical CPUs, engine on ${D.machine.engine_cpus} · Python ${D.machine.python} · `
  + `${D.sizes.chunks_per_job.toLocaleString('en')} empty chunks per job, large job ${D.sizes.chunks_scale_job.toLocaleString('en')}`
  + (D.sizes.chunks_long_job ? `, long jobs ${D.sizes.chunks_long_job.toLocaleString('en')}` : '') + ' · '
  + `data: <code>benchmarks/refactor_optimization/results</code>`;
document.getElementById('tiles').innerHTML = D.metrics.slice(0, 4).filter(m => last.m[m.key]).map(m => { const c = last.m[m.key];
  return `<div class="tile"><div class="v">${val(c)}</div><div class="l">${m.title.replace(/ \(.*/, '')} · `
    + (D.steps.length > 1 ? `<b>${times(c)}</b> than the baseline` : 'baseline') + '</div></div>'; }).join('')
  + `<div class="tile"><div class="v">${passed(last)} / ${Object.keys(last.checks).length}</div><div class="l">robustness checks pass on ${esc(last.label)}</div></div>`;
stepTable(document.getElementById('steptable'));
chart(document.getElementById('chart'));
checks(document.getElementById('checks'));
findings(document.getElementById('findings'));
table(document.getElementById('table'));
if (D.volumes) { document.getElementById('volumes-card').hidden = false; volumes(document.getElementById('volumes'), document.getElementById('drift')); jobs(); reads(); }
document.getElementById('how').innerHTML = D.metrics.map(m => `<p class="note"><b>${m.title.replace(/ \(.*/, '')}.</b> ${m.what.replace(/`([^`]+)`/g, '<code>$1</code>')}</p>`).join('');
</script>
</body>
</html>
"""

if __name__ == "__main__":
    main()
