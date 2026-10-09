"""The far side of a remote executor: run the elements a directory holds and leave there what they gave.

    python -m ms_flow.runner <dir> [--ray ADDRESS] [--cpus N]

`<dir>` is what `stage` wrote: `fn.bin` (the function, pickled: it has to be importable here),
`spec.json` (what an element needs) and `elements.bin` (the payloads, one pickle after another). The runner leaves in it:

- `results/part-00001.bin`...: `(index, result)` pickles, `index` being the place of the element
  in `elements.bin`. A part that is there is whole and never changes.
- `progress.json`: `{total, done, failed, running, status, error, home, at}`.
- `errors.jsonl`: a line `{index, error}` for each element that failed for good.
- `journal.db`: run again on the same directory, it goes on from where it was stopped.

A `File` in an element that is not a whole path is one that came with the job, under `<dir>`,
and a `Dir` that is not a whole path is made under `<dir>` for the task to write in.

Nothing here listens or connects anywhere: whoever watches reads these files. Without `--ray`
the elements run in a process pool on this machine; with it, on the Ray cluster at that address
(`auto`: the one running on this node), which whoever wrote the job script started.
"""
from __future__ import annotations

import argparse
import json
import os
import pickle
import signal
import sys
import threading
import time
import traceback
from pathlib import Path

from ms_flow.engine import Engine, ProcessExecutor, RayExecutor, content_key
from ms_flow.engine.files import Dir, swap

_PYTHON = "%d.%d" % sys.version_info[:2]
_GOING = ("waiting", "running")


def stage(home, fn, payloads, *, batched: bool = False, part_seconds: float = 30, **needs) -> int:
    """Write in `home` the job the runner runs there: `fn` over `payloads`. Returns how many.

    `needs` is what `Engine.submit` takes about the chunks: `cpu`, `gpu`, `mem_mb`, `retries`,
    `retry_wait`, `batch`. A part of results is closed `part_seconds` after its first result.
    """
    home = Path(home)
    home.mkdir(parents=True, exist_ok=True)
    total = 0
    with open(home / "elements.bin", "wb") as file:
        for payload in payloads:
            pickle.dump(payload, file)
            total += 1
    (home / "fn.bin").write_bytes(pickle.dumps(fn))
    spec = {"fn": repr(fn), "total": total, "batched": batched, "python": _PYTHON, "part_seconds": part_seconds,
            "submit": needs}
    (home / "spec.json").write_text(json.dumps(spec))
    return total


def read_part(file) -> list:
    """The `(index, result)` pairs of a results file."""
    pairs = []
    try:
        while True:
            pairs.append(pickle.load(file))
    except (EOFError, pickle.UnpicklingError):  # the end, or where a run that was killed stopped writing
        return pairs


class _Parts:
    """Results go to an open file as they come, and to disk before anybody counts them done; the
    file gets its name, and so shows, once it is `seconds` old."""

    def __init__(self, folder: Path, seconds: float):
        self.folder, self.seconds, self.lock = folder, seconds, threading.Lock()
        folder.mkdir(exist_ok=True)
        self.open, self.file, self.since = folder / "open.tmp", None, 0.0
        self.n = len(list(folder.glob("part-*.bin")))
        if self.open.exists():  # a run that was killed: what it wrote is in its journal as done
            self.file = open(self.open, "ab")
            self.roll(force=True)

    def write(self, pairs: list) -> None:
        with self.lock:
            if self.file is None:
                self.file, self.since = open(self.open, "wb"), time.monotonic()
            for pair in pairs:
                pickle.dump(pair, self.file)
            self.file.flush()
            os.fsync(self.file.fileno())

    def roll(self, force: bool = False) -> None:
        with self.lock:
            if self.file is not None and (force or time.monotonic() - self.since >= self.seconds):
                self.file.close()
                self.file = None
                self.n += 1
                os.replace(self.open, self.folder / f"part-{self.n:05d}.bin")


def _tell(home: Path, **progress) -> None:
    (home / "progress.tmp").write_text(json.dumps({**progress, "home": str(home.resolve()), "at": time.time()}))
    os.replace(home / "progress.tmp", home / "progress.json")


def run(home, ray: str | None = None, cpus: int | None = None) -> str:
    """Run the job staged in `home` to its end. Returns the status it ended with."""
    home = Path(home).resolve()
    spec = json.loads((home / "spec.json").read_text())
    if spec["python"] != _PYTHON:
        raise RuntimeError(f"staged with Python {spec['python']} and this is {_PYTHON}: what one pickles the "
                           "other may not read")
    fn = pickle.loads((home / "fn.bin").read_bytes())
    index_of, parts = {}, _Parts(home / "results", spec["part_seconds"])

    def here(mark):
        if isinstance(mark, Dir) and not os.path.isabs(mark):
            os.makedirs(home / mark, exist_ok=True)
        return os.path.join(home, mark)

    def elements():
        with open(home / "elements.bin", "rb") as file:
            for index in range(spec["total"]):
                # Plain paths from here on: whatever runs the task shares this disk.
                payload = swap(pickle.load(file), here)
                index_of[content_key(payload)] = index  # ponytail: equal payloads are one element, as in a journal
                yield payload

    def failed(key, error):
        with open(home / "errors.jsonl", "a") as file:
            file.write(json.dumps({"index": index_of[key], "error": error}) + "\n")

    cpus = cpus or (len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else os.cpu_count())
    with Engine(cpus=cpus) as engine:
        engine.register("run", ProcessExecutor(cpus) if ray is None else RayExecutor(ray))
        engine.attach("journal", home / "journal.db")
        job = engine.submit(
            "run", fn, elements(), total=spec["total"], journal="journal", key="job", batched=spec["batched"],
            on_results=lambda pairs: parts.write([(index_of[key], result) for key, result in pairs]),
            on_error=failed, **spec["submit"])
        try:
            while True:
                now = engine.wait(job, 2)
                going = now.status in _GOING
                parts.roll(force=not going)
                _tell(home, total=now.chunks_total, done=now.chunks_done, failed=now.chunks_failed,
                      running=now.chunks_inflight, status=now.status, error=None if going else now.error)
                if not going:
                    return now.status
        except BaseException:  # stopped from outside: what is done shows, and the next run goes on
            engine.close()
            parts.roll(force=True)
            now = engine.status(job)
            _tell(home, total=now.chunks_total, done=now.chunks_done, failed=now.chunks_failed, running=0,
                  status=now.status, error=now.error)
            raise


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ms_flow.runner", description=__doc__.split("\n")[0])
    parser.add_argument("dir")
    parser.add_argument("--ray", help="address of a running Ray cluster, or `auto`; without it, a process pool here")
    parser.add_argument("--cpus", type=int, help="processes of the pool; by default, the CPUs this job was given")
    args = parser.parse_args(argv)
    home = Path(args.dir)
    try:
        was = json.loads((home / "progress.json").read_text())["status"]
    except (OSError, ValueError, KeyError):
        was = None
    if was is not None and was not in (*_GOING, "interrupted", "canceled"):
        print(f"nothing to do: this job ended as {was}")
        return 0
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))  # what a queue sends before it kills
    try:
        status = run(home, args.ray, args.cpus)
    except Exception:  # it never got to run: say so where whoever watches looks
        _tell(home, status="failed", error=traceback.format_exc())
        raise
    return 0 if status.startswith("completed") else 1


if __name__ == "__main__":
    sys.exit(main())
