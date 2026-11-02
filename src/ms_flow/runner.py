"""The far side of a remote executor: run the elements a directory holds and leave there what they gave.

    python -m ms_flow.runner <dir> [--ray ADDRESS [--nodes N]] [--cpus N]

`<dir>` is what `stage` wrote: `fn.bin` (the function, pickled: it has to be importable here),
`spec.json` (what an element needs) and `elements.bin` (the payloads, one pickle after another). The runner leaves in it:

- `results/part-00001.bin`...: `(index, result)` pickles, `index` being the place of the element
  in `elements.bin`. A part that is there is whole and never changes.
- `progress.json`: `{total, done, failed, running, status, error, home, at}`.
- `errors.jsonl`: a line `{index, error}` for each element that failed for good.
- `journal.db`: run again on the same directory, it goes on from where it was stopped.

A `File` in an element that is not a whole path is one that came with the job, under `<dir>`,
and a `Dir` that is not a whole path is made under `<dir>` for the task to write in.

A campaign (`stage_campaign`, see `ms_flow.campaign`) has `stages.bin` too: its stages run one
after the other, each a job of its own in the journal, the first over `elements.bin` and each of
the others over what its `From` makes of an earlier one, made once into
`stages/<name>/elements.bin`. A stage leaves under `stages/<name>/` its `results/` and
`errors.jsonl` as above, `reduced.bin` (what its `reduce` made of its results) and `done` once it
ended. `progress.json` says which `stage` runs and how many are done (`stages_done`).

Nothing here listens or connects anywhere: whoever watches reads these files. Without `--ray`
the elements run in a process pool on this machine; with it, on the Ray cluster at that address
(`auto`: the one running on this node), which whoever wrote the job script started, once
`--nodes` nodes are in it.
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
from ms_flow.engine.tokens import usable_cpus

_PYTHON = "%d.%d" % sys.version_info[:2]
_GOING = ("waiting", "running")


def stage(home, fn, payloads, *, batched: bool = False, part_seconds: float = 30, **needs) -> int:
    """Write in `home` the job the runner runs there: `fn` over `payloads`. Returns how many.

    `needs` is what `Engine.submit` takes about the chunks: `cpu`, `gpu`, `mem_mb`, `retries`,
    `retry_wait`, `batch`. A part of results is closed `part_seconds` after its first result.
    """
    home = Path(home)
    home.mkdir(parents=True, exist_ok=True)
    total = _elements(home / "elements.bin", payloads)
    (home / "fn.bin").write_bytes(pickle.dumps(fn))
    spec = {"fn": repr(fn), "total": total, "batched": batched, "python": _PYTHON, "part_seconds": part_seconds,
            "submit": needs}
    (home / "spec.json").write_text(json.dumps(spec))
    return total


def stage_campaign(home, stages: list[dict], payloads, *, part_seconds: float = 30) -> int:
    """Write in `home` a campaign: `stages` run one after the other, the first over `payloads`.
    Returns how many those are.

    A stage is `{name, fn, needs, reduce, from}`: `needs` what `Engine.submit` takes about its
    chunks, `reduce` None or a function of the iterator of its results, `from` None for the first
    and `(stage, fn)` for the others, `fn` taking what that earlier stage left (what its `reduce`
    made, or else the iterator of its results) and giving the elements of this one.
    """
    home = Path(home)
    home.mkdir(parents=True, exist_ok=True)
    total = _elements(home / "elements.bin", payloads)
    (home / "stages.bin").write_bytes(pickle.dumps(stages))
    (home / "spec.json").write_text(json.dumps({"fn": " > ".join(stage["name"] for stage in stages), "total": total,
                                                "python": _PYTHON, "part_seconds": part_seconds,
                                                "stages": [stage["name"] for stage in stages]}))
    return total


def _elements(path: Path, payloads) -> int:
    """Write `payloads` to `path`, one pickle after another, whole or not at all. Returns how many."""
    total = 0
    with open(path.with_suffix(".tmp"), "wb") as file:
        for payload in payloads:
            pickle.dump(payload, file)
            total += 1
    os.replace(path.with_suffix(".tmp"), path)
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


def run(home, ray: str | None = None, cpus: int | None = None, nodes: int = 1) -> str:
    """Run the job staged in `home` to its end. Returns the status it ended with."""
    home = Path(home).resolve()
    spec = json.loads((home / "spec.json").read_text())
    if spec["python"] != _PYTHON:
        raise RuntimeError(f"staged with Python {spec['python']} and this is {_PYTHON}: what one pickles the "
                           "other may not read")
    if ray is not None and nodes > 1:
        _wait_for_nodes(ray, nodes)
    cpus = cpus or usable_cpus()
    with Engine(cpus=cpus) as engine:
        # Every node of a cluster's allocation sees its disk: Ray carries no file.
        engine.register("run", ProcessExecutor(cpus) if ray is None else RayExecutor(ray, shared=True))
        engine.attach("journal", home / "journal.db")
        if (home / "stages.bin").exists():
            return _stages(engine, home, spec)
        fn = pickle.loads((home / "fn.bin").read_bytes())
        return _run_job(engine, home, home, fn, home / "elements.bin", spec["total"], "job",
                        {"batched": spec["batched"], **spec["submit"]}, spec["part_seconds"])


def _wait_for_nodes(address: str, nodes: int, seconds: float = 300) -> None:
    """Until the Ray at `address` has `nodes` nodes: the job script starts them all at once."""
    import ray

    ray.init(address=address, include_dashboard=False, log_to_driver=False)
    try:
        limit = time.monotonic() + seconds
        while sum(node["Alive"] for node in ray.nodes()) < nodes:
            if time.monotonic() > limit:
                raise RuntimeError(f"the Ray at {address} has not got its {nodes} nodes after {seconds:.0f} s")
            time.sleep(1)
    finally:
        ray.shutdown()


def _stages(engine, home: Path, spec: dict) -> str:
    stages = pickle.loads((home / "stages.bin").read_bytes())
    status = "completed"
    for place, step in enumerate(stages):
        folder = home / "stages" / step["name"]
        folder.mkdir(parents=True, exist_ok=True)
        if not (folder / "done").exists():
            if place == 0:
                source, total = home / "elements.bin", spec["total"]
            else:
                source = folder / "elements.bin"
                if not (folder / "elements.json").exists():
                    earlier, make = step["from"]
                    total = _elements(source, make(_left(home / "stages" / earlier)))
                    (folder / "elements.json").write_text(json.dumps({"total": total}))
                total = json.loads((folder / "elements.json").read_text())["total"]
            ended = _run_job(engine, home, folder, step["fn"], source, total, f"stage:{step['name']}", step["needs"],
                             spec["part_seconds"], last=place == len(stages) - 1, stage=step["name"],
                             stages_done=place)
            if not ended.startswith("completed"):
                return ended
            if step["reduce"] is not None:
                (folder / "reduced.tmp").write_bytes(pickle.dumps(step["reduce"](_results(folder))))
                os.replace(folder / "reduced.tmp", folder / "reduced.bin")
            (folder / "done").touch()
        if (folder / "errors.jsonl").exists():
            status = "completed_with_errors"
    try:
        told = json.loads((home / "progress.json").read_text())
    except (OSError, ValueError):
        told = {}
    told.pop("home", None), told.pop("at", None)
    _tell(home, **{**told, "status": status, "stage": None, "stages_done": len(stages), "running": 0})
    return status


def _results(folder: Path):
    """The results a stage left, each once: a part a killed run wrote twice says it twice."""
    seen = set()
    for part in sorted((folder / "results").glob("part-*.bin")):
        with open(part, "rb") as file:
            for index, result in read_part(file):
                if index not in seen:
                    seen.add(index)
                    yield result


def _left(folder: Path):
    """What a stage that ended left for the next: what its `reduce` made, or its results."""
    if (folder / "reduced.bin").exists():
        return pickle.loads((folder / "reduced.bin").read_bytes())
    return _results(folder)


def _run_job(engine, home: Path, folder: Path, fn, source: Path, total: int, key: str, needs: dict,
             part_seconds: float, last: bool = True, **said) -> str:
    """Run `fn` over the elements in `source` as the job `key` of the journal, its results and
    errors to `folder`. Only the `last` job of a run says it completed: the next run would do nothing."""
    index_of, parts = {}, _Parts(folder / "results", part_seconds)

    def here(mark):
        if isinstance(mark, Dir) and not os.path.isabs(mark):
            os.makedirs(home / mark, exist_ok=True)
        return os.path.join(home, mark)

    def elements():
        with open(source, "rb") as file:
            for index in range(total):
                # Plain paths from here on: whatever runs the task shares this disk.
                payload = swap(pickle.load(file), here)
                index_of[content_key(payload)] = index  # ponytail: equal payloads are one element, as in a journal
                yield payload

    def failed(key, error):
        with open(folder / "errors.jsonl", "a") as file:
            file.write(json.dumps({"index": index_of[key], "error": error}) + "\n")

    def tell(now, running):
        status = now.status if last or not now.status.startswith("completed") else "running"
        _tell(home, total=now.chunks_total, done=now.chunks_done, failed=now.chunks_failed, running=running,
              status=status, error=None if now.status in _GOING else now.error, **said)

    job = engine.submit(
        "run", fn, elements(), total=total, journal="journal", key=key,
        on_results=lambda pairs: parts.write([(index_of[key], result) for key, result in pairs]),
        on_error=failed, **needs)
    try:
        while True:
            now = engine.wait(job, 2)
            going = now.status in _GOING
            parts.roll(force=not going)
            tell(now, now.chunks_inflight)
            if not going:
                return now.status
    except BaseException:  # stopped from outside: what is done shows, and the next run goes on
        engine.close()
        parts.roll(force=True)
        tell(engine.status(job), 0)
        raise


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ms_flow.runner", description=__doc__.split("\n")[0])
    parser.add_argument("dir")
    parser.add_argument("--ray", help="address of a running Ray cluster, or `auto`; without it, a process pool here")
    parser.add_argument("--nodes", type=int, default=1, help="with `--ray`: how many nodes to wait for in it")
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
        status = run(home, args.ray, args.cpus, args.nodes)
    except Exception:  # it never got to run: say so where whoever watches looks
        _tell(home, status="failed", error=traceback.format_exc())
        raise
    return 0 if status.startswith("completed") else 1


if __name__ == "__main__":
    sys.exit(main())
