"""Campaigns: jobs whose stages all run on a cluster, from the first element to the last result.

    @campaign(output=table_sink("docking_results"), volatile=("run_id",))
    def screen(params, project):
        return [Stage("vs", dock, shards_of(project)),
                Stage("top", dock, From("vs", partial(best, n=params.top)), deliver="none"),
                Stage("off", dock, From("top", against_off_targets))]

    workspace.submit(screen, params, executor="hpc")      # an executor of kind `queue`

A stage runs its task over its source: what the source function gives for the first, and what
`From(stage, fn)` makes there of an earlier stage for the others (`fn` gets what that stage's
`reduce` made of its results or, without one, the iterator of its results). `deliver` is what
of a stage comes back: each result as it is written there (`stream`), what its `reduce` made
(`reduced`), or nothing (`none`). Every element that failed for good comes back as a failed
chunk, whatever the stage.

Here the campaign is a job of the engine like any other: what comes back goes to its `output`
and is recorded in its journal as it comes. One job of the queue runs every stage (see
`ms_flow.runner`), and it goes on with this process closed: submitted again, the campaign is
found where it is by its identity, the hash of the job's name, its params but the `volatile`
ones and what was sent (`manifest.sha`). It is then
- queued or running: followed;
- left the queue before its end: what was sent is checked and what is missing is sent again,
  and it is queued again where it is, going on from its journal;
- canceled (here, or by the queue's own command): left as it is, and a new attempt starts;
- complete: only what this side has not got yet comes.

Each attempt has its own directory, `c<identity>-<n>` in the host's work directory, and what
travels is the files of the elements, once each, by their sha256 (`Stage.digests` are the ones
already known). A directory goes once all of it came back, or it was canceled, and
`keep_after` days passed (see `RemoteExecutor`), when a campaign is submitted to that host.
"""
from __future__ import annotations

import dataclasses
import hashlib
import inspect
import io
import json
import logging
import os
import pickle
import re
import shutil
import tarfile
import tempfile
import threading
from functools import partial
from pathlib import Path
from typing import Any, Callable, Optional

from ms_flow.engine.files import Dir, File, swap
from ms_flow.engine.flow import job, task
from ms_flow.remote import RemoteExecutor, _untar
from ms_flow.runner import read_part, stage_campaign

log = logging.getLogger(__name__)

_DELIVER = ("stream", "reduced", "none")
_CHUNK_NEEDS = ("cpu", "gpu", "mem_mb", "retries", "batch", "batched")


@dataclasses.dataclass(frozen=True)
class From:
    """The elements of a stage: what `fn` makes of what the stage `stage` left."""

    stage: str
    fn: Callable


@dataclasses.dataclass(frozen=True)
class Stage:
    """A step of a campaign: `task` (a `@task`) over `source` (see the module's note).
    `digests` maps the path of a `File` in the elements to its sha256, which then is not read."""

    name: str
    task: Callable
    source: Any
    reduce: Optional[Callable] = None
    deliver: str = "stream"
    digests: Optional[dict] = None


@task(cpu=0, thread=True)
def _take(element):
    """What came back of an element there, as its result here."""
    if "error" in element:
        raise RuntimeError(element["error"])
    return element["result"]


def campaign(*, name: str = "", params_model=None, output=None, volatile=(), reads=(), finalize=None):
    """Mark a function `(params, project)` that returns the `Stage`s of a campaign as a job.

    `volatile` names the params that are not the campaign's identity (a run's id): its tasks
    over there get them as None. The rest is as in `job`.
    """
    def mark(source):
        job(task=_take, name=name, params_model=params_model, output=output, reads=reads, finalize=finalize)(source)
        source.__job__["campaign"] = tuple(volatile)
        return source
    return mark


def _check(stages: list) -> None:
    names = [stage.name for stage in stages]
    if not stages or len(set(names)) != len(names):
        raise ValueError("a campaign is one or more stages, each with its own name")
    for place, stage in enumerate(stages):
        if not re.fullmatch(r"[A-Za-z0-9_-]+", stage.name):
            raise ValueError(f"stage {stage.name!r}: a name of letters, digits, `_` and `-`")
        if not hasattr(stage.task, "__task__"):
            raise TypeError(f"stage {stage.name!r}: {stage.task!r} is not a task: decorate it with @task(...)")
        if stage.deliver not in _DELIVER or stage.deliver == "reduced" and stage.reduce is None:
            raise ValueError(f"stage {stage.name!r}: deliver is one of {_DELIVER}, and `reduced` needs a reduce")
        if place and not (isinstance(stage.source, From) and stage.source.stage in names[:place]):
            raise ValueError(f"stage {stage.name!r}: the source of a stage after the first is From(an earlier stage)")
        if not place and isinstance(stage.source, From):
            raise ValueError("the first stage's source is its elements, not From")


def _sha256(path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as file:
        for block in iter(lambda: file.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _params(params) -> dict:
    if params is None:
        return {}
    return params.model_dump(mode="json") if hasattr(params, "model_dump") else dict(params)


def prepare(engine, job, params, executor: str, project, journal: str | None):
    """The campaign `job` is with `params` on `executor`, staged here, and the id of the job of
    `engine` that already follows it, or None. Called by `Flow.submit`."""
    remote = engine.executor(executor)
    if not isinstance(remote, RemoteExecutor):
        raise ValueError(f"a campaign runs on an executor of kind `queue`, and `{executor}` is not one")
    far = Campaign(remote, job, params, project)
    running = next((job_id for job_id, held in tuple(engine._jobs.items()) if held.key == far.key), None)
    if running is not None:
        far.close()
        return far, running
    if journal is not None:
        # ponytail: an interrupted one continues; one that ended any other way starts afresh here
        # and finds over there what is left of it.
        was = next((row["status"] for row in engine.recorded(journal) if row["job_id"] == far.key), None)
        if was is not None and was != "interrupted":
            engine.discard(journal, far.key)
    return far, None


class Campaign:
    """The source of the job that follows a campaign: it sends it, or finds it, and gives what
    comes back as it comes, `{"_key", "result"}` or `{"_key", "error"}`. Read on the engine's
    feeding thread; `ended` stops it."""

    def __init__(self, remote: RemoteExecutor, job, params, project):
        spec = job.__job__
        stages = list(job(params, project))
        _check(stages)
        self.remote, self.stages, self.dir, self.n = remote, stages, None, 0
        self._stop = threading.Event()
        # What the tasks over there get: the params without the volatile ones.
        volatile = spec["campaign"]
        there = params
        if volatile and hasattr(params, "model_copy"):
            there = params.model_copy(update=dict.fromkeys(volatile))
        elif volatile and params is not None:
            there = {**params, **dict.fromkeys(volatile)}

        def bound(fn):
            return partial(fn, params=there) if "params" in inspect.signature(fn).parameters else fn

        self._tmp = Path(tempfile.mkdtemp(prefix="msflow-campaign-"))
        try:
            carried, known = {}, dict(stages[0].digests or {})

            def carry(mark):
                if isinstance(mark, Dir):
                    raise TypeError("what comes back of a campaign is its results, not files: no Dir in its elements")
                if mark not in carried:
                    carried[mark] = f"files/{known.get(mark) or _sha256(mark)}/{os.path.basename(mark)}"
                return File(carried[mark])

            listed = [{"name": stage.name, "fn": bound(stage.task),
                       "needs": {need: stage.task.__task__[need] for need in _CHUNK_NEEDS},
                       "reduce": stage.reduce,
                       "from": None if place == 0 else (stage.source.stage, stage.source.fn)}
                      for place, stage in enumerate(stages)]
            stage_campaign(self._tmp, listed, (swap(payload, carry) for payload in stages[0].source),
                           part_seconds=remote.part_seconds)
            manifest = {name: _sha256(self._tmp / name) for name in ("spec.json", "stages.bin", "elements.bin")}
            manifest.update({there: there.split("/")[1] for there in carried.values()})
            (self._tmp / "manifest.sha").write_text("".join(f"{sha}  {name}\n" for name, sha in sorted(manifest.items())))
            (self._tmp / "job.sh").write_text(remote.script)
            self._local = {name: self._tmp / name for name in (*manifest, "manifest.sha", "job.sh")}
            self._local.update({there: here for here, there in carried.items()})
            identity = {"job": spec["name"], "params": {name: value for name, value in _params(params).items()
                                                         if name not in volatile},
                        "manifest": manifest}
            self.key = hashlib.sha256(json.dumps(identity, sort_keys=True, default=str).encode()).hexdigest()
        except BaseException:
            self.close()
            raise
        self.stem = f"c{self.key[:24]}"

    # --- What the engine reads ---------------------------------------------------------------

    def __iter__(self):
        try:
            self._sweep()
            self._place()
            self.close()  # all of it is there now
            yield from self._follow()
        finally:
            self.close()

    def close(self) -> None:
        shutil.rmtree(self._tmp, ignore_errors=True)

    def ended(self, snapshot) -> None:
        """The job that follows it ended: a campaign canceled here is canceled there, and one
        that is all here is kept for `keep_after` days. Interrupted or failed, it is left as it is."""
        self._stop.set()
        if self.dir is None:
            return
        here, queue = f"{self.remote.workdir}/{self.dir}", self.remote.queue
        try:
            if snapshot.status == "canceled":
                self._run(f"cd {here} || exit 0; id=$(cat queue.id 2>/dev/null); "
                          f"[ -z \"$id\" ] || {{ {queue['cancel'].format(id='$id')}; }} 2>/dev/null; touch cancelled")
            elif snapshot.status.startswith("completed"):
                self._run(f"cd {here} && touch ingested" if self.remote.keep_after else
                          f"rm -rf {here} 2>/dev/null || {{ sleep 2; rm -rf {here}; }}")
        except Exception:  # ponytail: what is not tidied up now is when `keep_after` passes
            log.warning("could not tell %s that campaign %s ended", self.remote.remote.address or "this machine",
                        self.dir, exc_info=True)

    # --- Over there ----------------------------------------------------------------------

    def _run(self, command: str, data=b"") -> bytes:
        return self.remote.remote.run(command, data)

    def _sweep(self) -> None:
        minutes = int(self.remote.keep_after * 1440)
        self._run(f"cd {self.remote.workdir} 2>/dev/null || exit 0; for m in c*-*/ingested c*-*/cancelled; do "
                  f"[ -f \"$m\" ] && [ -n \"$(find \"$m\" -mmin +{minutes})\" ] && rm -rf \"${{m%/*}}\"; done; :")

    def _look(self, dir: str) -> dict:
        """What the attempt in `dir` is: `id`, `told` (its progress), `cancelled`, `alive`, `state`."""
        queue = self.remote.queue
        said = self._run(
            f"cd {self.remote.workdir}/{dir} 2>/dev/null || exit 0; id=$(cat queue.id 2>/dev/null); echo \"id=$id\"; "
            "[ -f cancelled ] && echo cancelled; [ -f progress.json ] && echo \"told=$(cat progress.json)\"; "
            f"[ -n \"$id\" ] && {{ {{ {queue['alive'].format(ids='$id')}; }} 2>/dev/null | grep -qx \"$id\" && echo alive; "
            f"echo \"state=$({{ {queue['state'].format(id='$id')}; }} 2>/dev/null)\"; }}; :").decode()
        look = {"id": "", "told": {}, "cancelled": False, "alive": False, "state": ""}
        for line in said.splitlines():
            name, _, value = line.partition("=")
            if name in ("cancelled", "alive"):
                look[name] = True
            elif name == "told":
                try:
                    look["told"] = json.loads(value)
                except ValueError:
                    pass
            elif name in ("id", "state"):
                look[name] = value.strip()
        return look

    def _complete(self, told: dict) -> bool:
        return str(told.get("status", "")).startswith("completed") and told.get("stages_done") == len(self.stages)

    def _place(self) -> None:
        """Find the attempt this campaign is at over there, or start one, and see that it runs."""
        said = self._run(f"cd {self.remote.workdir} 2>/dev/null || exit 0; ls -d {self.stem}-* 2>/dev/null; :").decode()
        tried = sorted(int(name.rpartition("-")[2]) for name in said.split() if name.rpartition("-")[2].isdigit())
        if tried:
            self.n = tried[-1]
            self.dir = f"{self.stem}-{self.n}"
            look = self._look(self.dir)
            if look["state"].startswith("CANCELLED") and not look["cancelled"]:  # by the queue's own command
                self._run(f"touch {self.remote.workdir}/{self.dir}/cancelled")
                look["cancelled"] = True
            # ponytail: one whose runner failed (its code, not its allocation) starts afresh too.
            fresh = look["cancelled"] or look["told"].get("status") == "failed"
            if not fresh:
                if look["alive"] or self._complete(look["told"]):
                    return
                self._fill()
                self._submit()
                return
            self.n += 1
        self.dir = f"{self.stem}-{self.n}"
        self._upload(list(self._local), make=True)
        self._fill()
        self._submit()

    def _upload(self, names: list, make: bool = False) -> None:
        with tempfile.TemporaryFile() as packed:  # on disk: the files of the elements, of any size
            with tarfile.open(fileobj=packed, mode="w", dereference=True) as tar:
                for name in names:
                    tar.add(self._local[name], arcname=name)
            packed.seek(0)
            here = f"{self.remote.workdir}/{self.dir}"
            self._run(f"{'mkdir -p ' + here + ' && ' if make else ''}cd {here} && tar xf -", packed)

    def _fill(self) -> None:
        """What was sent is all there, as it was here: what is missing or not the same goes again."""
        for _ in range(2):
            said = self._run(f"cd {self.remote.workdir}/{self.dir} && "
                             "{ sha256sum -c manifest.sha 2>/dev/null || :; } | sed -n 's/: FAILED.*$//p'").decode()
            wrong = [name for name in said.splitlines() if name]
            if not wrong:
                return self._upload(["job.sh"])  # the script of this side, a template that changed too
            log.info("campaign %s: %d files sent again", self.dir, len(wrong))
            self._upload([name for name in wrong if name in self._local] + ["manifest.sha"])
        raise OSError(f"campaign {self.dir}: what was sent is not what arrived: {', '.join(wrong[:5])}")

    def _submit(self) -> None:
        self.remote._queue(f"cd {self.remote.workdir}/{self.dir} && rm -f cancelled")

    def _follow(self):
        """Every round, one command: the progress, whether the job is in the queue, and whatever
        is new of each stage. What comes is given, under a key of its attempt."""
        streamed = [stage.name for stage in self.stages if stage.deliver == "stream"]
        reduced = {stage.name for stage in self.stages if stage.deliver == "reduced"}
        parts, errors = dict.fromkeys(streamed, 0), {stage.name: 0 for stage in self.stages}
        queue, ending, was = self.remote.queue, False, None
        while not self._stop.is_set():
            asked = " ".join(f"{name}:{parts[name]}" for name in streamed)
            files = _untar(self._run(
                f"cd {self.remote.workdir}/{self.dir} || exit 1; t=.poll.$$; "
                "cat progress.json > $t.told 2>/dev/null; id=$(cat queue.id 2>/dev/null); "
                f"{{ [ -n \"$id\" ] && {{ {queue['alive'].format(ids='$id')}; }} | grep -x \"$id\"; }} > $t.alive 2>/dev/null; "
                "echo $t.told > $t; echo $t.alive >> $t; "
                f"for a in {asked}; do s=${{a%:*}}; n=${{a#*:}}; ls stages/$s/results 2>/dev/null | "
                "awk -v d=stages/$s/results -v n=$n -F'[-.]' '/^part-/ && $2 + 0 > n {print d \"/\" $0}'; done >> $t; "
                f"for f in stages/*/errors.jsonl {' '.join(f'stages/{name}/reduced.bin' for name in sorted(reduced))}; "
                "do [ -f \"$f\" ] && echo \"$f\"; done >> $t; tar cf - -T $t 2>/dev/null; rm -f $t $t.told $t.alive"))
            told = json.loads(next((data for name, data in files.items() if name.endswith(".told")), b"") or b"{}")
            alive = any(data.strip() for name, data in files.items() if name.endswith(".alive"))
            for name in streamed:
                new = sorted(file for file in files if file.startswith(f"stages/{name}/results/part-"))
                for file in new:
                    for index, result in read_part(io.BytesIO(files[file])):
                        yield {"_key": f"{self.n}:{name}:{index}", "result": result}
                if new:
                    parts[name] = int(new[-1][-9:-4])
            for stage in self.stages:
                lines = files.get(f"stages/{stage.name}/errors.jsonl", b"").splitlines()
                for line in lines[errors[stage.name]:]:
                    row = json.loads(line)
                    yield {"_key": f"{self.n}:{stage.name}:e{row['index']}", "error": f"stage {stage.name}: {row['error']}"}
                errors[stage.name] = len(lines)
            for name in sorted(reduced):
                if f"stages/{name}/reduced.bin" in files:
                    reduced.discard(name)
                    yield {"_key": f"{self.n}:{name}:reduced", "result": pickle.loads(files[f"stages/{name}/reduced.bin"])}
            # The progress was read before the parts were listed: complete, nothing is left out.
            if self._complete(told):
                return
            if told.get("status") == "failed":
                raise RuntimeError(f"campaign {self.dir} failed there: {told.get('error')}{self._kept()}")
            if alive:
                ending = False
            elif not ending:  # its last word may have come after this round was read
                ending = True
                continue
            else:
                state = self._look(self.dir)["state"]
                if state.startswith("CANCELLED"):
                    self._run(f"touch {self.remote.workdir}/{self.dir}/cancelled")
                    raise RuntimeError(f"campaign {self.dir} was canceled in the queue{self._kept()}")
                now = (told.get("stages_done", 0), told.get("done", 0))
                if was is not None and now <= was:
                    raise RuntimeError(f"campaign {self.dir} left the queue before its end, no further than when it "
                                       f"was queued again{self._kept()}")
                # Out of the queue before its end (its time ran out, its node failed): once more,
                # and the runner goes on from its journal.
                was, ending = now, False
                log.info("campaign %s left the queue at %s: queued again", self.dir, now)
                self._submit()
            self._stop.wait(self.remote.poll_s)

    def _kept(self) -> str:
        return (f"\nits directory is kept: {self.remote.workdir}/{self.dir} on "
                f"{self.remote.remote.address or 'this machine'} (see runner.log)")
