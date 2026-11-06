"""Campaigns: jobs whose stages all run on a cluster, from the first element to the last result.

    @campaign(output=table_sink("docking_results"), volatile=("run_id",))
    def screen(params, project):
        return [Stage("vs", dock, shards_of(project)),
                Stage("top", dock, From("vs", partial(best, n=params.top)), deliver="none"),
                Stage("off", dock, From("top", against_off_targets))]

    workspace.submit(screen, params, executor="hpc")      # an executor of kind `queue`

A stage runs its task over its source: what the source function gives for the first, and what
`From(stage, fn, args)` makes there of an earlier stage for the others (`fn` gets what that
stage's `reduce` made of its results or, without one, the iterator of its results, and then
`args`). `deliver` is what
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
travels is the files of the elements and of the `args` of `From`, once each, by their sha256
(`Stage.digests` are the ones already known). A `Dir` is a directory there for a task to write
in, and stays there: only results come back. A directory goes once all of it came back, or it was canceled, and
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
import re
import shlex
import shutil
import subprocess
import tarfile
import tempfile
import threading
import time
from functools import partial
from pathlib import Path
from typing import Any, Callable, Optional

from ms_flow import locations
from ms_flow.engine.files import Dir, File, swap
from ms_flow.engine.flow import job, task
from ms_env import HostError
from ms_flow.remote import RemoteExecutor, _untar, read_usage, unreachable
from ms_flow.runner import part_number, read_part, stage_campaign, unplain

log = logging.getLogger(__name__)

_DELIVER = ("stream", "reduced", "none")
_CHUNK_NEEDS = ("cpu", "gpu", "mem_mb", "retries", "batch", "batched")


@dataclasses.dataclass(frozen=True)
class From:
    """The elements of a stage: what `fn` makes of what the stage `stage` left, and of `args`
    (whose files travel with the campaign)."""

    stage: str
    fn: Callable
    args: tuple = ()


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


_LET_GO = 120.0  # seconds a queue has to let a canceled job go before that is an error


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


def prepare(engine, job, params, executor: str, project, journal: str | None, asks: dict | None = None):
    """The campaign `job` is with `params` on `executor`, staged here, and the id of the job of
    `engine` that already follows it, or None. `asks` is what its jobs ask of the queue, instead
    of what the executor asks (see `RemoteExecutor.script_for`). Called by `Flow.submit`."""
    remote = engine.executor(executor)
    if not isinstance(remote, RemoteExecutor):
        raise ValueError(f"a campaign runs on an executor of kind `queue`, and `{executor}` is not one")
    far = Campaign(remote, job, params, project, asks, executor=executor)
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


# How often, at most, a running campaign asks the queue's accounting what its job used (it
# asks once more at the end): sacct goes to the cluster's database, which sites ask to spare.
_ACCOUNT_EVERY = 300.0


class Campaign:
    """The source of the job that follows a campaign: it sends it, or finds it, and gives what
    comes back as it comes, `{"_key", "result"}` or `{"_key", "error"}`. Read on the engine's
    feeding thread; `ended` stops it. `executor` is the queue it goes to, which is where the job
    that follows it is shown; `total` is how many results come back, when that is known before
    (one stage, each result as it comes), else None; `note` is what it is at, in a line, and
    `steps` every step it went through, `(time, line)`, the quick ones too; `queue_usage` what the
    queue's accounting says its job there used (`read_usage`, and `job`), None until it says."""

    def __init__(self, remote: RemoteExecutor, job, params, project, asks: dict | None = None, executor: str = ""):
        spec = job.__job__
        stages = list(job(params, project))
        _check(stages)
        self.remote, self.stages, self.dir, self.n, self.job = remote, stages, None, 0, ""
        self.executor, self.total, self.steps = executor or None, None, []
        self.queue_usage, self._accounted = None, 0.0
        self.note = "staging its elements and their files here"
        # Canceling: nothing goes to the queue from then on, and what is there is stopped (`stop_there`).
        self._submitting, self._canceling, self._stopped = threading.Lock(), False, False
        self.project, uid = project, locations.uid(project)
        # The project's files go to its folder there, once for every campaign; any other, with it.
        self._store = f"../projects/{uid}/" if uid else None
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
            carried, made, known, self._shas = {}, {}, dict(stages[0].digests or {}), {}

            def carry(mark):
                if isinstance(mark, Dir):  # made there, under the campaign's directory, and left there
                    return Dir(made.setdefault(mark, f"out/{len(made)}"))
                if mark not in carried:
                    sha, path = known.get(mark) or _sha256(mark), self._store and locations.inside(project, mark)
                    carried[mark] = self._store + path if path else f"files/{sha}/{os.path.basename(mark)}"
                    self._shas[carried[mark]] = sha
                return File(carried[mark])

            listed = [{"name": stage.name, "fn": bound(stage.task),
                       "needs": {need: stage.task.__task__[need] for need in _CHUNK_NEEDS},
                       "reduce": stage.reduce,
                       "from": None if place == 0 else (stage.source.stage, stage.source.fn,
                                                        swap(tuple(stage.source.args), carry))}
                      for place, stage in enumerate(stages)]
            elements = stage_campaign(self._tmp, listed, (swap(payload, carry) for payload in stages[0].source),
                           part_seconds=remote.part_seconds)
            if len(stages) == 1 and stages[0].deliver == "stream":
                self.total = elements
            self.note = f"checksumming the {elements:,} elements staged and their files"
            manifest = {name: _sha256(self._tmp / name) for name in ("spec.json", "elements.jsonl")}
            manifest.update(self._shas)
            (self._tmp / "manifest.sha").write_text("".join(f"{sha}  {name}\n" for name, sha in sorted(manifest.items())))
            # not in its identity: the same campaign asking for more is the same campaign
            (self._tmp / "job.sh").write_text(remote.script_for(asks))
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

    @property
    def note(self) -> str | None:
        return self._note

    @note.setter
    def note(self, line: str | None) -> None:
        # The same step with new figures ("running in the queue (job 7): 3 of 9 done") is the
        # line of the step it is in, not one more step.
        self._note = line
        if line is not None and self.steps and self.steps[-1][1].split(":")[0] == line.split(":")[0]:
            self.steps[-1] = (self.steps[-1][0], line)
        elif line is not None:
            self.steps.append((time.time(), line))

    # --- What the engine reads ---------------------------------------------------------------

    def __iter__(self):
        try:
            self.note = f"looking for it on {self._where()}"
            self._patiently(lambda: (self._sweep(), self._place()))  # finding it there again does not repeat it
            # What was sent stays here until the end: an attempt that lost what it made goes again.
            yield from self._follow()
        finally:
            self.close()

    def close(self) -> None:
        shutil.rmtree(self._tmp, ignore_errors=True)

    @property
    def unreachable(self) -> str | None:
        return self.remote.unreachable

    def reconnect(self) -> None:
        self.remote.reconnect()

    def stop_there(self) -> None:
        """Cancel its job in the queue and wait for the queue to let it go, asking again while
        the other machine does not answer: what `Engine.cancel` waits for before the job that
        follows it counts as canceled. Its directory is marked `cancelled` there."""
        with self._submitting:
            self._canceling, job = True, self.job
        if self.dir is None:
            return
        here, queue = f"{self.remote.workdir}/{self.dir}", self.remote.queue
        self.note = f"canceling {f'job {job}' if job else 'it'} in the queue of {self._where()}"
        self._patiently(lambda: self._run(f"cd {here} || exit 0; touch cancelled; "
                                          f"{'' if not job else queue['cancel'].format(id=job) + ' 2>/dev/null'}; :"))
        limit = time.monotonic() + _LET_GO
        while job and not self._stop.is_set() and self._patiently(lambda: self._look(self.dir))["alive"]:
            if time.monotonic() > limit:
                raise RuntimeError(f"job {job} is still in the queue of {self._where()} {_LET_GO:.0f} s after "
                                   f"it was canceled{self._kept()}")
            self._stop.wait(2)
        self._stopped = True

    def ended(self, snapshot) -> None:
        """The job that follows it ended: a campaign canceled here is canceled there, and one
        that is all here is kept for `keep_after` days. Interrupted or failed, it is left as it is."""
        self._stop.set()
        if self.dir is None:
            return
        here, queue = f"{self.remote.workdir}/{self.dir}", self.remote.queue
        try:
            if snapshot.status == "canceled" and not self._stopped:
                self._run(f"cd {here} || exit 0; id=$(cat queue.id 2>/dev/null); "
                          f"[ -z \"$id\" ] || {{ {queue['cancel'].format(id='$id')}; }} 2>/dev/null; touch cancelled")
            elif snapshot.status.startswith("completed"):
                self._run(f"cd {here} && touch ingested" if self.remote.keep_after else
                          f"rm -rf {here} 2>/dev/null || {{ sleep 2; rm -rf {here}; }}")
        except Exception:  # ponytail: what is not tidied up now is when `keep_after` passes
            log.warning("could not tell %s that campaign %s ended", self.remote.remote.address or "this machine",
                        self.dir, exc_info=True)

    # --- Over there ----------------------------------------------------------------------

    def _run(self, command: str, data=b"", timeout: float = 600) -> bytes:
        return self.remote.remote.run(command, data, timeout)

    def _patiently(self, ask):
        """What `ask()` gives, asked again while the other machine does not answer (see
        `RemoteExecutor.unreachable`); any other error, or one when this campaign ended, is raised."""
        while True:
            try:
                made = ask()
            except Exception as error:
                if not unreachable(error):
                    raise
                log.warning("campaign %s: %s does not answer: %s", self.dir, self.remote.remote.address or "this machine", error)
                wait, retries = time.monotonic() + self.remote._reached(error), self.remote.retries
                while time.monotonic() < wait and retries == self.remote.retries and not self._stop.is_set():
                    self._stop.wait(1)
                if self._stop.is_set():
                    raise
                continue
            self.remote._reached()
            return made

    def _sweep(self) -> None:
        minutes = int(self.remote.keep_after * 1440)
        self._run(f"cd {self.remote.workdir} 2>/dev/null || exit 0; for m in c*-*/ingested c*-*/cancelled c*-*/lost; do "
                  f"[ -f \"$m\" ] && [ -n \"$(find \"$m\" -mmin +{minutes})\" ] && rm -rf \"${{m%/*}}\"; done; :")

    def _look(self, dir: str) -> dict:
        """What the attempt in `dir` is: `id`, `told` (its progress), `cancelled`, `alive`, `state`."""
        queue = self.remote.queue
        said = self._run(
            f"cd {self.remote.workdir}/{dir} 2>/dev/null || exit 0; id=$(cat queue.id 2>/dev/null); echo \"id=$id\"; "
            "[ -f cancelled ] && echo cancelled; [ -f lost ] && echo lost; [ -f progress.json ] && echo \"told=$(cat progress.json)\"; "
            f"[ -n \"$id\" ] && {{ {{ {queue['alive'].format(ids='$id')}; }} 2>/dev/null | grep -qx \"$id\" && echo alive; "
            f"echo \"state=$({{ {queue['state'].format(id='$id')}; }} 2>/dev/null)\"; }}; :").decode()
        look = {"id": "", "told": {}, "cancelled": False, "lost": False, "alive": False, "state": ""}
        for line in said.splitlines():
            name, _, value = line.partition("=")
            if name in ("cancelled", "lost", "alive"):
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
            self.job = look["id"]
            if look["state"].startswith("CANCELLED") and not look["cancelled"]:  # by the queue's own command
                self._run(f"touch {self.remote.workdir}/{self.dir}/cancelled")
                look["cancelled"] = True
            # ponytail: one whose runner failed (its code, not its allocation) starts afresh too.
            fresh = look["cancelled"] or look["lost"] or look["told"].get("status") == "failed"
            if not fresh:
                if look["alive"] or self._complete(look["told"]):
                    return
                self._fill()
                self._submit()
                return
            self.n += 1
        self._attempt()

    def _attempt(self) -> None:
        self.dir = f"{self.stem}-{self.n}"
        self.note = f"sending its files to {self._where()}"
        self._upload([name for name in self._local if not self._stored(name)], make=True)
        self._fill()  # and what of the project's folder is not there yet
        self._submit()

    def _stored(self, name: str) -> bool:
        return bool(self._store) and name.startswith(self._store)

    def _upload(self, names: list, make: bool = False) -> None:
        stored = [name[len(self._store):] for name in names if self._stored(name)]
        if stored and not self._rsync(locations.root(self.remote, self.project), stored):
            self._send(locations.root(self.remote, self.project), {path: self._local[self._store + path] for path in stored})
        names = [name for name in names if not self._stored(name)]
        if names or make:
            here = f"{self.remote.workdir}/{self.dir}"
            self._send(here, {name: self._local[name] for name in names}, make)

    def _rsync(self, there: str, paths: list) -> bool:
        """`paths` of the project to its folder `there` with rsync, which goes on from what a cut
        left half sent: False when this machine or the other has no rsync (tar sends them then)."""
        if getattr(self.remote, "_no_rsync", False) or not shutil.which("rsync"):
            return False
        address = self.remote.remote.address
        self._run(f"mkdir -p {shlex.quote(there)}")
        said = subprocess.run(
            ["rsync", "-rtL", "--partial", "--timeout=120", "--files-from=-",
             *(["-e", shlex.join(self.remote.remote.ssh)] if address else []),
             f"{Path(self.project.project_dir).resolve()}/", f"{address}:{there}/" if address else f"{there}/"],
            input="".join(f"{path}\n" for path in paths).encode(), capture_output=True)
        if said.returncode == 0:
            return True
        error = said.stderr.decode(errors="replace").strip()
        if said.returncode in (12, 127) and "rsync" in error and "not found" in error:  # none over there
            self.remote._no_rsync = True
            return False
        raise HostError(f"{error}: {said.returncode}")

    def _send(self, there: str, files: dict, make: bool = True) -> None:
        """`files`, `{path there: path here}`, to the directory `there`."""
        with tempfile.TemporaryFile() as packed:  # on disk: the files of the elements, of any size
            with tarfile.open(fileobj=packed, mode="w", dereference=True) as tar:
                for name, here in files.items():
                    tar.add(here, arcname=name)
            packed.seek(0)
            self._run(f"{'mkdir -p ' + there + ' && ' if make else ''}cd {there} && tar xf -", packed)

    def _fill(self) -> None:
        """What was sent is all there, as it was here: what is missing or not the same goes again.
        What of the project's folder was verified there before is checked by its size, the rest by
        its sha256 (`ms_flow.locations`)."""
        self.note = f"checking the files on {self._where()}"
        host = locations.host_of(self.remote)
        known = locations.known(self.project, host) if self._store else {}
        lines = []
        for line in (self._tmp / "manifest.sha").read_text().splitlines():
            sha, _, name = line.partition("  ")
            size = os.path.getsize(self._local[name])
            seen = self._stored(name) and known.get(name[len(self._store):]) == (size, sha)
            lines.append(f"S {size} {name}" if seen else f"H {sha} {name}")
        listing = "".join(line + "\n" for line in lines).encode()
        for _ in range(2):
            said = self._run(
                f"cd {self.remote.workdir}/{self.dir} && while read -r k a p; do case $k in "
                "S) [ \"$(stat -c %s -- \"$p\" 2>/dev/null)\" = \"$a\" ] || echo \"$p\";; "
                "*) printf '%s  %s\\n' \"$a\" \"$p\" | sha256sum -c --status 2>/dev/null || echo \"$p\";; esac; done",
                listing).decode()
            wrong = [name for name in said.splitlines() if name]
            if not wrong:
                locations.record(self.project, host, {
                    name[len(self._store):]: (os.path.getsize(self._local[name]), self._shas[name])
                    for name in self._shas if self._stored(name)})
                return self._upload(["job.sh"])  # the script of this side, a template that changed too
            log.info("campaign %s: %d files sent again", self.dir, len(wrong))
            self._upload([name for name in wrong if name in self._local] + ["manifest.sha"])
        raise OSError(f"campaign {self.dir}: what was sent is not what arrived: {', '.join(wrong[:5])}")

    def _submit(self) -> None:
        with self._submitting:  # never after `stop_there` read which job to cancel
            if self._canceling:
                return
            self.note = f"submitting it to the queue of {self._where()}"
            self.job = self.remote._queue(f"cd {self.remote.workdir}/{self.dir} && rm -f cancelled")

    def _follow(self):
        """Every round, one command: the progress, whether the job is in the queue, and whatever
        is new of each stage. What comes is given, under a key of its attempt."""
        streamed = [stage.name for stage in self.stages if stage.deliver == "stream"]
        reduced = {stage.name for stage in self.stages if stage.deliver == "reduced"}
        parts, errors = dict.fromkeys(streamed, 0), {stage.name: 0 for stage in self.stages}
        queue, ending, was, again = self.remote.queue, False, None, False
        while not self._stop.is_set():
            asked = " ".join(f"{name}:{parts[name]}" for name in streamed)
            command = (
                f"cd {self.remote.workdir}/{self.dir} 2>/dev/null || exit 0; t=.poll.$$; "
                "cat progress.json > $t.told 2>/dev/null; id=$(cat queue.id 2>/dev/null); "
                f"{{ [ -n \"$id\" ] && {{ {queue['alive'].format(ids='$id')}; }} | grep -x \"$id\"; }} > $t.alive 2>/dev/null; "
                "echo $t.told > $t; echo $t.alive >> $t; "
                f"for a in {asked}; do s=${{a%:*}}; n=${{a#*:}}; ls stages/$s/results 2>/dev/null | "
                "awk -v d=stages/$s/results -v n=$n -F'[-.]' '/^part-/ && $2 + 0 > n {print d \"/\" $0}'; done >> $t; "
                f"for f in stages/*/errors.jsonl {' '.join(f'stages/{name}/reduced.json' for name in sorted(reduced))}; "
                "do [ -f \"$f\" ] && echo \"$f\"; done >> $t; tar cf - -T $t 2>/dev/null; rm -f $t $t.told $t.alive")
            files = self._patiently(lambda: _untar(self._run(command, timeout=self.remote.poll_timeout)))
            told = json.loads(next((data for name, data in files.items() if name.endswith(".told")), b"") or b"{}")
            alive = any(data.strip() for name, data in files.items() if name.endswith(".alive"))
            if not self._canceling:
                self._tell(told, alive)
                if alive and time.monotonic() > self._accounted + _ACCOUNT_EVERY:
                    self._account()
            for name in streamed:
                new = sorted(file for file in files if file.startswith(f"stages/{name}/results/part-"))
                for file in new:
                    for index, result in read_part(io.BytesIO(files[file])):
                        yield {"_key": f"{self.n}:{name}:{index}", "result": result}
                if new:
                    parts[name] = part_number(new[-1])
            for stage in self.stages:
                lines = files.get(f"stages/{stage.name}/errors.jsonl", b"").splitlines()
                for line in lines[errors[stage.name]:]:
                    row = json.loads(line)
                    yield {"_key": f"{self.n}:{stage.name}:e{row['index']}", "error": f"stage {stage.name}: {row['error']}"}
                errors[stage.name] = len(lines)
            for name in sorted(reduced):
                if f"stages/{name}/reduced.json" in files:
                    reduced.discard(name)
                    made = unplain(json.loads(files[f"stages/{name}/reduced.json"]))
                    yield {"_key": f"{self.n}:{name}:reduced", "result": made}
            if self._canceling:  # its end there is `stop_there`'s to see, not a failure to go on from
                self._stop.wait(self.remote.poll_s)
                continue
            # The progress was read before the parts were listed: complete, nothing is left out,
            # unless something removed it there (a scratch purge). Nothing at all: no directory.
            gone = None
            if not files:
                gone = "its directory is no longer there"
            elif self._complete(told):
                left = told.get("left", {})
                lost = [name for name in parts if parts[name] < left.get(name, {}).get("parts", 0)]
                lost += [name for name in errors if errors[name] < left.get(name, {}).get("errors", 0)]
                if not lost and not reduced:
                    self._account(final=True)
                    return
                gone = "it ended there, but what it made is not all there any longer"
            if gone:
                if again:
                    raise RuntimeError(f"campaign {self.dir}: {gone}, twice{self._kept()}")
                log.warning("campaign %s: %s (purged?): it starts again as a new attempt", self.dir, gone)
                self._run(f"cd {self.remote.workdir}/{self.dir} 2>/dev/null && touch lost; "
                          f"[ -z '{self.job}' ] || {{ {queue['cancel'].format(id=self.job)}; }} >/dev/null 2>&1; :")
                self.n += 1
                self._attempt()
                parts, errors = dict.fromkeys(streamed, 0), dict.fromkeys(errors, 0)
                reduced = {stage.name for stage in self.stages if stage.deliver == "reduced"}
                again, ending, was = True, False, None
                continue
            if told.get("status") == "failed":
                self._account()
                raise RuntimeError(f"campaign {self.dir} failed there: {told.get('error')}{self._kept()}")
            if alive:
                ending = False
            elif not ending:  # its last word may have come after this round was read
                ending = True
                continue
            else:
                state = self._patiently(lambda: self._look(self.dir))["state"]
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

    def _account(self, final: bool = False) -> None:
        """`queue_usage`, from the accounting of the queue over there (its `usage` command); with
        `final`, its last word, waited for a while (the job leaves the queue a moment after its
        runner ended; qacct writes it some seconds later). No accounting there: nothing."""
        command = self.remote.queue.get("usage")
        if not command or not self.job:
            return
        if final:
            self.note = "reading what it used from the queue's accounting"
        self._accounted = time.monotonic()
        for _ in range(10 if final else 1):  # ponytail: 30 s at most, then what it said last
            try:
                used = read_usage(self._run(f"{{ {command.format(id=self.job)}; }} 2>/dev/null; :", timeout=60).decode())
            except Exception:  # what it used is told, never what stops the campaign
                log.debug("campaign %s: no accounting of job %s", self.dir, self.job, exc_info=True)
                return
            if used["cpus"] or used["cpu_s"] is not None:
                self.queue_usage = {**used, "job": self.job}
            if used["final"] or not final:
                return
            time.sleep(3)

    def _where(self) -> str:
        return self.remote.remote.address or "this machine"

    def _tell(self, told: dict, alive: bool) -> None:
        """`note` from what the runner over there last said (`progress.json`)."""
        job = f"job {self.job}" if self.job else "its job"
        if str(told.get("status", "")).startswith("completed"):
            self.note = "it ended there: bringing back what it made"
        elif alive and told.get("status") in ("waiting", "running"):
            stage = f"stage {told.get('stage')} ({told.get('stages_done', 0) + 1} of {len(self.stages)}): " \
                if len(self.stages) > 1 else ""
            failed = f", {told['failed']} failed" if told.get("failed") else ""
            self.note = f"running in the queue ({job}): {stage}{told.get('done', 0):,} of {told.get('total', 0):,} done{failed}"
        elif alive and not told:
            self.note = f"waiting in the queue ({job})"
        elif alive:
            self.note = f"in the queue again ({job}): {told.get('done', 0):,} of {told.get('total', 0):,} done before"
        else:
            self.note = f"{job} is not in the queue: looking at why"

    def _kept(self) -> str:
        return self.remote._said(self.dir)
