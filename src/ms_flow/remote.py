"""A remote executor: each of its tasks is a job in the queue of another machine.

The engine hands it elements. It stages them in a directory of that machine, has the queue run
`ms_flow.runner` there and asks every `poll_s` seconds, with one command for all of its jobs,
what the queue says and what the runners have left in their files. An element ends for the
engine as soon as its result is fetched, with the job that computes the others still running.

Nothing listens on the other side: every exchange is a command that ends. A job of the engine
that has a key goes on there with this process gone, and is taken up when it is submitted again.
"""
from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import queue
import re
import shlex
import sys
import tarfile
import tempfile
import threading
import time
import traceback
import uuid
from pathlib import Path

from ms_env import Host, HostError

from ms_flow.engine.executors import CANCELED, DONE, ERROR
from ms_flow.engine.files import Dir, File, swap
from ms_flow.runner import read_part, stage

log = logging.getLogger(__name__)

# One entry for each kind of queue. `submit` runs in the directory of the job and prints its id
# last; `alive` prints the ids, of those in `{ids}` (with commas), that are queued or running;
# `cancel` stops the job `{id}`.
SCHEDULERS = {
    # Its ids are process ids, which the system gives out again: one counts only while it is this.
    "none": {"submit": "setsid nohup sh job.sh > runner.log 2>&1 < /dev/null & echo $!",
             "alive": "ps -ww -o pid=,args= -p {ids} | awk '/job.sh|ms_flow.runner/ {{print $1}}'",
             "cancel": "ps -ww -o args= -p {id} | grep -q -e job.sh -e ms_flow.runner && kill -- -{id}"},
    # ponytail: written from Slurm's manual and never run against a cluster.
    "slurm": {"submit": "sbatch --parsable job.sh", "alive": "squeue -h -o %i -t PD,CF,R,CG,S,RQ,RS -j {ids}",
              "cancel": "scancel {id}"},
}

# What the queue runs, in the directory of the job. A cluster's own asks the queue for what it
# needs (`#SBATCH` lines) and starts Ray on its nodes before the last line.
JOB_SH = "#!/bin/sh\n@setup@\nexec @python@ -m ms_flow.runner . @runner@\n"


class _Task:
    """A task of the engine: the elements it waits for, wherever they are computed."""

    __slots__ = ("tag", "fn", "payloads", "keys", "whole", "single", "cost", "key", "group", "left", "outcome",
                 "canceled")

    def __init__(self, tag, fn, payloads, whole, single, cost, key, keys):
        self.tag, self.fn, self.payloads, self.whole, self.single, self.cost = tag, fn, payloads, whole, single, cost
        # An element is known over there by its key, and without one by nothing: it is nobody else's.
        self.key, self.keys = key, [_name(k) for k in keys] if keys else [uuid.uuid4().hex for _ in payloads]
        self.left = set(range(len(payloads)))  # the elements nothing is known of yet
        self.group = self.outcome = None
        self.canceled = False


class _Queued:
    """A job in the queue and its directory. `waits[i]` is who waits for its element `i`: a task
    and the index it has there, None while nobody asked for it, False once it is answered."""

    __slots__ = ("group", "dir", "id", "keys", "waits", "left", "dirs", "parts", "errors", "done", "ending", "over",
                 "kept")

    def __init__(self, group, dir, keys):
        self.group, self.dir, self.keys, self.waits, self.id = group, dir, keys, [None] * len(keys), None
        self.left = set(range(len(keys)))  # the elements nothing is known of yet
        self.dirs = {}  # where its tasks write over there -> the directory here that it stands for
        self.parts = self.errors = self.done = 0  # the last part fetched, the error lines read, what it had done when sent
        self.ending = self.over = self.kept = False


class _Group:
    """What the other machine has of one job of the engine: directories named `<stem>-<n>`."""

    __slots__ = ("stem", "n", "where", "stash", "done")

    def __init__(self, stem):
        self.stem, self.n, self.done = stem, 0, False  # `done`: nobody will ask it for anything else
        self.where = {}  # the key of an element that is there, not answered -> its job and index
        # ponytail: the results that came with nobody waiting, kept until asked for or the job ends.
        # After reopening that is every result the jobs taken up have, the ones written before
        # closing too. Leave them over there and read them when asked if a job's results are heavy.
        self.stash = {}


def _name(key) -> str:
    return key.hex() if isinstance(key, (bytes, bytearray)) else str(key)


class RemoteExecutor:
    """`host` and `workdir` say where (a relative `workdir` is under the home of the account);
    `scheduler` names an entry of `SCHEDULERS`; `template` is the text of the script the queue
    runs, with `@python@`, `@setup@` (the lines that prepare the environment) and `@runner@`
    (arguments for the runner, like `--ray auto`) filled in. `elements_per_job` is how many
    elements go in each job of the queue, all of them by default, and `max_jobs` how many tasks
    of the engine it holds at once, any number by default.

    The function has to be importable on the other side, and an element that fails and is tried
    again goes in a job of its own. Each `File` in an element travels with the job, once however
    many elements name it, and what a result names under a `Dir` comes back before the result is
    given, unless `shared` says that the other side sees the disk of this one (as it does without
    a `host`).

    What it has there of a job with a key (see `outlives` in `ms_flow.engine.executors`) stays
    until the engine says the job ended: closing leaves it running, and the job submitted again
    with its key, from this process or another, waits for the elements that are already there
    instead of sending them. A job without a key is stopped and removed when it is canceled or
    the engine closes.
    """

    # ponytail: no `kill` (no time limit: the queue has its own) and no `progress` inside an element.
    uses_tokens, wants_cost, outlives = False, True, True

    def __init__(self, host: str | None = None, *, workdir: str = "ms_flow_jobs", scheduler: str = "none",
                 template: str = JOB_SH, python: str = "python", setup: str = "", runner: str = "",
                 elements_per_job: int | None = None, max_jobs: int | None = None, poll_s: float = 30,
                 part_seconds: float = 30, shared: bool | None = None):
        self.remote, self.workdir, self.queue = Host(host), shlex.quote(workdir), SCHEDULERS[scheduler]
        self.script = template.replace("@python@", python).replace("@setup@", setup).replace("@runner@", runner)
        self.capacity, self.batch = max_jobs or sys.maxsize, elements_per_job or sys.maxsize
        self.poll_s, self.part_seconds, self.shared = poll_s, part_seconds, host is None if shared is None else shared
        self._inbox, self._wake, self._closed = queue.SimpleQueue(), threading.Event(), False
        # The watching thread's own: the tasks not answered, the jobs it has there, the groups of
        # the jobs with a key, and what is to be run there at the end of a round.
        self._tasks, self._jobs, self._groups, self._orders = set(), [], {}, []
        self._thread = threading.Thread(target=self._watch, name="msflow-remote", daemon=True)
        self._thread.start()

    # --- What the engine calls: nothing here waits for the other machine ---------------

    def bind(self, on_done) -> None:
        self._on_done = on_done

    def bind_parts(self, on_part) -> None:
        self._on_part = on_part

    def submit(self, fn, payload, tag, cost=(1, 0, 0), key=None, keys=None):
        return self._tell(_Task(tag, fn, [payload], False, True, cost, key, keys))

    def submit_many(self, fn, payloads, tag, whole=False, cost=(1, 0, 0), key=None, keys=None):
        return self._tell(_Task(tag, fn, payloads, whole, False, cost, key, keys))

    def cancel(self, handle: _Task) -> None:
        handle.canceled = True
        self._wake.set()

    def ended(self, key: str, kept: bool) -> None:
        self._tell((key, kept))

    def _tell(self, what):
        self._inbox.put(what)
        self._wake.set()
        return what

    def shutdown(self) -> None:
        self._closed = True
        self._wake.set()
        self._thread.join(5)  # ponytail: a command that is taking longer is left to end by itself

    # --- For whoever looks after the other machine: these wait for it, so not for the engine ---

    def jobs_there(self) -> list[dict]:
        """What the account has in `workdir` over there, whichever process sent it: for each job
        of the queue its `dir`, the `key` of the job of the engine it belongs to (None without
        one), `fn`, `total`, `done`, `failed`, `status` (what its runner last said, `at` that
        time), `sent_at`, whether it is `in_queue`, and whether this executor is waiting for it
        (`followed`)."""
        found = {}
        for name, data in _untar(self.remote.run(
                f"cd {self.workdir} 2>/dev/null || exit 0; f=$(ls */spec.json */record.json */queue.id "
                "*/progress.json */failed 2>/dev/null); [ -z \"$f\" ] || tar cf - $f")).items():
            folder, _, file = name.partition("/")
            found.setdefault(folder, {})[file] = data
        ids = {folder: _job_id(has["queue.id"]) for folder, has in found.items() if has.get("queue.id", b"").strip()}
        alive = self.queue["alive"].format(ids=",".join(ids.values()))
        in_queue = set(self.remote.run(f"{{ {alive}; }} 2>/dev/null || :").decode().split()) if ids else set()
        followed = {queued.dir for queued in tuple(self._jobs)}
        rows = []
        for folder, has in sorted(found.items()):
            spec, record, told = (json.loads(has.get(file) or b"{}") for file in ("spec.json", "record.json", "progress.json"))
            rows.append({"dir": folder, "key": record.get("key"), "fn": (spec.get("fn") or "")[:200],
                         "total": spec.get("total"), "done": told.get("done", 0), "failed": told.get("failed", 0),
                         "status": "failed" if "failed" in has else told.get("status", "waiting"), "at": told.get("at"),
                         "sent_at": record.get("at"), "in_queue": ids.get(folder) in in_queue,
                         "followed": folder in followed})
        return rows

    def remove_there(self, dir: str) -> None:
        """Take the job in `dir` (as `jobs_there` names it) out of the queue and remove what it
        has there. Not one this executor is waiting for: that is for canceling its job."""
        if not re.fullmatch(r"[0-9a-f]+(-\d+)?", dir):
            raise ValueError(f"not a directory of a job: {dir!r}")
        if dir in {queued.dir for queued in tuple(self._jobs)}:
            raise ValueError(f"{dir} belongs to a job that is running here: cancel the job")
        said = self.remote.run(f"cat {self.workdir}/{dir}/queue.id 2>/dev/null || :")
        stop = self.queue["cancel"].format(id=_job_id(said)) if said.strip() else ":"
        self.remote.run(f"cd {self.workdir} || exit 1; {stop}; rm -rf {dir} 2>/dev/null || {{ sleep 2; rm -rf {dir}; }}")

    # --- The one thread that talks to the other machine --------------------------------

    def _watch(self) -> None:
        due = 0.0
        while True:
            live = [queued for queued in self._jobs if not queued.over]
            woken = self._wake.wait(max(0.0, due - time.monotonic()) if live else None)
            self._wake.clear()
            closed = self._closed  # read first: what the engine said before closing is still done
            for task in [task for task in self._tasks if task.canceled]:
                self._cancel(task)
            while not self._inbox.empty():
                what = self._inbox.get()
                try:
                    if not isinstance(what, _Task):
                        self._ended(*what)
                    elif what.canceled or closed:
                        self._on_done(what.tag, CANCELED, None, None, 0)
                    else:
                        self._accept(what)
                except Exception:
                    log.warning("could not reach %s", self.remote.address or "this machine", exc_info=True)
            live = [queued for queued in self._jobs if not queued.over]
            if live and not closed and (time.monotonic() >= due or (woken and any(q.ending for q in live))):
                due = time.monotonic() + self.poll_s
                try:
                    self._poll(live)
                except Exception:  # the network, most likely: the next round asks again
                    log.warning("could not ask %s about its jobs", self.remote.address or "this machine", exc_info=True)
            if closed:  # what only waited for its job to leave the queue by itself waits no longer
                for group in {queued.group for queued in self._jobs if queued.group.done}:
                    self._stop(group, now=True)
            self._sweep()
            if closed:
                return

    def _accept(self, task: _Task) -> None:
        """Take a task: each element is waited for where it already is, and sent if it is nowhere."""
        try:
            group = task.group = self._group(task.key)
            fresh = []
            for index, key in enumerate(task.keys):
                queued, at = group.where.get(key, (None, None))
                if queued is not None and queued.waits[at] is None:
                    queued.waits[at] = (task, index)
                elif key not in group.stash:
                    fresh.append(index)
            if fresh:
                self._send(task, group, fresh)
        except Exception:
            self._tasks.discard(task)
            for queued in self._jobs:  # whatever it had got to wait for is free again
                queued.waits = [None if waits and waits[0] is task else waits for waits in queued.waits]
            return self._over(task, ERROR, None, "the job could not be sent\n" + traceback.format_exc())
        self._tasks.add(task)
        task.payloads = task.fn = None  # they are over there now
        for index, key in enumerate(task.keys):
            if key in group.stash:
                self._end(task, index, DONE, group.stash.pop(key), None)

    def _group(self, key) -> _Group:
        if key is None:
            return _Group(uuid.uuid4().hex[:12])
        group = self._groups.get(key)
        if group is None:
            group = self._take_up(_Group(hashlib.blake2b(key.encode(), digest_size=8).hexdigest()))
            self._groups[key] = group
        return group

    def _take_up(self, group: _Group) -> _Group:
        """Find the jobs an earlier run left there for this group. One command."""
        files = _untar(self.remote.run(
            f"cd {self.workdir} 2>/dev/null || exit 0; f=$(ls {group.stem}-*/record.json {group.stem}-*/queue.id "
            f"{group.stem}-*/errors.jsonl {group.stem}-*/failed 2>/dev/null); [ -z \"$f\" ] || tar cf - $f"))
        for name in sorted(files):
            folder, _, file = name.partition("/")
            group.n = max(group.n, int(folder.rpartition("-")[2]) + 1)
            if file != "record.json" or f"{folder}/failed" in files:  # a job given up is only looked at
                continue
            if f"{folder}/queue.id" not in files:  # it never got to the queue
                self._orders.append(f"rm -rf {folder};")
                continue
            record = json.loads(files[name])
            queued = _Queued(group, folder, record["keys"])
            queued.id, queued.dirs = _job_id(files[f"{folder}/queue.id"]), record["dirs"]
            # What failed there is not waited for: asked for again, it is run again.
            failed = files.get(f"{folder}/errors.jsonl", b"").splitlines()
            queued.errors = len(failed)
            for index in {json.loads(line)["index"] for line in failed}:
                queued.waits[index] = False
                queued.left.discard(index)
            for index in queued.left:
                group.where[queued.keys[index]] = (queued, index)
            self._jobs.append(queued)
        return group

    def _send(self, task: _Task, group: _Group, indexes: list) -> None:
        queued = _Queued(group, f"{group.stem}-{group.n}", [task.keys[index] for index in indexes])
        with tempfile.TemporaryDirectory() as tmp:
            cpu, gpu, mem_mb = task.cost
            payloads, carried = [task.payloads[index] for index in indexes], {}

            def carry(mark):
                if isinstance(mark, Dir):
                    there = next((there for there, here in queued.dirs.items() if here == mark), f"out/{len(queued.dirs)}")
                    queued.dirs[there] = str(mark)
                    return Dir(there)
                if mark not in carried:
                    carried[mark] = f"files/{len(carried)}/{os.path.basename(mark)}"
                return File(carried[mark])

            if not self.shared:
                payloads = [swap(payload, carry) for payload in payloads]
            stage(tmp, task.fn, payloads, batched=task.whole, part_seconds=self.part_seconds,
                  cpu=cpu, gpu=gpu, mem_mb=mem_mb)
            Path(tmp, "job.sh").write_text(self.script)
            # What another run needs to take this job up: whose it is, its elements in order, its dirs.
            Path(tmp, "record.json").write_text(json.dumps({"key": task.key, "keys": queued.keys, "dirs": queued.dirs,
                                                            "at": time.time()}))
            where = f"{self.workdir}/{queued.dir}"
            with tempfile.TemporaryFile() as packed:  # on disk: the job and its files, of any size
                with tarfile.open(fileobj=packed, mode="w", dereference=True) as tar:
                    tar.add(tmp, arcname=".")
                    for here, there in carried.items():
                        tar.add(here, arcname=there)
                packed.seek(0)
                group.n += 1
                queued.id = self._queue(f"mkdir -p {where} && cd {where} && tar xf -", packed)
        for at, index in enumerate(indexes):
            queued.waits[at] = (task, index)
            group.where[queued.keys[at]] = (queued, at)
        self._jobs.append(queued)

    def _queue(self, first: str, data=b"") -> str:
        # In `$( )`: a `&` in what submits must not send what comes before it to the background too.
        return _job_id(self.remote.run(
            f"{first} && id=$({self.queue['submit']}) && echo \"$id\" > queue.id && echo \"$id\"", data))

    def _cancel(self, task: _Task) -> None:
        self._tasks.discard(task)
        for queued in self._jobs:
            if queued.group is task.group:
                queued.waits = [None if waits and waits[0] is task else waits for waits in queued.waits]
        if task.key is None:
            self._stop(task.group)
        self._over(task, CANCELED, None, None)

    def _ended(self, key: str, kept: bool) -> None:
        if kept:  # it goes on there; here it is forgotten until its key comes again
            group = self._groups.pop(key, None)
            self._jobs = [queued for queued in self._jobs if queued.group is not group]
        else:  # whatever there is of it there, from this run or an earlier one
            self._stop(self._group(key))
            del self._groups[key]

    def _stop(self, group: _Group, now: bool = False) -> None:
        """Nobody will ask this group for anything else: its jobs leave the queue and their
        directories go, at once if something was left to compute (or `now`) and when they end
        by themselves if not."""
        group.done = True
        for queued in self._jobs:
            if queued.group is group and not queued.over and (now or queued.left):
                self._orders.append(f"{self.queue['cancel'].format(id=queued.id)};")
                queued.over = True

    def _sweep(self) -> None:
        for queued in self._jobs:
            if queued.over and queued.group.done and not queued.kept:
                # A runner that is being stopped may write once more while its directory is removed.
                self._orders.append(f"rm -rf {queued.dir} 2>/dev/null || {{ sleep 2; rm -rf {queued.dir}; }};")
        self._jobs = [queued for queued in self._jobs if not (queued.over and queued.group.done)]
        if self._orders:
            orders, self._orders = self._orders, []
            try:
                self.remote.run(f"cd {self.workdir} || exit 1; " + " ".join(orders) + " :")
            except Exception:  # ponytail: tried again at the end of the next round, whenever that is
                self._orders = orders + self._orders
                log.warning("could not tidy up on %s", self.remote.address or "this machine", exc_info=True)

    def _poll(self, jobs: list) -> None:
        # One command: who is in the queue, and a tar of every progress file, list of errors and
        # part of results that is new. A part is listed only once it has its name, so it is whole.
        alive = self.queue["alive"].format(ids=",".join(queued.id for queued in jobs))
        asked = " ".join(f"{queued.dir}:{queued.parts}" for queued in jobs)
        files = _untar(self.remote.run(
            f"cd {self.workdir} || exit 1; t=.poll.$$; {{ {alive}; }} > $t.alive 2>/dev/null; echo $t.alive > $t; "
            f"for a in {asked}; do d=${{a%:*}}; n=${{a#*:}}; "
            "ls $d/results 2>/dev/null | awk -v d=$d -v n=$n -F'[-.]' "
            "'/^part-/ && $2 + 0 > n {print d \"/results/\" $0}'; "
            "for f in $d/progress.json $d/errors.jsonl; do [ -f $f ] && echo $f; done; done >> $t; "
            "tar cf - -T $t 2>/dev/null; rm -f $t $t.alive"))
        in_queue = set(next(data for name, data in files.items() if name.endswith(".alive")).decode().split())
        for queued in jobs:
            self._read(queued, files, queued.id in in_queue)

    def _read(self, queued: _Queued, files: dict, in_queue: bool) -> None:
        told = json.loads(files.get(f"{queued.dir}/progress.json") or b"{}")
        parts = sorted(name for name in files if name.startswith(f"{queued.dir}/results/"))
        if parts and queued.dirs and "home" not in told:
            return  # not yet known where its tasks write: the next round reads these parts
        pairs = [pair for name in parts for pair in read_part(io.BytesIO(files[name]))]
        if pairs and queued.dirs:
            pairs = self._bring(queued, told["home"], pairs)
        for index, result in pairs:
            self._answer(queued, index, DONE, result, None)
        if parts:
            queued.parts = max(queued.parts, int(parts[-1][-9:-4]))
        lines = files.get(f"{queued.dir}/errors.jsonl", b"").splitlines()
        for line in lines[queued.errors:]:
            row = json.loads(line)
            self._answer(queued, row["index"], ERROR, None, row["error"])
        queued.errors = len(lines)
        over = told.get("status") in ("completed", "completed_with_errors", "failed")
        if in_queue and not over:
            queued.ending = False
        elif not queued.left:
            queued.over = True
        elif not queued.ending:  # its last part may have got its name while this round was being read
            queued.ending = True
            self._wake.set()
        elif not over and told.get("done", 0) > queued.done:
            # Out of the queue before its end (its time ran out) and further than it was: once more,
            # and the runner goes on from its journal.
            queued.done, queued.ending = told["done"], False
            queued.id = self._queue(f"cd {self.workdir}/{queued.dir}")
        else:
            why = told.get("error") or "the job left the queue before its end, no further than when it was sent"
            why += (f"\nits directory is kept: {self.workdir}/{queued.dir} on {self.remote.address or 'this machine'} "
                    "(see runner.log)")
            queued.over = queued.kept = True
            self._orders.append(f"touch {queued.dir}/failed;")  # so that no later run waits for it
            for index in tuple(queued.left):
                self._answer(queued, index, ERROR, None, why)

    def _bring(self, queued: _Queued, home: str, pairs: list) -> list:
        """The results with the paths they have here, once what they name is here."""
        root, wanted = f"{home}/out/", set()

        def here(text):
            if not text.startswith(root):
                return text
            n, _, rest = text[len(root):].partition("/")
            wanted.add(f"{queued.dir}/out/{n}/{rest}")
            return os.path.join(queued.dirs[f"out/{n}"], rest)

        pairs = [(index, swap(result, here, str)) for index, result in pairs]
        if wanted:
            # What a result names and is not there is the task's to explain, not a reason to stop.
            packed = self.remote.run(f"cd {self.workdir} && tar cf - --ignore-failed-read -T - 2>/dev/null || :",
                                     "\n".join(sorted(wanted)).encode())
            for name, data in _untar(packed).items():
                n, _, rest = name[len(queued.dir) + 5:].partition("/")
                if ".." in rest.split("/"):
                    raise HostError(f"a file that would land outside its directory: {name}")
                target = Path(queued.dirs[f"out/{n}"], rest)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data)
        return pairs

    def _answer(self, queued: _Queued, index: int, status: str, result, error) -> None:
        """An element of a job of the queue ended: for whoever waits for it, or for whoever asks later."""
        if index not in queued.left:  # a part that a killed run wrote twice says it twice
            return
        queued.left.discard(index)
        key, waits, group = queued.keys[index], queued.waits[index], queued.group
        queued.waits[index] = False
        if group.where.get(key) == (queued, index):
            del group.where[key]
        if waits:
            self._end(*waits, status, result, error)
        elif status == DONE and not group.done:
            group.stash[key] = result

    def _end(self, task: _Task, index: int, status: str, result, error) -> None:
        task.left.discard(index)
        if task.single:
            task.outcome = (status, result, error)
        else:
            self._on_part(task.tag, index, status, result, error)
        if not task.left:
            self._tasks.discard(task)
            if task.key is None:
                self._stop(task.group)
            self._over(task, *(task.outcome or (DONE, (), None)))  # of several, each one ended ahead

    def _over(self, task: _Task, status: str, result, error) -> None:
        tag, task.tag = task.tag, None
        if tag is not None:
            self._on_done(tag, status, result, error, 0)


def _job_id(said: bytes) -> str:
    return said.split()[-1].decode().split(";")[0]  # Slurm may print `id;cluster`


def _untar(data: bytes) -> dict:
    if not data:
        return {}
    with tarfile.open(fileobj=io.BytesIO(data)) as tar:
        return {member.name: tar.extractfile(member).read() for member in tar if member.isfile()}
