"""A remote executor: each of its tasks is a job in the queue of another machine.

The engine hands it elements. It stages them in a directory of that machine, has the queue run
`ms_flow.runner` there and asks every `poll_s` seconds, with one command for all of its jobs,
what the queue says and what the runners have left in their files. An element ends for the
engine as soon as its result is fetched, with the job that computes the others still running.

Nothing listens on the other side: every exchange is a command that ends. Shutting the executor
down does not stop what is queued there.
"""
from __future__ import annotations

import io
import json
import logging
import queue
import shlex
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import traceback
import uuid
from pathlib import Path

from ms_flow.engine.executors import CANCELED, DONE, ERROR
from ms_flow.runner import read_part, stage

log = logging.getLogger(__name__)

# One entry for each kind of queue. `submit` runs in the directory of the job and prints its id
# last; `alive` prints the ids, of those in `{ids}` (with commas), that are queued or running;
# `cancel` stops the job `{id}`.
SCHEDULERS = {
    "none": {"submit": "setsid nohup sh job.sh > runner.log 2>&1 < /dev/null & echo $!",
             "alive": "ps -o pid= -p {ids}", "cancel": "kill -- -{id}"},
    # ponytail: written from Slurm's manual and never run against a cluster.
    "slurm": {"submit": "sbatch --parsable job.sh", "alive": "squeue -h -o %i -t PD,CF,R,CG,S,RQ,RS -j {ids}",
              "cancel": "scancel {id}"},
}

# What the queue runs, in the directory of the job. A cluster's own asks the queue for what it
# needs (`#SBATCH` lines) and starts Ray on its nodes before the last line.
JOB_SH = "#!/bin/sh\n@setup@\nexec @python@ -m ms_flow.runner . @runner@\n"


class RemoteError(RuntimeError):
    pass


class Remote:
    """A machine, known by what running a command on it gives back: `host` as `ssh` knows it (its
    keys, users and jumps are in `~/.ssh/config`), or None for this one."""

    def __init__(self, host: str | None = None, ssh=("ssh", "-o", "BatchMode=yes")):
        self.host, self.ssh = host, tuple(ssh)

    def run(self, command: str, data: bytes = b"", timeout: float = 600) -> bytes:
        argv = ["sh", "-c", command] if self.host is None else [*self.ssh, self.host, command]
        done = subprocess.run(argv, input=data, capture_output=True, timeout=timeout)
        if done.returncode:
            said = done.stderr.decode(errors="replace").strip()
            if "Permission denied" in said:  # BatchMode: ssh does not ask, and here nobody could answer
                said += f"\nssh has to log in to {self.host} with a key, without asking: see ssh-copy-id"
            raise RemoteError(f"`{command[:200]}` on {self.host or 'this machine'}: {said or done.returncode}")
        return done.stdout


class _Sent:
    """A task of the engine as a job in the queue."""

    __slots__ = ("tag", "fn", "payloads", "whole", "single", "cost", "dir", "id", "parts", "errors", "left",
                 "done", "ending", "canceled", "outcome")

    def __init__(self, tag, fn, payloads, whole, single, cost):
        self.tag, self.fn, self.payloads, self.whole, self.single, self.cost = tag, fn, payloads, whole, single, cost
        self.dir = self.id = self.outcome = None
        self.parts = self.errors = self.done = 0  # the last part fetched, the error lines read, what it had done when sent
        self.left = set(range(len(payloads)))  # the elements nothing is known of yet
        self.ending = self.canceled = False


class RemoteExecutor:
    """`host` and `workdir` say where (a relative `workdir` is under the home of the account);
    `scheduler` names an entry of `SCHEDULERS`; `template` is the text of the script the queue
    runs, with `@python@`, `@setup@` (the lines that prepare the environment) and `@runner@`
    (arguments for the runner, like `--ray auto`) filled in. `elements_per_job` is how many
    elements go in each job of the queue, all of them by default, and `max_jobs` how many of its
    jobs it has there at once, any number by default.

    The function has to be importable on the other side, and an element that fails and is tried
    again goes in a job of its own.
    """

    # ponytail: no `kill` (no time limit: the queue has its own) and no `progress` inside an element.
    uses_tokens, wants_cost = False, True

    def __init__(self, host: str | None = None, *, workdir: str = "ms_flow_jobs", scheduler: str = "none",
                 template: str = JOB_SH, python: str = "python", setup: str = "", runner: str = "",
                 elements_per_job: int | None = None, max_jobs: int | None = None, poll_s: float = 30,
                 part_seconds: float = 30):
        self.remote, self.workdir, self.queue = Remote(host), shlex.quote(workdir), SCHEDULERS[scheduler]
        self.script = template.replace("@python@", python).replace("@setup@", setup).replace("@runner@", runner)
        self.capacity, self.batch = max_jobs or sys.maxsize, elements_per_job or sys.maxsize
        self.poll_s, self.part_seconds = poll_s, part_seconds
        self._inbox, self._wake, self._closed = queue.SimpleQueue(), threading.Event(), False
        self._thread = threading.Thread(target=self._watch, name="msflow-remote", daemon=True)
        self._thread.start()

    # --- What the engine calls: nothing here waits for the other machine ---------------

    def bind(self, on_done) -> None:
        self._on_done = on_done

    def bind_parts(self, on_part) -> None:
        self._on_part = on_part

    def submit(self, fn, payload, tag, cost=(1, 0, 0)):
        return self._take(_Sent(tag, fn, [payload], False, True, cost))

    def submit_many(self, fn, payloads, tag, whole=False, cost=(1, 0, 0)):
        return self._take(_Sent(tag, fn, payloads, whole, False, cost))

    def _take(self, sent: _Sent) -> _Sent:
        self._inbox.put(sent)
        self._wake.set()
        return sent

    def cancel(self, handle: _Sent) -> None:
        handle.canceled = True
        self._wake.set()

    def shutdown(self) -> None:
        self._closed = True
        self._wake.set()
        self._thread.join(5)  # ponytail: a command that is taking longer is left to end by itself

    # --- The one thread that talks to the other machine --------------------------------

    def _watch(self) -> None:
        jobs, due = [], 0.0
        while True:
            woken = self._wake.wait(max(0.0, due - time.monotonic()) if jobs else None)
            self._wake.clear()
            if self._closed:
                return
            while not self._inbox.empty():
                sent = self._inbox.get()
                if self._send(sent):
                    jobs.append(sent)
            if jobs and (time.monotonic() >= due or (woken and any(s.canceled or s.ending for s in jobs))):
                due = time.monotonic() + self.poll_s
                try:
                    self._poll(jobs)
                except Exception:  # the network, most likely: the next round asks again
                    log.warning("could not ask %s about its jobs", self.remote.host or "this machine", exc_info=True)
                jobs = [sent for sent in jobs if sent.tag is not None]

    def _send(self, sent: _Sent) -> bool:
        if sent.canceled:
            self._on_done(sent.tag, CANCELED, None, None, 0)
            return False
        try:
            with tempfile.TemporaryDirectory() as tmp:
                cpu, gpu, mem_mb = sent.cost
                stage(tmp, sent.fn, sent.payloads, batched=sent.whole, part_seconds=self.part_seconds,
                      cpu=cpu, gpu=gpu, mem_mb=mem_mb)
                Path(tmp, "job.sh").write_text(self.script)
                packed = io.BytesIO()
                with tarfile.open(fileobj=packed, mode="w") as tar:
                    tar.add(tmp, arcname=".")
            sent.dir = uuid.uuid4().hex[:12]
            where = f"{self.workdir}/{sent.dir}"
            sent.id = self._queue(f"mkdir -p {where} && cd {where} && tar xf -", packed.getvalue())
            sent.payloads = sent.fn = None  # they are over there now
            return True
        except Exception:
            self._on_done(sent.tag, ERROR, None, "the job could not be sent\n" + traceback.format_exc(), 0)
            return False

    def _queue(self, first: str, data: bytes = b"") -> str:
        # In braces: a `&` in what submits must not send what comes before it to the background too.
        said = self.remote.run(f"{first} && {{ {self.queue['submit']}; }}", data)
        return said.split()[-1].decode().split(";")[0]  # Slurm may print `id;cluster`

    def _poll(self, jobs: list) -> None:
        stopped = [sent for sent in jobs if sent.canceled]
        if stopped:
            # A runner that is being stopped may write once more while its directory is removed.
            self.remote.run(f"cd {self.workdir} || exit 1; " + " ".join(
                f"{self.queue['cancel'].format(id=sent.id)}; rm -rf {sent.dir} 2>/dev/null || {{ sleep 2; rm -rf {sent.dir}; }};"
                for sent in stopped) + " :")
            for sent in stopped:
                self._over(sent, CANCELED, None, None)
        jobs = [sent for sent in jobs if sent.tag is not None]
        if not jobs:
            return
        # One command: who is in the queue, and a tar of every progress file, list of errors and
        # part of results that is new. A part is listed only once it has its name, so it is whole.
        alive = self.queue["alive"].format(ids=",".join(sent.id for sent in jobs))
        asked = " ".join(f"{sent.dir}:{sent.parts}" for sent in jobs)
        files = _untar(self.remote.run(
            f"cd {self.workdir} || exit 1; t=.poll.$$; {{ {alive}; }} > $t.alive 2>/dev/null; echo $t.alive > $t; "
            f"for a in {asked}; do d=${{a%:*}}; n=${{a#*:}}; "
            "ls $d/results 2>/dev/null | awk -v d=$d -v n=$n -F'[-.]' "
            "'/^part-/ && $2 + 0 > n {print d \"/results/\" $0}'; "
            "for f in $d/progress.json $d/errors.jsonl; do [ -f $f ] && echo $f; done; done >> $t; "
            "tar cf - -T $t 2>/dev/null; rm -f $t $t.alive"))
        queued = set(next(data for name, data in files.items() if name.endswith(".alive")).decode().split())
        gone = []
        for sent in jobs:
            self._read(sent, files, sent.id in queued, gone)
        if gone:
            self.remote.run(f"cd {self.workdir} && rm -rf {' '.join(gone)}")

    def _read(self, sent: _Sent, files: dict, queued: bool, gone: list) -> None:
        for name in sorted(name for name in files if name.startswith(f"{sent.dir}/results/")):
            for index, result in read_part(io.BytesIO(files[name])):
                self._end(sent, index, DONE, result, None)
            sent.parts = max(sent.parts, int(name[-9:-4]))
        lines = files.get(f"{sent.dir}/errors.jsonl", b"").splitlines()
        for line in lines[sent.errors:]:
            row = json.loads(line)
            self._end(sent, row["index"], ERROR, None, row["error"])
        sent.errors = len(lines)
        told = json.loads(files.get(f"{sent.dir}/progress.json") or b"{}")
        over = told.get("status") in ("completed", "completed_with_errors", "failed")
        if queued and not over:
            sent.ending = False
        elif not sent.left:
            gone.append(sent.dir)
            self._over(sent, DONE, None, None)
        elif not sent.ending:  # its last part may have got its name while this round was being read
            sent.ending = True
            self._wake.set()
        elif not over and told.get("done", 0) > sent.done:
            # Out of the queue before its end (its time ran out) and further than it was: once more,
            # and the runner goes on from its journal.
            sent.done, sent.ending = told["done"], False
            sent.id = self._queue(f"cd {self.workdir}/{sent.dir}")
        else:
            why = told.get("error") or "the job left the queue before its end, no further than when it was sent"
            self._over(sent, ERROR, None, f"{why}\nits directory is kept: {self.workdir}/{sent.dir} "
                                          f"on {self.remote.host or 'this machine'} (see runner.log)")

    def _end(self, sent: _Sent, index: int, status: str, result, error) -> None:
        if index in sent.left:  # a part that a killed run wrote twice says it twice
            sent.left.discard(index)
            if sent.single:
                sent.outcome = (status, result, error)
            else:
                self._on_part(sent.tag, index, status, result, error)

    def _over(self, sent: _Sent, status: str, result, error) -> None:
        if status == DONE:
            status, result, error = sent.outcome if sent.single else (DONE, (), None)  # each one ended ahead
        tag, sent.tag = sent.tag, None
        self._on_done(tag, status, result, error, 0)


def _untar(data: bytes) -> dict:
    with tarfile.open(fileobj=io.BytesIO(data)) as tar:
        return {member.name: tar.extractfile(member).read() for member in tar if member.isfile()}
