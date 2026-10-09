"""Journal: what the engine remembers on disk. One SQLite file, one owner, one writer thread.

The engine loop only appends records to a queue. The writer thread puts them on disk, one
transaction per interval, so a crash loses at most the last interval: those chunks count
as not done and run again. A chunk is written once, when it ends, and never its payload.
"""
from __future__ import annotations

import logging
import os
import socket
import sqlite3
import threading
import time
import traceback
from collections import deque
from contextlib import closing
from pathlib import Path

log = logging.getLogger(__name__)

VERSION = 2
CHUNK_DONE, CHUNK_FAILED = 1, 2
INTERRUPTED = "interrupted"
RESUMABLE = (INTERRUPTED, "canceled")  # the job statuses that keep their done chunks: these continue

_SCHEMA = """
CREATE TABLE jobs (
    n INTEGER PRIMARY KEY,        -- short number, the key of `chunks`
    job_id TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    spec TEXT NOT NULL,           -- JSON: executor, function, options
    total INTEGER, done INTEGER NOT NULL, failed INTEGER NOT NULL,
    error TEXT, started_at REAL, finished_at REAL,
    source TEXT                   -- JSON: what its source asked to keep, given back when it continues
);
CREATE TABLE chunks (
    job INTEGER NOT NULL, key BLOB NOT NULL,
    status INTEGER NOT NULL, attempt INTEGER NOT NULL, error TEXT,
    PRIMARY KEY (job, key)
) WITHOUT ROWID;
"""
_JOB_COUNTERS = ("UPDATE jobs SET status = ?, total = ?, done = ?, failed = ?, error = ?, finished_at = ?, source = ? "
                 "WHERE n = ?")


class Busy(RuntimeError):
    """Another process owns the file."""


def own(path):
    """Take the operating system lock on `path` and keep it for as long as the returned
    file stays open. The kernel frees it when the process dies, so no lock is ever orphaned.
    """
    handle = open(path, "a+")
    try:
        if os.name == "nt":  # ponytail: written from the documentation, never run on Windows
            import msvcrt

            handle.seek(4096)  # a byte past the text, so the owner's name stays readable
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.seek(0)
        owner = handle.read().strip() or "another process"
        handle.close()
        raise Busy(f"{path} is owned by {owner}") from None
    handle.seek(0)
    handle.truncate()
    handle.write(f"pid {os.getpid()} on {socket.gethostname()}")  # for the message only
    handle.flush()
    return handle


def _connect(path) -> sqlite3.Connection:
    db = sqlite3.connect(path, isolation_level=None)
    db.execute("PRAGMA journal_mode = WAL")
    db.execute("PRAGMA synchronous = NORMAL")
    return db


def _read(path, query: str, args=()) -> list[dict]:
    """Rows for whoever watches: read without owning the file, so another process may be writing
    it. Nothing when no journal of this version is there."""
    if not Path(path).exists():
        return []
    with closing(sqlite3.connect(f"{Path(path).resolve().as_uri()}?mode=ro", uri=True)) as db:
        if db.execute("PRAGMA user_version").fetchone()[0] != VERSION:
            return []
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute(query, args)]


def recorded(path, limit: int | None = None) -> list[dict]:
    """The jobs on record in the journal at `path`, oldest first; with `limit`, the last ones.

    The counters of a job that is running are at most one write interval old.
    """
    return _read(path, "SELECT * FROM jobs ORDER BY n DESC LIMIT ?", (-1 if limit is None else limit,))[::-1]


def failures(path, job_id: str, limit: int = 200) -> list[dict]:
    """`attempt` and `error` of the chunks of a recorded job that failed, at most `limit`.

    ponytail: the journal keeps the key of a chunk, never its payload, so which input failed
    is only known if the error says it.
    """
    return _read(path, "SELECT attempt, error FROM chunks WHERE job = (SELECT n FROM jobs WHERE job_id = ?) "
                       "AND status = ? LIMIT ?", (job_id, CHUNK_FAILED, limit))


class Journal:
    def __init__(self, path, interval: float = 0.05, backlog: int = 200_000):
        self.path = Path(path)
        self.interval, self.backlog = interval, backlog
        self.failed: str | None = None  # the traceback, once a write did not reach the disk
        self._lock = own(f"{self.path}.lock")
        try:
            db = sqlite3.connect(self.path)
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if not version and not db.execute("SELECT count(*) FROM sqlite_master").fetchone()[0]:
                db.executescript(f"PRAGMA auto_vacuum = INCREMENTAL; {_SCHEMA} PRAGMA user_version = {VERSION};")
            elif version != VERSION:
                raise RuntimeError(f"{self.path} is not a version {VERSION} journal: move it away to start a new one")
            # Whatever never ended belonged to a process that is gone: the lock says so.
            db.execute("UPDATE jobs SET status = ?, finished_at = ? WHERE finished_at IS NULL",
                       (INTERRUPTED, time.time()))
            db.commit()
            self._next = db.execute("SELECT coalesce(max(n), 0) FROM jobs").fetchone()[0] + 1
            db.close()
        except BaseException:
            self._lock.close()
            raise
        self._records = deque()
        self._claimed: set[str] = set()  # ids of the jobs this engine has not ended yet
        self._guard = threading.Lock()
        self._wake = threading.Event()
        self._writer = threading.Thread(target=self._write, name="msflow-journal", daemon=True)
        self._writer.start()

    # --- Engine loop: nothing here touches the disk -----------------------------------

    def job_created(self, job) -> None:
        if job.n is None:  # a job that continues keeps its number
            job.n = self._next
            self._next += 1
        self._records.append((job,))

    def chunk_finished(self, job, key, attempt: int, error: str | None) -> None:
        self._records.append((job.n, key, CHUNK_FAILED if error else CHUNK_DONE, attempt, error))

    def job_ended(self, job, forget_done: bool) -> None:
        """`forget_done` drops the rows of the chunks that went well: the job will not resume."""
        self._records.append((job, forget_done))
        self._claimed.discard(job.uid)

    def wait_if_behind(self) -> None:
        # ponytail: this holds the engine loop, so every job waits for this disk. Brake only
        # the jobs of this journal if a slow project disk ever holds application jobs back.
        while len(self._records) > self.backlog and self.failed is None:
            self._wake.set()
            time.sleep(0.005)

    # --- Any thread -------------------------------------------------------------------

    def flush(self) -> None:
        """Return once everything recorded so far is on disk."""
        written = threading.Event()
        self._records.append(written)
        self._wake.set()
        while self._writer.is_alive() and not written.wait(1.0):
            pass  # forgetting the done chunks of a job of millions takes seconds

    def claim(self, job_id: str):
        """Reserve `job_id` for a job about to start.

        Returns `(n, chunks done, source)` when it continues a job that was interrupted or canceled
        and `None` when it is new.
        """
        with self._guard:
            if job_id in self._claimed:
                raise ValueError(f"job {job_id!r} is already running")
            self._claimed.add(job_id)
        try:
            self.flush()
            with closing(sqlite3.connect(self.path)) as db:
                row = db.execute("SELECT n, status, source FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
                if row is None:
                    return None
                if row[1] not in RESUMABLE:
                    raise ValueError(f"job {job_id!r} already ended as {row[1]}; "
                                     "only an interrupted or canceled job continues")
                done = db.execute("SELECT count(*) FROM chunks WHERE job = ? AND status = ?", (row[0], CHUNK_DONE))
                return row[0], done.fetchone()[0], row[2]
        except BaseException:
            self._claimed.discard(job_id)
            raise

    def done_before(self, n: int):
        """`(seen, close)` for the calling thread: `seen(key)` says whether that chunk of job `n` is
        on record as done. Each key is looked up in the file, so a job of millions continues
        without its keys in memory. A payload equal to one done earlier in the same run is seen too.
        """
        db = sqlite3.connect(self.path)
        find = "SELECT 1 FROM chunks WHERE job = ? AND key = ? AND status = ?"
        return (lambda key: db.execute(find, (n, key, CHUNK_DONE)).fetchone() is not None), db.close

    def discard(self, job_id: str) -> None:
        """Drop a job that is not running from the record, with its chunks. Its id is free again."""
        with self._guard:  # against a `claim` of the same id
            if job_id in self._claimed:
                raise ValueError(f"job {job_id!r} is running")
            self._records.append(job_id)
        self.flush()

    def jobs(self) -> list[dict]:
        """Every job on record, oldest first."""
        self.flush()
        return recorded(self.path)

    def close(self) -> None:
        self._records.append(None)
        self._wake.set()
        self._writer.join()
        self._lock.close()

    # --- Writer thread ----------------------------------------------------------------

    def _write(self) -> None:
        db = _connect(self.path)
        records, live, open_ = self._records, {}, True
        while open_:
            self._wake.wait(self.interval)
            self._wake.clear()
            if not records:
                continue
            chunks, waiting, shrink = [], [], False
            try:
                db.execute("BEGIN")
                while records:
                    record = records.popleft()
                    if record is None:
                        open_ = False
                    elif type(record) is str:  # a discard()
                        db.execute("DELETE FROM chunks WHERE job = (SELECT n FROM jobs WHERE job_id = ?)", (record,))
                        db.execute("DELETE FROM jobs WHERE job_id = ?", (record,))
                        shrink = True
                    elif type(record) is not tuple:
                        waiting.append(record)  # a flush()
                    elif len(record) == 5:
                        chunks.append(record)
                    else:  # a job: started `(job,)` or ended `(job, forget_done)`
                        db.executemany("INSERT OR REPLACE INTO chunks VALUES (?, ?, ?, ?, ?)", chunks)
                        chunks.clear()
                        job = record[0]
                        if len(record) == 1:
                            live[job.n] = job
                            db.execute("INSERT OR REPLACE INTO jobs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                       (job.n, job.uid, job.name, job.status, job.spec, job.total, job.done,
                                        job.failed, job.error, job.started_at, job.finished_at, job.source))
                            # one that continues runs its failures again
                            db.execute("DELETE FROM chunks WHERE job = ? AND status = ?", (job.n, CHUNK_FAILED))
                        else:
                            live.pop(job.n, None)
                            db.execute(_JOB_COUNTERS, _counters(job))
                            if record[1]:
                                db.execute("DELETE FROM chunks WHERE job = ? AND status = ?", (job.n, CHUNK_DONE))
                                shrink = True
                db.executemany("INSERT OR REPLACE INTO chunks VALUES (?, ?, ?, ?, ?)", chunks)
                db.executemany(_JOB_COUNTERS, [_counters(job) for job in live.values()])  # for whoever watches
                db.execute("COMMIT")
                if shrink:  # hand the freed pages back: a finished job leaves one row and its failures
                    db.execute("PRAGMA incremental_vacuum").fetchall()
                    db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
            except Exception:
                self.failed = traceback.format_exc()
                log.error("journal %s: write failed\n%s", self.path, self.failed)
                if db.in_transaction:
                    db.execute("ROLLBACK")
            for written in waiting:
                written.set()
        db.close()


def _counters(job) -> tuple:
    return job.status, job.total, job.done, job.failed, job.error, job.finished_at, job.source, job.n
