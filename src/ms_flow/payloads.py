"""Per-entity payloads in append-only volumes.

The bytes of each entity (an SDF, a PDBQT, a pose) live in `volumes/<id:09d>.vol`
next to `project.db`; a row of `ms_payloads` says where. Apps keep only
`<role>_payload_id INTEGER REFERENCES ms_payloads(id)`: moving bytes rewrites
`ms_payloads` and never an app table, and ms_flow never needs the app schema.

Volume: a 32-byte header (`MSVOL`, version, volume id, series), then needles.
Needle: a 16-byte header (crc32, stored length, raw length, codec, flags), then
the bytes. A volume has one writer and only grows; a sealed volume never changes.

POSIX only (pread/pwritev). Nothing here imports SQL at module level: tasks
import `Payload` in workers.
"""

from __future__ import annotations

import os
import shutil
import struct
import tempfile
import threading
import zlib
from collections import OrderedDict
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import datetime
from itertools import islice
from pathlib import Path
from typing import Any, Iterable

from ms_flow.core.data.shard import _fsync_dir

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows
    fcntl = None

VOLUME_SIZE = 1 << 30  # a payload larger than this goes alone in its own volume
MAX_OPEN_FDS = 256  # volumes a reader keeps open
LOCATE_CHUNK = 900  # ids per location query, under SQLite's variable limit
READ_WINDOW = 500  # ids read at once by `with_payloads` and `iter_bytes`

MAGIC = b"MSVOL"
VERSION = 1
CODEC_NONE = 0
_VOLUME_HEADER = struct.Struct("<5sBHQ16s")  # magic, version, reserved, volume id, series
_NEEDLE = struct.Struct("<IIIHH")  # crc32, stored length, raw length, codec, flags
HEADER = _VOLUME_HEADER.size
NEEDLE = _NEEDLE.size
_IOV_MAX = 1024
_datasync = getattr(os, "fdatasync", os.fsync)  # macOS has no fdatasync

# Crash tests (V2-9): `MSFLOW_PAYLOADS_FAULT=<point>[:<n>]` kills the process the n-th time a
# write reaches `appended` (bytes in, not synced), `synced` (not committed) or `committed`.
_FAULT_POINT, _, _FAULT_AT = os.environ.get("MSFLOW_PAYLOADS_FAULT", "").partition(":")
_fault_seen = 0


def _fault(point: str) -> None:
    global _fault_seen
    if point == _FAULT_POINT:
        _fault_seen += 1
        if _fault_seen >= int(_FAULT_AT or 1):
            os._exit(137)


class PayloadCorrupt(Exception):
    """The stored bytes of a payload are missing, truncated or fail their checksum."""

    def __init__(self, payload_id: int, reason: str):
        super().__init__(f"payload {payload_id}: {reason}")
        self.payload_id = payload_id


@dataclass(frozen=True, slots=True)
class Payload:
    """Bytes to store. `series` groups payloads with the same lifetime in the same volumes."""

    data: bytes
    kind: str = ""
    series: str = "default"


class _Volume:
    """A volume this writer owns: its file stays open and locked until it is sealed."""

    __slots__ = ("id", "series", "fd", "end")

    def __init__(self, volume_id: int, series: str, fd: int, end: int):
        self.id = volume_id
        self.series = series
        self.fd = fd
        self.end = end


class _Transaction:
    """What one caller transaction wrote; undone unless it commits."""

    __slots__ = ("session", "committed", "leased", "start", "created", "retired", "dir_dirty")

    def __init__(self, session: Any):
        self.session = session
        self.committed = False
        self.leased: dict[str, _Volume] = {}  # series -> the volume this transaction appends to
        self.start: dict[_Volume, int] = {}  # volume -> its end before this transaction
        self.created: set[_Volume] = set()
        self.retired: list[_Volume] = []  # sealed in this transaction
        self.dir_dirty = False


class PayloadStore:
    """Writes and reads the payloads of one project."""

    def __init__(self, project: Any):
        import weakref

        from ms_flow.core.database import ProjectStore, resolve_project_store

        if isinstance(project, (str, os.PathLike)):
            store = ProjectStore.open_cached(ProjectStore.resolve_db_path(project))
        else:
            store = resolve_project_store(project)
        self.engine = store.engine
        self.directory = Path(store.db_path).parent / "volumes"
        self._lock = threading.Lock()  # guards _free and _transactions; never held during I/O
        self._free: dict[str, list[_Volume]] = {}  # active volumes no open transaction is appending to
        self._transactions: dict[Any, _Transaction] = {}  # root SessionTransaction -> its writes
        self._hooked: weakref.WeakSet = weakref.WeakSet()
        self._fds: OrderedDict[int, list] = OrderedDict()  # volume id -> [fd, users, evicted]
        self._fd_lock = threading.Lock()
        self._recover()

    def path(self, volume_id: int) -> Path:
        return self.directory / f"{volume_id:09d}.vol"

    def _recover(self) -> None:
        """F3/F4: seal the active volumes of dead writers, cut back to their committed bytes.

        A writer holds a lock on each volume it owns, so a volume we can lock has none. Its
        tail past the last committed needle was never committed: it goes.
        """
        # ponytail: always seals; adopt the volume instead if partial volumes pile up
        if fcntl is None or not self.directory.is_dir():
            return
        with self.engine.begin() as conn:
            active = conn.exec_driver_sql("SELECT id FROM ms_volumes WHERE state = 'active'").fetchall()
            for (volume_id,) in active:
                try:
                    fd = os.open(self.path(volume_id), os.O_RDWR)
                except FileNotFoundError:  # check() reports it
                    continue
                try:
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:  # a live writer owns it
                        continue
                    (end,) = conn.exec_driver_sql(
                        f'SELECT COALESCE(MAX("offset" + {NEEDLE} + length), {HEADER})'
                        " FROM ms_payloads WHERE volume_id = ?",
                        (volume_id,),
                    ).fetchone()
                    if os.fstat(fd).st_size > end:
                        os.ftruncate(fd, end)
                        os.fsync(fd)
                    conn.exec_driver_sql(
                        "UPDATE ms_volumes SET state = 'sealed', size = ? WHERE id = ?", (end, volume_id)
                    )
                finally:
                    os.close(fd)

    # -- writing ---------------------------------------------------------

    def put(self, session: Any, payloads: Iterable[Payload]) -> list[int]:
        """Store `payloads` inside the caller's transaction; return their ids, in order.

        The bytes are on disk (fdatasync) when this returns, so the caller's commit never
        points to lost bytes. If that transaction does not commit, the volumes go back to
        the size they had before it.
        """
        payloads = list(payloads)
        if not payloads:
            return []
        connection = session.connection()  # begins the caller's transaction
        tx = self._transaction(session)
        cursor = connection.connection.dbapi_connection.cursor()
        try:
            pending: dict[_Volume, list] = {}
            placed = []
            for payload in payloads:
                data = payload.data
                size = len(data)
                volume = self._volume_for(tx, cursor, payload.series, NEEDLE + size)
                crc = zlib.crc32(data)
                pending.setdefault(volume, [volume.end, []])[1] += (
                    _NEEDLE.pack(crc, size, size, CODEC_NONE, 0),
                    data,
                )
                placed.append((volume.id, volume.end, size, size, CODEC_NONE, crc, payload.kind))
                volume.end += NEEDLE + size
            for volume, (offset, buffers) in pending.items():
                try:
                    _write_all(volume.fd, buffers, offset)
                except OSError as error:  # a full disk: the caller's rollback cuts what got in
                    raise OSError(error.errno, f"cannot write payloads to {self.path(volume.id)}: "
                                               f"{error.strerror}") from error
            _fault("appended")
            for volume in pending:
                _datasync(volume.fd)
                cursor.execute("UPDATE ms_volumes SET size = ? WHERE id = ?", (volume.end, volume.id))
            if tx.dir_dirty:  # a new volume file must survive a crash too
                _fsync_dir(self.directory)
                tx.dir_dirty = False
            _fault("synced")
            ids = []
            for row in placed:
                cursor.execute(
                    'INSERT INTO ms_payloads (volume_id, "offset", length, raw_length, codec, crc32, kind)'
                    " VALUES (?, ?, ?, ?, ?, ?, ?)",
                    row,
                )
                ids.append(cursor.lastrowid)
            return ids
        finally:
            cursor.close()

    def _transaction(self, session: Any) -> _Transaction:
        from sqlalchemy import event

        root = session.get_transaction()
        with self._lock:
            tx = self._transactions.get(root)
            if tx is None:
                if session not in self._hooked:
                    event.listen(session, "after_commit", self._after_commit)
                    event.listen(session, "after_transaction_end", self._after_transaction_end)
                    self._hooked.add(session)
                tx = self._transactions[root] = _Transaction(session)
            return tx

    def _after_commit(self, session: Any) -> None:
        with self._lock:
            for tx in self._transactions.values():
                if tx.session is session:
                    tx.committed = True
        _fault("committed")

    def _after_transaction_end(self, session: Any, transaction: Any) -> None:
        with self._lock:
            tx = self._transactions.pop(transaction, None)
        if tx is None:
            return
        keep = []
        if tx.committed:
            for volume in tx.retired:
                os.close(volume.fd)
            keep = list(tx.leased.values())
        else:  # rollback, a failed commit, or close() without commit (F2)
            for volume, start in tx.start.items():
                if volume in tx.created:  # its ms_volumes row is gone: the file is nobody's
                    os.close(volume.fd)
                    self.path(volume.id).unlink(missing_ok=True)
                else:  # its row is back to what it was, active
                    os.ftruncate(volume.fd, start)
                    volume.end = start
                    keep.append(volume)
        with self._lock:
            for volume in keep:
                self._free.setdefault(volume.series, []).append(volume)

    def _volume_for(self, tx: _Transaction, cursor, series: str, need: int) -> _Volume:
        volume = tx.leased.get(series)
        if volume is None:
            with self._lock:
                free = self._free.get(series)
                volume = free.pop() if free else None
            if volume is not None:
                tx.start[volume] = volume.end
                tx.leased[series] = volume
        if volume is not None and volume.end > HEADER and volume.end + need > VOLUME_SIZE:
            cursor.execute(
                "UPDATE ms_volumes SET state = 'sealed', size = ? WHERE id = ?", (volume.end, volume.id)
            )
            tx.retired.append(volume)
            volume = None
        if volume is None:
            volume = self._create(cursor, series)
            tx.created.add(volume)
            tx.start[volume] = HEADER
            tx.leased[series] = volume
            tx.dir_dirty = True
        return volume

    def _create(self, cursor, series: str) -> _Volume:
        label = series.encode()
        if len(label) > 16:
            raise ValueError(f"series name longer than 16 bytes: {series!r}")
        self.directory.mkdir(parents=True, exist_ok=True)
        while True:
            cursor.execute(
                "INSERT INTO ms_volumes (series, state, size, created_at) VALUES (?, 'active', ?, ?)",
                (series, HEADER, datetime.now().isoformat(" ")),
            )
            volume_id = cursor.lastrowid
            try:
                fd = os.open(self.path(volume_id), os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o644)
                break
            except FileExistsError:  # left by a transaction that died with this id; the next one is free
                cursor.execute("DELETE FROM ms_volumes WHERE id = ?", (volume_id,))
        if fcntl is not None:  # held while this writer owns the volume: recovery skips it
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.pwrite(fd, _VOLUME_HEADER.pack(MAGIC, VERSION, 0, volume_id, label), 0)
        return _Volume(volume_id, series, fd, HEADER)

    def referencing_columns(self) -> list[tuple[str, str]]:
        """(table, column) of every app column that holds payload ids.

        Read from the schema (`PRAGMA foreign_key_list`): an app declares
        `REFERENCES ms_payloads(id)` and registers nothing with ms_flow.
        """
        with self.engine.connect() as conn:
            tables = [r[0] for r in conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE type = 'table'")]
            return [
                (table, fk[3])
                for table in tables
                for fk in conn.exec_driver_sql(f'PRAGMA foreign_key_list("{table}")')
                if fk[2] == "ms_payloads"
            ]

    # -- reading ---------------------------------------------------------

    def get(self, payload_id: int) -> bytes:
        return self.get_many([payload_id])[int(payload_id)]

    def get_many(self, ids: Iterable[int]) -> dict[int, bytes]:
        """Bytes by id. Read in (volume, offset) order; unknown ids raise KeyError."""
        wanted = list(dict.fromkeys(int(i) for i in ids))
        located = self._locate(wanted)
        missing = [i for i in wanted if i not in located]
        if missing:
            raise KeyError(f"unknown payload ids: {missing[:10]}")
        return {i: self._read(i, located[i]) for i in sorted(located, key=located.__getitem__)}

    def iter_bytes(self, ids: Iterable[int], window: int = READ_WINDOW):
        """`(id, bytes)` for each id, `window` ids at a time, each window in (volume, offset) order: to export."""
        ids = iter(ids)
        while batch := list(islice(ids, window)):
            yield from self.get_many(batch).items()

    def materialize(self, payload_id: int, directory: str | os.PathLike, suffix: str = ""):
        """The payload as the file `<directory>/<id><suffix>`, written once: what a job hands its
        tasks as a `File` (a receptor), which executors without the disk carry once per job."""
        from ms_flow.engine.files import File

        path = Path(directory) / f"{int(payload_id)}{suffix}"
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            part = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}")
            part.write_bytes(self.get(payload_id))
            os.replace(part, path)
        return File(path)

    def _locate(self, ids: list[int]) -> dict[int, tuple]:
        found = {}
        with self.engine.connect() as conn:
            for start in range(0, len(ids), LOCATE_CHUNK):
                chunk = ids[start : start + LOCATE_CHUNK]
                rows = conn.exec_driver_sql(
                    'SELECT id, volume_id, "offset", length, raw_length, codec, crc32 FROM ms_payloads'
                    f" WHERE id IN ({','.join('?' * len(chunk))})",
                    tuple(chunk),
                )
                for row in rows:
                    found[row[0]] = tuple(row[1:])
        return found

    def _read(self, payload_id: int, location: tuple, retry: bool = True) -> bytes:
        volume_id, offset, length, raw_length, codec, crc = location
        try:
            entry = self._open(volume_id)
        except FileNotFoundError:
            moved = self._locate([payload_id]).get(payload_id) if retry else None
            if moved is not None and moved != location:  # compacted meanwhile: read the new place once
                return self._read(payload_id, moved, retry=False)
            raise PayloadCorrupt(payload_id, f"volume {volume_id} is missing") from None
        try:
            buffer = os.pread(entry[0], NEEDLE + length, offset)
        finally:
            self._release(entry)
        if len(buffer) != NEEDLE + length:
            raise PayloadCorrupt(payload_id, f"truncated in volume {volume_id}")
        data = memoryview(buffer)[NEEDLE:]
        if _NEEDLE.unpack_from(buffer)[:4] != (crc, length, raw_length, codec) or zlib.crc32(data) != crc:
            raise PayloadCorrupt(payload_id, f"checksum mismatch in volume {volume_id}")
        if codec != CODEC_NONE:
            raise PayloadCorrupt(payload_id, f"unknown codec {codec}")
        return bytes(data)

    def _open(self, volume_id: int) -> list:
        with self._fd_lock:
            entry = self._fds.pop(volume_id, None)
            if entry is None:
                entry = [os.open(self.path(volume_id), os.O_RDONLY), 0, False]
            self._fds[volume_id] = entry
            entry[1] += 1
            while len(self._fds) > MAX_OPEN_FDS:
                _, old = self._fds.popitem(last=False)
                old[2] = True
                if old[1] == 0:
                    os.close(old[0])
            return entry

    def _release(self, entry: list) -> None:
        with self._fd_lock:
            entry[1] -= 1
            if entry[1] == 0 and entry[2]:  # evicted while another thread was reading it
                os.close(entry[0])

    def close(self) -> None:
        """Close files. Call it with no transaction of this store open."""
        with self._lock:
            volumes = [v for free in self._free.values() for v in free]
            self._free.clear()
        for volume in volumes:
            os.close(volume.fd)
        with self._fd_lock:
            for entry in self._fds.values():
                entry[2] = True
                if entry[1] == 0:
                    os.close(entry[0])
            self._fds.clear()


class _WithPayloads:
    def __init__(self, store: PayloadStore, rows: Iterable[dict], fields: dict[str, str], window: int):
        self._store, self._rows, self._fields, self._window = store, rows, dict(fields), window

    def __getattr__(self, name: str):
        if name in ("open", "remaining", "reads"):  # only what `rows` has: the engine asks with hasattr
            return getattr(self.__dict__["_rows"], name)
        raise AttributeError(name)

    def __iter__(self):
        rows = iter(self._rows)
        while batch := list(islice(rows, self._window)):
            ids = {row[column] for row in batch for column in self._fields if row.get(column) is not None}
            found = self._store.get_many(ids) if ids else {}
            for row in batch:
                yield {**row, **{key: found.get(row.get(column)) for column, key in self._fields.items()}}


def with_payloads(project: Any, rows: Iterable[dict], fields: dict[str, str], window: int = READ_WINDOW):
    """A job's source whose rows carry the bytes of their payloads, so a task gets bytes and never a path.

    `fields` maps an id column to the key that gets its bytes (`{"current_payload_id": "ligand"}`;
    a NULL id gives `None`). Rows are read `window` at a time in (volume, offset) order, so at most
    that many hold bytes here, and the engine reads only its window ahead (I8). What `rows` knows
    (`open`, `remaining`, `reads` of `ms_flow.query.Rows`) passes through.
    """
    from ms_flow.core.database import resolve_project_store

    return _WithPayloads(resolve_project_store(project).payloads, rows, fields, window)


@contextmanager
def scratch_file(data: bytes, suffix: str = ""):
    """`data` as a temporary file, for a program that only reads paths: in /dev/shm when it is
    there and has room for it, in the system's temporary folder if not. Deleted on leaving."""
    directory = None
    if os.path.isdir("/dev/shm") and os.access("/dev/shm", os.W_OK):
        if shutil.disk_usage("/dev/shm").free > 2 * len(data):  # ponytail: room now, not while it is written
            directory = "/dev/shm"
    fd, name = tempfile.mkstemp(suffix=suffix, prefix="msflow-", dir=directory)
    try:
        with os.fdopen(fd, "wb") as file:
            file.write(data)
        yield Path(name)
    finally:
        with suppress(FileNotFoundError):
            os.unlink(name)


def _write_all(fd: int, buffers: list, offset: int) -> None:
    for start in range(0, len(buffers), _IOV_MAX):
        chunk = buffers[start : start + _IOV_MAX]
        want = sum(map(len, chunk))
        done = os.pwritev(fd, chunk, offset)
        if done < want:  # short write: finish it, or let the OSError (disk full) surface
            rest = memoryview(b"".join(chunk))[done:]
            while rest:
                written = os.pwrite(fd, rest, offset + done)
                done += written
                rest = rest[written:]
        offset += want
