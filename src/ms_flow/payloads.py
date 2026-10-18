"""Per-entity payloads in append-only volumes.

The bytes of each entity (an SDF, a PDBQT, a pose) live in `volumes/<id:09d>.vol`
next to `project.db`; a row of `ms_payloads` says where. Apps keep only
`<role>_payload_id INTEGER REFERENCES ms_payloads(id)`: moving bytes rewrites
`ms_payloads` and never an app table, and ms_flow never needs the app schema.

Volume: a 32-byte header (`MSVOL`, version, volume id, series), then needles.
Needle: a 16-byte header (crc32, stored length, raw length, codec, flags), then
the bytes. A volume has one writer and only grows; a sealed volume never changes.

Each payload says its codec (`none`, `zlib`, `zstd`), so volumes mix them. A
`Payload` compresses itself when it is pickled, i.e. in the worker that made it:
the writer only appends bytes. A zstd frame names its dictionary (`dicts/<id>.zdict`,
trained per series with `PayloadStore.train_dictionary`), so reading needs no table.

Maintenance: `check` reports, `release` drops payloads nobody references, `compact`
copies the live needles of mostly dead volumes to new ones (recompressing them with
the series' current dictionary) and `repair` clears what crashes leave. Ids never
change: compacting rewrites the location of a row, with compare-and-set (I6).

POSIX only (pread/pwritev). Nothing here imports SQL at module level: tasks
import `Payload` in workers.
"""

from __future__ import annotations

import os
import shutil
import struct
import tempfile
import threading
import time
import zlib
from collections import OrderedDict
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
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
ORPHAN_GRACE = 60  # seconds before a volume file without a row or a writer counts as an orphan
READ_WINDOW = 500  # ids read at once by `with_payloads` and `iter_bytes`

MAGIC = b"MSVOL"
VERSION = 1
CODEC_NONE, CODEC_ZLIB, CODEC_ZSTD = 0, 1, 2
CODECS = {"none": CODEC_NONE, "zlib": CODEC_ZLIB, "zstd": CODEC_ZSTD}
DEFAULT_CODEC = "zstd"
ZLIB_LEVEL = 6
ZSTD_LEVEL = 3
DICT_SIZE = 112_640  # zstd's default dictionary size
DICT_SAMPLES = 10_000
_VOLUME_HEADER = struct.Struct("<5sBHQ16s")  # magic, version, reserved, volume id, series
_NEEDLE = struct.Struct("<IIIHH")  # crc32, stored length, raw length, codec, flags
HEADER = _VOLUME_HEADER.size
NEEDLE = _NEEDLE.size
_IOV_MAX = 1024
_datasync = getattr(os, "fdatasync", os.fsync)  # macOS has no fdatasync

# Crash tests (V2-9): `MSFLOW_PAYLOADS_FAULT=<point>[:<n>]` kills the process the n-th time a
# write reaches `appended` (bytes in, not synced), `synced` (not committed) or `committed`;
# a compaction, `copied` (new volume synced, rows not moved) or `swapped` (moved, old not deleted).
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
    """Bytes to store. `series` groups payloads with the same lifetime in the same volumes.

    `codec` is `none`, `zlib` or `zstd`; `dictionary` is the path of a zstd dictionary
    (`PayloadStore.dictionary(series)`, a `File` a job hands its tasks). `raw_length` is set
    once `data` holds the compressed bytes: pickling compresses, so it happens in the worker.
    """

    data: bytes
    kind: str = ""
    series: str = "default"
    codec: str = DEFAULT_CODEC
    dictionary: str | None = None
    raw_length: int | None = None

    def packed(self) -> Payload:
        """This payload with `data` compressed by its codec (itself if it already is)."""
        if self.raw_length is not None:
            return self
        if self.codec not in CODECS:
            raise ValueError(f"unknown codec {self.codec!r}; one of {sorted(CODECS)}")
        return Payload(_compress(self.codec, self.data, self.dictionary), self.kind, self.series, self.codec,
                       None, len(self.data))

    def __reduce__(self):
        packed = self.packed()
        return Payload, (packed.data, packed.kind, packed.series, packed.codec, None, packed.raw_length)


_codecs = threading.local()  # zstd (de)compressors are not thread-safe: one set per thread


def _zstd():
    try:
        import zstandard
    except ImportError:  # pragma: no cover - a declared dependency
        raise RuntimeError("zstd payloads need the zstandard package: pip install zstandard") from None
    return zstandard


def _compress(codec: str, data: bytes, dictionary: str | None) -> bytes:
    if codec == "none":
        return bytes(data)
    if codec == "zlib":
        return zlib.compress(data, ZLIB_LEVEL)
    cache = _codecs.__dict__.setdefault("compressors", {})
    key = os.fspath(dictionary) if dictionary else None
    if key not in cache:
        zstandard = _zstd()
        dict_data = zstandard.ZstdCompressionDict(Path(key).read_bytes()) if key else None
        cache[key] = zstandard.ZstdCompressor(level=ZSTD_LEVEL, dict_data=dict_data)
    return cache[key].compress(data)


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


@dataclass
class Report:
    """What `PayloadStore.check` found. Only `broken` and `missing_dicts` are errors (I5):
    the rest is garbage that `repair` clears."""

    broken: dict[int, str] = field(default_factory=dict)  # payload id -> why its bytes cannot be read
    missing_volumes: list[int] = field(default_factory=list)  # with rows; their ids are in `broken`
    missing_dicts: list[int] = field(default_factory=list)  # zstd dictionaries listed without a file
    unreferenced: list[int] = field(default_factory=list)  # payload rows no app row points to
    empty: list[int] = field(default_factory=list)  # sealed or trash volumes without rows
    orphans: list[int] = field(default_factory=list)  # volume files without a row and without a writer
    dead: dict[int, float] = field(default_factory=dict)  # sealed volume -> share of its bytes no row covers

    @property
    def ok(self) -> bool:
        return not (self.broken or self.missing_dicts)

    @property
    def clean(self) -> bool:
        """Nothing to repair either."""
        return self.ok and not (self.unreferenced or self.empty or self.orphans)


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
                payload = payload.packed()  # done already when it came from a worker
                data, codec = payload.data, CODECS[payload.codec]
                size = len(data)
                volume = self._volume_for(tx, cursor, payload.series, NEEDLE + size)
                crc = zlib.crc32(data)
                pending.setdefault(volume, [volume.end, []])[1] += (
                    _NEEDLE.pack(crc, size, payload.raw_length, codec, 0),
                    data,
                )
                placed.append((volume.id, volume.end, size, payload.raw_length, codec, crc, payload.kind))
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

    def kinds(self, ids: Iterable[int]) -> dict[int, str]:
        """The `kind` each payload was stored with (a format: `sdf`, `pdbqt`...), by id; unknown ids are left out."""
        ids = list(dict.fromkeys(int(i) for i in ids))
        found = {}
        with self.engine.connect() as conn:
            for start in range(0, len(ids), LOCATE_CHUNK):
                chunk = ids[start : start + LOCATE_CHUNK]
                found.update(tuple(row) for row in conn.exec_driver_sql(
                    f"SELECT id, kind FROM ms_payloads WHERE id IN ({','.join('?' * len(chunk))})", tuple(chunk)))
        return found

    def iter_bytes(self, ids: Iterable[int], window: int = READ_WINDOW):
        """`(id, bytes)` for each id, `window` ids at a time, each window in (volume, offset) order: to export."""
        ids = iter(ids)
        while batch := list(islice(ids, window)):
            yield from self.get_many(batch).items()

    def materialize(self, payload_id: int, directory: str | os.PathLike, suffix: str | None = None):
        """The payload as the file `<directory>/<id><suffix>`, written once: what a job hands its
        tasks as a `File` (a receptor), which executors without the disk carry once per job.
        Without `suffix`, the payload's kind is the extension (`.pdbqt`)."""
        from ms_flow.engine.files import File

        if suffix is None:
            kind = self.kinds([payload_id]).get(int(payload_id))
            suffix = f".{kind}" if kind else ""
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

    def _read(self, payload_id: int, location: tuple) -> bytes:
        data, location = self._stored(payload_id, location)
        return self._decode(payload_id, data, location[4], location[3])

    def _stored(self, payload_id: int, location: tuple, retry: int = 3) -> tuple:
        """The checked stored bytes of a needle, and the location they came from. A needle that
        moved follows its row (I7), `retry` times: compactions can run back to back."""
        volume_id, offset, length, raw_length, codec, crc = location
        try:
            entry = self._open(volume_id)
        except FileNotFoundError:
            moved = self._locate([payload_id]).get(payload_id) if retry else None
            if moved is not None and moved != location:  # compacted meanwhile: read the new place
                return self._stored(payload_id, moved, retry - 1)
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
        return data, location

    def _decode(self, payload_id: int, data, codec: int, raw_length: int) -> bytes:
        if codec == CODEC_NONE:
            return bytes(data)
        if codec == CODEC_ZLIB:
            return zlib.decompress(data)
        if codec == CODEC_ZSTD:
            return self._decompressor(payload_id, data).decompress(data, max_output_size=raw_length)
        raise PayloadCorrupt(payload_id, f"unknown codec {codec}")

    def _decompressor(self, payload_id: int, data) -> Any:
        zstandard = _zstd()
        dict_id = zstandard.get_frame_parameters(data).dict_id
        cache = _codecs.__dict__.setdefault("decompressors", {})
        key = (self.directory, dict_id)
        if key not in cache:
            dict_data = None
            if dict_id:
                path = self.directory / "dicts" / f"{dict_id}.zdict"
                try:
                    dict_data = zstandard.ZstdCompressionDict(path.read_bytes())
                except FileNotFoundError:
                    raise PayloadCorrupt(payload_id, f"its zstd dictionary {path} is missing") from None
            cache[key] = zstandard.ZstdDecompressor(dict_data=dict_data)
        return cache[key]

    # -- dictionaries ----------------------------------------------------

    def train_dictionary(self, series: str, samples: int = DICT_SAMPLES, size: int = DICT_SIZE):
        """Train a zstd dictionary on `samples` payloads spread over `series` (the head of a
        library is not like the rest of it) and make it the series' current one: a `File`."""
        import random

        from ms_flow.engine.files import File

        with self.engine.connect() as conn:
            ids = [r[0] for r in conn.exec_driver_sql(
                "SELECT p.id FROM ms_payloads p JOIN ms_volumes v ON v.id = p.volume_id WHERE v.series = ?",
                (series,))]
        if len(ids) < 100:
            raise ValueError(f"series {series!r} has {len(ids)} payloads; a dictionary needs a few thousand")
        picked = sorted(random.Random(0).sample(ids, min(samples, len(ids))))
        data = [b for _, b in self.iter_bytes(picked)]
        with self.engine.begin() as conn:
            dict_id = conn.exec_driver_sql(
                "INSERT INTO ms_payload_dicts (series, samples, created_at) VALUES (?, ?, ?)",
                (series, len(data), datetime.now().isoformat(" "))).lastrowid
            trained = _zstd().train_dictionary(size, data, dict_id=dict_id)
            path = self.directory / "dicts" / f"{dict_id}.zdict"
            path.parent.mkdir(parents=True, exist_ok=True)
            part = path.with_name(path.name + ".part")
            with open(part, "wb") as file:
                file.write(trained.as_bytes())
                file.flush()
                os.fsync(file.fileno())
            os.replace(part, path)
            _fsync_dir(path.parent)
        return File(path)

    def dictionary(self, series: str):
        """The current zstd dictionary of `series` as a `File`, or None if it has none yet."""
        from ms_flow.engine.files import File

        with self.engine.connect() as conn:
            row = conn.exec_driver_sql(
                "SELECT max(id) FROM ms_payload_dicts WHERE series = ?", (series,)).scalar()
        return None if row is None else File(self.directory / "dicts" / f"{row}.zdict")

    # -- maintenance -----------------------------------------------------

    def check(self, full: bool = False) -> Report:
        """What is wrong or left over; `full` also reads every needle (crc32, codec, dictionary)."""
        report = Report()
        with self.engine.connect() as conn:
            volumes = {r[0]: r[1:] for r in conn.exec_driver_sql("SELECT id, state, size FROM ms_volumes")}
            stats = {r[0]: r[1:] for r in conn.exec_driver_sql(
                f'SELECT volume_id, sum({NEEDLE} + length), max("offset" + {NEEDLE} + length)'
                " FROM ms_payloads GROUP BY volume_id")}
            report.unreferenced = [r[0] for r in conn.exec_driver_sql(
                f"SELECT id FROM ms_payloads WHERE {self._unreferenced()}")]
            dicts = [r[0] for r in conn.exec_driver_sql("SELECT id FROM ms_payload_dicts")]
        short = {}  # volume id -> its size on disk, when rows point past it
        for volume_id, (state, size) in volumes.items():
            live, end = stats.get(volume_id, (0, HEADER))
            try:
                on_disk = self.path(volume_id).stat().st_size
            except FileNotFoundError:
                if volume_id in stats:
                    report.missing_volumes.append(volume_id)
                continue
            if state == "trash" or (state == "sealed" and volume_id not in stats):
                report.empty.append(volume_id)
            elif state == "sealed":
                report.dead[volume_id] = round(1 - live / max(size - HEADER, 1), 4)
            if end > on_disk:
                short[volume_id] = on_disk
        with self.engine.connect() as conn:
            for volume_id in report.missing_volumes:
                for (pid,) in conn.exec_driver_sql("SELECT id FROM ms_payloads WHERE volume_id = ?", (volume_id,)):
                    report.broken[pid] = f"volume {volume_id} is missing"
            for volume_id, on_disk in short.items():
                for (pid,) in conn.exec_driver_sql(
                        f'SELECT id FROM ms_payloads WHERE volume_id = ? AND "offset" + {NEEDLE} + length > ?',
                        (volume_id, on_disk)):
                    report.broken[pid] = f"past the end of volume {volume_id}"
            if full:
                rows = conn.exec_driver_sql('SELECT id, volume_id, "offset", length, raw_length, codec, crc32'
                                            ' FROM ms_payloads ORDER BY volume_id, "offset"')
                for pid, *location in rows:
                    if pid not in report.broken:
                        try:
                            self._read(pid, tuple(location))
                        except Exception as error:  # a report: whatever stops a read goes in it
                            report.broken[pid] = str(error)
        report.missing_dicts = [d for d in dicts if not (self.directory / "dicts" / f"{d}.zdict").exists()]
        known = set(volumes)
        report.orphans = sorted(volume_id for volume_id in self._volume_files()
                                if volume_id not in known and self._unowned(volume_id))
        return report

    def release(self, ids: Iterable[int]) -> int:
        """Delete the payload rows of `ids` that no app row references (after the app deleted its
        rows); a sealed volume left without rows goes, unread. Returns how many rows went."""
        ids = list(dict.fromkeys(int(i) for i in ids))
        guard = self._unreferenced()
        released = 0
        with self.engine.begin() as conn:
            for start in range(0, len(ids), LOCATE_CHUNK):
                chunk = ids[start : start + LOCATE_CHUNK]
                released += conn.exec_driver_sql(
                    f"DELETE FROM ms_payloads WHERE id IN ({','.join('?' * len(chunk))}) AND {guard}",
                    tuple(chunk)).rowcount
            self._trash_empty(conn)
        self._empty_trash()
        return released

    def compact(self, min_dead: float = 0.5, small: int | None = None, order_by: str | None = None) -> dict:
        """Copy the live needles of sealed volumes with at least `min_dead` dead bytes (or of two or
        more volumes of a series under `small` bytes, a quarter of VOLUME_SIZE by default) to new volumes, move their rows there and
        delete the old ones. Needles are recompressed with the series' current dictionary.

        `order_by` is SQL that gives payload ids in the order wanted next to each other on disk
        (`SELECT current_payload_id FROM molecules ORDER BY score`); the rest keep theirs.
        Never touches an active volume, so it can run next to writers and readers (I6, I7).
        """
        small = VOLUME_SIZE // 4 if small is None else small
        self._empty_trash()
        with self.engine.connect() as conn:
            rows = conn.exec_driver_sql(
                f"SELECT v.id, v.series, v.size, coalesce(sum({NEEDLE} + p.length), 0) FROM ms_volumes v"
                " LEFT JOIN ms_payloads p ON p.volume_id = v.id WHERE v.state = 'sealed' GROUP BY v.id").fetchall()
        picked: dict[str, list[int]] = {}
        for series in {r[1] for r in rows}:
            mine = [(vid, 1 - live / max(size - HEADER, 1), size) for vid, s, size, live in rows if s == series]
            chosen = [vid for vid, dead, _ in mine if dead >= min_dead]
            smalls = [vid for vid, dead, size in mine if dead < min_dead and size < small]
            chosen += smalls if len(smalls) > 1 else []  # one small volume alone gains nothing
            if chosen:
                picked[series] = chosen
        stats = {"volumes": 0, "moved": 0, "read_mb": 0.0, "written_mb": 0.0}
        for series, volume_ids in picked.items():
            self._compact_series(series, volume_ids, order_by, stats)
        self._empty_trash()
        return stats

    def _compact_series(self, series: str, volume_ids: list[int], order_by: str | None, stats: dict) -> None:
        marks = ",".join("?" * len(volume_ids))
        with self.engine.connect() as conn:
            rows = conn.exec_driver_sql('SELECT id, volume_id, "offset", length, raw_length, codec, crc32'
                                        f' FROM ms_payloads WHERE volume_id IN ({marks}) ORDER BY volume_id, "offset"',
                                        tuple(volume_ids)).fetchall()
            if order_by and rows:
                rank = {r[0]: i for i, r in enumerate(conn.exec_driver_sql(order_by)) if r[0] is not None}
                rows.sort(key=lambda r: rank.get(r[0], len(rank)))  # stable: the rest stay in location order
        dictionary = self.dictionary(series)
        dict_id = int(Path(dictionary).stem) if dictionary else 0
        targets, moved, buffers, volume, offset = [], [], [], None, 0
        try:
            for pid, *location in rows:
                try:
                    data, _ = self._stored(pid, tuple(location), retry=0)
                except PayloadCorrupt:  # stays where it is: check() names it
                    continue
                old_volume, old_offset, length, raw_length, codec, crc = location
                stats["read_mb"] += (NEEDLE + length) / 1e6
                if codec != CODEC_NONE and (codec != CODEC_ZSTD or _zstd().get_frame_parameters(data).dict_id != dict_id):
                    data = _compress("zstd", self._decode(pid, data, codec, raw_length), dictionary)  # V5-7
                    codec, crc = CODEC_ZSTD, zlib.crc32(data)
                if volume is None or (volume.end > HEADER and volume.end + NEEDLE + len(data) > VOLUME_SIZE):
                    if buffers:
                        _write_all(volume.fd, buffers, offset)
                        buffers = []
                    volume = self._new_volume(series)
                    targets.append(volume)
                if not buffers:
                    offset = volume.end
                buffers += (_NEEDLE.pack(crc, len(data), raw_length, codec, 0), data)
                moved.append((volume.id, volume.end, len(data), raw_length, codec, crc, pid, old_volume, old_offset))
                volume.end += NEEDLE + len(data)
                if len(buffers) >= _IOV_MAX:
                    _write_all(volume.fd, buffers, offset)
                    buffers = []
            if buffers:
                _write_all(volume.fd, buffers, offset)
            for target in targets:
                _datasync(target.fd)
            _fault("copied")
            with self.engine.begin() as conn:
                if moved:  # compare-and-set: a row released or moved meanwhile stays as it is
                    conn.exec_driver_sql(
                        'UPDATE ms_payloads SET volume_id = ?, "offset" = ?, length = ?, raw_length = ?, codec = ?,'
                        ' crc32 = ? WHERE id = ? AND volume_id = ? AND "offset" = ?', moved)
                for target in targets:
                    conn.exec_driver_sql("UPDATE ms_volumes SET state = 'sealed', size = ? WHERE id = ?",
                                         (target.end, target.id))
                self._trash_empty(conn, volume_ids)
            _fault("swapped")
        finally:
            for target in targets:  # sealed, or left active for _recover to seal if this failed
                os.close(target.fd)
        stats["volumes"] += len(volume_ids)
        stats["moved"] += len(moved)
        stats["written_mb"] += sum(t.end - HEADER for t in targets) / 1e6

    def repair(self, report: Report | None = None) -> Report:
        """Clear what `check` reports as garbage: cut the tails of dead writers, delete unreferenced
        rows, empty volumes and orphan files. A referenced row is never deleted. Returns a new check."""
        report = report or self.check()
        self._recover()
        self.release(report.unreferenced)
        for volume_id in report.orphans:
            if self._unowned(volume_id):
                self.path(volume_id).unlink(missing_ok=True)
        with self.engine.begin() as conn:
            self._trash_empty(conn)
        self._empty_trash()
        return self.check()

    def _new_volume(self, series: str) -> _Volume:
        """A volume of its own, committed now so that a crash leaves a row (_recover seals it) and
        not an orphan file. Its lock keeps every _recover off it until it is closed."""
        with self.engine.begin() as conn:
            cursor = conn.connection.dbapi_connection.cursor()
            try:
                volume = self._create(cursor, series)
            finally:
                cursor.close()
        _fsync_dir(self.directory)
        return volume

    def _unreferenced(self) -> str:
        refs = self.referencing_columns()
        return " AND ".join(f'NOT EXISTS (SELECT 1 FROM "{t}" WHERE "{c}" = ms_payloads.id)' for t, c in refs) or "1"

    @staticmethod
    def _trash_empty(conn, volume_ids: Iterable[int] | None = None) -> None:
        only = "" if volume_ids is None else f" AND id IN ({','.join(str(int(v)) for v in volume_ids) or 'NULL'})"
        conn.exec_driver_sql("UPDATE ms_volumes SET state = 'trash' WHERE state = 'sealed'"
                             f" AND NOT EXISTS (SELECT 1 FROM ms_payloads WHERE volume_id = ms_volumes.id){only}")

    def _empty_trash(self) -> None:
        """Delete trash volumes: the file, then the row (a crash between leaves a row to retry)."""
        with self.engine.connect() as conn:
            trash = [r[0] for r in conn.exec_driver_sql("SELECT id FROM ms_volumes WHERE state = 'trash'")]
        if not trash:
            return
        with self._fd_lock:  # our cached descriptors would keep the space taken
            for volume_id in trash:
                entry = self._fds.pop(volume_id, None)
                if entry is not None:
                    entry[2] = True
                    if entry[1] == 0:
                        os.close(entry[0])
        # ponytail: another process's reader keeps a deleted volume's space until it evicts the descriptor
        for volume_id in trash:
            self.path(volume_id).unlink(missing_ok=True)
        _fsync_dir(self.directory)
        with self.engine.begin() as conn:
            conn.exec_driver_sql(f"DELETE FROM ms_volumes WHERE state = 'trash' AND id IN ({','.join(map(str, trash))})")

    def _volume_files(self) -> list[int]:
        return [int(p.stem) for p in self.directory.glob("*.vol") if p.stem.isdigit()] if self.directory.is_dir() else []

    def _unowned(self, volume_id: int) -> bool:
        """No writer holds the volume's lock, and the file is older than ORPHAN_GRACE seconds:
        a writer takes the lock right after creating the file, and that instant is a window."""
        path = self.path(volume_id)
        try:
            fd = os.open(path, os.O_RDONLY)
        except FileNotFoundError:
            return False
        try:
            if fcntl is not None:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    return False
            return time.time() - os.fstat(fd).st_mtime > ORPHAN_GRACE
        finally:
            os.close(fd)

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
    def __init__(self, store: PayloadStore, rows: Iterable[dict], fields: dict[str, str], window: int, kinds: bool):
        self._store, self._rows, self._fields, self._window, self._kinds = store, rows, dict(fields), window, kinds

    def __getattr__(self, name: str):
        if name in ("open", "remaining", "reads"):  # only what `rows` has: the engine asks with hasattr
            return getattr(self.__dict__["_rows"], name)
        raise AttributeError(name)

    def __iter__(self):
        rows = iter(self._rows)
        while batch := list(islice(rows, self._window)):
            ids = {row[column] for row in batch for column in self._fields if row.get(column) is not None}
            found = self._store.get_many(ids) if ids else {}
            kinds = self._store.kinds(ids) if ids and self._kinds else {}
            for row in batch:
                row = {**row, **{key: found.get(row.get(column)) for column, key in self._fields.items()}}
                if self._kinds:
                    row.update({f"{key}_kind": kinds.get(row.get(column)) for column, key in self._fields.items()})
                yield row


def with_payloads(project: Any, rows: Iterable[dict], fields: dict[str, str], window: int = READ_WINDOW,
                  kinds: bool = False):
    """A job's source whose rows carry the bytes of their payloads, so a task gets bytes and never a path.

    `fields` maps an id column to the key that gets its bytes (`{"current_payload_id": "ligand"}`;
    a NULL id gives `None`). Rows are read `window` at a time in (volume, offset) order, so at most
    that many hold bytes here, and the engine reads only its window ahead (I8). What `rows` knows
    (`open`, `remaining`, `reads` of `ms_flow.query.Rows`) passes through. With `kinds`, each key
    also gets `<key>_kind`, the kind its payload was stored with, so a task knows how to parse it.
    """
    from ms_flow.core.database import resolve_project_store

    return _WithPayloads(resolve_project_store(project).payloads, rows, fields, window, kinds)


def compaction(min_dead: float = 0.5, order_by: str | None = None):
    """`PayloadStore.compact` as a job of ms_flow, `flow.submit(compaction())`: it waits for the
    jobs writing payloads (every table writer says it writes `ms_payloads`), and a job reading
    `ms_payloads` or `ms_volumes` waits for it. It runs in the process that owns the project (I4)."""
    from ms_flow.engine.flow import job, task

    class Compactor:
        tables = ("ms_payloads", "ms_volumes")

        def __init__(self, params, project):
            from ms_flow.core.database import resolve_project_store

            self.store = resolve_project_store(project).payloads

        def write(self, results):
            pass

        def finish(self):
            self.stats = self.store.compact(min_dead, order_by=order_by)

    @task(thread=True)
    def nothing(element):
        return None

    return job(task=nothing, name="compact payloads", output=Compactor, reads=("ms_payloads",))(
        lambda params, project: [None])


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
