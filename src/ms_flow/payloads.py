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
import struct
import threading
import zlib
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows
    fcntl = None

VOLUME_SIZE = 1 << 30  # a payload larger than this goes alone in its own volume
MAX_OPEN_FDS = 256  # volumes a reader keeps open
LOCATE_CHUNK = 900  # ids per location query, under SQLite's variable limit

MAGIC = b"MSVOL"
VERSION = 1
CODEC_NONE = 0
_VOLUME_HEADER = struct.Struct("<5sBHQ16s")  # magic, version, reserved, volume id, series
_NEEDLE = struct.Struct("<IIIHH")  # crc32, stored length, raw length, codec, flags
HEADER = _VOLUME_HEADER.size
NEEDLE = _NEEDLE.size
_IOV_MAX = 1024


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


class PayloadStore:
    """Writes and reads the payloads of one project."""

    def __init__(self, project: Any):
        from ms_flow.core.database import ProjectStore, resolve_project_store

        if isinstance(project, (str, os.PathLike)):
            store = ProjectStore.open_cached(ProjectStore.resolve_db_path(project))
        else:
            store = resolve_project_store(project)
        self.engine = store.engine
        self.directory = Path(store.db_path).parent / "volumes"
        self._active: dict[str, _Volume] = {}
        self._retired: list[_Volume] = []  # sealed by this writer; closed once their bytes are written
        self._fds: OrderedDict[int, list] = OrderedDict()  # volume id -> [fd, users, evicted]
        self._fd_lock = threading.Lock()

    def path(self, volume_id: int) -> Path:
        return self.directory / f"{volume_id:09d}.vol"

    # -- writing ---------------------------------------------------------

    def put(self, session: Any, payloads: Iterable[Payload]) -> list[int]:
        """Store `payloads` inside the caller's transaction; return their ids, in order."""
        payloads = list(payloads)
        if not payloads:
            return []
        cursor = session.connection().connection.dbapi_connection.cursor()
        try:
            pending: dict[_Volume, list] = {}
            placed = []
            for payload in payloads:
                data = payload.data
                size = len(data)
                volume = self._volume_for(cursor, payload.series, NEEDLE + size)
                crc = zlib.crc32(data)
                pending.setdefault(volume, [volume.end, []])[1] += (
                    _NEEDLE.pack(crc, size, size, CODEC_NONE, 0),
                    data,
                )
                placed.append((volume.id, volume.end, size, size, CODEC_NONE, crc, payload.kind))
                volume.end += NEEDLE + size
            for volume, (offset, buffers) in pending.items():
                _write_all(volume.fd, buffers, offset)
                cursor.execute("UPDATE ms_volumes SET size = ? WHERE id = ?", (volume.end, volume.id))
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
            for volume in self._retired:
                os.close(volume.fd)
            self._retired.clear()

    def _volume_for(self, cursor, series: str, need: int) -> _Volume:
        volume = self._active.get(series)
        if volume is not None and volume.end > HEADER and volume.end + need > VOLUME_SIZE:
            cursor.execute(
                "UPDATE ms_volumes SET state = 'sealed', size = ? WHERE id = ?", (volume.end, volume.id)
            )
            self._retired.append(volume)
            volume = None
        if volume is None:
            volume = self._active[series] = self._create(cursor, series)
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
        for volume in self._active.values():
            os.close(volume.fd)
        self._active.clear()
        with self._fd_lock:
            for entry in self._fds.values():
                entry[2] = True
                if entry[1] == 0:
                    os.close(entry[0])
            self._fds.clear()


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
