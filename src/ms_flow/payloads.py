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
import zlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows
    fcntl = None

VOLUME_SIZE = 1 << 30  # a payload larger than this goes alone in its own volume

MAGIC = b"MSVOL"
VERSION = 1
CODEC_NONE = 0
_VOLUME_HEADER = struct.Struct("<5sBHQ16s")  # magic, version, reserved, volume id, series
_NEEDLE = struct.Struct("<IIIHH")  # crc32, stored length, raw length, codec, flags
HEADER = _VOLUME_HEADER.size
NEEDLE = _NEEDLE.size
_IOV_MAX = 1024


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

    def close(self) -> None:
        for volume in self._active.values():
            os.close(volume.fd)
        self._active.clear()


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
