"""Shards: a compact, reliable, indexed container for many small byte records.

The problem it solves is *file count*, not file format. A screening library or a stage of a
pipeline produces millions of tiny payloads; as one file each they exhaust inodes, copy at
metadata speed and cannot be moved anywhere as a unit. A shard packs a contiguous range of
them into one mmap-backed file that can be checksummed, shipped to a cluster and read back
record by record without unpacking. It is a container, not an interchange format: nothing
outside this module is expected to open it.

What the container guarantees:

* **Indexed access.** A record is addressed by its logical id (``base_id .. base_id +
  slot_count - 1``), not by its position. Ids may be missing — a filtered-out molecule, a
  failed preparation — and the shard records that instead of silently renumbering. When every
  id in the range is present the shard is *dense* and pays nothing for the slot map.
* **Reliability.** The writer commits with ``os.replace()`` onto the final path only after
  everything is fsynced, so a crash leaves a ``.tmp`` and never a half-written shard, and a
  BLAKE2b-256 footer lets a reader prove a shard survived the trip (``verify()``).
* **Opacity.** The core has no idea what a record *is*. ``kind``, ``serializer`` and ``codec``
  are u16 header values whose meaning belongs to the caller; ``add()``/``view()`` see ``bytes``.

Limits, both from the u16 slot map: at most 65 535 records per shard, and 65 535 slots. Both
are checked when the writer opens, not when it closes.
"""

from __future__ import annotations

import hashlib
import json
import mmap
import os
import struct
import sys
import uuid
from array import array
from pathlib import Path
from typing import Any, Callable, Iterator

__all__ = [
    "Codec",
    "PayloadKind",
    "Record",
    "Serializer",
    "Shard",
    "ShardWriter",
    "get_serializer",
    "register",
]

MAGIC = b"MSHARD"
VERSION = 1
NOT_PRESENT = 0xFFFF
MAX_RECORDS = NOT_PRESENT  # a record index must be distinguishable from "absent"
DIGEST_SIZE = 32  # BLAKE2b-256
_HASH_CHUNK = 1 << 20

# "<6sH16sIIQIHHHQ": magic, version, dataset_uuid, shard_id, slot_count, base_id,
# record_count, kind, serializer, codec, index_off. No native padding: '<' is unaligned.
_HEADER_FMT = "<6sH16sIIQIHHHQ"
HEADER_SIZE = struct.calcsize(_HEADER_FMT)  # 58

# offset, stored_size, original_size. The last two are equal while codec is NONE; the field
# stays because it is *on disk*, and adding it later would mean a format version. The rule for
# this module: what costs a v2 to add is kept, what costs a line to add is not.
_RECORD_FMT = "<QII"
_RECORD_SIZE = struct.calcsize(_RECORD_FMT)


class PayloadKind:
    """What a record *represents*. Opaque to the core — meaning lives in the caller."""

    GENERIC_BYTES = 0
    PDBQT = 3
    SDF = 4
    SMILES = 7


class Serializer:
    """How a record's bytes decode back into a Python object; see `register`."""

    RAW = 0
    UTF8 = 1


class Codec:
    """How a record's bytes are stored. Only NONE exists; the field reserves the layout."""

    NONE = 0


# ---------------------------------------------------------------------------
# Serializer registry — how record.load() turns bytes back into an object.
# ---------------------------------------------------------------------------

_SERIALIZERS: dict[int, tuple[Callable[[Any], bytes], Callable[[bytes], Any]]] = {}


def register(
    serializer_id: int,
    dumps_fn: Callable[[Any], bytes],
    loads_fn: Callable[[bytes], Any],
) -> None:
    """Bind a serializer id to a (dumps, loads) pair. Last registration for an id wins.

    This is the extension point: a caller that stores, say, RDKit binary molecules registers
    its own id here instead of this module growing a dependency on RDKit.
    """
    _SERIALIZERS[int(serializer_id)] = (dumps_fn, loads_fn)


def get_serializer(serializer_id: int) -> tuple[Callable[[Any], bytes], Callable[[bytes], Any]]:
    try:
        return _SERIALIZERS[int(serializer_id)]
    except KeyError:
        raise KeyError(
            f"no serializer registered for id={serializer_id!r}; call shard.register() first"
        ) from None


register(Serializer.RAW, bytes, bytes)
register(Serializer.UTF8, lambda s: s.encode("utf-8"), lambda b: bytes(b).decode("utf-8"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _uuid_bytes(dataset_id: uuid.UUID | str | bytes | None) -> bytes:
    if dataset_id is None:
        return b"\x00" * 16
    if isinstance(dataset_id, uuid.UUID):
        return dataset_id.bytes
    if isinstance(dataset_id, (bytes, bytearray)):
        raw = bytes(dataset_id)
        if len(raw) != 16:
            raise ValueError(f"dataset_id bytes must be exactly 16 bytes, got {len(raw)}")
        return raw
    return uuid.UUID(str(dataset_id)).bytes


def _check_uint(name: str, value: int, bits: int) -> int:
    value = int(value)
    if not 0 <= value < (1 << bits):
        raise ValueError(f"{name}={value} does not fit in u{bits}")
    return value


def _hash_file_range(read_into, size: int) -> bytes:
    """BLAKE2b-256 over `size` bytes, a megabyte at a time — never the whole file in RAM."""
    hasher = hashlib.blake2b(digest_size=DIGEST_SIZE)
    for start in range(0, size, _HASH_CHUNK):
        hasher.update(read_into(start, min(_HASH_CHUNK, size - start)))
    return hasher.digest()


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------


class ShardWriter:
    """Builds one `.mshard` file. Ids must be added in strictly increasing order.

    Commits atomically: writes to ``<path>.tmp`` and ``os.replace()``s it onto ``path`` only
    once DATA, INDEX, META, header and footer are all flushed to disk. A crash mid-write leaves
    only the ``.tmp`` behind — ``path`` never exists half-written.

    ``slot_count`` is the *capacity* of the id range; the writer shrinks it on close to the
    highest id actually written, so a partial last shard with contiguous ids ends up dense and
    pays no slot map.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        dataset_id: uuid.UUID | str | bytes | None,
        shard_id: int,
        base_id: int,
        kind: int,
        serializer: int,
        slot_count: int = 4096,
        codec: int = Codec.NONE,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if codec != Codec.NONE:
            raise ValueError("shard v1 only writes codec=Codec.NONE (uncompressed)")
        slot_count = int(slot_count)
        if not 0 < slot_count <= MAX_RECORDS:
            # Not a soft limit: a record index of 0xFFFF is indistinguishable from "absent",
            # so a bigger shard would lose records silently. Split the range instead.
            raise ValueError(f"slot_count must be in 1..{MAX_RECORDS}, got {slot_count}")

        self.path = Path(path)
        self._tmp_path = self.path.with_name(self.path.name + ".tmp")
        self.dataset_id = _uuid_bytes(dataset_id)
        self.shard_id = _check_uint("shard_id", shard_id, 32)
        self.base_id = int(base_id)
        self.slot_count = slot_count
        self.kind = _check_uint("kind", kind, 16)
        self.serializer = _check_uint("serializer", serializer, 16)
        self.codec = _check_uint("codec", codec, 16)
        self.metadata = dict(metadata or {})

        self._records: list[tuple[int, int, int]] = []
        self._slots: list[int] = []  # parallel to _records; ids arrive sorted
        self._closed = False

        self._fh = open(self._tmp_path, "w+b")
        self._fh.write(b"\x00" * HEADER_SIZE)
        self._offset = HEADER_SIZE

    def add(self, id_: int, data: bytes) -> None:
        if self._closed:
            raise ValueError(f"{self.path}: shard already closed")
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(
                f"shard records must be bytes-like, got {type(data).__name__}; "
                "encode/serialize before calling add()"
            )
        id_ = int(id_)
        if id_ < self.base_id or id_ >= self.base_id + self.slot_count:
            raise ValueError(
                f"id {id_} out of range [{self.base_id}, {self.base_id + self.slot_count})"
            )
        if self._slots and id_ - self.base_id <= self._slots[-1]:
            raise ValueError(
                f"ids must be strictly increasing: {id_} after {self.base_id + self._slots[-1]}"
            )

        raw = bytes(data)
        self._slots.append(id_ - self.base_id)
        self._records.append((self._offset, len(raw), len(raw)))
        self._fh.write(raw)
        self._offset += len(raw)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            record_count = len(self._records)
            # Trailing empty slots are not information: nothing distinguishes "id never came"
            # from "id past the end". Shrinking makes the contiguous case dense.
            slot_count = self._slots[-1] + 1 if self._slots else 0
            dense = record_count == slot_count
            index_off = self._offset

            if not dense:
                slot_map = array("H", [NOT_PRESENT]) * slot_count
                for record_index, slot in enumerate(self._slots):
                    slot_map[slot] = record_index
                if sys.byteorder != "little":
                    slot_map.byteswap()
                self._fh.write(slot_map.tobytes())

            self._fh.write(b"".join(struct.pack(_RECORD_FMT, *r) for r in self._records))
            self._fh.write(json.dumps(self.metadata).encode("utf-8"))
            end_offset = self._fh.tell()

            self._fh.seek(0)
            self._fh.write(
                struct.pack(
                    _HEADER_FMT,
                    MAGIC,
                    VERSION,
                    self.dataset_id,
                    self.shard_id,
                    slot_count,
                    self.base_id,
                    record_count,
                    self.kind,
                    self.serializer,
                    self.codec,
                    index_off,
                )
            )
            self._fh.flush()
            os.fsync(self._fh.fileno())

            def _read(start: int, size: int) -> bytes:
                self._fh.seek(start)
                return self._fh.read(size)

            self._fh.seek(end_offset)
            self._fh.write(_hash_file_range(_read, end_offset))
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self.slot_count = slot_count
        finally:
            self._fh.close()

        os.replace(self._tmp_path, self.path)
        _fsync_dir(self.path.parent)

    def __enter__(self) -> "ShardWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is not None:
            self._closed = True
            self._fh.close()
            self._tmp_path.unlink(missing_ok=True)
            return False
        self.close()
        return False


def _fsync_dir(directory: Path) -> None:
    """Make the rename itself durable. Without this a crash can lose a committed shard."""
    try:
        fd = os.open(directory, os.O_RDONLY)
    except OSError:  # Windows, and any filesystem that won't open a directory
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------


class Record:
    """A view onto one stored blob. Cheap: holds only a shard reference and an index."""

    __slots__ = ("_shard", "_index")

    def __init__(self, shard: "Shard", index: int) -> None:
        self._shard = shard
        self._index = index

    @property
    def id(self) -> int:
        return self._shard._id_for_record(self._index)

    def view(self) -> memoryview:
        """Zero-copy slice of the mmap. Only valid while the shard is open — see `Shard.close`."""
        if self._shard.codec != Codec.NONE:
            raise NotImplementedError("compressed records don't support zero-copy view()")
        offset, stored_size, _original_size = self._shard._records[self._index]
        return self._shard._view[offset : offset + stored_size]

    def load(self) -> Any:
        _dumps_fn, loads_fn = get_serializer(self._shard.serializer)
        return loads_fn(bytes(self.view()))

    def __repr__(self) -> str:
        return f"Record(id={self.id}, index={self._index})"


class Shard:
    """A read-only, memory-mapped `.mshard` file. Open once, index for the life of the mmap."""

    def __init__(
        self,
        path: Path,
        fh,
        mm: mmap.mmap,
        *,
        dataset_id: bytes,
        shard_id: int,
        slot_count: int,
        base_id: int,
        record_count: int,
        kind: int,
        serializer: int,
        codec: int,
        index_off: int,
        slot_to_record: "array[int] | None",
        records: list[tuple[int, int, int]],
        record_to_slot: list[int] | None,
        metadata: dict[str, Any],
        file_size: int,
    ) -> None:
        self.path = path
        self._fh = fh
        self._mmap = mm
        self._view = memoryview(mm)
        self.dataset_id = uuid.UUID(bytes=dataset_id)
        self.shard_id = shard_id
        self.slot_count = slot_count
        self.base_id = base_id
        self.record_count = record_count
        self.kind = kind
        self.serializer = serializer
        self.codec = codec
        self.index_off = index_off
        self.metadata = metadata
        self._dense = slot_to_record is None
        self._slot_to_record = slot_to_record
        self._records = records
        self._record_to_slot = record_to_slot
        self._file_size = file_size
        self._closed = False

    @classmethod
    def open(cls, path: str | Path, *, dataset_id: uuid.UUID | str | bytes | None = None) -> "Shard":
        path = Path(path)
        fh = open(path, "rb")
        try:
            file_size = os.fstat(fh.fileno()).st_size
            if file_size < HEADER_SIZE + DIGEST_SIZE:
                raise ValueError(f"{path}: too small to be a .mshard file ({file_size} bytes)")
            mm = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)
        except Exception:
            fh.close()
            raise

        try:
            (
                magic,
                version,
                uuid_bytes,
                shard_id,
                slot_count,
                base_id,
                record_count,
                kind,
                serializer,
                codec,
                index_off,
            ) = struct.unpack(_HEADER_FMT, mm[:HEADER_SIZE])

            if magic != MAGIC:
                raise ValueError(f"{path}: bad magic {magic!r}, not a .mshard file")
            if version != VERSION:
                raise ValueError(f"{path}: unsupported format version {version}")
            if dataset_id is not None and _uuid_bytes(dataset_id) != uuid_bytes:
                raise ValueError(f"{path}: dataset_id mismatch")
            if not HEADER_SIZE <= index_off <= file_size - DIGEST_SIZE:
                raise ValueError(f"{path}: index_off {index_off} outside file bounds (truncated?)")

            dense = record_count == slot_count
            pos = index_off
            slot_to_record: array | None = None
            record_to_slot: list[int] | None = None
            if not dense:
                slot_to_record = array("H")
                slot_to_record.frombytes(bytes(mm[pos : pos + slot_count * 2]))
                if sys.byteorder != "little":
                    slot_to_record.byteswap()
                pos += slot_count * 2
                record_to_slot = [-1] * record_count
                for slot, record_index in enumerate(slot_to_record):
                    if record_index != NOT_PRESENT:
                        record_to_slot[record_index] = slot

            record_bytes = bytes(mm[pos : pos + record_count * _RECORD_SIZE])
            records = [
                struct.unpack_from(_RECORD_FMT, record_bytes, i * _RECORD_SIZE)
                for i in range(record_count)
            ]
            pos += record_count * _RECORD_SIZE

            meta_bytes = bytes(mm[pos : file_size - DIGEST_SIZE])
            metadata = json.loads(meta_bytes.decode("utf-8")) if meta_bytes else {}
        except Exception:
            mm.close()
            fh.close()
            raise

        return cls(
            path,
            fh,
            mm,
            dataset_id=uuid_bytes,
            shard_id=shard_id,
            slot_count=slot_count,
            base_id=base_id,
            record_count=record_count,
            kind=kind,
            serializer=serializer,
            codec=codec,
            index_off=index_off,
            slot_to_record=slot_to_record,
            records=records,
            record_to_slot=record_to_slot,
            metadata=metadata,
            file_size=file_size,
        )

    def _id_for_record(self, record_index: int) -> int:
        if self._dense:
            return self.base_id + record_index
        return self.base_id + self._record_to_slot[record_index]

    def _record_index_for_id(self, id_: int) -> int | None:
        slot = id_ - self.base_id
        if slot < 0 or slot >= self.slot_count:
            return None
        if self._dense:
            return slot
        record_index = self._slot_to_record[slot]
        return None if record_index == NOT_PRESENT else record_index

    def __contains__(self, id_: int) -> bool:
        return self._record_index_for_id(int(id_)) is not None

    def __len__(self) -> int:
        return self.record_count

    def __getitem__(self, id_: int) -> Record:
        record_index = self._record_index_for_id(int(id_))
        if record_index is None:
            raise KeyError(id_)
        return Record(self, record_index)

    def __iter__(self) -> Iterator[Record]:
        for record_index in range(self.record_count):
            yield Record(self, record_index)

    def iter_views(self) -> Iterator[memoryview]:
        """The sequential fast path: memoryviews straight off the mmap, no Record objects.

        The views borrow the shard's buffer, so they must not outlive it: `close()` raises
        `BufferError` while any is alive. Consume them inside the loop, or keep `bytes(view)`.
        """
        view = self._view
        for offset, stored_size, _original_size in self._records:
            yield view[offset : offset + stored_size]

    def verify(self) -> bool:
        """BLAKE2b-256 over the whole file except the footer itself. Not run on open()."""
        footer_off = self._file_size - DIGEST_SIZE
        stored_digest = bytes(self._view[footer_off : self._file_size])
        digest = _hash_file_range(lambda start, size: self._view[start : start + size], footer_off)
        return digest == stored_digest

    def close(self) -> None:
        """Release the mapping. Raises `BufferError` if any record view is still alive."""
        if self._closed:
            return
        self._view.release()
        self._mmap.close()
        self._fh.close()
        self._closed = True

    def __enter__(self) -> "Shard":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False

    def __repr__(self) -> str:
        return (
            f"Shard(path={self.path!s}, shard_id={self.shard_id}, "
            f"base_id={self.base_id}, slot_count={self.slot_count}, "
            f"record_count={self.record_count})"
        )
