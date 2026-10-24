"""Shards: a compact, reliable, indexed container for many small byte records.

The problem it solves is *file count*, not file format. A screening library or a stage of a
pipeline produces millions of tiny payloads; as one file each they exhaust inodes, copy at
metadata speed and cannot be moved anywhere as a unit. A shard packs a contiguous range of
them into one Parquet file that can be checksummed, shipped to a cluster, read back record by
record — and queried in place by DuckDB or pyarrow (``SELECT id, record FROM 'shards/*.parquet'``).

The file is plain Parquet: an ``id`` column (uint64) and a ``record`` column (``string`` when
the serializer is UTF-8, ``binary`` otherwise), zstd-compressed, with the header below as JSON
under the ``ms_flow.shard`` key of the file metadata.

What the container guarantees:

* **Indexed access.** A record is addressed by its logical id (``base_id .. base_id +
  slot_count - 1``), not by its position. Ids may be missing — a filtered-out molecule, a
  failed preparation — and the shard records that instead of silently renumbering.
* **Reliability.** The writer commits with ``os.replace()`` onto the final path only after
  everything is fsynced, so a crash leaves a ``.tmp`` and never a half-written shard. Every
  data page carries a CRC that `Shard.open` checks, so a damaged shard fails to open instead of
  handing back wrong bytes.
* **Opacity.** The core has no idea what a record *is*. ``kind``, ``serializer`` and ``codec``
  are integers whose meaning belongs to the caller; ``add()``/``view()`` see ``bytes``.

pyarrow is imported on first write or open, so the modules that only need `_fsync_dir` (and a
worker that never touches a shard) do not pay for it.
"""

from __future__ import annotations

import json
import os
import uuid
from bisect import bisect_left
from pathlib import Path
from typing import Any, Callable, Iterator

__all__ = [
    "Codec",
    "MAX_RECORDS",
    "PayloadKind",
    "Record",
    "Serializer",
    "Shard",
    "ShardWriter",
    "get_serializer",
    "register",
]

FORMAT = "ms_flow-shard-parquet-v1"
METADATA_KEY = b"ms_flow.shard"
# ponytail: the writer holds the whole shard in memory until close, so the range is capped;
# a streaming writer (one row group per batch) lifts this if shards ever need to grow.
MAX_RECORDS = 0xFFFF
_LEGACY_MAGIC = b"MSHARD"


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
    """How a record's bytes are stored inside the column. Parquet compresses the whole column,
    so only NONE exists; the field stays so a caller can tell the layout apart if one is added."""

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


def _uuid(dataset_id: uuid.UUID | str | bytes | None) -> uuid.UUID:
    if dataset_id is None:
        return uuid.UUID(int=0)
    if isinstance(dataset_id, uuid.UUID):
        return dataset_id
    if isinstance(dataset_id, (bytes, bytearray)):
        if len(dataset_id) != 16:
            raise ValueError(f"dataset_id bytes must be exactly 16 bytes, got {len(dataset_id)}")
        return uuid.UUID(bytes=bytes(dataset_id))
    return uuid.UUID(str(dataset_id))


def _check_uint(name: str, value: int, bits: int) -> int:
    value = int(value)
    if not 0 <= value < (1 << bits):
        raise ValueError(f"{name}={value} does not fit in u{bits}")
    return value


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
# Writer
# ---------------------------------------------------------------------------


class ShardWriter:
    """Builds one shard file. Ids must be added in strictly increasing order.

    Commits atomically: writes ``<path>.tmp``, fsyncs it and ``os.replace()``s it onto
    ``path``. A crash mid-write leaves only the ``.tmp`` behind — ``path`` never exists
    half-written. ``metadata`` may still be edited until the writer closes.

    ``slot_count`` is the *capacity* of the id range; the writer shrinks it on close to the
    highest id actually written, so a partial last shard does not claim ids it never had.
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
            raise ValueError("shards only write codec=Codec.NONE; Parquet compresses the column")
        slot_count = int(slot_count)
        if not 0 < slot_count <= MAX_RECORDS:
            raise ValueError(f"slot_count must be in 1..{MAX_RECORDS}, got {slot_count}")

        self.path = Path(path)
        self._tmp_path = self.path.with_name(self.path.name + ".tmp")
        self.dataset_id = _uuid(dataset_id)
        self.shard_id = _check_uint("shard_id", shard_id, 32)
        self.base_id = int(base_id)
        self.slot_count = slot_count
        self.kind = _check_uint("kind", kind, 16)
        self.serializer = _check_uint("serializer", serializer, 16)
        self.codec = codec
        self.metadata = dict(metadata or {})
        self._ids: list[int] = []
        self._records: list[bytes] = []
        self._closed = False

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
        if self._ids and id_ <= self._ids[-1]:
            raise ValueError(f"ids must be strictly increasing: {id_} after {self._ids[-1]}")
        self._ids.append(id_)
        self._records.append(bytes(data))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        import pyarrow as pa
        import pyarrow.parquet as pq

        # Trailing empty slots are not information: nothing distinguishes "id never came"
        # from "id past the end".
        self.slot_count = self._ids[-1] - self.base_id + 1 if self._ids else 0
        records = pa.array(self._records, type=pa.binary())
        if self.serializer == Serializer.UTF8:
            records = records.cast(pa.string())  # text DuckDB can read; also validates the UTF-8
        header = {
            "format": FORMAT,
            "dataset_id": str(self.dataset_id),
            "shard_id": self.shard_id,
            "base_id": self.base_id,
            "slot_count": self.slot_count,
            "record_count": len(self._ids),
            "kind": self.kind,
            "serializer": self.serializer,
            "codec": self.codec,
            "metadata": self.metadata,
        }
        table = pa.table(
            {"id": pa.array(self._ids, type=pa.uint64()), "record": records}
        ).replace_schema_metadata({METADATA_KEY: json.dumps(header).encode("utf-8")})
        self._ids, self._records = [], []
        try:
            with open(self._tmp_path, "wb") as fh:
                pq.write_table(table, fh, compression="zstd", write_page_checksum=True)
                fh.flush()
                os.fsync(fh.fileno())
        except BaseException:
            self._tmp_path.unlink(missing_ok=True)
            raise
        os.replace(self._tmp_path, self.path)
        _fsync_dir(self.path.parent)

    def __enter__(self) -> "ShardWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is not None:
            self._closed = True  # nothing was written yet: the records only lived in memory
            self._tmp_path.unlink(missing_ok=True)
            return False
        self.close()
        return False


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
        return self._shard._ids[self._index]

    def view(self) -> memoryview:
        """Zero-copy view of the decoded column. Only valid while the shard is open."""
        return memoryview(self._shard._records[self._index].as_buffer())

    def load(self) -> Any:
        _dumps_fn, loads_fn = get_serializer(self._shard.serializer)
        return loads_fn(bytes(self.view()))

    def __repr__(self) -> str:
        return f"Record(id={self.id}, index={self._index})"


class Shard:
    """A read-only shard, decoded into memory on open (a shard is a few MB)."""

    def __init__(self, path: Path, header: dict[str, Any], ids: list[int], records: Any) -> None:
        self.path = path
        self.dataset_id = uuid.UUID(str(header["dataset_id"]))
        self.shard_id = int(header["shard_id"])
        self.slot_count = int(header["slot_count"])
        self.base_id = int(header["base_id"])
        self.record_count = int(header["record_count"])
        self.kind = int(header["kind"])
        self.serializer = int(header["serializer"])
        self.codec = int(header["codec"])
        self.metadata = dict(header.get("metadata") or {})
        self._ids = ids
        self._records = records

    @classmethod
    def open(cls, path: str | Path, *, dataset_id: uuid.UUID | str | bytes | None = None) -> "Shard":
        import pyarrow as pa
        import pyarrow.parquet as pq

        path = Path(path)
        try:
            table = pq.read_table(path, page_checksum_verification=True)
        except (OSError, pa.ArrowException) as exc:  # a failed page CRC is an OSError
            with open(path, "rb") as fh:  # a missing file raises here, as itself
                if fh.read(len(_LEGACY_MAGIC)) == _LEGACY_MAGIC:
                    raise ValueError(
                        f"{path}: a .mshard container from an older version; re-import the library"
                    ) from None
            raise ValueError(f"{path}: unreadable shard ({exc})") from exc
        raw = (table.schema.metadata or {}).get(METADATA_KEY)
        if raw is None:
            raise ValueError(f"{path}: Parquet file without a shard header, not a shard")
        header = json.loads(raw.decode("utf-8"))
        if header.get("format") != FORMAT:
            raise ValueError(f"{path}: unsupported shard format {header.get('format')!r}")
        if dataset_id is not None and _uuid(dataset_id) != uuid.UUID(str(header["dataset_id"])):
            raise ValueError(f"{path}: dataset_id mismatch")
        records = table.column("record").combine_chunks().cast(pa.binary())
        return cls(path, header, table.column("id").to_pylist(), records)

    def _record_index_for_id(self, id_: int) -> int | None:
        index = bisect_left(self._ids, id_)
        return index if index < len(self._ids) and self._ids[index] == id_ else None

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
        """The sequential fast path: memoryviews of the decoded column, no Record objects."""
        for value in self._records:
            yield memoryview(value.as_buffer())

    def verify(self) -> bool:
        """Re-read the file checking every page CRC: the shard on disk is still the one written."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        try:
            pq.read_table(self.path, page_checksum_verification=True)
        except (OSError, pa.ArrowException):
            return False
        return True

    def close(self) -> None:
        self._records = None

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
