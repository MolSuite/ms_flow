"""Shards: a compact, reliable, indexed container for many small byte records.

The problem it solves is *file count*, not file format. A screening library or a stage of a
pipeline produces millions of tiny payloads; as one file each they exhaust inodes, copy at
metadata speed and cannot be moved anywhere as a unit. A shard packs a contiguous range of
them into one Parquet file that can be checksummed, shipped to a cluster, read back record by
record — and queried in place by DuckDB or pyarrow (``SELECT id, record FROM 'shards/*.parquet'``).

The file is plain Parquet: an ``id`` column (uint64) and a ``record`` column (``string`` when
the serializer is UTF-8, ``binary`` otherwise), zstd-compressed, with the header below as JSON
under the ``ms_flow.shard`` key of the file metadata. An optional ``source_index`` column
(uint64) keeps where each record came from in its source file.

One file may hold many *logical* shards: contiguous id ranges, each closed as its own row group
(`ShardWriter.end_group`). ``Shard.open(path, ids=(lo, hi))`` reads only the row groups whose
``id`` statistics overlap the range, so a library of millions sits in a few big files instead of
one small file per shard. The file lists its shards itself, in the header's ``groups``
(`file_groups`): ``[base_id, slot_count, record_count, scope]`` each. A catalog keeps one row
per *file*, never one per shard; what is inside is read from here.

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
    "file_groups",
    "merge_shards",
    "register",
]

FORMAT = "ms_flow-shard-parquet-v1"
METADATA_KEY = b"ms_flow.shard"
# ponytail: the writer streams, but `Shard.open` still decodes the whole file into memory, so
# the id range stays capped; a reader that maps row groups lifts this if shards ever need more.
MAX_RECORDS = 1 << 20
# The writer sends a row group to disk once this many record bytes wait in memory, so a shard of
# any size is built holding one row group at most.
_ROW_GROUP_BYTES = 16 << 20
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

    Records stream to ``<path>.tmp`` a row group at a time, so memory stays flat however big the
    shard. Commits atomically: on close the header goes into the file's metadata, the ``.tmp`` is
    fsynced and ``os.replace()``d onto ``path``. A crash mid-write leaves only the ``.tmp``
    behind — ``path`` never exists half-written. ``metadata`` may still be edited until the
    writer closes.

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
        source_index: bool = False,
        scope: int | None = None,
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
        self._with_source = bool(source_index)
        self.scope = scope  # of the groups closed without one: whatever the app numbers records in
        self._groups: list[list[Any]] = []  # [base_id, slot_count, record_count, scope]: the shards
        self._group_first: int | None = None  # the first id of the shard being filled
        self._group_count = 0
        self._ids: list[int] = []  # the row group being filled
        self._records: list[bytes] = []
        self._sources: list[int] = []
        self._buffered = 0
        self._count = 0
        self._last: int | None = None
        self._file: Any = None
        self._writer: Any = None
        self._closed = False

    def add(self, id_: int, data: bytes, source_index: int | None = None) -> None:
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
        if self._last is not None and id_ <= self._last:
            raise ValueError(f"ids must be strictly increasing: {id_} after {self._last}")
        if self._with_source:
            if source_index is None:
                raise ValueError("this writer stores a source_index per record; pass one")
            self._sources.append(int(source_index))
        self._ids.append(id_)
        self._records.append(bytes(data))
        if self._group_first is None:
            self._group_first = id_
        self._group_count += 1
        self._last, self._count, self._buffered = id_, self._count + 1, self._buffered + len(data)
        if self._buffered >= _ROW_GROUP_BYTES:
            self._flush()

    def end_group(self, *, scope: int | None = None, base_id: int | None = None,
                  slot_count: int | None = None) -> None:
        """Close the shard being filled: its own row group, and an entry in the header's
        ``groups``. Its ids are ``[base_id, base_id + slot_count)``, by default from its first
        id to its last; pass them to keep the range of the shard it came from, holes and all (a
        prepared twin whose last molecule failed, or that failed whole, still covers its parent's
        range). ``scope`` defaults to the writer's."""
        if self._ids:
            self._flush()
        if base_id is None and self._group_first is None:
            return  # nothing added and no range to claim: no shard
        # By default a shard starts at its first id; the first one at the writer's base_id, so a
        # file written as one shard keeps the holes before its first record, as its header does.
        lo = int(base_id) if base_id is not None else self.base_id if not self._groups else int(self._group_first)
        hi = lo + int(slot_count) if slot_count is not None else int(self._last) + 1
        self._groups.append([lo, hi - lo, self._group_count, self.scope if scope is None else scope])
        self._group_first, self._group_count = None, 0

    def _flush(self) -> None:
        """The waiting records as one row group; the first call opens the ``.tmp``."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        try:
            text = self.serializer == Serializer.UTF8  # text DuckDB can read
            if self._writer is None:
                self._file = open(self._tmp_path, "wb")
                fields = [("id", pa.uint64()), ("record", pa.string() if text else pa.binary())]
                if self._with_source:
                    fields.append(("source_index", pa.uint64()))
                # Statistics on `id` only: they are what finds a shard's row groups; a min/max over
                # the record text is most of the footer and nothing filters on it.
                self._writer = pq.ParquetWriter(self._file, pa.schema(fields), compression="zstd",
                                                write_page_checksum=True, write_statistics=["id"])
            if self._ids:
                records = pa.array(self._records, type=pa.binary())
                if text:
                    records = records.cast(pa.string())  # also validates the UTF-8
                columns = {"id": pa.array(self._ids, type=pa.uint64()), "record": records}
                if self._with_source:
                    columns["source_index"] = pa.array(self._sources, type=pa.uint64())
                self._writer.write_table(pa.table(columns), row_group_size=len(self._ids))
        except BaseException:
            self._discard()
            raise
        self._ids, self._records, self._sources, self._buffered = [], [], [], 0

    def _discard(self) -> None:
        """Drop what was written: the ``.tmp`` goes, ``path`` is never touched."""
        self._closed = True
        self._ids, self._records, self._sources = [], [], []
        for closing in (self._writer, self._file):
            try:
                if closing is not None:
                    closing.close()
            except Exception:
                pass
        self._writer = self._file = None
        self._tmp_path.unlink(missing_ok=True)

    def close(self) -> None:
        if self._closed:
            return
        if self._group_first is not None:
            self.end_group()  # what was added since the last end_group is a shard too
        self._flush()  # the last row group (and the file itself, when nothing has gone yet)
        # Trailing empty slots are not information: nothing distinguishes "id never came"
        # from "id past the end".
        self.slot_count = self._last - self.base_id + 1 if self._last is not None else 0
        header = {
            "format": FORMAT,
            "dataset_id": str(self.dataset_id),
            "shard_id": self.shard_id,
            "base_id": self.base_id,
            "slot_count": self.slot_count,
            "record_count": self._count,
            "kind": self.kind,
            "serializer": self.serializer,
            "codec": self.codec,
            "metadata": self.metadata,
            "groups": self._groups,
        }
        try:
            self._writer.add_key_value_metadata({METADATA_KEY: json.dumps(header).encode("utf-8")})
            self._writer.close()
            self._file.flush()
            os.fsync(self._file.fileno())
            self._file.close()
        except BaseException:
            self._discard()
            raise
        self._closed = True
        self._writer = self._file = None
        os.replace(self._tmp_path, self.path)
        _fsync_dir(self.path.parent)

    def __enter__(self) -> "ShardWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is not None:
            self._discard()
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


# path -> ((mtime_ns, size), footer, [(min id, max id) per row group]). A worker reading many
# shards of one big file parses its footer once. ponytail: cleared whole past 256 files.
_FOOTERS: dict[Path, tuple[tuple[int, int], Any, list[tuple[int, int]]]] = {}


def _footer(path: Path) -> tuple[Any, list[tuple[int, int]]]:
    import pyarrow.parquet as pq

    stat = path.stat()
    key = (stat.st_mtime_ns, stat.st_size)
    hit = _FOOTERS.get(path)
    if hit is not None and hit[0] == key:
        return hit[1], hit[2]
    meta = pq.read_metadata(path)
    column = meta.schema.names.index("id") if "id" in meta.schema.names else None
    spans = []
    for index in range(meta.num_row_groups):
        stats = None if column is None else meta.row_group(index).column(column).statistics
        has = stats is not None and stats.has_min_max
        spans.append((int(stats.min), int(stats.max)) if has else (0, (1 << 64) - 1))
    if len(_FOOTERS) >= 256:
        _FOOTERS.clear()
    _FOOTERS[path] = (key, meta, spans)
    return meta, spans


def file_groups(path: str | Path) -> list[tuple[int, int, int, int | None]]:
    """The shards of a file, ``(base_id, slot_count, record_count, scope)`` each, from its
    header alone (footer cached). A file written before groups is one shard."""
    meta, _spans = _footer(Path(path))
    header = json.loads((meta.metadata or {})[METADATA_KEY].decode("utf-8"))
    groups = header.get("groups")
    if groups is None:
        return [(int(header["base_id"]), int(header["slot_count"]), int(header["record_count"]), None)]
    return [(int(g[0]), int(g[1]), int(g[2]), None if g[3] is None else int(g[3])) for g in groups]


class Shard:
    """A read-only shard, decoded into memory on open."""

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
    def open(
        cls,
        path: str | Path,
        *,
        ids: tuple[int, int] | None = None,
        record_ids: Any = None,
        dataset_id: uuid.UUID | str | bytes | None = None,
    ) -> "Shard":
        """The whole file, or with ``ids=(lo, hi)`` the logical shard of ids ``lo <= id < hi``
        inside it: only the row groups that can hold them are read. ``record_ids`` narrows
        further to those ids (what survives the exclusions, a Top-N); unknown ones are ignored."""
        import pyarrow as pa
        import pyarrow.compute as pc
        import pyarrow.parquet as pq

        path = Path(path)
        try:
            meta, spans = _footer(path)
            with pq.ParquetFile(path, metadata=meta, page_checksum_verification=True) as parquet:
                wanted = None if record_ids is None else sorted({int(i) for i in record_ids})
                if ids is None and wanted is None:
                    table = parquet.read(columns=["id", "record"])
                else:
                    lo, hi = (0, 1 << 64) if ids is None else (int(ids[0]), int(ids[1]))
                    if wanted is not None:  # prune row groups by the wanted ids' span too
                        lo, hi = (max(lo, wanted[0]), min(hi, wanted[-1] + 1)) if wanted else (0, 0)
                    groups = [i for i, (first, last) in enumerate(spans) if first < hi and last >= lo]
                    table = parquet.read_row_groups(groups, columns=["id", "record"])
                    table = table.filter(pc.and_(pc.greater_equal(table["id"], lo), pc.less(table["id"], hi)))
                    if wanted is not None:
                        table = table.filter(pc.is_in(table["id"], pa.array(wanted, type=table["id"].type)))
                    if ids is not None:
                        lo, hi = int(ids[0]), int(ids[1])
                # The writer adds the header on close, so it lives in the file's key-value
                # metadata, which shards written in one go (before streaming) also have.
                raw = (meta.metadata or {}).get(METADATA_KEY)
        except (OSError, pa.ArrowException) as exc:  # a failed page CRC is an OSError
            with open(path, "rb") as fh:  # a missing file raises here, as itself
                if fh.read(len(_LEGACY_MAGIC)) == _LEGACY_MAGIC:
                    raise ValueError(
                        f"{path}: a .mshard container from an older version; re-import the library"
                    ) from None
            raise ValueError(f"{path}: unreadable shard ({exc})") from exc
        if raw is None:
            raise ValueError(f"{path}: Parquet file without a shard header, not a shard")
        header = json.loads(raw.decode("utf-8"))
        if header.get("format") != FORMAT:
            raise ValueError(f"{path}: unsupported shard format {header.get('format')!r}")
        if dataset_id is not None and _uuid(dataset_id) != uuid.UUID(str(header["dataset_id"])):
            raise ValueError(f"{path}: dataset_id mismatch")
        records = table.column("record").combine_chunks().cast(pa.binary())
        if ids is not None:  # the header describes the file; this is one shard of it
            header = {**header, "base_id": lo, "slot_count": hi - lo}
        if ids is not None or record_ids is not None:
            header = {**header, "record_count": table.num_rows}
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


def merge_shards(sources: list[tuple[Any, Any]], dest: str | Path, *, metadata: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Many files' shards into one file: ``sources`` are ``(path, ids)`` in id order (``ids=None``
    = every shard of the file, a range = its shards inside it), ``dest`` may be one of them. Each
    shard keeps its ids, range and scope, as its own row group and ``groups`` entry. Returns the
    shards written, ``base_id``/``slot_count``/``record_count``/``scope`` each.

    What a worker writes per task (a shard, a prepared twin) is adopted into a big file by the
    parent once the job is done, and the few shards left to run travel to another host as a
    mini file with the parent's ids. Atomic like any `ShardWriter`.
    ponytail: records only — a source with a ``source_index`` column is refused, not merged
    without it (nothing merges those today: the import already writes big files).
    """
    import pyarrow.parquet as pq

    written: list[dict[str, Any]] = []
    writer: ShardWriter | None = None
    try:
        for path, ids in sources:
            path = Path(path)
            if "source_index" in pq.read_schema(path).names:
                raise ValueError(f"{path}: merging a shard with a source_index column is not supported")
            lo, hi = (0, 1 << 64) if ids is None else (int(ids[0]), int(ids[1]))
            for base, slots, _count, scope in file_groups(path):
                if base + slots <= lo or base >= hi:
                    continue
                with Shard.open(path, ids=(base, base + slots)) as shard:
                    if writer is None:
                        writer = ShardWriter(dest, dataset_id=shard.dataset_id, shard_id=0, base_id=base,
                                             kind=shard.kind, serializer=shard.serializer, slot_count=MAX_RECORDS,
                                             metadata={**shard.metadata, **dict(metadata or {})})
                    for record in shard:
                        writer.add(record.id, record.view())
                    writer.end_group(scope=scope, base_id=base, slot_count=slots)
                    written.append({"base_id": base, "slot_count": slots, "record_count": len(shard),
                                    "scope": scope})
    except BaseException:
        if writer is not None:
            writer._discard()
        raise
    if writer is not None:
        writer.close()
    return written
