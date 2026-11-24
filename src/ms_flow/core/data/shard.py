"""Shards: a compact, reliable, indexed container for many small byte records.

The problem it solves is *file count*, not file format. A dataset or a stage of a
pipeline produces millions of tiny payloads; as one file each they exhaust inodes, copy at
metadata speed and cannot be moved anywhere as a unit. A shard packs a contiguous range of
them, and a file packs many shards: one Arrow IPC stream ``X.arrows`` with its index
``X.arrows.index`` beside it (`container`), which travel together (I14).

Each shard is its own run of record batches: an ``id`` column (uint64), a ``record`` column
(``string`` when the serializer is UTF-8, ``binary`` otherwise; null for a dropped id) and a
``reason`` column (null unless dropped), zstd-compressed, with an optional ``source_index``
column (uint64): where each record came from in its source file. The
header below sits in the index (``app``); the index lists the shards (`file_groups`).

``Shard.open(path, shard=n)`` reads only shard number ``n``, so a dataset of millions sits in a
few big files instead of one small file per shard. Shards are addressed by number, never by id
range: ids restart per import and repeat after a redo, and a retired shard keeps its number. A
catalog keeps one row per *file*, never one per shard; what is inside is read from here.

A *derived* shard (what a step made of a source shard, same ids) names the shard it came from in
``origin``, and keeps the ids it could not produce (`ShardWriter.drop`) as rows with a null
``record`` and a ``reason``: the index counts them (``n_dropped``), reads skip them, and
`read_dropped` lists them.

What the container guarantees:

* **Indexed access.** A record is addressed by its logical id (``base_id .. base_id +
  slot_count - 1``), not by its position. Ids may be missing — a filtered-out record, a
  failed transformation — and the shard records that instead of silently renumbering.
* **Reliability.** The writer builds ``<path>.tmp`` and moves it onto ``path`` only after
  everything is fsynced, so a crash leaves a ``.tmp`` and never a half-written shard. Every
  shard carries a CRC that a read checks, so a damaged shard fails to open instead of handing
  back wrong bytes.
* **Opacity.** The core has no idea what a record *is*. ``kind``, ``serializer`` and ``codec``
  are integers whose meaning belongs to the caller; ``add()``/``view()`` see ``bytes``.

pyarrow is imported on first write or open, so the modules that only need `_fsync_dir` (and a
worker that never touches a shard) do not pay for it.
"""

from __future__ import annotations

import os
import uuid
from bisect import bisect_left
from pathlib import Path
from typing import Any, Callable, Iterator, NamedTuple

__all__ = [
    "Codec",
    "MAX_RECORDS",
    "PayloadKind",
    "Record",
    "SUFFIX",
    "Serializer",
    "Shard",
    "ShardWriter",
    "get_serializer",
    "Group",
    "file_groups",
    "merge_shards",
    "read_dropped",
    "usable_ids",
    "register",
]

FORMAT = "ms_flow-shard-arrows-v1"
SUFFIX = ".arrows"
# ponytail: `Shard.open` decodes a whole shard into memory, so the id range stays capped; a
# reader that maps batches lifts this if shards ever need more.
MAX_RECORDS = 1 << 20
# The open shard turns its records into a batch once this many bytes wait as Python objects.
_BATCH_BYTES = 16 << 20
# Encoded shards wait for one commit (an fsync of data, index and folder) up to this many bytes.
_COMMIT_BYTES = 64 << 20
_OLD_MAGICS = (b"PAR1", b"MSHARD")
# A shard's index lists the ids that have a usable record as a bitmap up to this many slots.
PRESENT_SLOTS = 1 << 16


class PayloadKind:
    """What a record *represents*. Opaque to the core — meaning lives in the caller."""

    GENERIC_BYTES = 0


class Serializer:
    """How a record's bytes decode back into a Python object; see `register`."""

    RAW = 0
    UTF8 = 1


class Codec:
    """How a record's bytes are stored inside the column. The container compresses the whole
    column, so only NONE exists; the field stays so a caller can tell the layout apart if one is
    added."""

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

    This is the extension point: a caller that stores its own binary objects registers its own
    id here instead of this module growing a dependency on whatever produces them.
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


class Group(NamedTuple):
    """One shard of a file, as its index lists it. ``shard`` is its number; a ``retired`` one keeps
    it but is out of every read (callers skip it)."""

    base_id: int
    slot_count: int
    n_records: int
    scope: int | None
    origin: int | None = None  # the shard a derived one was made from
    n_dropped: int = 0
    shard: int = 0
    present: bytes | None = None
    part: str | None = None
    meta: str | None = None
    retired: bool = False


def _tmp(path: Path) -> Path:
    return path.with_name(path.name + ".tmp")


def read_dropped(path: str | Path, shard: int | None = None) -> list[tuple[int, str]]:
    """``(id, reason)`` of what the live shards of a file dropped, those of shard number ``shard``
    only. Reads two columns of the shards that have drops."""
    import pyarrow.compute as pc

    from ms_flow.core.data import container

    path = Path(path)
    info, rows, _header, groups = _index(path)
    picked = [_group(path, shard)] if shard is not None else [g for g in groups if not g.retired]
    chosen = [rows[g.shard] for g in picked if g.n_dropped]
    if not chosen:
        return []
    table = container.read_table(path, info, chosen, ["id", "reason"])
    table = table.filter(pc.is_valid(table["reason"]))
    return list(zip(table.column("id").to_pylist(), table.column("reason").to_pylist()))


def usable_ids(path: str | Path, group: Any) -> Any:
    """The ids of shard ``group`` (a `Group`, or a row with its ``base_id``, ``slot_count``,
    ``n_records``, ``present``, ``shard_index``) that have a usable record, sorted (uint64): not its
    holes, not its drops. From the index: its ``present`` bitmap, or its whole range when every
    slot has one; only a sparse shard over `PRESENT_SLOTS` slots reads its ``id`` and ``reason``."""
    import numpy as np

    get = group.get if isinstance(group, dict) else lambda name, default=None: getattr(group, name, default)
    lo, n, present = int(get("base_id")), int(get("slot_count")), get("present")
    if present is not None:
        bits = np.unpackbits(np.frombuffer(present, dtype=np.uint8), count=n, bitorder="little")
        return np.flatnonzero(bits).astype(np.uint64) + np.uint64(lo)
    if int(get("n_records")) == n:
        return np.arange(lo, lo + n, dtype=np.uint64)
    import pyarrow.compute as pc

    from ms_flow.core.data import container

    path = Path(path)
    number = get("shard", None) if not isinstance(group, dict) else get("shard_index")
    info, rows, _header, _groups = _index(path)
    table = container.read_table(path, info, [rows[_group(path, int(number)).shard]], ["id", "reason"])
    return table.filter(pc.is_null(table["reason"]))["id"].to_numpy()


def _present(batches: list, lo: int, n: int) -> bytes | None:
    """The bitmap of a shard's ids with a record (5.2): None when every slot has one, or past
    `PRESENT_SLOTS` (then readers take the ids from the data)."""
    if n > PRESENT_SLOTS:
        return None
    import numpy as np

    bits = np.zeros(n, dtype=np.uint8)
    for batch in batches:
        ids = batch.column(0).filter(batch.column(1).is_valid()).to_numpy()
        bits[ids - np.uint64(lo)] = 1
    return None if bits.all() else np.packbits(bits, bitorder="little").tobytes()


def _group(path: Path, shard: int) -> "Group":
    groups = file_groups(path)
    if not 0 <= int(shard) < len(groups):
        raise ValueError(f"{path}: no shard {shard} (it holds {len(groups)})")
    if groups[int(shard)].retired:
        raise ValueError(f"{path}: shard {shard} is retired")
    return groups[int(shard)]


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


def _unlink_container(path: Path) -> None:
    from ms_flow.core.data import container

    path.unlink(missing_ok=True)
    container.index_path(path).unlink(missing_ok=True)
    _tmp(container.index_path(path)).unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------


class ShardWriter:
    """Builds one shard file (``.arrows``). Ids must be added in strictly increasing order.

    The open shard waits in memory as record batches; `end_group` encodes it, and encoded shards
    go to ``<path>.tmp`` a few at a time. On close the header goes into the index and the
    ``.tmp`` is moved onto ``path``: nothing is visible before, and an exception discards it all.
    ``metadata`` may still be edited until the writer closes.

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
            raise ValueError("shards only write codec=Codec.NONE; the container compresses the column")
        slot_count = int(slot_count)
        if not 0 < slot_count <= MAX_RECORDS:
            raise ValueError(f"slot_count must be in 1..{MAX_RECORDS}, got {slot_count}")
        self.path = Path(path)
        if not self.path.name.endswith(SUFFIX):
            raise ValueError(f"{self.path}: a shard file is named *{SUFFIX}")
        self._tmp_path = _tmp(self.path)
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
        self._n_groups = 0
        self._group_first: int | None = None  # the first id (added or dropped) of the shard being filled
        self._group_count = 0
        self._group_dropped = 0
        self._ids: list[int] = []  # the batch being filled
        self._records: list[bytes | None] = []  # None = dropped
        self._reasons: list[str | None] = []
        self._sources: list[int] = []
        self._buffered = 0
        self._batches: list[Any] = []  # the open shard's batches
        self._pending: list[tuple[dict, bytes]] = []  # encoded shards waiting for a commit
        self._pending_bytes = 0
        self._schema_cache: Any = None
        self._appender: Any = None
        self._count = 0
        self._last: int | None = None  # the last id of the shard being filled
        self._top: int | None = None  # the highest id of the file
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
            raise ValueError(f"ids must be strictly increasing inside a shard: {id_} after {self._last}")
        if self._with_source:
            if source_index is None:
                raise ValueError("this writer stores a source_index per record; pass one")
            self._sources.append(int(source_index))
        self._ids.append(id_)
        self._records.append(bytes(data))
        self._reasons.append(None)
        if self._group_first is None:
            self._group_first = id_
        self._group_count += 1
        self._last, self._count, self._buffered = id_, self._count + 1, self._buffered + len(data)
        self._top = id_ if self._top is None else max(self._top, id_)
        if self._buffered >= _BATCH_BYTES:
            self._batch()

    def drop(self, id_: int, reason: str = "") -> None:
        """An id of the shard being filled that has no record (a transformation that failed for
        it): a row with no record and ``reason``, counted apart. Same order as `add`."""
        if self._closed:
            raise ValueError(f"{self.path}: shard already closed")
        id_ = int(id_)
        if id_ < self.base_id or id_ >= self.base_id + self.slot_count:
            raise ValueError(f"id {id_} out of range [{self.base_id}, {self.base_id + self.slot_count})")
        if self._last is not None and id_ <= self._last:
            raise ValueError(f"ids must be strictly increasing inside a shard: {id_} after {self._last}")
        if self._group_first is None:
            self._group_first = id_
        self._last = id_
        self._top = id_ if self._top is None else max(self._top, id_)
        if self._with_source:
            self._sources.append(None)
        self._ids.append(id_)
        self._records.append(None)
        self._reasons.append(str(reason or ""))
        self._group_dropped += 1

    def end_group(self, *, scope: int | None = None, base_id: int | None = None,
                  slot_count: int | None = None, origin: int | None = None) -> None:
        """Close the shard being filled: encoded, and listed in the index. Its ids are
        ``[base_id, base_id + slot_count)``, by default from its first id to its last; pass them to
        keep the range of the shard it came from, holes and all (a derived shard whose last record
        was dropped, or that was dropped whole, still covers its origin's range). ``scope``
        defaults to the writer's; ``origin``: the shard it was made from, for a derived one."""
        from ms_flow.core.data import container

        self._batch()
        if base_id is None and self._group_first is None:
            return  # nothing added and no range to claim: no shard
        # By default a shard starts at its first id; the first one at the writer's base_id, so a
        # file written as one shard keeps the holes before its first record, as its header does.
        lo = int(base_id) if base_id is not None else self.base_id if not self._n_groups else int(self._group_first)
        hi = lo + int(slot_count) if slot_count is not None else int(self._last) + 1
        attrs = {"base_id": lo, "slot_count": hi - lo, "n_records": self._group_count,
                 "n_dropped": self._group_dropped, "scope": self.scope if scope is None else scope,
                 "origin": None if origin is None else int(origin), "present": _present(self._batches, lo, hi - lo)}
        try:
            encoded = container.encode(self._schema(), self._batches, attrs)
        except BaseException:
            self._discard()
            raise
        self._batches = []
        self._pending.append(encoded)
        self._pending_bytes += len(encoded[1])
        self._n_groups += 1
        self._group_first, self._group_count, self._group_dropped, self._last = None, 0, 0, None
        if self._pending_bytes >= _COMMIT_BYTES:
            self._commit()

    def _schema(self) -> Any:
        if self._schema_cache is None:
            import pyarrow as pa

            fields = [("id", pa.uint64()),
                      ("record", pa.string() if self.serializer == Serializer.UTF8 else pa.binary()),
                      ("reason", pa.string())]
            if self._with_source:
                fields.append(("source_index", pa.uint64()))
            self._schema_cache = pa.schema(fields)
        return self._schema_cache

    def _batch(self) -> None:
        """The waiting records as one batch of the open shard."""
        if not self._ids:
            return
        import pyarrow as pa

        try:
            records = pa.array(self._records, type=pa.binary())
            if self.serializer == Serializer.UTF8:
                records = records.cast(pa.string())  # also validates the UTF-8
            columns = [pa.array(self._ids, type=pa.uint64()), records, pa.array(self._reasons, type=pa.string())]
            if self._with_source:
                columns.append(pa.array(self._sources, type=pa.uint64()))
            self._batches.append(pa.RecordBatch.from_arrays(columns, schema=self._schema()))
        except BaseException:
            self._discard()
            raise
        self._ids, self._records, self._reasons, self._sources, self._buffered = [], [], [], [], 0

    def _commit(self) -> None:
        """The waiting shards into the ``.tmp``; the first call creates it."""
        from ms_flow.core.data import container

        try:
            if self._appender is None:
                _unlink_container(self._tmp_path)  # what a crashed writer left
                self._appender = container.Appender.create(self._tmp_path, self._schema())
            if self._pending:
                self._appender.commit(self._pending)
        except BaseException:
            self._discard()
            raise
        self._pending, self._pending_bytes = [], 0

    def _discard(self) -> None:
        """Drop what was written: the ``.tmp`` goes, ``path`` is never touched."""
        self._closed = True
        self._ids, self._records, self._reasons, self._sources = [], [], [], []
        self._batches, self._pending, self._appender = [], [], None
        _unlink_container(self._tmp_path)

    def close(self) -> None:
        from ms_flow.core.data import container

        if self._closed:
            return
        if self._group_first is not None:
            self.end_group()  # what was added since the last end_group is a shard too
        # Trailing empty slots are not information: nothing distinguishes "id never came"
        # from "id past the end".
        self.slot_count = self._top - self.base_id + 1 if self._top is not None else 0
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
        }
        self._commit()  # the last shards; creates the .tmp when nothing went yet
        try:
            # ponytail: one more index write for the header; fold it into the last commit if it shows
            self._appender.info["app"] = header
            container._save_index(self._tmp_path, self._appender.info, self._appender.rows)
            container.move(self._tmp_path, self.path)
        except BaseException:
            self._discard()
            raise
        self._closed, self._appender = True, None

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


# path -> (the index file's (inode, mtime_ns, size), (info, rows, header, groups)). A worker
# reading many shards of one big file reads its index once. ponytail: cleared whole past 256 files.
_INDEXES: dict[Path, tuple[tuple[int, int, int], tuple[dict, list[dict], dict, list[Group]]]] = {}


def _refuse(path: Path) -> None:
    """A file without an index: an older shard, or no shard at all."""
    with open(path, "rb") as handle:  # a missing file raises here, as itself
        start = handle.read(8)
    if start.startswith(_OLD_MAGICS):
        raise ValueError(f"{path}: a shard file from an older version; re-import the project")
    raise ValueError(f"{path}: no index beside it, not a shard file")


def _index(path: Path) -> tuple[dict, list[dict], dict, list[Group]]:
    from ms_flow.core.data import container

    try:
        stat = container.index_path(path).stat()
    except FileNotFoundError:
        _refuse(path)
    key = (stat.st_ino, stat.st_mtime_ns, stat.st_size)
    hit = _INDEXES.get(path)
    if hit is not None and hit[0] == key:
        return hit[1]
    info, rows = container.read_index(path)
    header = info.get("app") or {}
    if header.get("format") != FORMAT:
        raise ValueError(f"{path}: unsupported shard format {header.get('format')!r}")
    groups = [Group(int(r["base_id"]), int(r["slot_count"]), int(r["n_records"]), r["scope"], r["origin"],
                    int(r["n_dropped"]),
                    int(r["shard"]), r["present"], r["part"], r["meta"], bool(r["retired"]))
              for r in rows]
    if len(_INDEXES) >= 256:
        _INDEXES.clear()
    _INDEXES[path] = (key, (info, rows, header, groups))
    return info, rows, header, groups


def file_groups(path: str | Path) -> list[Group]:
    """The shards of a file, a `Group` each and retired ones too, from its index alone (cached:
    callers must not mutate the list). A shard's number is its ``shard``, never its place in a
    filtered list."""
    return _index(Path(path))[3]


def _ambiguous(groups: list[Group]) -> bool:
    """Shards whose ids are not in one ascending order across the file (I7)."""
    return any(a.base_id + a.slot_count > b.base_id for a, b in zip(groups, groups[1:]))


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
        shard: int | None = None,
        record_ids: Any = None,
        dataset_id: uuid.UUID | str | bytes | None = None,
    ) -> "Shard":
        """Every live shard of the file, or with ``shard`` the shard of that number inside it:
        only its bytes are read. Ids restart per import and repeat after a redo, so a file whose
        shards' ids overlap only opens by number (I7). ``record_ids`` narrows further to those ids
        (what survives the exclusions, a Top-N); unknown ones are ignored."""
        import pyarrow as pa
        import pyarrow.compute as pc

        from ms_flow.core.data import container

        path = Path(path)
        try:
            info, rows, header, groups = _index(path)
        except container.Damaged as exc:
            raise ValueError(f"{path}: unreadable shard ({exc})") from exc
        if dataset_id is not None and _uuid(dataset_id) != uuid.UUID(str(header["dataset_id"])):
            raise ValueError(f"{path}: dataset_id mismatch")
        if shard is not None:
            group = _group(path, shard)
            chosen = [rows[group.shard]]
            header = {**header, "base_id": group.base_id, "slot_count": group.slot_count}
        else:
            live = [g for g in groups if not g.retired]
            if _ambiguous(live):
                raise ValueError(f"{path}: its shards' ids overlap; open one by number")
            chosen = [rows[g.shard] for g in live]
        wanted = None if record_ids is None else sorted({int(i) for i in record_ids})
        if wanted is not None:  # only the shards whose range holds a wanted id
            lo, hi = (wanted[0], wanted[-1] + 1) if wanted else (0, 0)
            chosen = [r for r in chosen if r["base_id"] < hi and r["base_id"] + r["slot_count"] > lo]
        try:
            table = container.read_table(path, info, chosen, ["id", "record"])
        except (OSError, pa.ArrowException, container.Damaged) as exc:
            raise ValueError(f"{path}: unreadable shard ({exc})") from exc
        if any(r["n_dropped"] for r in chosen):
            table = table.filter(pc.is_valid(table["record"]))
        if wanted is not None:
            table = table.filter(pc.is_in(table["id"], pa.array(wanted, type=table["id"].type)))
        records = table.column("record").combine_chunks().cast(pa.binary())
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
        """Re-read the file checking every shard's CRC: the file on disk is still the one written."""
        from ms_flow.core.data import container

        try:
            info, rows, _header, _groups = _index(self.path)
            container.read_head(self.path, info)
            for row in rows:
                if not row["retired"]:
                    container.read_raw(self.path, row)
        except (OSError, ValueError):  # `container.Damaged` is a ValueError
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


def merge_shards(sources: list[tuple[Any, Any]], dest: str | Path, *, metadata: dict[str, Any] | None = None,
                 skip: frozenset[tuple[str, int]] | set[tuple[str, int]] = frozenset()) -> list[dict[str, Any]]:
    """Many files' shards into one file, copied as bytes (nothing is decoded): ``sources`` are
    ``(path, shard)`` in the order to write (``shard=None`` = every live shard of the file, a
    number = that one), ``dest`` may be one of them; ``skip``: ``(str(path), shard number)``
    pairs left out. Each shard keeps its ids, range, scope, origin and dropped rows. Returns the
    shards written, ``base_id``/``slot_count``/``record_count``/``scope``/``origin``/``n_dropped``
    each. Every source must have the same columns.

    What a worker writes per task (a shard, a derived shard) is adopted into a big file by the
    parent once the job is done, and the few shards left to run travel to another host as a
    mini file with the parent's ids. Atomic like any `ShardWriter`.
    """
    import pyarrow.ipc as ipc

    from ms_flow.core.data import container

    dest = Path(dest)
    picked = []  # (path, row)
    first = None  # (path, info, header, head)
    for path, number in sources:
        path = Path(path)
        info, rows, header, groups = _index(path)
        head = container.read_head(path, info)
        if first is None:
            first = (path, info, header, head)
        elif head != first[3] and not ipc.read_schema(ipc.read_message(head)).equals(
                ipc.read_schema(ipc.read_message(first[3]))):
            raise ValueError(f"{path}: other columns than {first[0]}")
        for group in groups:
            if group.retired or (number is not None and group.shard != int(number)) \
                    or (str(path), group.shard) in skip:
                continue
            picked.append((path, rows[group.shard]))
    if not picked:
        return []
    _path, _info, header, head = first
    tmp = _tmp(dest)
    _unlink_container(tmp)
    try:
        appender = container.Appender.create(tmp, ipc.read_schema(ipc.read_message(head)))
        lo = min(row["base_id"] for _p, row in picked)
        appender.info["app"] = {
            **header, "base_id": lo,
            "slot_count": max(row["base_id"] + row["slot_count"] for _p, row in picked) - lo,
            "record_count": sum(row["n_records"] for _p, row in picked),
            "metadata": {**(header.get("metadata") or {}), **dict(metadata or {})}}
        appender.commit((row, container._blocks(path, row)) for path, row in picked)
        container.move(tmp, dest)
    except BaseException:
        _unlink_container(tmp)
        raise
    return [{"base_id": row["base_id"], "slot_count": row["slot_count"], "record_count": row["n_records"],
             "scope": row["scope"], "origin": row["origin"], "n_dropped": row["n_dropped"]}
            for _p, row in picked]
