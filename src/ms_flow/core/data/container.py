"""Append-only containers of shards: an Arrow IPC stream plus a small index beside it."""
from __future__ import annotations

import json
import os
import zlib
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc

FORMAT = "ms_flow-container-arrows-v1"
KEY = b"ms_flow.container"
SHARD = b"ms_flow.shard"
SHARD_LIMIT = 512 << 20  # bytes of one encoded shard
BLOCK = 8 << 20  # most that a raw append holds in memory at once
META_LIMIT = 16 << 10  # of a shard's `meta`, repeated in the data beside its first batch
_INDEX = pa.schema([
    ("shard", pa.uint32()), ("offset", pa.uint64()), ("length", pa.uint64()), ("crc", pa.uint32()),
    ("n_batches", pa.uint32()), ("base_id", pa.uint64()), ("slot_count", pa.uint64()),
    ("n_records", pa.uint32()), ("n_dropped", pa.uint32()), ("scope", pa.int64()), ("origin", pa.int64()),
    ("part", pa.string()), ("meta", pa.string()), ("present", pa.binary()), ("retired", pa.bool_()),
])
_REQUIRED = ("n_batches", "n_records", "base_id", "slot_count")
_ATTRS = ("base_id", "slot_count", "n_records", "n_dropped", "scope", "origin", "part", "meta", "present")


class Damaged(ValueError):
    """What is on disk is not what was committed. Nothing was cut."""


def _read_options(fields=None):
    return ipc.IpcReadOptions(use_threads=False, included_fields=fields)


def _write_options():
    return ipc.IpcWriteOptions(compression=pa.Codec("zstd", compression_level=3), use_threads=False)


def index_path(path) -> Path:
    return Path(f"{path}.index")


def _sync(file) -> None:
    file.flush()
    try:
        import fcntl

        if hasattr(fcntl, "F_FULLFSYNC"):  # macOS: fsync does not reach the platter there
            fcntl.fcntl(file.fileno(), fcntl.F_FULLFSYNC)
            return
    except (ImportError, OSError):
        pass
    getattr(os, "fdatasync", os.fsync)(file.fileno())


def _sync_dir(folder) -> None:
    if os.name == "nt":
        return
    fd = os.open(folder, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write(file, data) -> None:
    view = memoryview(data)
    while len(view):  # a raw write may write part of it
        done = file.write(view)
        view = view[done:]


def _fill(file, path, offset: int, view) -> None:
    file.seek(offset)
    got = 0
    while got < len(view):
        n = file.readinto(view[got:])
        if not n:
            raise Damaged(f"{path}: committed data is missing at {offset + got}")
        got += n


def _pread(path, offset: int, size: int) -> bytearray:
    out = bytearray(size)
    with open(path, "rb", buffering=0) as file:
        _fill(file, path, offset, memoryview(out))
    return out


def _blocks(path, row):
    """A shard's bytes in blocks of at most BLOCK. Fails at the end if they are not what was committed."""
    crc, left = 0, row["length"]
    buffer = bytearray(min(BLOCK, left))
    with open(path, "rb", buffering=0) as file:
        while left:
            view = memoryview(buffer)[: min(BLOCK, left)]
            _fill(file, path, row["offset"] + row["length"] - left, view)
            crc = zlib.crc32(view, crc)
            left -= len(view)
            yield view
    if crc != row["crc"]:
        raise Damaged(f"{path}: shard {row['shard']} fails its checksum")


def check_schema(schema) -> None:
    def walk(kind):
        if pa.types.is_dictionary(kind):
            raise ValueError("dictionary columns cannot be appended")
        if isinstance(kind, pa.ExtensionType):
            walk(kind.storage_type)
            return
        if pa.types.is_map(kind):
            walk(kind.key_type)
            walk(kind.item_type)
        elif pa.types.is_list(kind) or pa.types.is_large_list(kind) or pa.types.is_fixed_size_list(kind):
            walk(kind.value_type)
        elif pa.types.is_struct(kind) or pa.types.is_union(kind):
            for at in range(kind.num_fields):
                walk(kind.field(at).type)

    for field in schema:
        walk(field.type)


def encode(schema, batches, attrs: dict) -> tuple[dict, bytes]:
    """One shard as the messages a container holds (no schema, no end of stream), with its
    attributes completed (`n_batches`). Both go to `Appender.commit`."""
    if not batches:
        batches = [pa.RecordBatch.from_arrays([pa.array([], field.type) for field in schema], schema=schema)]
    attrs = {name: attrs.get(name) for name in _ATTRS}
    if len((attrs["meta"] or "").encode()) > META_LIMIT:
        raise ValueError("a shard's meta is for a few facts, not for data")
    attrs["n_dropped"] = attrs["n_dropped"] or 0
    attrs["n_batches"] = len(batches)
    stamp = {**attrs, "present": attrs["present"].hex() if attrs["present"] is not None else None}
    out = pa.BufferOutputStream()
    with ipc.new_stream(out, schema, options=_write_options()) as writer:
        for at, batch in enumerate(batches):
            # on the first batch: what rebuilds the index if it is lost
            writer.write_batch(batch, custom_metadata={SHARD: json.dumps(stamp)} if at == 0 else None)
    data = out.getvalue().to_pybytes()[schema.serialize().size:-8]  # 8: the end of stream
    if len(data) > SHARD_LIMIT:
        raise ValueError(f"a shard is at most {SHARD_LIMIT >> 20} MiB, this one is {len(data) >> 20} MiB")
    return attrs, data


def _save_index(path: Path, info: dict, rows) -> None:
    """``rows``: the index as an Arrow table, or a list of row dicts."""
    if not isinstance(rows, pa.Table):
        rows = pa.Table.from_pylist(rows, schema=_INDEX)
    table = rows.replace_schema_metadata({KEY: json.dumps(info).encode()})
    out = pa.BufferOutputStream()
    with ipc.new_stream(out, table.schema, options=_write_options()) as writer:
        writer.write_table(table)
    data = out.getvalue().to_pybytes()
    target = index_path(path)
    tmp = target.with_name(target.name + ".tmp")
    with open(tmp, "wb") as file:
        _write(file, data + zlib.crc32(data).to_bytes(4, "little"))
        _sync(file)
    os.replace(tmp, target)  # the commit
    _sync_dir(target.parent)


def read_index(path) -> tuple[dict, list[dict]]:
    info, table = read_index_table(path)
    return info, table.to_pylist()


def read_index_table(path) -> tuple[dict, pa.Table]:
    """The index as it is stored: an Arrow table, never Python rows."""
    return parse_index(index_path(path).read_bytes(), path)


def parse_index(data: bytes, path="an index") -> tuple[dict, pa.Table]:
    """An index from its bytes (one read on another host): its crc checked."""
    if len(data) <= 4 or zlib.crc32(data[:-4]) != int.from_bytes(data[-4:], "little"):
        raise Damaged(f"{path}: damaged index")
    table = ipc.open_stream(data[:-4], options=_read_options()).read_all()
    info = json.loads(table.schema.metadata[KEY])
    if info.get("format") != FORMAT:
        raise ValueError(f"{path}: unsupported container format {info.get('format')!r}")
    return info, table.replace_schema_metadata(None).combine_chunks()


def index_end(info: dict, table: pa.Table) -> int:
    """Where the next shard goes: past the head and every committed shard."""
    if not table.num_rows:
        return info["head_length"]
    return max(info["head_length"], pc.max(pc.add(table["offset"], table["length"])).as_py())


def read_head(path, info) -> bytearray:
    head = _pread(path, 0, info["head_length"])
    if zlib.crc32(head) != info["head_crc"]:
        raise Damaged(f"{path}: damaged schema")
    return head


def read_raw(path, row) -> bytearray:
    data = _pread(path, row["offset"], row["length"])
    if zlib.crc32(data) != row["crc"]:
        raise Damaged(f"{path}: shard {row['shard']} fails its checksum")
    return data


def _fields(head, path, columns):
    if columns is None:
        return None
    schema = ipc.read_schema(ipc.read_message(head))
    fields = sorted(schema.get_field_index(name) for name in columns)
    if fields and fields[0] < 0:
        raise KeyError(f"{path}: no such column among {columns}")
    return fields


def iter_tables(path, info, rows, columns=None, max_bytes=64 << 20):
    """These shards, in the order given, as tables of contiguous segments of at most `max_bytes`
    of stored bytes (a shard over it goes alone). Memory stays near one segment."""
    head = read_head(path, info)
    fields = _fields(head, path, columns)
    rows, at = list(rows), 0
    while at < len(rows):
        end, size = at + 1, rows[at]["length"]
        while end < len(rows) and size + rows[end]["length"] <= max_bytes:
            size += rows[end]["length"]
            end += 1
        body, put = bytearray(len(head) + size), len(head)
        body[:put] = head
        view = memoryview(body)
        with open(path, "rb", buffering=0) as file:
            for row in rows[at:end]:
                _fill(file, path, row["offset"], view[put:put + row["length"]])
                if zlib.crc32(view[put:put + row["length"]]) != row["crc"]:
                    raise Damaged(f"{path}: shard {row['shard']} fails its checksum")
                put += row["length"]
        table = ipc.open_stream(view, options=_read_options(fields)).read_all()
        if table.num_rows != sum(row["n_records"] + row["n_dropped"] for row in rows[at:end]):
            raise Damaged(f"{path}: not the rows the index says")
        yield table if columns is None else table.select(columns)
        at = end


def read_table(path, info, rows, columns=None) -> pa.Table:
    """These shards as one table, in the order given."""
    tables = list(iter_tables(path, info, rows, columns))
    if tables:
        return pa.concat_tables(tables)
    head = read_head(path, info)
    table = ipc.open_stream(head, options=_read_options(_fields(head, path, columns))).read_all()
    return table if columns is None else table.select(columns)


def move(tmp, path) -> None:
    """Publish a container written under another name: the data, then its index."""
    os.replace(tmp, path)
    os.replace(index_path(tmp), index_path(path))
    _sync_dir(Path(path).parent)


class Appender:
    """The one writer of a container. Whoever creates one holds the container's lock."""

    def __init__(self, path):
        self.path, self._broken = Path(path), None
        self.info, self.index = read_index_table(self.path)
        self._rows = None
        self.head = read_head(self.path, self.info)
        self.end = index_end(self.info, self.index)
        size = os.path.getsize(self.path)
        if size < self.end:
            raise Damaged(f"{self.path}: shorter than what was committed")
        if size > self.end:  # only what no index ever named
            with open(self.path, "r+b") as file:
                file.truncate(self.end)
                _sync(file)

    @classmethod
    def create(cls, path, schema, app: dict | None = None) -> "Appender":
        check_schema(schema)
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        head = schema.serialize().to_pybytes()
        with open(path, "xb") as file:
            _write(file, head)
            _sync(file)
        _save_index(path, {"format": FORMAT, "head_length": len(head), "head_crc": zlib.crc32(head),
                           "app": app or {}}, [])
        return cls(path)

    @property
    def rows(self) -> list[dict]:
        """The index as Python rows, built on demand: the commit path never needs them."""
        if self._rows is None:
            self._rows = self.index.to_pylist()
        return self._rows

    def commit(self, shards) -> list[int]:
        """`(attrs, data)` of any number of shards, as one commit; their indices here. `attrs` come
        from `encode`; `data` is its bytes, or an iterable of blocks."""
        if self._broken is not None:
            raise RuntimeError(f"{self.path}: reopen it, the last commit failed") from self._broken
        added, at, reading = [], self.end, False
        try:
            with open(self.path, "r+b") as file:
                file.seek(at)
                source = iter(shards)
                while True:
                    reading = True
                    item = next(source, None)
                    reading = False
                    if item is None:
                        break
                    attrs, data = item
                    missing = [name for name in _REQUIRED if attrs.get(name) is None]
                    if missing:
                        reading = True  # the caller's mistake: the container stays as it was
                        raise ValueError(f"shard attributes lack {missing}: use `encode`")
                    row = {name: attrs.get(name) for name in _INDEX.names}
                    length = crc = 0
                    blocks = iter([data] if isinstance(data, (bytes, bytearray, memoryview)) else data)
                    while True:
                        reading = True  # a block of a part that cannot be read is the caller's, not ours
                        block = next(blocks, None)
                        reading = False
                        if block is None:
                            break
                        _write(file, block)
                        crc, length = zlib.crc32(block, crc), length + len(block)
                    row.update(shard=self.index.num_rows + len(added), offset=at, length=length, crc=crc,
                               retired=False, n_dropped=attrs.get("n_dropped") or 0)
                    added.append(row)
                    at += length
                if not added:
                    return []
                _sync(file)
            table = pa.concat_tables([self.index, pa.Table.from_pylist(added, schema=_INDEX)]).combine_chunks()
            _save_index(self.path, self.info, table)
        except BaseException as error:
            if reading:  # the caller's shard was unreadable: this container is as it was
                try:
                    os.truncate(self.path, self.end)
                except OSError as failed:
                    self._broken = failed
            else:
                self._broken = error
            raise
        self.index, self._rows = table, None
        self.end = at
        return [row["shard"] for row in added]

    def append(self, parts) -> list[int]:
        """Every live shard of these containers, copied as bytes: nothing is decoded."""
        found = []
        mine = ipc.read_schema(ipc.read_message(self.head))
        for part in parts:
            info, rows = read_index(part)
            theirs = read_head(part, info)
            if theirs != self.head and not ipc.read_schema(ipc.read_message(theirs)).equals(mine, check_metadata=True):
                raise ValueError(f"{part}: another schema")
            found.extend((part, row) for row in rows if not row["retired"])
        return self.commit((row, _blocks(part, row)) for part, row in found)

    def retire(self, shards) -> None:
        """Out of every read, at once. Their bytes stay until the container is merged."""
        if self._broken is not None:
            raise RuntimeError(f"{self.path}: reopen it, the last commit failed") from self._broken
        gone = {int(shard) for shard in shards}
        missing = gone - set(range(self.index.num_rows))  # a shard's number is its row
        if missing:
            raise ValueError(f"{self.path}: no such shards {sorted(missing)}")
        hit = pc.is_in(self.index["shard"], pa.array(sorted(gone), pa.uint32()))
        table = self.index.set_column(self.index.schema.get_field_index("retired"), "retired",
                                      pc.or_(self.index["retired"], hit))
        try:
            _save_index(self.path, self.info, table)
        except BaseException as error:
            self._broken = error
            raise
        self.index, self._rows = table, None


def rebuild_index(path, app: dict | None = None, *, cut_tail: bool = False) -> int:
    """After the index was lost, and only when someone asks: every whole shard of the file.
    It cannot see damage Arrow decodes without complaint, nor what had been retired. Bytes it
    cannot read stop it, unless `cut_tail`: the next writer would cut them off."""
    path, rows, pending = Path(path), [], None
    with pa.OSFile(str(path)) as file:
        size = file.size()
        ipc.read_message(file)
        head_length = file.tell()
        head = _pread(path, 0, head_length)
        while file.tell() < size:
            at = file.tell()
            try:
                ipc.read_message(file)
                data = _pread(path, at, file.tell() - at)
                batch, meta = ipc.open_stream(head + data, options=_read_options()).read_next_batch_with_custom_metadata()
                batch.validate()
                if meta is not None and meta.get(SHARD) is not None:
                    attrs = json.loads(meta[SHARD])
                    left = attrs.pop("n_batches")
                    if attrs["present"] is not None:
                        attrs["present"] = bytes.fromhex(attrs["present"])
                    pending = [attrs, at, left, left]
            except (pa.ArrowException, OSError, EOFError, StopIteration, ValueError, KeyError):
                break
            if pending is None:
                break
            pending[2] -= 1
            if pending[2] == 0:
                attrs, start, _left, count = pending
                rows.append({**attrs, "shard": len(rows), "offset": start, "length": file.tell() - start,
                             "crc": zlib.crc32(_pread(path, start, file.tell() - start)),
                             "n_batches": count, "retired": False})
                pending = None
    end = rows[-1]["offset"] + rows[-1]["length"] if rows else head_length
    if end != size and not cut_tail:
        raise Damaged(f"{path}: {size - end} bytes after the last whole shard; nothing was written")
    _save_index(path, {"format": FORMAT, "head_length": head_length, "head_crc": zlib.crc32(head),
                       "app": app or {}}, rows)
    return len(rows)
