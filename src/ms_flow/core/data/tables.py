"""An app's plain tables (a sidecar, a run's points, a task's rows): written by ms_flow, whole or
not at all, so no app picks a storage format of its own. Parquet, zstd."""
from __future__ import annotations

import io
import os
import uuid
from pathlib import Path
from typing import Any

__all__ = ["from_bytes", "read", "to_bytes", "write"]


def _arrow(table: Any) -> Any:
    """A pyarrow table of ``table``: one already, a pandas frame (its ``attrs`` kept, no index) or
    a mapping of columns."""
    import pyarrow as pa

    if isinstance(table, pa.Table):
        return table
    if hasattr(table, "iloc"):
        return pa.Table.from_pandas(table, preserve_index=False)
    return pa.table(table)


def write(path: str | Path, table: Any) -> Path:
    """``table`` at ``path``: a tmp file beside it, fsynced, then moved onto it. A failure leaves
    what was there."""
    import pyarrow.parquet as pq

    from ms_flow.core.data.shard import _fsync_dir

    path = Path(path)
    table = _arrow(table)
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex[:8]}.tmp")
    try:
        with open(tmp, "wb") as file:
            pq.write_table(table, file, compression="zstd")
            file.flush()
            os.fsync(file.fileno())
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    _fsync_dir(path.parent)
    return path


def read(path: str | Path) -> Any:
    import pyarrow.parquet as pq

    return pq.read_table(Path(path))


def to_bytes(table: Any) -> bytes:
    """``table`` as bytes, for a payload store."""
    import pyarrow.parquet as pq

    buffer = io.BytesIO()
    pq.write_table(_arrow(table), buffer, compression="zstd")
    return buffer.getvalue()


def from_bytes(data: bytes) -> Any:
    import pyarrow as pa
    import pyarrow.parquet as pq

    return pq.read_table(pa.BufferReader(data))
