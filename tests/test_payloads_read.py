"""Reading payloads: jobs get bytes in their elements, programs get files, exports get a stream (V3)."""
import os
import shutil
import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path

import pytest

from ms_flow.core.database.project import ProjectStore
from ms_flow.engine import File
from ms_flow.payloads import Payload, scratch_file, with_payloads
from ms_flow.query import QuerySpec, Rows


def structure(n: int) -> bytes:
    return f"molecule {n}\n".encode() * (n % 7 + 1)


@pytest.fixture
def project(tmp_path):
    store = ProjectStore.open_at(tmp_path / "project.db")
    with closing(sqlite3.connect(store.db_path)) as conn, conn:
        conn.execute("CREATE TABLE ligands (id INTEGER PRIMARY KEY, n INT NOT NULL,"
                     " structure_payload_id INTEGER REFERENCES ms_payloads(id))")
    yield store
    store.dispose()


def add_ligands(project, count, size=None):
    """`count` ligands with their structure, and one more without: ids 1..count+1."""
    with project.get_session() as session:
        data = [structure(n) if size is None else os.urandom(size) for n in range(count)]
        ids = project.payloads.put(session, [Payload(d, kind="sdf", series="ligands") for d in data]) + [None]
        session.connection().exec_driver_sql("INSERT INTO ligands (n, structure_payload_id) VALUES (?, ?)",
                                             list(enumerate(ids)))
        session.commit()
    return dict(enumerate(data))


def test_a_source_with_payloads_gives_each_row_its_bytes_and_keeps_what_its_rows_know(project):
    add_ligands(project, 30)
    rows = Rows(project, QuerySpec(table="ligands"), page_size=4)
    source = with_payloads(project, rows, {"structure_payload_id": "structure"}, window=7)
    assert source.reads == ("ligands",) and source.open(None) == {"cap": 31} and source.remaining() == 31
    got = list(source)
    assert [row["n"] for row in got] == list(range(31))
    assert all(row["structure"] == structure(row["n"]) for row in got[:30])
    assert got[30]["structure"] is None and got[30]["structure_payload_id"] is None
    plain = with_payloads(project, iter(got[:3]), {"structure_payload_id": "structure"})
    assert not hasattr(plain, "open") and not hasattr(plain, "remaining")  # the engine asks with hasattr
    assert [row["structure"] for row in plain] == [structure(n) for n in range(3)]


def test_iter_bytes_streams_every_id_once_in_bounded_windows(project):
    data = add_ligands(project, 50)
    got = list(project.payloads.iter_bytes(reversed(range(1, 51)), window=8))
    assert sorted(got) == [(n + 1, data[n]) for n in range(50)]
    first = [pid for pid, _ in got[:8]]
    assert first == sorted(first)  # a window is read in (volume, offset) order


def test_materialize_writes_the_file_once_and_names_it_as_a_file(project, tmp_path):
    add_ligands(project, 3)
    path = project.payloads.materialize(2, tmp_path / "job", suffix=".sdf")
    assert type(path) is File and Path(path).name == "2.sdf" and Path(path).read_bytes() == structure(1)
    inode = os.stat(path).st_ino
    assert project.payloads.materialize(2, tmp_path / "job", suffix=".sdf") == path and os.stat(path).st_ino == inode
    assert sorted(p.name for p in (tmp_path / "job").iterdir()) == ["2.sdf"]  # no partial left


def test_a_scratch_file_goes_to_dev_shm_when_it_fits_and_is_gone_after(monkeypatch):
    with scratch_file(b"ATOM", suffix=".pdbqt") as path:
        assert path.read_bytes() == b"ATOM" and path.suffix == ".pdbqt"
        if os.access("/dev/shm", os.W_OK):
            assert path.parent == Path("/dev/shm")
    assert not path.exists()
    monkeypatch.setattr(shutil, "disk_usage", lambda _: shutil._ntuple_diskusage(1, 1, 0))
    with pytest.raises(RuntimeError), scratch_file(b"ATOM") as path:
        assert path.parent == Path(tempfile.gettempdir())  # no room in /dev/shm
        raise RuntimeError
    assert not path.exists()
