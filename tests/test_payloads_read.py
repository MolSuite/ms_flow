"""Reading payloads: jobs get bytes in their elements, programs get files, exports get a stream (V3)."""
import os
import shutil
import sqlite3
import sys
import tempfile
import zlib
from contextlib import closing
from pathlib import Path

import pytest

from benchmarks.refactor_optimization import workers
from ms_flow.core.database.project import ProjectStore
from ms_flow.engine import Engine, File, Flow, job, task
from ms_flow.payloads import Payload, scratch_file, with_payloads
from ms_flow.query import QuerySpec, Rows
from ms_flow.sinks import table_sink


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


def test_kinds_name_each_payload_format_for_sources_and_materialize(project, tmp_path):
    add_ligands(project, 3)
    assert project.payloads.kinds([1, 3, 99]) == {1: "sdf", 3: "sdf"}
    rows = with_payloads(project, Rows(project, QuerySpec(table="ligands")), {"structure_payload_id": "structure"}, kinds=True)
    assert [(row["structure"], row["structure_kind"]) for row in rows] == [(structure(n), "sdf") for n in range(3)] + [(None, None)]
    assert Path(project.payloads.materialize(2, tmp_path)).name == "2.sdf"


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


# The peak is VmHWM after resetting it: ru_maxrss survives exec, so it would start at pytest's own.
_READ_ALL = """
import sys
sys.path[:0] = sys.argv[2].split(":")
from ms_flow.core.database.project import ProjectStore
from ms_flow.payloads import with_payloads
from ms_flow.query import QuerySpec, Rows

def status(key):
    return next(int(line.split()[1]) for line in open("/proc/self/status") if line.startswith(key))

project = ProjectStore.open_at(sys.argv[1])
source = with_payloads(project, Rows(project, QuerySpec(table="ligands")), {"structure_payload_id": "structure"},
                       window=int(sys.argv[3]))
next(iter(project.payloads.iter_bytes([1])))
open("/proc/self/clear_refs", "w").write("5")
before = status("VmRSS:")
total = sum(len(row["structure"] or b"") for row in source)
total += sum(len(data) for _, data in project.payloads.iter_bytes(range(1, 100_001)))
print(total, status("VmHWM:") - before)
"""


@pytest.mark.slow
@pytest.mark.skipif(not os.path.exists("/proc/self/clear_refs"), reason="needs Linux's peak RSS reset")
def test_reading_every_payload_keeps_memory_bounded(project):  # I8
    import subprocess

    add_ligands(project, 100_000, size=4096)  # 400 MB, read twice
    path = os.pathsep.join(sys.path)

    def peak_mb(window):
        out = subprocess.run([sys.executable, "-c", _READ_ALL, str(project.db_path), path, str(window)],
                             capture_output=True, text=True, timeout=300)
        assert out.returncode == 0, out.stderr
        total, grown_kb = map(int, out.stdout.split())
        assert total == 2 * 100_000 * 4096
        return grown_kb // 1024

    assert peak_mb(500) < 64
    assert peak_mb(100_000) > 300, "the measure would not see a source that holds everything"


@job(task=task()(workers.uses_payloads), output=table_sink(
    "seen", columns=("n", "got_bytes", "crc", "receptor_path", "receptor_text")))
def far_reads(params, project):
    receptor = project.payloads.materialize(params["receptor"], params["directory"], suffix=".pdbqt")
    for row in with_payloads(project, Rows(project, QuerySpec(table="ligands", filters={"n__lt": 30})),
                             {"structure_payload_id": "structure"}):
        yield {"n": row["n"], "structure": row["structure"], "receptor": receptor}


@pytest.mark.slow
@pytest.mark.parametrize("name", ["ray", "remote"])
def test_a_task_far_away_gets_bytes_and_one_copy_of_the_receptor(project, tmp_path, name):
    from test_payloads_jobs import _executor

    add_ligands(project, 30)
    with project.get_session() as session:
        (receptor,) = project.payloads.put(session, [Payload(b"ATOM receptor\n", kind="pdbqt", series="receptors")])
        session.commit()
    with closing(sqlite3.connect(project.db_path)) as conn, conn:
        conn.execute("CREATE TABLE seen (n INT, got_bytes INT, crc INT, receptor_path TEXT, receptor_text TEXT)")
    here = tmp_path / "materialized"
    with Engine(cpus=2, mem_mb=0) as engine:
        engine.register(name, _executor(name, tmp_path))
        job_id = Flow(engine, project).submit(far_reads, {"receptor": receptor, "directory": str(here)}, executor=name)
        ended = engine.wait(job_id, 180)
        assert ended.status == "completed", ended.error
    with closing(sqlite3.connect(project.db_path)) as conn:
        seen = conn.execute("SELECT n, got_bytes, crc, receptor_path, receptor_text FROM seen ORDER BY n").fetchall()
    assert [(n, got, crc) for n, got, crc, _, _ in seen] == [(n, 1, zlib.crc32(structure(n))) for n in range(30)]
    assert {text for *_, text in seen} == {"ATOM receptor\n"}
    paths = {path for _, _, _, path, _ in seen}
    assert len(paths) == 1 and paths != {str(here / f"{receptor}.pdbqt")}, "one copy, read where the task ran"
