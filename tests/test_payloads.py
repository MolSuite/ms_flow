from __future__ import annotations

import os
import sqlite3

import pytest

from ms_flow.core.database import ProjectStore


@pytest.fixture
def project(tmp_path):
    store = ProjectStore.open_at(tmp_path / "project.db")
    yield store
    store.dispose()


def _app_table(db_path):
    """A table the way an app declares it: ms_flow never sees this schema."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(
        "CREATE TABLE app_items (id INTEGER PRIMARY KEY,"
        " structure_payload_id INTEGER REFERENCES ms_payloads(id))"
    )
    conn.execute("CREATE INDEX ix_app_items_payload ON app_items(structure_payload_id)")
    return conn


def test_payload_tables_exist_with_autoincrement_and_enforced_fk(project):
    conn = _app_table(project.db_path)
    sql = dict(conn.execute("SELECT name, sql FROM sqlite_master WHERE type='table'").fetchall())
    assert "AUTOINCREMENT" in sql["ms_volumes"] and "AUTOINCREMENT" in sql["ms_payloads"]

    conn.execute("INSERT INTO ms_volumes (series, state, size) VALUES ('s', 'active', 0)")
    conn.execute(
        'INSERT INTO ms_payloads (volume_id, "offset", length, raw_length, codec, crc32, kind)'
        " VALUES (1, 32, 0, 0, 0, 0, '')"
    )
    conn.execute("INSERT INTO app_items (structure_payload_id) VALUES (1)")
    with pytest.raises(sqlite3.IntegrityError):  # a reference to bytes that don't exist
        conn.execute("INSERT INTO app_items (structure_payload_id) VALUES (99)")
    with pytest.raises(sqlite3.IntegrityError):  # a payload still referenced
        conn.execute("DELETE FROM ms_payloads WHERE id = 1")
    conn.close()


# -- V1-2 writer ------------------------------------------------------------

import functools
import struct
import zlib

from ms_flow import payloads as P
from ms_flow.payloads import PayloadStore

# The volume format, with the bytes as given: codecs are tested in test_payloads_codecs.py.
Payload = functools.partial(P.Payload, codec="none")


def _put(store, project, items):
    with project.get_session() as session:
        ids = store.put(session, items)
        session.commit()
    return ids


def _rows(project, sql, params=()):
    with project.engine.connect() as c:
        return c.exec_driver_sql(sql, params).fetchall()


def test_put_writes_volume_header_and_needles(project):
    store = PayloadStore(project)
    ids = _put(store, project, [Payload(b"abc", kind="sdf", series="ligands"), Payload(b"", series="ligands")])
    store.close()
    (vid, off, length, crc, kind), second = _rows(
        project, 'SELECT volume_id, "offset", length, crc32, kind FROM ms_payloads ORDER BY id'
    )
    raw = store.path(vid).read_bytes()
    magic, version, _, header_vid, series = struct.unpack_from("<5sBHQ16s", raw)
    assert (magic, version, header_vid, series.rstrip(b"\0")) == (b"MSVOL", 1, vid, b"ligands")
    assert struct.unpack_from("<IIIHH", raw, off) == (zlib.crc32(b"abc"), 3, 3, 0, 0)
    assert raw[off + 16 : off + 16 + length] == b"abc" and crc == zlib.crc32(b"abc") and kind == "sdf"
    assert second[1] == off + 16 + 3 and second[2] == 0  # a 0-byte payload is a bare needle
    assert len(raw) == second[1] + 16
    assert _rows(project, "SELECT state, size FROM ms_volumes") == [("active", len(raw))]
    assert len(ids) == 2 and ids[1] == ids[0] + 1


def test_volume_switches_at_size_and_large_payload_goes_alone(project, monkeypatch):
    monkeypatch.setattr(P, "VOLUME_SIZE", P.HEADER + 2 * (16 + 10))
    store = PayloadStore(project)
    _put(store, project, [Payload(b"x" * 10)] * 3 + [Payload(b"y" * 500), Payload(b"z" * 10)])
    store.close()
    placed = _rows(project, 'SELECT volume_id, "offset", length FROM ms_payloads ORDER BY id')
    assert [(v, n) for v, _, n in placed] == [(1, 10), (1, 10), (2, 10), (3, 500), (4, 10)]
    stored = [store.path(v).read_bytes()[o + 16 : o + 16 + n] for v, o, n in placed]
    assert stored == [b"x" * 10] * 3 + [b"y" * 500, b"z" * 10]
    assert _rows(project, "SELECT id, state FROM ms_volumes ORDER BY id") == [
        (1, "sealed"), (2, "sealed"), (3, "sealed"), (4, "active")
    ]


def test_one_active_volume_per_series(project):
    store = PayloadStore(project)
    _put(store, project, [Payload(b"a", series="ligands"), Payload(b"b", series="poses"), Payload(b"c", series="ligands")])
    store.close()
    assert _rows(project, "SELECT volume_id FROM ms_payloads ORDER BY id") == [(1,), (2,), (1,)]
    assert _rows(project, "SELECT series FROM ms_volumes ORDER BY id") == [("ligands",), ("poses",)]


def test_volume_file_left_by_a_dead_transaction_is_never_reused(project):
    store = PayloadStore(project)
    store.directory.mkdir()
    store.path(1).write_bytes(b"leftover")
    _put(store, project, [Payload(b"a")])
    store.close()
    assert _rows(project, "SELECT id FROM ms_volumes") == [(2,)]
    assert store.path(1).read_bytes() == b"leftover"


# -- V1-3 reader ------------------------------------------------------------


def test_get_and_get_many_round_trip_in_location_order(project, monkeypatch):
    monkeypatch.setattr(P, "VOLUME_SIZE", P.HEADER + 3 * (16 + 8))
    store = PayloadStore(project)
    blobs = [f"blob{i:04d}".encode() for i in range(10)]
    ids = _put(store, project, [Payload(b, series="s") for b in blobs])
    assert store.get(ids[3]) == blobs[3]
    reads = []
    real = P.os.pread
    monkeypatch.setattr(P.os, "pread", lambda fd, n, off: reads.append(off) or real(fd, n, off))
    asked = [ids[9], ids[0], ids[5], ids[0], ids[2]]  # unordered, one repeated
    got = store.get_many(asked)
    assert got == {i: blobs[ids.index(i)] for i in asked}
    assert len(reads) == 4  # each id read once
    located = [_rows(project, 'SELECT volume_id, "offset" FROM ms_payloads WHERE id=?', (i,))[0] for i in got]
    assert located == sorted(located)
    with pytest.raises(KeyError):
        store.get_many([ids[0], 10_000])
    store.close()


def test_reader_keeps_at_most_max_open_fds(project, monkeypatch):
    monkeypatch.setattr(P, "VOLUME_SIZE", P.HEADER + 16 + 1)  # one payload per volume
    monkeypatch.setattr(P, "MAX_OPEN_FDS", 2)
    store = PayloadStore(project)
    ids = _put(store, project, [Payload(bytes([i])) for i in range(6)])
    store.close()
    before = len(os.listdir("/proc/self/fd"))
    for _ in range(2):
        assert store.get_many(ids) == {pid: bytes([i]) for i, pid in enumerate(ids)}
        assert len(store._fds) == 2 and len(os.listdir("/proc/self/fd")) == before + 2
    store.close()


# -- V1-4 put inside the caller's transaction --------------------------------


def test_put_syncs_bytes_and_new_files_before_returning(project, monkeypatch):
    synced = []
    monkeypatch.setattr(P, "_datasync", lambda fd: synced.append(("file", fd)))
    monkeypatch.setattr(P, "_fsync_dir", lambda d: synced.append(("dir", d)))
    store = PayloadStore(project)
    with project.get_session() as session:
        store.put(session, [Payload(b"a", series="s1"), Payload(b"b", series="s2")])
        assert sorted(kind for kind, _ in synced) == ["dir", "file", "file"]
        synced.clear()
        store.put(session, [Payload(b"c", series="s1")])
        assert [kind for kind, _ in synced] == ["file"]  # same file, no new directory entry
        session.commit()
    store.close()


def test_payload_rows_commit_and_roll_back_with_the_app_rows(project):
    conn = _app_table(project.db_path)
    conn.close()
    store = PayloadStore(project)
    with project.get_session() as session:
        (pid,) = store.put(session, [Payload(b"kept")])
        session.connection().exec_driver_sql("INSERT INTO app_items (structure_payload_id) VALUES (?)", (pid,))
        session.commit()
    with project.get_session() as session:
        (lost,) = store.put(session, [Payload(b"lost")])
        session.connection().exec_driver_sql("INSERT INTO app_items (structure_payload_id) VALUES (?)", (lost,))
        session.rollback()
    assert _rows(project, "SELECT structure_payload_id FROM app_items") == [(pid,)]
    assert _rows(project, "SELECT id FROM ms_payloads") == [(pid,)]
    with pytest.raises(KeyError):
        store.get(lost)
    assert store.get(pid) == b"kept"
    store.close()


def test_rollback_truncates_reused_volume_and_removes_new_ones(project, monkeypatch):
    monkeypatch.setattr(P, "VOLUME_SIZE", P.HEADER + 2 * (16 + 4))
    store = PayloadStore(project)
    (first,) = _put(store, project, [Payload(b"aaaa")])
    size = store.path(1).stat().st_size
    for end in ("rollback", "close"):
        session = project.get_session()
        store.put(session, [Payload(b"bbbb"), Payload(b"cccc")])  # fills volume 1, seals it, opens another
        assert len(list(store.directory.iterdir())) == 2
        session.rollback() if end == "rollback" else session.close()
        session.close()
        assert [p.name for p in store.directory.iterdir()] == ["000000001.vol"]
        assert store.path(1).stat().st_size == size
        assert _rows(project, "SELECT id, state, size FROM ms_volumes") == [(1, "active", size)]
    (second,) = _put(store, project, [Payload(b"dddd")])  # the volume is reused where it was
    assert _rows(project, 'SELECT volume_id, "offset" FROM ms_payloads WHERE id=?', (second,)) == [(1, size)]
    assert store.get_many([first, second]) == {first: b"aaaa", second: b"dddd"}
    store.close()


# -- V1-5 recovery at open -------------------------------------------------

import subprocess
import sys
import textwrap


def _run(code, *args):
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
    return subprocess.run([sys.executable, "-c", textwrap.dedent(code), *map(str, args)], env=env)


def test_open_cuts_the_uncommitted_tail_of_a_dead_writer(project):
    done = _run(
        """
        import os, sys
        from ms_flow.core.database import ProjectStore
        from ms_flow.payloads import Payload, PayloadStore
        project = ProjectStore.open_at(sys.argv[1])
        store = PayloadStore(project)
        with project.get_session() as session:
            store.put(session, [Payload(b"committed", codec="none")])
            session.commit()
        session = project.get_session()
        store.put(session, [Payload(b"x" * 1000, codec="none")])  # synced, never committed
        os._exit(137)
        """,
        project.db_path,
    )
    assert done.returncode == 137
    volume = project.db_path.parent / "volumes" / "000000001.vol"
    assert volume.stat().st_size > 1000
    live = PayloadStore(project)
    end = P.HEADER + 16 + len(b"committed")
    assert volume.stat().st_size == end
    (kept,) = _put(live, project, [Payload(b"live")])
    PayloadStore(project).close()  # another writer opening leaves the live one alone
    assert _rows(project, "SELECT id, state, size FROM ms_volumes ORDER BY id") == [
        (1, "sealed", end), (2, "active", P.HEADER + 16 + 4)
    ]
    assert live.get(kept) == b"live" and live.get(1) == b"committed"
    live.close()


# -- V1-6 edges ------------------------------------------------------------


def test_edges_empty_exact_fit_one_over_and_oversize(project, monkeypatch):
    monkeypatch.setattr(P, "VOLUME_SIZE", P.HEADER + 16 + 100)
    store = PayloadStore(project)
    blobs = [b"", b"e" * 84, b"f" * 100, b"g" * 101, b"h"]
    ids = _put(store, project, [Payload(b) for b in blobs])
    # "" and 84 bytes fill volume 1 exactly; 100 fits a fresh volume exactly;
    # 101 is larger than any volume and goes alone; "h" opens the next one.
    assert _rows(project, "SELECT volume_id FROM ms_payloads ORDER BY id") == [(1,), (1,), (2,), (3,), (4,)]
    assert store.path(1).stat().st_size == P.VOLUME_SIZE == store.path(2).stat().st_size
    assert store.get_many(reversed(ids)) == dict(zip(ids, blobs))
    assert store.get(ids[0]) == b""
    store.close()


# -- V1-7 checksums --------------------------------------------------------


def test_corrupt_bytes_raise_payload_corrupt_never_return_data(project):
    store = PayloadStore(project)
    ids = _put(store, project, [Payload(b"first"), Payload(b"second"), Payload(b"third")])
    store.close()
    offsets = dict(_rows(project, 'SELECT id, "offset" FROM ms_payloads'))
    path = store.path(1)
    raw = bytearray(path.read_bytes())
    raw[offsets[ids[0]] + 16] ^= 0x01  # one bit of the bytes
    raw[offsets[ids[1]] + 4] ^= 0x01  # the stored length in the needle header
    path.write_bytes(bytes(raw[: offsets[ids[2]] + 16 + 2]))  # the last needle cut short
    for pid, reason in zip(ids, ("checksum", "checksum", "truncated")):
        with pytest.raises(P.PayloadCorrupt, match=reason) as caught:
            store.get(pid)
        assert caught.value.payload_id == pid
    store.close()  # an open descriptor still reads an unlinked file (F12)
    path.unlink()
    with pytest.raises(P.PayloadCorrupt, match="missing"):
        store.get(ids[0])
    store.close()


# -- V1-8 two writers and a reader, in three processes (F15) ----------------

_WRITER = """
import sys
from ms_flow.core.database import ProjectStore
from ms_flow.payloads import Payload, PayloadStore
project = ProjectStore.open_at(sys.argv[1])
store = PayloadStore(project)
name = sys.argv[2]
for txn in range(300):
    keep = txn % 3 != 2
    tag = "ok" if keep else "rolled-back"
    texts = [f"{name}:{txn}:{i}:{tag}" for i in range(5)]
    with project.get_session() as session:
        store.put(session, [Payload(t.encode(), kind=t) for t in texts])
        session.commit() if keep else session.rollback()
"""

_READER = """
import sys
from ms_flow.core.database import ProjectStore
from ms_flow.payloads import PayloadStore
project = ProjectStore.open_at(sys.argv[1])
store = PayloadStore(project)
seen, partial = 0, set()
while seen < 2 * 200 * 5:
    with project.engine.connect() as c:
        kinds = dict(c.exec_driver_sql("SELECT id, kind FROM ms_payloads").fetchall())
    for pid, data in store.get_many(kinds).items():
        assert data == kinds[pid].encode() and kinds[pid].endswith(":ok"), (pid, data)
    seen = len(kinds)
    partial.add(seen)
print(len(partial))
"""


def test_two_writing_processes_and_a_reader_see_only_committed_bytes(project):
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
    procs = [
        subprocess.Popen([sys.executable, "-c", code, str(project.db_path), *extra], env=env, stdout=subprocess.PIPE)
        for code, extra in ((_READER, ()), (_WRITER, ("a",)), (_WRITER, ("b",)))
    ]
    assert [p.wait(timeout=120) for p in procs] == [0, 0, 0]
    assert int(procs[0].stdout.read()) > 2  # the reader ran while the writers wrote
    rows = _rows(project, "SELECT volume_id, kind FROM ms_payloads")
    assert len(rows) == 2 * 200 * 5
    owners = {}
    for volume_id, kind in rows:
        owners.setdefault(volume_id, set()).add(kind.split(":")[0])
    assert all(len(names) == 1 for names in owners.values())  # one writer per volume (I3)
    store = PayloadStore(project)
    kinds = dict(_rows(project, "SELECT id, kind FROM ms_payloads"))
    assert {pid: data.decode() for pid, data in store.get_many(kinds).items()} == kinds
    store.close()


# -- D2: app references are found from the schema --------------------------


def test_referencing_columns_come_from_the_app_schema(project):
    conn = _app_table(project.db_path)
    conn.execute("CREATE TABLE poses (id INTEGER PRIMARY KEY, pose_payload_id INTEGER REFERENCES ms_payloads(id), n INTEGER)")
    conn.close()
    store = PayloadStore(project)
    assert sorted(store.referencing_columns()) == [("app_items", "structure_payload_id"), ("poses", "pose_payload_id")]
    store.close()


# -- V2-1 the type that travels -----------------------------------------------

import pickle
import subprocess
import sys


def test_payload_pickles_its_bytes_once_and_loads_without_sql():
    item = Payload(os.urandom(100_000), kind="sdf", series="ligands")
    blob = pickle.dumps(item, protocol=pickle.HIGHEST_PROTOCOL)
    assert pickle.loads(blob) == item.packed()
    assert len(blob) - len(item.data) < 200  # the bytes once, plus the class and two names

    probe = ("import pickle, sys; item = pickle.loads(sys.stdin.buffer.read()); "
             "print(len(item.data), 'sqlalchemy' in sys.modules)")
    out = subprocess.run([sys.executable, "-c", probe], input=blob, capture_output=True, check=True,
                         env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}).stdout.split()
    assert out == [b"100000", b"False"]  # a worker that loads a result never pulls in the database
