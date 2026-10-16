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

    conn.execute("INSERT INTO ms_volumes (series, state, size, created_at) VALUES ('s', 'active', 0, '')")
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

import struct
import zlib

from ms_flow import payloads as P
from ms_flow.payloads import Payload, PayloadStore


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
            store.put(session, [Payload(b"committed")])
            session.commit()
        session = project.get_session()
        store.put(session, [Payload(b"x" * 1000)])  # synced, never committed
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
