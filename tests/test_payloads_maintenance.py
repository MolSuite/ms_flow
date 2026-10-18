"""V4: check, release, compact and repair, and what a crash in the middle of them leaves."""
from __future__ import annotations

import os
import random
import sqlite3
import subprocess
import sys
import textwrap

import pytest

from ms_flow import payloads as P
from ms_flow.core.database import ProjectStore
from ms_flow.payloads import Payload, PayloadStore


@pytest.fixture
def project(tmp_path, monkeypatch):
    monkeypatch.setattr(P, "VOLUME_SIZE", 4096)  # a few volumes from a few payloads
    monkeypatch.setattr(P, "ORPHAN_GRACE", -1)
    store = open_project(tmp_path / "project.db")
    yield store
    store.dispose()


def open_project(db_path):
    store = ProjectStore.open_at(db_path)
    conn = sqlite3.connect(db_path)  # the app's table: ms_flow only reads the schema
    conn.execute("CREATE TABLE app_items (id INTEGER PRIMARY KEY, key TEXT,"
                 " payload_id INTEGER REFERENCES ms_payloads(id))")
    conn.execute("CREATE INDEX ix_app_items_payload ON app_items(payload_id)")
    conn.close()
    return store


def data(key: str) -> bytes:
    return (f"{key}\n" * 40).encode()


def add(store, project, keys, codec="none", dictionary=None):
    """Payloads and the app rows that point to them, in one transaction (I2)."""
    with project.get_session() as session:
        ids = store.put(session, [Payload(data(k), series="ligands", codec=codec, dictionary=dictionary) for k in keys])
        conn = session.connection()
        for key, pid in zip(keys, ids):
            conn.exec_driver_sql("INSERT INTO app_items (key, payload_id) VALUES (?, ?)", (key, pid))
        session.commit()
    return dict(zip(keys, ids))


def drop(project, keys):
    """The app deletes its rows; their payloads become garbage until released."""
    with project.engine.begin() as conn:
        marks = ",".join("?" * len(keys))
        return [r[0] for r in conn.exec_driver_sql(
            f"DELETE FROM app_items WHERE key IN ({marks}) RETURNING payload_id", tuple(keys))]


def rows(project, sql, params=()):
    with project.engine.connect() as c:
        return c.exec_driver_sql(sql, params).fetchall()


def live(project):
    return dict(rows(project, "SELECT key, payload_id FROM app_items"))


def same_bytes(store, project):
    found = live(project)
    assert {k: store.get(pid) for k, pid in found.items()} == {k: data(k) for k in found}


def run(code, *args, env=None):
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path), **(env or {})}
    return subprocess.run([sys.executable, "-c", textwrap.dedent(code), *map(str, args)], env=env)


# -- V4-1 check, V4-2 release ---------------------------------------------------

def test_check_is_clean_then_names_garbage_and_dead_bytes(project):
    store = PayloadStore(project)
    add(store, project, [f"k{i}" for i in range(60)])
    report = store.check(full=True)
    assert report.clean and report.dead and set(report.dead.values()) == {0.0}
    gone = drop(project, [f"k{i}" for i in range(20)])
    report = store.check()
    assert report.ok and not report.clean and sorted(report.unreferenced) == sorted(gone)
    (store.directory / "999999999.vol").write_bytes(b"left by a dead writer")
    assert store.check().orphans == [999999999]
    store.close()


def test_release_keeps_referenced_rows_and_drops_emptied_sealed_volumes(project):
    store = PayloadStore(project)
    ids = add(store, project, [f"k{i}" for i in range(60)])
    first = rows(project, "SELECT volume_id FROM ms_payloads WHERE id = ?", (ids["k0"],))[0][0]
    keys = [k for k, pid in ids.items() if rows(project, "SELECT volume_id FROM ms_payloads WHERE id = ?", (pid,))[0][0] == first]
    assert store.release(ids.values()) == 0  # all still referenced
    gone = drop(project, keys)
    assert store.release(gone) == len(keys)
    assert not store.path(first).exists() and not rows(project, "SELECT 1 FROM ms_volumes WHERE id = ?", (first,))
    same_bytes(store, project)
    assert store.check(full=True).clean
    store.close()


# -- V4-3 compact, V5-7 recompress -----------------------------------------------

def test_compact_moves_live_needles_keeps_ids_and_never_touches_the_active_volume(project):
    store = PayloadStore(project)
    ids = add(store, project, [f"k{i}" for i in range(200)])
    active = rows(project, "SELECT id FROM ms_volumes WHERE state = 'active'")
    sealed = [r[0] for r in rows(project, "SELECT id FROM ms_volumes WHERE state = 'sealed'")]
    store.release(drop(project, [f"k{i}" for i in range(200) if i % 3]))  # two thirds dead everywhere
    size = lambda: sum(f.stat().st_size for f in store.directory.glob("*.vol"))
    before = size()
    stats = store.compact()
    assert stats["volumes"] == len(sealed) and stats["written_mb"] == pytest.approx(stats["read_mb"])
    assert size() < before / 2
    assert rows(project, "SELECT id FROM ms_volumes WHERE state = 'active'") == active
    assert not any(store.path(v).exists() for v in sealed)
    assert {k: pid for k, pid in live(project).items()} == {k: ids[k] for k in live(project)}  # ids never change
    same_bytes(store, project)
    assert store.check(full=True).clean and store.compact()["volumes"] == 0  # nothing left to gain
    store.close()


def test_compact_follows_order_by(project):
    store = PayloadStore(project)
    add(store, project, [f"k{i:03d}" for i in range(100)])
    store.release(drop(project, [f"k{i:03d}" for i in range(0, 100, 2)]))
    store.compact(order_by="SELECT payload_id FROM app_items ORDER BY key DESC")
    on_disk = [r[0] for r in rows(project, "SELECT a.key FROM app_items a JOIN ms_payloads p ON p.id = a.payload_id"
                                            " JOIN ms_volumes v ON v.id = p.volume_id WHERE v.state = 'sealed'"
                                            ' ORDER BY p.volume_id, p."offset"')]
    assert on_disk and on_disk == sorted(on_disk, reverse=True)
    store.close()


def test_compact_recompresses_with_the_series_dictionary(project, monkeypatch):
    monkeypatch.setattr(P, "VOLUME_SIZE", 1 << 20)
    store = PayloadStore(project)
    add(store, project, [f"mol{i}" for i in range(400)], codec="zlib")
    add(store, project, [f"x{i}" for i in range(400)], codec="zstd")
    dictionary = store.train_dictionary("ligands", samples=400, size=4096)
    store.release(drop(project, [f"mol{i}" for i in range(0, 400, 2)]))
    with project.engine.begin() as c:  # seal it, as a full volume would be
        c.exec_driver_sql("UPDATE ms_volumes SET state = 'sealed'")
    store.compact(min_dead=0.1)
    codecs = {r[0] for r in rows(project, "SELECT codec FROM ms_payloads")}
    assert codecs == {P.CODEC_ZSTD}
    zstandard = P._zstd()
    dict_ids = {zstandard.get_frame_parameters(store._stored(pid, loc)[0]).dict_id
                for pid, *loc in rows(project, 'SELECT id, volume_id, "offset", length, raw_length, codec, crc32 FROM ms_payloads')}
    assert dict_ids == {int(os.path.basename(dictionary).split(".")[0])}
    same_bytes(store, project)
    store.close()


# -- V4-4 the compaction as a job ----------------------------------------------

def test_compaction_job_runs_after_jobs_writing_payloads(project):
    from ms_flow.engine import Engine, Flow, ThreadExecutor

    store = PayloadStore(project)
    add(store, project, [f"k{i}" for i in range(200)])
    store.release(drop(project, [f"k{i}" for i in range(200) if i % 3]))
    assert P.compaction().__job__["reads"] == {"ms_payloads"}  # and every table writer writes it (test_engine_flow)
    with Engine(cpus=2, mem_mb=0) as engine:
        engine.register("thread", ThreadExecutor(2))
        flow = Flow(engine, project)
        job_id = flow.submit(P.compaction(), executor="thread")
        assert flow._writing[job_id] == {"ms_payloads", "ms_volumes"}
        ended = engine.wait(job_id, 60)
        assert ended.status == "completed", ended.error
    assert not rows(project, "SELECT 1 FROM ms_volumes WHERE state = 'sealed' AND id IN (SELECT volume_id FROM ms_payloads"
                             " GROUP BY volume_id HAVING count(*) < 10)")
    same_bytes(store, project)
    store.close()


# -- V4-5 repair, V4-6 F8 and F9 --------------------------------------------------

def test_repair_clears_garbage_and_never_deletes_a_referenced_row(project):
    store = PayloadStore(project)
    ids = add(store, project, [f"k{i}" for i in range(60)])
    drop(project, ["k1", "k2"])
    (store.directory / "999999999.vol").write_bytes(b"orphan")
    victim = rows(project, "SELECT volume_id FROM ms_payloads WHERE id = ?", (ids["k30"],))[0][0]
    store.path(victim).unlink()  # F9: someone deleted a volume
    report = store.repair()
    assert not report.unreferenced and not report.orphans and not report.empty
    assert report.missing_volumes == [victim] and ids["k30"] in report.broken and not report.ok
    assert rows(project, "SELECT 1 FROM ms_payloads WHERE id = ?", (ids["k30"],))  # the app decides
    with pytest.raises(P.PayloadCorrupt, match=f"volume {victim} is missing"):
        store.get(ids["k30"])
    store.close()


def test_corrupt_bytes_are_listed_by_full_check_and_never_returned(project):
    store = PayloadStore(project)
    ids = add(store, project, [f"k{i}" for i in range(20)])
    volume, offset = rows(project, 'SELECT volume_id, "offset" FROM ms_payloads WHERE id = ?', (ids["k5"],))[0]
    with open(store.path(volume), "r+b") as f:  # F8: bit rot inside the needle's bytes
        f.seek(offset + P.NEEDLE + 3)
        f.write(b"\x00")
    assert store.check().ok  # only reading finds it
    report = store.check(full=True)
    assert list(report.broken) == [ids["k5"]] and "checksum" in report.broken[ids["k5"]]
    with pytest.raises(P.PayloadCorrupt):
        store.get(ids["k5"])
    store.compact(min_dead=0.0)  # a corrupt needle is never copied: it stays, and so does its volume
    assert rows(project, "SELECT volume_id FROM ms_payloads WHERE id = ?", (ids["k5"],)) == [(volume,)]
    assert store.path(volume).exists() and list(store.check(full=True).broken) == [ids["k5"]]
    store.close()


def test_missing_dictionary_is_named_by_check(project):
    store = PayloadStore(project)
    add(store, project, [f"k{i}" for i in range(200)], codec="zstd")
    dictionary = store.train_dictionary("ligands", samples=200, size=2048)
    pid = add(store, project, ["new"], codec="zstd", dictionary=dictionary)["new"]
    os.unlink(dictionary)
    report = store.check(full=True)
    assert report.missing_dicts and "dictionary" in report.broken[pid] and not report.ok
    store.close()


# -- V4-7 F10 and F11: a compaction killed halfway --------------------------------

_COMPACT = """
import sys
from ms_flow import payloads as P
from ms_flow.core.database import ProjectStore
P.VOLUME_SIZE = 4096
project = ProjectStore.open_at(sys.argv[1])
P.PayloadStore(project).compact()
"""


@pytest.mark.parametrize("point", ["copied", "swapped"])
def test_compaction_killed_halfway_leaves_garbage_never_a_broken_reference(project, point):
    store = PayloadStore(project)
    add(store, project, [f"k{i}" for i in range(200)])
    store.release(drop(project, [f"k{i}" for i in range(200) if i % 3]))
    store.close()
    assert run(_COMPACT, project.db_path, env={"MSFLOW_PAYLOADS_FAULT": point}).returncode == 137
    store = PayloadStore(project)  # opening seals what the dead compaction left active
    report = store.check(full=True)
    assert report.ok and not report.unreferenced
    if point == "copied":  # F10: the copies are garbage, the old volumes intact
        assert report.empty
    else:  # F11: the old volumes are trash, still on disk
        assert report.empty and all(store.path(v).exists() for v in report.empty)
    same_bytes(store, project)
    assert store.repair().clean
    store.compact()
    assert store.check(full=True).clean
    same_bytes(store, project)
    store.close()


# -- V4-8 F12: reading while another process compacts -----------------------------

_READER = """
import sys, time
from ms_flow import payloads as P
from ms_flow.core.database import ProjectStore
project = ProjectStore.open_at(sys.argv[1])
store = P.PayloadStore(project)
end, loops = time.time() + float(sys.argv[2]), 0
while time.time() < end:
    with project.engine.connect() as c:
        found = c.exec_driver_sql("SELECT key, payload_id FROM app_items WHERE key LIKE 'stable%'").fetchall()
    got = store.get_many(pid for _, pid in found)
    assert all(got[pid] == (key + "\\n").encode() * 40 for key, pid in found), "bytes differ"
    loops += 1
print(loops)
"""


def test_a_reader_in_another_process_never_fails_while_volumes_are_compacted(project):
    store = PayloadStore(project)
    add(store, project, [f"{'stable' if i % 3 == 0 else 'k'}{i}" for i in range(300)])  # the reader reads the stable ones
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
    reader = subprocess.Popen([sys.executable, "-c", _READER, str(project.db_path), "4"], env=env, stdout=subprocess.PIPE)
    compactions, n = 0, 300
    while reader.poll() is None:
        store.release(drop(project, random.Random(n).sample(sorted(k for k in live(project) if k[0] == "k"), 40)))
        add(store, project, [f"k{i}" for i in range(n, n + 40)])
        n += 40
        compactions += store.compact(min_dead=0.2)["volumes"] > 0
    assert reader.returncode == 0 and int(reader.stdout.read()) > 3 and compactions > 3
    same_bytes(store, project)
    store.close()


# -- V4-9 random sequences with a kill anywhere --------------------------------

def _sequence(db_path, seed, pipe):
    """In a forked child: random adds, deletes and compactions, killed at a random write point."""
    rng = random.Random(seed)
    P._FAULT_POINT = rng.choice(["appended", "synced", "committed", "copied", "swapped", ""])
    P._FAULT_AT, P._fault_seen = str(rng.randint(1, 6)), 0
    project = ProjectStore.open_at(db_path)
    project.engine.dispose(close=False)  # the parent's pooled connections are not ours
    store = PayloadStore(project)
    for step in range(rng.randint(3, 10)):
        op = rng.choice(["add", "add", "drop", "compact"])
        keys = [f"{seed}-{step}-{i}" for i in range(rng.randint(1, 30))]
        if op == "drop":
            keys = rng.sample(sorted(live(project)), min(len(live(project)), rng.randint(1, 30)))
        os.write(pipe, f"B {op} {' '.join(keys)}\n".encode())
        if op == "add":
            add(store, project, keys)
        elif op == "drop":
            store.release(drop(project, keys))
        else:
            store.compact(min_dead=rng.choice([0.1, 0.5]))
        os.write(pipe, b"E\n")
    os._exit(0)


def test_random_sequences_killed_anywhere_end_clean(tmp_path):
    """1000 sequences (MSFLOW_V4_SEQUENCES), in rounds of 25 that share a project, so state builds up
    across kills but the project stays small. In a process of their own: pytest has threads, and
    forking a process with threads can deadlock the child."""
    sequences = int(os.environ.get("MSFLOW_V4_SEQUENCES", "1000"))
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
    done = subprocess.run([sys.executable, __file__, str(tmp_path), str(sequences)], env=env,
                          capture_output=True, text=True, timeout=1200)
    assert done.returncode == 0, done.stderr[-3000:]
    assert done.stdout.split() == ["clean", str(sequences)]


def _random_rounds(tmp_path, sequences):
    P.VOLUME_SIZE, P.ORPHAN_GRACE = 4096, -1
    project = None
    for seed in range(sequences):
        if seed % 25 == 0:
            if project is not None:
                project.dispose()
            project = open_project(tmp_path / f"round{seed // 25}" / "project.db")
            expected: set[str] = set()
        project.engine.dispose()  # no pooled connection crosses the fork
        read, write = os.pipe()
        pid = os.fork()
        if pid == 0:  # pragma: no cover - the child
            os.close(read)
            try:
                _sequence(project.db_path, seed, write)
            finally:
                os._exit(1)
        os.close(write)
        with os.fdopen(read) as log:
            lines = log.read().splitlines()
        _, status = os.waitpid(pid, 0)
        assert os.WEXITSTATUS(status) in (0, 137), (seed, status)
        pending = None
        for line in lines:
            if line.startswith("B "):
                op, *keys = line[2:].split(" ")
                pending = (op, set(keys))
            else:
                op, keys = pending
                expected = expected | keys if op == "add" else expected - keys if op == "drop" else expected
                pending = None
        store = PayloadStore(project)  # recovery, as the next open does
        found = set(live(project))
        allowed = [expected]
        if pending and pending[0] == "add":
            allowed.append(expected | pending[1])
        elif pending and pending[0] == "drop":
            allowed.append(expected - pending[1])
        assert found in allowed, seed
        expected = found
        report = store.repair()
        assert report.clean, (seed, report)
        same_bytes(store, project)
        store.close()
    store = PayloadStore(project)
    assert store.check(full=True).clean
    store.close()
    print("clean", sequences)


if __name__ == "__main__":
    from pathlib import Path

    _random_rounds(Path(sys.argv[1]), int(sys.argv[2]))
