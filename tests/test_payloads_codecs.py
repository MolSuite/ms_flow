"""Codecs of payloads: each needle says its own, compression happens in the worker, zstd dictionaries per series (V5)."""
import pickle
import random
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

import pytest

from benchmarks.refactor_optimization import workers
from ms_flow.core.database.project import ProjectStore
from ms_flow.engine import Engine, File, Flow, job, task
from ms_flow.payloads import Payload, PayloadCorrupt
from ms_flow.sinks import table_sink


def specimen(n: int) -> bytes:
    """A text record: the shape repeats, the numbers do not."""
    rng = random.Random(n)
    rows = "".join(f"{rng.uniform(-9, 9):10.4f}{rng.uniform(-9, 9):10.4f}{rng.uniform(-9, 9):10.4f} "
                   f"{rng.choice('AABCDE')}   0  0  0  0  0  0  0  0  0  0  0  0\n" for _ in range(rng.randint(15, 40)))
    return (f"REC{n:07d}\n     header          v1\n\n" + rows + "END\n> <score>\n"
            f"{rng.uniform(-12, -3):.2f}\n\n----\n").encode()


@pytest.fixture
def project(tmp_path):
    store = ProjectStore.open_at(tmp_path / "project.db")
    yield store
    store.dispose()


def put(project, items):
    with project.get_session() as session:
        ids = project.payloads.put(session, items)
        session.commit()
    return ids


def test_every_codec_reads_back_and_a_volume_mixes_them(project):
    data = [specimen(n) for n in range(4)]
    ids = put(project, [Payload(data[0], codec="none"), Payload(data[1], codec="zlib"), Payload(data[2], codec="zstd"),
                        Payload(data[3])])
    assert project.payloads.get_many(ids) == dict(zip(ids, data))
    with closing(sqlite3.connect(project.db_path)) as conn:
        rows = conn.execute("SELECT volume_id, codec, length, raw_length FROM ms_payloads ORDER BY id").fetchall()
    assert [r[1] for r in rows] == [0, 1, 2, 2] and len({r[0] for r in rows}) == 1  # zstd is the default
    assert rows[0][2] == rows[0][3] and all(length < raw for _, _, length, raw in rows[1:])
    with pytest.raises(ValueError, match="unknown codec"):
        put(project, [Payload(b"x", codec="lzma")])


def test_pickling_compresses_so_the_worker_does_it_and_the_writer_only_appends(project, monkeypatch):
    item = Payload(specimen(1) * 20, kind="csv", series="guests")
    blob = pickle.dumps(item)
    assert len(blob) < len(item.data) / 4  # what crosses the process is compressed
    arrived = pickle.loads(blob)
    assert arrived.raw_length == len(item.data) and arrived.packed() is arrived

    from ms_flow import payloads
    monkeypatch.setattr(payloads, "_compress", lambda *a: pytest.fail("the writer compressed again"))
    (pid,) = put(project, [arrived])
    assert project.payloads.get(pid) == item.data


def test_a_dictionary_trained_on_a_series_compresses_more_and_is_named_by_its_frames(project):
    assert project.payloads.dictionary("guests") is None
    put(project, [Payload(specimen(n), series="guests") for n in range(2000)])
    path = project.payloads.train_dictionary("guests", samples=1000)
    assert type(path) is File and project.payloads.dictionary("guests") == path and Path(path).name == "1.zdict"

    data = [specimen(n) for n in range(5000, 5300)]
    plain = put(project, [Payload(d, series="guests") for d in data])
    with_dict = put(project, [Payload(d, series="guests", dictionary=path) for d in data])
    assert project.payloads.get_many(with_dict) == dict(zip(with_dict, data))
    with closing(sqlite3.connect(project.db_path)) as conn:
        size = lambda ids: conn.execute(f"SELECT sum(length) FROM ms_payloads WHERE id IN ({','.join(map(str, ids))})").fetchone()[0]
        assert size(with_dict) < 0.8 * size(plain), (size(with_dict), size(plain))

    Path(path).unlink()
    reader = ProjectStore.open_at(project.db_path)  # nothing cached
    from ms_flow import payloads
    payloads._codecs.__dict__.clear()
    with pytest.raises(PayloadCorrupt, match=r"zstd dictionary .*1\.zdict is missing"):
        reader.payloads.get(with_dict[0])
    assert reader.payloads.get(plain[0]) == data[0]


@job(task=task()(workers.packs_with_dictionary), output=table_sink("guests", columns=("n", "structure_payload_id")))
def packed_far(params, project):
    dictionary = project.payloads.dictionary("guests")
    return [{"n": n, "data": specimen(n), "dictionary": dictionary} for n in range(3000, 3040)]


@pytest.mark.slow
@pytest.mark.parametrize("name", ["process", "remote"])
def test_a_job_compresses_with_the_series_dictionary_where_its_tasks_run(project, tmp_path, name):
    from test_payloads_jobs import _executor

    with closing(sqlite3.connect(project.db_path)) as conn, conn:
        conn.execute("CREATE TABLE guests (id INTEGER PRIMARY KEY, n INT NOT NULL,"
                     " structure_payload_id INTEGER REFERENCES ms_payloads(id))")
    put(project, [Payload(specimen(n), series="guests") for n in range(1000)])
    project.payloads.train_dictionary("guests")
    with Engine(cpus=2, mem_mb=0) as engine:
        engine.register(name, _executor(name, tmp_path))
        ended = engine.wait(Flow(engine, project).submit(packed_far, {}, executor=name), 180)
        assert ended.status == "completed", ended.error
    with closing(sqlite3.connect(project.db_path)) as conn:
        rows = conn.execute("SELECT n, structure_payload_id FROM guests ORDER BY n").fetchall()
    assert [n for n, _ in rows] == list(range(3000, 3040))
    got = project.payloads.get_many([pid for _, pid in rows])
    assert all(got[pid] == specimen(n) for n, pid in rows)
    import zstandard
    from ms_flow import payloads
    with closing(sqlite3.connect(project.db_path)) as conn:
        vol, off, length = conn.execute('SELECT volume_id, "offset", length FROM ms_payloads WHERE id = ?', (rows[0][1],)).fetchone()
    frame = project.payloads.path(vol).read_bytes()[off + payloads.NEEDLE: off + payloads.NEEDLE + length]
    assert zstandard.get_frame_parameters(frame).dict_id == 1  # compressed with the dictionary, far away


def _zstandard_fails(monkeypatch, error):
    """`import zstandard` raises `error`, as where the package is missing or cannot load."""
    import builtins
    import threading

    from ms_flow import payloads

    real_import = builtins.__import__

    def failing_import(name, *args, **kwargs):
        if name == "zstandard":
            raise error
        return real_import(name, *args, **kwargs)

    monkeypatch.delitem(sys.modules, "zstandard", raising=False)
    monkeypatch.setattr(builtins, "__import__", failing_import)
    monkeypatch.setattr(payloads, "_codecs", threading.local())  # no (de)compressor made before


def test_a_missing_zstandard_says_what_to_install(project, monkeypatch):
    (pid,) = put(project, [Payload(molecule(1))])
    _zstandard_fails(monkeypatch, ModuleNotFoundError("No module named 'zstandard'", name="zstandard"))
    reader = ProjectStore.open_at(project.db_path)
    try:
        with pytest.raises(RuntimeError, match="pip install zstandard$"):
            reader.payloads.get(pid)
    finally:
        reader.dispose()


def test_a_zstandard_that_cannot_load_says_why(project, monkeypatch):
    """Installed but not loadable: the reason is in the message, not "pip install zstandard"."""
    (pid,) = put(project, [Payload(molecule(1))])
    mismatch = ImportError("zstd C API versions mismatch; Python bindings were not compiled/linked against "
                           "expected zstd version (10408 returned by the lib, 10507 hardcoded in zstd headers, "
                           "10506 hardcoded in the cext)")
    _zstandard_fails(monkeypatch, mismatch)
    reader = ProjectStore.open_at(project.db_path)
    try:
        with pytest.raises(RuntimeError, match="cannot load: zstd C API versions mismatch") as caught:
            reader.payloads.get(pid)
    finally:
        reader.dispose()
    assert "another library loaded a different libzstd first" in str(caught.value)
    assert "--force-reinstall --no-deps zstandard" in str(caught.value)
    assert caught.value.__cause__ is mismatch
    with pytest.raises(RuntimeError, match="cannot load: undefined symbol") as caught:
        _zstandard_fails(monkeypatch, ImportError("undefined symbol: ZSTD_foo"))
        from ms_flow.payloads import _compress
        _compress("zstd", b"x", None)
    assert "libzstd" not in str(caught.value)
