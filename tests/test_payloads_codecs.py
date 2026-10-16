"""Codecs of payloads: each needle says its own, compression happens in the worker, zstd dictionaries per series (V5)."""
import pickle
import random
import sqlite3
from contextlib import closing

import pytest

from ms_flow.core.database.project import ProjectStore
from ms_flow.payloads import Payload


def molecule(n: int) -> bytes:
    """An SDF-like record: the shape repeats, the numbers do not."""
    rng = random.Random(n)
    atoms = "".join(f"{rng.uniform(-9, 9):10.4f}{rng.uniform(-9, 9):10.4f}{rng.uniform(-9, 9):10.4f} "
                    f"{rng.choice('CCCNOS')}   0  0  0  0  0  0  0  0  0  0  0  0\n" for _ in range(rng.randint(15, 40)))
    return (f"MOL{n:07d}\n     RDKit          3D\n\n" + atoms + "M  END\n> <score>\n"
            f"{rng.uniform(-12, -3):.2f}\n\n$$$$\n").encode()


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
    data = [molecule(n) for n in range(4)]
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
    item = Payload(molecule(1) * 20, kind="sdf", series="ligands")
    blob = pickle.dumps(item)
    assert len(blob) < len(item.data) / 4  # what crosses the process is compressed
    arrived = pickle.loads(blob)
    assert arrived.raw_length == len(item.data) and arrived.packed() is arrived

    from ms_flow import payloads
    monkeypatch.setattr(payloads, "_compress", lambda *a: pytest.fail("the writer compressed again"))
    (pid,) = put(project, [arrived])
    assert project.payloads.get(pid) == item.data
