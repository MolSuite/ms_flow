"""The container: an Arrow IPC stream of shards plus an index, appended as bytes.

Gate counts: `MSFLOW_CONTAINER_KILLS` (200) and `MSFLOW_CONTAINER_FLIPS` (1500). Another
interpreter with another pyarrow: `MSFLOW_CONTAINER_OTHER_PYTHON`. A 2 GiB shard (5 GB of disk and
RAM): `MSFLOW_CONTAINER_BIG=1`.
"""
from __future__ import annotations

import json
import os
import random
import signal
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from ms_flow.core.data import container as c

KILLS = int(os.environ.get("MSFLOW_CONTAINER_KILLS", 20))
FLIPS = int(os.environ.get("MSFLOW_CONTAINER_FLIPS", 200))
SRC = str(Path(c.__file__).resolve().parents[3])

S = pa.schema([("id", pa.uint64()), ("record", pa.binary()), ("reason", pa.string())],
              metadata={b"a": b"1", b"b": b"2"})


def batch(base, n, size=40, holes=()):
    return pa.RecordBatch.from_arrays([
        pa.array(range(base, base + n), pa.uint64()),
        pa.array([None if i in holes else (b"%d" % (base + i)) * (size // 8) for i in range(n)], pa.binary()),
        pa.array(["no" if i in holes else None for i in range(n)], pa.string())], schema=S)


def shard(base, n=256, size=40, pieces=1, holes=(), part="p", meta=None):
    """`(attrs, data)` as `Appender.commit` takes it."""
    whole = batch(base, n, size, holes)
    cuts = [n * i // pieces for i in range(pieces + 1)]
    batches = [whole.slice(cuts[i], cuts[i + 1] - cuts[i]) for i in range(pieces)] if n else []
    attrs = dict(base_id=base, slot_count=n, n_records=n - len(holes), n_dropped=len(holes), scope=1,
                 origin=base // 256, part=part, meta=meta, present=b"\xff" * (n // 8) if holes else None)
    return c.encode(S, batches, attrs)


def items():
    return [shard(0), shard(256, pieces=3), shard(512, 0), shard(768, holes=(1, 7, 255)),
            shard(1024, 256, 3000, 5, meta=json.dumps({"k": 1}))]


def raises(function, *kinds):
    try:
        function()
    except kinds or Exception:
        return True
    return False


def table(path, rows=None, columns=None):
    info, all_rows = c.read_index(path)
    return c.read_table(path, info, all_rows if rows is None else rows, columns)


@pytest.fixture
def one(tmp_path):
    """Five shards written by one writer."""
    appender = c.Appender.create(tmp_path / "one.arrows", S)
    appender.commit(items())
    return appender


# ---------------------------------------------------------------- the format

def test_raw_append_is_byte_identical_to_one_writer(tmp_path, one):
    parts = []
    for at, item in enumerate(items()):
        part = c.Appender.create(tmp_path / f"p{at}.arrows", S)
        part.commit([item])
        parts.append(part.path)
    merged = c.Appender.create(tmp_path / "m.arrows", S)
    merged.append(parts)
    assert merged.path.read_bytes() == one.path.read_bytes()
    assert c.index_path(merged.path).read_bytes() == c.index_path(one.path).read_bytes()


def test_plain_pyarrow_reads_it_without_an_end_of_stream(one):
    assert ipc.open_stream(one.path.read_bytes()).read_all().num_rows == 1024


def test_a_shard_is_read_by_its_offset_whatever_its_batches(one):
    info, rows = c.read_index(one.path)
    assert [r["n_batches"] for r in rows] == [1, 3, 1, 1, 5]
    assert [c.read_table(one.path, info, [r]).num_rows for r in rows] == [256, 256, 0, 256, 256]


def test_a_segment_equals_the_whole_stream(one):
    info, rows = c.read_index(one.path)
    assert c.read_table(one.path, info, rows).equals(ipc.open_stream(one.path.read_bytes()).read_all())


def test_projection_follows_the_order_asked(one):
    info, rows = c.read_index(one.path)
    assert c.read_table(one.path, info, rows[1:4], ["reason", "id"]).column_names == ["reason", "id"]


def test_drops_are_null_records_with_a_reason(one):
    info, rows = c.read_index(one.path)
    assert c.read_table(one.path, info, [rows[3]], ["reason"]).column(0).null_count == 253
    assert c.read_table(one.path, info, [rows[3]], ["record"]).column(0).null_count == 3


def test_a_child_container_from_raw_ranges(tmp_path, one):
    info, rows = c.read_index(one.path)
    child = c.Appender.create(tmp_path / "child.arrows", S)
    child.commit([(r, c.read_raw(one.path, r)) for r in (rows[1], rows[4])])
    assert table(child.path).equals(c.read_table(one.path, info, [rows[1], rows[4]]))


def test_a_rebuilt_index_is_identical(one):
    saved = c.index_path(one.path).read_bytes()
    c.index_path(one.path).unlink()
    assert c.rebuild_index(one.path) == 5
    assert c.index_path(one.path).read_bytes() == saved


def test_retire_keeps_numbers_and_append_goes_on(one):
    appender = c.Appender(one.path)
    appender.retire([1])
    appender.commit([shard(2000, part="q")])
    rows = c.read_index(one.path)[1]
    assert [r["shard"] for r in rows] == list(range(6))
    assert [r["retired"] for r in rows] == [False, True, False, False, False, False]


def test_retire_rejects_an_unknown_shard(one):
    before = c.index_path(one.path).read_bytes()
    with pytest.raises(ValueError, match="no such shards"):
        c.Appender(one.path).retire([1, 99])
    assert c.index_path(one.path).read_bytes() == before


def test_encode_fills_n_batches_and_commit_needs_it(tmp_path):
    attrs, data = shard(0, pieces=3)
    assert attrs["n_batches"] == 3 and isinstance(data, bytes)
    assert c.encode(S, [], dict(base_id=0, slot_count=0, n_records=0, part="z"))[0]["n_batches"] == 1
    appender = c.Appender.create(tmp_path / "a.arrows", S)
    for lacking in ("n_batches", "n_records", "base_id", "slot_count"):
        with pytest.raises(ValueError, match=lacking):
            appender.commit([({**attrs, lacking: None}, data)])
        with pytest.raises(ValueError, match=lacking):  # a hand-made dict, with no key at all
            appender.commit([({k: v for k, v in attrs.items() if k != lacking}, data)])
    assert appender.rows == [] and os.path.getsize(appender.path) == appender.end
    assert appender.commit([(attrs, data)]) == [0]  # not poisoned by the mistakes


def test_encode_refuses_a_shard_over_the_limit(monkeypatch):
    monkeypatch.setattr(c, "SHARD_LIMIT", 1 << 10)
    attrs = dict(base_id=0, slot_count=1, n_records=1, part="big")
    noise = pa.RecordBatch.from_arrays([pa.array([0], pa.uint64()), pa.array([os.urandom(4096)], pa.binary()),
                                        pa.array([None], pa.string())], schema=S)
    with pytest.raises(ValueError, match="at most"):
        c.encode(S, [noise], attrs)
    assert c.encode(S, [batch(0, 1)], {**attrs, "n_records": 1})  # a small one still goes


def test_iter_tables_gives_contiguous_segments_of_at_most_max_bytes(one):
    info, rows = c.read_index(one.path)
    sizes = [r["length"] for r in rows]
    limit = sizes[0] + sizes[1] + 1
    got = list(c.iter_tables(one.path, info, rows, max_bytes=limit))
    assert len(got) > 1 and pa.concat_tables(got).equals(c.read_table(one.path, info, rows))
    assert all(t.num_rows for t in got[:-1])
    one_each = list(c.iter_tables(one.path, info, rows, max_bytes=1))  # a shard over it goes alone
    assert [t.num_rows for t in one_each] == [256, 256, 0, 256, 256]
    assert list(c.iter_tables(one.path, info, [])) == []
    assert c.read_table(one.path, info, rows[::-1]).column("id")[0].as_py() == 1024  # order as given


def test_read_table_reads_through_iter_tables(one, monkeypatch):
    calls = []
    real = c.iter_tables
    monkeypatch.setattr(c, "iter_tables", lambda *a, **k: calls.append(1) or real(*a, **k))
    info, rows = c.read_index(one.path)
    assert c.read_table(one.path, info, rows).num_rows == 1024 and calls


# ---------------------------------------------------------------- damage

def test_arrow_alone_reads_a_cut_at_a_boundary_as_complete(one):
    rows = c.read_index(one.path)[1]
    assert ipc.open_stream(one.path.read_bytes()[:rows[4]["offset"]]).read_all().num_rows == 768


def test_every_cut_of_committed_data_is_refused(tmp_path, one):
    raw, (info, rows) = one.path.read_bytes(), c.read_index(one.path)
    boundaries = [r["offset"] for r in rows] + [rows[-1]["offset"] + rows[-1]["length"] - 1]
    for cut in boundaries + [info["head_length"] + 5]:
        short = tmp_path / "short.arrows"
        short.write_bytes(raw[:cut])
        c.index_path(short).write_bytes(c.index_path(one.path).read_bytes())
        assert raises(lambda: c.Appender(short), c.Damaged)
        assert short.stat().st_size == cut  # nothing was cut or repaired
        assert raises(lambda: c.read_raw(short, rows[-1]), c.Damaged)


def test_no_bit_flip_is_silent(tmp_path, one):
    raw, (info, rows) = one.path.read_bytes(), c.read_index(one.path)
    rng, silent = random.Random(7), []
    flipped = tmp_path / "flipped.arrows"
    for i in range(FLIPS):
        data = bytearray(raw)
        at = rng.randrange(len(data)) if i % 3 else rng.randrange(info["head_length"])  # a third in the schema
        data[at] ^= 1 << rng.randrange(8)
        flipped.write_bytes(data)
        try:
            c.read_table(flipped, info, rows)
            silent.append(at)
        except c.Damaged:
            pass
    assert not silent


def test_a_damaged_index_is_refused_at_every_byte(tmp_path, one):
    index = c.index_path(one.path).read_bytes()
    bent = tmp_path / "bent.arrows"
    silent = 0
    for cut in range(len(index)):
        for data in (index[:cut], index[:cut] + bytes([index[cut] ^ 16]) + index[cut + 1:]):
            c.index_path(bent).write_bytes(data)
            silent += not raises(lambda: c.read_index(bent), c.Damaged)
    assert silent == 0


def test_only_the_unnamed_tail_is_truncated(one):
    end = one.end
    for n in range(1, 400):
        with open(one.path, "ab") as file:
            file.write(os.urandom(n) if n % 2 else b"\0" * n)
        appender = c.Appender(one.path)
        assert os.path.getsize(one.path) == end and appender.end == end
    assert table(one.path).num_rows == 1024


def test_another_schema_metadata_is_refused(tmp_path, one):
    other = c.Appender.create(tmp_path / "o.arrows", S.with_metadata({b"a": b"1", b"b": b"3"}))
    before = one.path.read_bytes()
    with pytest.raises(ValueError, match="another schema"):
        one.append([other.path])
    assert one.path.read_bytes() == before


def test_dictionary_columns_are_refused_at_any_depth():
    d = pa.dictionary(pa.int32(), pa.string())
    nests = [d, pa.list_(d), pa.large_list(d), pa.list_(d, 2), pa.map_(pa.string(), d),
             pa.struct([("a", pa.struct([("b", d)]))])]
    for kind in nests:
        with pytest.raises(ValueError, match="dictionary"):
            c.check_schema(pa.schema([("x", kind)]))
    c.check_schema(pa.schema([("x", pa.list_(pa.struct([("a", pa.string())])))]))


def test_nested_null_sliced_and_empty_batches_round_trip(tmp_path):
    n = pa.schema([("l", pa.list_(pa.uint64())), ("m", pa.map_(pa.string(), pa.int32())),
                   ("s", pa.struct([("a", pa.string())])), ("n", pa.null())])
    whole = pa.RecordBatch.from_arrays([
        pa.array([[1], None, [2, 3]], n[0].type), pa.array([[("a", 1)], [], None], n[1].type),
        pa.array([{"a": "x"}, None, {"a": None}], n[2].type), pa.nulls(3)], schema=n)
    appender = c.Appender.create(tmp_path / "n.arrows", n)
    for piece in (whole, whole.slice(1), whole.slice(1, 0)):
        attrs = dict(base_id=0, slot_count=0, n_records=piece.num_rows, part="t")
        appender.commit([c.encode(n, [piece], attrs)])
    assert table(appender.path).num_rows == 5


def test_a_failed_sync_poisons_until_reopened(tmp_path, monkeypatch):
    appender = c.Appender.create(tmp_path / "fail.arrows", S)
    appender.commit([shard(0)])

    def boom(file):
        raise OSError(5, "fsync")

    with monkeypatch.context() as patch:
        patch.setattr(c, "_sync", boom)
        with pytest.raises(OSError):
            appender.commit([shard(256)])
    with pytest.raises(RuntimeError, match="reopen"):
        appender.commit([shard(256)])
    with pytest.raises(RuntimeError, match="reopen"):
        appender.retire([0])
    assert len(c.Appender(appender.path).rows) == 1


def test_a_bad_part_does_not_poison_the_container(tmp_path):
    target = c.Appender.create(tmp_path / "w.arrows", S)
    good = c.Appender.create(tmp_path / "g.arrows", S)
    good.commit([shard(0)])
    bad = c.Appender.create(tmp_path / "b.arrows", S)
    bad.commit([shard(256)])
    damaged = bytearray(bad.path.read_bytes())
    damaged[-20] ^= 1
    bad.path.write_bytes(damaged)
    with pytest.raises(c.Damaged):
        target.append([good.path, bad.path])
    assert os.path.getsize(target.path) == target.end and not c.read_index(target.path)[1]
    assert target.append([good.path]) == [0]


def test_append_refuses_at_the_end_a_shard_whose_checksum_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(c, "BLOCK", 256)  # many blocks: the failure is only known after the last
    part = c.Appender.create(tmp_path / "part.arrows", S)
    part.commit([shard(0, size=400)])
    row = part.rows[0]
    assert row["length"] > 4 * 256
    damaged = bytearray(part.path.read_bytes())
    damaged[row["offset"] + 300] ^= 1  # in the first blocks
    part.path.write_bytes(damaged)
    target = c.Appender.create(tmp_path / "t.arrows", S)
    with pytest.raises(c.Damaged, match="checksum"):
        target.append([part.path])
    assert os.path.getsize(target.path) == target.end and not c.read_index(target.path)[1]
    clean = c.Appender.create(tmp_path / "clean.arrows", S)
    clean.commit([shard(0, size=400)])
    assert target.append([clean.path]) == [0]  # many small blocks, same bytes
    assert target.path.read_bytes() == clean.path.read_bytes()


def test_no_shards_reads_as_an_empty_table(tmp_path):
    empty = c.Appender.create(tmp_path / "e.arrows", S)
    info = c.read_index(empty.path)[0]
    assert c.read_table(empty.path, info, []).num_rows == 0
    assert c.read_table(empty.path, info, [], ["id"]).column_names == ["id"]


def test_move_publishes_under_its_name(tmp_path):
    tmp = c.Appender.create(tmp_path / "pub.tmp", S)
    tmp.commit([shard(512)])
    c.move(tmp.path, tmp_path / "pub.arrows")
    assert table(tmp_path / "pub.arrows").num_rows == 256
    assert not tmp.path.exists() and not c.index_path(tmp.path).exists()


def test_a_rebuild_over_damage_writes_nothing(tmp_path, one):
    rows = c.read_index(one.path)[1]
    data = bytearray(one.path.read_bytes())
    data[rows[1]["offset"] + 9] ^= 255
    hurt = tmp_path / "hurt.arrows"
    hurt.write_bytes(data)
    with pytest.raises(c.Damaged):
        c.rebuild_index(hurt)
    assert not c.index_path(hurt).exists() and hurt.read_bytes() == bytes(data)


def test_a_rebuild_cuts_a_tail_only_when_told(tmp_path, one):
    tail = tmp_path / "tail.arrows"
    tail.write_bytes(one.path.read_bytes() + b"\0" * 13)
    with pytest.raises(c.Damaged):
        c.rebuild_index(tail)
    assert c.rebuild_index(tail, cut_tail=True) == 5
    assert c.Appender(tail).end == one.end


# ---------------------------------------------------------------- crashes

def _child(path):
    appender = c.Appender(path)
    while True:
        k = len(appender.rows)
        if k >= 4000:
            break
        appender.commit([shard((k + i) * 256, 256, part=str(k + i)) for i in range(3)])
        print(len(appender.rows), flush=True)


@pytest.fixture(scope="module")
def killed(tmp_path_factory):
    path = tmp_path_factory.mktemp("kills") / "k.arrows"
    c.Appender.create(path, S)
    rng, acked, bad = random.Random(7), 0, []
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([SRC, os.environ.get("PYTHONPATH", "")])}
    for i in range(KILLS):
        process = subprocess.Popen([sys.executable, __file__, "child", str(path)], stdout=subprocess.PIPE,
                                   text=True, env=env)
        for _ in range(rng.randint(1, 5)):
            line = process.stdout.readline()
            if not line:
                break
            acked = int(line)
        time.sleep(rng.random() * 0.01)
        process.send_signal(signal.SIGKILL)
        rest = process.communicate()[0].split()
        acked = max(acked, int(rest[-1])) if rest else acked
        rows = c.read_index(path)[1]
        if len(rows) < acked or len(rows) % 3 or len({r["part"] for r in rows}) != len(rows):
            bad.append((i, len(rows), acked))
        if rows and raises(lambda: c.read_raw(path, rows[-1]), c.Damaged):
            bad.append((i, "last shard unreadable"))
    return path, bad


def test_a_killed_writer_loses_no_committed_shard(killed):
    path, bad = killed
    assert not bad
    info, rows = c.read_index(path)
    result = c.read_table(path, info, rows)
    assert result.num_rows == 256 * len(rows) and result["id"][-1].as_py() == 256 * len(rows) - 1


def test_rebuild_after_the_kills_is_identical(killed):
    path, _ = killed
    saved = c.index_path(path).read_bytes()
    c.Appender(path)
    c.rebuild_index(path)
    assert c.index_path(path).read_bytes() == saved


# ---------------------------------------------------------------- readers, versions, memory

def test_readers_during_commits_see_whole_shards(tmp_path):
    appender = c.Appender.create(tmp_path / "live.arrows", S)
    stop, bad = threading.Event(), []

    def read():
        while not stop.is_set():
            try:
                info, rows = c.read_index(appender.path)
                if rows:
                    c.read_table(appender.path, info, rows[-2:])
                if len(rows) % 2:
                    bad.append(len(rows))
            except Exception as error:
                bad.append(repr(error))

    threads = [threading.Thread(target=read) for _ in range(4)]
    for thread in threads:
        thread.start()
    for k in range(300):
        appender.commit([shard(k * 512, part=f"{k}a"), shard(k * 512 + 256, part=f"{k}b")])
    stop.set()
    for thread in threads:
        thread.join()
    assert not bad[:3]


_OTHER = textwrap.dedent('''
    import importlib.util, sys
    from pathlib import Path
    import pyarrow as pa
    spec = importlib.util.spec_from_file_location("container", sys.argv[1])
    c = importlib.util.module_from_spec(spec); sys.modules["container"] = c; spec.loader.exec_module(c)
    mode, folder = sys.argv[2], Path(sys.argv[3])
    S = pa.schema([("id", pa.uint64()), ("record", pa.binary())], metadata={b"x": b"1", b"y": b"2"})
    def mk(base, n, rec):
        b = pa.RecordBatch.from_arrays([pa.array(range(base, base + n), pa.uint64()), pa.array([rec] * n, pa.binary())], schema=S)
        return c.encode(S, [b], dict(base_id=base, slot_count=n, n_records=n, part=f"{pa.__version__}:{base}"))
    if mode == "write":
        a = c.Appender.create(folder / f"v{pa.__version__}.arrows", S)
        a.commit([mk(i * 256, 256, b"abc" * 20) for i in range(8)])
        print("wrote", pa.__version__)
    else:
        for p in sorted(folder.glob("v*.arrows")):
            i, r = c.read_index(p); assert c.read_table(p, i, r).num_rows == 2048
            a = c.Appender.create(folder / f"x{pa.__version__}_{p.name}", S); a.append([p]); a.commit([mk(9999, 256, b"q")])
            assert c.read_table(a.path, *c.read_index(a.path)).num_rows == 2304
            import shutil
            q = folder / f"y{pa.__version__}_{p.name}"
            shutil.copy(p, q); shutil.copy(c.index_path(p), c.index_path(q))
            b = c.Appender(q); b.commit([mk(5000, 256, b"r")])  # the other's container, grown here
            assert c.read_table(q, *c.read_index(q)).num_rows == 2304
            print("PASS", pa.__version__, "reads and appends", p.name)
''')


def test_each_pyarrow_reads_and_appends_the_others_files(tmp_path):
    pythons = [sys.executable] + [p for p in [os.environ.get("MSFLOW_CONTAINER_OTHER_PYTHON")] if p]
    script = tmp_path / "other.py"
    script.write_text(_OTHER)
    folder = tmp_path / "shared"
    folder.mkdir()

    def run(python, mode):
        done = subprocess.run([python, "-I", str(script), c.__file__, mode, str(folder)], capture_output=True,
                              text=True)
        assert done.returncode == 0, done.stderr[-2000:]
        return done.stdout

    for python in pythons:
        run(python, "write")
    for python in pythons:
        assert run(python, "cross").count("PASS") == len(pythons)


_PEAK = textwrap.dedent('''
    import sys
    from pathlib import Path
    sys.path.insert(0, sys.argv[1])
    import pyarrow as pa
    from ms_flow.core.data import container as c
    import resource
    def peak():  # MiB: VmHWM; kB on Linux, bytes on macOS
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (2 ** 20 if sys.platform == "darwin" else 1024)
    path = Path(sys.argv[3])
    if sys.argv[2] == "append":
        target = c.Appender(path)
        parts = [Path(p) for p in sys.argv[4:]]
        before = peak(); target.append(parts)
    else:
        info, rows = c.read_index(path)
        before = peak(); n = 0
        for t in c.iter_tables(path, info, rows, max_bytes=int(sys.argv[4])):
            n += t.num_rows; del t
        assert n == int(sys.argv[5]), n
    print(peak() - before)
''')


def _noise_container(path, shards, per_shard, batch_bytes=8 << 20):
    """Incompressible shards: what is stored is as big as what is read."""
    schema = pa.schema([("id", pa.uint64()), ("record", pa.binary())])
    appender = c.Appender.create(path, schema)
    for s in range(shards):
        batches = [pa.RecordBatch.from_arrays([pa.array([s * 1000 + b], pa.uint64()),
                                               pa.array([os.urandom(batch_bytes)], pa.binary())], schema=schema)
                   for b in range(per_shard // batch_bytes)]
        attrs = dict(base_id=s * 1000, slot_count=1000, n_records=len(batches), part=f"s{s}")
        appender.commit([c.encode(schema, batches, attrs)])
    return appender


def _measure(tmp_path, *args):
    script = tmp_path / "peak.py"
    script.write_text(_PEAK)
    done = subprocess.run([sys.executable, str(script), SRC, *map(str, args)], capture_output=True, text=True)
    assert done.returncode == 0, done.stderr[-2000:]
    return float(done.stdout.split()[-1])


def test_append_rss_is_bounded_by_the_block(tmp_path):
    part = _noise_container(tmp_path / "part.arrows", 1, 400 << 20)
    assert part.rows[0]["length"] > 390 << 20
    target = c.Appender.create(tmp_path / "target.arrows", pa.schema([("id", pa.uint64()), ("record", pa.binary())]))
    grown = _measure(tmp_path, "append", target.path, part.path)
    assert grown < 64, f"the parent grew {grown:.0f} MiB appending a 400 MiB shard"
    rows = c.read_index(target.path)[1]
    assert len(rows) == 1 and rows[0]["length"] == part.rows[0]["length"] and rows[0]["crc"] == part.rows[0]["crc"]


def test_batch_read_rss_is_bounded_by_the_segment(tmp_path):
    big = _noise_container(tmp_path / "big.arrows", 16, 24 << 20)  # 384 MiB stored
    segment = 48 << 20  # two shards
    grown = _measure(tmp_path, "read", big.path, segment, 48)
    assert grown < 4 * (segment >> 20), f"{grown:.0f} MiB for segments of {segment >> 20} MiB over 384 MiB"


@pytest.mark.skipif(not os.environ.get("MSFLOW_CONTAINER_BIG"), reason="needs 5 GB of disk and RAM")
def test_a_shard_over_2_gib_and_an_end_over_4_gib_are_read(tmp_path):
    schema = pa.schema([("id", pa.uint64()), ("record", pa.binary())])
    one = pa.RecordBatch.from_arrays([pa.array([0], pa.uint64()), pa.array([os.urandom(16 << 20)], pa.binary())],
                                     schema=schema)
    out = pa.BufferOutputStream()
    with ipc.new_stream(out, schema, options=c._write_options()) as writer:
        writer.write_batch(one)
    piece = out.getvalue().to_pybytes()[schema.serialize().size:-8]
    attrs = dict(base_id=0, slot_count=140, n_records=140, part="b", n_batches=140)
    data = piece * 140  # 2.2 GiB: past the encode limit on purpose, this is the reader's test
    appender = c.Appender.create(tmp_path / "big.arrows", schema)
    appender.commit([(attrs, data)])
    appender.commit([({**attrs, "part": "c"}, data)])
    del data
    rows = c.read_index(appender.path)[1]
    assert rows[1]["offset"] + rows[1]["length"] > 1 << 32
    assert len(c.read_raw(appender.path, rows[1])) == rows[1]["length"]


if __name__ == "__main__" and sys.argv[1:2] == ["child"]:
    _child(sys.argv[2])
