"""Where the bytes of each molecule should live: SQLite blobs or append-only volumes (V0 of docs/08-plan-volumenes.md).

    python -m benchmarks.refactor_optimization.bench_volumes --mode serial   --n 700000 --label baseline --data-dir /mnt/x
    python -m benchmarks.refactor_optimization.bench_volumes --mode parallel --n 700000 --label baseline --data-dir /mnt/x
    python -m benchmarks.refactor_optimization.bench_volumes --mode serial --quick --label try   # 3000 molecules, checks the bytes

Each run adds its variants to `results/volumes_<label>.json` under `serial/<variant>` or `parallel/<variant>`, so a
serial and a parallel run of the same label end up in one file. Each variant is measured on its own copy of the data,
deleted when it ends unless `--keep`. A variant that does not fit in the free disk (plus 1.5 GB) is refused.

Payloads are synthetic with the sizes of AMDockVS: original 2 KB, prepared 3 KB, PDBQT 2.5 KB and a 20 KB pose
for 20 % of the molecules, about 13 KB per molecule. The index is one SQLite row per molecule; the variants differ in
where the payloads go. Numbers are bounded by the disk, so compare variants on the same disk.

serial: one writer, 1000 molecules per commit (the `bench2.py` of 2026-10-15)
  HT  volumes, no fsync: the same durability as RT
  H   volumes, fsync of the volume before each commit
  RT  SQLite blobs (Ringtail-like), tuned: synchronous=OFF
  PS  ms_flow.payloads.PayloadStore in a real project.db: put + molecule rows + commit per 1000 molecules,
      fdatasync of the touched volumes per commit (D3). Import only (V1-9); its reads are V3
  PSN PS with the fdatasync skipped: PS - PSN is what the group fsync costs
parallel: `--workers` loky processes produce the payloads and one parent writes (the `bench3.py` of 2026-10-15)
  VP  volumes, the parent appends the bytes that come back with each result
  VC  volumes, each worker writes one segment file per chunk and the parent keeps the references
  RT  SQLite blobs, the parent inserts
  PJ  a real ms_flow job: `Flow` on a `ProcessExecutor`, the task returns rows with `Payload`s and the `table_sink`
      stores them through PayloadStore, group fsync included (V2-12). Its reads are cold only, with the V3 readers:
      `stream_all_current_s_cold` through `with_payloads` over `Rows` (a job's source), `export_all_current_s_cold`
      through `iter_bytes` (an export)

  PJZ PJ with zstd payloads, compressed in the workers (V5)
  PJD PJ with zstd and a dictionary per series, trained first on `--sample`'s train.pkl (V5)
  PM  PJZ in volumes of 64 MB, then, cold: check(full), the app deletes half the molecules,
      release, and compact (V4-10)

`--sample DIR` replaces the synthetic payloads with real ones: DIR/bench.pkl is `{"sdf": [...], "pdbqt": [...],
"pose": [...]}` (original and current are SDF), cycled over the molecules; DIR/train.pkl, the same shape and
disjoint, trains PJD's dictionaries. Without it the payloads are random hex and compress about 1.8x.

`durable_s` is `import_s` plus the `sync` right after it: the time until the import is on the disk. Variants that never
sync (HT, PSN) otherwise finish in the page cache whenever the data fits in RAM.

The read metrics are measured twice. The plain ones are warm, as on 2026-10-15: the files were just written, so SQLite
and the volumes sit in the page cache. The `_cold` ones come after, each preceded by `fsync` + `posix_fadvise(DONTNEED)`
on every file of the variant (no sudo) and run on a new reader; pool and loky workers start after the eviction.
`stream_all_current_cold_disk_mb` and `parallel_<w>x20k_random_cold_disk_mb` (the 8x10k one in serial mode) are the
MB the device read meanwhile, from /proc/diskstats: the proof that the cache was cold (null when the device is not
listed, e.g. tmpfs or overlay).

`--quick` verifies, for every variant, that 200 random molecules read back byte-identical to what generation produced.
Its numbers are not comparable with a full run.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import socket
import sqlite3
import subprocess
import time
from datetime import datetime, timezone
from multiprocessing import get_context
from pathlib import Path

from loky import get_reusable_executor

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parents[1]

KINDS = {"original": (1, 2000), "current": (2, 3000), "pdbqt": (3, 2500), "pose": (4, 20000)}
SHARD = 1000  # molecules per commit, serial
CHUNK = 1000  # molecules per task, parallel
VOL_MAX = 256 << 20
FLUSH_EVERY = 16  # results per index commit in the parallel import, as ms_flow output_flush_every
DOCKED_FRACTION = 0.2
SPARE = 1_500_000_000  # bytes that must stay free after a variant
VARIANTS = {"serial": ("HT", "H", "RT", "PS", "PSN"), "parallel": ("VP", "VC", "RT", "PJ", "PJZ", "PJD", "PM")}
JOB_CODECS = {"PJ": "none", "PJZ": "zstd", "PJD": "zstd-dict", "PM": "zstd"}
PM_VOLUME_MB = 64  # so that 100k molecules fill sealed volumes, the only ones compact touches
SAMPLE_KIND = {"original": "sdf", "current": "sdf", "pdbqt": "pdbqt", "pose": "pose"}
# The payloads come from this pool; seeded, so every process (a loky worker, a test) produces the same bytes.
_POOL = random.Random(0).randbytes(32768).hex().encode()


_SAMPLE = {}  # --sample: real payloads by kind, in this process


def use_sample(directory):
    if directory and _SAMPLE.get("dir") != str(directory):
        import pickle

        with open(Path(directory) / "bench.pkl", "rb") as f:
            _SAMPLE.update(pickle.load(f), dir=str(directory))


def payload(i, kind):
    if _SAMPLE:
        records = _SAMPLE[SAMPLE_KIND[kind]]
        return f"{i}:{kind}\n".encode() + records[(i * 7 + KINDS[kind][0]) % len(records)]
    size = KINDS[kind][1] + (i * 7919) % 800
    off = (i * 104729) % (len(_POOL) - size)
    return f"{i}:{kind}\n".encode() + _POOL[off:off + size]


def key(i, kind):
    return i * 16 + KINDS[kind][0]


def docked_hash(i):  # parallel: 20 % of the ids, the same answer in every process
    return (i * 2654435761) % 100 < 20


def kinds_for(i, is_docked):
    return ["original", "current", "pdbqt"] + (["pose"] if is_docked(i) else [])


def vol_path(root, vol):
    return Path(root) / "volumes" / f"{vol:09d}.vol"


# --- Writers ------------------------------------------------------------------------

class VolumeWriter:
    """Serial: haystack-style append-only volumes; rolls over at VOL_MAX."""

    def __init__(self, root, sync, start=None):
        self.root, self.sync = Path(root), sync
        (self.root / "volumes").mkdir(exist_ok=True)
        existing = sorted(int(p.stem) for p in (self.root / "volumes").glob("*.vol"))
        self.vol = start if start is not None else (existing[-1] + 1 if existing else 0)
        self.fh = open(vol_path(root, self.vol), "ab")

    def append(self, data):
        if self.fh.tell() + len(data) > VOL_MAX:
            self.fh.close()
            self.vol += 1
            self.fh = open(vol_path(self.root, self.vol), "ab")
        off = self.fh.tell()
        self.fh.write(data)
        return self.vol, off, len(data)

    def flush(self):  # bytes durable before the index row that points at them commits
        self.fh.flush()
        if self.sync:  # H only; HT skips it, the durability of RT (synchronous=OFF)
            os.fsync(self.fh.fileno())


class Volume:
    """Parallel: one volume file, written by one process."""

    def __init__(self, path):
        self.fh = open(path, "xb")  # O_EXCL: never shared by two writers

    def append(self, data):
        off = self.fh.tell()
        self.fh.write(data)
        return off, len(data)


def connect(path, incremental=False):
    con = sqlite3.connect(path)
    if incremental:  # must precede anything that initializes the file
        con.execute("PRAGMA auto_vacuum=INCREMENTAL")
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    return con


def import_serial(variant, root, n, docked):
    con = connect(root / "project.db", incremental=variant == "RT")
    if variant in ("HT", "RT"):
        con.execute("PRAGMA synchronous=OFF")
        con.execute("PRAGMA cache_size=-262144")
    con.execute("CREATE TABLE molecules(id INTEGER PRIMARY KEY, mw REAL, logp REAL, score REAL,"
                " original_path TEXT, current_path TEXT, pdbqt_path TEXT, pose_path TEXT)")
    con.execute("CREATE INDEX ix_score ON molecules(score)")
    if variant == "RT":  # key = mol_id*16 + kind -> one rowid lookup
        con.execute("CREATE TABLE blobs(key INTEGER PRIMARY KEY, data BLOB)")
    else:
        con.execute("CREATE TABLE refs(key INTEGER PRIMARY KEY, vol INTEGER, off INTEGER, len INTEGER)")
        con.execute("CREATE INDEX ix_refs_vol ON refs(vol, off)")
    is_docked = docked.__contains__
    vol = None if variant == "RT" else VolumeWriter(root, sync=variant == "H")
    rng = random.Random(1)
    t = time.perf_counter()
    for start in range(0, n, SHARD):
        ids = range(start, min(n, start + SHARD))
        rows = [(i, rng.uniform(150, 700), rng.uniform(-2, 7), rng.uniform(-12, -3), None, None, None, None) for i in ids]
        con.executemany("INSERT INTO molecules VALUES (?,?,?,?,?,?,?,?)", rows)
        if variant == "RT":
            con.executemany("INSERT INTO blobs VALUES (?,?)",
                            [(key(i, k), payload(i, k)) for i in ids for k in kinds_for(i, is_docked)])
        else:
            con.executemany("INSERT INTO refs VALUES (?,?,?,?)",
                            [(key(i, k), *vol.append(payload(i, k))) for i in ids for k in kinds_for(i, is_docked)])
            vol.flush()
        con.commit()
    con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    con.close()
    elapsed = time.perf_counter() - t
    if vol:
        vol.fh.close()
    return elapsed


def produce(start, stop, mode, root):
    """Parallel, in a worker: the rows and payloads of one chunk; with VC, the segment is written here."""
    rng = random.Random(start)
    rows, out = [], []
    if mode == "VC":
        vol_id = start // CHUNK
        seg = Volume(vol_path(root, vol_id))
    for i in range(start, stop):
        rows.append((i, rng.uniform(150, 700), rng.uniform(-2, 7), rng.uniform(-12, -3) if docked_hash(i) else None))
        for k in kinds_for(i, docked_hash):
            data = payload(i, k)
            if mode == "VC":
                out.append((key(i, k), vol_id, *seg.append(data)))
            else:
                out.append((key(i, k), data))
    if mode == "VC":
        seg.fh.close()
    return rows, out


def _noop():
    pass


class ParentWriter:
    """Parallel, in the parent: the rows, and the bytes of RT and VP; VC only gets references."""

    def __init__(self, variant, root):
        self.v, self.root = variant, root
        self.con = sqlite3.connect(root / "project.db")
        self.con.execute("PRAGMA journal_mode=WAL")
        self.con.execute("PRAGMA synchronous=OFF")
        self.con.execute("PRAGMA cache_size=-262144")
        self.con.execute("CREATE TABLE molecules(id INTEGER PRIMARY KEY, mw REAL, logp REAL, score REAL)")
        self.con.execute("CREATE INDEX ix_score ON molecules(score)")
        if variant == "RT":
            self.con.execute("CREATE TABLE blobs(key INTEGER PRIMARY KEY, data BLOB)")
        else:
            self.con.execute("CREATE TABLE refs(key INTEGER PRIMARY KEY, vol INTEGER, off INTEGER, len INTEGER)")
        if variant == "VP":
            self.vol_id, self.vol = 0, Volume(vol_path(root, 0))
        self.pending = 0

    def write(self, rows, out):
        c = self.con
        c.executemany("INSERT INTO molecules VALUES (?,?,?,?)", rows)
        if self.v == "RT":
            c.executemany("INSERT INTO blobs VALUES (?,?)", out)
        elif self.v == "VP":
            refs = []
            for k, d in out:
                if self.vol.fh.tell() + len(d) > VOL_MAX:
                    self.vol.fh.close()
                    self.vol_id += 1
                    self.vol = Volume(vol_path(self.root, self.vol_id))
                refs.append((k, self.vol_id, *self.vol.append(d)))
            self.vol.fh.flush()
            c.executemany("INSERT INTO refs VALUES (?,?,?,?)", refs)
        else:  # VC: the workers wrote the bytes, only refs come back
            c.executemany("INSERT INTO refs VALUES (?,?,?,?)", out)
        self.pending += 1
        if self.pending >= FLUSH_EVERY:
            c.commit()
            self.pending = 0

    def close(self):
        if self.v == "VP":
            self.vol.fh.close()
        self.con.commit()
        self.con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.con.close()


def import_parallel(variant, root, n, workers):
    ex = get_reusable_executor(max_workers=workers, timeout=30)
    try:
        ex.submit(_noop).result()  # workers up before the clock starts
        w = ParentWriter(variant, root)
        try:
            chunks = [(s, min(n, s + CHUNK)) for s in range(0, n, CHUNK)]
            t = time.perf_counter()
            inflight, it = [], iter(chunks)
            for s, e in it:  # refill window of 2x workers, as the engine reads ahead
                inflight.append(ex.submit(produce, s, e, variant, str(root)))
                if len(inflight) >= 2 * workers:
                    break
            while inflight:
                fut = inflight.pop(0)
                w.write(*fut.result())
                nxt = next(it, None)
                if nxt:
                    inflight.append(ex.submit(produce, *nxt, variant, str(root)))
            w.close()
        except BaseException:
            w.con.close()
            raise
        return time.perf_counter() - t
    finally:
        ex.shutdown(wait=True, kill_workers=True)


# --- Reader -------------------------------------------------------------------------

def store_project(root):
    """PS/PSN/PJ: a real project.db whose `molecules` table references `ms_payloads`, as an app declares it."""
    from ms_flow.core.database import ProjectStore

    project = ProjectStore.open_at(root / "project.db")
    with project.engine.begin() as c:
        c.exec_driver_sql("CREATE TABLE molecules(id INTEGER PRIMARY KEY, mw REAL, logp REAL, score REAL, "
                          + ", ".join(f"{k}_payload_id INTEGER REFERENCES ms_payloads(id)" for k in KINDS) + ")")
        c.exec_driver_sql("CREATE INDEX ix_score ON molecules(score)")
        for k in KINDS:
            c.exec_driver_sql(f"CREATE INDEX ix_{k} ON molecules({k}_payload_id)")
    return project


def verify_store(project, store, n, is_docked):
    """--quick: 200 random molecules have exactly the payloads they should, byte-identical."""
    mols = random.Random(4).sample(range(n), min(200, n))
    with project.engine.connect() as c:
        refs = {r[0]: r[1:] for r in c.exec_driver_sql("SELECT id, " + ", ".join(f"{k}_payload_id" for k in KINDS)
                                                         + " FROM molecules")}
    assert len(refs) == n
    for i in mols:
        for k, pid in zip(KINDS, refs[i]):
            assert (pid is not None) == (k in kinds_for(i, is_docked))
            assert pid is None or store.get(pid) == payload(i, k), (i, k)
    return len(mols)


def store_rows(start, stop, config=None):
    """PJ, in a worker: the rows of one chunk, each with its `Payload`s, as an AMDockVS task would return them.
    `config`: the sample directory, the codec and the dictionary of each series (PJZ, PJD)."""
    from ms_flow.payloads import Payload

    config = config or {}
    use_sample(config.get("sample"))
    codec = "none" if config.get("codec", "none") == "none" else "zstd"
    dicts = config.get("dicts") or {}
    rng = random.Random(start)
    rows = []
    for i in range(start, stop):
        row = {"id": i, "mw": rng.uniform(150, 700), "logp": rng.uniform(-2, 7),
               "score": rng.uniform(-12, -3) if docked_hash(i) else None}
        for k in KINDS:
            row[f"{k}_payload_id"] = None
        for k in kinds_for(i, docked_hash):
            series = "poses" if k == "pose" else "ligands"
            row[f"{k}_payload_id"] = Payload(payload(i, k), kind=k, series=series, codec=codec, dictionary=dicts.get(series))
        rows.append(row)
    return rows


def read_all_job(root, n, dev):
    """PJ, cold: every current payload as a job's source reads it, then as an export does (V3)."""
    from ms_flow.core.database import ProjectStore
    from ms_flow.payloads import with_payloads
    from ms_flow.query import QuerySpec, Rows

    def run(read):
        project = ProjectStore.open_at(root / "project.db")
        try:
            seconds, count = timed(lambda: read(project))
        finally:
            project.dispose()
        assert count == n, (count, n)
        return seconds

    def source(project):
        rows = Rows(project, QuerySpec(table="molecules"))
        return sum(1 for row in with_payloads(project, rows, {"current_payload_id": "current"}) if row["current"])

    def export(project):
        with project.engine.connect() as c:
            ids = [r[0] for r in c.exec_driver_sql("SELECT current_payload_id FROM molecules")]
        return sum(1 for _, data in project.payloads.iter_bytes(ids) if data)

    res = {}
    for name, read in (("stream_all_current", source), ("export_all_current", export)):
        res[name + "_s_cold"], res[name + "_cold_disk_mb"] = cold(root, "PJ", dev, lambda _: run(read), fresh=False)
    return res


def maintain_job(root, dev):
    """PM, cold: check(full), then the app deletes every other molecule, releases their payloads and
    compacts. MB/s of compact is of live needles read (and written as they are: no dictionary)."""
    from ms_flow.core.database import ProjectStore
    from ms_flow.payloads import PayloadStore

    def run(fn):
        project = ProjectStore.open_at(root / "project.db")
        store = PayloadStore(project)
        try:
            return timed(lambda: fn(project, store))
        finally:
            store.close()
            project.dispose()

    def delete_half(project, store):
        with project.engine.begin() as c:
            gone = c.exec_driver_sql("DELETE FROM molecules WHERE id % 2 = 0 RETURNING "
                                     + ", ".join(f"{k}_payload_id" for k in KINDS)).fetchall()
        return store.release(pid for row in gone for pid in row if pid is not None)

    res = {"volume_mb": PM_VOLUME_MB}
    (res["check_full_s_cold"], report), res["check_full_cold_disk_mb"] = cold(
        root, "PJ", dev, lambda _: run(lambda project, store: store.check(full=True)), fresh=False)
    assert report.clean, report
    res["release_s"], res["released"] = run(delete_half)
    (res["compact_s_cold"], stats), res["compact_cold_disk_mb"] = cold(
        root, "PJ", dev, lambda _: run(lambda project, store: store.compact()), fresh=False)
    res["compact_mb_s"] = round(stats["read_mb"] / res["compact_s_cold"], 1)
    res.update(compacted_volumes=stats["volumes"], compact_read_mb=round(stats["read_mb"]),
               compact_written_mb=round(stats["written_mb"]))
    (_, report), _ = cold(root, "PJ", dev, lambda _: run(lambda project, store: store.check(full=True)), fresh=False)
    assert report.clean, report
    return res


def train_dictionaries(project, sample):
    """PJD: each series gets a dictionary trained on train.pkl, stored as payloads first, as an app would."""
    import pickle

    from ms_flow.payloads import Payload

    with open(Path(sample) / "train.pkl", "rb") as f:
        train = pickle.load(f)
    dicts = {}
    for series, kinds in (("ligands", ("sdf", "pdbqt")), ("poses", ("pose",))):
        with project.get_session() as session:
            project.payloads.put(session, [Payload(r, series=series) for k in kinds for r in train[k]])
            session.commit()
        dicts[series] = str(project.payloads.train_dictionary(series))
    return dicts


def measure_job(root, n, workers, quick, dev=None, codec="none", sample=None, maintain=False):
    """PJ: the import as a job of the engine writes it (V2-12); PJZ/PJD with zstd (V5); PM then maintains it (V4)."""
    from ms_flow import payloads
    from ms_flow.engine import Engine, Flow, ProcessExecutor, job, task
    from ms_flow.sinks import table_sink

    from benchmarks.refactor_optimization import workers as tasks

    store_chunk = task()(tasks.store_chunk)
    project = store_project(root)
    config = {"sample": str(sample) if sample else None, "codec": codec,
              "dicts": train_dictionaries(project, sample) if codec == "zstd-dict" else {}}
    with project.engine.connect() as c:  # the training payloads stay out of raw_mb/stored_mb
        first = c.exec_driver_sql("SELECT coalesce(max(id), 0) FROM ms_payloads").scalar() + 1
    output = table_sink("molecules", columns=("id", "mw", "logp", "score", *(f"{k}_payload_id" for k in KINDS)))
    chunks = job(task=store_chunk, output=output)(
        lambda params, project: [(i, min(i + CHUNK, n), config) for i in range(0, n, CHUNK)])
    volume_size = payloads.VOLUME_SIZE
    payloads.VOLUME_SIZE = PM_VOLUME_MB << 20 if maintain else volume_size  # the writer is this process
    try:
        with Engine(cpus=workers, mem_mb=0) as engine:
            engine.register("process", ProcessExecutor(workers))
            flow = Flow(engine, project)
            warm = flow.submit(job(task=store_chunk)(lambda params, project: [(0, 0, config)] * workers))
            engine.wait(warm, 120)  # workers up before the clock starts
            t = time.perf_counter()
            ended = engine.wait(flow.submit(chunks), 3600)
            elapsed = time.perf_counter() - t
            assert ended.status == "completed", ended.error
        with project.engine.connect() as c:
            c.exec_driver_sql("PRAGMA wal_checkpoint(TRUNCATE)")
        res = {"n": n, "workers": workers, "codec": codec, "import_s": elapsed}
        res["durable_s"] = res["import_s"] + timed_sync()
        with project.engine.connect() as c:
            raw, stored = c.exec_driver_sql(
                "SELECT sum(raw_length), sum(length) FROM ms_payloads WHERE id >= ?", (first,)).one()
            train = c.exec_driver_sql("SELECT coalesce(sum(length), 0) FROM ms_payloads WHERE id < ?",
                                      (first,)).scalar()
        res["raw_mb"], res["stored_mb"], res["train_mb"] = round(raw / 1e6), round(stored / 1e6), round(train / 1e6, 1)
        if quick:
            res["verified_ids"] = verify_store(project, project.payloads, n, docked_hash)
    finally:
        payloads.VOLUME_SIZE = volume_size
        project.dispose()
    res.update(maintain_job(root, dev) if maintain else read_all_job(root, n, dev))
    res["files"] = sum(len(f) for _, _, f in os.walk(root))  # closed: no -wal/-shm
    res["size_mb"] = round(dir_size(root) / 1e6)
    return res


def measure_store(variant, root, n, quick):
    """PS/PSN: the import through PayloadStore, the way a job's writer will do it."""
    from ms_flow import payloads

    docked = set(random.Random(2).sample(range(n), int(n * DOCKED_FRACTION)))
    project = store_project(root)
    real_sync = payloads._datasync
    if variant == "PSN":
        payloads._datasync = lambda fd: None
    store = payloads.PayloadStore(project)
    rng = random.Random(1)
    insert = f"INSERT INTO molecules VALUES (?,?,?,?,{','.join('?' * len(KINDS))})"
    try:
        t = time.perf_counter()
        for start in range(0, n, SHARD):
            ids = range(start, min(n, start + SHARD))
            wanted = [(i, k) for i in ids for k in kinds_for(i, docked.__contains__)]
            got = dict(zip(wanted, store.put(session := project.get_session(), [
                payloads.Payload(payload(i, k), kind=k, series="poses" if k == "pose" else "ligands") for i, k in wanted])))
            rows = [(i, rng.uniform(150, 700), rng.uniform(-2, 7), rng.uniform(-12, -3), *(got.get((i, k)) for k in KINDS))
                    for i in ids]
            session.connection().connection.dbapi_connection.executemany(insert, rows)
            session.commit()
            session.close()
        with project.engine.connect() as c:
            c.exec_driver_sql("PRAGMA wal_checkpoint(TRUNCATE)")
        res = {"n": n, "import_s": time.perf_counter() - t}
        res["durable_s"] = res["import_s"] + timed_sync()
        if quick:
            res["verified_ids"] = verify_store(project, store, n, docked.__contains__)
    finally:
        store.close()
        payloads._datasync = real_sync
        project.dispose()
    res["files"] = sum(len(files) for _, _, files in os.walk(root))
    res["size_mb"] = round(dir_size(root) / 1e6)
    return res


class Reader:
    """Both modes: key -> bytes, from SQLite blobs or from volumes through (vol, off, len)."""

    def __init__(self, variant, root):
        self.v, self.root = variant, Path(root).resolve()
        self.con = sqlite3.connect(f"{(self.root / 'project.db').as_uri()}?mode=ro", uri=True)
        self.fds = {}

    def _pread(self, vol, off, ln):
        fd = self.fds.get(vol)
        if fd is None:
            fd = self.fds[vol] = os.open(vol_path(self.root, vol), os.O_RDONLY)
        return os.pread(fd, ln, off)

    def get(self, k):
        if self.v == "RT":
            return self.con.execute("SELECT data FROM blobs WHERE key=?", (k,)).fetchone()[0]
        return self._pread(*self.con.execute("SELECT vol, off, len FROM refs WHERE key=?", (k,)).fetchone())

    def read(self, mol_id, kind):
        return self.get(key(mol_id, kind))

    def get_many(self, keys):
        """A batch for a worker: volumes read in (vol, off) order."""
        if self.v == "RT":
            return [self.get(k) for k in keys]
        refs = []
        for s in range(0, len(keys), 900):
            part = keys[s:s + 900]
            refs += self.con.execute(f"SELECT key, vol, off, len FROM refs WHERE key IN ({','.join('?' * len(part))})",
                                     part).fetchall()
        refs.sort(key=lambda r: (r[1], r[2]))
        got = {k: self._pread(v, o, ln) for k, v, o, ln in refs}
        return [got[k] for k in keys]

    def close(self):  # open descriptors would keep the bytes of a deleted tree on disk
        self.con.close()
        for fd in self.fds.values():
            os.close(fd)
        self.fds = {}


_R = None


def _init(variant, root):  # serial pool initializer
    global _R
    _R = Reader(variant, root)


def _read_many(keys):  # serial pool task: (molecule, kind) pairs
    return sum(len(_R.read(m, k)) for m, k in keys)


def _read_keys(variant, root, keys):  # parallel loky task: integer keys
    global _R
    if _R is None:
        _R = Reader(variant, root)
    return sum(len(_R.get(k)) for k in keys)


# --- Measurements -------------------------------------------------------------------

def timed_sync():
    """Seconds to flush what the import left in the page cache: without it, an import that never syncs
    looks as fast as the RAM, not the disk."""
    t = time.perf_counter()
    subprocess.run(["sync"])
    return time.perf_counter() - t


def timed(fn):
    t = time.perf_counter()
    out = fn()
    return time.perf_counter() - t, out


def dir_size(path):
    return sum(f.stat().st_size for f in Path(path).rglob("*") if f.is_file())


def percentiles(lat):
    lat.sort()
    return lat[len(lat) // 2], lat[len(lat) * 99 // 100]  # p50 and p99 (indexes 1000 and 1980 of 2000)


def verify(read, n, is_docked, updated=frozenset()):
    """Quick self-check: 200 random molecules read back byte-identical to what generation produced."""
    ids = random.Random(7).sample(range(n), min(200, n))
    for i in ids:
        for kind in kinds_for(i, is_docked):
            want = payload(i, kind) + (b"v2" if kind == "current" and i in updated else b"")
            got = read(i, kind)
            if got != want:
                raise AssertionError(f"molecule {i} {kind}: read back {len(got)} bytes that differ from the {len(want)} written")
    return len(ids)


# --- Cold reads ---------------------------------------------------------------------

def drop_cache(root):
    """Evict every file under `root` from the page cache. fsync first: DONTNEED only drops clean pages."""
    for p in Path(root).rglob("*"):
        if p.is_file():
            fd = os.open(p, os.O_RDONLY)
            try:
                os.fsync(fd)
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
            finally:
                os.close(fd)


def sectors_read(dev):
    """Sectors the kernel has read from block device `dev` so far (field 6 of /proc/diskstats); None if it is not listed."""
    try:
        with open("/proc/diskstats") as f:
            for line in f:
                fields = line.split()
                if fields[2] == dev:
                    return int(fields[5])
    except OSError:
        pass
    return None


def cold(root, variant, dev, fn, fresh=True):
    """One measure with nothing cached: evict the files, then run `fn(reader)` on a new reader, closed afterwards
    (`fn(None)` when `fresh` is False: the measure starts its own readers or processes).
    Returns (what fn returned, MB the device read meanwhile or None)."""
    drop_cache(root)
    before = sectors_read(dev)
    r = Reader(variant, root) if fresh else None
    try:
        out = fn(r)
    finally:
        if r:
            r.close()
    after = sectors_read(dev)
    return out, None if None in (before, after) else round((after - before) * 512 / 1e6, 1)


# --- Reads, shared by the warm and the cold runs ------------------------------------

TOP = "SELECT id FROM molecules WHERE mw < 450 AND logp < 4 ORDER BY score LIMIT 1000"


def latency(r, mols, is_docked):
    """p50 and p99 (us) of reading all the payloads of one molecule, over `mols`."""
    lat = []
    for m in mols:
        t = time.perf_counter()
        [r.read(m, k) for k in kinds_for(m, is_docked)]
        lat.append((time.perf_counter() - t) * 1e6)
    return percentiles(lat)


def query_top(r):  # serial: (seconds, the top 1000 ids)
    return timed(lambda: [row[0] for row in r.con.execute(TOP)])


def sql_s(query):
    return lambda r: timed(lambda: r.con.execute(query).fetchall())[0]


def serial_reads(n, top, docked):
    """The serial reads after the top query, as (metric, fn(reader) -> seconds): warm they share one reader,
    cold each gets its own."""
    docked_top = [i for i in top if i in docked]
    return [
        ("read_top1000_current_s", lambda r: timed(lambda: [r.read(i, "current") for i in top])[0]),
        ("read_top_poses_s", lambda r: timed(lambda: [r.read(i, "pose") for i in docked_top])[0]),
        ("stream_all_current_s", lambda r: timed(lambda: sum(len(r.read(i, "current")) for i in range(n)))[0]),
        # ordering / filtering on the index (no payloads touched)
        ("sort_unindexed_logp_top1000_s", sql_s("SELECT id FROM molecules WHERE mw < 500 ORDER BY logp DESC LIMIT 1000")),
        ("count_filter_s", sql_s("SELECT count(*) FROM molecules WHERE mw BETWEEN 300 AND 400 AND logp < 3")),
        ("page_by_score_offset_s", sql_s("SELECT id FROM molecules WHERE score IS NOT NULL ORDER BY score"
                                         " LIMIT 200 OFFSET %d" % (n // 10))),
    ]


def open_and_get_mol_x(variant, root, n, is_docked):
    """Access molecule X: a fresh reader and the first molecule. Returns (ms, the reader, which the caller closes)."""
    t = time.perf_counter()
    r = Reader(variant, root)
    x = n // 2
    [r.read(x, k) for k in kinds_for(x, is_docked)]
    return (time.perf_counter() - t) * 1e3, r


def serial_pool(variant, root, chunks):
    ctx = get_context("spawn")
    t = time.perf_counter()
    with ctx.Pool(8, initializer=_init, initargs=(variant, str(root))) as pool:
        pool.map(_read_many, chunks)
    return time.perf_counter() - t


def top_batch(r):  # parallel
    t = time.perf_counter()
    top = [i for (i,) in r.con.execute(TOP)]
    r.get_many([key(i, "current") for i in top])
    return time.perf_counter() - t


def stream_batches(r, n):  # parallel
    t = time.perf_counter()
    for s in range(0, n, 10_000):
        r.get_many([key(i, "current") for i in range(s, min(n, s + 10_000))])
    return time.perf_counter() - t


def loky_read(variant, root, keys, workers):  # parallel: the workers start here, after any cache drop
    ex = get_reusable_executor(max_workers=workers, timeout=30)
    try:
        ex.submit(_noop).result()
        t = time.perf_counter()
        list(ex.map(_read_keys, [variant] * workers, [str(root)] * workers, [keys[i::workers] for i in range(workers)]))
        return time.perf_counter() - t
    finally:
        ex.shutdown(wait=True, kill_workers=True)


# --- Modes --------------------------------------------------------------------------

def measure_serial(variant, root, n, quick, dev=None):
    docked = set(random.Random(2).sample(range(n), int(n * DOCKED_FRACTION)))
    is_docked = docked.__contains__
    res = {"n": n}

    res["import_s"] = import_serial(variant, root, n, docked)
    res["durable_s"] = res["import_s"] + timed_sync()
    res["files"] = sum(len(files) for _, _, files in os.walk(root))
    res["size_mb"] = round(dir_size(root) / 1e6)
    res["du_mb"] = int(subprocess.run(["du", "-sm", str(root)], capture_output=True, text=True).stdout.split()[0])

    reader = r2 = None
    try:
        reader = Reader(variant, root)
        res["query_top1000_s"], top = query_top(reader)
        reads = serial_reads(n, top, docked)
        for name, fn in reads:
            res[name] = fn(reader)
        res["open_and_get_mol_x_ms"], r2 = open_and_get_mol_x(variant, root, n, is_docked)
        mols = random.Random(5).sample(range(n), min(2000, n))
        res["get_mol_p50_us"], res["get_mol_p99_us"] = latency(r2, mols, is_docked)
        rng = random.Random(3)
        keys = [(m, rng.choice(["original", "current", "pdbqt"])) for m in (rng.randrange(n) for _ in range(80_000))]
        chunks = [keys[i::8] for i in range(8)]
        res["parallel_8x10k_random_s"] = serial_pool(variant, root, chunks)

        # mutation: re-prepare 5000 'current' payloads
        upd = rng.sample(range(n), min(5000, n))
        con = connect(root / "project.db")
        t = time.perf_counter()
        if variant == "RT":
            con.executemany("UPDATE blobs SET data=? WHERE key=?", [(payload(i, "current") + b"v2", key(i, "current")) for i in upd])
        else:
            vol = VolumeWriter(root, sync=variant == "H")
            rows = [(*vol.append(payload(i, "current") + b"v2"), key(i, "current")) for i in upd]
            vol.flush()
            con.executemany("UPDATE refs SET vol=?, off=?, len=? WHERE key=?", rows)
        con.commit()
        res["update_5000_s"] = time.perf_counter() - t
        if variant != "RT":
            vol.fh.close()
        con.close()

        # the same reads with nothing cached: each one evicts the files and reads through a new reader
        for r in (reader, r2):
            r.close()
        (res["query_top1000_s_cold"], _), _ = cold(root, variant, dev, query_top)
        for name, fn in reads:
            res[name + "_cold"], mb = cold(root, variant, dev, fn)
            if name == "stream_all_current_s":
                res["stream_all_current_cold_disk_mb"] = mb
        (res["open_and_get_mol_x_ms_cold"], rx), _ = cold(root, variant, dev, lambda _: open_and_get_mol_x(variant, root, n, is_docked), fresh=False)
        rx.close()
        (res["get_mol_p50_us_cold"], res["get_mol_p99_us_cold"]), _ = cold(root, variant, dev, lambda r: latency(r, mols, is_docked))
        res["parallel_8x10k_random_s_cold"], res["parallel_8x10k_random_cold_disk_mb"] = cold(
            root, variant, dev, lambda _: serial_pool(variant, root, chunks), fresh=False)

        if quick:  # after the mutation: it covers both the imported and the re-prepared bytes
            check = Reader(variant, root)
            try:
                res["verified_ids"] = verify(check.read, n, is_docked, set(upd))
            finally:
                check.close()
    finally:
        for r in (reader, r2):
            if r:
                r.close()
    return res


def measure_parallel(variant, root, n, workers, quick, dev=None):
    (root / "volumes").mkdir(parents=True)
    res = {"n": n, "workers": workers}
    res["import_s"] = import_parallel(variant, root, n, workers)
    res["durable_s"] = res["import_s"] + timed_sync()
    res["files"] = sum(len(f) for _, _, f in os.walk(root))
    res["size_mb"] = round(dir_size(root) / 1e6)

    mols = random.Random(5).sample(range(n), min(2000, n))
    r = Reader(variant, root)
    try:
        p50, p99 = latency(r, mols, docked_hash)
        res["get_mol_p50_us"], res["get_mol_p99_us"] = round(p50, 1), round(p99, 1)
        res["top1000_batch_s"] = top_batch(r)
        res["stream_all_current_s"] = stream_batches(r, n)
        rng = random.Random(3)
        keys = [key(rng.randrange(n), rng.choice(["original", "current", "pdbqt"])) for _ in range(20_000 * workers)]
        random_read = f"parallel_{workers}x20k_random"
        res[random_read + "_s"] = loky_read(variant, root, keys, workers)
    finally:
        r.close()

    # the same reads with nothing cached: each one evicts the files and reads through a new reader
    (p50, p99), _ = cold(root, variant, dev, lambda r: latency(r, mols, docked_hash))
    res["get_mol_p50_us_cold"], res["get_mol_p99_us_cold"] = round(p50, 1), round(p99, 1)
    res["top1000_batch_s_cold"], _ = cold(root, variant, dev, top_batch)
    res["stream_all_current_s_cold"], res["stream_all_current_cold_disk_mb"] = cold(root, variant, dev, lambda r: stream_batches(r, n))
    res[random_read + "_s_cold"], res[random_read + "_cold_disk_mb"] = cold(
        root, variant, dev, lambda _: loky_read(variant, root, keys, workers), fresh=False)

    if quick:
        check = Reader(variant, root)
        try:
            res["verified_ids"] = verify(check.read, n, docked_hash)
        finally:
            check.close()
    return res


# --- Record -------------------------------------------------------------------------

def _git(*args):
    return subprocess.run(["git", *args], cwd=_REPO, capture_output=True, text=True).stdout.strip()


def _block_model(name):
    """Model of the physical disk under a block device name, following partitions and dm/LVM slaves."""
    sysdir = Path("/sys/class/block", name).resolve()
    if (sysdir / "partition").exists():
        sysdir = sysdir.parent
    model = sysdir / "device" / "model"
    if model.exists():
        return model.read_text().strip()
    for slave in sorted((sysdir / "slaves").glob("*")):
        found = _block_model(slave.name)
        if found:
            return found
    return None


def disk_info(path):
    """Device, filesystem and model of the disk holding `path`, or None if /sys and findmnt do not say."""
    try:
        out = subprocess.run(["findmnt", "-no", "SOURCE,FSTYPE", "--target", str(path)],
                             capture_output=True, text=True, timeout=10).stdout.split()
        device = out[0].split("[")[0]  # btrfs: /dev/sda1[/@subvol]
        return {"device": device, "fstype": out[1], "model": _block_model(Path(device).resolve().name)}
    except (OSError, IndexError, subprocess.SubprocessError):
        return None


def diskstats_name(disk):
    """The name /proc/diskstats gives the device of `disk_info`, e.g. nvme0n1p3; None for tmpfs, overlay, network."""
    if disk and disk["device"].startswith("/dev/"):
        return Path(disk["device"]).resolve().name
    return None


def mean_molecule_bytes():
    """Payload bytes per molecule, from the generator itself, plus about 100 bytes of index rows."""
    return sum(len(payload(i, k)) for i in range(2000) for k in kinds_for(i, docked_hash)) / 2000 + 100


def save(out, meta, mode, variant, res):
    """Add one variant to the label's file, replacing the same variant of an earlier run and keeping the rest."""
    record = json.loads(out.read_text()) if out.exists() else {}
    variants = record.get("variants", {})
    variants[f"{mode}/{variant}"] = {k: round(v, 3) if isinstance(v, float) else v for k, v in res.items()}
    record = {**meta, "recorded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "mode": mode,
              "n": res["n"], "workers": res.get("workers"), "variants": variants}
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(record, indent=2) + "\n")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--mode", choices=sorted(VARIANTS), required=True)
    ap.add_argument("--label", required=True, help="name of this run, e.g. 'baseline'")
    ap.add_argument("--n", type=int, help="molecules (default 100000, or 3000 with --quick)")
    ap.add_argument("--variants", help=f"comma list (serial: {','.join(VARIANTS['serial'])}; parallel: {','.join(VARIANTS['parallel'])})")
    ap.add_argument("--workers", type=int, default=14, help="parallel mode: loky processes")
    ap.add_argument("--data-dir", type=Path, default=_REPO / ".bench_runs" / "refactor_optimization" / "volumes",
                    help="where the variants write; the disk it is on is the one measured")
    ap.add_argument("--results-dir", type=Path, default=_HERE / "results")
    ap.add_argument("--sample", type=Path, help="directory with bench.pkl and train.pkl of real payloads (see above)")
    ap.add_argument("--keep", action="store_true", help="do not delete the data of each variant")
    ap.add_argument("--quick", action="store_true", help="small n, and check 200 molecules read back identical; not comparable")
    args = ap.parse_args(argv)

    # PJZ/PJD/PM only on request: the codecs want --sample, the synthetic pool is not chemistry
    variants = args.variants.split(",") if args.variants else [v for v in VARIANTS[args.mode]
                                                               if v not in ("PJZ", "PJD", "PM")]
    if bad := [v for v in variants if v not in VARIANTS[args.mode]]:
        ap.error(f"{args.mode} has no variant {bad}; choose from {','.join(VARIANTS[args.mode])}")
    if args.sample and (other := [v for v in variants if v not in JOB_CODECS]):
        ap.error(f"--sample is for {','.join(JOB_CODECS)}; not {other}")  # the other variants generate in their own workers
    if "PJD" in variants and not args.sample:
        ap.error("PJD trains its dictionaries on <sample>/train.pkl: give --sample")
    use_sample(args.sample)
    n = args.n or (3000 if args.quick else 100_000)
    data_dir = args.data_dir.resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    out = args.results_dir / ("volumes_" + args.label.replace("/", "-").replace(" ", "_") + ".json")
    meta = {"label": args.label,
            "commit": _git("rev-parse", "--short", "HEAD") + ("+dirty" if _git("status", "--porcelain", "-uno") else ""),
            "machine": {"hostname": socket.gethostname(), "cpus": os.cpu_count(), "kernel": os.uname().release},
            "disk": disk_info(data_dir), **({"sample": str(args.sample)} if args.sample else {})}
    dev = diskstats_name(meta["disk"])

    for variant in variants:
        need = int(n * mean_molecule_bytes() * (1.3 if variant == "RT" else 1.0))  # SQLite blobs weigh ~1.3x volumes
        free = shutil.disk_usage(data_dir).free
        if free < need + SPARE:
            raise SystemExit(f"{args.mode}/{variant} needs about {need / 1e9:.1f} GB and only {free / 1e9:.1f} GB are free in "
                             f"{data_dir}; it must leave {SPARE / 1e9:.1f} GB spare, so it is not run")
        root = data_dir / f"{args.mode}_{variant}"
        shutil.rmtree(root, ignore_errors=True)
        root.mkdir()
        try:
            if variant in ("PS", "PSN"):
                res = measure_store(variant, root, n, args.quick)
            elif variant in JOB_CODECS:
                res = measure_job(root, n, args.workers, args.quick, dev, JOB_CODECS[variant], args.sample,
                                  maintain=variant == "PM")
            elif args.mode == "serial":
                res = measure_serial(variant, root, n, args.quick, dev)
            else:
                res = measure_parallel(variant, root, n, args.workers, args.quick, dev)
        finally:
            if not args.keep:
                shutil.rmtree(root, ignore_errors=True)
        save(out, meta, args.mode, variant, res)
        print(json.dumps({"variant": f"{args.mode}/{variant}",
                          **{k: round(v, 3) if isinstance(v, float) else v for k, v in res.items()}}), flush=True)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
