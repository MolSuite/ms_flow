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
parallel: `--workers` loky processes produce the payloads and one parent writes (the `bench3.py` of 2026-10-15)
  VP  volumes, the parent appends the bytes that come back with each result
  VC  volumes, each worker writes one segment file per chunk and the parent keeps the references
  RT  SQLite blobs, the parent inserts

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
VARIANTS = {"serial": ("HT", "H", "RT"), "parallel": ("VP", "VC", "RT")}
# The payloads come from this pool; seeded, so every process (a loky worker, a test) produces the same bytes.
_POOL = random.Random(0).randbytes(32768).hex().encode()


def payload(i, kind):
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


def measure_serial(variant, root, n, quick):
    docked = set(random.Random(2).sample(range(n), int(n * DOCKED_FRACTION)))
    is_docked = docked.__contains__
    res = {"n": n}

    res["import_s"] = import_serial(variant, root, n, docked)
    subprocess.run(["sync"])
    res["files"] = sum(len(files) for _, _, files in os.walk(root))
    res["size_mb"] = round(dir_size(root) / 1e6)
    res["du_mb"] = int(subprocess.run(["du", "-sm", str(root)], capture_output=True, text=True).stdout.split()[0])

    reader = r2 = None
    try:
        reader = Reader(variant, root)
        q = ("SELECT id FROM molecules WHERE mw < 450 AND logp < 4 ORDER BY score LIMIT 1000")
        res["query_top1000_s"], top = timed(lambda: [r[0] for r in reader.con.execute(q)])
        res["read_top1000_current_s"], _ = timed(lambda: [reader.read(i, "current") for i in top])
        docked_top = [i for i in top if i in docked]
        res["read_top_poses_s"], _ = timed(lambda: [reader.read(i, "pose") for i in docked_top])
        res["stream_all_current_s"], _ = timed(lambda: sum(len(reader.read(i, "current")) for i in range(n)))

        # ordering / filtering on the index (no payloads touched)
        res["sort_unindexed_logp_top1000_s"], _ = timed(lambda: reader.con.execute(
            "SELECT id FROM molecules WHERE mw < 500 ORDER BY logp DESC LIMIT 1000").fetchall())
        res["count_filter_s"], _ = timed(lambda: reader.con.execute(
            "SELECT count(*) FROM molecules WHERE mw BETWEEN 300 AND 400 AND logp < 3").fetchall())
        res["page_by_score_offset_s"], _ = timed(lambda: reader.con.execute(
            "SELECT id FROM molecules WHERE score IS NOT NULL ORDER BY score LIMIT 200 OFFSET %d" % (n // 10)).fetchall())
        # access molecule X: fresh reader, first molecule, then 2000 random molecules with all their payloads
        t = time.perf_counter()
        r2 = Reader(variant, root)
        x = n // 2
        [r2.read(x, k) for k in kinds_for(x, is_docked)]
        res["open_and_get_mol_x_ms"] = (time.perf_counter() - t) * 1e3
        mols = random.Random(5).sample(range(n), min(2000, n))
        lat = []
        for m in mols:
            t = time.perf_counter()
            [r2.read(m, k) for k in kinds_for(m, is_docked)]
            lat.append((time.perf_counter() - t) * 1e6)
        res["get_mol_p50_us"], res["get_mol_p99_us"] = percentiles(lat)
        rng = random.Random(3)
        keys = [(m, rng.choice(["original", "current", "pdbqt"])) for m in (rng.randrange(n) for _ in range(80_000))]
        chunks = [keys[i::8] for i in range(8)]
        ctx = get_context("spawn")
        t = time.perf_counter()
        with ctx.Pool(8, initializer=_init, initargs=(variant, str(root))) as pool:
            pool.map(_read_many, chunks)
        res["parallel_8x10k_random_s"] = time.perf_counter() - t

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


def measure_parallel(variant, root, n, workers, quick):
    (root / "volumes").mkdir(parents=True)
    res = {"n": n, "workers": workers}
    res["import_s"] = import_parallel(variant, root, n, workers)
    subprocess.run(["sync"])
    res["files"] = sum(len(f) for _, _, f in os.walk(root))
    res["size_mb"] = round(dir_size(root) / 1e6)

    r = Reader(variant, root)
    try:
        mols = random.Random(5).sample(range(n), min(2000, n))
        lat = []
        for m in mols:
            t = time.perf_counter()
            [r.get(key(m, k)) for k in kinds_for(m, docked_hash)]
            lat.append((time.perf_counter() - t) * 1e6)
        p50, p99 = percentiles(lat)
        res["get_mol_p50_us"], res["get_mol_p99_us"] = round(p50, 1), round(p99, 1)

        t = time.perf_counter()
        top = [i for (i,) in r.con.execute("SELECT id FROM molecules WHERE mw < 450 AND logp < 4 ORDER BY score LIMIT 1000")]
        r.get_many([key(i, "current") for i in top])
        res["top1000_batch_s"] = time.perf_counter() - t

        t = time.perf_counter()
        for s in range(0, n, 10_000):
            r.get_many([key(i, "current") for i in range(s, min(n, s + 10_000))])
        res["stream_all_current_s"] = time.perf_counter() - t

        rng = random.Random(3)
        keys = [key(rng.randrange(n), rng.choice(["original", "current", "pdbqt"])) for _ in range(20_000 * workers)]
        ex = get_reusable_executor(max_workers=workers, timeout=30)
        try:
            ex.submit(_noop).result()
            t = time.perf_counter()
            list(ex.map(_read_keys, [variant] * workers, [str(root)] * workers, [keys[i::workers] for i in range(workers)]))
            res[f"parallel_{workers}x20k_random_s"] = time.perf_counter() - t
        finally:
            ex.shutdown(wait=True, kill_workers=True)

        if quick:
            res["verified_ids"] = verify(r.read, n, docked_hash)
    finally:
        r.close()
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
    ap.add_argument("--keep", action="store_true", help="do not delete the data of each variant")
    ap.add_argument("--quick", action="store_true", help="small n, and check 200 molecules read back identical; not comparable")
    args = ap.parse_args(argv)

    variants = args.variants.split(",") if args.variants else list(VARIANTS[args.mode])
    if bad := [v for v in variants if v not in VARIANTS[args.mode]]:
        ap.error(f"{args.mode} has no variant {bad}; choose from {','.join(VARIANTS[args.mode])}")
    n = args.n or (3000 if args.quick else 100_000)
    data_dir = args.data_dir.resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    out = args.results_dir / ("volumes_" + args.label.replace("/", "-").replace(" ", "_") + ".json")
    meta = {"label": args.label,
            "commit": _git("rev-parse", "--short", "HEAD") + ("+dirty" if _git("status", "--porcelain", "-uno") else ""),
            "machine": {"hostname": socket.gethostname(), "cpus": os.cpu_count(), "kernel": os.uname().release},
            "disk": disk_info(data_dir)}

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
            if args.mode == "serial":
                res = measure_serial(variant, root, n, args.quick)
            else:
                res = measure_parallel(variant, root, n, args.workers, args.quick)
        finally:
            if not args.keep:
                shutil.rmtree(root, ignore_errors=True)
        save(out, meta, args.mode, variant, res)
        print(json.dumps({"variant": f"{args.mode}/{variant}",
                          **{k: round(v, 3) if isinstance(v, float) else v for k, v in res.items()}}), flush=True)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
