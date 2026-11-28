"""M13 gate: Ray over two real machines, no shared disk, end to end and a node lost (2026-11-27).

    PYTHONPATH=src:. python benchmarks/ray_two_machines.py --host 192.168.1.6 --env /tmp/msf_env \
        --root /home/mario/bench_gates/ray2

`--env` is an environment there whose `bin/ray` is the same Ray and Python as here and imports this
ms_flow. Three jobs on `RayOverHosts([host])`:
  out    a file of this machine read by tasks on both machines (it lands once per node);
  back   each task leaves a container of `--mb` MiB on its node; it comes here byte for byte;
  lost   2 s tasks, and the other machine's Ray stopped halfway: every result still comes, the
         tasks it was running run again here.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import threading
import time
from pathlib import Path


def _sha(path) -> str:
    with open(path, "rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def read_it(payload: dict) -> dict:
    import hashlib
    import socket

    import time

    time.sleep(1)  # long enough that this machine alone does not take them all
    with open(payload["file"], "rb") as file:
        return {"host": socket.gethostname(), "sha": hashlib.file_digest(file, "sha256").hexdigest()}


def leave_it(payload: dict) -> dict:
    import hashlib
    import os
    import socket
    import time

    time.sleep(1)
    path = os.path.join(payload["out"], "part.arrows")
    with open(path, "wb") as file:
        file.write(os.urandom(payload["mb"] << 20))
    with open(path + ".index", "wb") as file:
        file.write(b"index")
    with open(path, "rb") as file:
        sha = hashlib.file_digest(file, "sha256").hexdigest()
    return {"host": socket.gethostname(), "sha": sha, "named": payload["out"]}


def nap(payload: dict) -> dict:
    import socket
    import time

    time.sleep(2)
    return {"host": socket.gethostname(), "n": payload["n"]}


def main() -> None:
    from ray import cloudpickle

    from ms_env import Host
    from ms_flow.engine import Dir, Engine, File
    from ms_flow.ray_hosts import RayOverHosts

    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--env", required=True)
    ap.add_argument("--root", type=Path, required=True)
    ap.add_argument("--mb", type=int, default=32)
    a = ap.parse_args()
    cloudpickle.register_pickle_by_value(sys.modules[__name__])  # the other machine has no copy of this file
    shutil.rmtree(a.root, ignore_errors=True)
    a.root.mkdir(parents=True)
    said = {"host": a.host}
    source = a.root / "in.bin"
    source.write_bytes(os.urandom(a.mb << 20))
    with Engine(cpus=1) as engine:
        engine.register("ray", RayOverHosts([a.host], a.env))

        t = time.perf_counter()
        got = []
        job = engine.submit("ray", read_it, [{"file": File(str(source)), "n": n} for n in range(60)], batch=1,
                            on_results=got.extend)
        status = engine.wait(job, 600).status
        hosts = {r["host"] for _k, r in got}
        said["out"] = {"status": status, "s": round(time.perf_counter() - t, 1),
                       "by_host": {h: sum(1 for _k, r in got if r["host"] == h) for h in hosts},
                       "same_bytes": all(r["sha"] == _sha(source) for _k, r in got), "results": len(got)}

        t = time.perf_counter()
        got = []
        job = engine.submit("ray", leave_it, [{"out": Dir(str(a.root / "back" / str(n))), "mb": a.mb, "n": n}
                                              for n in range(40)], batch=1, on_results=got.extend)
        status = engine.wait(job, 1200).status
        same = all(_sha(Path(r["named"]) / "part.arrows") == r["sha"] for _k, r in got)
        said["back"] = {"status": status, "s": round(time.perf_counter() - t, 1),
                        "by_host": {h: sum(1 for _k, r in got if r["host"] == h) for h in {r["host"] for _k, r in got}}, "same_bytes": same, "results": len(got),
                        "mib": a.mb * len(got)}

        got = []
        job = engine.submit("ray", nap, [{"n": n} for n in range(80)], batch=1, retries=1, on_results=got.extend)
        seen = set()
        while len(got) < 10:
            time.sleep(0.2)
        Host(a.host).run(f"{a.env}/bin/ray stop --force >/dev/null 2>&1; :", timeout=60)  # the node goes
        stopped_at = len(got)
        status = engine.wait(job, 900).status
        by_host = {}
        for _k, r in got:
            by_host[r["host"]] = by_host.get(r["host"], 0) + 1
        said["lost"] = {"status": status, "results": len(got), "distinct": len({r["n"] for _k, r in got}),
                        "done_when_stopped": stopped_at, "by_host": by_host}
    print(json.dumps(said), flush=True)


if __name__ == "__main__":
    main()
