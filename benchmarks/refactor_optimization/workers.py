"""Chunk functions for the refactor benchmark. Separate module: `__main__` is not importable."""
from __future__ import annotations

import os
import time


def noop(payload: dict) -> dict:
    return {"ok": 1}


def row(payload: dict) -> dict:
    return {"idx": payload["idx"], "ok": 1}


def die_on_7(payload: dict) -> dict:
    if payload["idx"] == 7:
        os._exit(1)
    return {"ok": 1}


def read(payload: dict) -> dict:
    with open(payload["path"]) as file:
        return {"path": str(payload["path"]), "text": file.read()}


def leave(payload: dict) -> dict:
    path = os.path.join(payload["out"], f"{payload['n']}.txt")
    with open(path, "w") as file:
        file.write(str(payload["n"]))
    return {"made": path}


def pair(payload: dict) -> dict:
    """Reads the container it is given and the index beside it, if any; leaves one in `out`."""
    said = ""
    if payload.get("container"):
        with open(payload["container"]) as data, open(payload["container"] + ".index") as index:
            said = f"{data.read()}|{index.read()}"
    made = os.path.join(payload["out"], f"{payload['n']}.arrows")
    with open(made, "w") as data, open(made + ".index", "w") as index:
        data.write(f"data {payload['n']}")
        index.write(f"index {payload['n']}")
    return {"said": said, "made": made}


def half_then_held(payload: dict) -> int:
    """Says it is halfway and stays there until the gate file is there."""
    from ms_flow.engine import report

    report(0.5)
    while not os.path.exists(payload["gate"]):
        time.sleep(0.01)
    return payload["idx"]


def nap(payload: dict) -> dict:
    time.sleep(float(payload["s"]))
    return {"ok": 1}


def flaky(payload: dict) -> dict:
    """Fails until it has been tried `payload["fails"]` times; the count lives in a file, across workers."""
    with open(payload["path"], "a+") as tries:
        tries.write("x")
        tries.seek(0)
        if len(tries.read()) <= payload["fails"]:
            raise RuntimeError("not yet")
    return {"ok": 1}


def structure(payload: int) -> dict:
    """A row with the bytes of a specimen, the way a task hands them to a sink (`tests/test_payloads_jobs.py`)."""
    from ms_flow.payloads import Payload

    data = f"specimen {payload}\n".encode() * (payload % 7 + 1)
    return {"n": payload, "structure_payload_id": Payload(data, kind="csv", series="guests")}


def uses_payloads(element: dict) -> dict:
    """What a task got from `with_payloads` and a materialized anchor, as a row (`tests/test_payloads_read.py`)."""
    import zlib

    with open(element["anchor"]) as file:
        text = file.read()
    return {"n": element["n"], "got_bytes": int(type(element["structure"]) is bytes), "crc": zlib.crc32(element["structure"]),
            "anchor_path": str(element["anchor"]), "anchor_text": text}



def store_chunk(bounds):
    """`bench_volumes` PJ: the specimens `[start, stop)` as rows with their `Payload`s."""
    from benchmarks.refactor_optimization import bench_volumes

    return bench_volumes.store_rows(*bounds)  # (start, stop, config)


def packs_with_dictionary(element: dict) -> dict:
    """A row whose payload uses the job's zstd dictionary, a `File` (`tests/test_payloads_codecs.py`)."""
    from ms_flow.payloads import Payload

    data = element["data"]
    return {"n": element["n"], "structure_payload_id": Payload(data, series="guests", dictionary=element["dictionary"])}


def _digest(path: str) -> str:
    import hashlib

    digest = hashlib.blake2b(digest_size=16)
    with open(path, "rb") as file:
        while block := file.read(1 << 22):
            digest.update(block)
    return digest.hexdigest()


def landed(payload: dict) -> dict:
    """Where the node keeps the container it was given, and what it holds."""
    import tempfile

    path = payload["container"]
    return {"ino": os.stat(path).st_ino, "tmp": tempfile.gettempdir(), "data": _digest(path),
            "index": _digest(path + ".index")}


def leave_bytes(payload: dict) -> dict:
    """Leaves in `out` a container of `size` random bytes with its index, a small top one with its
    index, and an empty file. Names `out`, or the top container alone with `top_only`."""
    out, size = payload["out"], payload["size"]
    part, top = os.path.join(out, "part.arrows"), os.path.join(out, "top.arrows")
    with open(part, "wb") as file:
        left, n = size, 0
        while left:
            block = os.urandom(min(left, 1 << 24))
            file.write(block)
            left, n = left - len(block), n + 1
    for path, text in ((part + ".index", b"index"), (top, b"top"), (top + ".index", b"top index"),
                       (os.path.join(out, "empty.bin"), b"")):
        with open(path, "wb") as file:
            file.write(text)
    made = {name: _digest(os.path.join(out, name)) for name in os.listdir(out)}
    return {"named": top if payload.get("top_only") else out, "made": made}


def held_on(payload: dict) -> dict:
    """Says it began in `log`; on the node whose temporary directory is `victim`, the first time,
    waits there until that node is taken away."""
    import tempfile

    with open(payload["log"], "a") as file:
        file.write(f"{payload['n']} {tempfile.gettempdir()}\n")
    if tempfile.gettempdir() == payload["victim"]:
        time.sleep(600)
    made = os.path.join(payload["out"], f"{payload['n']}.txt")
    with open(made, "w") as file:
        file.write(str(payload["n"]))
    return {"made": made}


def part_of(payload: dict) -> dict:
    """A task that leaves a part of 4 records in its `out` and says so; every run is a line in `log`."""
    import uuid

    import pyarrow as pa

    from ms_flow.core.data.shard import write_table

    n = int(payload["n"])
    with open(payload["log"], "a") as log:
        log.write(f"{n}\n")
    time.sleep(float(payload.get("s", 0)))
    path = os.path.join(payload["out"], "parts", f"{n}-{uuid.uuid4().hex[:8]}.arrows")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    write_table(path, pa.table({"x": [4 * n + i for i in range(4)]}), part=f"p:{n}", base_id=4 * n)
    return {"n": n, "part": path}


def receipts_of(result: dict) -> list:
    """What `part_of` left, as the receipt its stage commits there."""
    return [{"part": f"p:{result['n']}", "path": result["part"], "role": "made", "generation_id": None,
             "folder": os.path.dirname(os.path.dirname(result["part"]))}]
