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
    """A row with the bytes of a molecule, the way a task hands them to a sink (`tests/test_payloads_jobs.py`)."""
    from ms_flow.payloads import Payload

    data = f"molecule {payload}\n".encode() * (payload % 7 + 1)
    return {"n": payload, "structure_payload_id": Payload(data, kind="sdf", series="ligands")}


def uses_payloads(element: dict) -> dict:
    """What a task got from `with_payloads` and a materialized receptor, as a row (`tests/test_payloads_read.py`)."""
    import zlib

    with open(element["receptor"]) as file:
        text = file.read()
    return {"n": element["n"], "got_bytes": int(type(element["structure"]) is bytes), "crc": zlib.crc32(element["structure"]),
            "receptor_path": str(element["receptor"]), "receptor_text": text}



def store_chunk(bounds):
    """`bench_volumes` PJ: the molecules `[start, stop)` as rows with their `Payload`s."""
    from benchmarks.refactor_optimization import bench_volumes

    return bench_volumes.store_rows(*bounds)


def packs_with_dictionary(element: dict) -> dict:
    """A row whose payload uses the job's zstd dictionary, a `File` (`tests/test_payloads_codecs.py`)."""
    from ms_flow.payloads import Payload

    data = element["data"]
    return {"n": element["n"], "structure_payload_id": Payload(data, series="ligands", dictionary=element["dictionary"])}
