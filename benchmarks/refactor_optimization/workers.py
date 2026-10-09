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
