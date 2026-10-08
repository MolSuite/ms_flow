"""Chunk functions for the refactor benchmark. Separate module: `__main__` is not importable."""
from __future__ import annotations

import os
import time


def noop(payload: dict) -> dict:
    return {"ok": 1}


def die_on_7(payload: dict) -> dict:
    if payload["idx"] == 7:
        os._exit(1)
    return {"ok": 1}


def nap(payload: dict) -> dict:
    time.sleep(float(payload["s"]))
    return {"ok": 1}
