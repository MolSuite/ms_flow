"""`MSFLOW_SLOW_MS=<ms>` logs each step known to stall that takes at least that long: a call of
the app to the engine's process, a writer's delivery, the start of the engine's process, and an
engine's process taken for hung. One line each on stderr and in `<molsuite home>/logs/slow.log`,
from either process. Off unless set.
"""
from __future__ import annotations

import os
import sys
import time


def slow(what: str, started: float, *, always: bool = False) -> None:
    """Log `what` if `MSFLOW_SLOW_MS` is set and it took that long since `started`
    (`time.perf_counter()`); `always`: whatever it took."""
    limit = os.environ.get("MSFLOW_SLOW_MS")
    if not limit:
        return
    ms = (time.perf_counter() - started) * 1000
    if not always and ms < float(limit):
        return
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} pid {os.getpid()} {what}: {ms:.0f} ms\n"
    sys.stderr.write(line)
    try:
        from ms_flow.core.settings.models import molsuite_home

        path = molsuite_home() / "logs" / "slow.log"
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as log:
            log.write(line)
    except OSError:
        pass
