"""A worker function whose module takes long to import, as one that imports a heavy library does."""

import time

time.sleep(2.0)


def nap(payload):
    time.sleep(payload["s"])
    return payload["idx"]
