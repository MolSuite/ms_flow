"""Main MolSuite package.

Recommended path:
- `molsuite.api` for the normal public API

Specialised surfaces:
- `molsuite.tasking` for the full declarative layer
- `molsuite.query` for query helpers
- `molsuite.core.*` for internal/advanced extensions
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ms_flow.api import AppRuntime, MolSuite
    from ms_flow.progress import watch_job, watch_jobs
    from ms_flow.runtime import BaseRuntime
    from ms_flow.selection import MultipleResultsFound, NoResultFound, Selection

# Loaded on first use: a worker process that only runs chunks must not pay for the whole API.
_LAZY = {
    "AppRuntime": "ms_flow.api",
    "MolSuite": "ms_flow.api",
    "BaseRuntime": "ms_flow.runtime",
    "MultipleResultsFound": "ms_flow.selection",
    "NoResultFound": "ms_flow.selection",
    "Selection": "ms_flow.selection",
    "watch_job": "ms_flow.progress",
    "watch_jobs": "ms_flow.progress",
}


def __getattr__(name: str):
    if name not in _LAZY:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from importlib import import_module

    value = globals()[name] = getattr(import_module(_LAZY[name]), name)
    return value


__all__ = [
    "AppRuntime",
    "BaseRuntime",
    "MolSuite",
    "MultipleResultsFound",
    "NoResultFound",
    "Selection",
    "watch_job",
    "watch_jobs",
]
