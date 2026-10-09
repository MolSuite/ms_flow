"""Main MolSuite package.

Recommended path:
- `ms_flow.api` for the normal public API

Specialised surfaces:
- `ms_flow.engine` for the engine without a project
- `ms_flow.query` for query helpers
- `ms_flow.core.*` for internal/advanced extensions
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ms_flow.api import AppRuntime, MolSuite
    from ms_flow.engine import job, report, task
    from ms_flow.runtime import BaseRuntime
    from ms_flow.selection import MultipleResultsFound, NoResultFound, Selection
    from ms_flow.sinks import file_sink, graph_sink, table_sink

# Loaded on first use: a worker process that only runs chunks must not pay for the whole API.
_LAZY = {
    "AppRuntime": "ms_flow.api",
    "MolSuite": "ms_flow.api",
    "BaseRuntime": "ms_flow.runtime",
    "MultipleResultsFound": "ms_flow.selection",
    "NoResultFound": "ms_flow.selection",
    "Selection": "ms_flow.selection",
    "job": "ms_flow.engine",
    "report": "ms_flow.engine",
    "task": "ms_flow.engine",
    "file_sink": "ms_flow.sinks",
    "graph_sink": "ms_flow.sinks",
    "table_sink": "ms_flow.sinks",
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
    "file_sink",
    "graph_sink",
    "job",
    "report",
    "table_sink",
    "task",
]
