"""Shared fixtures and bootstrap for the ms_flow suite.

Imports (`src/` and `benchmarks/` on `sys.path`) are handled by pytest via
`pythonpath` in `pyproject.toml`; only what needs code lives here.
"""
from __future__ import annotations

import os

import pytest

# Qt needs a platform plugin: with no display, use offscreen so the tests that
# touch the UI can run in CI.
if not os.environ.get("QT_QPA_PLATFORM") and not os.environ.get("DISPLAY"):
    os.environ["QT_QPA_PLATFORM"] = "offscreen"


@pytest.fixture(autouse=True)
def isolated_workspace(tmp_path_factory, monkeypatch):
    """Isolate app discovery from the developer's disk.

    `discover_workspace_apps()` looks at the directory containing the repo and
    imports every `*/src/*/manifest.py` it finds. Without this the suite depends on
    which projects the developer keeps as neighbours and whether their dependencies
    are installed: green in CI, red locally, or the other way round.
    """
    empty = tmp_path_factory.mktemp("empty_workspace")
    monkeypatch.setenv("MS_FLOW_WORKSPACE_ROOT", str(empty))
