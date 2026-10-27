"""The bare minimum: create a project, run a job and read its table.

    python examples/run_to_table.py
"""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "src"))  # to run straight from the repo without installing
sys.path.insert(0, str(_REPO / "examples"))

from ms_flow import Workspace  # noqa: E402
from workers import Result, double_values  # noqa: E402


def main() -> None:
    ms = Workspace(app_id="demo")
    try:
        ms.create_or_open_project(name="demo_project", folder=Path("./demo_project").resolve(), activate=True)
        ms.project_db.delete(Result)  # the same result on every run

        job_id = ms.submit(double_values, {"values": [1, 2, 3, 4]}, executor="thread")
        print(ms.engine.wait(job_id).status)
        print(ms.project_db.select_rows(Result, fields=("value", "double_value"), order=("value",)))
    finally:
        ms.shutdown()


if __name__ == "__main__":
    main()
