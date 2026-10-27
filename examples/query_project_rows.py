"""Feed a job with rows of the project itself, with no files.

    python examples/query_project_rows.py
"""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "src"))  # to run straight from the repo without installing
sys.path.insert(0, str(_REPO / "examples"))

from ms_flow import Workspace  # noqa: E402
from workers import Label, Molecule, label_ready_molecules  # noqa: E402


def main() -> None:
    ms = Workspace(app_id="demo")
    try:
        ms.create_or_open_project(
            name="query_demo_project", folder=Path("./query_demo_project").resolve(), activate=True
        )
        ms.project_db.delete(Label)  # the same result on every run
        ms.project_db.delete(Molecule)
        ms.project_db.insert_batch([
            Molecule(id=1, name="MolA", status="ready"),
            Molecule(id=2, name="MolB", status="ready"),
            Molecule(id=3, name="MolC", status="archived"),
        ])

        # The job reads `molecules` in pages (`Rows`), so the rows never travel in the submit.
        job_id = ms.submit(label_ready_molecules, executor="thread")
        print(ms.engine.wait(job_id).status)
        print(ms.project_db.select_rows(Label, fields=("molecule_id", "label"), order=("molecule_id",)))
    finally:
        ms.shutdown()


if __name__ == "__main__":
    main()
