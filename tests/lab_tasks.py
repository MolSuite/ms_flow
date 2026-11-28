"""The campaign of `test_lab.py`: imported here and, from a copy, on the lab's nodes."""
import os
import time
from pathlib import Path

from pydantic import BaseModel

from ms_flow.campaign import Stage, campaign
from ms_flow.engine import Dir, File, task

GOT = []  # what the campaign delivered to this process


class Params(BaseModel):
    n: int = 8
    nap: float = 0
    tag: str = ""      # not volatile: a new tag is a new campaign
    run_id: str = ""


@task()
def square(element, params):
    time.sleep(params.nap)
    Path(element["out"], f"{element['n']}.txt").write_text("x")
    return {"n": element["n"], "v": element["n"] ** 2, "said": Path(element["f"]).read_text(),
            "host": os.uname().nodename}


@task()
def granted(element):
    """What the queue gave the job this element ran in."""
    return {"n": element, "cpus": os.environ.get("SLURM_CPUS_ON_NODE", "")}


class _Collect:
    def write(self, results):
        GOT.extend(results)


@campaign(params_model=Params, output=lambda params, project: _Collect(), volatile=("run_id",))
def sweep(params, project):
    said = Path(os.environ["CAMPAIGN_FILE"])
    return [Stage("vs", square, [{"n": n, "f": File(said), "out": Dir(said.parent / "work"), "_cpu_required": 1}
                                 for n in range(params.n)])]


@task()
def leave_part(element):
    """A task that leaves a part of one shard of 4 records in the folder of its key (one of ten)."""
    import uuid

    import pyarrow as pa

    from ms_flow.core.data.shard import write_table

    time.sleep(element["nap"])
    n = element["n"]
    parts = Path(element["out"], f"k{n // 10}", "parts")
    parts.mkdir(parents=True, exist_ok=True)
    path = parts / f"{n}-{uuid.uuid4().hex[:8]}.arrows"
    write_table(path, pa.table({"x": list(range(4 * n, 4 * n + 4))}), base_id=4 * n)
    return {"n": n, "part": str(path), "host": os.uname().nodename}


def receipts(result):
    """What `leave_part` left, as the receipt committed there into its key's container."""
    folder = Path(result["part"]).parent.parent
    return [{"part": f"p:{result['n']}", "path": result["part"], "role": folder.name, "generation_id": None,
             "folder": str(folder)}]


@campaign(params_model=Params, output=lambda params, project: _Collect(), volatile=("run_id",))
def committing(params, project):
    said = Path(os.environ["CAMPAIGN_FILE"])
    return [Stage("vs", leave_part, [{"n": n, "nap": params.nap, "out": Dir(said.parent / "made"), "_cpu_required": 1}
                                     for n in range(params.n)], commit=receipts)]
