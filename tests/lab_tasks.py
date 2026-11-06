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
def screen(params, project):
    said = Path(os.environ["CAMPAIGN_FILE"])
    return [Stage("vs", square, [{"n": n, "f": File(said), "out": Dir(said.parent / "work"), "_cpu_required": 1}
                                 for n in range(params.n)])]
