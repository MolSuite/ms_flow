"""What a job of a cluster's queue asks for, written as the queue reads it, and read back from a
script of the site.

A site documents its queue with a script: its `#SBATCH` lines, the `module` commands that give
the programs, a few `export` or `source` lines. `read` takes such a script apart into the fields
of a queue executor (see `ms_flow.core.settings.ExecutorConfig`), and `directives` writes those
fields back as the first lines of the script ms_flow sends (its `@setup@`).
"""
from __future__ import annotations

import math
import re
import shlex

# The fields asked for, with what each is to Slurm. 0 or "" asks nothing: the queue's default.
RESOURCES = ("partition", "account", "nodes", "cpus", "gpus", "memory_gb", "time")

_OPTIONS = {"-p": "partition", "--partition": "partition", "-A": "account", "--account": "account",
            "-N": "nodes", "--nodes": "nodes", "-c": "cpus", "--cpus-per-task": "cpus", "-t": "time",
            "--time": "time", "--mem": "memory_gb", "--gres": "gpus", "--gpus": "gpus", "--gpus-per-node": "gpus",
            "-G": "gpus"}
# What ms_flow decides itself: one task on each node, run in the job's directory, whose output
# goes where ms_flow reads it (slurm-<id>.out). A site's own value would break that.
_OURS = {"-n", "--ntasks", "--ntasks-per-node", "-o", "--output", "-e", "--error", "-D", "--chdir"}
# The lines of a site's script that prepare the environment; any other runs its program.
_PREPARES = re.compile(r"(export|source|\.|conda|mamba|micromamba|ulimit|umask|set|unset)\s")


def _memory_gb(value: str) -> int:
    number, unit = re.fullmatch(r"(\d+(?:\.\d+)?)([KMGT]?)B?", value.upper()).groups()
    # down: asking for more than a node has keeps the job waiting for ever
    return max(1, math.floor(float(number) * {"K": 1 / 1024 ** 2, "": 1 / 1024, "M": 1 / 1024, "G": 1, "T": 1024}[unit]))


def _value(field: str, value: str):
    if field == "memory_gb":
        return _memory_gb(value)
    if field == "gpus":  # gpu:2 or the number alone; one of a type (gpu:a100:2) stays as the site wrote it
        if not re.fullmatch(r"(gpu:)?\d+", value):
            raise ValueError(value)
        return int(value.rsplit(":", 1)[-1])
    return int(value) if field in ("nodes", "cpus") else value


def read(text: str) -> dict:
    """The fields of a queue executor in a Slurm script of a site: the resources (`RESOURCES`),
    `modules` (its `module` commands, in order, without the word: `purge`, `load gcc/12`) and
    `setup` (the other `#SBATCH` lines, then the lines that prepare the environment). `dropped`
    lists the lines left out: the ones that run the site's own program, and the options ms_flow
    sets itself."""
    found: dict = {"partition": "", "account": "", "nodes": 0, "cpus": 0, "gpus": 0, "memory_gb": 0, "time": "",
                   "modules": [], "setup": "", "dropped": []}
    asked, prepares = [], []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("#SBATCH"):
            words = shlex.split(line[len("#SBATCH"):].split(" #", 1)[0])
            option, _, value = (words[0].partition("=") if words else ("", "", ""))
            value = value or " ".join(words[1:])
            if option in _OURS:
                found["dropped"].append(line)
            elif option in _OPTIONS:
                try:
                    found[_OPTIONS[option]] = _value(_OPTIONS[option], value)
                except (ValueError, AttributeError):  # one that does not say a number: the queue reads it as it is
                    asked.append(line)
            else:
                asked.append(line)
        elif not line or line.startswith("#"):
            continue
        elif re.match(r"(module|ml)\s", line):
            command = line.split(None, 1)[1]
            found["modules"].append(command if line.startswith("module") or command.split()[0] in
                                    ("purge", "load", "unload", "use", "swap") else f"load {command}")
        elif _PREPARES.match(line + " "):
            prepares.append(line)
        else:
            found["dropped"].append(line)
    found["setup"] = "\n".join([*asked, *prepares])
    return found


def directives(scheduler: str, *, partition: str = "", account: str = "", nodes: int = 0, cpus: int = 0,
               gpus: int = 0, memory_gb: int = 0, time: str = "", modules=(), setup: str = "") -> str:
    """The first lines of a job's script: what it asks of the queue, then the `#SBATCH` lines of
    `setup`, then its `modules`, then the rest of `setup`. A queue reads its directives only above
    the first command."""
    own = setup.splitlines()
    mark = "#$" if scheduler == "sge" else "#SBATCH"
    lines = []
    if scheduler == "slurm":  # ponytail: Slurm's resources only; a Grid Engine site writes its own in `setup`
        asked = [("partition", partition), ("account", account), ("nodes", nodes), ("ntasks-per-node", nodes and 1),
                 ("cpus-per-task", cpus), ("gres", gpus and f"gpu:{gpus}"), ("mem", memory_gb and f"{memory_gb}G"),
                 ("time", time)]
        lines = [f"#SBATCH --{option}={value}" for option, value in asked if value]
    return "\n".join([*lines, *(line for line in own if line.startswith(mark)),
                      *(f"module {command}" for command in modules),
                      *(line for line in own if not line.startswith(mark))])


__all__ = ["RESOURCES", "directives", "read"]
