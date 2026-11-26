"""The far side of a remote executor: run the elements a directory holds and leave there what they gave.

    python -m ms_flow.runner <dir> [--ray ADDRESS [--nodes N]] [--cpus N] [--gpus N]

`<dir>` is what `stage` wrote: `spec.json` (the task, by the name of its module and its own, its
params as data, and what an element needs) and `elements.jsonl` (the payloads, a line each). The
runner leaves in it:

- `results/part-00001.jsonl`...: a line `[index, result]` each, `index` being the place of the
  element in `elements.jsonl`. A part that is there is whole and never changes.
- `progress.json`: `{total, done, failed, running, status, error, home, at}`.
- `errors.jsonl`: a line `{index, error}` for each element that failed for good.
- `journal.db`: run again on the same directory, it goes on from where it was stopped.

A `File` in an element that is not a whole path is one that came with the job, under `<dir>`,
and a `Dir` that is not a whole path is made under `<dir>` for the task to write in.

A campaign (`stage_campaign`, see `ms_flow.campaign`) has its stages in `spec.json`: they run one
after the other, each a job of its own in the journal, the first over `elements.jsonl` and each of
the others over what its `From` makes of an earlier one, made once into
`stages/<name>/elements.jsonl`. A stage leaves under `stages/<name>/` its `results/` and
`errors.jsonl` as above, `reduced.json` (what its `reduce` made of its results) and `done` once it
ended, which says how many parts and lines of errors it left. `progress.json` says which `stage`
runs and how many are done (`stages_done`), and at the end what each left (`left`).
A stage with a `commit` has the parts its tasks wrote committed here, by the runner, into
containers, each folder of them with its catalog (`catalog.db`, `_There`), before each result is written; the result then
says where they went (`_committed`), and its `done` lists those containers (`files`).

Nothing here listens or connects anywhere: whoever watches reads these files. Without `--ray`
the elements run in a process pool on this machine; with it, on the Ray cluster at that address
(`auto`: the one running on this node), which whoever wrote the job script started, once
`--nodes` nodes are in it.

Nothing that travels is a pickle: what comes back from there is read here, and a pickle read is
code run. Functions and the class of the params go by name (`module:qualname`, found by import
there), and everything else is data: dicts (whose keys are strings, numbers, booleans or None),
lists, strings, numbers, booleans, None, bytes, `File`, `Dir` and the payloads of
`ms_flow.payloads`. Anything else (an object) is refused when the job is staged, or fails its
element when a task returns it.
"""
from __future__ import annotations

import argparse
import base64
import importlib
import json
import os
import signal
import sys
import threading
import time
import traceback
from functools import partial
from pathlib import Path

from ms_flow.engine import Busy, Engine, ProcessExecutor, RayExecutor, content_key
from ms_flow.engine.engine import ran_on
from ms_flow.engine.files import Dir, File, swap
from ms_flow.engine.tokens import usable_cpus
from ms_flow.payloads import Payload, PayloadGroup, PayloadPart, _packed_group

_FORMAT = 2  # what `stage` writes; 1 was pickles
_GOING = ("waiting", "running")
_DATA = "data (dicts, lists, strings, numbers, booleans, None, bytes, File, Dir and payloads)"


def named(name: str):
    """What `module:qualname` names, imported."""
    module, _, qualname = name.partition(":")
    found = importlib.import_module(module)
    for part in qualname.split("."):
        found = getattr(found, part)
    return found


def _name(thing) -> str:
    name = f"{thing.__module__}:{thing.__qualname__}"
    try:
        same = thing.__module__ != "__main__" and named(name) is thing
    except (ImportError, AttributeError):
        same = False
    if not same:
        raise TypeError(f"{thing!r} goes by its name and is not found by it ({name}): define it at the top of a "
                        "module the other side imports")
    return name


def plain(value, where: str, refs: bool = False, _groups=None):
    """`value` as JSON holds it, `File`, `Dir`, bytes and payloads marked (a payload compressed,
    as a pickle of it is, and a group that several parts share once). With `refs`, also functions
    (a `partial` with its arguments) and pydantic models, by name: what this side writes for the
    other, never what it reads from there. Not data, a `TypeError` that says `where` it is."""
    groups = {} if _groups is None else _groups
    if value is None or type(value) in (bool, int, float, str):
        return value
    if isinstance(value, File):
        return {"$file": str.__str__(value)}
    if isinstance(value, Dir):
        return {"$dir": str.__str__(value)}
    for kind in (str, int, float):  # a StrEnum, an IntEnum, a numpy float
        if isinstance(value, kind):
            return kind(value) if kind is not str else str.__str__(value)
    if type(value) in (bytes, bytearray):
        return {"$bytes": base64.b64encode(value).decode()}
    if type(value) is Payload:
        packed = value.packed()
        return {"$payload": [plain(packed.data, where), packed.kind, packed.series, packed.codec, packed.raw_length]}
    if type(value) is PayloadPart:
        return {"$part": value.index, "of": plain(value.group, where, refs, groups)}
    if type(value) is PayloadGroup:
        if id(value) in groups:
            return {"$group": groups[id(value)][0]}
        groups[id(value)] = len(groups), value  # held: an id is not reused while this runs
        whole, sizes = value.packed()
        return {"$group": groups[id(value)][0], "payload": plain(whole, where), "sizes": sizes}
    if type(value) in (list, tuple):
        return [plain(item, f"{where}[{at}]", refs, groups) for at, item in enumerate(value)]
    if type(value) is dict:
        if all(type(key) is str for key in value):
            return {key: plain(item, f"{where}.{key}", refs, groups) for key, item in value.items()}
        odd = next((key for key in value if key is not None and type(key) not in (str, int, float, bool)), None)
        if odd is not None:
            raise TypeError(f"{where}: the key {odd!r} is not a string, a number, a boolean or None")
        # JSON would make every key a string: the pairs, then.
        return {"$items": [[key, plain(item, f"{where}[{key!r}]", refs, groups)] for key, item in value.items()]}
    if refs and hasattr(value, "model_dump"):
        return {"$model": _name(type(value)), "data": value.model_dump(mode="json")}
    if refs and isinstance(value, partial):
        return {"$call": _name(value.func), "args": plain(value.args, where, refs),
                "kwargs": plain(value.keywords, where, refs)}
    if refs and callable(value):
        return {"$call": _name(value)}
    raise TypeError(f"{where}: a {type(value).__name__} is not {_DATA}; what is not goes as a File")


def unplain(value, refs: bool = False, _groups=None):
    """What `plain` made, back. Names are followed only with `refs`: this side never imports
    what came from the other."""
    groups = {} if _groups is None else _groups
    if type(value) is list:
        return [unplain(item, refs, groups) for item in value]
    if type(value) is not dict:
        return value
    if len(value) == 1 and "$file" in value:
        return File(value["$file"])
    if len(value) == 1 and "$dir" in value:
        return Dir(value["$dir"])
    if len(value) == 1 and "$bytes" in value:
        return base64.b64decode(value["$bytes"])
    if len(value) == 1 and "$payload" in value:
        data, kind, series, codec, raw_length = value["$payload"]
        return Payload(unplain(data), kind, series, codec, None, raw_length)
    if len(value) == 1 and "$items" in value:
        return {key: unplain(item, refs, groups) for key, item in value["$items"]}
    if "$part" in value:
        return PayloadPart(unplain(value["of"], refs, groups), value["$part"])
    if "$group" in value:
        if "payload" in value:
            groups[value["$group"]] = _packed_group(unplain(value["payload"]), value["sizes"])
        return groups[value["$group"]]
    if refs and "$model" in value:
        # A None is put back as it was, unchecked: the volatile params of a campaign are None
        # whatever their type says (see `ms_flow.campaign`).
        data = value["data"]
        model = named(value["$model"]).model_validate({key: item for key, item in data.items() if item is not None})
        return model.model_copy(update={key: None for key, item in data.items() if item is None})
    if refs and "$call" in value:
        fn = named(value["$call"])
        args, kwargs = unplain(value.get("args", []), refs), unplain(value.get("kwargs", {}), refs)
        return partial(fn, *args, **kwargs) if args or kwargs else fn
    return {key: unplain(item, refs, groups) for key, item in value.items()}


def _line(value, where: str) -> str:
    return json.dumps(plain(value, where), sort_keys=True, separators=(",", ":")) + "\n"


def stage(home, fn, payloads, *, batched: bool = False, part_seconds: float = 30, **needs) -> int:
    """Write in `home` the job the runner runs there: `fn` over `payloads`. Returns how many.

    `needs` is what `Engine.submit` takes about the chunks: `cpu`, `gpu`, `mem_mb`, `retries`,
    `retry_wait`, `batch`. A part of results is closed `part_seconds` after its first result.
    """
    home = Path(home)
    home.mkdir(parents=True, exist_ok=True)
    call = plain(fn, "the task", refs=True)
    total = _elements(home / "elements.jsonl", payloads)
    spec = {"format": _FORMAT, "fn": call["$call"], "call": call, "total": total, "batched": batched,
            "part_seconds": part_seconds, "submit": needs}
    (home / "spec.json").write_text(json.dumps(spec, sort_keys=True))
    return total


def stage_campaign(home, stages: list[dict], payloads, *, part_seconds: float = 30) -> int:
    """Write in `home` a campaign: `stages` run one after the other, the first over `payloads`.
    Returns how many those are.

    A stage is `{name, fn, needs, reduce, from}`: `needs` what `Engine.submit` takes about its
    chunks, `reduce` None or a function of the iterator of its results, `from` None for the first
    and `(stage, fn)` for the others, `fn` taking what that earlier stage left (what its `reduce`
    made, or else the iterator of its results) and giving the elements of this one.
    """
    home = Path(home)
    home.mkdir(parents=True, exist_ok=True)
    listed = plain(stages, "the stages", refs=True)
    total = _elements(home / "elements.jsonl", payloads)
    (home / "spec.json").write_text(json.dumps({"format": _FORMAT, "fn": " > ".join(stage["name"] for stage in stages),
                                                "total": total, "part_seconds": part_seconds, "stages": listed},
                                               sort_keys=True))
    return total


def _elements(path: Path, payloads) -> int:
    """Write `payloads` to `path`, a line each, whole or not at all. Returns how many."""
    total = 0
    with open(path.with_suffix(".tmp"), "w") as file:
        for payload in payloads:
            file.write(_line(payload, f"element {total}"))
            total += 1
    os.replace(path.with_suffix(".tmp"), path)
    return total


def read_part(file) -> list:
    """The `(index, result)` pairs of a results file (opened as bytes)."""
    pairs = []
    for line in file:
        if not line.endswith(b"\n"):  # where a run that was killed stopped writing
            break
        index, result = json.loads(line)
        pairs.append((index, unplain(result)))
    return pairs


def part_number(name: str) -> int:
    """The number in the name of a part, `.../part-00012.jsonl`."""
    return int(name.rpartition("-")[2].partition(".")[0])


class _Parts:
    """Results go to an open file as they come, and to disk before anybody counts them done; the
    file gets its name, and so shows, once it is `seconds` old."""

    def __init__(self, folder: Path, seconds: float):
        self.folder, self.seconds, self.lock = folder, seconds, threading.Lock()
        folder.mkdir(exist_ok=True)
        self.open, self.file, self.since = folder / "open.tmp", None, 0.0
        self.n = len(list(folder.glob("part-*.jsonl")))
        if self.open.exists():  # a run that was killed: what it wrote is in its journal as done
            self.file = open(self.open, "ab")
            self.roll(force=True)

    def write(self, lines: list) -> None:
        with self.lock:
            if self.file is None:
                self.file, self.since = open(self.open, "wb"), time.monotonic()
            self.file.write("".join(lines).encode())
            self.file.flush()
            os.fsync(self.file.fileno())

    def roll(self, force: bool = False) -> None:
        with self.lock:
            if self.file is not None and (force or time.monotonic() - self.since >= self.seconds):
                self.file.close()
                self.file = None
                self.n += 1
                os.replace(self.open, self.folder / f"part-{self.n:05d}.jsonl")


class _There:
    """The containers the parts of a stage's tasks go into, there, as `catalog_sink` does here: one
    writer per (folder, generation, roles), each folder of containers with a catalog of its own
    (`catalog.db` in it, the same ids of generations as the project's), so a folder a campaign
    keeps for the next ones (a `KeptDir`) is found by them as it was left. The runner is the one
    process of its job that appends, and a part committed once is not twice (I10): what a task
    that ran again left goes, so a folder holds containers, not a file per task.
    ponytail: two campaigns of one project appending to one kept folder at once: the second's
    commits fail on the writer's lock (Busy), and it goes again later."""

    def __init__(self, home: Path):
        self.home = Path(home)
        self.writers, self.dbs, self.paths, self.generations = {}, {}, {}, set()
        # The folders parts came from, and those of the containers, every run of the job: what a
        # killed task left is known by the first (`tidy`), what the job made by the second (`files`).
        self.folders = {name: self._read(name) for name in ("part_folders", "container_folders")}

    def _read(self, name: str) -> set:
        path = self.home / name
        return set(path.read_text().splitlines()) if path.exists() else set()

    def _note(self, name: str, folders: set) -> None:
        new = folders - self.folders[name]
        if new:
            self.home.mkdir(parents=True, exist_ok=True)
            with open(self.home / name, "a") as file:
                file.write("".join(f"{folder}\n" for folder in sorted(new)))
                file.flush()
                os.fsync(file.fileno())
            self.folders[name] |= new

    def _db(self, folder: str):
        if folder not in self.dbs:
            from ms_flow.core.database.project import ProjectStore

            Path(folder).mkdir(parents=True, exist_ok=True)
            self.dbs[folder] = db = ProjectStore()
            db.connect_path(Path(folder) / "catalog.db", project_dir=Path(folder))
        return self.dbs[folder]

    def commit(self, receipts: list) -> list:
        """`receipts` committed: for each, `{part, path, shards}`, the container and its shard
        numbers in it. A receipt is `{part, path, role, generation_id}` and maybe `folder` (where
        its containers are: its part's folder by default), or two parts written as one
        (`catalog.PairWriter`), `path` and `role` its index and `bulk`, `bulk_role` the other."""
        groups: dict = {}
        for at, receipt in enumerate(receipts):
            key = (str(receipt.get("folder") or Path(receipt["path"]).parent), receipt.get("generation_id"),
                   str(receipt["role"]), receipt.get("bulk_role"))
            groups.setdefault(key, []).append(at)
        self._note("part_folders", {str(Path(r[name]).parent) for r in receipts for name in ("path", "bulk")
                                    if r.get(name)})
        self._note("container_folders", {key[0] for key in groups})
        where = [None] * len(receipts)
        for key, places in groups.items():
            done = self._writer(*key).commit_parts([
                {"part": str(receipts[at]["part"]), "path": str(receipts[at]["path"]),
                 **({"bulk": str(receipts[at]["bulk"])} if key[3] else {})} for at in places])
            for at, receipt in zip(places, done):
                if "error" in receipt:
                    raise RuntimeError(f"not committed there: {receipt['error']}")
                where[at] = self._where(key[0], receipt)
        return where

    def _writer(self, folder: str, generation_id, role: str, bulk_role):
        from ms_flow.core.data import catalog

        key = (folder, generation_id, role, bulk_role)
        if key not in self.writers:
            db = self._db(folder)
            if generation_id is not None and (folder, generation_id) not in self.generations:
                from ms_flow.core.database.project_models import ShardGeneration

                with db.get_session() as session:  # the project's id, so what comes back names it
                    if session.get(ShardGeneration, int(generation_id)) is None:
                        session.add(ShardGeneration(id=int(generation_id), step="there"))
                        session.commit()
                self.generations.add((folder, generation_id))
            self.writers[key] = (
                catalog.PairWriter(db, folder, generation_id=generation_id, roles=(str(bulk_role), role))
                if bulk_role else catalog.ContainerWriter(db, folder, generation_id=generation_id, role=role))
        return self.writers[key]

    def _where(self, folder: str, receipt: dict) -> dict:
        from ms_flow.core.data.catalog import split_shard_id
        from ms_flow.core.database.project_models import CatalogFile

        def at(ids) -> dict:
            pairs = [split_shard_id(sid) for sid in ids]
            file_id = pairs[0][0] if pairs else None  # a part never spans two containers (I18)
            if file_id is not None and (folder, file_id) not in self.paths:
                with self._db(folder).get_session() as session:
                    self.paths[folder, file_id] = session.get(CatalogFile, file_id).path
            return {"path": self.paths.get((folder, file_id), ""), "shards": [n for _, n in pairs]}

        where = {"part": receipt["part"], **at(receipt["shards"])}
        if "bulk" in receipt:  # a pair: its other part's shards, named in the index shards' meta
            bulk = (receipt.get("meta") or {}).get("bulk")
            if bulk is None:  # committed before: the index says
                from ms_flow.core.data import container

                mine = set(where["shards"])
                bulk = [sid for row in container.read_index(where["path"])[1] if row["shard"] in mine
                        for sid in json.loads(row["meta"] or "{}").get("bulk", ())]
            where["bulk"] = at(bulk)
        return where

    def tidy(self) -> int:
        """Once no task runs: what parts are left in their folders, of tasks a kill cut short (theirs
        ran again under another name), goes. How many files went."""
        gone = 0
        for folder in self.folders["part_folders"]:
            for path in Path(folder).glob("*"):
                if path.is_file() and path.name.endswith((".arrows", ".index", ".tmp")):  # a write cut short too
                    path.unlink(missing_ok=True)
                    gone += 1
        return gone

    def files(self) -> list:
        """The containers of the folders the job committed to, as the rows the project registers
        them with (`catalog.add_remote`)."""
        from sqlmodel import select

        from ms_flow.core.data import catalog
        from ms_flow.core.database.project_models import CatalogFile

        rows = []
        for folder in sorted(self.folders["container_folders"]):
            with self._db(folder).get_session() as session:
                rows += session.exec(select(CatalogFile).order_by(CatalogFile.id)).all()
        return [{key: getattr(row, key) for key in ("path", "role", "generation_id", *catalog._COUNTS)} for row in rows]

    def close(self) -> None:
        for writer in self.writers.values():
            writer.close()
        self.writers.clear()
        for db in self.dbs.values():
            db.disconnect()
        self.dbs.clear()


def commit_there(home, receipts: list) -> list:
    """`receipts` committed into the containers of the job in `home`, as its runner does (`_There`)."""
    there = _There(Path(home))
    try:
        return there.commit(list(receipts))
    finally:
        there.close()


def found_there(folder, part: str) -> dict | None:
    """Where the part `part` was committed in the containers of `folder` (`{part, path, shards}`), or
    None: what a task over there asks of a folder an earlier campaign kept, so it does not make it
    again. From the containers' indexes alone."""
    from ms_flow.core.data import container

    for path in sorted(Path(folder).glob("*.arrows")):
        shards = [row["shard"] for row in container.read_index(path)[1] if row["part"] == part and not row["retired"]]
        if shards:
            return {"part": part, "path": str(path), "shards": shards}
    return None


_RAN_ON: dict = {}  # what this runner runs with, told with every progress (see `ran_on`)
_RAY_NODES = 0  # on Ray: how many nodes the job asked for, and its state is told too (`ray_state`)


def ray_state(nodes_asked: int) -> dict:
    """How the Ray this runner drives is: its nodes alive of those the job asked for, CPUs and cards
    busy of all, and its object store used, in MB. Read here, where the job runs."""
    import ray

    total, free, mb = ray.cluster_resources(), ray.available_resources(), 1 << 20

    def busy(name):
        return total.get(name, 0) - free.get(name, 0)

    return {"nodes": sum(1 for node in ray.nodes() if node["Alive"]), "nodes_asked": int(nodes_asked),
            "cpus": int(total.get("CPU", 0)), "cpus_busy": int(busy("CPU")),
            "gpus": int(total.get("GPU", 0)), "gpus_busy": int(busy("GPU")),
            "store_mb": int(total.get("object_store_memory", 0) // mb),
            "store_used_mb": int(busy("object_store_memory") // mb)}


def _tell(home: Path, **progress) -> None:
    told = {**progress, "home": str(home.resolve()), "at": time.time(), "ran_on": _RAN_ON}
    if _RAY_NODES:
        try:
            told["ray"] = ray_state(_RAY_NODES)
        except Exception:  # what it says of Ray never stops what it says of the job
            pass
    (home / "progress.tmp").write_text(json.dumps(told))
    os.replace(home / "progress.tmp", home / "progress.json")


def run(home, ray: str | None = None, cpus: int | None = None, nodes: int = 1, gpus: int | None = None) -> str:
    """Run the job staged in `home` to its end. Returns the status it ended with."""
    home = Path(home).resolve()
    spec = json.loads((home / "spec.json").read_text())
    if spec.get("format") != _FORMAT:
        raise RuntimeError(f"{home} was staged by another ms_flow (format {spec.get('format', 1)}, this one reads "
                           f"{_FORMAT}): submit the job again")
    if ray is not None and nodes > 1:
        _wait_for_nodes(ray, nodes)
    global _RAY_NODES
    _RAY_NODES = nodes if ray is not None else 0
    cpus = cpus or usable_cpus()
    if gpus is None:  # the cards the queue gave this job
        gpus = len([card for card in os.environ.get("CUDA_VISIBLE_DEVICES", "").split(",") if card.strip()])
    with Engine(cpus=cpus, gpus=gpus if ray is None else 0) as engine:
        # Every node of a cluster's allocation sees its disk: Ray carries no file.
        engine.register("run", ProcessExecutor(cpus, gpus=gpus) if ray is None else RayExecutor(ray, shared=True))
        engine.attach("journal", home / "journal.db")
        calls = [stage["fn"] for stage in spec["stages"]] if "stages" in spec else [spec["call"]]
        _RAN_ON.update(ran_on(*(str(call.get("$call", "")).partition(":")[0] for call in calls)))
        if "stages" in spec:
            return _stages(engine, home, spec)
        fn = unplain(spec["call"], refs=True)
        return _run_job(engine, home, home, fn, home / "elements.jsonl", spec["total"], "job",
                        {"batched": spec["batched"], **spec["submit"]}, spec["part_seconds"])


def _wait_for_nodes(address: str, nodes: int, seconds: float = 300) -> None:
    """Until the Ray at `address` has `nodes` nodes: the job script starts them all at once."""
    import ray

    ray.init(address=address, include_dashboard=False, log_to_driver=False)
    try:
        limit = time.monotonic() + seconds
        while sum(node["Alive"] for node in ray.nodes()) < nodes:
            if time.monotonic() > limit:
                raise RuntimeError(f"the Ray at {address} has not got its {nodes} nodes after {seconds:.0f} s")
            time.sleep(1)
    finally:
        ray.shutdown()


def _stages(engine, home: Path, spec: dict) -> str:
    stages = unplain(spec["stages"], refs=True)
    status = "completed"
    for place, step in enumerate(stages):
        folder = home / "stages" / step["name"]
        folder.mkdir(parents=True, exist_ok=True)
        if not (folder / "done").exists():
            if place == 0:
                source, total = home / "elements.jsonl", spec["total"]
            else:
                source = folder / "elements.jsonl"
                if not (folder / "elements.json").exists():
                    earlier, make, args = step["from"]
                    args = swap(args, lambda mark: type(mark)(os.path.join(home, mark)))  # sent with the campaign
                    total = _elements(source, make(_left(home / "stages" / earlier), *args))
                    (folder / "elements.json").write_text(json.dumps({"total": total}))
                total = json.loads((folder / "elements.json").read_text())["total"]
            ended = _run_job(engine, home, folder, step["fn"], source, total, f"stage:{step['name']}", step["needs"],
                             spec["part_seconds"], last=place == len(stages) - 1, commit=step.get("commit"),
                             stage=step["name"], stages_done=place)
            if not ended.startswith("completed"):
                return ended
            if step["reduce"] is not None:
                made = step["reduce"](_results(folder))
                (folder / "reduced.tmp").write_text(_line(made, f"what the reduce of {step['name']} made"))
                os.replace(folder / "reduced.tmp", folder / "reduced.json")
            # What it left, for whoever takes it to see that all of it came (and nothing was purged).
            errors = folder / "errors.jsonl"
            left = {"parts": len(list((folder / "results").glob("part-*.jsonl"))),
                    "errors": len(errors.read_text().splitlines()) if errors.exists() else 0}
            if step.get("commit") is not None:  # the containers there, for the project to register
                there = _There(home)
                try:
                    there.tidy()
                    left["files"] = there.files()
                finally:
                    there.close()
            (folder / "done").write_text(json.dumps(left, default=str))
        if (folder / "errors.jsonl").exists():
            status = "completed_with_errors"
    try:
        told = json.loads((home / "progress.json").read_text())
    except (OSError, ValueError):
        told = {}
    told.pop("home", None), told.pop("at", None)
    left = {step["name"]: json.loads((home / "stages" / step["name"] / "done").read_text()) for step in stages}
    _tell(home, **{**told, "status": status, "stage": None, "stages_done": len(stages), "running": 0, "left": left})
    return status


def _results(folder: Path):
    """The results a stage left, each once: a part a killed run wrote twice says it twice."""
    seen = set()
    for part in sorted((folder / "results").glob("part-*.jsonl")):
        with open(part, "rb") as file:
            for index, result in read_part(file):
                if index not in seen:
                    seen.add(index)
                    yield result


def _left(folder: Path):
    """What a stage that ended left for the next: what its `reduce` made, or its results."""
    if (folder / "reduced.json").exists():
        return unplain(json.loads((folder / "reduced.json").read_text()))
    return _results(folder)


def _returns_data(fn, batched, payload):
    """What `fn` returns as `plain` makes it, there where it ran: what is not data fails its
    element, and a payload is compressed by the process that made it."""
    result = fn(payload)
    if batched:
        return [plain(one, "what the task returned") for one in result]
    return plain(result, "what the task returned")


def _run_job(engine, home: Path, folder: Path, fn, source: Path, total: int, key: str, needs: dict,
             part_seconds: float, last: bool = True, commit=None, **said) -> str:
    """Run `fn` over the elements in `source` as the job `key` of the journal, its results and
    errors to `folder`. Only the `last` job of a run says it completed: the next run would do nothing.
    `commit`: what makes the receipts of a result (see `_There`); they are committed there before
    the result is written, which then says where they went (`_committed`)."""
    index_of, parts = {}, _Parts(folder / "results", part_seconds)
    there = _There(home) if commit is not None else None

    def delivered(pairs):
        lines = []
        for key, result in pairs:
            if there is not None:
                result = {**result, "_committed": there.commit(commit(result))}
            lines.append(_line([index_of[key], result], "a result"))
        parts.write(lines)

    def here(mark):
        if isinstance(mark, Dir) and not os.path.isabs(mark):
            os.makedirs(home / mark, exist_ok=True)
        return os.path.join(home, mark)

    def elements():
        with open(source) as file:
            for index in range(total):
                # Plain paths from here on: whatever runs the task shares this disk.
                payload = swap(unplain(json.loads(file.readline())), here)
                key = content_key(payload)
                index_of[key] = index  # ponytail: equal payloads are one element, as in a journal
                # Said, not left to the engine: it keys a dict without its per-chunk costs.
                yield {**payload, "_key": key} if type(payload) is dict else payload

    def failed(key, error):
        with open(folder / "errors.jsonl", "a") as file:
            file.write(json.dumps({"index": index_of[key], "error": error}) + "\n")

    def tell(now, running):
        status = now.status if last or not now.status.startswith("completed") else "running"
        _tell(home, total=now.chunks_total, done=now.chunks_done, failed=now.chunks_failed, running=running,
              status=status, error=None if now.status in _GOING else now.error, **said)

    job = engine.submit(
        "run", partial(_returns_data, fn, needs.get("batched", False)), elements(), total=total, journal="journal",
        key=key,
        on_results=delivered,
        on_error=failed, **needs)
    try:
        while True:
            now = engine.wait(job, 2)
            going = now.status in _GOING
            parts.roll(force=not going)
            tell(now, now.chunks_inflight)
            if not going:
                return now.status
    except BaseException:  # stopped from outside: what is done shows, and the next run goes on
        engine.close()
        parts.roll(force=True)
        tell(engine.status(job), 0)
        raise
    finally:
        if there is not None:
            there.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ms_flow.runner", description=__doc__.split("\n")[0])
    parser.add_argument("dir")
    parser.add_argument("--ray", help="address of a running Ray cluster, or `auto`; without it, a process pool here")
    parser.add_argument("--nodes", type=int, default=1, help="with `--ray`: how many nodes to wait for in it")
    parser.add_argument("--cpus", type=int, help="processes of the pool; by default, the CPUs this job was given")
    parser.add_argument("--gpus", type=int, help="cards of the pool; by default, those in CUDA_VISIBLE_DEVICES")
    args = parser.parse_args(argv)
    home = Path(args.dir)
    try:
        was = json.loads((home / "progress.json").read_text())["status"]
    except (OSError, ValueError, KeyError):
        was = None
    if was is not None and was not in (*_GOING, "interrupted", "canceled"):
        print(f"nothing to do: this job ended as {was}")
        return 0
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))  # what a queue sends before it kills
    try:
        status = run(home, args.ray, args.cpus, args.nodes, args.gpus)
    except Busy as busy:  # another runner has it (two clients queued it): that one says how it goes
        print(f"nothing to do: {busy}")
        return 0
    except Exception:  # it never got to run: say so where whoever watches looks
        _tell(home, status="failed", error=traceback.format_exc())
        raise
    return 0 if status.startswith("completed") else 1


if __name__ == "__main__":
    sys.exit(main())
