"""Derived shards (M5): a step's output per task is a part, appended to the containers of its role
as the results arrive; same ids as its source, the catalog row is its state, no merge pass."""
from pathlib import Path
from uuid import uuid4

from ms_flow.core.data import catalog, container
from ms_flow.core.data.shard import Serializer, Shard, ShardWriter, file_groups, read_dropped
from ms_flow.core.database import ProjectStore
from ms_flow.engine import Engine, Flow, ThreadExecutor, job, task
from ms_flow.sinks import catalog_sink

ROLE = "derived:upper"


def _project(tmp_path):
    db = ProjectStore()
    db.connect(tmp_path / "project")
    return db


def _source(db, folder, n_files=3, per_file=2):
    """``n_files`` source files of ``per_file`` 4-id shards each, ids 0..; one generation, active."""
    gen = catalog.create_generation(db, step="load")
    for f in range(n_files):
        base = 4 * per_file * f
        path = folder / f"src{f}.arrows"
        with ShardWriter(path, dataset_id=None, shard_id=0, base_id=base, kind=0, serializer=Serializer.UTF8,
                         slot_count=1 << 20) as writer:
            for lo in range(base, base + 4 * per_file, 4):
                for id_ in range(lo, lo + 4):
                    writer.add(id_, f"r{id_}".encode())
                writer.end_group(scope=1, base_id=lo, slot_count=4)
        catalog.add_file(db, path, generation_id=gen)
    catalog.activate_generation(db, gen)
    return gen


@task(thread=True)
def upper(element):
    """One task: its source shards into one part; id 5 cannot be made."""
    if element["shards"][0]["base_id"] in element["fail"]:
        raise RuntimeError("this task dies")
    out = element["out"]  # the writer publishes it by atomic rename at close
    with ShardWriter(out, dataset_id=None, shard_id=0, base_id=element["shards"][0]["base_id"], kind=0,
                     serializer=Serializer.UTF8, slot_count=1 << 20) as writer:
        for shard in element["shards"]:
            with Shard.open(shard["path"], shard=shard["shard_index"]) as src:
                for record in src:
                    if record.id == 5:
                        writer.drop(5, "no upper case")
                    else:
                        writer.add(record.id, (record.load().upper() + element["tag"]).encode())
            writer.end_group(scope=shard["scope"], base_id=shard["base_id"], slot_count=shard["slot_count"],
                             origin=shard["id"])
    receipt = catalog.file_row(out, role=ROLE, generation_id=element["gen"], part=element["part"])
    if element["shards"][0]["base_id"] in element["garbage"]:  # damaged after the fact: found while copying
        info, rows = container.read_index(out)
        container._save_index(out, info, [{**row, "crc": row["crc"] ^ 1} for row in rows])
    return receipt


@job(task=upper, output=catalog_sink())
def derive(params, project):
    catalog.sweep(project, params["folder"])
    todo = list(catalog.underived(project, ROLE)) if not params.get("force") else \
        [s for s in catalog.shards(project, whole=True) if s["base_id"] in params["force"]]
    size, run = params.get("size", 2), uuid4().hex[:8]  # a part id per submission: a redo is not a replay
    return [{"shards": todo[i:i + size], "out": f"{params['folder']}/t{run}_{i}.arrows", "gen": params["gen"],
             "part": f"{run}:{i}", "tag": params.get("tag", ""), "fail": params.get("fail", ()),
             "garbage": params.get("garbage", ())}
            for i in range(0, len(todo), size)]


def _run(db, params, status="completed"):
    with Engine(cpus=2, mem_mb=0) as engine:
        engine.register("thread", ThreadExecutor(2))
        ended = engine.wait(Flow(engine, db).submit(derive, params, executor="thread"), 120)
        assert ended.status == status, ended.error
        return ended


def _made(db):
    return list(catalog.files(db, catalog.shard_spec(role=ROLE)))


def test_a_derive_job_appends_its_parts_to_one_container(tmp_path):
    db = _project(tmp_path)
    try:
        gen = _source(db, tmp_path)
        out = tmp_path / "upper"
        out.mkdir()
        (out / "orphan.arrows").write_bytes(b"published, never registered")
        assert len(list(catalog.underived(db, ROLE))) == 6
        _run(db, {"gen": gen, "folder": str(out)})

        assert not (out / "orphan.arrows").exists()  # swept at the job's start
        made = _made(db)
        assert len(made) == 1 and made[0]["n_shards"] == 6 and made[0]["n_records"] == 23
        assert list(catalog.underived(db, ROLE)) == []
        assert sum(g.n_dropped for g in file_groups(made[0]["path"])) == 1
        assert read_dropped(made[0]["path"]) == [(5, "no upper case")]
        derived = list(catalog.derived(db, ROLE))
        assert [d["origin"] for d in derived] == [s["id"] for s in catalog.shards(db)]
        with Shard.open(derived[0]["path"], shard=derived[0]["shard_index"]) as shard:
            assert [r.load() for r in shard] == ["R0", "R1", "R2", "R3"]
        assert not list(out.glob("t*.arrows*"))  # parts go once committed, not at the job's end

        # The source generation goes: its derived files go with it, lock file included.
        catalog.activate_generation(db, catalog.create_generation(db, step="reload"))
        catalog.activate_generation(db, catalog.create_generation(db, step="again"))
        assert list(out.iterdir()) == []
    finally:
        db.dispose()


def test_a_redo_is_read_newest(tmp_path):
    db = _project(tmp_path)
    try:
        gen = _source(db, tmp_path)
        out = tmp_path / "upper"
        _run(db, {"gen": gen, "folder": str(out)})
        _run(db, {"gen": gen, "folder": str(out), "force": [8], "size": 1, "tag": "!"})

        made = _made(db)
        assert len(made) == 1 and made[0]["n_shards"] == 7  # appended, not merged
        derived = {d["base_id"]: d for d in catalog.derived(db, ROLE)}
        assert len(derived) == 6
        with Shard.open(derived[8]["path"], shard=derived[8]["shard_index"]) as shard:
            assert [r.load() for r in shard] == ["R8!", "R9!", "R10!", "R11!"]
        with Shard.open(derived[12]["path"], shard=derived[12]["shard_index"]) as shard:
            assert [r.load() for r in shard] == ["R12", "R13", "R14", "R15"]
    finally:
        db.dispose()


def test_derive_killed_halfway_resumes_without_duplicates(tmp_path):
    """A task that died and a part that cannot be read: the job fails, what was made stays, and
    the next run makes only what is missing."""
    db = _project(tmp_path)
    try:
        gen = _source(db, tmp_path)
        out = tmp_path / "upper"
        ended = _run(db, {"gen": gen, "folder": str(out), "size": 1, "fail": [8], "garbage": [16]}, "failed")
        assert "1 part(s) not committed" in ended.error and "checksum" in ended.error
        assert sorted(s["base_id"] for s in catalog.underived(db, ROLE)) == [8, 16]
        _run(db, {"gen": gen, "folder": str(out), "size": 1})

        made = _made(db)
        assert len(made) == 1 and made[0]["n_shards"] == 6
        origins = [g.origin for g in file_groups(made[0]["path"])]
        assert sorted(origins) == sorted(s["id"] for s in catalog.shards(db))  # each once
        assert list(catalog.underived(db, ROLE)) == []
    finally:
        db.dispose()


def test_derived_files_do_not_grow_with_the_number_of_tasks(tmp_path, monkeypatch):
    monkeypatch.setattr(catalog, "MAX_SHARDS", 64)
    db = _project(tmp_path)
    try:
        gen = _source(db, tmp_path, n_files=4, per_file=50)
        out = tmp_path / "upper"
        _run(db, {"gen": gen, "folder": str(out), "size": 1})  # 200 tasks, one shard each

        made = _made(db)
        assert len(made) == 4  # ceil(200 / 64)
        assert sum(row["n_shards"] for row in made) == 200
        assert sorted(row["status"] for row in made).count(catalog.SEALED) == 3
        assert list(catalog.underived(db, ROLE)) == []
        assert sorted(p.name for p in out.iterdir() if not p.name.endswith(".lock")) == sorted(
            name for row in made for name in (Path(row["path"]).name, Path(row["path"]).name + ".index"))
    finally:
        db.dispose()


def test_parts_committed_out_of_id_order_open_whole(tmp_path):
    """Parts land in arrival order; a container whose shards do not overlap still opens whole."""
    db = _project(tmp_path)
    try:
        gen = catalog.create_generation(db, step="load")
        receipts = []
        for base in (8, 0, 4):
            path = tmp_path / f"p{base}.arrows"
            with ShardWriter(path, dataset_id=None, shard_id=0, base_id=base, kind=0, serializer=Serializer.UTF8,
                             slot_count=1 << 20) as writer:
                for id_ in range(base, base + 4):
                    writer.add(id_, f"r{id_}".encode())
                writer.end_group(scope=1, base_id=base, slot_count=4)
            receipts.append(catalog.file_row(path, role=ROLE, generation_id=gen, part=f"x:{base}"))
        with catalog.ContainerWriter(db, tmp_path / "out", generation_id=gen, role=ROLE) as writer:
            writer.commit_parts(receipts)
        (path,) = [row["path"] for row in _made(db)]
        with Shard.open(path) as shard:
            assert [r.id for r in shard] == list(range(12)) and shard[6].load() == "r6"
        with Shard.open(path, record_ids=[9, 1]) as shard:
            assert [r.load() for r in shard] == ["r1", "r9"]
    finally:
        db.dispose()
