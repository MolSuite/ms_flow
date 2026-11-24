"""Derived files (phase 3): a step's output per task, same ids as its source, the catalog row is its state."""
from pathlib import Path

from ms_flow.core.data import catalog
from ms_flow.core.data.shard import Serializer, Shard, ShardWriter, file_groups, read_dropped
from ms_flow.core.database import ProjectStore
from ms_flow.engine import Engine, Flow, ThreadExecutor, job, task
from ms_flow.sinks import catalog_sink

ROLE = "derived:upper"


def _project(tmp_path):
    db = ProjectStore()
    db.connect(tmp_path / "project")
    return db


def _source(db, folder, n_files=3):
    """``n_files`` source files of two 4-id shards each, ids 0..; one generation, active."""
    gen = catalog.create_generation(db, step="load")
    for f in range(n_files):
        base = 8 * f
        path = folder / f"src{f}.arrows"
        with ShardWriter(path, dataset_id=None, shard_id=0, base_id=base, kind=0, serializer=Serializer.UTF8,
                         slot_count=1 << 20) as writer:
            for lo in (base, base + 4):
                for id_ in range(lo, lo + 4):
                    writer.add(id_, f"r{id_}".encode())
                writer.end_group(scope=1, base_id=lo, slot_count=4)
        catalog.add_file(db, path, generation_id=gen)
    catalog.activate_generation(db, gen)
    return gen


@task(thread=True)
def upper(element):
    """One task: its source shards into one derived file; id 5 cannot be made."""
    out = element["out"]  # the writer publishes it by atomic rename at close
    with ShardWriter(out, dataset_id=None, shard_id=0, base_id=element["shards"][0]["base_id"], kind=0,
                     serializer=Serializer.UTF8, slot_count=1 << 20) as writer:
        for shard in element["shards"]:
            with Shard.open(shard["path"], shard=shard["shard_index"]) as src:
                for record in src:
                    if record.id == 5:
                        writer.drop(5, "no upper case")
                    else:
                        writer.add(record.id, record.load().upper().encode())
            writer.end_group(scope=shard["scope"], base_id=shard["base_id"], slot_count=shard["slot_count"],
                             origin=shard["id"])
    return catalog.file_row(out, role=ROLE, generation_id=element["gen"])


def _compact(params, project, snapshot):
    catalog.compact(project, params["gen"], role=ROLE)


@job(task=upper, output=catalog_sink(), finalize=_compact)
def derive(params, project):
    catalog.sweep(project, params["folder"])
    todo = list(catalog.underived(project, ROLE)) if not params.get("force") else \
        [s for s in catalog.shards(project, whole=True) if s["base_id"] in params["force"]]
    return [{"shards": todo[i:i + 2], "out": f"{params['folder']}/t{params['run']}_{i}.arrows", "gen": params["gen"]}
            for i in range(0, len(todo), 2)]


def _run(db, params):
    with Engine(cpus=2, mem_mb=0) as engine:
        engine.register("thread", ThreadExecutor(2))
        ended = engine.wait(Flow(engine, db).submit(derive, params, executor="thread"), 60)
        assert ended.status == "completed", ended.error


def test_a_derive_job_registers_per_task_files_compacts_them_and_a_redo_shadows_the_old_copy(tmp_path):
    db = _project(tmp_path)
    try:
        gen = _source(db, tmp_path)
        out = tmp_path / "upper"
        out.mkdir()
        (out / "orphan.arrows").write_bytes(b"published, never registered")
        assert len(list(catalog.underived(db, ROLE))) == 6
        _run(db, {"gen": gen, "folder": str(out), "run": 0})

        assert not (out / "orphan.arrows").exists()  # swept at the job's start
        made = list(catalog.files(db, catalog.shard_spec(role=ROLE)))
        assert len(made) == 1 and made[0]["n_shards"] == 6 and made[0]["n_records"] == 23
        assert list(catalog.underived(db, ROLE)) == []
        assert sum(g.n_dropped for g in file_groups(made[0]["path"])) == 1
        assert read_dropped(made[0]["path"]) == [(5, "no upper case")]
        derived = list(catalog.derived(db, ROLE))
        assert [d["origin"] for d in derived] == [s["id"] for s in catalog.shards(db)]
        with Shard.open(derived[0]["path"], shard=derived[0]["shard_index"]) as shard:
            assert [r.load() for r in shard] == ["R0", "R1", "R2", "R3"]

        # A forced redo of the shard at 8: its newest copy is the one read, and compaction keeps one.
        _run(db, {"gen": gen, "folder": str(out), "run": 1, "force": [8]})
        made = list(catalog.files(db, catalog.shard_spec(role=ROLE)))
        assert len(made) == 1 and made[0]["n_shards"] == 6
        assert sorted(p.name for p in out.iterdir()) == sorted(
            [Path(made[0]["path"]).name, Path(made[0]["path"]).name + ".index"])
        assert len(list(catalog.derived(db, ROLE))) == 6

        # The source generation goes: its derived files go with it.
        catalog.activate_generation(db, catalog.create_generation(db, step="reload"))
        catalog.activate_generation(db, catalog.create_generation(db, step="again"))
        assert list(out.iterdir()) == []
    finally:
        db.dispose()

