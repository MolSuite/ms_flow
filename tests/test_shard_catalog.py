"""The shard catalog: a row per file, its shards read from the file, generations (window of
two), reads already narrowed."""
from pathlib import Path

from ms_flow.core.data import catalog
from ms_flow.core.data.exclusions import exclude, set_exclusion_active
from ms_flow.core.data.shard import PayloadKind, Serializer, Shard, ShardWriter
from ms_flow.core.database import ProjectStore


def _project(tmp_path):
    db = ProjectStore()
    db.connect(tmp_path / "project")
    return db


def _file(path, groups, *, step=""):
    """A file of shards: ``groups`` = [(scope, base_id, ids...)]."""
    first = min(base for _scope, base, *_ids in groups)
    with ShardWriter(path, dataset_id=None, shard_id=0, base_id=first, kind=PayloadKind.SDF,
                     serializer=Serializer.UTF8, slot_count=1 << 20, metadata={"step": step}) as writer:
        for scope, base, *ids in groups:
            for id_ in ids:
                writer.add(id_, f"r{scope}.{id_}".encode())
            writer.end_group(scope=scope, base_id=base, slot_count=4)
    return path


def test_one_row_per_file_its_shards_read_from_it_pinned_and_narrowed(tmp_path):
    db = _project(tmp_path)
    try:
        first = catalog.create_generation(db, step="import")
        catalog.activate_generation(db, first)
        lib = _file(tmp_path / "lib.parquet", [(7, 4 * i, *range(4 * i, 4 * i + 4)) for i in range(3)])
        file_id = catalog.add_file(db, lib, generation_id=first)
        (row,) = catalog.files(db)
        assert (row["n_shards"], row["n_records"], row["n_active"], row["scopes"]) == (3, 12, 12, "7")
        assert (catalog.count(db), catalog.record_count(db)) == (3, 12)

        exclude(db, {7: [0, 1, 2, 3, 5]})
        assert next(catalog.files(db))["n_active"] == 7  # the view's Active column, kept by the cut
        rows = list(catalog.shards(db))
        assert [r["shard_index"] for r in rows] == [1, 2] and rows[0]["record_ids"] == [4, 6, 7]
        assert rows[0]["id"] == catalog.shard_id(file_id, 1) and catalog.split_shard_id(rows[0]["id"]) == (file_id, 1)
        assert (catalog.count(db), catalog.record_count(db), catalog.count(db, whole=True)) == (2, 7, 3)
        assert catalog.count(db, catalog.shard_spec(scope=8)) == 0
        (picked,) = catalog.shards(db, catalog.shard_spec(shard_id=[rows[1]["id"]]))
        assert (picked["base_id"], picked["slot_count"]) == (8, 4)
        with Shard.open(picked["path"], ids=(8, 12)) as shard:
            assert [r.load() for r in shard] == [f"r7.{i}" for i in range(8, 12)]

        # A rewrite is invisible until activated, then it is the library.
        second = catalog.create_generation(db, step="filter")
        catalog.add_file(db, _file(tmp_path / "f.parquet", [(7, 20, 20, 21)]), generation_id=second)
        assert catalog.count(db, whole=True) == 3
        assert catalog.record_count(db, catalog.shard_spec(generation_id=second)) == 2
        catalog.activate_generation(db, second)
        assert catalog.active_generation_id(db) == second and catalog.count(db, whole=True) == 1
    finally:
        db.disconnect()


def test_a_file_being_written_is_listed_but_never_read(tmp_path):
    db = _project(tmp_path)
    try:
        gen = catalog.create_generation(db, step="import")
        catalog.activate_generation(db, gen)
        path = tmp_path / "lib.parquet"
        file_id = catalog.add_file(db, path, generation_id=gen, status=catalog.WRITING)
        catalog.update_file(db, file_id, n_shards=1, n_records=4)
        assert [(r["status"], r["n_records"]) for r in catalog.files(db)] == [("writing", 4)]
        assert catalog.count(db, whole=True) == 0 and list(catalog.shards(db)) == []

        _file(path, [(1, 0, 0, 1, 2, 3), (1, 4, 4, 5)])
        catalog.finish_file(db, file_id)
        assert [(r["status"], r["n_shards"], r["n_records"]) for r in catalog.files(db)] == [("ready", 2, 6)]
        assert catalog.count(db) == 2
    finally:
        db.disconnect()


def test_undoing_a_cut_gives_the_records_back_to_the_active_count(tmp_path):
    db = _project(tmp_path)
    try:
        gen = catalog.create_generation(db, step="import")
        catalog.activate_generation(db, gen)
        catalog.add_file(db, _file(tmp_path / "lib.parquet", [(1, 0, 0, 1, 2, 3)]), generation_id=gen)
        cut = exclude(db, {1: [0, 1]})
        assert catalog.record_count(db) == 2
        set_exclusion_active(db, cut, False)
        assert catalog.record_count(db) == 4 and next(catalog.files(db))["n_active"] == 4
    finally:
        db.disconnect()


def test_activating_forgets_the_grandparent_and_its_files(tmp_path):
    db = _project(tmp_path)
    try:
        files = []
        for step in range(3):
            gen = catalog.create_generation(db, step=f"s{step}")
            files.append(_file(tmp_path / f"g{step}.parquet", [(1, 0, 0)]))
            catalog.add_file(db, files[-1], generation_id=gen)
            catalog.activate_generation(db, gen)
        assert [p.exists() for p in files] == [False, True, True]
        assert catalog.count(db, catalog.shard_spec(generation_id=gen - 1), whole=True) == 1
        assert catalog.count(db, catalog.shard_spec(generation_id=gen - 2), whole=True) == 0
    finally:
        db.disconnect()


def test_a_generation_the_app_still_references_is_kept_and_on_drop_clears_the_rest(tmp_path):
    db = _project(tmp_path)
    try:
        with db.get_session() as session:
            session.connection().exec_driver_sql(
                "CREATE TABLE receipts (file_id INTEGER REFERENCES ms_shard_files(id))")
            session.commit()
        files, dropped = [], []
        for step in range(4):
            gen = catalog.create_generation(db, step=f"s{step}")
            files.append(_file(tmp_path / f"g{step}.parquet", [(1, 0, 0)]))
            file_id = catalog.add_file(db, files[-1], generation_id=gen)
            if step == 0:  # something computed on the first generation: it must survive
                with db.get_session() as session:
                    session.connection().exec_driver_sql(f"INSERT INTO receipts VALUES ({file_id})")
                    session.commit()
            twin = tmp_path / f"twin{step}"
            twin.write_bytes(b"x")

            def on_drop(_session, rows):
                dropped.extend(row.path for row in rows)
                return [tmp_path / f"twin{int(Path(rows[0].path).stem[1:])}"]

            catalog.activate_generation(db, gen, on_drop=on_drop)
        # g0 is pinned by its receipt, g1 went (with its twin), g2 + g3 are the window.
        assert [p.exists() for p in files] == [True, False, True, True]
        assert dropped.count(str(files[1])) == 1 and not (tmp_path / "twin1").exists()
        assert (tmp_path / "twin0").exists()
        assert catalog.count(db, catalog.shard_spec(generation_id=1), whole=True) == 1
        assert catalog.active_generation_id(db) == 4
    finally:
        db.disconnect()


def test_compact_merges_a_generations_files_scopes_together_ids_never(tmp_path):
    db = _project(tmp_path)
    try:
        gen = catalog.create_generation(db, step="chem")
        paths = []
        for scope, base in ((1, 0), (1, 4), (2, 0), (3, 8)):
            paths.append(_file(tmp_path / f"s{scope}_{base}.parquet", [(scope, base, base, base + 2)], step="chem"))
        for path in reversed(paths):  # file order is not id order
            catalog.add_file(db, path, generation_id=gen)

        # Scope 3 (ids 8-11) joins scope 1 in one file; scope 2 reuses ids 0-3, so it stays apart.
        assert catalog.compact(db, gen) == 3
        assert len(list(catalog.files(db, catalog.shard_spec(generation_id=gen)))) == 2
        got = sorted(catalog.shards(db, catalog.shard_spec(generation_id=gen), whole=True),
                     key=lambda r: (r["scope"], r["base_id"]))
        assert [(r["scope"], r["base_id"], r["slot_count"], r["n_records"]) for r in got] == [
            (1, 0, 4, 2), (1, 4, 4, 2), (2, 0, 4, 2), (3, 8, 4, 2)]
        assert got[0]["path"] == got[1]["path"] == got[3]["path"] and not paths[0].exists()
        with Shard.open(got[1]["path"], ids=(4, 8)) as shard:
            assert [r.load() for r in shard] == ["r1.4", "r1.6"] and shard.metadata["step"] == "chem"
        assert catalog.compact(db, gen) == 0
    finally:
        db.disconnect()
