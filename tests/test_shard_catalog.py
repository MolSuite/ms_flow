"""The shard catalog: generations (window of two), registered shards, reads already narrowed."""
from pathlib import Path

from ms_flow.core.data import catalog
from ms_flow.core.data.exclusions import exclude
from ms_flow.core.database import ProjectStore


def _project(tmp_path):
    db = ProjectStore()
    db.connect(tmp_path / "project")
    return db


def test_reads_are_pinned_to_the_active_generation_and_narrowed(tmp_path):
    db = _project(tmp_path)
    try:
        first = catalog.create_generation(db, step="import")
        catalog.activate_generation(db, first)
        catalog.add_shards(db, [
            {"scope": 7, "shard_index": i, "path": "lib.parquet", "base_id": 4 * i, "slot_count": 4, "n_records": 4}
            for i in range(3)
        ], generation_id=first)
        assert (catalog.count(db), catalog.record_count(db)) == (3, 12)

        exclude(db, {7: [0, 1, 2, 3, 5]})
        rows = list(catalog.shards(db))
        assert [r["shard_index"] for r in rows] == [1, 2] and rows[0]["record_ids"] == [4, 6, 7]
        assert (catalog.count(db), catalog.record_count(db), catalog.count(db, whole=True)) == (2, 7, 3)
        assert catalog.count(db, catalog.shard_spec(scope=8)) == 0

        # A rewrite is invisible until activated, then it is the library.
        second = catalog.create_generation(db, step="filter")
        catalog.add_shards(db, [{"scope": 7, "shard_index": 0, "path": "f.parquet", "base_id": 20, "slot_count": 2,
                             "n_records": 2}],
                           generation_id=second)
        assert catalog.count(db, whole=True) == 3
        assert catalog.record_count(db, catalog.shard_spec(generation_id=second)) == 2
        catalog.activate_generation(db, second)
        assert catalog.active_generation_id(db) == second and catalog.count(db, whole=True) == 1
    finally:
        db.disconnect()


def test_activating_forgets_the_grandparent_and_its_files(tmp_path):
    db = _project(tmp_path)
    try:
        files = []
        for step in range(3):
            gen = catalog.create_generation(db, step=f"s{step}")
            path = tmp_path / f"g{step}.parquet"
            path.write_bytes(b"x")
            files.append(path)
            catalog.add_shards(db, [{"scope": 1, "path": str(path), "n_records": 1}], generation_id=gen)
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
                "CREATE TABLE receipts (shard_id INTEGER REFERENCES ms_shards(id))")
            session.commit()
        files, dropped = [], []
        for step in range(4):
            gen = catalog.create_generation(db, step=f"s{step}")
            path = tmp_path / f"g{step}.parquet"
            path.write_bytes(b"x")
            files.append(path)
            catalog.add_shards(db, [{"scope": 1, "path": str(path), "n_records": 1}], generation_id=gen)
            if step == 0:  # something computed on the first generation: it must survive
                (shard,) = catalog.shards(db, catalog.shard_spec(generation_id=gen), whole=True)
                with db.get_session() as session:
                    session.connection().exec_driver_sql(f"INSERT INTO receipts VALUES ({shard['id']})")
                    session.commit()
            twin = tmp_path / f"twin{step}"
            twin.write_bytes(b"x")

            def on_drop(_session, shards):
                dropped.extend(shard.path for shard in shards)
                return [tmp_path / f"twin{int(Path(shards[0].path).stem[1:])}"]

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
    from ms_flow.core.data.shard import PayloadKind, Serializer, Shard, ShardWriter

    db = _project(tmp_path)
    try:
        gen = catalog.create_generation(db, step="chem")
        rows = []
        for scope, base in ((1, 0), (1, 4), (2, 0), (3, 8)):
            path = tmp_path / f"s{scope}_{base}.mshard"
            with ShardWriter(path, dataset_id=None, shard_id=0, base_id=base, kind=PayloadKind.SDF,
                             serializer=Serializer.UTF8, slot_count=4, metadata={"step": "chem"}) as writer:
                for id_ in (base, base + 2):  # holes stay holes
                    writer.add(id_, f"r{scope}.{id_}".encode())
            rows.append({"scope": scope, "shard_index": len(rows), "path": str(path), "n_records": 2})
        catalog.add_shards(db, list(reversed(rows)), generation_id=gen)  # file order is not id order

        # Scope 3 (ids 8-11) joins scope 1 in one file; scope 2 reuses ids 0-3, so it stays apart.
        assert catalog.compact(db, gen) == 3
        got = sorted(catalog.shards(db, catalog.shard_spec(generation_id=gen), whole=True),
                     key=lambda r: (r["scope"], r["base_id"] or 0))
        assert [(r["scope"], r["base_id"], r["slot_count"]) for r in got] == [
            (1, 0, 3), (1, 4, 3), (2, None, None), (3, 8, 3)]
        assert got[0]["path"] == got[1]["path"] == got[3]["path"] and not Path(rows[0]["path"]).exists()
        with Shard.open(got[1]["path"], ids=(4, 7)) as shard:
            assert [r.load() for r in shard] == ["r1.4", "r1.6"] and shard.metadata["step"] == "chem"
        assert catalog.compact(db, gen) == 0
    finally:
        db.disconnect()
