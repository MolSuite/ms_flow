"""The shard catalog: generations (window of two), registered shards, reads already narrowed."""
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
