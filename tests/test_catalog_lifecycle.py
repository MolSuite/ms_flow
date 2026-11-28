"""M4a: the catalog's lifecycle. Ids never given twice, kept generations, sealed containers, an
index older than the catalog refused before anything is cut, and the project schema checked."""
import hashlib
import shutil
import sqlite3

import pytest

from ms_flow.core.data import catalog, container
from ms_flow.core.data.shard import PayloadKind, Serializer, ShardWriter
from ms_flow.core.database import ProjectStore


def _project(tmp_path):
    db = ProjectStore()
    db.connect(tmp_path / "project")
    return db


def _file(path, *bases):
    with ShardWriter(path, dataset_id=None, shard_id=0, base_id=min(bases), kind=PayloadKind.GENERIC_BYTES,
                     serializer=Serializer.UTF8, slot_count=1 << 20) as writer:
        for base in bases:
            for id_ in range(base, base + 4):
                writer.add(id_, f"r{id_}".encode())
            writer.end_group(scope=1, base_id=base, slot_count=4)
    return path


def _active(db):
    gen = catalog.create_generation(db, step="import")
    catalog.activate_generation(db, gen)
    return gen


# Ids (trap 5)

def test_a_deleted_files_id_is_never_given_again(tmp_path):
    db = _project(tmp_path)
    try:
        gen = _active(db)
        catalog.add_file(db, _file(tmp_path / "a.arrows", 0), generation_id=gen)
        newest = catalog.add_file(db, _file(tmp_path / "b.arrows", 0), generation_id=gen, role="made")
        assert catalog.remove(db, "made") == 1
        again = catalog.add_file(db, _file(tmp_path / "c.arrows", 0), generation_id=gen, role="made")
        assert again > newest

        older = catalog.create_generation(db, step="a")
        newer = catalog.create_generation(db, step="b")
        catalog.activate_generation(db, older)  # forgets `newer`, the newest generation
        assert catalog.create_generation(db, step="c") > newer
    finally:
        db.disconnect()


# Kept generations (D4)

def test_a_kept_generation_survives_three_activations(tmp_path):
    db = _project(tmp_path)
    try:
        kept = catalog.create_generation(db, step="import", kept=True)
        path = _file(tmp_path / "kept.arrows", 0)
        catalog.add_file(db, path, generation_id=kept)
        catalog.activate_generation(db, kept)
        for step in range(3):
            gen = catalog.create_generation(db, step=f"s{step}")
            catalog.add_file(db, _file(tmp_path / f"g{step}.arrows", 0), generation_id=gen)
            catalog.activate_generation(db, gen)
        assert path.exists() and container.index_path(path).exists()
        assert catalog.count(db, catalog.shard_spec(generation_id=kept), whole=True) == 1
        assert not (tmp_path / "g0.arrows").exists()  # the window still moves for the others
        catalog.activate_generation(db, kept)  # going back deletes nothing kept
        assert path.exists() and catalog.count(db, whole=True) == 1
    finally:
        db.disconnect()


# Sealed (READABLE)

def test_a_sealed_container_is_read(tmp_path):
    from ms_flow.core.data.exclusions import exclude

    db = _project(tmp_path)
    try:
        gen = _active(db)
        file_id = catalog.add_file(db, _file(tmp_path / "a.arrows", 0, 4), generation_id=gen)
        catalog.seal(db, file_id)
        (row,) = catalog.files(db)
        assert row["status"] == catalog.SEALED
        assert (catalog.count(db), catalog.record_count(db)) == (2, 8)
        exclude(db, {1: [0, 1]})
        assert next(catalog.files(db))["n_active"] == 6  # a sealed file's counts still follow the cuts
        with pytest.raises(ValueError, match="sealed"):
            catalog.appender(db, file_id)  # read, never appended again (I19)
    finally:
        db.disconnect()


# I6: an index below the catalog is refused before an Appender opens (and cuts)

def test_an_index_older_than_the_catalog_is_refused_and_not_cut(tmp_path):
    db = _project(tmp_path)
    try:
        gen = _active(db)
        path = _file(tmp_path / "a.arrows", 0)
        old_index = container.index_path(path).read_bytes()
        container.Appender(path).append([_file(tmp_path / "part.arrows", 4)])
        file_id = catalog.add_file(db, path, generation_id=gen)
        (row,) = catalog.files(db)
        assert (row["n_rows"], row["n_retired"]) == (2, 0) and row["end"] == path.stat().st_size

        container.index_path(path).write_bytes(old_index)  # the index of one shard, put back
        size = path.stat().st_size
        with pytest.raises(catalog.StaleIndex, match="rows"):
            catalog.appender(db, file_id)
        with pytest.raises(catalog.StaleIndex):
            catalog.recount(db, file_id)
        assert path.stat().st_size == size  # nothing was cut
        assert next(catalog.files(db))["n_rows"] == 2  # nor lowered
    finally:
        db.disconnect()


def test_a_stale_index_after_a_retire_is_refused_and_not_cut(tmp_path):
    db = _project(tmp_path)
    try:
        gen = _active(db)
        path = _file(tmp_path / "a.arrows", 0, 4)
        file_id = catalog.add_file(db, path, generation_id=gen)
        before = container.index_path(path).read_bytes()
        container.Appender(path).retire([0])
        catalog.recount(db, file_id)
        (row,) = catalog.files(db)
        assert (row["n_shards"], row["n_rows"], row["n_retired"]) == (1, 2, 1)

        container.index_path(path).write_bytes(before)  # same rows and end, the retire lost
        size = path.stat().st_size
        with pytest.raises(catalog.StaleIndex, match="retired"):
            catalog.appender(db, file_id)
        assert path.stat().st_size == size
        # The live count is never the check: a retire lowered it, and that is fine.
        container.Appender(path).retire([0])
        appender = catalog.appender(db, file_id)
        assert appender.end == size
    finally:
        db.disconnect()


def test_a_creation_that_never_finished_is_removed(tmp_path):
    db = _project(tmp_path)
    try:
        gen = _active(db)
        folder = tmp_path / "out"
        known = _file(folder / "known.arrows", 0)
        catalog.add_file(db, known, generation_id=gen)
        # Created in the order file, index, catalog row: a crash before the row leaves these.
        container.Appender.create(folder / "no_row.arrows", _schema(known))
        (folder / "no_index.arrows").write_bytes(b"ARROW")
        assert catalog.sweep(db, folder) == 3
        assert sorted(p.name for p in folder.iterdir()) == ["known.arrows", "known.arrows.index"]
    finally:
        db.disconnect()


def _schema(path):
    import pyarrow.ipc as ipc

    info, _rows = container.read_index(path)
    return ipc.read_schema(ipc.read_message(container.read_head(path, info)))


# The project schema (D5, D5a)

def _version(db_path):
    with sqlite3.connect(db_path) as conn:
        return dict(conn.execute("SELECT schema_name, version FROM _schema_versions").fetchall())


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _made(tmp_path):
    db = _project(tmp_path)
    path = db.db_path
    db.disconnect()
    return path


def test_a_new_project_is_created_at_2(tmp_path):
    path = _made(tmp_path)
    assert _version(path) == {"project": 2}


def test_a_newer_project_is_refused_untouched(tmp_path):
    path = _made(tmp_path)
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE _schema_versions SET version = 3 WHERE schema_name = 'project'")
    sha = _sha(path)
    with pytest.raises(RuntimeError, match="newer version"):
        _project(tmp_path)
    assert _sha(path) == sha


def test_an_older_project_is_refused_untouched(tmp_path):
    path = _made(tmp_path)
    with sqlite3.connect(path) as conn:  # what the released versions wrote
        conn.execute("DELETE FROM _schema_versions")
        conn.execute("INSERT INTO _schema_versions (schema_name, version) VALUES ('_projectsqlitehandle', 1)")
    sha = _sha(path)
    with pytest.raises(RuntimeError, match="new project") as error:
        _project(tmp_path)
    assert str(path.parent) in str(error.value)
    assert _sha(path) == sha


def test_both_handles_read_one_version(tmp_path):
    from ms_flow.core.database.project_store_ops import _StandaloneProjectSQLiteDB

    path = _made(tmp_path)
    other = _StandaloneProjectSQLiteDB(path, auto_setup=True)
    other.engine.dispose()
    db = _project(tmp_path)
    db.disconnect()
    assert _version(path) == {"project": 2}
    copy = tmp_path / "copy.db"
    shutil.copy(path, copy)
    with sqlite3.connect(copy) as conn:
        conn.execute("UPDATE _schema_versions SET version = 3")
    with pytest.raises(RuntimeError, match="newer version"):
        _StandaloneProjectSQLiteDB(copy, auto_setup=True)
