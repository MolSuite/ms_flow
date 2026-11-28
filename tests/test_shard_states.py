"""Shard states: the done set as a sidecar, the fraction in the database, and the mini file
that carries only what is left to run with its parent's ids."""
import uuid

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ms_flow.core.data.shard import PayloadKind, Serializer, Shard, ShardWriter, merge_shards
from ms_flow.core.data import catalog, container
from ms_flow.core.data.exclusions import exclude, set_exclusion_active
from ms_flow.core.data.states import done, failed, forget, mark_done, merge, pending, progress, sidecar
from ms_flow.core.database import ProjectStore


def _project(tmp_path):
    db = ProjectStore()
    db.connect(tmp_path / "project")
    return db


def test_done_grows_and_the_rest_stays_pending(tmp_path):
    db = _project(tmp_path)
    try:
        assert pending(db, "step:a", "lib.parquet", [1, 2, 3]) == [1, 2, 3]
        assert mark_done(db, "step:a", "lib.parquet", [2, 1, 1], total=4) == 2
        assert mark_done(db, "step:a", "lib.parquet", [3], failed=1) == 3
        assert done(db, "step:a", "lib.parquet").tolist() == [1, 2, 3]
        assert pending(db, "step:a", "lib.parquet", [1, 2, 3, 4]) == [4]
        # Another kind, or another file, is its own state.
        assert pending(db, "step:b", "lib.parquet", [1]) == [1]
        # The database alone answers "202/1000", with no id read.
        (row,) = progress(db, "step:a")
        assert (row["shards_done"], row["shards_total"], row["shards_failed"], row["status"]) == (3, 4, 1, "partial")
        assert mark_done(db, "step:a", "lib.parquet", [4]) == 4
        assert progress(db, "step:a")[0]["status"] == "complete"
    finally:
        db.disconnect()


def test_forgetting_runs_it_again(tmp_path):
    db = _project(tmp_path)
    try:
        mark_done(db, "step:a", "lib.parquet", [1, 2, 3], total=3)
        assert forget(db, "step:a", "lib.parquet", [2]) == 2
        assert pending(db, "step:a", "lib.parquet", [1, 2, 3]) == [2]
        assert forget(db, "step:a", "lib.parquet") == 0
        assert pending(db, "step:a", "lib.parquet", [1, 2, 3]) == [1, 2, 3]
        assert progress(db)[0]["status"] == "inactive"
    finally:
        db.disconnect()


def test_a_mini_file_of_the_pending_shards_keeps_the_parent_ids(tmp_path):
    """What travels to a host that has most of the dataset done already: the few shards left,
    under the same ids, so its results pair back with the parent with nothing to translate."""
    parent = tmp_path / "lib.arrows"
    with ShardWriter(parent, dataset_id=uuid.UUID(int=1), shard_id=0, base_id=0, slot_count=8,
                     kind=PayloadKind.GENERIC_BYTES, serializer=Serializer.UTF8) as writer:
        for i in range(8):
            writer.add(i, f"m{i}".encode())
            if i % 2:
                writer.end_group()  # four shards of two records

    mini = tmp_path / "mini.arrows"
    ranges = merge_shards([(parent, 2), (parent, 3)], mini)

    assert [r["base_id"] for r in ranges] == [4, 6]
    with Shard.open(mini) as shard:
        assert [(r.id, r.load()) for r in shard] == [(i, f"m{i}") for i in range(4, 8)]


def test_what_another_host_did_comes_back_as_its_own_file_and_is_unioned(tmp_path):
    """A host keeps its own done set; back here it is merged, and what either did is done."""
    import shutil

    db, other = _project(tmp_path), _project(tmp_path / "other")
    try:
        mark_done(db, "run", "k", [1, 2], total=6)
        mark_done(other, "run", "k", [3, 4])
        mark_done(other, "run", "k", [5], narrowed_by="cut-1")
        came = tmp_path / "came.arrows"
        shutil.copyfile(sidecar(other, "run", "k"), came)

        assert merge(db, "run", "k", came) == 5
        assert [int(s) for s in done(db, "run", "k")] == [1, 2, 3, 4]
        assert [int(s) for s in done(db, "run", "k", narrowed_by="cut-1")] == [1, 2, 3, 4, 5]
        assert [row["shards_done"] for row in progress(db, "run")] == [5]
    finally:
        db.disconnect()
        other.disconnect()


def test_a_shard_failed_whole_lists_no_ids(tmp_path):
    """Every record of shard 1 failed: it is not done, it runs again whole, nothing is listed."""
    db = _project(tmp_path)
    try:
        assert mark_done(db, "run", "k", [1, 2], records={1: ([], [5, 6]), 2: ([1, 2, 3], [4])},
                         total=4, records_total=10) == 1
        assert pending(db, "run", "k", [1, 2]) == [1]
        assert pq.read_table(sidecar(db, "run", "k"))["shard"].to_pylist() == [2]
        assert failed(db, "run", "k", 1).tolist() == []
        (row,) = progress(db, "run")
        assert (row["shards_done"], row["shards_failed"], row["shards_total"]) == (1, 1, 4)
        assert (row["records_done"], row["records_failed"], row["records_total"]) == (3, 1, 10)
    finally:
        db.disconnect()


def test_the_minority_is_listed(tmp_path):
    db = _project(tmp_path)
    try:
        mark_done(db, "run", "k", [1, 2], records={1: (np.arange(8), [9, 8]), 2: ([0, 1], np.arange(2, 10))})
        rows = pq.read_table(sidecar(db, "run", "k")).to_pylist()
        assert [(r["shard"], r["n_total"], r["n_done"], r["listed"], r["ids"]) for r in rows] == [
            (1, 10, 8, "failed", [8, 9]), (2, 10, 2, "done", [0, 1])]
        assert failed(db, "run", "k", 1).tolist() == [8, 9]
        assert failed(db, "run", "k", 2, ids=np.arange(10)).tolist() == list(range(2, 10))
        with pytest.raises(ValueError, match="ids"):
            failed(db, "run", "k", 2)  # the done ones are listed: the rest needs the shard's ids
        mark_done(db, "run", "k", [3])  # no records given: done, counts unknown, nothing listed
        assert failed(db, "run", "k", 3).tolist() == []
        (row,) = progress(db, "run")
        assert (row["shards_done"], row["records_done"], row["records_failed"]) == (3, 10, 10)
        # Again, the shard as it ran this time replaces its counts.
        mark_done(db, "run", "k", [2], records={2: (np.arange(10), [])})
        assert progress(db, "run")[0]["records_failed"] == 2
    finally:
        db.disconnect()


def test_a_state_file_without_counts_still_reads(tmp_path):
    """A sidecar of before the counts: its shards are done, their counts unknown."""
    db = _project(tmp_path)
    try:
        mark_done(db, "run", "k", [7])
        pq.write_table(pa.table({"shard": pa.array([1, 2], pa.uint64()), "narrowed": ["", "cut"]}),
                       sidecar(db, "run", "k"))
        assert mark_done(db, "run", "k", [3], records={3: ([0, 1], [])}) == 3
        assert done(db, "run", "k").tolist() == [1, 3]
        assert done(db, "run", "k", narrowed_by="cut").tolist() == [1, 2, 3]
        assert progress(db, "run")[0]["records_done"] == 2
    finally:
        db.disconnect()


def _library(db, folder, *, scope=1):
    """One source file of three shards of four ids (0-11), the active generation."""
    gen = catalog.create_generation(db, step="load")
    path = folder / "src.arrows"
    with ShardWriter(path, dataset_id=None, shard_id=0, base_id=0, kind=0, serializer=Serializer.UTF8,
                     slot_count=1 << 20) as writer:
        for lo in (0, 4, 8):
            for id_ in range(lo, lo + 4):
                writer.add(id_, b"x")
            writer.end_group(scope=scope, base_id=lo, slot_count=4)
    file_id = catalog.add_file(db, path, generation_id=gen)
    catalog.activate_generation(db, gen)
    return gen, file_id


def _row(db, file_id):
    from ms_flow.core.database.project_models import CatalogFile

    with db.get_session() as session:
        row = session.get(CatalogFile, file_id)
        return {"n_shards": row.n_shards, "n_records": row.n_records, "n_active": row.n_active,
                "n_shards_excluded": row.n_shards_excluded}


def _derived(db, folder, gen, sources, name="prep.arrows"):
    """A step made shards of two of the three source shards; one record of the first was dropped."""
    path = folder / name
    with ShardWriter(path, dataset_id=None, shard_id=0, base_id=0, kind=0, serializer=Serializer.UTF8,
                     slot_count=1 << 20) as writer:
        for source, lo in zip(sources, (0, 4)):
            for id_ in range(lo, lo + 4):
                if id_ == 2:
                    writer.drop(id_, "bad")
                else:
                    writer.add(id_, b"y")
            writer.end_group(scope=1, base_id=lo, slot_count=4, origin=source)
    return catalog.add_file(db, path, generation_id=gen, role="prep")


def test_progress_is_one_shape_for_derived_and_done_sets(tmp_path, monkeypatch):
    db = _project(tmp_path)
    try:
        gen, _ = _library(db, tmp_path)
        sources = [row["id"] for row in catalog.shards(db)]
        _derived(db, tmp_path, gen, sources[:2])
        mark_done(db, "run", "k", sources[:1], records={sources[0]: ([0, 1, 3], [2])}, total=3,
                  records_total=12, generation_id=gen)

        def no_data(*_args, **_kwargs):
            raise AssertionError("a summary opened a data file")

        for name in ("read_raw", "read_table", "iter_tables"):
            monkeypatch.setattr(container, name, no_data)
        step = catalog.progress(db, gen, "prep")
        (state,) = progress(db, "run")
        assert set(step) == set(state)
        assert {k: step[k] for k in ("shards_done", "shards_total", "shards_failed", "records_done",
                                     "records_failed", "records_total", "status")} == {
            "shards_done": 2, "shards_total": 3, "shards_failed": 0, "records_done": 7,
            "records_failed": 1, "records_total": 12, "status": "partial"}
        assert (state["shards_done"], state["records_done"], state["records_failed"]) == (1, 3, 1)
    finally:
        db.disconnect()


def test_states_behind_the_index_are_rebuilt_from_it(tmp_path):
    """A crash between a commit and its counts leaves the row behind: `recount` reads the index."""
    from ms_flow.core.database.project_models import CatalogFile

    db = _project(tmp_path)
    try:
        gen, _ = _library(db, tmp_path)
        sources = [row["id"] for row in catalog.shards(db)]
        file_id = _derived(db, tmp_path, gen, sources[:2])
        before = catalog.progress(db, gen, "prep")
        with db.get_session() as session:
            row = session.get(CatalogFile, file_id)
            row.n_shards = row.n_records = row.n_active = 0
            session.add(row)
            session.commit()
        catalog.recount(db, file_id)
        assert _row(db, file_id)["n_records"] == 7 and catalog.progress(db, gen, "prep") == before
    finally:
        db.disconnect()


def test_a_cut_of_a_whole_shard_counts_as_an_excluded_shard(tmp_path):
    db = _project(tmp_path)
    try:
        exclude(db, {1: [0, 1, 2, 3, 5]})  # before the file: its counts at registration see it
        _, file_id = _library(db, tmp_path)
        assert _row(db, file_id) == {"n_shards": 3, "n_records": 12, "n_active": 7, "n_shards_excluded": 1}
    finally:
        db.disconnect()


def test_undo_restores_both_counts(tmp_path):
    db = _project(tmp_path)
    try:
        _, file_id = _library(db, tmp_path)
        cut = exclude(db, {1: [4, 5, 6, 7, 8]})
        assert _row(db, file_id) == {"n_shards": 3, "n_records": 12, "n_active": 7, "n_shards_excluded": 1}
        set_exclusion_active(db, cut, False)
        assert _row(db, file_id) == {"n_shards": 3, "n_records": 12, "n_active": 12, "n_shards_excluded": 0}
        set_exclusion_active(db, cut, True)
        assert _row(db, file_id)["n_shards_excluded"] == 1
    finally:
        db.disconnect()


def _stat(path):
    st = path.stat()
    return st.st_ino, st.st_mtime_ns, st.st_size


def test_one_more_shard_does_not_rewrite_the_done_set(tmp_path, monkeypatch):
    """A shard marked done adds a small piece: the file of what was done before stays as it was."""
    from ms_flow.core.data import states

    monkeypatch.setattr(states, "COMPACT_AT", 1000)
    db = _project(tmp_path)
    try:
        mark_done(db, "run", "k", range(100))
        states.sidecar(db, "run", "k")  # folded into one file
        base = states._file(db, "run", "k")
        before = _stat(base)
        assert mark_done(db, "run", "k", [100], records={100: ([0, 1], [2])}) == 101
        assert _stat(base) == before
        assert done(db, "run", "k").tolist() == list(range(101))
        (state,) = progress(db, "run")
        assert (state["shards_done"], state["records_done"], state["records_failed"]) == (101, 2, 1)
    finally:
        db.disconnect()


def test_the_pieces_fold_into_one_file(tmp_path, monkeypatch):
    from ms_flow.core.data import states

    monkeypatch.setattr(states, "COMPACT_AT", 3)
    db = _project(tmp_path)
    try:
        for shard in range(7):
            mark_done(db, "run", "k", [shard], records={shard: ([0], [1])} if shard == 4 else None)
        pieces = states._file(db, "run", "k").with_suffix(".pieces")
        assert len(list(pieces.glob("*.parquet"))) < 3
        states._CACHE.clear()  # what another process reads off the disk
        assert done(db, "run", "k").tolist() == list(range(7))
        assert failed(db, "run", "k", 4).tolist() == [1]
    finally:
        db.disconnect()


def test_a_fold_that_crashed_before_deleting_its_pieces_reads_the_same(tmp_path, monkeypatch):
    from ms_flow.core.data import states

    monkeypatch.setattr(states, "COMPACT_AT", 1000)
    db = _project(tmp_path)
    try:
        mark_done(db, "run", "k", [1], narrowed_by="cut-1")
        mark_done(db, "run", "k", [1], records={1: ([0, 1, 2], [3])})  # whole, and its counts
        pieces = states._file(db, "run", "k").with_suffix(".pieces")
        kept = {p.name: p.read_bytes() for p in pieces.glob("*.parquet")}
        states.sidecar(db, "run", "k")
        for name, data in kept.items():  # as if the fold died before deleting them
            (pieces / name).write_bytes(data)
        states._CACHE.clear()
        assert done(db, "run", "k").tolist() == [1]
        assert failed(db, "run", "k", 1).tolist() == [3]
    finally:
        db.disconnect()


def test_a_reader_sees_what_another_process_wrote_since(tmp_path, monkeypatch):
    from ms_flow.core.data import states

    monkeypatch.setattr(states, "COMPACT_AT", 2)
    db = _project(tmp_path)
    try:
        mark_done(db, "run", "k", [1])
        assert done(db, "run", "k").tolist() == [1]
        mine = dict(states._CACHE)
        states._CACHE.clear()
        for shard in (2, 3, 4):  # "another process": its own cache, a fold among them
            mark_done(db, "run", "k", [shard])
        states._CACHE.clear()
        states._CACHE.update(mine)  # back to the reader's stale view
        assert done(db, "run", "k").tolist() == [1, 2, 3, 4]
    finally:
        db.disconnect()
