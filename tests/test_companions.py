"""I14: a container and its index go together. Every mover knows both (M2)."""
import subprocess
from types import SimpleNamespace

from ms_flow import locations
from ms_flow.core.data import catalog
from ms_flow.core.data.companions import companions
from ms_flow.core.database import ProjectStore


def _project(tmp_path):
    db = ProjectStore()
    db.connect(tmp_path / "project")
    return db


def _pair(path):
    path.write_bytes(b"data")
    (path.parent / (path.name + ".index")).write_bytes(b"index")
    return path


def test_only_a_container_has_a_companion(tmp_path):
    assert companions(tmp_path / "a.arrows") == [str(tmp_path / "a.arrows.index")]
    assert companions("a.parquet") == companions("a.arrows.index") == companions("a.arrows.tmp") == []


def test_sweep_keeps_the_index_of_a_known_container(tmp_path):
    db = _project(tmp_path)
    try:
        out = tmp_path / "out"
        out.mkdir()
        known, stray = _pair(out / "known.arrows"), _pair(out / "stray.arrows")
        (out / "known.arrows.lock").write_text("1")
        (out / "known.arrows.index.tmp").write_bytes(b"half")
        catalog.add_file(db, known, generation_id=None, counts={"n_shards": 1})
        assert catalog.sweep(db, out) == 3  # the stray pair and the tmp
        assert sorted(p.name for p in out.iterdir()) == ["known.arrows", "known.arrows.index", "known.arrows.lock"]
        assert not stray.exists()
    finally:
        db.disconnect()


def test_removing_a_container_removes_its_index(tmp_path):
    db = _project(tmp_path)
    try:
        path = _pair(tmp_path / "r.arrows")
        catalog.add_file(db, path, generation_id=None, role="result", counts={"n_shards": 1})
        assert catalog.remove(db, "result") == 1
        assert list(tmp_path.glob("r.arrows*")) == []
    finally:
        db.disconnect()


def test_a_row_names_the_index_so_it_comes_back(tmp_path, monkeypatch):
    path = _pair(tmp_path / "s.arrows")
    monkeypatch.setattr(catalog, "_counts", lambda *a, **k: {})  # a container's counts come with M3b
    assert catalog.file_row(path, role="result", generation_id=None)["sidecars"] == [str(path) + ".index"]


def test_adopting_a_container_needs_its_index(tmp_path):
    db = _project(tmp_path)
    try:
        there = catalog.add_file(db, "/scratch/c.arrows", generation_id=None, location="host:/scratch",
                                 counts={"n_shards": 1})
        brought = tmp_path / "c.arrows"
        brought.write_bytes(b"data")
        assert catalog.adopt(db, there, brought) is False  # half of it arrived: the row is left alone
        assert {row["id"]: row["location"] for row in catalog.files(db)} == {there: "host:/scratch"}
    finally:
        db.disconnect()


class _Here:
    """A host that is this machine: its commands run in `sh`."""

    address = None

    def run(self, command, data=b""):
        return subprocess.run(["sh", "-c", command], input=data, capture_output=True, check=True).stdout


def test_reconcile_forgets_both_or_neither(tmp_path):
    db = _project(tmp_path)
    try:
        remote = SimpleNamespace(remote=_Here(), workdir=str(tmp_path / "far"))
        host, there = locations.host_of(remote), tmp_path / "far" / "projects" / locations.uid(db)
        (there / "in").mkdir(parents=True)
        for name in ("a.arrows", "b.arrows"):
            _pair(there / "in" / name)
        locations.record(db, host, {f"in/{name}": (len(body), "sha") for name, body in
                                    (("a.arrows", b"data"), ("a.arrows.index", b"index"),
                                     ("b.arrows", b"data"), ("b.arrows.index", b"index"))})
        assert locations.reconcile(db, remote) == []  # both there: neither goes

        (there / "in" / "a.arrows.index").unlink()  # half of a gone: a whole
        assert sorted(locations.reconcile(db, remote)) == ["in/a.arrows", "in/a.arrows.index"]
        assert sorted(locations.known(db, host)) == ["in/b.arrows", "in/b.arrows.index"]

        locations.forget(db, host, ["in/b.arrows"])  # forgetting one forgets its index
        assert locations.known(db, host) == {}
    finally:
        db.disconnect()
