"""M11: containers made on another host, brought here as far as their index there goes, and the
copies there cleaned up only where a verified copy is here. Against this machine's `none` queue."""
import os
import shutil
import sys
from pathlib import Path

import pytest

from benchmarks.refactor_optimization import workers
from ms_flow import locations
from ms_flow.core.data import catalog, container
from ms_flow.core.database.project import ProjectStore
from ms_flow.remote import RemoteExecutor
from ms_flow.runner import _There, commit_there

pytestmark = pytest.mark.slow
_ROOT = Path(__file__).resolve().parents[1]


def _far(where):
    return RemoteExecutor(workdir=str(where), python=sys.executable, poll_s=0.2, part_seconds=0.1,
                          setup=f"export PYTHONPATH={_ROOT}:{_ROOT / 'src'}:{_ROOT / 'tests'}")


@pytest.fixture
def project(tmp_path):
    db = ProjectStore()
    db.connect(tmp_path / "p")
    yield db
    db.disconnect()


def _made_there(home, n, role="made", *, start=0):
    """`n` parts committed there into the container of `role`, as the runner does; the rows a
    stage's `done` lists."""
    home.mkdir(parents=True, exist_ok=True)
    receipts = []
    for i in range(start, start + n):
        made = workers.part_of({"n": i, "log": str(home / "log"), "out": str(home / "out")})
        receipts += [{**r, "role": role, "folder": str(home / "out" / role)} for r in workers.receipts_of(made)]
    commit_there(home, receipts)
    there = _There(home)
    try:
        return there.files()
    finally:
        there.close()


def _ids(project, role):
    return [row["id"] for row in catalog.files(project, catalog.shard_spec(role=role))]


def test_a_file_is_registered_on_two_hosts_and_here(project, tmp_path):
    far = _far(tmp_path / "far")
    rows = _made_there(tmp_path / "far" / "c1", 3)
    locations.register_there(project, "far", rows)
    [file_id] = _ids(project, "made")
    assert locations.bring(project, far, [file_id], location="far") == [file_id]
    row = catalog.get_shard(project, catalog.shard_id(file_id, 0))
    here, there = Path(row["path"]), rows[0]["path"]
    assert here.is_file() and str(here).startswith(str(tmp_path / "p")) and here.read_bytes() == Path(there).read_bytes()
    assert [r["location"] for r in catalog.files(project, catalog.shard_spec(role="made"))] == [""]
    assert catalog.count(project, catalog.shard_spec(role="made")) == 3  # read here now
    assert there in locations.known(project, locations.host_of(far))
    # and on a third place: what is here goes to another host, which then has it too
    other = _far(tmp_path / "other")
    relative = locations.inside(project, here)
    locations.record(project, locations.host_of(other), {relative: (1, "x"), relative + ".index": (1, "y")})
    hosts = {host for host in (locations.host_of(far), locations.host_of(other))
             if any(path in (there, relative) for path in locations.known(project, host))}
    assert len(hosts) == 2


def test_cleanup_never_takes_the_only_copy_silently(project, tmp_path):
    far = _far(tmp_path / "far")
    brought = _made_there(tmp_path / "far" / "c1", 2, "brought")
    alone = _made_there(tmp_path / "far" / "c1", 2, "alone", start=10)
    locations.register_there(project, "far", brought + alone)
    locations.bring(project, far, _ids(project, "brought"), location="far")
    said = locations.cleanup(project, far, location="far")
    assert {(Path(r["path"]).name.split(".")[0], r["verified"], r["removed"]) for r in said} == {
        ("brought", True, True), ("alone", False, False)}
    assert not Path(brought[0]["path"]).exists() and Path(alone[0]["path"]).exists()
    assert catalog.count(project, catalog.shard_spec(role="brought")) == 2  # the copy here stays
    assert [r["location"] for r in catalog.files(project, catalog.shard_spec(role="alone"))] == ["far"]
    said = locations.cleanup(project, far, location="far", confirm=True)
    assert [(r["verified"], r["removed"]) for r in said] == [(False, True)]
    assert not Path(alone[0]["path"]).exists() and not list(catalog.files(project, catalog.shard_spec(role="alone")))


def test_a_cut_copy_resumes(project, tmp_path, monkeypatch):
    from ms_env import HostError

    far = _far(tmp_path / "far")
    rows = _made_there(tmp_path / "far" / "c1", 12)
    locations.register_there(project, "far", rows)
    [file_id] = _ids(project, "made")
    run, pieces = far.remote.run, []

    def cut(command, data=b"", timeout=600):
        if "tail -c" in command:
            pieces.append(command)
            if len(pieces) == 3:
                raise HostError("ssh: Connection reset by peer")
        return run(command, data, timeout)

    monkeypatch.setattr(far.remote, "run", cut)
    with pytest.raises(HostError):
        locations.bring(project, far, [file_id], location="far", piece=600)
    assert [r["location"] for r in catalog.files(project, catalog.shard_spec(role="made"))] == ["far"]
    first = len(pieces)
    monkeypatch.setattr(far.remote, "run", lambda c, d=b"", t=600: pieces.append(c) or run(c, d, t)
                        if "tail -c" in c else run(c, d, t))
    locations.bring(project, far, [file_id], location="far", piece=600)
    again = pieces[first:]
    assert int(again[0].split("tail -c +")[1].split()[0]) > 1, "it began again from the start"
    row = next(catalog.files(project, catalog.shard_spec(role="made")))
    assert Path(row["path"]).read_bytes() == Path(rows[0]["path"]).read_bytes()
    assert catalog.count(project, catalog.shard_spec(role="made")) == 12


def test_bringing_one_step_brings_nothing_else(project, tmp_path):
    far = _far(tmp_path / "far")
    rows = _made_there(tmp_path / "far" / "c1", 2, "first") + _made_there(tmp_path / "far" / "c1", 2, "second", start=5)
    locations.register_there(project, "far", rows)
    locations.bring(project, far, _ids(project, "first"), location="far")
    assert [r["location"] for r in catalog.files(project, catalog.shard_spec(role="second"))] == ["far"]
    assert not [p for p in (tmp_path / "p").rglob("second*")]


def test_two_parts_written_as_one_still_name_each_other_here(project, tmp_path):
    """A pair's index names its bulk shards by catalog id: there, ids of the catalog there; here,
    once both are brought, ids of this project's."""
    import json

    far, home = _far(tmp_path / "far"), tmp_path / "far" / "c1"
    home.mkdir(parents=True)
    receipts = []
    for i in range(3):
        index = workers.part_of({"n": i, "log": str(home / "log"), "out": str(home / "out")})
        bulk = workers.part_of({"n": 100 + i, "log": str(home / "log"), "out": str(home / "out")})
        receipts.append({"part": f"p:{i}", "path": index["part"], "role": "index", "bulk": bulk["part"],
                         "bulk_role": "bulk", "generation_id": None, "folder": str(home / "out" / "k")})
    commit_there(home, receipts)
    there = _There(home)
    rows = there.files()
    there.close()
    locations.register_there(project, "far", rows)
    locations.bring(project, far, _ids(project, "index") + _ids(project, "bulk"), location="far")
    [index_row] = list(catalog.files(project, catalog.shard_spec(role="index")))
    for row in container.read_index(index_row["path"])[1]:
        for sid in json.loads(row["meta"])["bulk"]:
            named = catalog.get_shard(project, sid)
            assert named is not None and named["role"] == "bulk" and Path(named["path"]).is_file()
            assert str(named["path"]).startswith(str(tmp_path / "p"))
