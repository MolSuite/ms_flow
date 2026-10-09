"""`File` and `Dir`: paths to whoever shares the disk, marks to whoever has to carry the files."""
import pickle

import pytest

from benchmarks.refactor_optimization import workers
from ms_flow.engine import Dir, Engine, File, ProcessExecutor
from ms_flow.engine.files import swap


def test_marks_are_found_wherever_they_are_and_nothing_else_is_touched():
    element = {"receptor": File("/a/r.pdbqt"), "ligands": [File("/a/1"), "plain"], "out": Dir("/a/out"), "n": (1, "x")}
    seen = []
    there = swap(element, lambda mark: seen.append(mark) or type(mark)("/far" + mark))
    assert seen == ["/a/r.pdbqt", "/a/1", "/a/out"]
    assert there == {"receptor": "/far/a/r.pdbqt", "ligands": ["/far/a/1", "plain"], "out": "/far/a/out", "n": (1, "x")}
    assert type(pickle.loads(pickle.dumps(element))["out"]) is Dir


def test_on_this_machine_a_mark_is_the_path(tmp_path):
    (tmp_path / "in.txt").write_text("here")
    got = []
    with Engine(cpus=2) as engine:
        engine.register("process", ProcessExecutor(2))
        job = engine.submit("process", workers.read, [{"path": File(tmp_path / "in.txt")}], on_results=got.extend)
        assert engine.wait(job, 60).status == "completed"
    assert got[0][1] == {"path": str(tmp_path / "in.txt"), "text": "here"}


@pytest.mark.slow
def test_on_ray_without_a_shared_disk_files_go_once_and_what_results_name_comes_back(tmp_path):
    pytest.importorskip("ray")
    from ms_flow.engine import RayExecutor

    (tmp_path / "receptor.txt").write_text("here")
    read, left, poses = [], [], tmp_path / "poses"
    with Engine(cpus=1) as engine:
        engine.register("ray", RayExecutor(num_cpus=2, shared=False))
        one = engine.submit("ray", workers.read, [{"path": File(tmp_path / "receptor.txt"), "n": i} for i in range(6)],
                            on_results=read.extend)
        two = engine.submit("ray", workers.leave, [{"out": Dir(poses), "n": i} for i in range(7)], batch=3,
                            on_results=left.extend)
        assert (engine.wait(one, 120).status, engine.wait(two, 120).status) == ("completed", "completed")
    seen = {result["path"] for key, result in read}
    assert len(read) == 6 and all(result["text"] == "here" for key, result in read)
    assert len(seen) == 1 and not seen.pop().startswith(str(tmp_path)), "one copy, kept by the node"
    assert sorted(result["made"] for key, result in left) == [str(poses / f"{i}.txt") for i in range(7)]
    assert [(poses / f"{i}.txt").read_text() for i in range(7)] == [str(i) for i in range(7)]
