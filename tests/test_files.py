"""`File` and `Dir`: paths to whoever shares the disk, marks to whoever has to carry the files."""
import gc
import pickle

import pytest

from benchmarks.refactor_optimization import workers
from ms_flow.engine import Dir, Engine, File, ProcessExecutor
from ms_flow.engine.files import swap


def test_marks_are_found_wherever_they_are_and_nothing_else_is_touched():
    element = {"anchor": File("/a/r.tsv"), "guests": [File("/a/1"), "plain"], "out": Dir("/a/out"), "n": (1, "x")}
    seen = []
    there = swap(element, lambda mark: seen.append(mark) or type(mark)("/far" + mark))
    assert seen == ["/a/r.tsv", "/a/1", "/a/out"]
    assert there == {"anchor": "/far/a/r.tsv", "guests": ["/far/a/1", "plain"], "out": "/far/a/out", "n": (1, "x")}
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

    (tmp_path / "anchor.txt").write_text("here")
    (tmp_path / "in.arrows").write_text("sent")
    (tmp_path / "in.arrows.index").write_text("its index")
    read, left, pairs, layouts, out = [], [], [], tmp_path / "layouts", tmp_path / "out"
    with Engine(cpus=1) as engine:
        ray = RayExecutor(num_cpus=2, shared=False)
        engine.register("ray", ray)
        one = engine.submit("ray", workers.read, [{"path": File(tmp_path / "anchor.txt"), "n": i} for i in range(6)],
                            on_results=read.extend)
        two = engine.submit("ray", workers.leave, [{"out": Dir(layouts), "n": i} for i in range(7)], batch=3,
                            on_results=left.extend)
        # a container: its index goes with it, and comes back with it
        three = engine.submit("ray", workers.pair, [{"container": File(tmp_path / "in.arrows"), "out": Dir(out), "n": i}
                                                    for i in range(3)], on_results=pairs.extend)
        assert [engine.wait(job, 120).status for job in (one, two, three)] == ["completed"] * 3
        gc.collect()
        assert not ray._carried, "a file nobody reads any more is not kept in the object store"
    assert {result["said"] for key, result in pairs} == {"sent|its index"}
    assert sorted(path.name for path in out.iterdir()) == sorted(f"{i}.arrows{end}" for i in range(3) for end in ("", ".index"))
    assert not list(out.glob("*.tmp"))
    seen = {result["path"] for key, result in read}
    assert len(read) == 6 and all(result["text"] == "here" for key, result in read)
    assert len(seen) == 1 and not seen.pop().startswith(str(tmp_path)), "one copy, kept by the node"
    assert sorted(result["made"] for key, result in left) == [str(layouts / f"{i}.txt") for i in range(7)]
    assert [(layouts / f"{i}.txt").read_text() for i in range(7)] == [str(i) for i in range(7)]
