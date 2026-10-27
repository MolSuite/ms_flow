"""Transport decisions: who gets a path, who gets bytes, and who gets a copy."""

from __future__ import annotations

from pathlib import Path

from ms_flow.core.data.bridge import DataBridge
from ms_flow.core.data.contracts import FileInputSpec
from ms_flow.core.data.planner import DataTransportPlanner
from ms_flow.core.data.runtime import DataContext, ExecutorTransportProfile


def _profile(backend: str, *, shared_fs: bool, mode: str = "external") -> ExecutorTransportProfile:
    return ExecutorTransportProfile(backend=backend, mode=mode, shared_fs=shared_fs)


def _plan(spec: FileInputSpec, profile: ExecutorTransportProfile) -> str:
    handle = DataTransportPlanner().plan_input(spec, context=DataContext(), profile=profile)
    return handle.strategy


def test_shared_filesystem_beats_the_cache():
    """A cached input on a shared FS must still travel as a path, not as bytes."""
    cached = FileInputSpec("/tmp/receptor.pdbqt", delivery="path", cache=True)
    assert _plan(cached, _profile("ray", shared_fs=True)) == "shared_path"
    assert _plan(cached, _profile("ray", shared_fs=False)) == "ray_object_transfer"


def test_uncached_path_without_shared_fs_transfers_bytes():
    spec = FileInputSpec("/tmp/shard.mshard", delivery="path")
    assert _plan(spec, _profile("ray", shared_fs=False)) == "ray_object_transfer"
    assert _plan(spec, _profile("ray", shared_fs=True)) == "shared_path"
    assert _plan(spec, _profile("thread", shared_fs=True, mode="local")) == "local_path"


def test_hpc_inputs_are_staged_once_and_shared_by_every_chunk(tmp_path: Path):
    source = tmp_path / "receptor.pdbqt"
    source.write_text("ATOM\n")
    wdir = tmp_path / "wdir"
    wdir.mkdir()
    bridge = DataBridge()
    spec = FileInputSpec(str(source), delivery="path")

    def stage(chunk_id: str) -> str:
        context = DataContext(
            project_dir=tmp_path,
            extras={"hpc_wdir": str(wdir), "job_id": "job-1", "chunk_id": chunk_id},
        )
        return bridge._stage_file_to_hpc(spec, context)

    first, second = stage("chunk-0"), stage("chunk-1")
    assert first == second, "the same input must not be copied once per chunk"
    assert Path(first).read_text() == "ATOM\n"
    assert list(wdir.glob("molsuite_inputs/*/receptor.pdbqt")) == [Path(first)]

    # A changed input is a different input: it gets its own staged copy.
    source.write_text("ATOM\nATOM\n")
    assert stage("chunk-2") != first


if __name__ == "__main__":
    import tempfile

    test_shared_filesystem_beats_the_cache()
    test_uncached_path_without_shared_fs_transfers_bytes()
    with tempfile.TemporaryDirectory() as directory:
        test_hpc_inputs_are_staged_once_and_shared_by_every_chunk(Path(directory))
    print("ok")
