"""The file sink in append mode writes only its own batch.

What deserves a test is exactly what breaks unnoticed: the default mode keeps everything
produced and rewrites the whole file on every flush (O(N) RAM, O(N^2) IO), so a long job
degrades without failing. With append, neither `_file_history` nor the file grows with the
job's total.
"""
from pathlib import Path

import pytest

from ms_flow.core.data import DataBridge, DataContext, FileOutputSpec
from ms_flow.core.executor.result_handlers import OutputSpecResultHandler, StagedOutput


def _handler(tmp_path: Path, **spec_kwargs) -> OutputSpecResultHandler:
    return OutputSpecResultHandler(
        executor_db=object(),
        job_id="job",
        bridge=DataBridge(),
        output_spec=FileOutputSpec(path="out.txt", root="project", fmt="text", **spec_kwargs),
        data_context=DataContext(project_dir=tmp_path),
        flush_every=2,
    )


def _batch(handler: OutputSpecResultHandler, *lines: str) -> list[StagedOutput]:
    staged = []
    for index, line in enumerate(lines):
        ref = Path(handler._write_payload(f"{line}-{index}", line))
        staged.append(StagedOutput(f"{line}-{index}", str(ref), ref.stat().st_size, 1))
        handler._pending[staged[-1].chunk_id] = staged[-1]
        if not (handler._file_append or handler._file_per_batch):
            handler._file_history.append(staged[-1])
    return staged


def test_append_writes_only_the_current_batch(tmp_path):
    handler = _handler(tmp_path, append=True)
    handler.write_batch(_batch(handler, "a", "b"))
    handler.write_batch(_batch(handler, "c", "d"))

    assert (tmp_path / "out.txt").read_text() == "a\nb\nc\nd\n"
    assert handler._file_history == []  # nothing accumulated: memory does not grow with the job


def test_without_append_every_flush_rewrites_the_whole_file(tmp_path):
    handler = _handler(tmp_path)
    handler.write_batch(_batch(handler, "a", "b"))
    handler.write_batch(_batch(handler, "c", "d"))

    assert (tmp_path / "out.txt").read_text() == "a\nb\nc\nd"
    assert len(handler._file_history) == 4  # the ceiling: everything produced is still in RAM


def test_append_rejects_json_because_arrays_do_not_concatenate():
    with pytest.raises(ValueError, match="append"):
        FileOutputSpec(path="out.json", fmt="json", append=True)


def test_append_survives_the_wire_round_trip():
    from ms_flow.core.data.contracts import OUTPUT_WIRE_KEY, wire_to_output_spec

    spec = FileOutputSpec(path="out.txt", fmt="text", append=True)
    assert wire_to_output_spec(spec.to_wire()[OUTPUT_WIRE_KEY]).append is True


def test_per_batch_writes_one_numbered_file_per_flush(tmp_path):
    handler = _handler(tmp_path, per_batch=True)
    handler.write_batch(_batch(handler, "a", "b"))
    receipt = handler.write_batch(_batch(handler, "c", "d"))

    assert (tmp_path / "out.000000.txt").read_text() == "a\nb"
    assert (tmp_path / "out.000001.txt").read_text() == "c\nd"
    assert handler._file_history == []  # only the manifest stays in memory
    assert [Path(p).name for p in handler._file_manifest] == ["out.000000.txt", "out.000001.txt"]
    assert len(next(iter(receipt.values()))["batch_paths"]) == 2


def test_append_and_per_batch_are_exclusive():
    with pytest.raises(ValueError, match="exclusive"):
        FileOutputSpec(path="out.txt", fmt="text", append=True, per_batch=True)
