from __future__ import annotations

from typing import Iterable

from pydantic import BaseModel

from ms_flow.core.callable_refs import resolve_callable_ref
from ms_flow.tasking import CapabilitySpec, JobDefinition, JobSpec, RequirementSpec


class ExampleParams(BaseModel):
    values: list[int]
    batch_size: int = 2


class ExampleJobSpec(JobSpec):
    name = "example_jobspec"
    description = "Example class-based job declaration."
    params_model = ExampleParams
    executor = "thread"
    supported_executors = ("thread",)
    store_results = False
    required = (
        RequirementSpec(entity_kind="ligand", capability="has_3d"),
    )
    produces = (
        CapabilitySpec(entity_kind="ligand", capability="prepared_for:vina", artifact_kind="molecule", format="pdbqt"),
    )

    @staticmethod
    def build_chunks(params: dict, config: dict | None = None) -> Iterable[dict]:
        del config
        parsed = ExampleParams(**params)
        for start in range(0, len(parsed.values), parsed.batch_size):
            yield {"values": parsed.values[start:start + parsed.batch_size]}

    @staticmethod
    def run_chunk(payload: dict, progress=None):
        if progress is not None:
            progress(100.0)
        return {"total": sum(payload["values"])}


def test_jobspec_compiles_to_job_definition_with_contract_metadata():
    job_def = ExampleJobSpec.to_job_definition()

    assert isinstance(job_def, JobDefinition)
    assert job_def.name == "example_jobspec"
    assert job_def.params_model is ExampleParams
    assert job_def.required[0].capability == "has_3d"
    assert job_def.produces[0].capability == "prepared_for:vina"
    assert job_def.store_results is False


def test_jobspec_uses_importable_callable_refs():
    job_def = ExampleJobSpec.to_job_definition()

    assert resolve_callable_ref(job_def.task.handler_ref) is ExampleJobSpec.run_chunk
    assert resolve_callable_ref(job_def.chunker_ref) is ExampleJobSpec.build_chunks


def test_jobspec_builds_chunks_and_runs_task():
    job_def = ExampleJobSpec.to_job_definition()

    chunks = list(job_def.build_chunks({"values": [1, 2, 3], "batch_size": 2}))

    assert chunks == [{"values": [1, 2]}, {"values": [3]}]
    assert job_def.task(chunks[0]) == {"total": 3}
