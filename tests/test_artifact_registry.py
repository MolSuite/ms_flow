from __future__ import annotations

import json

from ms_flow.artifacts import ArtifactRegistry
from ms_flow.core.database import ProjectStore


def test_artifact_registry_registers_and_finds_capabilities(tmp_path):
    project_db = ProjectStore()
    project_db.connect(tmp_path / "project")
    try:
        registry = ArtifactRegistry(project_db, app_id="amdockvs")

        artifact = registry.register(
            entity_kind="ligand",
            entity_id=1,
            artifact_kind="molecule",
            role="docking_input",
            path="/tmp/ligand_1.pdbqt",
            format="pdbqt",
            producer_job_id="job-1",
        )
        capability = registry.add_capability(
            artifact.id,
            "prepared_for:vina",
            method="meeko",
            params={"engine": "vina"},
        )

        rows = registry.find(entity_kind="ligand", entity_id=1, capability="prepared_for:vina")
        ready_ids = registry.entity_ids_with_capability(
            entity_kind="ligand",
            capability="prepared_for:vina",
            entity_ids=[1, 2, 3],
        )

        assert capability.artifact_id == artifact.id
        assert len(rows) == 1
        assert ready_ids == {1}
        assert rows[0].path == "/tmp/ligand_1.pdbqt"
        assert registry.current(entity_kind="ligand", entity_id=1, capability="prepared_for:vina").id == artifact.id
    finally:
        project_db.disconnect()


def test_artifact_registry_records_operation_runs(tmp_path):
    project_db = ProjectStore()
    project_db.connect(tmp_path / "project")
    try:
        registry = ArtifactRegistry(project_db, app_id="amdockvs")

        run = registry.record_operation_run(
            operation_name="docking.vina",
            job_id="job-2",
            input_ref={"ligand_set_id": 10},
            params={"vina_cpu": 1},
        )

        assert run.operation_name == "docking.vina"
        assert run.job_id == "job-2"
        assert run.status == "pending"
    finally:
        project_db.disconnect()


def test_artifact_registry_records_scope_state_per_snapshot(tmp_path):
    project_db = ProjectStore()
    project_db.connect(tmp_path / "project")
    try:
        registry = ArtifactRegistry(project_db, app_id="amdockvs")

        pending = registry.record_scope_state(
            scope_kind="ligand_set",
            scope_id=10,
            snapshot_ref="selection:sha256:abc",
            state_kind="ligand_3d",
            status="pending",
            producer_job_id="job-3d-1",
            coverage_total=100,
            coverage_ready=0,
        )
        running = registry.record_scope_state(
            scope_kind="ligand_set",
            scope_id=10,
            snapshot_ref="selection:sha256:abc",
            state_kind="ligand_3d",
            status="running",
            producer_job_id="job-3d-1",
            coverage_total=100,
            coverage_ready=25,
            params_hash="rdkit-etkdg:v1",
        )
        other_snapshot = registry.record_scope_state(
            scope_kind="ligand_set",
            scope_id=10,
            snapshot_ref="selection:sha256:def",
            state_kind="ligand_3d",
            status="completed",
            producer_job_id="job-3d-2",
            coverage_total=80,
            coverage_ready=80,
        )

        current = registry.current_scope_state(
            scope_kind="ligand_set",
            scope_id=10,
            snapshot_ref="selection:sha256:abc",
            state_kind="ligand_3d",
        )
        rows = registry.find(
            entity_kind="ligand_set",
            entity_id=10,
            artifact_kind="ligand_3d",
            role="scope_state",
            status=None,
        )

        assert pending.id == running.id
        assert current is not None
        assert current.id == running.id
        assert current.status == "running"
        assert current.producer_job_id == "job-3d-1"
        assert current.ref == "selection:sha256:abc"

        metadata = json.loads(current.metadata_json)
        assert metadata["snapshot_ref"] == "selection:sha256:abc"
        assert metadata["coverage_total"] == 100
        assert metadata["coverage_ready"] == 25
        assert metadata["params_hash"] == "rdkit-etkdg:v1"

        assert other_snapshot.id != running.id
        assert len(rows) == 2
    finally:
        project_db.disconnect()
