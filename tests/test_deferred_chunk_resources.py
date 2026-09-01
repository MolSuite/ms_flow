"""Deferred chunk builds (depends_on + chunker) must keep project_resources.

Regression: a job submitted with depends_on defers its chunk build; the deferred build
rebuilds config from the persisted `_data_context`. If project_resources isn't persisted
there, jobs that resolve a project resource at chunk-build time (e.g. chemistry's 'molecules'
storage dir) crash with "Missing project resource". build_chunk_source_config must surface it.
"""
from types import SimpleNamespace

from ms_flow.core.executor.submission_service import SubmissionService


def test_build_chunk_source_config_restores_project_resources():
    service = SubmissionService(manager=None)  # method doesn't touch the manager
    job = SimpleNamespace(job_id="job-1", project_id="proj-1")
    payload = {
        "_data_context": {
            "project_path": "",
            "project_resources": {"molecules": {"path": "/tmp/proj/data/molecules"}},
        }
    }
    config, _resources = service.build_chunk_source_config(
        job=job, payload=payload, lifecycle_meta={}, cursor_position=0
    )
    assert config["project_resources"]["molecules"]["path"] == "/tmp/proj/data/molecules"
