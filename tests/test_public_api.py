import inspect
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path

import ms_flow
import ms_flow.api as public_api
import ms_flow.advanced as public_advanced
import ms_flow.query as public_query
import ms_flow.job_templates as public_job_templates
from ms_flow.core.executor import ExecutorManager
from ms_flow.core.executor.job_snapshot import JobSnapshot


def test_public_api_module_exports_recommended_runtime_surface():
    # Minimum contract, not a closed list: removing a name breaks its importers,
    # adding one does not. With `==` this test failed every time the API grew.
    assert set(public_api.__all__) >= {
        "AppManifest",
        "AppProjectCatalog",
        "AppRegistry",
        "AppRuntime",
        "ArtifactRegistry",
        "CapabilitySpec",
        "JobSnapshot",
        "JobSpec",
        "MolSuite",
        "ProjectCatalog",
        "ProjectCatalogBackend",
        "ProjectCatalogEditor",
        "ProjectLauncher",
        "ProjectResource",
        "ProjectResourceContract",
        "ProjectResourceSpec",
        "RequirementSpec",
        "WorkflowSpec",
        "batch_job",
        "file_sink",
        "graph_sink",
        "inline_items",
        "project_file",
        "project_file_out",
        "project_query",
        "project_table",
        "project_table_out",
        "streaming_job",
        "table_sink",
        "workflow",
    }
    assert public_api.__all__ == sorted(public_api.__all__)
    assert public_api.MolSuite is ms_flow.MolSuite
    assert public_api.AppRuntime is ms_flow.AppRuntime
    assert hasattr(public_api, "batch_job")
    assert hasattr(public_api, "file_sink")
    assert hasattr(public_api, "graph_sink")
    assert hasattr(public_api, "inline_items")
    assert hasattr(public_api, "project_table")
    assert hasattr(public_api, "project_table_out")
    assert hasattr(public_api, "ProjectResourceSpec")
    assert hasattr(public_api, "streaming_job")
    assert hasattr(public_api, "table_sink")
    assert hasattr(public_api, "workflow")
    assert not hasattr(public_api, "build_streaming_job_definition")


def test_job_templates_module_keeps_advanced_builder_outside_runtime_surface():
    assert public_job_templates.__all__ == [
        "batch_job",
        "build_streaming_job_definition",
        "streaming_job",
    ]
    assert hasattr(public_job_templates, "build_streaming_job_definition")


def test_advanced_module_is_explicit_and_not_mixed_into_simple_api_surface():
    assert public_advanced.__all__ == ["MolSuiteAdvancedAccess"]
    assert hasattr(public_advanced, "MolSuiteAdvancedAccess")
    assert not hasattr(public_api, "MolSuiteAdvancedAccess")


def test_query_module_exports_query_surface():
    assert public_query.__all__ == [
        "QuerySpec",
        "db_input_for",
        "db_count",
        "db_pages",
        "db_rows",
        "db_stream",
    ]


def test_top_level_package_is_a_minimal_entrypoint():
    assert ms_flow.MolSuite is public_api.MolSuite
    assert ms_flow.AppRuntime is public_api.AppRuntime
    assert hasattr(ms_flow, "BaseRuntime")
    assert ms_flow.__all__ == ["AppRuntime", "BaseRuntime", "MolSuite"]
    assert not hasattr(ms_flow, "ProjectCatalogBackend")
    assert not hasattr(ms_flow, "batch_job")
    assert not hasattr(ms_flow, "QuerySpec")
    assert not hasattr(ms_flow, "db_input_for")
    assert not hasattr(ms_flow, "file_sink")
    assert not hasattr(ms_flow, "graph_sink")
    assert not hasattr(ms_flow, "inline_items")
    assert not hasattr(ms_flow, "db_rows")
    assert not hasattr(ms_flow, "db_stream")
    assert not hasattr(ms_flow, "project_table")
    assert not hasattr(ms_flow, "project_table_out")
    assert not hasattr(ms_flow, "task")
    assert not hasattr(ms_flow, "job")
    assert not hasattr(ms_flow, "table_sink")
    assert not hasattr(ms_flow, "workflow")


def test_molsuite_no_longer_exposes_plugin_action_runtime_api():
    assert not hasattr(ms_flow.MolSuite, "submit_plugin_action")
    assert not hasattr(ms_flow.MolSuite, "run_plugin_action_sync")


def test_dispatch_api_surface_no_longer_exposes_window_size_controls():
    run_sig = inspect.signature(public_api.MolSuite.run)
    submit_sig = inspect.signature(ExecutorManager.submit_job)

    assert "window_size" not in run_sig.parameters
    assert "window_size" not in submit_sig.parameters
    assert "params" in run_sig.parameters
    assert "batch_size" in submit_sig.parameters
    assert "max_job_cpu" in submit_sig.parameters
    assert "max_inflight_tasks" in submit_sig.parameters
    assert "total_chunks" in submit_sig.parameters
    assert "max_inflight_items" in submit_sig.parameters
    assert "prefetch_factor" in submit_sig.parameters
    assert "refill_threshold" in submit_sig.parameters


def test_job_snapshot_keeps_minimal_legacy_indexing_without_full_mapping_surface():
    snapshot = JobSnapshot(
        job_id="job-1",
        origin_id="amdockvs",
        task_type="score",
        status="completed",
        executor_name="thread",
        created_at=datetime.now(),
    )

    assert snapshot["status"] == "completed"
    assert snapshot.get("chunks_done") == 0
    assert snapshot.to_mapping()["executor_name"] == "thread"
    assert not isinstance(snapshot, Mapping)
