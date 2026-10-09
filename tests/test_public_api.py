import ms_flow
import ms_flow.advanced as public_advanced
import ms_flow.api as public_api
import ms_flow.engine as public_engine
import ms_flow.query as public_query


def test_public_api_module_exports_recommended_runtime_surface():
    # Minimum contract, not a closed list: removing a name breaks its importers,
    # adding one does not. With `==` this test failed every time the API grew.
    assert set(public_api.__all__) >= {
        "AppManifest",
        "AppProjectCatalog",
        "AppRegistry",
        "AppRuntime",
        "ArtifactRegistry",
        "JobSnapshot",
        "ProjectCatalog",
        "ProjectCatalogBackend",
        "ProjectCatalogEditor",
        "ProjectLauncher",
        "ProjectResource",
        "ProjectResourceContract",
        "ProjectResourceSpec",
        "Workspace",
        "file_sink",
        "graph_sink",
        "inline_items",
        "job",
        "project_file",
        "project_file_out",
        "project_query",
        "project_table",
        "project_table_out",
        "report",
        "table_sink",
        "task",
    }
    assert public_api.__all__ == sorted(public_api.__all__)
    assert all(hasattr(public_api, name) for name in public_api.__all__)
    assert public_api.Workspace is ms_flow.Workspace
    assert public_api.AppRuntime is ms_flow.AppRuntime
    assert public_api.JobSnapshot is public_engine.JobSnapshot


def test_advanced_module_is_explicit_and_not_mixed_into_simple_api_surface():
    assert public_advanced.__all__ == ["WorkspaceAdvancedAccess"]
    assert hasattr(public_advanced, "WorkspaceAdvancedAccess")
    assert not hasattr(public_api, "WorkspaceAdvancedAccess")


def test_query_module_exports_query_surface():
    assert public_query.__all__ == [
        "QuerySpec",
        "Rows",
        "db_input_for",
        "db_count",
        "db_pages",
        "db_rows",
        "db_stream",
    ]


def test_top_level_package_is_a_minimal_entrypoint():
    assert ms_flow.__all__ == [
        "AppRuntime", "BaseRuntime", "MultipleResultsFound", "NoResultFound", "Selection", "Workspace",
        "file_sink", "graph_sink", "job", "report", "table_sink", "task",
    ]
    assert all(getattr(ms_flow, name) is not None for name in ms_flow.__all__)
    assert ms_flow.job is public_engine.job and ms_flow.report is public_engine.report
    for name in ("ProjectCatalogBackend", "QuerySpec", "db_rows", "inline_items", "project_table", "Engine"):
        assert not hasattr(ms_flow, name)


def test_the_facade_has_one_way_to_run_a_job():
    for gone in ("run", "submit_job", "task", "job", "wait_for_job", "executor_manager", "executor_db"):
        assert not hasattr(ms_flow.Workspace, gone)
    assert hasattr(ms_flow.Workspace, "submit") and hasattr(ms_flow.Workspace, "engine")
