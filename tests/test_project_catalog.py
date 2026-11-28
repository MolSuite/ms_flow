from pathlib import Path

from ms_flow.api import (
    AppManifest,
    AppProjectCatalog,
    ProjectCatalog,
    ProjectCatalogEditor,
    ProjectResourceSpec,
)


def _patch_fake_home(monkeypatch, fake_home: Path):
    monkeypatch.setattr("ms_flow.core.settings.manager.Path.home", lambda: fake_home)
    monkeypatch.setattr("ms_flow.core.settings.models.Path.home", lambda: fake_home)


def _build_manifest(app_id: str, scope_id: str, name: str) -> AppManifest:
    return AppManifest(
        app_id=app_id,
        scope_id=scope_id,
        name=name,
        version="0.1.0",
        entry_module=f"{app_id}.app",
        package_name=app_id,
    )


def _build_manifest_with_resources(app_id: str, scope_id: str, name: str) -> AppManifest:
    return AppManifest(
        app_id=app_id,
        scope_id=scope_id,
        name=name,
        version="0.1.0",
        entry_module=f"{app_id}.app",
        package_name=app_id,
        project_resources=(
            ProjectResourceSpec(key="guests", relative_path="data/guests"),
            ProjectResourceSpec(key="coupling_results", relative_path="results/coupling"),
        ),
    )


def test_project_catalog_lists_injected_manifests_with_filter(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    manifests = [
        _build_manifest("demoapp", "coupling", "DemoApp"),
        _build_manifest("statflow", "statistics", "StatFlow"),
    ]
    catalog = AppProjectCatalog(
        "demoapp",
        manifests=manifests,
        discover_apps=False,
    )

    try:
        apps = catalog.list_apps()

        assert [app.app_id for app in apps] == ["demoapp"]
        assert not hasattr(catalog, "executor_manager")
    finally:
        catalog.shutdown()


def test_project_catalog_creates_project_using_manifest_contract(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    catalog = ProjectCatalog(
        manifests=[_build_manifest("demoapp", "coupling", "DemoApp")],
        discover_apps=False,
    )
    editor = ProjectCatalogEditor(
        manifests=[_build_manifest("demoapp", "coupling", "DemoApp")],
        discover_apps=False,
    )

    try:
        context = editor.create_project(
            app_id="demoapp",
            name="couple-project",
            folder=tmp_path / "couple-project",
            description="catalog project",
            tags=["demo"],
        )
        stored = catalog.get_project(context.id)

        assert stored.app_id == "demoapp"
        assert stored.scope == "coupling"
        assert catalog.get_total_projects() == 1
        assert [project.name for project in catalog.list_projects()] == ["couple-project"]
    finally:
        editor.shutdown()
        catalog.shutdown()


def test_project_catalog_creates_declared_project_resource_dirs(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    manifest = _build_manifest_with_resources("demoapp", "coupling", "DemoApp")
    editor = ProjectCatalogEditor(manifests=[manifest], discover_apps=False)

    try:
        project_root = tmp_path / "couple-resource-project"
        editor.create_project(
            app_id="demoapp",
            name="couple-resource-project",
            folder=project_root,
            description="catalog project",
        )

        # Resource dirs appear on their first write, not at creation.
        assert not (project_root / "data").exists()
        assert not (project_root / "results").exists()
    finally:
        editor.shutdown()


def test_app_project_catalog_filters_shared_database_by_app(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    manifests = [
        _build_manifest("demoapp", "coupling", "DemoApp"),
        _build_manifest("otherapp", "other", "Other App"),
    ]
    global_catalog = ProjectCatalog(manifests=manifests, discover_apps=False)
    global_editor = ProjectCatalogEditor(manifests=manifests, discover_apps=False)
    scoped_catalog = AppProjectCatalog("demoapp", manifests=manifests, discover_apps=False)

    try:
        global_editor.create_project(
            app_id="demoapp",
            name="couple-project-a",
            folder=tmp_path / "couple-project-a",
            description="demoapp project",
        )
        global_editor.create_project(
            app_id="otherapp",
            name="other-project-a",
            folder=tmp_path / "other-project-a",
            description="other project",
        )

        rows = scoped_catalog.list_projects(page=1, items_per_page=10)

        assert scoped_catalog.get_total_projects() == 1
        assert [project.name for project in rows] == ["couple-project-a"]
        assert all(project.app_id == "demoapp" for project in rows)
    finally:
        scoped_catalog.shutdown()
        global_editor.shutdown()
        global_catalog.shutdown()
