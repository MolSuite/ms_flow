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
            ProjectResourceSpec(key="ligands", relative_path="data/ligands"),
            ProjectResourceSpec(key="docking_results", relative_path="results/docking"),
        ),
    )


def test_project_catalog_lists_injected_manifests_with_filter(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    manifests = [
        _build_manifest("amdockvs", "docking", "AMDockVS"),
        _build_manifest("chemflow", "chemistry", "ChemFlow"),
    ]
    catalog = AppProjectCatalog(
        "amdockvs",
        manifests=manifests,
        discover_apps=False,
    )

    try:
        apps = catalog.list_apps()

        assert [app.app_id for app in apps] == ["amdockvs"]
        assert not hasattr(catalog, "executor_manager")
    finally:
        catalog.shutdown()


def test_project_catalog_creates_project_using_manifest_contract(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    catalog = ProjectCatalog(
        manifests=[_build_manifest("amdockvs", "docking", "AMDockVS")],
        discover_apps=False,
    )
    editor = ProjectCatalogEditor(
        manifests=[_build_manifest("amdockvs", "docking", "AMDockVS")],
        discover_apps=False,
    )

    try:
        context = editor.create_project(
            app_id="amdockvs",
            name="dock-project",
            folder=tmp_path / "dock-project",
            description="catalog project",
            tags=["demo"],
        )
        stored = catalog.get_project(context.id)

        assert stored.app_id == "amdockvs"
        assert stored.scope == "docking"
        assert catalog.get_total_projects() == 1
        assert [project.name for project in catalog.list_projects()] == ["dock-project"]
    finally:
        editor.shutdown()
        catalog.shutdown()


def test_project_catalog_creates_declared_project_resource_dirs(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    manifest = _build_manifest_with_resources("amdockvs", "docking", "AMDockVS")
    editor = ProjectCatalogEditor(manifests=[manifest], discover_apps=False)

    try:
        project_root = tmp_path / "dock-resource-project"
        editor.create_project(
            app_id="amdockvs",
            name="dock-resource-project",
            folder=project_root,
            description="catalog project",
        )

        assert (project_root / "data" / "ligands").is_dir()
        assert (project_root / "results" / "docking").is_dir()
    finally:
        editor.shutdown()


def test_app_project_catalog_filters_shared_database_by_app(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    manifests = [
        _build_manifest("amdockvs", "docking", "AMDockVS"),
        _build_manifest("otherapp", "other", "Other App"),
    ]
    global_catalog = ProjectCatalog(manifests=manifests, discover_apps=False)
    global_editor = ProjectCatalogEditor(manifests=manifests, discover_apps=False)
    scoped_catalog = AppProjectCatalog("amdockvs", manifests=manifests, discover_apps=False)

    try:
        global_editor.create_project(
            app_id="amdockvs",
            name="dock-project-a",
            folder=tmp_path / "dock-project-a",
            description="amdock project",
        )
        global_editor.create_project(
            app_id="otherapp",
            name="other-project-a",
            folder=tmp_path / "other-project-a",
            description="other project",
        )

        rows = scoped_catalog.list_projects(page=1, items_per_page=10)

        assert scoped_catalog.get_total_projects() == 1
        assert [project.name for project in rows] == ["dock-project-a"]
        assert all(project.app_id == "amdockvs" for project in rows)
    finally:
        scoped_catalog.shutdown()
        global_editor.shutdown()
        global_catalog.shutdown()
