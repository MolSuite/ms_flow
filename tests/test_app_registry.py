import sys
import types
from pathlib import Path

from ms_flow.core.app_settings import AppSettingSpec
from ms_flow.core.apps import AppManifest, AppRegistry
from ms_flow.core.database.master_models import Project
from ms_flow.core.project.resources import ProjectResourceSpec


def _install_manifest_module(module_name: str):
    module = types.ModuleType(module_name)
    module.manifest = AppManifest(
        app_id="testcouple",
        scope_id="coupling",
        name="Test Couple",
        version="0.1.0",
        entry_module="testcouple.app",
        package_name="testcouple",
    )
    sys.modules[module_name] = module
    return module


def test_app_registry_discovers_manifest_module():
    module_name = "tests_fake_manifest_module"
    _install_manifest_module(module_name)
    registry = AppRegistry()
    try:
        loaded = registry.discover_modules([module_name])
    finally:
        sys.modules.pop(module_name, None)

    assert len(loaded) == 1
    manifest = registry.get("testcouple")
    assert manifest is not None
    assert manifest.scope_id == "coupling"
    assert manifest.entry_module == "testcouple.app"

def test_app_registry_resolves_project_by_app_id():
    module_name = "tests_fake_manifest_module_resolve"
    _install_manifest_module(module_name)
    registry = AppRegistry()
    try:
        registry.discover_modules([module_name])
    finally:
        sys.modules.pop(module_name, None)

    project = Project(name="couple", path="/tmp/couple", app_id="testcouple", scope="coupling")
    manifest = registry.resolve_for_project(project)

    assert manifest is not None
    assert manifest.app_id == "testcouple"


def test_app_registry_normalizes_project_resources():
    registry = AppRegistry()

    manifest = registry.register(
        AppManifest(
            app_id="testcouple",
            scope_id="coupling",
            name="Test Couple",
            version="0.1.0",
            entry_module="testcouple.app",
            package_name="testcouple",
            project_resources=(
                {"key": "guests", "relative_path": "data/guests", "description": "Guest files"},
                ProjectResourceSpec(key="results", relative_path="results/coupling"),
            ),
        )
    )

    assert tuple(spec.key for spec in manifest.project_resources) == ("guests", "results")
    assert manifest.project_resources[0].relative_path == "data/guests"
    assert manifest.project_resources[0].description == "Guest files"


def test_app_registry_normalizes_app_settings():
    registry = AppRegistry()

    manifest = registry.register(
        AppManifest(
            app_id="testcouple",
            scope_id="coupling",
            name="Test Couple",
            version="0.1.0",
            settings=(
                {
                    "key": "preview_limit",
                    "default": 50,
                    "value_type": "integer",
                    "label": "Preview limit",
                    "minimum": 1,
                },
            ),
        )
    )

    assert manifest.settings == (
        AppSettingSpec(
            key="preview_limit",
            default=50,
            value_type="integer",
            label="Preview limit",
            minimum=1,
        ),
    )


def test_app_registry_discovers_manifest_file_and_records_source_root(tmp_path):
    source_root = tmp_path / "src"
    package_dir = source_root / "coupleapp"
    package_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "manifest.py").write_text(
        "\n".join(
            [
                "from ms_flow.core.apps import AppManifest",
                "manifest = AppManifest(",
                "    app_id='coupleapp',",
                "    scope_id='coupling',",
                "    name='Couple App',",
                "    version='0.1.0',",
                "    entry_module='coupleapp.app',",
                "    package_name='coupleapp',",
                ")",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    registry = AppRegistry()
    loaded = registry.discover_manifest_files([package_dir / "manifest.py"])

    assert len(loaded) == 1
    manifest = registry.get("coupleapp")
    assert manifest is not None
    assert manifest.source_root == source_root.resolve()


def test_app_registry_discovers_workspace_apps_from_sibling_sources(tmp_path):
    workspace_root = tmp_path / "workspace"
    source_root = workspace_root / "CoupleApp" / "src"
    package_dir = source_root / "coupleapp"
    package_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "manifest.py").write_text(
        "\n".join(
            [
                "from ms_flow.core.apps import AppManifest",
                "manifest = AppManifest(",
                "    app_id='coupleapp',",
                "    scope_id='coupling',",
                "    name='Couple App',",
                "    version='0.1.0',",
                "    entry_module='coupleapp.app',",
                "    package_name='coupleapp',",
                ")",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    registry = AppRegistry()
    loaded = registry.discover_workspace_apps(workspace_root)

    assert len(loaded) == 1
    manifest = registry.get("coupleapp")
    assert manifest is not None
    assert manifest.source_root == source_root.resolve()
