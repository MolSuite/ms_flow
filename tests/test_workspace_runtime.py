from datetime import datetime, timedelta
from pathlib import Path

import pytest
from ms_flow.core.app_settings import AppSettingSpec
from ms_flow.core.database.master_models import Project
from ms_flow.core.project.context import ProjectContext
from ms_flow.core.project.resources import ProjectResourceSpec
from ms_flow.core.settings.models import Settings
from ms_flow.runtime import AppRuntime
from ms_flow.main import Workspace
from ms_flow.runtime import BaseRuntime


def _patch_fake_home(monkeypatch, fake_home: Path):
    monkeypatch.setattr("ms_flow.core.settings.manager.Path.home", lambda: fake_home)
    monkeypatch.setattr("ms_flow.core.settings.models.Path.home", lambda: fake_home)


def test_project_context_accepts_legacy_update_at_input_but_exposes_updated_at():
    now = datetime.now()
    context = ProjectContext(
        name="demo",
        path=Path("/tmp/demo"),
        settings=Settings(),
        update_at=now,
    )

    assert context.updated_at == now


def test_open_project_after_create_project_works_on_molsuite(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "project_alias_runtime"

    ms = Workspace(app_id="testruntime")
    try:
        created = ms.create_or_open_project(
            name="project_alias_runtime",
            folder=project_dir,
            description="runtime alias test",
            activate=False,
        )
        opened = ms.open_project(created.id)

        assert opened.id == created.id
        assert ms.active_context is not None
        assert ms.active_context.id == created.id
    finally:
        ms.shutdown()


def test_register_task_canceller_requires_active_project_and_is_project_scoped(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "project_canceller_scope"

    ms = Workspace(app_id="testruntime")
    try:
        with pytest.raises(RuntimeError):
            ms.register_task_canceller("00000000-0000-0000-0000-000000000000", lambda: None)

        project = ms.create_or_open_project(
            name="project_canceller_scope",
            folder=project_dir,
            description="canceller scope",
            activate=True,
        )
        ms.register_task_canceller(project.id, lambda: None)
        assert ms._runtime_state.active_project is not None
        assert len(ms._runtime_state.active_project.task_cancellers) == 1

        ms.close_project(cancel_running_tasks=False)
        assert ms._runtime_state.active_project is None
    finally:
        ms.shutdown()


def test_a_canceller_that_fails_keeps_the_project_open(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    ms = Workspace(app_id="testruntime")
    try:
        one = ms.create_or_open_project(name="one", folder=tmp_path / "one", description="", activate=True)
        other = ms.create_or_open_project(name="other", folder=tmp_path / "other", description="", activate=False)
        engine = ms.engine

        def boom():
            raise ValueError("no")

        for leave in (ms.close_project, lambda: ms.open_project(other.id)):
            ms.register_task_canceller(one.id, boom)
            with pytest.raises(RuntimeError, match="Cancellation failed"):
                leave()
            assert ms.active_context.id == one.id and ms.project_db is not None

        ms.close_project()
        assert (ms.active_context, ms.project_db, ms.project_store, ms.project_logger) == (None,) * 4
        assert ms._engine is engine  # the engine outlives the project
    finally:
        ms.shutdown()


def test_switch_project_executes_registered_project_cancellers(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_a_dir = tmp_path / "project_cancel_a"
    project_b_dir = tmp_path / "project_cancel_b"

    ms = Workspace(app_id="testruntime")
    try:
        project_a = ms.create_or_open_project(
            name="project_cancel_a",
            folder=project_a_dir,
            description="cancel a",
            activate=True,
        )
        project_b = ms.create_or_open_project(
            name="project_cancel_b",
            folder=project_b_dir,
            description="cancel b",
            activate=False,
        )

        state = {"count": 0}
        ms.register_task_canceller(project_a.id, lambda: state.__setitem__("count", state["count"] + 1))

        opened = ms.open_project(project_b.id)

        assert opened.id == project_b.id
        assert ms.active_context is not None
        assert ms.active_context.id == project_b.id
        assert state["count"] == 1
    finally:
        ms.shutdown()


def test_entity_loader_context_exposes_project_db(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "project_loader_ctx"

    ms = Workspace(app_id="testruntime")
    try:
        ms.create_or_open_project(
            name="project_loader_ctx",
            folder=project_dir,
            description="loader ctx test",
            scope="docking",
            activate=True,
        )
        loader_ctx = ms.get_entity_loader_context()
        assert loader_ctx.workspace is ms
        assert loader_ctx.active_context is ms.active_context
        assert loader_ctx.project_db is ms.project_db
        assert loader_ctx.project_store is ms.project_store
        assert ms.advanced.project_data_context().project_db is ms.project_db
        assert ms.advanced.project_data_context().project_store is ms.project_store
        assert ms.advanced.project_store is ms.project_store
        assert ms.get_project_store() is ms.project_store
    finally:
        ms.shutdown()


def test_workspace_project_resources_are_resolved_from_active_project(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "project_resources_runtime"

    ms = Workspace(
        app_id="testruntime",
        project_resources=(
            ProjectResourceSpec(key="ligands", relative_path="data/ligands"),
            ProjectResourceSpec(key="docking_results", relative_path="results/docking"),
        ),
    )
    try:
        ms.create_or_open_project(
            name="project_resources_runtime",
            folder=project_dir,
            description="resource contract test",
            activate=True,
        )

        resources = ms.get_project_resource_map()
        loader_ctx = ms.get_project_data_context()

        assert set(resources) == {"docking_results", "ligands"}
        assert resources["ligands"]["relative_path"] == "data/ligands"
        assert Path(resources["ligands"]["path"]) == project_dir / "data" / "ligands"
        assert loader_ctx.project_resources == resources
        assert ms.get_project_resource_path("docking_results", "scores.csv", create_parent=True) == (
            project_dir / "results" / "docking" / "scores.csv"
        )
        assert (project_dir / "results" / "docking").is_dir()
    finally:
        ms.shutdown()


def test_base_runtime_open_project_accepts_owned_project(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    runtime = BaseRuntime("amdockvs")

    try:
        owned_context = runtime.workspace.create_project(
            name="dock-owned",
            folder=tmp_path / "dock-owned",
            description="owned by runtime",
            activate=False,
        )

        opened_context = runtime.open_project(owned_context.id)

        assert opened_context.id == owned_context.id
        assert runtime.active_context is not None
        assert runtime.active_context.app_id == "amdockvs"
    finally:
        runtime.shutdown()


def test_base_runtime_open_project_rejects_foreign_app_project(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    runtime = BaseRuntime("amdockvs")
    foreign_ms = Workspace(app_id="otherapp")

    try:
        foreign_context = foreign_ms.create_project(
            name="dock-foreign",
            folder=tmp_path / "dock-foreign",
            description="foreign app project",
            activate=False,
        )

        with pytest.raises(ValueError, match="otherapp"):
            runtime.open_project(foreign_context.id)

        assert runtime.active_context is None
    finally:
        foreign_ms.shutdown()
        runtime.shutdown()


def test_base_runtime_create_or_open_project_ignores_foreign_name_match(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    runtime = BaseRuntime("amdockvs")
    foreign_ms = Workspace(app_id="otherapp")

    try:
        foreign_context = foreign_ms.create_project(
            name="shared-name",
            folder=tmp_path / "otherapp-project",
            description="foreign app project",
            activate=False,
        )

        own_context = runtime.create_or_open_project(
            name="shared-name",
            folder=tmp_path / "amdockvs-project",
            description="owned by runtime",
        )

        assert own_context.id != foreign_context.id
        assert own_context.app_id == "amdockvs"
        assert own_context.path == (tmp_path / "amdockvs-project")
    finally:
        foreign_ms.shutdown()
        runtime.shutdown()


def test_base_runtime_create_or_open_project_rejects_foreign_path_match(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    runtime = BaseRuntime("amdockvs")
    foreign_ms = Workspace(app_id="otherapp")

    try:
        foreign_ms.create_project(
            name="otherapp-project",
            folder=tmp_path / "shared-folder",
            description="foreign app project",
            activate=False,
        )

        with pytest.raises(ValueError, match="already belongs to project"):
            runtime.create_or_open_project(
                name="amdockvs-project",
                folder=tmp_path / "shared-folder",
                description="should fail",
            )

        assert runtime.active_context is None
    finally:
        foreign_ms.shutdown()
        runtime.shutdown()


def test_workspace_open_project_rejects_foreign_project_id(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    scoped_ms = Workspace(app_id="amdockvs")
    foreign_ms = Workspace(app_id="otherapp")

    try:
        foreign_context = foreign_ms.create_project(
            name="foreign-activate",
            folder=tmp_path / "foreign-activate",
            description="foreign project",
            activate=False,
        )

        with pytest.raises(ValueError, match="otherapp"):
            scoped_ms.open_project(foreign_context.id)

        assert scoped_ms.active_context is None
    finally:
        foreign_ms.shutdown()
        scoped_ms.shutdown()


def test_workspace_find_project_is_scoped_when_names_repeat(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    scoped_ms = Workspace(app_id="amdockvs")
    foreign_ms = Workspace(app_id="otherapp")

    try:
        foreign_ms.create_project(
            name="shared-name",
            folder=tmp_path / "other-shared",
            description="foreign project",
            activate=False,
        )
        own_context = scoped_ms.create_project(
            name="shared-name",
            folder=tmp_path / "own-shared",
            description="own project",
            activate=False,
        )

        found = scoped_ms.find_project(name="shared-name")

        assert found is not None
        assert found.id == own_context.id
        assert found.app_id == "amdockvs"
    finally:
        foreign_ms.shutdown()
        scoped_ms.shutdown()


def test_workspace_scoped_app_id_filters_pagination_at_query_level(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    scoped_ms = Workspace(app_id="amdockvs")

    try:
        now = datetime.now()
        with scoped_ms.master_db.get_session() as session:
            session.add(
                Project(
                    name="amdock-old-1",
                    path=str(tmp_path / "amdock-old-1"),
                    app_id="amdockvs",
                    updated_at=now - timedelta(minutes=4),
                )
            )
            session.add(
                Project(
                    name="amdock-old-2",
                    path=str(tmp_path / "amdock-old-2"),
                    app_id="amdockvs",
                    updated_at=now - timedelta(minutes=3),
                )
            )
            session.add(
                Project(
                    name="other-new-1",
                    path=str(tmp_path / "other-new-1"),
                    app_id="otherapp",
                    updated_at=now - timedelta(minutes=2),
                )
            )
            session.add(
                Project(
                    name="other-new-2",
                    path=str(tmp_path / "other-new-2"),
                    app_id="otherapp",
                    updated_at=now - timedelta(minutes=1),
                )
            )
            session.commit()

        page = scoped_ms.list_projects(page=1, items_per_page=2)

        assert [project.name for project in page] == ["amdock-old-2", "amdock-old-1"]
        assert all(project.app_id == "amdockvs" for project in page)
    finally:
        scoped_ms.shutdown()


def test_workspace_scoped_app_id_filters_pagination_across_multiple_pages(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    scoped_ms = Workspace(app_id="amdockvs")

    try:
        now = datetime.now()
        with scoped_ms.master_db.get_session() as session:
            for index in range(6):
                session.add(
                    Project(
                        name=f"amdock-{index}",
                        path=str(tmp_path / f"amdock-{index}"),
                        app_id="amdockvs",
                        updated_at=now - timedelta(minutes=20 - index),
                    )
                )
                session.add(
                    Project(
                        name=f"other-{index}",
                        path=str(tmp_path / f"other-{index}"),
                        app_id="otherapp",
                        updated_at=now - timedelta(minutes=10 - index),
                    )
                )
            session.commit()

        page1 = scoped_ms.list_projects(page=1, items_per_page=2)
        page2 = scoped_ms.list_projects(page=2, items_per_page=2)
        page3 = scoped_ms.list_projects(page=3, items_per_page=2)

        assert [project.name for project in page1] == ["amdock-5", "amdock-4"]
        assert [project.name for project in page2] == ["amdock-3", "amdock-2"]
        assert [project.name for project in page3] == ["amdock-1", "amdock-0"]
        assert all(project.app_id == "amdockvs" for project in page1 + page2 + page3)
    finally:
        scoped_ms.shutdown()


def test_workspace_scoped_app_id_defaults_project_creation(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    ms = Workspace(app_id="amdockvs")

    try:
        context = ms.create_project(
            name="scoped-project",
            folder=tmp_path / "scoped-project",
            description="scoped runtime",
            activate=False,
        )

        assert context.app_id == "amdockvs"
        assert context.scope == "amdockvs"
    finally:
        ms.shutdown()


def test_app_runtime_exposes_scoped_project_catalog(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    runtime = AppRuntime("amdockvs")

    try:
        catalog = runtime.project_catalog

        assert catalog.app_id == "amdockvs"
        assert runtime.project_catalog is catalog
    finally:
        runtime.shutdown()


def test_app_runtime_exposes_declarative_app_settings(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    setting = AppSettingSpec(
        key="preview_limit",
        default=50,
        value_type="integer",
        minimum=1,
    )
    runtime = AppRuntime("amdockvs", app_settings=(setting,))

    try:
        assert runtime.app_setting_specs() == (setting,)
        assert runtime.get_app_setting("preview_limit") == 50

        runtime.update_app_setting("preview_limit", 75)

        assert runtime.get_app_setting("preview_limit") == 75
    finally:
        runtime.shutdown()


def test_app_runtime_project_resources_follow_manifest_like_contract(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    runtime = AppRuntime(
        "amdockvs",
        project_resources=(
            ProjectResourceSpec(key="ligands", relative_path="data/ligands"),
            ProjectResourceSpec(key="receptors", relative_path="data/receptors"),
        ),
    )

    try:
        runtime.create_project(
            name="amdock-project",
            folder=tmp_path / "amdock-project",
            description="runtime resource test",
        )

        assert tuple(spec.key for spec in runtime.project_resource_specs()) == ("ligands", "receptors")
        assert runtime.get_project_resource_path("ligands") == tmp_path / "amdock-project" / "data" / "ligands"
        assert not (tmp_path / "amdock-project" / "data").exists()
    finally:
        runtime.shutdown()
