from pathlib import Path

import toml

from ms_flow.core.settings.manager import SettingsManager
from ms_flow.core.app_settings import AppSettingSpec


def _build_manager(tmp_path, monkeypatch):
    fake_home = tmp_path
    monkeypatch.setattr("ms_flow.core.settings.manager.Path.home", lambda: fake_home)
    sm = SettingsManager()
    return sm, fake_home


def test_init_creates_global_config_file(tmp_path, monkeypatch):
    sm, fake_home = _build_manager(tmp_path, monkeypatch)
    assert sm.global_path == fake_home / ".molsuite" / "config.toml"
    assert sm.global_path.exists()


def test_update_setting_persists_global_value(tmp_path, monkeypatch):
    sm, _ = _build_manager(tmp_path, monkeypatch)
    sm.update_setting("general.log_level", "DEBUG")

    assert sm.settings.general.log_level == "DEBUG"
    saved = toml.load(sm.global_path)
    assert saved["general"]["log_level"] == "DEBUG"


def test_local_project_override_does_not_replace_global(tmp_path, monkeypatch):
    sm, fake_home = _build_manager(tmp_path, monkeypatch)
    sm.update_setting("general.poll_interval", 9)

    project_dir = fake_home / "project-a"
    project_dir.mkdir(parents=True, exist_ok=True)
    sm.set_project(project_dir)

    sm.update_setting("general.poll_interval", 2)
    assert sm.settings.general.poll_interval == 2

    sm.clear_project()
    assert sm.settings.general.poll_interval == 9


def test_update_setting_with_save_global_too_updates_global(tmp_path, monkeypatch):
    sm, fake_home = _build_manager(tmp_path, monkeypatch)
    project_dir = fake_home / "project-b"
    project_dir.mkdir(parents=True, exist_ok=True)
    sm.set_project(project_dir)

    sm.update_setting("general.log_level", "WARNING", save_global_too=True)
    sm.clear_project()

    assert sm.settings.general.log_level == "WARNING"


def test_executor_db_defaults_next_to_projects_db(tmp_path, monkeypatch):
    sm, _ = _build_manager(tmp_path, monkeypatch)
    assert sm.settings.executor_db == sm.settings.projects_db.parent / "executor.db"


def test_logging_settings_defaults_exist(tmp_path, monkeypatch):
    sm, _ = _build_manager(tmp_path, monkeypatch)
    assert sm.settings.logging.max_file_size_mb == 10
    assert sm.settings.logging.backup_count == 10
    assert sm.settings.logging.retention_days == 30
    assert sm.settings.logging.app_level == "INFO"
    assert sm.settings.logging.executor_level == "INFO"
    assert sm.settings.logging.project_level == "INFO"


def test_update_logging_level_persists(tmp_path, monkeypatch):
    sm, _ = _build_manager(tmp_path, monkeypatch)
    sm.update_setting("logging.app_level", "DEBUG")
    assert sm.settings.logging.app_level == "DEBUG"
    saved = toml.load(sm.global_path)
    assert saved["logging"]["app_level"] == "DEBUG"


def test_settings_manager_exposes_packaged_configuration_provider(tmp_path, monkeypatch):
    sm, _ = _build_manager(tmp_path, monkeypatch)

    assert sm.config_id == "ms_flow"
    assert sm.default_path.name == "defaults.toml"
    assert sm.default_path.exists()
    assert "general.log_level" in {entry.path for entry in sm.entries()}
    assert not any(entry.path.startswith("applications") for entry in sm.entries())


def test_executor_db_is_derived_from_custom_projects_db_when_missing_in_config(tmp_path, monkeypatch):
    fake_home = tmp_path
    monkeypatch.setattr("ms_flow.core.settings.manager.Path.home", lambda: fake_home)
    monkeypatch.setattr("ms_flow.core.settings.models.Path.home", lambda: fake_home)

    config_path = fake_home / ".molsuite" / "config.toml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    custom_projects_db = fake_home / "custom_store" / "projects.db"
    with config_path.open("w") as f:
        toml.dump({"projects_db": str(custom_projects_db), "logging": {"executor_level": "DEBUG"}}, f)

    sm = SettingsManager()
    assert sm.settings.projects_db == custom_projects_db
    assert sm.settings.executor_db == custom_projects_db.parent / "executor.db"
    assert sm.settings.logging.executor_level == "DEBUG"


def test_app_setting_defaults_and_project_overrides_are_persisted(tmp_path, monkeypatch):
    sm, fake_home = _build_manager(tmp_path, monkeypatch)
    spec = AppSettingSpec(
        key="preview_limit",
        default=50,
        value_type="integer",
        minimum=1,
    )
    sm.register_app_settings("testdock", (spec,))

    assert sm.get_app_setting("testdock", "preview_limit") == 50
    assert sm.app_setting_specs("testdock") == (spec,)

    project_dir = fake_home / "project-with-app-setting"
    project_dir.mkdir(parents=True)
    sm.set_project(project_dir)
    sm.update_app_setting("testdock", "preview_limit", 75)

    assert sm.get_app_setting("testdock", "preview_limit") == 75
    saved = toml.load(project_dir / "config.toml")
    assert saved["applications"]["testdock"]["preview_limit"] == 75

    sm.clear_project()
    assert sm.get_app_setting("testdock", "preview_limit") == 50


def test_app_setting_rejects_invalid_values_without_persisting_them(tmp_path, monkeypatch):
    sm, _ = _build_manager(tmp_path, monkeypatch)
    sm.register_app_settings(
        "testdock",
        (AppSettingSpec(key="preview_limit", default=50, value_type="integer", minimum=1),),
    )

    try:
        sm.update_app_setting("testdock", "preview_limit", 0)
    except ValueError as exc:
        assert "must be >= 1" in str(exc)
    else:
        raise AssertionError("Expected invalid app setting value to be rejected.")

    assert sm.get_app_setting("testdock", "preview_limit") == 50
    saved = toml.load(sm.global_path)
    assert saved.get("applications", {}).get("testdock", {}).get("preview_limit") is None


def test_empty_legacy_contract_removes_historical_values(tmp_path, monkeypatch):
    fake_home = tmp_path
    monkeypatch.setattr("ms_flow.core.settings.manager.Path.home", lambda: fake_home)
    config_path = fake_home / ".molsuite" / "config.toml"
    config_path.parent.mkdir(parents=True)
    with config_path.open("w", encoding="utf-8") as handle:
        toml.dump({"applications": {"testdock": {"preview_limit": 60}}}, handle)

    sm = SettingsManager()
    assert sm.register_app_settings("testdock", ()) == ()
    assert "testdock" not in sm.settings.applications
    assert "testdock" not in toml.load(config_path).get("applications", {})
