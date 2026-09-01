from pathlib import Path

import toml
from pydantic import BaseModel, Field

from ms_flow.core.configuration import PydanticConfiguration


class _PreviewSettings(BaseModel):
    limit: int = Field(default=50, ge=1, title="Preview limit")


class _Settings(BaseModel):
    previews: _PreviewSettings


def _provider(tmp_path: Path) -> PydanticConfiguration:
    default_path = tmp_path / "package" / "defaults.toml"
    default_path.parent.mkdir()
    default_path.write_text("[previews]\nlimit = 50\n", encoding="utf-8")
    return PydanticConfiguration(
        config_id="viewer",
        display_name="Viewer",
        model_type=_Settings,
        default_path=default_path,
        global_path=tmp_path / "user" / "viewer.toml",
        project_relative_path=".molsuite/config/viewer.toml",
    )


def test_configuration_layers_default_global_and_project(tmp_path):
    provider = _provider(tmp_path)
    assert provider.get_value("previews.limit") == 50
    assert provider.get_source("previews.limit") == "default"

    provider.set_value("previews.limit", 60)
    assert provider.get_source("previews.limit") == "global"
    assert toml.load(provider.global_path)["previews"]["limit"] == 60

    provider.set_project_root(tmp_path / "project")
    assert provider.get_value("previews.limit") == 60
    provider.set_value("previews.limit", 70)
    assert provider.get_source("previews.limit") == "project"

    provider.reset_value("previews.limit", "global")
    assert provider.get_value("previews.limit") == 60
    assert provider.get_source("previews.limit") == "global"

    provider.reset_value("previews.limit", "default")
    assert provider.get_value("previews.limit") == 50
    assert provider.get_source("previews.limit") == "project"


def test_configuration_rejects_invalid_value_without_persisting(tmp_path):
    provider = _provider(tmp_path)
    try:
        provider.set_value("previews.limit", 0)
    except ValueError:
        pass
    else:
        raise AssertionError("Expected Pydantic validation to reject the value.")

    assert provider.get_value("previews.limit") == 50
    assert not provider.global_path.exists()
