"""Recommended public surface: the facade, projects, settings, and the names to write a job.

The engine itself (`Engine`, executors, `Flow`) is `ms_flow.engine`.
"""

from ms_flow.core.apps import AppManifest, AppRegistry
from ms_flow.core.app_settings import AppSettingSpec
from ms_flow.core.configuration import ConfigurationEntry, PydanticConfiguration
from ms_flow.artifacts import ArtifactRegistry
from ms_flow.core.data import FileArtifact, FileInputSpec, ProjectOutputDirSpec
from ms_flow.engine import JobSnapshot, job, report, task
from ms_flow.main import Workspace
from ms_flow.core.project.catalog import (
    AppProjectCatalog,
    ProjectCatalog,
    ProjectCatalogBackend,
    ProjectCatalogEditor,
    ProjectLauncher,
)
from ms_flow.core.project.resources import (
    ProjectResource,
    ProjectResourceContract,
    ProjectResourceSpec,
)
from ms_flow.runtime import AppRuntime
from ms_flow.sinks import file_sink, graph_sink, table_sink
from ms_flow.specs import (
    inline_items,
    project_file,
    project_file_out,
    project_query,
    project_table,
    project_table_out,
)

__all__ = [
    "AppManifest",
    "AppProjectCatalog",
    "AppRegistry",
    "AppRuntime",
    "AppSettingSpec",
    "ArtifactRegistry",
    "ConfigurationEntry",
    "FileArtifact",
    "FileInputSpec",
    "JobSnapshot",
    "ProjectCatalog",
    "ProjectCatalogBackend",
    "ProjectCatalogEditor",
    "ProjectLauncher",
    "ProjectOutputDirSpec",
    "ProjectResource",
    "ProjectResourceContract",
    "ProjectResourceSpec",
    "PydanticConfiguration",
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
]
