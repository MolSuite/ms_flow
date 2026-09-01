"""Recommended public surface for MolSuite's simple, declarative path.

`molsuite.api` exposes the happy path for apps, workflows and reusable jobs.
Advanced engine access is deliberately kept apart in `molsuite.advanced` and
behind `ms.advanced`.
"""

from ms_flow.core.apps import AppManifest, AppRegistry
from ms_flow.core.app_settings import AppSettingSpec
from ms_flow.core.configuration import ConfigurationEntry, PydanticConfiguration
from ms_flow.artifacts import ArtifactRegistry
from ms_flow.core.executor.job_snapshot import JobSnapshot
from ms_flow.core.data import FileArtifact, FileInputSpec, ProjectOutputDirSpec
from ms_flow.job_templates import batch_job, streaming_job
from ms_flow.main import MolSuite
from ms_flow.tasking import CapabilitySpec, JobSpec, RequirementSpec
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
    WorkflowSpec,
    inline_items,
    project_file,
    project_file_out,
    project_query,
    project_table,
    project_table_out,
    workflow,
)

__all__ = [
    "AppManifest",
    "AppProjectCatalog",
    "AppRegistry",
    "AppRuntime",
    "AppSettingSpec",
    "ArtifactRegistry",
    "CapabilitySpec",
    "ConfigurationEntry",
    "FileArtifact",
    "FileInputSpec",
    "JobSnapshot",
    "JobSpec",
    "MolSuite",
    "ProjectCatalog",
    "ProjectCatalogBackend",
    "ProjectCatalogEditor",
    "ProjectLauncher",
    "ProjectOutputDirSpec",
    "ProjectResource",
    "ProjectResourceContract",
    "ProjectResourceSpec",
    "PydanticConfiguration",
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
]
