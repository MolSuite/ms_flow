from datetime import datetime
from pathlib import Path
from uuid import UUID, uuid4

from pydantic import AliasChoices, BaseModel, Field

from ms_flow.core.settings.manager import Settings


class ProjectContext(BaseModel):
    name: str
    path: Path
    app_id: str = ""
    scope: str = "full"
    settings: Settings
    id: UUID = Field(default_factory=uuid4)
    description: str = ""
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(
        default_factory=datetime.now,
        validation_alias=AliasChoices("updated_at", "update_at"),
    )


class ProjectDataContext(BaseModel):
    """
    Minimal context for loaders/queries of the active project.

    It avoids coupling the core to app facades or runtime internals.
    """

    molsuite: object
    active_context: object | None = None
    project_store_handle: object | None = None
    project_resources: dict[str, object] = Field(default_factory=dict)

    @property
    def project_db(self) -> object | None:
        # Delegated (not frozen at construction) so the context always
        # reflects the currently active project.
        return getattr(self.molsuite, "project_db", None)

    @property
    def project_store(self) -> object | None:
        return getattr(self.molsuite, "project_store", None)
