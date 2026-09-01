from __future__ import annotations

from typing import Any

from ms_flow.core.data.contracts import DataContractError, DbOutputSpec
from ms_flow.core.data.runtime import DataContext


def clear_project_store_cache() -> None:
    from ms_flow.core.database import ProjectStore

    ProjectStore.clear_cached_stores()


def _project_db_path_from_context(context: DataContext):
    db_path = context.project_db_path
    if db_path is None:
        raise DataContractError("project output requires project_db_path in DataContext.")
    return db_path.expanduser().resolve()


def persist_project_output(spec: DbOutputSpec, data: Any, context: DataContext) -> dict[str, Any]:
    from ms_flow.core.database import ProjectStore

    store = ProjectStore.open_cached(_project_db_path_from_context(context))
    commit_key = ProjectStore.commit_key_from_extras(context.extras)
    return store.persist_output_spec(spec, data, commit_key=commit_key)
