from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Mapping

from sqlmodel import select

from ms_flow.core.database import resolve_project_store
from ms_flow.core.database.project_models import (
    ProjectArtifact,
    ProjectArtifactCapability,
    ProjectOperationRun,
)


def _json_text(payload: Mapping[str, Any] | None) -> str:
    return json.dumps(dict(payload or {}), ensure_ascii=True, default=str)


class ArtifactRegistry:
    """Small project-level registry for artifacts and their capabilities."""

    def __init__(self, project_source, *, app_id: str = ""):
        if project_source is None:
            raise RuntimeError("ArtifactRegistry requires an active project store.")
        self.project_store = resolve_project_store(project_source)
        self.app_id = str(app_id or "").strip()

    def register(
        self,
        *,
        entity_kind: str,
        entity_id: int | None = None,
        artifact_kind: str = "",
        role: str = "",
        path: str = "",
        ref: str = "",
        format: str = "",
        status: str = "available",
        producer_job_id: str = "",
        metadata: Mapping[str, Any] | None = None,
    ) -> ProjectArtifact:
        now = datetime.now()
        row = ProjectArtifact(
            app_id=self.app_id,
            entity_kind=str(entity_kind or "").strip(),
            entity_id=None if entity_id is None else int(entity_id),
            artifact_kind=str(artifact_kind or "").strip(),
            role=str(role or "").strip(),
            path=str(path or ""),
            ref=str(ref or ""),
            format=str(format or "").strip(),
            status=str(status or "available").strip(),
            producer_job_id=str(producer_job_id or ""),
            metadata_json=_json_text(metadata),
            created_at=now,
            updated_at=now,
        )
        if not row.entity_kind:
            raise ValueError("entity_kind must not be empty.")
        with self.project_store.get_session() as session:
            session.add(row)
            session.commit()
            session.refresh(row)
            return row

    def add_capability(
        self,
        artifact_id: int,
        capability: str,
        *,
        value: Mapping[str, Any] | None = None,
        method: str = "",
        params: Mapping[str, Any] | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> ProjectArtifactCapability:
        row = ProjectArtifactCapability(
            artifact_id=int(artifact_id),
            capability=str(capability or "").strip(),
            value_json=_json_text(value),
            method=str(method or "").strip(),
            params_json=_json_text(params),
            metadata_json=_json_text(metadata),
        )
        if row.artifact_id <= 0:
            raise ValueError("artifact_id must be positive.")
        if not row.capability:
            raise ValueError("capability must not be empty.")
        with self.project_store.get_session() as session:
            artifact = session.get(ProjectArtifact, int(artifact_id))
            if artifact is None:
                raise ValueError(f"Artifact {artifact_id} does not exist.")
            session.add(row)
            session.commit()
            session.refresh(row)
            return row

    def find(
        self,
        *,
        entity_kind: str | None = None,
        entity_id: int | None = None,
        capability: str | None = None,
        artifact_kind: str | None = None,
        role: str | None = None,
        ref: str | None = None,
        format: str | None = None,
        status: str | None = "available",
        limit: int | None = None,
    ) -> list[ProjectArtifact]:
        with self.project_store.get_session() as session:
            statement = select(ProjectArtifact)
            if self.app_id:
                statement = statement.where(ProjectArtifact.app_id == self.app_id)
            if entity_kind is not None:
                statement = statement.where(ProjectArtifact.entity_kind == str(entity_kind))
            if entity_id is not None:
                statement = statement.where(ProjectArtifact.entity_id == int(entity_id))
            if artifact_kind is not None:
                statement = statement.where(ProjectArtifact.artifact_kind == str(artifact_kind))
            if role is not None:
                statement = statement.where(ProjectArtifact.role == str(role))
            if ref is not None:
                statement = statement.where(ProjectArtifact.ref == str(ref))
            if format is not None:
                statement = statement.where(ProjectArtifact.format == str(format))
            if status is not None:
                statement = statement.where(ProjectArtifact.status == str(status))
            if capability is not None:
                statement = (
                    statement.join(
                        ProjectArtifactCapability,
                        ProjectArtifactCapability.artifact_id == ProjectArtifact.id,
                    )
                    .where(ProjectArtifactCapability.capability == str(capability))
                )
            statement = statement.order_by(ProjectArtifact.id.desc())
            if limit is not None:
                statement = statement.limit(max(0, int(limit)))
            return list(session.exec(statement))

    def current(self, **filters) -> ProjectArtifact | None:
        rows = self.find(limit=1, **filters)
        return rows[0] if rows else None

    def update_artifact(
        self,
        artifact_id: int,
        *,
        status: str | None = None,
        producer_job_id: str | None = None,
        path: str | None = None,
        ref: str | None = None,
        format: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        merge_metadata: bool = True,
    ) -> ProjectArtifact:
        with self.project_store.get_session() as session:
            artifact = session.get(ProjectArtifact, int(artifact_id))
            if artifact is None:
                raise ValueError(f"Artifact {artifact_id} does not exist.")

            if status is not None:
                artifact.status = str(status or "").strip() or artifact.status
            if producer_job_id is not None:
                artifact.producer_job_id = str(producer_job_id or "")
            if path is not None:
                artifact.path = str(path or "")
            if ref is not None:
                artifact.ref = str(ref or "")
            if format is not None:
                artifact.format = str(format or "").strip()
            if metadata is not None:
                next_metadata = dict(metadata)
                if merge_metadata:
                    current_metadata = json.loads(artifact.metadata_json or "{}")
                    if isinstance(current_metadata, dict):
                        current_metadata.update(next_metadata)
                        next_metadata = current_metadata
                artifact.metadata_json = _json_text(next_metadata)
            artifact.updated_at = datetime.now()
            session.add(artifact)
            session.commit()
            session.refresh(artifact)
            return artifact

    def record_scope_state(
        self,
        *,
        scope_kind: str,
        scope_id: int | None = None,
        snapshot_ref: str,
        state_kind: str,
        status: str = "pending",
        producer_job_id: str = "",
        params_hash: str = "",
        coverage_total: int | None = None,
        coverage_ready: int | None = None,
        coverage_failed: int | None = None,
        metadata: Mapping[str, Any] | None = None,
        upsert: bool = True,
    ) -> ProjectArtifact:
        normalized_snapshot_ref = str(snapshot_ref or "").strip()
        normalized_scope_kind = str(scope_kind or "").strip()
        normalized_state_kind = str(state_kind or "").strip()
        if not normalized_scope_kind:
            raise ValueError("scope_kind must not be empty.")
        if not normalized_snapshot_ref:
            raise ValueError("snapshot_ref must not be empty.")
        if not normalized_state_kind:
            raise ValueError("state_kind must not be empty.")

        payload = dict(metadata or {})
        payload["snapshot_ref"] = normalized_snapshot_ref
        if params_hash:
            payload["params_hash"] = str(params_hash)
        if coverage_total is not None:
            payload["coverage_total"] = int(coverage_total)
        if coverage_ready is not None:
            payload["coverage_ready"] = int(coverage_ready)
        if coverage_failed is not None:
            payload["coverage_failed"] = int(coverage_failed)

        existing = None
        if upsert:
            existing = self.current(
                entity_kind=normalized_scope_kind,
                entity_id=scope_id,
                artifact_kind=normalized_state_kind,
                role="scope_state",
                ref=normalized_snapshot_ref,
                status=None,
            )
        if existing is not None:
            return self.update_artifact(
                existing.id,
                status=status,
                producer_job_id=producer_job_id,
                metadata=payload,
                merge_metadata=True,
            )

        return self.register(
            entity_kind=normalized_scope_kind,
            entity_id=scope_id,
            artifact_kind=normalized_state_kind,
            role="scope_state",
            ref=normalized_snapshot_ref,
            status=status,
            producer_job_id=producer_job_id,
            metadata=payload,
        )

    def current_scope_state(
        self,
        *,
        scope_kind: str,
        scope_id: int | None = None,
        snapshot_ref: str,
        state_kind: str,
    ) -> ProjectArtifact | None:
        return self.current(
            entity_kind=str(scope_kind or "").strip(),
            entity_id=scope_id,
            artifact_kind=str(state_kind or "").strip(),
            role="scope_state",
            ref=str(snapshot_ref or "").strip(),
            status=None,
        )

    def entity_ids_with_capability(
        self,
        *,
        entity_kind: str,
        capability: str,
        entity_ids: list[int] | tuple[int, ...] | set[int] | None = None,
        status: str | None = "available",
    ) -> set[int]:
        normalized_ids = {int(value) for value in (entity_ids or ()) if int(value) > 0}
        with self.project_store.get_session() as session:
            statement = (
                select(ProjectArtifact.entity_id)
                .join(
                    ProjectArtifactCapability,
                    ProjectArtifactCapability.artifact_id == ProjectArtifact.id,
                )
                .where(ProjectArtifact.entity_kind == str(entity_kind))
                .where(ProjectArtifactCapability.capability == str(capability))
                .where(ProjectArtifact.entity_id.is_not(None))
            )
            if self.app_id:
                statement = statement.where(ProjectArtifact.app_id == self.app_id)
            if status is not None:
                statement = statement.where(ProjectArtifact.status == str(status))
            if normalized_ids:
                statement = statement.where(ProjectArtifact.entity_id.in_(sorted(normalized_ids)))
            rows = session.exec(statement).all()
        return {int(value) for value in rows if value is not None}

    def record_operation_run(
        self,
        *,
        operation_name: str,
        job_id: str = "",
        status: str = "pending",
        input_ref: Mapping[str, Any] | None = None,
        output_ref: Mapping[str, Any] | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> ProjectOperationRun:
        row = ProjectOperationRun(
            app_id=self.app_id,
            operation_name=str(operation_name or "").strip(),
            job_id=str(job_id or ""),
            status=str(status or "pending").strip(),
            input_ref_json=_json_text(input_ref),
            output_ref_json=_json_text(output_ref),
            params_json=_json_text(params),
        )
        if not row.operation_name:
            raise ValueError("operation_name must not be empty.")
        with self.project_store.get_session() as session:
            session.add(row)
            session.commit()
            session.refresh(row)
            return row


__all__ = ["ArtifactRegistry"]
