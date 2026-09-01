from __future__ import annotations

from datetime import datetime
from pathlib import Path
from uuid import UUID

from sqlalchemy import cast, func, or_, String
from sqlmodel import select

from ms_flow.core.database.master_models import Project


class ProjectRepository:
    """Project repository over the Master DB."""

    def __init__(self, master_db, app_id_filter: str | None = None):
        self.master_db = master_db
        self.app_id_filter = (app_id_filter or "").strip() or None

        self.sort_mode = "recent"
        self.filter_mode = "all"
        self.search_field = "name"
        self.search_query = ""

    @staticmethod
    def parse_tags(raw_tags: str | None) -> list[str]:
        if not raw_tags:
            return []
        return [tag.strip() for tag in raw_tags.split(",") if tag.strip()]

    @staticmethod
    def serialize_tags(tags: list[str]) -> str:
        clean_tags = []
        for tag in tags:
            normalized = tag.strip()
            if normalized and normalized not in clean_tags:
                clean_tags.append(normalized)
        return ", ".join(clean_tags)

    def set_sort_mode(self, sort_mode: str):
        self.sort_mode = sort_mode

    def set_filter_mode(self, filter_mode: str):
        self.filter_mode = filter_mode

    def set_search(self, field: str, query: str):
        self.search_field = field
        self.search_query = query.strip().lower()

    def _build_conditions(self):
        conditions = []

        if self.app_id_filter is not None:
            conditions.append(Project.app_id == self.app_id_filter)

        if self.filter_mode == "has_description":
            conditions.append(func.length(func.trim(Project.description)) > 0)
        elif self.filter_mode == "has_tags":
            conditions.append(func.length(func.trim(Project.tags)) > 0)
        elif self.filter_mode == "favorites":
            conditions.append(Project.favorite.is_(True))


        if self.search_query:
            q = f"%{self.search_query}%"
            search_map = {
                "name": func.lower(Project.name).like(q),
                "description": func.lower(Project.description).like(q),
                "path": func.lower(Project.path).like(q),
                "tags": func.lower(Project.tags).like(q),
                "id": func.lower(cast(Project.id, String)).like(q),
            }
            if self.search_field == "all":
                conditions.append(or_(*search_map.values()))
            else:
                conditions.append(search_map.get(self.search_field, search_map["name"]))

        return conditions

    def get_projects_paginated(self, page: int, items_per_page: int) -> list[Project]:
        offset = (page - 1) * items_per_page
        order_map = {
            "recent": Project.updated_at.desc(),
            "name_asc": Project.name.asc(),
            "name_desc": Project.name.desc(),
            "path_asc": Project.path.asc(),
        }
        order_clause = order_map.get(self.sort_mode, Project.updated_at.desc())
        conditions = self._build_conditions()

        with self.master_db.get_session() as session:
            statement = (
                select(Project)
                .where(*conditions)
                .order_by(order_clause)
                .offset(offset)
                .limit(items_per_page)
            )
            return session.exec(statement).all()

    def get_total_projects(self) -> int:
        conditions = self._build_conditions()
        with self.master_db.get_session() as session:
            statement = select(func.count()).select_from(Project).where(*conditions)
            return int(session.exec(statement).one() or 0)

    def assert_unique_path(self, folder: Path | str, exclude_id: UUID | None = None):
        folder_path = Path(folder).expanduser().resolve()
        with self.master_db.get_session() as session:
            statement = select(Project).where(Project.path == str(folder_path))
            if exclude_id is not None:
                statement = statement.where(Project.id != exclude_id)
            existing = session.exec(statement).first()
            if existing:
                raise ValueError(
                    f"A project already exists at that path (ID: {existing.id}). "
                    "Use another path or edit the existing project."
                )

    @staticmethod
    def _normalize_id(project_id) -> UUID:
        try:
            return UUID(str(project_id))
        except ValueError as exc:
            raise ValueError("Invalid project id.") from exc

    def get_project_by_id(self, project_id) -> Project:
        normalized_id = self._normalize_id(project_id)
        with self.master_db.get_session() as session:
            if self.app_id_filter is None:
                project = session.get(Project, normalized_id)
            else:
                statement = select(Project).where(
                    Project.id == normalized_id,
                    Project.app_id == self.app_id_filter,
                )
                project = session.exec(statement).first()
            if project is None:
                raise ValueError("Project not found.")
            return project

    def get_project_by_path(self, folder: Path | str) -> Project:
        folder_path = Path(folder).expanduser().resolve()
        with self.master_db.get_session() as session:
            statement = select(Project).where(Project.path == str(folder_path))
            if self.app_id_filter is not None:
                statement = statement.where(Project.app_id == self.app_id_filter)
            project = session.exec(statement).first()
            if project is None:
                raise ValueError("Project not found.")
            return project

    def get_project_by_name(self, name: str) -> Project:
        with self.master_db.get_session() as session:
            statement = select(Project).where(Project.name == name)
            if self.app_id_filter is not None:
                statement = statement.where(Project.app_id == self.app_id_filter)
            project = session.exec(statement).first()
            if project is None:
                raise ValueError("Project not found.")
            return project

    def touch_project(self, project_id):
        normalized_id = self._normalize_id(project_id)
        with self.master_db.get_session() as session:
            project = session.get(Project, normalized_id)
            if project is None:
                raise ValueError("Project not found.")
            project.updated_at = datetime.now()
            session.add(project)
            session.commit()

    def create_project_record(
        self,
        project_id,
        name: str,
        folder: Path | str,
        description: str,
        app_id: str = "",
        scope: str = "full",
        tags: list[str] | None = None,
    ):
        folder_path = Path(folder).expanduser().resolve()
        self.assert_unique_path(folder_path)

        with self.master_db.get_session() as session:
            session.add(
                Project(
                    id=self._normalize_id(project_id),
                    name=name,
                    path=str(folder_path),
                    app_id=app_id,
                    description=description,
                    scope=scope or "full",
                    tags=self.serialize_tags(tags or []),
                    favorite=False
                )
            )
            session.commit()

    def update_project(
        self,
        project_id,
        name: str,
        folder: Path | str,
        description: str,
        tags: list[str],
        app_id: str | None = None,
        scope: str | None = None,
    ):
        folder_path = Path(folder).expanduser().resolve()
        folder_path.mkdir(parents=True, exist_ok=True)
        try:
            normalized_id = UUID(str(project_id))
        except ValueError as exc:
            raise ValueError("Invalid project id.") from exc

        with self.master_db.get_session() as session:
            project = session.get(Project, normalized_id)
            if project is None:
                raise ValueError("Project to edit not found.")
        self.assert_unique_path(folder_path, exclude_id=normalized_id)

        with self.master_db.get_session() as session:
            project = session.get(Project, normalized_id)
            if project is None:
                raise ValueError("Project to edit not found.")
            project.name = name
            project.path = str(folder_path)
            if app_id is not None:
                project.app_id = app_id
            project.description = description
            if scope is not None:
                project.scope = scope or "full"
            project.tags = self.serialize_tags(tags)
            project.updated_at = datetime.now()
            session.add(project)
            session.commit()

    def toggle_favorite(self, project_ids):
        with self.master_db.get_session() as session:
            for project_id in project_ids:
                try:
                    normalized_id = UUID(str(project_id))
                except ValueError:
                    continue
                project = session.get(Project, normalized_id)
                if project is not None:
                    project.favorite = not bool(project.favorite)
                    project.updated_at = datetime.now()
                    session.add(project)
            session.commit()

    def delete_projects(self, project_ids):
        with self.master_db.get_session() as session:
            for project_id in project_ids:
                try:
                    normalized_id = UUID(str(project_id))
                except ValueError:
                    continue
                project = session.get(Project, normalized_id)
                if project is not None:
                    session.delete(project)
            session.commit()
