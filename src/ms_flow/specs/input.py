from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence

from ms_flow.core.data import FileInputSpec
from ms_flow.query import QuerySpec, db_pages


def _coerce_payload_map(value: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): item for key, item in dict(value).items()}


def _normalize_batch_size(raw_value: Any, *, default: int) -> int:
    if raw_value in (None, ""):
        return max(1, int(default))
    return max(1, int(raw_value))


@dataclass(frozen=True)
class InputSource:
    batch_size: int = 100
    item_key: str = "items"
    predicates: tuple[Callable[[Mapping[str, Any]], bool], ...] = ()

    def iter_items(self, params: dict[str, Any], config: dict[str, Any]) -> Iterator[dict[str, Any]]:
        raise NotImplementedError

    def filter(self, predicate: Callable[[Mapping[str, Any]], bool]) -> "InputSource":
        """Narrow the source without walking it: for what the db cannot answer
        (the file exists, the pair is already computed). Applied inside `iter_chunks`."""
        return replace(self, predicates=(*self.predicates, predicate))

    def _iter_kept(self, params: dict[str, Any], config: dict[str, Any]) -> Iterator[dict[str, Any]]:
        for item in self.iter_items(params, config):
            if all(keep(item) for keep in self.predicates):
                yield item

    def iter_chunks(
        self,
        params: Mapping[str, Any] | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        params_map = dict(params or {})
        config_map = dict(config or {})
        effective_batch_size = _normalize_batch_size(params_map.get("batch_size"), default=self.batch_size)
        extra_payload = {k: v for k, v in params_map.items() if str(k) != "batch_size"}

        batch: list[dict[str, Any]] = []
        for item in self._iter_kept(params_map, config_map):
            batch.append(_coerce_payload_map(item))
            if len(batch) >= effective_batch_size:
                yield {self.item_key: list(batch), **extra_payload}
                batch.clear()
        if batch:
            yield {self.item_key: list(batch), **extra_payload}


@dataclass(frozen=True)
class SimpleInputSource(InputSource):
    items: Iterable[Mapping[str, Any]] = field(default_factory=tuple)

    def iter_items(self, params: dict[str, Any], config: dict[str, Any]) -> Iterator[dict[str, Any]]:
        del params, config
        for item in self.items:
            yield _coerce_payload_map(item)

    def iter_chunks(
        self,
        params: Mapping[str, Any] | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        params_map = dict(params or {})
        config_map = dict(config or {})
        effective_batch_size = _normalize_batch_size(params_map.get("batch_size"), default=self.batch_size)
        extra_payload = {k: v for k, v in params_map.items() if str(k) != "batch_size"}

        if effective_batch_size <= 1:
            for item in self._iter_kept(params_map, config_map):
                yield {**item, **extra_payload}
            return

        batch: list[dict[str, Any]] = []
        for item in self._iter_kept(params_map, config_map):
            batch.append(item)
            if len(batch) >= effective_batch_size:
                yield {self.item_key: list(batch), **extra_payload}
                batch.clear()
        if batch:
            yield {self.item_key: list(batch), **extra_payload}


@dataclass(frozen=True)
class InlineItemsInput(InputSource):
    items: Iterable[Mapping[str, Any]] = field(default_factory=tuple)

    def iter_items(self, params: dict[str, Any], config: dict[str, Any]) -> Iterator[dict[str, Any]]:
        del params, config
        for item in self.items:
            yield _coerce_payload_map(item)


@dataclass(frozen=True)
class ProjectTableInput(InputSource):
    table: str = ""
    fields: tuple[str, ...] = ()
    filters: dict[str, Any] = field(default_factory=dict)
    order: tuple[str, ...] = ()
    fetch_size: int = 500
    key: str = "id"

    def iter_items(self, params: dict[str, Any], config: dict[str, Any]) -> Iterator[dict[str, Any]]:
        project_db = config.get("project_db")
        if project_db is None:
            raise ValueError("ProjectTableInput requires project_db in config.")
        spec = QuerySpec(
            table=self.table,
            fields=self.fields,
            filters=self.filters,
            order=self.order,
        )
        yield from db_pages(project_db, spec, key=self.key, page_size=max(1, int(self.fetch_size)))


@dataclass(frozen=True)
class ProjectQueryInput(InputSource):
    query: QuerySpec | None = None
    fetch_size: int = 500
    key: str = "id"

    def iter_items(self, params: dict[str, Any], config: dict[str, Any]) -> Iterator[dict[str, Any]]:
        project_db = config.get("project_db")
        if project_db is None:
            raise ValueError("ProjectQueryInput requires project_db in config.")
        if self.query is None:
            raise ValueError("ProjectQueryInput requires a QuerySpec.")
        yield from db_pages(
            project_db, self.query, key=self.key, page_size=max(1, int(self.fetch_size))
        )


@dataclass(frozen=True)
class ProjectFileInput(InputSource):
    path: str = ""
    field_name: str = "file"
    root: str = "project"
    fmt: str = "binary"
    encoding: str = "utf-8"
    delivery: str = "content"
    cache: bool = False

    def iter_items(self, params: dict[str, Any], config: dict[str, Any]) -> Iterator[dict[str, Any]]:
        del params, config
        yield {
            self.field_name: FileInputSpec(
                path=str(self.path),
                root=str(self.root),
                fmt=str(self.fmt),
                encoding=str(self.encoding),
                delivery=str(self.delivery),
                cache=bool(self.cache),
            )
        }


def inline_items(
    items: Iterable[Mapping[str, Any]],
    *,
    batch_size: int = 100,
    item_key: str = "items",
) -> InlineItemsInput:
    return InlineItemsInput(items=items, batch_size=batch_size, item_key=item_key)


def simple_items(
    items: Iterable[Mapping[str, Any]],
    *,
    batch_size: int = 1,
    item_key: str = "items",
) -> SimpleInputSource:
    return SimpleInputSource(items=items, batch_size=batch_size, item_key=item_key)


def project_table(
    table: str,
    *,
    fields: Sequence[str],
    filters: Mapping[str, Any] | None = None,
    order: Sequence[str] | None = None,
    batch_size: int = 100,
    item_key: str = "items",
    fetch_size: int = 500,
    key: str = "id",
) -> ProjectTableInput:
    return ProjectTableInput(
        table=str(table),
        fields=tuple(str(field) for field in fields),
        filters=dict(filters or {}),
        order=tuple(str(item) for item in (order or ())),
        batch_size=batch_size,
        item_key=item_key,
        fetch_size=fetch_size,
        key=str(key),
    )


def project_query(
    query: QuerySpec,
    *,
    batch_size: int = 100,
    item_key: str = "items",
    fetch_size: int = 500,
    key: str = "id",
) -> ProjectQueryInput:
    return ProjectQueryInput(
        query=query,
        batch_size=batch_size,
        item_key=item_key,
        fetch_size=fetch_size,
        key=str(key),
    )


def project_file(
    path: str | Path,
    *,
    field_name: str = "file",
    root: str = "project",
    fmt: str = "binary",
    encoding: str = "utf-8",
    delivery: str = "content",
    cache: bool = False,
) -> ProjectFileInput:
    return ProjectFileInput(
        path=str(path),
        field_name=str(field_name),
        root=str(root),
        fmt=str(fmt),
        encoding=str(encoding),
        delivery=str(delivery),
        cache=bool(cache),
        batch_size=1,
        item_key="items",
    )


__all__ = [
    "InlineItemsInput",
    "InputSource",
    "ProjectFileInput",
    "ProjectQueryInput",
    "ProjectTableInput",
    "inline_items",
    "project_file",
    "project_query",
    "project_table",
    "SimpleInputSource",
    "simple_items",
]
