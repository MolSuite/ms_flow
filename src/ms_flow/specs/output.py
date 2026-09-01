from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from ms_flow.sinks import file_sink, table_sink


@dataclass(frozen=True)
class OutputSink:
    def get_output_spec(self, config: dict[str, Any]) -> Any:
        del config
        raise NotImplementedError


@dataclass(frozen=True)
class ProjectTableOutput(OutputSink):
    table: str
    columns: tuple[str, ...] = ()

    def get_output_spec(self, config: dict[str, Any]) -> Any:
        del config
        return table_sink(self.table, columns=self.columns)


@dataclass(frozen=True)
class ProjectFileOutput(OutputSink):
    path: str
    root: str = "project"
    fmt: str = "json"
    encoding: str = "utf-8"
    ensure_parent: bool = True

    def get_output_spec(self, config: dict[str, Any]) -> Any:
        del config
        return file_sink(
            self.path,
            root=self.root,
            fmt=self.fmt,
            encoding=self.encoding,
            ensure_parent=self.ensure_parent,
        )


def project_table_out(table: str, *, columns: Iterable[str] = ()) -> ProjectTableOutput:
    return ProjectTableOutput(table=str(table), columns=tuple(str(col) for col in columns))


def project_file_out(
    path: str | Path,
    *,
    root: str = "project",
    fmt: str = "json",
    encoding: str = "utf-8",
    ensure_parent: bool = True,
) -> ProjectFileOutput:
    return ProjectFileOutput(
        path=str(path),
        root=str(root),
        fmt=str(fmt),
        encoding=str(encoding),
        ensure_parent=bool(ensure_parent),
    )


__all__ = [
    "OutputSink",
    "ProjectFileOutput",
    "ProjectTableOutput",
    "project_file_out",
    "project_table_out",
]
