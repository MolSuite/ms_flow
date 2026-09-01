from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

from ms_flow.core.data.backends import (
    InputBackendBase,
    LocalFileBackend,
    OutputBackendBase,
    build_default_input_backends,
    build_default_output_backends,
)
from ms_flow.core.data.contracts import (
    RAY_FILE_INPUT_KEY,
    RAY_OUTPUT_DIR_KEY,
    DataContractError,
    DbOutputSpec,
    FileInputSpec,
    InputSpec,
    OutputSpec,
    ProjectOutputDirSpec,
    from_wire_value,
)
from ms_flow.core.data.planner import DataTransportPlanner
from ms_flow.core.data.project_output import persist_project_output
from ms_flow.core.data.runtime import DataContext, ExecutorTransportProfile, ResolvedHandle


class DataBridge:
    def __init__(
        self,
        *,
        input_backends: Optional[Mapping[str, InputBackendBase]] = None,
        output_backends: Optional[Mapping[str, OutputBackendBase]] = None,
        planner: Optional[DataTransportPlanner] = None,
        project_output_persister: Optional[Callable[[DbOutputSpec, Any, DataContext], dict[str, Any]]] = None,
    ):
        self._input_backends: dict[str, InputBackendBase] = build_default_input_backends()
        self._output_backends: dict[str, OutputBackendBase] = build_default_output_backends()
        if input_backends:
            self._input_backends.update(dict(input_backends))
        if output_backends:
            self._output_backends.update(dict(output_backends))
        self._planner = planner or DataTransportPlanner()
        self._project_output_persister = project_output_persister or persist_project_output

    def _resolve_file_path(self, spec: FileInputSpec, context: DataContext) -> Path:
        backend = self._input_backends.get("file")
        if not isinstance(backend, LocalFileBackend):
            raise DataContractError("File input backend is not LocalFileBackend.")
        return backend.resolve_path(spec.path, spec.root, context)

    @staticmethod
    def _resolve_project_output_dir(spec: ProjectOutputDirSpec, context: DataContext) -> Path:
        if context.project_dir is None:
            raise DataContractError("ProjectOutputDirSpec requires project_dir in DataContext.")
        root = context.project_dir.expanduser().resolve()
        raw = Path(spec.path).expanduser()
        target = raw.resolve() if raw.is_absolute() else (root / raw).resolve()
        try:
            target.relative_to(root)
        except ValueError as exc:
            raise DataContractError("ProjectOutputDirSpec must stay inside the project directory.") from exc
        return target

    @staticmethod
    def _hpc_stage_destination(*, source: Path, context: DataContext) -> Path:
        raw_wdir = context.extras.get("hpc_wdir")
        if not raw_wdir:
            raise DataContractError("HPC transport requires 'hpc_wdir' in DataContext.")
        wdir = Path(str(raw_wdir)).expanduser().resolve()
        job_id = str(context.extras.get("job_id") or "job")
        chunk_id = str(context.extras.get("chunk_id") or "chunk")
        base = wdir / "molsuite_staging" / job_id / chunk_id
        base.mkdir(parents=True, exist_ok=True)
        return (base / source.name).resolve()

    def _stage_file_to_hpc(self, spec: FileInputSpec, context: DataContext) -> str:
        source = self._resolve_file_path(spec, context)
        target = self._hpc_stage_destination(source=source, context=context)
        shutil.copy2(source, target)
        return str(target)

    def _ray_file_input(self, spec: FileInputSpec, context: DataContext) -> dict[str, Any]:
        source = self._resolve_file_path(spec, context)
        if not source.is_file():
            raise DataContractError(f"Ray file input not found: {source}")
        return {
            RAY_FILE_INPUT_KEY: {
                "path": str(source),
                "name": source.name,
                "fmt": spec.fmt,
                "encoding": spec.encoding,
                "delivery": spec.delivery,
                "cache": spec.cache,
            }
        }

    def resolve_input(self, spec: InputSpec, context: DataContext) -> Any:
        backend = self._input_backends.get(spec.kind)
        if backend is None:
            raise DataContractError(f"No input backend registered for kind '{spec.kind}'.")
        return backend.read(spec, context)

    def persist_output(self, spec: OutputSpec, data: Any, context: DataContext) -> dict[str, Any]:
        if isinstance(spec, DbOutputSpec) and str(spec.db_role or "").strip().lower() == "project":
            return self._project_output_persister(spec, data, context)
        backend = self._output_backends.get(spec.kind)
        if backend is None:
            raise DataContractError(f"No output backend registered for kind '{spec.kind}'.")
        return backend.write(spec, data, context)

    def output_commits_exist(
        self, spec: OutputSpec, context: DataContext, commit_keys: list[str]
    ) -> set[str]:
        """Probe which commit keys are already recorded for this sink (recovery dedup)."""
        if isinstance(spec, DbOutputSpec) and str(spec.db_role or "").strip().lower() == "project":
            try:
                from ms_flow.core.database import ProjectStore

                sink_key = str(context.extras.get("molsuite_output_sink_key") or "").strip()
                if context.project_db_path is None or not sink_key:
                    return set()
                store = ProjectStore.open_cached(context.project_db_path)
                return store.output_commits_exist(sink_key, list(commit_keys))
            except Exception:
                return set()
        backend = self._output_backends.get(spec.kind)
        probe = getattr(backend, "output_commits_exist", None)
        if probe is None:
            return set()
        try:
            return set(probe(spec, context, list(commit_keys)))
        except Exception:
            return set()

    def materialize_payload(
        self,
        payload: Any,
        context: DataContext,
        *,
        executor_profile: Optional[ExecutorTransportProfile] = None,
    ) -> Any:
        decoded = from_wire_value(payload)
        profile = executor_profile or ExecutorTransportProfile()
        return self._materialize(decoded, context, profile)

    def _materialize(self, value: Any, context: DataContext, profile: ExecutorTransportProfile) -> Any:
        if isinstance(value, InputSpec):
            handle = self._planner.plan_input(value, context=context, profile=profile)
            if isinstance(value, FileInputSpec) and handle.strategy == "hpc_staged_copy":
                return self._stage_file_to_hpc(value, context)
            if isinstance(value, FileInputSpec) and handle.strategy == "ray_object_transfer":
                return self._ray_file_input(value, context)
            if isinstance(value, FileInputSpec) and handle.strategy in {"local_path", "shared_path"}:
                return str(self._resolve_file_path(value, context))
            if isinstance(value, ProjectOutputDirSpec):
                target = self._resolve_project_output_dir(value, context)
                relative = target.relative_to(context.project_dir.expanduser().resolve()).as_posix()
                if handle.strategy == "ray_output_transfer":
                    return {RAY_OUTPUT_DIR_KEY: {"destination": relative}}
                target.mkdir(parents=True, exist_ok=True)
                return str(target)
            return self.resolve_input(value, context)
        if isinstance(value, dict):
            return {key: self._materialize(item, context, profile) for key, item in value.items()}
        if isinstance(value, list):
            return [self._materialize(item, context, profile) for item in value]
        return value


__all__ = [
    "DataBridge",
    "DataContext",
    "DataTransportPlanner",
    "ExecutorTransportProfile",
    "ResolvedHandle",
]
