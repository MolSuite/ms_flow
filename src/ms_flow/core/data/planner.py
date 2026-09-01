from __future__ import annotations

from ms_flow.core.data.contracts import (
    BytesInputSpec,
    DataContractError,
    DbInputSpec,
    FileInputSpec,
    InlineInputSpec,
    InputSpec,
    ProjectOutputDirSpec,
)
from ms_flow.core.data.runtime import DataContext, ExecutorTransportProfile, ResolvedHandle


class DataTransportPlanner:
    @staticmethod
    def _is_remote(profile: ExecutorTransportProfile) -> bool:
        backend = profile.backend
        mode = profile.mode
        if backend in {"thread", "process_pool", "process_pool_loky", "local"}:
            return False
        if backend in {"ray"}:
            return mode in {"managed", "external"}
        if backend == "hpc":
            return True
        return mode in {"managed", "external"}

    def plan_input(
        self,
        spec: InputSpec,
        *,
        context: DataContext,
        profile: ExecutorTransportProfile,
    ) -> ResolvedHandle:
        del context
        remote = self._is_remote(profile)

        if isinstance(spec, (InlineInputSpec, BytesInputSpec)):
            return ResolvedHandle(strategy="inline_payload", spec_kind=spec.kind)

        if isinstance(spec, FileInputSpec):
            if not remote:
                strategy = "local_path" if spec.delivery == "path" else "driver_materialized_file"
                return ResolvedHandle(strategy=strategy, spec_kind=spec.kind, details={"path": spec.path})
            if profile.backend == "hpc":
                return ResolvedHandle(strategy="hpc_staged_copy", spec_kind=spec.kind, details={"path": spec.path})
            if profile.backend == "ray" and spec.cache:
                return ResolvedHandle(strategy="ray_object_transfer", spec_kind=spec.kind, details={"path": spec.path})
            if profile.shared_fs and spec.delivery == "path":
                return ResolvedHandle(strategy="shared_path", spec_kind=spec.kind, details={"path": spec.path})
            if profile.backend == "ray" and not profile.shared_fs:
                return ResolvedHandle(strategy="ray_object_transfer", spec_kind=spec.kind, details={"path": spec.path})
            return ResolvedHandle(strategy="driver_materialized_file", spec_kind=spec.kind)

        if isinstance(spec, ProjectOutputDirSpec):
            if not remote or profile.shared_fs:
                return ResolvedHandle(strategy="project_output_path", spec_kind=spec.kind)
            if profile.backend == "ray":
                return ResolvedHandle(strategy="ray_output_transfer", spec_kind=spec.kind)
            raise DataContractError("Remote project output directories require a shared filesystem or Ray transfer.")

        if isinstance(spec, DbInputSpec):
            if not remote:
                return ResolvedHandle(strategy="direct_db_access", spec_kind=spec.kind)
            # DB inputs are resolved by the MF process during staging; workers
            # receive the resulting serializable payload, never a DB handle.
            return ResolvedHandle(strategy="driver_materialized_db", spec_kind=spec.kind)

        raise DataContractError(f"No transport strategy for input spec kind '{spec.kind}'.")
