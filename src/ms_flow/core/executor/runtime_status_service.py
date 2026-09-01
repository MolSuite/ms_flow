from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from ms_flow.core.executor.job_monitoring import (
    build_runtime_health_db_snapshot,
    build_runtime_overview_metrics,
)

if TYPE_CHECKING:
    from ms_flow.core.executor.manager import ExecutorManager


class RuntimeStatusService:
    def __init__(self, manager: "ExecutorManager"):
        self.manager = manager

    def _sink_snapshots(self) -> list[dict[str, Any]]:
        return [
            handler.snapshot()
            for handler in self.manager.job_result_handlers_snapshot().values()
            if hasattr(handler, "snapshot")
        ]

    @staticmethod
    def _build_sink_summary(
        sink_snapshots: list[dict[str, Any]],
        *,
        sink_lag_chunks: int,
        sink_lag_bytes: int,
        oldest_sink_lag_age_s: float | None,
        quota_blocked_jobs: int = 0,
        writer_blocked_jobs: int = 0,
        cpu_blocked_jobs: int = 0,
    ) -> dict[str, Any]:
        summary = {
            "active_sinks": len(sink_snapshots),
            "buffered_items": sum(int(item.get("buffered_items", 0) or 0) for item in sink_snapshots),
            "buffered_bytes": sum(int(item.get("buffered_bytes", 0) or 0) for item in sink_snapshots),
            "flush_failures": sum(int(item.get("flush_failures", 0) or 0) for item in sink_snapshots),
            "retry_count": sum(int(item.get("retry_count", 0) or 0) for item in sink_snapshots),
            "rejected_items": sum(int(item.get("rejected_items", 0) or 0) for item in sink_snapshots),
            "oversized_items": sum(int(item.get("oversized_items", 0) or 0) for item in sink_snapshots),
            "flush_count": sum(int(item.get("flush_count", 0) or 0) for item in sink_snapshots),
            "writer_total_items_written": sum(int(item.get("total_items_written", 0) or 0) for item in sink_snapshots),
            "writer_total_bytes_written": sum(int(item.get("total_bytes_written", 0) or 0) for item in sink_snapshots),
            "writer_last_flush_duration_ms_max": max(
                [float(item.get("last_flush_duration_ms", 0.0) or 0.0) for item in sink_snapshots] or [0.0]
            ),
            "pending_chunks_quota": sum(int(item.get("max_pending_chunks", 0) or 0) for item in sink_snapshots),
            "pending_bytes_quota": sum(int(item.get("max_pending_bytes", 0) or 0) for item in sink_snapshots),
            "lag_chunks": int(sink_lag_chunks),
            "lag_bytes": int(sink_lag_bytes),
            "oldest_lag_age_s": oldest_sink_lag_age_s,
            "quota_blocked_jobs": int(quota_blocked_jobs),
            "writer_blocked_jobs": int(writer_blocked_jobs),
            "cpu_blocked_jobs": int(cpu_blocked_jobs),
        }
        pending_chunks_quota = int(summary["pending_chunks_quota"] or 0)
        pending_bytes_quota = int(summary["pending_bytes_quota"] or 0)
        summary["pending_chunks_pressure"] = (
            round(int(summary["buffered_items"]) / pending_chunks_quota, 6)
            if pending_chunks_quota > 0
            else 0.0
        )
        summary["pending_bytes_pressure"] = (
            round(int(summary["buffered_bytes"]) / pending_bytes_quota, 6)
            if pending_bytes_quota > 0
            else 0.0
        )
        return summary

    def get_status(self) -> dict[str, Any]:
        running_items = self.manager.running_chunks_snapshot()
        executors = self.manager.registered_executors()
        used_cpu = self.manager.used_local_cpu()
        reserved_cpu = self.manager.reserved_cpu()

        chunks_by_executor: dict[str, int] = {name: 0 for name in executors}
        for item in running_items:
            if item.executor_name in chunks_by_executor:
                chunks_by_executor[item.executor_name] += 1

        active_by_executor: dict[str, int] = {name: 0 for name in executors}
        total_active = 0
        backlog_chunks = 0
        sink_lag_chunks = 0
        sink_lag_bytes = 0
        oldest_active_work_age_s = None
        oldest_sink_lag_age_s = None
        quota_blocked_jobs = 0
        writer_blocked_jobs = 0
        cpu_blocked_jobs = 0
        overview = None  # read below via getattr: stays None while unbound from a project db

        if self.manager.executor_db is not None:
            with self.manager.executor_db.get_session() as session:
                overview = build_runtime_overview_metrics(
                    session,
                    executor_names=tuple(executors.keys()),
                    now=datetime.now(),
                )
                active_by_executor = dict(overview.active_by_executor)
                total_active = int(overview.total_active)
                backlog_chunks = int(overview.backlog_chunks)
                sink_lag_chunks = int(overview.sink_lag_chunks)
                sink_lag_bytes = int(overview.sink_lag_bytes)
                oldest_active_work_age_s = overview.oldest_active_work_age_s
                oldest_sink_lag_age_s = overview.oldest_sink_lag_age_s
                quota_blocked_jobs = int(overview.quota_blocked_jobs)
                writer_blocked_jobs = int(overview.writer_blocked_jobs)
                cpu_blocked_jobs = int(overview.cpu_blocked_jobs)

        active_feeds = self.manager.job_feeds_snapshot()
        staging_snapshot = self.manager.staging_runtime_snapshot()
        loop_snapshot = self.manager.loop_runtime_snapshot()
        limits_snapshot = self.manager.runtime_limits_snapshot()

        sink_snapshots = self._sink_snapshots()
        sink_summary = self._build_sink_summary(
            sink_snapshots,
            sink_lag_chunks=sink_lag_chunks,
            sink_lag_bytes=sink_lag_bytes,
            oldest_sink_lag_age_s=oldest_sink_lag_age_s,
            quota_blocked_jobs=quota_blocked_jobs,
            writer_blocked_jobs=writer_blocked_jobs,
            cpu_blocked_jobs=cpu_blocked_jobs,
        )
        feed_summary = {
            "active_feeds": len(active_feeds),
            "pending_emission": sum(
                max(0, int(feed.dispatch_policy.max_inflight_tasks) - int(feed.live_count))
                for feed in active_feeds
                if not feed.exhausted
            ),
            "exhausted_feeds": sum(1 for feed in active_feeds if feed.exhausted),
        }
        persistence_snapshot = self.manager.persistence_coordinator.snapshot()
        operational_summary = {
            "loop_latency_ms": float(loop_snapshot["last_latency_ms"]),
            "backlog_chunks": backlog_chunks,
            "staging_active_tasks": int(staging_snapshot["active_tasks"]),
            "persistence": {
                **persistence_snapshot,
                "lag_pending_units": int(persistence_snapshot.get("pending_transitions", 0) or 0)
                + int(persistence_snapshot.get("dirty_jobs", 0) or 0),
            },
            "sink": {
                "lag_chunks": sink_lag_chunks,
                "lag_bytes": sink_lag_bytes,
                "pending_chunks_pressure": sink_summary["pending_chunks_pressure"],
                "pending_bytes_pressure": sink_summary["pending_bytes_pressure"],
                "writer_blocked_jobs": sink_summary["writer_blocked_jobs"],
                "quota_blocked_jobs": sink_summary["quota_blocked_jobs"],
            },
        }

        return {
            "cpu": {
                "total": self.manager.total_cpu,
                "reserved": reserved_cpu,
                "used": used_cpu,
                "available": self.manager.available_cpu(),
            },
            "gpu": {
                "total": self.manager.total_gpu,
                "used": self.manager.used_gpu(),
                "available": self.manager.available_gpu(),
            },
            "local_budget_policy": self.manager.local_budget_policy_snapshot(),
            "executor_db": {
                "bound": self.manager.executor_db is not None,
                "path": (
                    str(self.manager.executor_db.db_path)
                    if self.manager.executor_db is not None
                    and self.manager.executor_db.db_path is not None
                    else None
                ),
            },
            "executors": {
                name: {
                    "reserved_cpu": adapter.reserved_cpu,
                    "backend": adapter.metadata.backend,
                    "mode": adapter.metadata.mode,
                    "support_level": adapter.metadata.support_level,
                    "shared_fs": adapter.metadata.shared_filesystem,
                    "integration": adapter.health_snapshot().get("integration", "unknown"),
                    "remote_backend": adapter.metadata.backend in {"ray", "hpc"},
                    "local_resource_accounting": self.manager.executor_local_accounting_mode(adapter),
                    "locally_constrained": self.manager.executor_participates_in_local_accounting(adapter),
                    "active_jobs": active_by_executor.get(name, 0),
                    "running_chunks": chunks_by_executor.get(name, 0),
                    "health": adapter.health_snapshot(),
                    **({"used_cpu": used_cpu} if adapter.metadata.consumes_local_cpu_tokens else {}),
                }
                for name, adapter in executors.items()
            },
            "jobs": {
                "total_active": total_active,
                "by_executor": active_by_executor,
                "backlog_chunks": backlog_chunks,
                "oldest_active_work_age_s": oldest_active_work_age_s,
                "blocked_jobs": int(getattr(overview, "blocked_jobs", 0) or 0),
                "blocked_by_reason": dict(getattr(overview, "blocked_by_reason", {}) or {}),
            },
            "loop": loop_snapshot,
            "staging": {
                "active_tasks": int(staging_snapshot["active_tasks"]),
                "capacity": int(staging_snapshot["capacity"]),
                "available_slots": int(staging_snapshot["available_slots"]),
            },
            "feeds": feed_summary,
            "sinks": sink_summary,
            "operational": operational_summary,
            "limits": limits_snapshot,
        }

    def get_operational_snapshot(self) -> dict[str, Any]:
        return self.get_status()

    def get_healthcheck(self) -> dict[str, Any]:
        now = datetime.now()
        db_bound = self.manager.executor_db is not None
        db_ok = True
        db_error = ""
        heartbeat_age_s = 0.0
        heartbeat_stale = False
        active_jobs = 0
        sink_lag_chunks = 0
        sink_lag_bytes = 0
        oldest_sink_lag_age_s = None

        if db_bound:
            try:
                with self.manager.executor_db.get_session() as session:
                    db_snapshot = build_runtime_health_db_snapshot(
                        session,
                        poll_interval=self.manager.poll_interval,
                        now=now,
                    )
                active_jobs = int(db_snapshot.active_jobs)
                heartbeat_age_s = float(db_snapshot.heartbeat_age_s)
                heartbeat_stale = bool(db_snapshot.heartbeat_stale)
                with self.manager.executor_db.get_session() as session:
                    overview = build_runtime_overview_metrics(
                        session,
                        executor_names=tuple(self.manager.registered_executors().keys()),
                        now=now,
                    )
                sink_lag_chunks = int(overview.sink_lag_chunks)
                sink_lag_bytes = int(overview.sink_lag_bytes)
                oldest_sink_lag_age_s = overview.oldest_sink_lag_age_s
            except Exception as exc:
                db_ok = False
                db_error = str(exc)
                heartbeat_stale = True

        thread_alive = self.manager.manager_thread_alive()
        staging_snapshot = self.manager.staging_runtime_snapshot()
        dispatch_snapshot = self.manager.dispatch_pool_snapshot()
        loop_snapshot = self.manager.loop_runtime_snapshot()

        executor_checks: dict[str, dict[str, Any]] = {}
        for name, adapter in self.manager.registered_executors().items():
            snapshot = dict(adapter.health_snapshot())
            snapshot.setdefault("ok", True)
            snapshot.setdefault("integration", getattr(adapter, "integration_kind", "unknown"))
            snapshot.setdefault("backend", adapter.metadata.backend)
            snapshot.setdefault("mode", adapter.metadata.mode)
            snapshot.setdefault("support_level", adapter.metadata.support_level)
            executor_checks[name] = snapshot

        sink_snapshots = self._sink_snapshots()
        sink_summary = self._build_sink_summary(
            sink_snapshots,
            sink_lag_chunks=sink_lag_chunks,
            sink_lag_bytes=sink_lag_bytes,
            oldest_sink_lag_age_s=oldest_sink_lag_age_s,
        )
        persistence_snapshot = self.manager.persistence_coordinator.snapshot()

        checks = {
            "executor_db": {
                "ok": db_ok,
                "bound": db_bound,
                "error": db_error,
            },
            "manager_thread": {
                "ok": thread_alive,
                "alive": thread_alive,
            },
            "manager_loop": {
                "ok": int(loop_snapshot["consecutive_errors"]) == 0,
                "consecutive_errors": int(loop_snapshot["consecutive_errors"]),
                "backoff_s": float(loop_snapshot["backoff_s"]),
                "last_error": loop_snapshot["last_error"],
                "last_error_at": loop_snapshot["last_error_at"],
            },
            "staging_pool": {
                "ok": bool(staging_snapshot["ready"]),
                "ready": bool(staging_snapshot["ready"]),
                "active_tasks": int(staging_snapshot["active_tasks"]),
                "capacity": int(staging_snapshot["capacity"]),
            },
            "dispatch_pool": {
                **dispatch_snapshot,
            },
            "heartbeat": {
                "ok": not heartbeat_stale,
                "age_s": heartbeat_age_s,
                "stale": heartbeat_stale,
            },
            "executors": executor_checks,
        }
        core_health_checks = {
            "manager_thread": checks["manager_thread"],
            "manager_loop": checks["manager_loop"],
            "staging_pool": checks["staging_pool"],
            "dispatch_pool": checks["dispatch_pool"],
            "executors": executor_checks,
        }
        core_ok = (
            checks["manager_thread"]["ok"]
            and checks["manager_loop"]["ok"]
            and checks["staging_pool"]["ok"]
            and bool(checks["dispatch_pool"].get("ok", True))
            and all(item.get("ok", True) for item in executor_checks.values())
        )
        core_health = {
            "status": "ok" if core_ok else "degraded",
            "ok": bool(core_ok),
            "checks": core_health_checks,
        }

        persistence_ok = bool(db_bound and db_ok)
        persistence_health = {
            "status": (
                "inactive"
                if not db_bound
                else "failed"
                if not db_ok
                else "degraded"
                if heartbeat_stale
                else "ok"
            ),
            "ok": bool(persistence_ok and not heartbeat_stale),
            "checks": {
                "executor_db": checks["executor_db"],
                "heartbeat": checks["heartbeat"],
                "journal": {
                    "ok": persistence_ok,
                    **persistence_snapshot,
                },
            },
        }

        sink_ok = int(sink_summary["flush_failures"]) == 0
        sink_health = {
            "status": "ok" if sink_ok else "degraded",
            "ok": bool(sink_ok),
            "checks": {
                "handlers": {
                    "ok": sink_ok,
                    "active_sinks": sink_summary["active_sinks"],
                    "buffered_items": sink_summary["buffered_items"],
                    "buffered_bytes": sink_summary["buffered_bytes"],
                    "flush_failures": sink_summary["flush_failures"],
                    "retry_count": sink_summary["retry_count"],
                },
                "lag": {
                    "ok": True,
                    "chunks": sink_summary["lag_chunks"],
                    "bytes": sink_summary["lag_bytes"],
                    "oldest_lag_age_s": sink_summary["oldest_lag_age_s"],
                },
            },
        }

        status = "ok"
        if not db_bound:
            status = "inactive"
        elif not db_ok:
            status = "failed"
        elif not core_health["ok"]:
            status = "degraded" if db_ok else "failed"

        return {
            "status": status,
            "active_jobs": active_jobs,
            "loop_latency_ms": float(loop_snapshot["last_latency_ms"]),
            "dispatch_pool_active_tasks": int(dispatch_snapshot.get("active_tasks", 0) or 0),
            "core_health": core_health,
            "persistence_health": persistence_health,
            "sink_health": sink_health,
            "checks": checks,
        }

    def get_executor_capability_matrix(self) -> dict[str, dict[str, Any]]:
        matrix: dict[str, dict[str, Any]] = {}
        for name, adapter in self.manager.registered_executors().items():
            metadata = adapter.metadata
            matrix[name] = {
                **metadata.to_mapping(),
                "shared_fs": metadata.shared_filesystem,
                "local_resource_accounting": self.manager.executor_local_accounting_mode(adapter),
            }
        return matrix
