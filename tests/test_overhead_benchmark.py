import json
import multiprocessing as mp
from pathlib import Path

from benchmarks.overhead_benchmark import (  # noqa: E402
    OverheadBenchmarkCase,
    OverheadBenchmarkConfig,
    run_overhead_benchmark,
)


def test_run_overhead_benchmark_emits_comparison_metrics(tmp_path):
    cases = [
        OverheadBenchmarkCase(
            name="direct_thread_noop_small",
            mode="direct_thread_pool",
            workload="noop",
            total_tasks=24,
            max_workers=2,
        ),
        OverheadBenchmarkCase(
            name="molsuite_thread_noop_small",
            mode="molsuite_thread",
            workload="noop",
            total_tasks=24,
            max_workers=2,
            baseline_name="direct_thread_noop_small",
        ),
        OverheadBenchmarkCase(
            name="direct_process_pool_spawn_noop_small",
            mode="direct_process_pool_spawn",
            workload="noop",
            total_tasks=12,
            max_workers=2,
        ),
        OverheadBenchmarkCase(
            name="molsuite_process_pool_spawn_noop_small",
            mode="molsuite_process_pool_spawn",
            workload="noop",
            total_tasks=12,
            max_workers=2,
            baseline_name="direct_process_pool_spawn_noop_small",
        ),
    ]
    if "forkserver" in {str(item).strip().lower() for item in mp.get_all_start_methods()}:
        cases.extend(
            [
                OverheadBenchmarkCase(
                    name="direct_process_pool_forkserver_noop_small",
                    mode="direct_process_pool_forkserver",
                    workload="noop",
                    total_tasks=12,
                    max_workers=2,
                ),
                OverheadBenchmarkCase(
                    name="molsuite_process_pool_forkserver_noop_small",
                    mode="molsuite_process_pool_forkserver",
                    workload="noop",
                    total_tasks=12,
                    max_workers=2,
                    baseline_name="direct_process_pool_forkserver_noop_small",
                ),
            ]
        )

    payload = run_overhead_benchmark(
        OverheadBenchmarkConfig(
            total_cpu=2,
            thread_workers=2,
            poll_interval_s=0.01,
            cases=tuple(cases),
        ),
        workdir=tmp_path / "molsuite_overhead",
    )

    assert payload["summary"]["runs"] == len(cases)
    assert len(payload["results"]) == len(cases)
    assert len(payload["summary"]["comparisons"]) >= 2

    by_name = {row["name"]: row for row in payload["results"]}
    direct_thread = by_name["direct_thread_noop_small"]
    molsuite_thread = by_name["molsuite_thread_noop_small"]
    direct_pool = by_name["direct_process_pool_spawn_noop_small"]
    molsuite_pool = by_name["molsuite_process_pool_spawn_noop_small"]

    assert direct_thread["tasks_completed"] == 24
    assert molsuite_thread["tasks_completed"] == 24
    assert direct_pool["tasks_completed"] == 12
    assert molsuite_pool["tasks_completed"] == 12
    assert molsuite_thread["job_status"] == "completed"
    assert molsuite_pool["job_status"] == "completed"
    assert molsuite_thread["throughput_tasks_s"] > 0.0
    assert molsuite_pool["throughput_tasks_s"] > 0.0

    encoded = json.dumps(payload, sort_keys=True)
    assert "\"comparisons\"" in encoded
    assert "\"overhead_wall_time_s\"" in encoded
