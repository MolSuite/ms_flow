import json
import multiprocessing as mp
from pathlib import Path

from benchmarks.process_executor_benchmark import (
    ProcessBenchmarkCase,
    ProcessBenchmarkConfig,
    run_process_executor_benchmark,
)

LOKY_INSTALLED = True
try:
    import loky  # noqa: F401
except Exception:
    LOKY_INSTALLED = False


def test_run_process_executor_benchmark_emits_process_metrics(tmp_path):
    benchmark_cases = [
        ProcessBenchmarkCase(
            name="thread_noop_small",
            executor_name="thread",
            workload="noop",
            total_chunks=16,
            timeout_s=10.0,
        ),
        ProcessBenchmarkCase(
            name="process_pool_spawn_noop_small",
            executor_name="process_pool",
            workload="noop",
            total_chunks=12,
            timeout_s=10.0,
        ),
    ]
    if "forkserver" in {str(item).strip().lower() for item in mp.get_all_start_methods()}:
        benchmark_cases.append(
            ProcessBenchmarkCase(
                name="process_pool_forkserver_noop_small",
                executor_name="process_pool_forkserver",
                workload="noop",
                total_chunks=12,
                timeout_s=10.0,
            )
        )
    if LOKY_INSTALLED:
        benchmark_cases.append(
            ProcessBenchmarkCase(
                name="process_pool_loky_noop_small",
                executor_name="process_pool_loky",
                workload="noop",
                total_chunks=12,
                timeout_s=10.0,
            )
        )
    benchmark_cases.append(
        ProcessBenchmarkCase(
            name="process_sleep_cancel_small",
            executor_name="process",
            workload="sleep",
            total_chunks=12,
            chunk_sleep_s=0.05,
            cancel_after_s=0.1,
            timeout_s=10.0,
        )
    )

    payload = run_process_executor_benchmark(
        ProcessBenchmarkConfig(
            total_cpu=2,
            thread_workers=2,
            poll_interval=0.02,
            sample_interval_s=0.05,
            post_stop_wait_s=0.1,
            cases=tuple(benchmark_cases),
        ),
        workdir=tmp_path / "process_bench",
    )

    assert payload["summary"]["runs"] == len(benchmark_cases)
    assert len(payload["results"]) == len(benchmark_cases)
    assert payload["summary"]["peak_python_descendants_max"] >= 0
    assert payload["summary"]["peak_zombie_descendants_max"] >= 0
    assert payload["summary"]["post_stop_python_descendants_max"] >= 0

    by_name = {row["name"]: row for row in payload["results"]}
    first = by_name["thread_noop_small"]
    second = by_name["process_pool_spawn_noop_small"]
    third = by_name["process_sleep_cancel_small"]
    assert first["executor_name"] == "thread"
    assert first["status"] == "completed"
    assert first["samples"]["sample_count"] >= 1
    assert second["executor_name"] == "process_pool"
    assert second["status"] == "completed"
    assert second["samples"]["peak_python_descendants"] >= 0
    forkserver_case = by_name.get("process_pool_forkserver_noop_small")
    if forkserver_case is not None:
        assert forkserver_case["executor_name"] == "process_pool_forkserver"
        assert forkserver_case["status"] == "completed"
        assert forkserver_case["samples"]["peak_python_descendants"] >= 0
    loky_case = by_name.get("process_pool_loky_noop_small")
    if loky_case is not None:
        assert loky_case["executor_name"] == "process_pool_loky"
        assert loky_case["status"] == "completed"
        assert loky_case["samples"]["peak_python_descendants"] >= 0
    assert third["executor_name"] == "process"
    assert third["cancel_issued"] is True
    assert third["samples"]["peak_python_descendants"] >= 0

    encoded = json.dumps(payload, sort_keys=True)
    assert "\"peak_python_descendants_max\"" in encoded
    assert "\"post_stop\"" in encoded
