import json
import multiprocessing as mp
from pathlib import Path

from benchmarks.batched_overhead_benchmark import (  # noqa: E402
    BatchedOverheadCase,
    BatchedOverheadConfig,
    run_batched_overhead_benchmark,
)


def test_run_batched_overhead_benchmark_emits_chunk_metrics(tmp_path):
    has_forkserver = "forkserver" in {str(item).strip().lower() for item in mp.get_all_start_methods()}
    process_direct_mode = "direct_process_pool_forkserver" if has_forkserver else "direct_process_pool_spawn"
    process_molsuite_mode = "molsuite_process_pool_forkserver" if has_forkserver else "molsuite_process_pool_spawn"
    cases = (
        BatchedOverheadCase(
            name="direct_thread_noop_chunk10_small",
            mode="direct_thread_pool",
            workload="noop",
            total_items=200,
            items_per_chunk=10,
            max_workers=2,
        ),
        BatchedOverheadCase(
            name="molsuite_thread_noop_chunk10_small",
            mode="molsuite_thread",
            workload="noop",
            total_items=200,
            items_per_chunk=10,
            max_workers=2,
            baseline_name="direct_thread_noop_chunk10_small",
        ),
        BatchedOverheadCase(
            name=f"{process_direct_mode}_cpu_light_chunk10_small",
            mode=process_direct_mode,
            workload="cpu_light",
            total_items=120,
            items_per_chunk=10,
            max_workers=2,
            cpu_iterations_per_item=5_000,
        ),
        BatchedOverheadCase(
            name=f"{process_molsuite_mode}_cpu_light_chunk10_small",
            mode=process_molsuite_mode,
            workload="cpu_light",
            total_items=120,
            items_per_chunk=10,
            max_workers=2,
            cpu_iterations_per_item=5_000,
            baseline_name=f"{process_direct_mode}_cpu_light_chunk10_small",
        ),
    )

    payload = run_batched_overhead_benchmark(
        BatchedOverheadConfig(
            total_cpu=2,
            thread_workers=2,
            poll_interval_s=0.01,
            cases=cases,
        ),
        workdir=tmp_path / "batched_overhead",
    )

    assert payload["summary"]["runs"] == len(cases)
    assert len(payload["results"]) == len(cases)
    assert len(payload["summary"]["comparisons"]) == 2

    by_name = {row["name"]: row for row in payload["results"]}
    direct_thread = by_name["direct_thread_noop_chunk10_small"]
    molsuite_thread = by_name["molsuite_thread_noop_chunk10_small"]
    direct_process = by_name[f"{process_direct_mode}_cpu_light_chunk10_small"]
    molsuite_process = by_name[f"{process_molsuite_mode}_cpu_light_chunk10_small"]

    assert direct_thread["chunks_submitted"] == 20
    assert molsuite_thread["chunks_submitted"] == 20
    assert direct_thread["items_completed"] == 200
    assert molsuite_thread["items_completed"] == 200
    assert direct_process["items_completed"] == 120
    assert molsuite_process["items_completed"] == 120
    assert molsuite_thread["job_status"] == "completed"
    assert molsuite_process["job_status"] == "completed"
    assert molsuite_thread["wall_time_per_item_us"] is not None
    assert molsuite_process["wall_time_per_chunk_ms"] is not None

    encoded = json.dumps(payload, sort_keys=True)
    assert "\"overhead_per_item_us\"" in encoded
    assert "\"overhead_per_chunk_ms\"" in encoded
