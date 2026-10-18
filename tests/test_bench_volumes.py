"""The volume benchmark's quick self-check: every variant writes, reads back identical bytes and cleans up."""
from __future__ import annotations

import json

import pytest

from benchmarks.refactor_optimization import bench_volumes


SERIAL_READS = ["query_top1000_s", "read_top1000_current_s", "read_top_poses_s", "stream_all_current_s",
                "sort_unindexed_logp_top1000_s", "count_filter_s", "page_by_score_offset_s",
                "open_and_get_mol_x_ms", "get_mol_p50_us", "get_mol_p99_us", "parallel_8x10k_random_s"]
PARALLEL_READS = ["get_mol_p50_us", "get_mol_p99_us", "top1000_batch_s", "stream_all_current_s", "parallel_2x20k_random_s"]


def check_cold(res, reads, random_read):
    for metric in reads:
        assert res[metric + "_cold"] >= 0, metric
    # the device counter proves the cache was cold; it is null where /proc/diskstats does not list the device
    for disk_mb in ("stream_all_current_cold_disk_mb", random_read + "_cold_disk_mb"):
        assert disk_mb in res
    if res["stream_all_current_cold_disk_mb"] is not None:
        assert res["stream_all_current_cold_disk_mb"] > 0


def run(tmp_path, mode, *args):
    bench_volumes.main(["--mode", mode, "--quick", "--label", "t", "--data-dir", str(tmp_path / "data"),
                        "--results-dir", str(tmp_path / "results"), *args])
    assert not any((tmp_path / "data").iterdir()), "a variant left its data behind"
    return json.loads((tmp_path / "results" / "volumes_t.json").read_text())


@pytest.mark.slow
def test_serial_variants_read_back_what_was_written(tmp_path):
    record = run(tmp_path, "serial")
    assert record["mode"] == "serial" and record["n"] == 3000 and record["workers"] is None
    assert record["machine"]["cpus"] and record["commit"]
    assert set(record["variants"]) == {"serial/HT", "serial/H", "serial/RT", "serial/PS", "serial/PSN"}
    for res in record["variants"].values():
        assert res["verified_ids"] == 200 and res["durable_s"] >= res["import_s"] > 0
    for name in ("serial/PS", "serial/PSN"):  # import only; their reads are V3
        assert record["variants"].pop(name)["files"] == 3  # project.db, a "ligands" and a "poses" volume
    for res in record["variants"].values():
        check_cold(res, SERIAL_READS, "parallel_8x10k_random")
        assert res["import_s"] > 0 and res["update_5000_s"] > 0 and res["get_mol_p99_us"] >= res["get_mol_p50_us"]
    assert record["variants"]["serial/RT"]["files"] == 1  # blobs live in project.db
    assert record["variants"]["serial/HT"]["files"] == 2  # project.db and one volume


@pytest.mark.slow
def test_parallel_variants_join_the_serial_run_of_the_same_label(tmp_path):
    results = tmp_path / "results"
    results.mkdir()
    (results / "volumes_t.json").write_text(json.dumps({"variants": {"serial/HT": {"n": 1}, "parallel/RT": {"n": 1}}}))
    record = run(tmp_path, "parallel", "--workers", "2")
    assert record["mode"] == "parallel" and record["workers"] == 2
    assert set(record["variants"]) == {"serial/HT", "parallel/VP", "parallel/VC", "parallel/RT", "parallel/PJ"}
    assert record["variants"]["serial/HT"] == {"n": 1}  # the other mode is kept
    assert record["variants"]["parallel/RT"]["n"] == 3000  # the same variant is replaced
    for name in ("parallel/VP", "parallel/VC", "parallel/RT"):
        assert record["variants"][name]["verified_ids"] == 200
        check_cold(record["variants"][name], PARALLEL_READS, "parallel_2x20k_random")
        assert "parallel_2x20k_random_s" in record["variants"][name]
    assert record["variants"]["parallel/VC"]["files"] == 4  # project.db and one segment per chunk of 1000
    job = record["variants"]["parallel/PJ"]  # a real ms_flow job; its reads are cold, with the V3 readers
    assert job["verified_ids"] == 200 and job["durable_s"] >= job["import_s"] > 0
    for name in ("stream_all_current", "export_all_current"):
        disk_mb = job[name + "_cold_disk_mb"]
        assert job[name + "_s_cold"] > 0 and (disk_mb is None or disk_mb > 0), name
    assert job["files"] == 3  # project.db, a "ligands" and a "poses" volume


@pytest.mark.slow
def test_codec_variants_store_a_sample_compressed(tmp_path):
    import pickle

    sample = tmp_path / "sample"
    sample.mkdir()
    fake = lambda kind, n, seed: [f"{kind} {seed + i}\n".encode() + b"".join(
        f"  {(seed + i) * j % 97:4d}.{j:04d}  C  0  0\n".encode() for j in range(40)) for i in range(n)]
    for name, seed in (("bench.pkl", 0), ("train.pkl", 10_000)):  # disjoint, as V5-1's sample
        (sample / name).write_bytes(pickle.dumps({k: fake(k, 300, seed) for k in ("sdf", "pdbqt", "pose")}))
    record = run(tmp_path, "parallel", "--workers", "2", "--variants", "PJ,PJZ,PJD", "--sample", str(sample))
    assert record["sample"] == str(sample)
    pj, pjz, pjd = (record["variants"][f"parallel/{k}"] for k in ("PJ", "PJZ", "PJD"))
    assert [v["codec"] for v in (pj, pjz, pjd)] == ["none", "zstd", "zstd-dict"]
    assert pj["raw_mb"] == pjz["raw_mb"] == pjd["raw_mb"] == pj["stored_mb"]  # the same bytes; training stays apart
    assert pjd["stored_mb"] <= pjz["stored_mb"] < pj["stored_mb"] and pjd["train_mb"] > 0 == pjz["train_mb"]
    for res in (pjz, pjd):
        assert res["verified_ids"] == 200 and res["stream_all_current_s_cold"] > 0
    assert pjd["files"] == pj["files"] + 2  # and a dictionary per series, in volumes/dicts/


def test_variant_that_does_not_fit_is_refused_before_it_runs(tmp_path):
    with pytest.raises(SystemExit, match="needs about"):
        run(tmp_path, "serial", "--n", str(10 ** 12))
    assert not (tmp_path / "results").exists() and not any((tmp_path / "data").iterdir())
