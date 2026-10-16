"""The volume benchmark's quick self-check: every variant writes, reads back identical bytes and cleans up."""
from __future__ import annotations

import json

import pytest

from benchmarks.refactor_optimization import bench_volumes


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
    assert set(record["variants"]) == {"serial/HT", "serial/H", "serial/RT"}
    for res in record["variants"].values():
        assert res["verified_ids"] == 200
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
    assert set(record["variants"]) == {"serial/HT", "parallel/VP", "parallel/VC", "parallel/RT"}
    assert record["variants"]["serial/HT"] == {"n": 1}  # the other mode is kept
    assert record["variants"]["parallel/RT"]["n"] == 3000  # the same variant is replaced
    for name in ("parallel/VP", "parallel/VC", "parallel/RT"):
        assert record["variants"][name]["verified_ids"] == 200
        assert "parallel_2x20k_random_s" in record["variants"][name]
    assert record["variants"]["parallel/VC"]["files"] == 4  # project.db and one segment per chunk of 1000


def test_variant_that_does_not_fit_is_refused_before_it_runs(tmp_path):
    with pytest.raises(SystemExit, match="needs about"):
        run(tmp_path, "serial", "--n", str(10 ** 12))
    assert not (tmp_path / "results").exists() and not any((tmp_path / "data").iterdir())
