from pathlib import Path

from ms_flow.core.executor.dispatch_model import DispatchPolicy


def test_dispatch_policy_builds_from_defaults():
    policy = DispatchPolicy()

    assert policy.max_inflight_tasks == 16
    assert policy.batch_size == 1
    assert policy.refill_threshold == 1


def test_dispatch_policy_restores_from_mapping():
    policy = DispatchPolicy.from_mapping(
        {
            "batch_size": "auto",
            "max_inflight_tasks": 12,
            "max_inflight_items": 48,
            "prefetch_factor": 2.0,
            "refill_threshold": 3,
        }
    )

    assert policy.batch_size == "auto"
    assert policy.max_inflight_tasks == 12
    assert policy.max_inflight_items == 48
    assert policy.prefetch_factor == 2.0
    assert policy.refill_threshold == 3


def test_dispatch_policy_normalizes_and_clamps_values():
    policy = DispatchPolicy(
        batch_size=" 3 ",
        max_inflight_tasks=0,
        max_inflight_items=1,
        prefetch_factor=-1.0,
        refill_threshold=99,
    )

    assert policy.batch_size == 3
    assert policy.max_inflight_tasks == 1
    assert policy.max_inflight_items == 1
    assert policy.prefetch_factor == 0.0
    assert policy.refill_threshold == 1


def test_dispatch_policy_round_trips_auto_batch_and_empty_mapping():
    default_policy = DispatchPolicy.from_mapping({})
    auto_policy = DispatchPolicy.from_mapping(
        {
            "batch_size": " ",
            "max_inflight_tasks": 8,
            "max_inflight_items": 4,
            "prefetch_factor": 1.5,
            "refill_threshold": 20,
        }
    )

    assert default_policy.to_mapping()["batch_size"] == 1
    assert auto_policy.batch_size == "auto"
    assert auto_policy.max_inflight_items == 8
    assert auto_policy.refill_threshold == 8
    assert DispatchPolicy.from_mapping(auto_policy.to_mapping()) == auto_policy
