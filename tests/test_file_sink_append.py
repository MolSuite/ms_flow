"""What a file sink in append or per-batch mode accepts."""

import pytest

from ms_flow.core.data import FileOutputSpec


def test_append_rejects_json_because_arrays_do_not_concatenate():
    with pytest.raises(ValueError, match="append"):
        FileOutputSpec(path="out.json", fmt="json", append=True)


def test_append_survives_the_wire_round_trip():
    from ms_flow.core.data.contracts import OUTPUT_WIRE_KEY, wire_to_output_spec

    spec = FileOutputSpec(path="out.txt", fmt="text", append=True)
    assert wire_to_output_spec(spec.to_wire()[OUTPUT_WIRE_KEY]).append is True


def test_append_and_per_batch_are_exclusive():
    with pytest.raises(ValueError, match="exclusive"):
        FileOutputSpec(path="out.txt", fmt="text", append=True, per_batch=True)
