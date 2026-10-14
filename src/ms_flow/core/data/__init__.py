"""How data reaches a task and leaves it.

Loaded on first use: `shard` has no dependency, and a worker that only reads one must not pay
for the database the bridge brings in.
"""
from importlib import import_module

_LAZY = {
    "BytesInputSpec": "contracts",
    "DataBridge": "bridge",
    "DataContext": "runtime",
    "DataContractError": "contracts",
    "DataTransportPlanner": "planner",
    "DbInputSpec": "contracts",
    "DbOutputSpec": "contracts",
    "ExecutorTransportProfile": "runtime",
    "FileArtifact": "contracts",
    "FileInputSpec": "contracts",
    "FileOutputSpec": "contracts",
    "INPUT_WIRE_KEY": "contracts",
    "InlineInputSpec": "contracts",
    "InputSpec": "contracts",
    "OUTPUT_WIRE_KEY": "contracts",
    "OutputSpec": "contracts",
    "ProjectOutputDirSpec": "contracts",
    "RAY_FILE_ARTIFACT_KEY": "contracts",
    "RAY_FILE_INPUT_KEY": "contracts",
    "RAY_OUTPUT_DIR_KEY": "contracts",
    "ResolvedHandle": "runtime",
    "TableOutputSpec": "contracts",
    "from_wire_value": "contracts",
    "payload_has_input_specs": "contracts",
    "to_wire_value": "contracts",
}


def __getattr__(name: str):
    if name not in _LAZY:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = globals()[name] = getattr(import_module(f"{__name__}.{_LAZY[name]}"), name)
    return value


__all__ = sorted(_LAZY)
