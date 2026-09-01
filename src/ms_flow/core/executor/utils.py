import base64
import json
from typing import Any, Tuple, Set


CHUNK_ACTIVE_STATUSES: Tuple[str, ...] = ("pending", "running", "staging")
JOB_ACTIVE_STATUSES: Tuple[str, ...] = ("pending", "pending_feed", "queued", "running", "staging")
JOB_RECOVERABLE_STATUSES: Tuple[str, ...] = JOB_ACTIVE_STATUSES + ("cancel_requested",)
TERMINAL_JOB_STATUSES: Set[str] = {"completed", "failed", "canceled"}


CHUNK_PAYLOAD_REF_KEY = "__molsuite_chunk_payload_ref__"
_BYTES_KEY = "__molsuite_bytes__"


def _json_default(value: Any) -> Any:
    if isinstance(value, bytes):
        return {_BYTES_KEY: base64.b64encode(value).decode("ascii")}
    return str(value)


def _json_object_hook(value: dict[str, Any]) -> Any:
    if set(value) == {_BYTES_KEY}:
        return base64.b64decode(str(value[_BYTES_KEY]).encode("ascii"))
    return value


def _safe_json_dumps(data: Any) -> str:
    """JSON dump that preserves bytes and stringifies other unsupported values."""
    return json.dumps(data, ensure_ascii=False, default=_json_default)


def _safe_json_loads(raw: str) -> Any:
    """Safe JSON load that handles empty strings and potential decode errors."""
    if not raw:
        return {}
    try:
        return json.loads(raw, object_hook=_json_object_hook)
    except json.JSONDecodeError:
        return {"raw": raw}
