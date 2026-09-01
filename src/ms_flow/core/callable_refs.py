from __future__ import annotations

from importlib import import_module
from typing import Any, Callable


def callable_ref(handler: Callable[..., Any]) -> str:
    return f"{handler.__module__}:{handler.__qualname__}"


def resolve_callable_ref(ref: str) -> Callable[..., Any]:
    value = ref.strip()
    if ":" not in value:
        raise ValueError(f"invalid callable_ref '{value}'. Expected 'module:callable'.")
    module_name, qualname = value.rsplit(":", 1)
    module = import_module(module_name)
    obj: Any = module
    for attr in qualname.split("."):
        obj = getattr(obj, attr)
    if not callable(obj):
        raise TypeError(f"'{value}' no es callable.")
    return obj


def validate_importable_callable(handler: Callable[..., Any]) -> str:
    module = getattr(handler, "__module__", "")
    qualname = getattr(handler, "__qualname__", "")
    if not module or not qualname:
        raise ValueError("The handler must expose __module__ and __qualname__.")
    if module == "__main__":
        raise ValueError(
            f"Handler '{qualname}' is defined in __main__. "
            "A top-level callable importable from a module is required."
        )
    if "<" in qualname:
        raise ValueError(
            f"Handler '{qualname}' is not importable. "
            "Lambdas and nested functions are not accepted."
        )

    ref = callable_ref(handler)
    try:
        resolved = resolve_callable_ref(ref)
    except AttributeError:
        # During decorator execution the function may not yet be bound on the
        # module object, even if it is a valid top-level definition.
        return ref
    if resolved is not handler:
        raise ValueError(
            f"Handler '{ref}' is not stable across reimport. "
            "Register a top-level function directly reachable from its module."
        )
    return ref


def validate_importable_ref(ref: str, *, label: str = "callable_ref") -> str:
    value = ref.strip()
    if not value:
        return ""
    try:
        resolve_callable_ref(value)
    except AttributeError:
        return value
    except Exception as exc:
        raise ValueError(f"{label} '{value}' no es importable: {exc}") from exc
    return value
