"""The files that go wherever another file goes (I14): a container's index. No pyarrow here, so
every mover can import it."""
from __future__ import annotations

_CONTAINER, _INDEX = ".arrows", ".arrows.index"


def companions(path) -> list[str]:
    """`X.arrows` gives `X.arrows.index`; anything else, nothing."""
    text = str(path)
    return [text + ".index"] if text.endswith(_CONTAINER) else []


def whole(path) -> list[str]:
    """The file `path` belongs to, then its companions: `path` alone for any other file."""
    text = str(path)
    main = text[: -len(".index")] if text.endswith(_INDEX) else text
    return [main, *companions(main)]


__all__ = ["companions", "whole"]
