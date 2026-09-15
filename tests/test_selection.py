from __future__ import annotations

import pytest

from ms_flow.selection import MultipleResultsFound, NoResultFound, Selection


def _selection(items):
    return Selection(
        scope={"kind": "test"},
        _stream=lambda: iter(items),
        _count=lambda: len(items),
    )


def test_selection_is_lazy_reiterable_and_counts_without_materializing():
    selection = _selection([1, 2, 3])
    assert list(selection) == [1, 2, 3]
    assert list(selection.stream()) == [1, 2, 3]
    assert selection.count() == 3
    assert selection.first() == 1


def test_selection_one_variants():
    assert _selection([1]).one() == 1
    assert _selection([]).one_or_none() is None
    with pytest.raises(NoResultFound):
        _selection([]).one()
    with pytest.raises(MultipleResultsFound):
        _selection([1, 2]).one()
