"""How an element says which files its task reads and where it leaves the ones it makes.

A path in an element is good only where the file is. Both marks are strings, so where the disk
is the same one (a pool on this machine, nodes that share a folder) nobody does anything and the
task uses them as paths. An executor whose tasks run on another disk looks for them in each
element, takes the files there and hands the task the paths they have there.
"""
from __future__ import annotations


class File(str):
    """The path of a file the task reads."""

    __slots__ = ()


class Dir(str):
    """The path of a directory where the task leaves files. On another disk the task gets a
    directory of that one, and what comes back is what its result names: each path under that
    directory in what the task returns is brought here, and the result says where it is here."""

    __slots__ = ()


def swap(value, change, kinds=(File, Dir)):
    """`value` with `change(mark)` in the place of each mark in it, inside dicts, lists and tuples.

    `kinds` is what counts as a mark: `str` to go over every string of a result.
    """
    if isinstance(value, kinds):
        return change(value)
    if isinstance(value, dict):
        return {key: swap(item, change, kinds) for key, item in value.items()}
    if type(value) in (list, tuple):  # ponytail: plain ones; a namedtuple of paths is not looked into
        return type(value)(swap(item, change, kinds) for item in value)
    return value
