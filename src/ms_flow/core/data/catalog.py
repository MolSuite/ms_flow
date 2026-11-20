"""The catalog of a sharded library: which files there are, and the shards inside them.

The app writes records and says who is in or out; ms_flow keeps where they are. A row of
`ms_shard_files` is one *file*, never one shard: 1M records are four rows, not four thousand. The
shards of a file are its row groups, listed in its own header (`shard.file_groups`), so the
database grows with the number of files while a read still hands out one row per shard,
``(path, [base_id, base_id + slot_count))``, pinned to the active generation and narrowed by the
active exclusions (`exclusions.py`). A caller never filters either itself.

A shard's id is ``(file_id << 20) | group index``: an int for every payload and log, stable
because a file never changes once it is ``ready`` (a rewrite is a new generation, and a
compaction happens before a generation is read).

A *generation* is one rewrite of the whole library. A step that renumbers records writes a new
one that nothing reads until `activate_generation` flips one flag, so a job that dies halfway
leaves an inert generation, not a library that is half the old one and half the new.
Activating forgets the grandparent, rows and files: the current state and the one it came from
are what a redo needs, and keeping every generation keeps the library once per step.
"""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any, Iterable, Iterator

from ms_flow.query import QuerySpec, db_pages

TABLE = "ms_shard_files"
FIELDS = ("id", "generation_id", "role", "path", "status", "n_shards", "n_records", "n_active", "scopes")
LIBRARY = "library"
WRITING, READY = "writing", "ready"
GROUP_BITS = 20  # groups of a file: MAX_RECORDS (1 << 20) ids hold at most that many shards
# Filters that name a shard, not a file: applied to the groups a file lists.
_SHARD_FILTERS = ("scope", "shard_id")


def shard_id(file_id: int, index: int) -> int:
    return (int(file_id) << GROUP_BITS) | int(index)


def split_shard_id(value: int) -> tuple[int, int]:
    """``(file_id, group index)`` of a shard id."""
    return int(value) >> GROUP_BITS, int(value) & ((1 << GROUP_BITS) - 1)


def shard_spec(**filters: Any) -> QuerySpec:
    """Files (and so their shards) matching ``filters``: a file's columns (``generation_id=``,
    ``role=``, ``id=``) or a shard's (``scope=``, ``shard_id=``). The library unless ``role`` says
    otherwise; unpinned until read."""
    return QuerySpec(table=TABLE, fields=FIELDS, filters=dict(filters), order=("id",))


def active_generation_id(project_db) -> int | None:
    """Which rewrite of the library is the current one. None before the first one."""
    from sqlmodel import select

    from ms_flow.core.database.project_models import ShardGeneration

    with project_db.get_session() as session:
        row = session.exec(
            select(ShardGeneration.id)
            .where(ShardGeneration.is_active == True)  # noqa: E712 - SQL, not Python truthiness
            .order_by(ShardGeneration.id.desc())
        ).first()
        return int(row) if row is not None else None


def create_generation(project_db, *, step: str, folder: str | Path = "", job_id: str = "") -> int:
    """Open a generation parented to the active one. Not active: nothing reads it yet."""
    from ms_flow.core.database.project_models import ShardGeneration

    parent_id = active_generation_id(project_db)
    with project_db.get_session() as session:
        row = ShardGeneration(parent_id=parent_id, step=str(step or ""), folder=str(folder or ""),
                              job_id=str(job_id or ""))
        session.add(row)
        session.commit()
        session.refresh(row)
        return int(row.id)


def activate_generation(project_db, generation_id: int, *, on_drop=None) -> None:
    """Make one generation the library and forget the ones outside the window, rows and files.

    ``on_drop(session, files)``: the app's turn to clear what it keeps per file before a
    generation goes, in the same transaction; it returns the paths of files of its own to delete
    with the generation's. A generation the app still references through a foreign key (docking
    receipts...) is kept as it is: what was computed on it stays readable, and the next
    activation tries again once that reference is gone.
    """
    from sqlalchemy.exc import IntegrityError
    from sqlmodel import select

    from ms_flow.core.database.project_models import CatalogFile, ShardGeneration

    paths: set[str] = set()
    with project_db.get_session() as session:
        generations = {int(row.id): row for row in session.exec(select(ShardGeneration)).all()}
        target = generations.get(int(generation_id))
        if target is None:
            raise ValueError(f"No shard generation {generation_id} to activate.")
        for row in generations.values():
            row.is_active = row.id == target.id
            session.add(row)
        keep = {int(target.id)} | ({int(target.parent_id)} if target.parent_id is not None else set())
        doomed = [gen_id for gen_id in generations if gen_id not in keep]
        for row in generations.values():
            # Its parent may go: cut the link first, or that delete trips the foreign key.
            if row.parent_id is not None and int(row.parent_id) in doomed:
                row.parent_id = None
                session.add(row)
        session.flush()
        dropped: set[str] = set()
        for gen_id in doomed:
            files = list(session.exec(select(CatalogFile).where(CatalogFile.generation_id == gen_id)).all())
            savepoint = session.begin_nested()
            try:
                extra = list(on_drop(session, files) or ()) if on_drop is not None and files else []
                for row in files:
                    session.delete(row)
                session.delete(generations[gen_id])
                session.flush()
            except IntegrityError:
                savepoint.rollback()  # still referenced: kept, retried at the next activation
                continue
            savepoint.commit()
            dropped |= {str(row.path or "") for row in files} | {str(path) for path in extra}
        # A file may be listed twice (a step that kept it): it goes only when no row points at it.
        paths = dropped - set(session.exec(select(CatalogFile.path)).all())
        session.commit()
    # Files last: a row that survives a missing file is a bug you can see, a file that survives
    # its row is an orphan nothing will ever look at again.
    for text in paths:
        if text:
            Path(text).unlink(missing_ok=True)


def add_file(
    project_db,
    path: str | Path,
    *,
    generation_id: int | None,
    role: str = LIBRARY,
    status: str = READY,
) -> int:
    """Register a file; its id. ``ready``: it is on disk and closed, and its counts are read from
    it. ``writing``: its writer still has it open — listed (a view shows it filling) but never
    read; `update_file` moves its counts, `finish_file` makes it the library's."""
    from ms_flow.core.database.project_models import CatalogFile

    counts = _counts(project_db, path) if status == READY else {}
    with project_db.get_session() as session:
        row = CatalogFile(generation_id=generation_id, role=str(role), path=str(path), status=status, **counts)
        session.add(row)
        session.commit()
        session.refresh(row)
        return int(row.id)


def update_file(project_db, file_id: int, **values: Any) -> None:
    """Move the columns of one file row (a writer's counts while it fills)."""
    from ms_flow.core.database.project_models import CatalogFile

    with project_db.get_session() as session:
        row = session.get(CatalogFile, int(file_id))
        if row is None:
            return
        for key, value in values.items():
            setattr(row, key, value)
        session.add(row)
        session.commit()


def finish_file(project_db, file_id: int, path: str | Path | None = None) -> None:
    """The writer closed it: ``ready``, with the counts its header says."""
    from ms_flow.core.database.project_models import CatalogFile

    with project_db.get_session() as session:
        row = session.get(CatalogFile, int(file_id))
        if row is None:
            return
        path = str(path or row.path)
    update_file(project_db, file_id, path=path, status=READY, **_counts(project_db, path))


def _counts(project_db, path: str | Path, exclusions=None) -> dict[str, Any]:
    from ms_flow.core.data.shard import file_groups

    groups = file_groups(path)
    n_records = sum(count for _base, _slots, count, _scope in groups)
    return {"n_shards": len(groups), "n_records": n_records,
            "n_active": n_records - _excluded(groups, exclusions, project_db),
            "scopes": ",".join(str(s) for s in sorted({s for *_r, s in groups if s is not None}))}


def _excluded(groups, exclusions, project_db) -> int:
    if exclusions is None:
        from ms_flow.core.data.exclusions import active_exclusions

        exclusions = active_exclusions(project_db)
    if not exclusions:
        return 0
    # ponytail: counts the ids cut, holes included — the approximation `Exclusions.narrow` makes.
    return sum(len(exclusions.in_range(scope, base, base + slots))
               for base, slots, count, scope in groups if scope is not None and count)


def refresh_active(project_db) -> None:
    """``n_active`` of every ready file, after a cut was made or undone: one footer read each."""
    from sqlmodel import select

    from ms_flow.core.data.exclusions import active_exclusions
    from ms_flow.core.data.shard import file_groups
    from ms_flow.core.database.project_models import CatalogFile

    exclusions = active_exclusions(project_db)
    with project_db.get_session() as session:
        for row in session.exec(select(CatalogFile).where(CatalogFile.status == READY)).all():
            try:
                groups = file_groups(row.path)
            except (OSError, KeyError, ValueError):
                continue  # a file gone missing is the reader's error to raise, not this one's
            row.n_active = int(row.n_records) - _excluded(groups, exclusions, project_db)
            session.add(row)
        session.commit()


def compact(project_db, generation_id: int, *, role: str | None = None) -> int:
    """The small files of a generation (a worker's file per task) merged into big ones, up to
    `MAX_RECORDS` ids per file, every shard kept as it was. Scopes share a file (a shard says its
    scope); ids never do: a file whose ids another one in the bin already holds — a second
    import, which restarts at 0 — goes to another. Returns how many files went. For the parent
    once a step is done, before anything reads the generation.

    Crash-safe in order: the big file is complete before a row points at it, and the small
    files go only after the rows moved.
    """
    from uuid import uuid4

    from sqlmodel import select

    from ms_flow.core.data.shard import MAX_RECORDS, file_groups, merge_shards
    from ms_flow.core.database.project_models import CatalogFile

    with project_db.get_session() as session:
        query = select(CatalogFile).where(CatalogFile.generation_id == int(generation_id),
                                          CatalogFile.status == READY)
        if role is not None:
            query = query.where(CatalogFile.role == str(role))
        rows = list(session.exec(query).all())
    groups: dict[tuple[str, str], list[tuple[int, int, Any]]] = {}
    for row in rows:
        path = Path(str(row.path))
        if not path.is_file():
            continue  # ponytail: left as it is; a missing file is the reader's error to raise
        spans = file_groups(path)
        if not spans:
            continue
        lo, hi = min(b for b, *_r in spans), max(b + s for b, s, *_r in spans)
        groups.setdefault((str(path.parent), str(row.role)), []).append((lo, hi, row))
    merged: list[tuple[list[Any], Path]] = []
    for (folder, _role), members in groups.items():
        # Ties by first scope: two imports both start at id 0, and each should keep to its own file.
        members.sort(key=lambda item: (item[0], str(item[2].scopes)))
        bins: list[list[tuple[int, int, Any]]] = []
        for member in members:  # first bin it fits: past that bin's ids, inside its window
            lo, hi, _row = member
            at = next((i for i, b in enumerate(bins) if b[-1][1] <= lo and hi - b[0][0] <= MAX_RECORDS), None)
            if at is None:
                bins.append([])
                at = len(bins) - 1
            bins[at].append(member)
        for members_bin in bins:
            if len(members_bin) < 2:
                continue  # one file already
            dest = Path(folder) / f"merged_{uuid4().hex[:12]}{Path(str(members_bin[0][2].path)).suffix}"
            merge_shards([(row.path, None) for _lo, _hi, row in members_bin], dest)
            merged.append(([row for _lo, _hi, row in members_bin], dest))
    if not merged:
        return 0
    old = []
    for members_bin, dest in merged:
        add_file(project_db, dest, generation_id=int(generation_id), role=str(members_bin[0].role))
        with project_db.get_session() as session:
            for row in members_bin:
                found = session.get(CatalogFile, int(row.id))
                if found is not None:
                    session.delete(found)
                old.append(str(row.path))
            session.commit()
    for text in old:
        Path(text).unlink(missing_ok=True)
    return len(old)


def _pinned(project_db, spec: QuerySpec | None) -> QuerySpec:
    """Any read, on the active generation and on the library. A spec that names a generation is
    left alone: that is how a redo reads its parent, and how a step counts what it has written
    so far. One that names a role reads that role."""
    spec = spec or shard_spec()
    filters = dict(spec.filters)
    filters.setdefault("role", LIBRARY)
    if "generation_id" not in filters:
        generation_id = active_generation_id(project_db)
        if generation_id is not None:
            filters["generation_id"] = generation_id
    return replace(spec, filters=filters)


def _file_query(spec: QuerySpec, *, ready: bool = True) -> tuple[QuerySpec, dict[str, Any]]:
    """The spec as SQL over the files, and the filters left for the shards inside them."""
    file_filters = {k: v for k, v in spec.filters.items() if k not in _SHARD_FILTERS}
    if ready:
        file_filters["status"] = READY
    return replace(spec, filters=file_filters), {k: v for k, v in spec.filters.items() if k in _SHARD_FILTERS}


def files(project_db, spec: QuerySpec | None = None, *, page_size: int = 1000) -> Iterator[dict[str, Any]]:
    """File rows, those still being written too: what a view lists."""
    query, _shard_filters = _file_query(_pinned(project_db, spec), ready=False)
    yield from db_pages(project_db, query, page_size=max(1, int(page_size)))


def _matches(value: Any, wanted: Any) -> bool:
    if isinstance(wanted, (list, tuple, set, frozenset)):
        return value in {int(w) if w is not None else None for w in wanted}
    return value == (int(wanted) if wanted is not None else None)


def _iter_shards(project_db, spec: QuerySpec | None, page_size: int) -> Iterator[dict[str, Any]]:
    from ms_flow.core.data.shard import file_groups

    query, wanted = _file_query(_pinned(project_db, spec))
    for row in db_pages(project_db, query, page_size=max(1, int(page_size))):
        for index, (base, slots, count, scope) in enumerate(file_groups(row["path"])):
            sid = shard_id(row["id"], index)
            if "scope" in wanted and not _matches(scope, wanted["scope"]):
                continue
            if "shard_id" in wanted and not _matches(sid, wanted["shard_id"]):
                continue
            yield {"id": sid, "file_id": int(row["id"]), "generation_id": row["generation_id"],
                   "role": row["role"], "path": row["path"], "shard_index": index, "scope": scope,
                   "base_id": base, "slot_count": slots, "n_records": count}


def get_shard(project_db, value: int) -> dict[str, Any] | None:
    """One shard by its id, whatever its generation or role (a receipt of an older one); None
    when its file is gone."""
    from ms_flow.core.data.shard import file_groups
    from ms_flow.core.database.project_models import CatalogFile

    file_id, index = split_shard_id(value)
    with project_db.get_session() as session:
        row = session.get(CatalogFile, file_id)
        if row is None:
            return None
        row = {"id": int(row.id), "generation_id": row.generation_id, "role": row.role, "path": row.path}
    try:
        base, slots, count, scope = file_groups(row["path"])[index]
    except (OSError, IndexError, KeyError):
        return None
    return {"id": int(value), "file_id": row["id"], "generation_id": row["generation_id"], "role": row["role"],
            "path": row["path"], "shard_index": index, "scope": scope, "base_id": base, "slot_count": slots,
            "n_records": count}


def shards(
    project_db, spec: QuerySpec | None = None, *, whole: bool = False, page_size: int = 1000
) -> Iterator[dict[str, Any]]:
    """Shard rows, minus the active exclusions: a shard left with nothing is not yielded, a
    partial one carries ``record_ids``. ``whole``: every record, for a step that transforms the
    library rather than selects from it (so undoing a cut needs no redo of it)."""
    from ms_flow.core.data.exclusions import active_exclusions

    rows = _iter_shards(project_db, spec, page_size)
    yield from rows if whole else active_exclusions(project_db).narrow(rows)


def _sum(project_db, spec: QuerySpec | None, column: str) -> int | None:
    """SUM of a file column over the files of ``spec``; None when it filters by shard."""
    from sqlalchemy import func
    from sqlmodel import select

    from ms_flow.core.database.project_models import CatalogFile

    query, wanted = _file_query(_pinned(project_db, spec))
    if wanted:
        return None
    statement = select(func.coalesce(func.sum(getattr(CatalogFile, column)), 0))
    for key, value in query.filters.items():
        field = getattr(CatalogFile, key)
        statement = statement.where(field.in_(list(value)) if isinstance(value, (list, tuple, set)) else field == value)
    with project_db.get_session() as session:
        return int(session.exec(statement).one())


def count(project_db, spec: QuerySpec | None = None, *, whole: bool = False) -> int:
    """How many shards `shards` yields: what a job declares as its work."""
    from ms_flow.core.data.exclusions import active_exclusions

    if whole or not active_exclusions(project_db):
        total = _sum(project_db, spec, "n_shards")
        if total is not None:
            return total
    return sum(1 for _row in shards(project_db, spec, whole=whole))


def record_count(project_db, spec: QuerySpec | None = None, *, whole: bool = False) -> int:
    """How many records those shards hold, from the file rows: no file is opened."""
    total = _sum(project_db, spec, "n_records" if whole else "n_active")
    if total is not None:
        return total
    return sum(int(row.get("n_records") or 0) for row in shards(project_db, spec, whole=whole))


def has_files(project_db, spec: QuerySpec | None = None) -> bool:
    """Is there any file at all (being written or ready) in the active generation."""
    return next(files(project_db, spec, page_size=1), None) is not None


__all__ = [
    "FIELDS", "GROUP_BITS", "LIBRARY", "READY", "TABLE", "WRITING", "activate_generation", "active_generation_id", "add_file",
    "compact", "count", "create_generation", "files", "finish_file", "get_shard", "has_files", "record_count",
    "refresh_active", "shard_id", "shard_spec", "shards", "split_shard_id", "update_file",
]
