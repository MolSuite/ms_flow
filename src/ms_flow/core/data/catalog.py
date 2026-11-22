"""The catalog of a sharded dataset: which files there are, and the shards inside them.

The app writes records and says who is in or out; ms_flow keeps where they are. A row of
`ms_shard_files` is one *file*, never one shard: 1M records are four rows, not four thousand. The
shards of a file are its row groups, listed in its own header (`shard.file_groups`), so the
database grows with the number of files while a read still hands out one row per shard,
``(path, [base_id, base_id + slot_count))``, pinned to the active generation and narrowed by the
active exclusions (`exclusions.py`). A caller never filters either itself.

A shard's id is ``(file_id << 20) | group index``: an int for every payload and log, stable
because a file never changes once it is ``ready`` (a rewrite is a new generation, and a
compaction happens before a generation is read).

A *derived* file is what a step made of source shards, with the same ids (``role`` names the
step): each of its shards names the source shard it came from (``origin``). The catalog row is
the state: a source shard is done for a step when a ready file of that role holds a shard with
its origin (`derived`, `underived`); the newest file wins, so redoing a shard needs no edit of
the older file, and `compact` drops the copy it shadows. A derived file takes the generation of
its source, so it goes with it.

A *generation* is one rewrite of the whole dataset. A step that renumbers records writes a new
one that nothing reads until `activate_generation` flips one flag, so a job that dies halfway
leaves an inert generation, not a dataset that is half the old one and half the new.
Activating forgets the grandparent, rows and files: the current state and the one it came from
are what a redo needs, and keeping every generation keeps the dataset once per step.
"""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any, Iterable, Iterator

from ms_flow.query import QuerySpec, db_pages

TABLE = "ms_shard_files"
FIELDS = ("id", "generation_id", "role", "path", "status", "n_shards", "n_records", "n_active",
          "scopes", "location")
SOURCE = "source"  # the dataset itself; a step's outputs carry roles of their own
WRITING, READY = "writing", "ready"
GROUP_BITS = 20  # groups of a file: MAX_RECORDS (1 << 20) ids hold at most that many shards
# Filters that name a shard, not a file: applied to the groups a file lists.
_SHARD_FILTERS = ("scope", "shard_id")
_COUNTS = ("n_shards", "n_records", "n_active", "scopes")  # what a file says of itself


def shard_id(file_id: int, index: int) -> int:
    return (int(file_id) << GROUP_BITS) | int(index)


def split_shard_id(value: int) -> tuple[int, int]:
    """``(file_id, group index)`` of a shard id."""
    return int(value) >> GROUP_BITS, int(value) & ((1 << GROUP_BITS) - 1)


def shard_spec(**filters: Any) -> QuerySpec:
    """Files (and so their shards) matching ``filters``: a file's columns (``generation_id=``,
    ``role=``, ``id=``) or a shard's (``scope=``, ``shard_id=``). The source files unless ``role`` says
    otherwise; unpinned until read."""
    return QuerySpec(table=TABLE, fields=FIELDS, filters=dict(filters), order=("id",))


def active_generation_id(project_db) -> int | None:
    """Which rewrite of the dataset is the current one. None before the first one."""
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
    """Make one generation the dataset and forget the ones outside the window, rows and files.

    ``on_drop(session, files)``: the app's turn to clear what it keeps per file before a
    generation goes, in the same transaction; it returns the paths of files of its own to delete
    with the generation's. A generation the app still references through a foreign key (receipts
    of a step...) is kept as it is: what was computed on it stays readable, and the next
    activation tries again once that reference is gone.
    """
    from sqlalchemy.exc import IntegrityError
    from sqlmodel import select

    from ms_flow.core.data.states import INACTIVE, progress
    from ms_flow.core.database.project_models import CatalogFile, ShardGeneration

    named = {int(row["generation_id"]) for row in progress(project_db)
             if row["status"] != INACTIVE and row["generation_id"] is not None}
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
        doomed = [gen_id for gen_id in generations if gen_id not in keep | named]
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
            _unlink(text)


def _unlink(path: str | Path) -> None:
    """A record file and its dropped sidecar."""
    from ms_flow.core.data.shard import dropped_path

    Path(path).unlink(missing_ok=True)
    dropped_path(path).unlink(missing_ok=True)


def file_row(path: str | Path, *, role: str, generation_id: int | None) -> dict[str, Any]:
    """A ready file's catalog row, from its header: what a task returns to `ms_flow.sinks.catalog_sink`
    so the parent registers its file as the chunk arrives (``n_active`` = every record: a
    derived file is read through `derived`, which narrows by its source's exclusions).
    ``sidecars`` names its dropped sidecar too, so a worker on another disk brings both back;
    it is no column, the sink leaves it."""
    from datetime import datetime

    from ms_flow.core.data.shard import dropped_path

    counts = _counts(None, path, exclusions=())
    sidecar = dropped_path(path)
    return {"generation_id": generation_id, "role": str(role), "path": str(path), "status": READY,
            "error": "", "created_at": datetime.now(), **counts,
            "sidecars": [str(sidecar)] if sidecar.is_file() else []}


def sweep(project_db, folder: str | Path) -> int:
    """Delete the files of ``folder`` the catalog does not know: a task's file published but never
    registered (the parent died in between), a writer's ``.tmp``. For a step, at its start, on its
    own output folder; their shards are simply made again. How many went."""
    import os

    from sqlmodel import select

    from ms_flow.core.database.project_models import CatalogFile

    folder = Path(folder)
    if not folder.is_dir():
        return 0
    with project_db.get_session() as session:
        known = {os.path.abspath(str(p)) for p in session.exec(select(CatalogFile.path)).all() if p}
    gone = 0
    for path in folder.iterdir():
        if not path.is_file():
            continue
        main = path.with_name(path.stem[: -len(".dropped")] + path.suffix) if path.stem.endswith(".dropped") else path
        if path.name.endswith(".tmp") or os.path.abspath(str(main)) not in known:
            path.unlink(missing_ok=True)
            gone += 1
    return gone


def add_file(
    project_db,
    path: str | Path,
    *,
    generation_id: int | None,
    role: str = SOURCE,
    status: str = READY,
    location: str = "",
    counts: dict[str, Any] | None = None,
    once: bool = False,
) -> int:
    """Register a file; its id. ``ready``: it is on disk and closed, and its counts are read from
    it. ``writing``: its writer still has it open — listed (a view shows it filling) but never
    read; `update_file` moves its counts, `finish_file` makes it readable.

    ``location``: the host that has it, for a file a step wrote there and left there — nothing
    here can open it, so its ``counts`` come with it (`file_row`, made over there)."""
    # ``once``: the id of the row this path already has, if it has one (a file made where it was
    # left: whoever found it registers the same thing).
    from ms_flow.core.database.project_models import CatalogFile

    counts = dict(counts) if counts is not None else (_counts(project_db, path) if status == READY else {})
    counts = {name: value for name, value in counts.items() if name in _COUNTS}
    with project_db.get_session() as session:
        if once:
            from sqlmodel import select

            found = session.exec(select(CatalogFile.id).where(CatalogFile.path == str(path))).first()
            if found is not None:
                return int(found)
        row = CatalogFile(generation_id=generation_id, role=str(role), path=str(path), status=status,
                          location=str(location), **counts)
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

    try:
        groups = file_groups(path)
    except KeyError:  # a table file: any Parquet, rows of its own schema, no shards
        import pyarrow.parquet as pq

        rows = pq.ParquetFile(path).metadata.num_rows
        return {"n_shards": 0, "n_records": rows, "n_active": rows, "scopes": ""}
    n_records = sum(g.n_records for g in groups)
    return {"n_shards": len(groups), "n_records": n_records,
            "n_active": n_records - _excluded(groups, exclusions, project_db),
            "scopes": ",".join(str(s) for s in sorted({g.scope for g in groups if g.scope is not None}))}


def _excluded(groups, exclusions, project_db) -> int:
    if exclusions is None:
        from ms_flow.core.data.exclusions import active_exclusions

        exclusions = active_exclusions(project_db)
    if not exclusions:
        return 0
    # ponytail: counts the ids cut, holes included — the approximation `Exclusions.narrow` makes.
    return sum(len(exclusions.in_range(g.scope, g.base_id, g.base_id + g.slot_count))
               for g in groups if g.scope is not None and g.n_records)


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
                                          CatalogFile.status == READY, CatalogFile.location == "")
        if role is not None:
            query = query.where(CatalogFile.role == str(role))
        rows = list(session.exec(query).all())
    listed = {}
    newest: dict[tuple[str, int], int] = {}  # (role, origin) -> the newest file holding it
    for row in sorted(rows, key=lambda r: int(r.id)):
        path = Path(str(row.path))
        if not path.is_file() or not row.n_shards:
            continue  # ponytail: left as it is; a missing file is the reader's error to raise. A
            # table file has no shards: `compact_tables` is its compaction.
        listed[int(row.id)] = file_groups(path)
        for group in listed[int(row.id)]:
            if group.origin is not None:
                newest[(str(row.role), group.origin)] = int(row.id)
    groups: dict[tuple[str, str], list[tuple[int, int, Any]]] = {}
    skip: set[tuple[str, int]] = set()  # shards a newer file of their role redid
    empty = []  # files whose every shard was redone elsewhere: they just go
    for row in rows:
        if int(row.id) not in listed:
            continue
        kept = []
        for index, group in enumerate(listed[int(row.id)]):
            if group.origin is not None and newest[(str(row.role), group.origin)] != int(row.id):
                skip.add((str(row.path), index))
            else:
                kept.append(group)
        if not kept:
            if listed[int(row.id)]:
                empty.append(row)
            continue
        lo, hi = min(g.base_id for g in kept), max(g.base_id + g.slot_count for g in kept)
        spans = [(g.base_id, g.base_id + g.slot_count) for g in kept]
        groups.setdefault((str(Path(str(row.path)).parent), str(row.role)), []).append((lo, hi, row, spans))
    merged: list[tuple[list[Any], Path]] = []
    for (folder, _role), members in groups.items():
        # Ties by first scope: two imports both start at id 0, and each should keep to its own file.
        members.sort(key=lambda item: (item[0], str(item[2].scopes)))
        bins: list[list[tuple[int, int, Any, list]]] = []
        for member in members:  # first bin it fits: no shard's ids taken there, inside its window
            lo, hi, _row, spans = member
            at = next((i for i, b in enumerate(bins)
                       if max(hi, max(m[1] for m in b)) - min(lo, b[0][0]) <= MAX_RECORDS
                       and not _overlaps(spans, b)), None)
            if at is None:
                bins.append([])
                at = len(bins) - 1
            bins[at].append(member)
        for members_bin in bins:
            if len(members_bin) < 2 and not any(key[0] == str(members_bin[0][2].path) for key in skip):
                continue  # one file already, nothing in it redone elsewhere
            dest = Path(folder) / f"merged_{uuid4().hex[:12]}{Path(str(members_bin[0][2].path)).suffix}"
            # Shard by shard in id order: a redone shard sits between the ones of an older file.
            parts = sorted((span, str(row.path)) for _lo, _hi, row, spans in members_bin for span in spans)
            merge_shards([(path, span) for span, path in parts], dest, skip=skip)
            merged.append(([m[2] for m in members_bin], dest))
    if empty:
        merged.append((empty, None))
    if not merged:
        return 0
    old = []
    for members_bin, dest in merged:
        if dest is not None:
            add_file(project_db, dest, generation_id=int(generation_id), role=str(members_bin[0].role))
        with project_db.get_session() as session:
            for row in members_bin:
                found = session.get(CatalogFile, int(row.id))
                if found is not None:
                    session.delete(found)
                old.append(str(row.path))
            session.commit()
    for text in old:
        _unlink(text)
    return len(old)


def compact_tables(project_db, role: str, *, unique_by: Iterable[str] = (), stats: Iterable[str] = (),
                   rows_per_group: int = 1 << 20) -> int:
    """The table files of ``role`` (a task's file each) concatenated into one per generation and
    folder: each file's rows stay one row group (or more, past ``rows_per_group``), statistics
    only on the ``stats`` columns (a reader filters on those). With ``unique_by``, the row of the
    newest file wins per value of those columns: a redo, or what a crash between `add_file` and
    `states.mark_done` ran twice. Columns a file lacks are null; the newest file's schema
    metadata is the merged one's. How many files went.

    Streams file by file: only the ``unique_by`` columns of the whole role are held in memory.
    Crash-safe in the order of `compact`.
    """
    from uuid import uuid4

    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    from sqlmodel import select

    from ms_flow.core.database.project_models import CatalogFile

    with project_db.get_session() as session:
        rows = list(session.exec(select(CatalogFile).where(
            CatalogFile.role == str(role), CatalogFile.status == READY, CatalogFile.n_shards == 0,
        ).order_by(CatalogFile.id)).all())
    bins: dict[tuple[Any, str], list[Any]] = {}
    for row in rows:
        if Path(str(row.path)).is_file():
            bins.setdefault((row.generation_id, str(Path(str(row.path)).parent)), []).append(row)
    unique_by, stats = list(unique_by), list(stats)
    gone = 0
    for (generation_id, folder), members in bins.items():
        if len(members) < 2:
            continue
        paths = [str(row.path) for row in members]
        schemas = [pq.read_schema(path) for path in paths]
        schema = pa.unify_schemas(schemas).with_metadata(schemas[-1].metadata)
        keep = None
        if unique_by:
            keys = pa.concat_tables([pq.read_table(path, columns=unique_by) for path in paths])
            order = pa.array(range(keys.num_rows), pa.int64())
            newest = keys.append_column("__row", order).group_by(unique_by).aggregate([("__row", "max")])
            keep = pc.is_in(order, value_set=newest["__row_max"])
        dest = Path(folder) / f"merged_{uuid4().hex[:12]}{Path(paths[0]).suffix}"
        tmp = dest.with_name(dest.name + ".tmp")
        offset = 0
        with pq.ParquetWriter(tmp, schema, compression="zstd", write_statistics=stats or False) as writer:
            for path in paths:
                table = pq.read_table(path)
                if keep is not None:
                    mask, offset = keep.slice(offset, table.num_rows), offset + table.num_rows
                    table = table.filter(mask)
                if table.num_rows:
                    table = pa.table([table[f.name] if f.name in table.column_names else pa.nulls(table.num_rows, f.type)
                                      for f in schema], schema=schema)
                    writer.write_table(table, row_group_size=int(rows_per_group))
        tmp.replace(dest)
        add_file(project_db, dest, generation_id=generation_id, role=str(role))
        with project_db.get_session() as session:
            for row in members:
                found = session.get(CatalogFile, int(row.id))
                if found is not None:
                    session.delete(found)
            session.commit()
        for path in paths:
            _unlink(path)
        gone += len(members)
    return gone


def roles(project_db, *, generation_id: int | None = None) -> list[str]:
    """The roles the active generation (or the one named) has files of, sorted: what a view's
    selector lists. `SOURCE` first when it is there."""
    from sqlmodel import select

    from ms_flow.core.database.project_models import CatalogFile

    if generation_id is None:
        generation_id = active_generation_id(project_db)
    with project_db.get_session() as session:
        query = select(CatalogFile.role).distinct()
        if generation_id is not None:
            query = query.where(CatalogFile.generation_id == int(generation_id))
        found = sorted({str(role) for role in session.exec(query).all() if role})
    return ([SOURCE] if SOURCE in found else []) + [role for role in found if role != SOURCE]


def adopt(project_db, file_id: int, path: str | Path, *, checksum: str = "") -> bool:
    """A file of another host, brought here: it is local from now on, with the counts its header
    says. False when ``checksum`` (its sha256 there) is not what arrived, and the row is left
    alone — never while anything still runs on it."""
    if checksum and _sha256(path) != str(checksum):
        return False
    update_file(project_db, int(file_id), path=str(path), location="", status=READY,
                **_counts(project_db, path))
    return True


def _sha256(path: str | Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with open(path, "rb") as file:
        for block in iter(lambda: file.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def remove(project_db, role: str) -> int:
    """Every file of ``role``, any generation: rows first, then the files. How many went."""
    from sqlmodel import select

    from ms_flow.core.database.project_models import CatalogFile

    with project_db.get_session() as session:
        rows = list(session.exec(select(CatalogFile).where(CatalogFile.role == str(role))).all())
        paths = [str(row.path or "") for row in rows]
        for row in rows:
            session.delete(row)
        session.commit()
        still = set(session.exec(select(CatalogFile.path)).all())
    for path in paths:
        if path and path not in still:
            _unlink(path)
    return len(rows)


def _overlaps(spans: list[tuple[int, int]], members: list) -> bool:
    """Does any of ``spans`` share an id with the shards of ``members``. ponytail: pairwise, a
    compaction sees tens of files; an interval tree if it ever sees thousands."""
    return any(lo < b_hi and b_lo < hi for lo, hi in spans for m in members for b_lo, b_hi in m[3])


def _pinned(project_db, spec: QuerySpec | None) -> QuerySpec:
    """Any read, on the active generation and on the source files. A spec that names a generation is
    left alone: that is how a redo reads its parent, and how a step counts what it has written
    so far. One that names a role reads that role."""
    spec = spec or shard_spec()
    filters = dict(spec.filters)
    filters.setdefault("role", SOURCE)
    if "generation_id" not in filters:
        generation_id = active_generation_id(project_db)
        if generation_id is not None:
            filters["generation_id"] = generation_id
    return replace(spec, filters=filters)


def _file_query(spec: QuerySpec, *, ready: bool = True) -> tuple[QuerySpec, dict[str, Any]]:
    """The spec as SQL over the files, and the filters left for the shards inside them."""
    file_filters = {k: v for k, v in spec.filters.items() if k not in _SHARD_FILTERS}
    if ready:  # read: what is closed and here (a file left on another host is listed, never opened)
        file_filters["status"] = READY
        file_filters.setdefault("location", "")
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
        if not row["n_shards"]:
            continue  # a table file: rows, no shards
        for index, group in enumerate(file_groups(row["path"])):
            sid = shard_id(row["id"], index)
            if "scope" in wanted and not _matches(group.scope, wanted["scope"]):
                continue
            if "shard_id" in wanted and not _matches(sid, wanted["shard_id"]):
                continue
            yield _shard_row(sid, row, index, group)


def _shard_row(sid: int, row: dict[str, Any], index: int, group) -> dict[str, Any]:
    return {"id": sid, "file_id": int(row["id"]), "generation_id": row["generation_id"], "role": row["role"],
            "path": row["path"], "shard_index": index, "scope": group.scope, "base_id": group.base_id,
            "slot_count": group.slot_count, "n_records": group.n_records, "origin": group.origin,
            "n_dropped": group.n_dropped}


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
        group = file_groups(row["path"])[index]
    except (OSError, IndexError, KeyError):
        return None
    return _shard_row(int(value), row, index, group)


def shards(
    project_db, spec: QuerySpec | None = None, *, whole: bool = False, page_size: int = 1000
) -> Iterator[dict[str, Any]]:
    """Shard rows, minus the active exclusions: a shard left with nothing is not yielded, a
    partial one carries ``record_ids``. ``whole``: every record, for a step that transforms the
    dataset rather than selects from it (so undoing a cut needs no redo of it)."""
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


def _newest(project_db, role: str, generation_id: Any, page_size: int) -> dict[int, dict[str, Any]]:
    """origin -> the newest shard of ``role`` made from it (files in id order: later wins).
    ponytail: one dict entry per derived shard (4k per 1M records); a SQL side index if that hurts."""
    filters = {"role": str(role)}
    if generation_id is not None:
        filters["generation_id"] = generation_id
    newest: dict[int, dict[str, Any]] = {}
    for row in _iter_shards(project_db, shard_spec(**filters), page_size):
        if row["origin"] is not None:
            newest[int(row["origin"])] = row
    return newest


def derived(project_db, role: str, spec: QuerySpec | None = None, *, whole: bool = False,
            page_size: int = 1000) -> Iterator[dict[str, Any]]:
    """The shards of ``role`` made from the source shards of ``spec`` (newest copy each), narrowed
    as those are: a partial source shard hands its ``record_ids`` to the derived one (same ids).
    ``whole``: not narrowed. A source shard not derived yet yields nothing (`underived`)."""
    generation_id = (spec.filters.get("generation_id") if spec is not None else None)
    newest = _newest(project_db, role, generation_id, page_size)
    for row in shards(project_db, spec, whole=whole, page_size=page_size):
        made = newest.get(int(row["id"]))
        if made is not None:
            yield {**made, "record_ids": row["record_ids"]} if "record_ids" in row else made


def underived(project_db, role: str, spec: QuerySpec | None = None, *, whole: bool = True,
              page_size: int = 1000) -> Iterator[dict[str, Any]]:
    """The source shards of ``spec`` with no shard of ``role`` made from them: a derive step's feed.
    ``whole`` by default: a derivation transforms the dataset, so undoing a cut needs no redo."""
    generation_id = (spec.filters.get("generation_id") if spec is not None else None)
    newest = _newest(project_db, role, generation_id, page_size)
    for row in shards(project_db, spec, whole=whole, page_size=page_size):
        if int(row["id"]) not in newest:
            yield row


def has_files(project_db, spec: QuerySpec | None = None) -> bool:
    """Is there any file at all (being written or ready) in the active generation."""
    return next(files(project_db, spec, page_size=1), None) is not None


__all__ = [
    "FIELDS", "GROUP_BITS", "READY", "SOURCE", "TABLE", "WRITING", "activate_generation", "active_generation_id", "add_file", "adopt",
    "compact", "compact_tables", "count", "create_generation", "derived", "file_row", "files", "finish_file", "get_shard", "has_files",
    "record_count", "refresh_active", "remove", "roles", "shard_id", "shard_spec", "shards", "split_shard_id", "sweep",
    "underived",
    "update_file",
]
