"""The catalog of a sharded dataset: which files there are, and the shards inside them.

The app writes records and says who is in or out; ms_flow keeps where they are. A row of
`ms_shard_files` is one *file*, never one shard: 1M records are four rows, not four thousand. The
shards of a file are listed in its own index (`shard.file_groups`), so the
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
          "scopes", "location", "n_rows", "n_retired", "end")
SOURCE = "source"  # the dataset itself; a step's outputs carry roles of their own
WRITING, READY, SEALED = "writing", "ready", "sealed"
READABLE = (READY, SEALED)  # a read takes these; an append only READY
GROUP_BITS = 20  # groups of a file: MAX_RECORDS (1 << 20) ids hold at most that many shards
# Filters that name a shard, not a file: applied to the groups a file lists.
_SHARD_FILTERS = ("scope", "shard_id")
_COUNTS = ("n_shards", "n_records", "n_active", "n_shards_excluded", "scopes", "n_rows", "n_retired", "end")  # what a file says of itself
_COMMITTED = ("n_rows", "n_retired", "end")  # what only grows (I6)


class StaleIndex(ValueError):
    """An index below what the catalog saw committed: stale or foreign. Nothing was cut (I6)."""


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


def create_generation(project_db, *, step: str, folder: str | Path = "", job_id: str = "",
                      kept: bool = False) -> int:
    """Open a generation parented to the active one. Not active: nothing reads it yet. ``kept``:
    no activation deletes it (D4)."""
    from ms_flow.core.database.project_models import ShardGeneration

    parent_id = active_generation_id(project_db)
    with project_db.get_session() as session:
        row = ShardGeneration(parent_id=parent_id, step=str(step or ""), folder=str(folder or ""),
                              job_id=str(job_id or ""), kept=bool(kept))
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
        keep |= {gen_id for gen_id, row in generations.items() if row.kept}
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
            # Their writers' lock files too: no writer appends to a generation that went.
            dropped |= {str(_lock_path(Path(row.path).parent, row.role, gen_id)) for row in files if row.path}
        # A file may be listed twice (a step that kept it): it goes only when no row points at it.
        paths = dropped - set(session.exec(select(CatalogFile.path)).all())
        session.commit()
    # Files last: a row that survives a missing file is a bug you can see, a file that survives
    # its row is an orphan nothing will ever look at again.
    for text in paths:
        if text:
            _unlink(text)


def _unlink(path: str | Path) -> None:
    """A record file and its companions (I14)."""
    from ms_flow.core.data.companions import companions

    Path(path).unlink(missing_ok=True)
    for companion in companions(path):
        Path(companion).unlink(missing_ok=True)


def _lock_path(folder: str | Path, role: str, generation_id: int | None) -> Path:
    """The lock file of the one `ContainerWriter` of ``(generation, role)`` in ``folder`` (I1)."""
    import re

    generation = "none" if generation_id is None else int(generation_id)
    return Path(folder) / f"{re.sub(r'[^A-Za-z0-9._-]+', '_', str(role))}.{generation}.lock"


def file_row(path: str | Path, *, role: str, generation_id: int | None, part: str | None = None) -> dict[str, Any]:
    """A ready file's catalog row, from its header (``n_active`` = every record: a derived file is
    read through `derived`, which narrows by its source's exclusions). ``sidecars`` names its
    companions too, so a worker on another disk brings them all back; it is no column.
    With ``part`` it is the receipt a task returns to `ms_flow.sinks.catalog_sink`: the file is a
    part, its shards are appended to the containers of its role and it goes."""
    from datetime import datetime

    from ms_flow.core.data.companions import companions

    counts = _counts(None, path, exclusions=())
    beside = companions(path)
    return {"generation_id": generation_id, "role": str(role), "path": str(path), "status": READY,
            "error": "", "created_at": datetime.now(), **counts,
            "sidecars": [name for name in beside if Path(name).is_file()], **({"part": str(part)} if part else {})}


def sweep(project_db, folder: str | Path) -> int:
    """Delete the files of ``folder`` the catalog does not know: a task's file published but never
    registered (the parent died in between), a writer's ``.tmp``. For a step, at its start, on its
    own output folder; their shards are simply made again. The companions of a known file are known,
    and so is every ``.lock``. How many went."""
    import os

    from sqlmodel import select

    from ms_flow.core.data.companions import whole
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
        if path.suffix == ".lock":  # a writer's (I1): never a step's to take away
            continue
        if path.name.endswith(".tmp") or os.path.abspath(whole(path)[0]) not in known:
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


def add_remote(project_db, path: str | Path, *, location: str, role: str, counts: dict[str, Any],
               generation_id: int | None) -> int:
    """A file a step made on another host and left there (``location``): registered once, its
    counts the ones made over there (`file_row`). The id of its row."""
    return add_file(project_db, path, generation_id=generation_id, role=role, location=location,
                    counts=counts, once=True)


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


def _counts(project_db, path: str | Path, exclusions=None, index=None) -> dict[str, Any]:
    """``index``: the container's ``(info, table)`` when the caller has just read it."""
    import pyarrow.compute as pc

    from ms_flow.core.data import container
    from ms_flow.core.data.shard import SUFFIX

    if not str(path).endswith(SUFFIX):  # a table file: any Parquet, rows of its own schema, no shards
        import pyarrow.parquet as pq

        rows = pq.ParquetFile(path).metadata.num_rows
        return {"n_shards": 0, "n_records": rows, "n_active": rows, "n_shards_excluded": 0, "scopes": ""}
    info, table = index or container.read_index_table(path)  # in Arrow: no Python row per shard
    live = table.filter(pc.invert(table["retired"]))
    n_records = pc.sum(live["n_records"]).as_py() or 0
    scopes = sorted(pc.unique(live["scope"].drop_null()).to_pylist())
    cut, cut_whole = _excluded(path, None, exclusions, project_db)
    return {"n_shards": live.num_rows, "n_records": n_records, **_committed(info, table),
            "n_active": n_records - cut, "n_shards_excluded": cut_whole,
            "scopes": ",".join(str(s) for s in scopes)}


def _committed(info: dict, table) -> dict[str, int]:
    """What an index commits (I6), from its Arrow table."""
    import pyarrow.compute as pc

    from ms_flow.core.data import container

    return {"n_rows": table.num_rows, "n_retired": pc.sum(table["retired"].cast("int64")).as_py() or 0,
            "end": container.index_end(info, table)}


def _check(row, now: dict[str, int]) -> None:
    low = [f"{name} {now[name]} < {getattr(row, name)}" for name in _COMMITTED if now[name] < int(getattr(row, name) or 0)]
    if low:
        raise StaleIndex(f"{row.path}: its index is below the catalog ({', '.join(low)}); refused, nothing cut")


def recount(project_db, file_id: int) -> dict[str, Any]:
    """A container's counts again, from its index (I5): what is committed, never lowered (I6)."""
    from ms_flow.core.data import container
    from ms_flow.core.data.shard import SUFFIX
    from ms_flow.core.database.project_models import CatalogFile

    with project_db.get_session() as session:
        row = session.get(CatalogFile, int(file_id))
        if row is None:
            raise ValueError(f"No catalog file {file_id}.")
        index = None
        if str(row.path).endswith(SUFFIX):
            index = container.read_index_table(row.path)
            _check(row, _committed(*index))
        counts = _counts(project_db, row.path, index=index)
        for name, value in counts.items():
            setattr(row, name, value)
        session.add(row)
        session.commit()
        return counts


def appender(project_db, file_id: int):
    """The `container.Appender` of a ready, local container, after I6: an index below the catalog
    is refused before the appender's open cuts anything. Its caller holds the locks (I1)."""
    from ms_flow.core.data import container
    from ms_flow.core.database.project_models import CatalogFile

    with project_db.get_session() as session:
        row = session.get(CatalogFile, int(file_id))
        if row is None:
            raise ValueError(f"No catalog file {file_id}.")
        if row.status != READY or row.location:
            raise ValueError(f"{row.path}: {row.status}{' on ' + row.location if row.location else ''}, not appended to (I19)")
        _check(row, _committed(*container.read_index_table(row.path)))
        return container.Appender(row.path)


def seal(project_db, file_id: int) -> None:
    """Read from now on, never appended again: what ships is sealed first (I15)."""
    from ms_flow.core.database.project_models import CatalogFile

    with project_db.get_session() as session:
        row = session.get(CatalogFile, int(file_id))
        if row is None or row.status not in READABLE:
            raise ValueError(f"No ready catalog file {file_id} to seal.")
        row.status = SEALED
        session.add(row)
        session.commit()


def _seal_shipped(project_db, path: str | Path) -> None:
    """A container of the catalog at ``path`` that still takes parts, sealed before it ships (I15):
    its hash is then the one that arrives. One its writer holds now cannot be sealed under it."""
    from sqlmodel import select

    from ms_flow.core.database.project_models import CatalogFile
    from ms_flow.engine.journal import Busy, own

    with project_db.get_session() as session:
        rows = session.exec(select(CatalogFile).where(CatalogFile.path == str(path), CatalogFile.location == "",
                                                      CatalogFile.status == READY)).all()
    for row in rows:
        try:
            lock = own(_lock_path(Path(path).parent, row.role, row.generation_id))
        except Busy as error:
            raise RuntimeError(f"{path} is still being written ({error}): send it once that ends") from None
        try:
            seal(project_db, row.id)
        finally:
            lock.close()


MAX_SHARDS = 4096  # of one container (I18): its index is rewritten whole per commit, ~10 ms at this size
MAX_BYTES = 4 << 30  # committed bytes of one container, compressed (I18)


def _step(step) -> None:
    """Where `ContainerWriter.commit_parts` is: a seam for the crash tests, nothing else."""


def _header(app: dict | None) -> dict:
    """What a container's header says of every shard in it: the part's own range and count left out.
    A part whose header differs goes to another container (a shard is read by its file's header)."""
    return {key: value for key, value in dict(app or {}).items()
            if key not in ("shard_id", "base_id", "slot_count", "record_count")}


class ContainerWriter:
    """The one writer of the containers of ``(generation, role)`` in ``folder``: receipts of parts
    in, shard ids out. A receipt is ``{"part": ..., "path": ..., ...}``: ``part`` names the unit of
    work and makes the commit idempotent (I10), ``path`` is the part container a worker made; its
    shards are copied as bytes, never decoded (I12). ``prefix``: the part ids of this submission,
    the only ones loaded from the indexes. ``on_commit(receipts)``: the caller's states, for every
    receipt of a group, the ones committed before included; it must be idempotent.

    The lock file is taken at the first commit and kept until `close` (I1), so what this writer read
    of the indexes stays true between commits. Two roles written as one: `PairWriter`.
    """

    def __init__(self, project_db, folder: str | Path, *, generation_id: int | None, role: str = SOURCE,
                 prefix: str = "", on_commit=None):
        import re

        # None: files of no generation (an app's own, outside any import)
        self.db, self.folder = project_db, Path(folder)
        self.generation_id = None if generation_id is None else int(generation_id)
        self.role, self.prefix, self.on_commit = str(role), str(prefix), on_commit
        self._name = re.sub(r"[^A-Za-z0-9._-]+", "_", self.role)
        self._lock = self._appender = self._file_id = self._head = self._app = None
        self._done: dict[str, list[int]] = {}

    def __enter__(self) -> "ContainerWriter":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    def close(self) -> None:
        self._appender = None
        if self._lock is not None:
            self._lock.close()
            self._lock = None

    def seal(self) -> None:
        """Seal the container being appended to (I15): the next part starts another."""
        if self._lock is None:
            self._open()
        if self._appender is not None:
            seal(self.db, self._file_id)
            self._appender = None

    def _open(self) -> None:
        from sqlmodel import select

        from ms_flow.core.data import container
        from ms_flow.core.data.shard import SUFFIX
        from ms_flow.core.database.project_models import CatalogFile
        from ms_flow.engine.journal import own

        self.folder.mkdir(parents=True, exist_ok=True)
        self._lock = own(_lock_path(self.folder, self.role, self.generation_id))  # Busy names the holder
        try:
            _step(2)
            with self.db.get_session() as session:
                rows = session.exec(
                    select(CatalogFile.id, CatalogFile.path, CatalogFile.status, CatalogFile.location)
                    .where(CatalogFile.generation_id == self.generation_id, CatalogFile.role == self.role)
                    .order_by(CatalogFile.id)
                ).all()
            rows = [row for row in rows if str(row.path).endswith(SUFFIX)]
            for file_id, path, _status, location in rows:
                if location:  # ponytail: parts committed on a remote copy are seen when it comes back (M10)
                    continue
                for row in container.read_index(path)[1]:
                    if row["part"] and row["part"].startswith(self.prefix):
                        self._done.setdefault(row["part"], []).append(shard_id(file_id, row["shard"]))
            if rows and rows[-1].status == READY and not rows[-1].location:  # I9: only the newest
                self._file_id = int(rows[-1].id)
                self._appender = appender(self.db, self._file_id)  # I6 first
                self._head = self._appender.head
                self._app = _header(self._appender.info.get("app"))
                recount(self.db, self._file_id)
        except BaseException:
            self.close()
            raise

    def _create(self, head: bytes, app: dict) -> None:
        """The next container: its file, its empty index, its catalog row, in that order (a crash
        before the row leaves files `sweep` removes)."""
        import uuid

        import pyarrow.ipc as ipc

        from ms_flow.core.data import container
        from ms_flow.core.data.shard import SUFFIX

        path = self.folder / f"{self._name}.{uuid.uuid4().hex[:16]}{SUFFIX}"
        self._appender = container.Appender.create(path, ipc.read_schema(ipc.read_message(head)), app)
        _step(5.5)
        self._file_id = add_file(self.db, path, generation_id=self.generation_id, role=self.role)
        self._head, self._app = self._appender.head, _header(app)

    def _flush(self, batch: list) -> None:
        """One commit of whole parts into the current container. An unreadable part fails its own
        receipt and the rest is committed again without it; a failed write poisons the writer."""
        from ms_flow.core.data import container

        while batch:
            _step(6)
            current = [None]

            def shards():
                for at, (receipt, live) in enumerate(batch):
                    current[0] = receipt
                    for row in live:
                        yield ({**row, "part": receipt["part"], "meta": _meta(row["meta"], receipt.get("meta"))},
                               container._blocks(receipt["path"], row))
                    if at == 0:
                        _step(6.5)

            try:
                ids = iter(self._appender.commit(shards()))
            except (OSError, ValueError) as error:
                if self._appender._broken is not None:
                    raise
                current[0]["error"] = f"{current[0]['path']}: {error}"
                batch = [item for item in batch if item[0] is not current[0]]
                continue
            for receipt, live in batch:
                self._done[receipt["part"]] = [shard_id(self._file_id, next(ids)) for _ in live]
            return

    def commit_parts(self, receipts: Iterable[dict], *, again: bool = False) -> list[dict]:
        """Commit a group of receipts; every one comes back with its ``shards`` (committed now or
        before) or an ``error``. Only then may the engine be told they are done (I11). A receipt's
        ``meta`` is added to its shards' own. ``again``: a part committed before is appended once
        more (what `PairWriter` asks of its bulk)."""
        import pyarrow.ipc as ipc

        from ms_flow.core.data import container
        from ms_flow.core.data.companions import whole

        # A receipt's `error` is what this commit says of it: a task's own note is not one.
        receipts = [{key: value for key, value in receipt.items() if key != "error"} for receipt in receipts]
        _step(1)
        if self._lock is None:
            self._open()
        _step(3)
        fresh, seen = [], set()
        for receipt in receipts:  # before any part is opened: it may be gone (I10)
            if (again or receipt["part"] not in self._done) and receipt["part"] not in seen:
                seen.add(receipt["part"])
                fresh.append(receipt)
        _step(4)
        valid = []
        for receipt in fresh:
            try:
                info, rows = container.read_index(receipt["path"])
                head = container.read_head(receipt["path"], info)
                if self._head is not None and head != self._head and not ipc.read_schema(ipc.read_message(head)).equals(
                        ipc.read_schema(ipc.read_message(self._head)), check_metadata=True):
                    raise ValueError("another schema")
            except (OSError, ValueError) as error:
                receipt["error"] = f"{receipt['path']}: {error}"
                continue
            self._head = self._head or head
            valid.append((receipt, [row for row in rows if not row["retired"]], head, info.get("app") or {}))
        _step(5)
        batch: list = []
        for receipt, live, head, app in valid:
            if self._appender is None:
                self._create(head, app)
            if not batch:
                used, size = self._appender.index.num_rows, self._appender.end
            length = sum(row["length"] for row in live)
            if (used and (used + len(live) > MAX_SHARDS or size + length > MAX_BYTES)) or _header(app) != self._app:
                self._flush(batch)  # I18: a part goes whole into one container
                batch = []
                seal(self.db, self._file_id)
                self._create(head, app)
                used, size = 0, self._appender.end
            batch.append((receipt, live))
            used, size = used + len(live), size + length
        self._flush(batch)
        accepted = []
        for receipt in receipts:
            if "error" in receipt:
                continue
            if receipt["part"] in self._done:
                receipt["shards"] = list(self._done[receipt["part"]])
                accepted.append(receipt)
            else:
                receipt["error"] = f"{receipt['path']}: not committed, another receipt of its part failed"
        _step(7)
        for file_id in sorted({split_shard_id(s)[0] for receipt in accepted for s in receipt["shards"]}):
            recount(self.db, file_id)  # idempotent: what the index says
        if self.on_commit is not None:
            self.on_commit(accepted)
        _step(8)
        for receipt in accepted:  # after the commits: a replay no longer needs the part (I11)
            for name in whole(receipt["path"]):
                Path(name).unlink(missing_ok=True)
        _step(9)
        return receipts


    def retire(self, shards: Iterable[int]) -> list[int]:
        """Out of every read: those of ``shards`` (shard ids) in the container being appended to.
        Those of a sealed container stay (I19). Returns the ones retired."""
        if self._lock is None:
            self._open()
        if self._appender is None:
            return []
        mine = sorted({split_shard_id(s)[1] for s in shards if split_shard_id(s)[0] == self._file_id})
        if mine:
            self._appender.retire(mine)
            recount(self.db, self._file_id)
        return [shard_id(self._file_id, number) for number in mine]

    def retire_covered(self, column: str) -> list[int]:
        """Retire what newer shards of the same origin cover: a live shard of the container being
        appended to, each of whose ``column`` values a newer one holds too. A redo hides what it
        covers and nothing else (a narrowed one keeps the older shard). Returns the ones retired.
        ponytail: a set of values per origin, only for origins with more than one live shard."""
        from collections import Counter

        from ms_flow.core.data import container

        if self._lock is None:
            self._open()
        if self._appender is None:
            return []
        live = [row for row in self._appender.rows if not row["retired"] and row["origin"] is not None]
        counts = Counter(row["origin"] for row in live)
        repeated = [row for row in live if counts[row["origin"]] > 1]
        if not repeated:
            return []
        values = container.read_table(self._appender.path, self._appender.info, repeated, [column])[column]
        at, slices = 0, []
        for row in repeated:
            size = row["n_records"] + row["n_dropped"]
            slices.append(set(values.slice(at, size).to_pylist()))
            at += size
        seen: dict[int, set] = {}
        gone = []
        for row, found in sorted(zip(repeated, slices), key=lambda item: -item[0]["shard"]):  # newest first (I9)
            newer = seen.setdefault(row["origin"], set())
            if found <= newer:
                gone.append(shard_id(self._file_id, row["shard"]))
            newer |= found
        return self.retire(gone)


def _meta(mine: str | None, extra: dict | None) -> str | None:
    """A shard's ``meta`` with a receipt's added. ponytail: in the index only; the copy beside the
    data keeps the part's own, so an index rebuilt from the data loses what a commit added."""
    if not extra:
        return mine
    import json

    from ms_flow.core.data.container import META_LIMIT

    text = json.dumps({**json.loads(mine or "{}"), **extra}, separators=(",", ":"))
    if len(text.encode()) > META_LIMIT:
        raise ValueError("a shard's meta is for a few facts, not for data")
    return text


class PairWriter:
    """Two roles of one key written as one (M6a): a task's ``bulk`` part (big, read rarely) and its
    index part (``path``: what is queried). A receipt is ``{"part", "path", "bulk"}``. The bulk is
    committed first, the index second, and the index commit is visibility. Idempotent on the index
    only: a part it has is skipped whole; one it has not gets its bulk appended again, even if an
    older copy is there (a crash between the two), and its index shards' ``meta["bulk"]`` names the
    bulk shards of that same attempt, never "the latest of the part".

    Both lock files are taken, bulk then index, at the first commit and held until `close` (I1). A
    lock file is per open file, so no other writer of either role, in this process or another, comes
    between the two commits of a receipt. ponytail: no in-process key lock on top, the two are it.
    """

    def __init__(self, project_db, folder: str | Path, *, generation_id: int | None, roles: tuple[str, str],
                 prefix: str = "", on_commit=None):
        bulk, index = roles
        self.bulk = ContainerWriter(project_db, folder, generation_id=generation_id, role=bulk, prefix=prefix)
        self.index = ContainerWriter(project_db, folder, generation_id=generation_id, role=index, prefix=prefix,
                                     on_commit=on_commit)

    def __enter__(self) -> "PairWriter":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    def close(self) -> None:
        self.index.close()
        self.bulk.close()

    def retire_covered(self, column: str) -> list[int]:
        """`ContainerWriter.retire_covered` of the index, and the bulk shards those named. Returns
        the index shards retired."""
        import json

        gone = self.index.retire_covered(column)
        numbers = {split_shard_id(s)[1] for s in gone}
        self.bulk.retire([s for row in self.index._appender.rows if row["shard"] in numbers
                          for s in json.loads(row["meta"] or "{}").get("bulk", ())] if gone else [])
        return gone

    def commit_parts(self, receipts: Iterable[dict]) -> list[dict]:
        """As `ContainerWriter.commit_parts`, for receipts of both parts."""
        from ms_flow.core.data.companions import whole

        receipts = [{key: value for key, value in receipt.items() if key not in ("error", "meta")}
                    for receipt in receipts]
        for writer in (self.bulk, self.index):
            if writer._lock is None:
                writer._open()
        skipped = [receipt for receipt in receipts if receipt["part"] in self.index._done]
        fresh = [receipt for receipt in receipts if receipt["part"] not in self.index._done]
        bulk = self.bulk.commit_parts([{"part": r["part"], "path": r["bulk"]} for r in fresh], again=True)
        _step("between")
        for receipt, done in zip(fresh, bulk):
            if "error" in done:
                receipt["error"] = done["error"]
            else:
                receipt["meta"] = {"bulk": done["shards"]}
        committed = iter(self.index.commit_parts([receipt for receipt in receipts if "error" not in receipt]))
        receipts = [receipt if "error" in receipt else next(committed) for receipt in receipts]
        for receipt in skipped:  # its index has it: this attempt's bulk is not needed
            for name in whole(receipt["bulk"]):
                Path(name).unlink(missing_ok=True)
        return receipts


def _excluded(path, groups, exclusions, project_db) -> tuple[int, int]:
    """How many usable records of ``groups`` (None: the file's live ones, read only when a cut
    exists) the cuts hide, and how many of those shards they hide whole: a cut naming a hole or a
    drop hides none."""
    import numpy as np

    from ms_flow.core.data.shard import file_groups, usable_ids

    if exclusions is None:
        from ms_flow.core.data.exclusions import active_exclusions

        exclusions = active_exclusions(project_db)
    if not exclusions:
        return 0, 0
    if groups is None:
        groups = [g for g in file_groups(path) if not g.retired]
    records = shards = 0
    for group in groups:
        if group.scope is None or not group.n_records:
            continue
        cut = exclusions.in_range(group.scope, group.base_id, group.base_id + group.slot_count)
        if len(cut):
            hidden = len(np.intersect1d(cut, usable_ids(path, group), assume_unique=True))
            records += hidden
            shards += hidden == group.n_records
    return records, shards


def refresh_active(project_db) -> None:
    """``n_active`` and ``n_shards_excluded`` of every ready file, after a cut was made or undone:
    one index read each."""
    from sqlmodel import select

    from ms_flow.core.data.exclusions import active_exclusions
    from ms_flow.core.data.shard import file_groups
    from ms_flow.core.database.project_models import CatalogFile

    exclusions = active_exclusions(project_db)
    with project_db.get_session() as session:
        for row in session.exec(select(CatalogFile).where(CatalogFile.status.in_(READABLE))).all():
            try:
                groups = [g for g in file_groups(row.path) if not g.retired]
            except (OSError, ValueError):
                continue  # a file gone missing is the reader's error to raise, not this one's
            cut, row.n_shards_excluded = _excluded(row.path, groups, exclusions, project_db)
            row.n_active = int(row.n_records) - cut
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

    from ms_flow.core.data.shard import MAX_RECORDS, SUFFIX, file_groups, merge_shards
    from ms_flow.core.database.project_models import CatalogFile

    with project_db.get_session() as session:
        # READY, not READABLE: a compaction deletes its inputs, and a sealed container stays as it shipped.
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
        listed[int(row.id)] = [g for g in file_groups(path) if not g.retired]
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
        for group in listed[int(row.id)]:
            if group.origin is not None and newest[(str(row.role), group.origin)] != int(row.id):
                skip.add((str(row.path), group.shard))
            else:
                kept.append((group.shard, group))
        if not kept:
            if listed[int(row.id)]:
                empty.append(row)
            continue
        lo, hi = min(g.base_id for _i, g in kept), max(g.base_id + g.slot_count for _i, g in kept)
        spans = [(g.base_id, g.base_id + g.slot_count, index) for index, g in kept]
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
            dest = Path(folder) / f"merged_{uuid4().hex[:12]}{SUFFIX}"
            # Shard by shard in id order: a redone shard sits between the ones of an older file.
            parts = sorted((span, str(row.path)) for _lo, _hi, row, spans in members_bin for span in spans)
            merge_shards([(path, span[2]) for span, path in parts], dest)
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


def referenced_shards(project_db):
    """Every shard id something of the project names, as a sorted numpy array: an origin, or the
    other part of a pair (``meta["bulk"]``), in the index of a container here, and a state
    (`states`). A merge leaves their containers as they are: it changes shard ids.
    ponytail: an app that keeps shard ids in tables of its own must not merge those roles."""
    import json

    import numpy as np
    from sqlmodel import select

    from ms_flow.core.data import container, states
    from ms_flow.core.database.project_models import CatalogFile

    with project_db.get_session() as session:
        paths = session.exec(select(CatalogFile.path).where(CatalogFile.location == "")).all()
    found = []
    for path in paths:
        if not Path(str(path)).is_file():
            continue
        _info, table = container.read_index_table(path)
        origins = table["origin"].drop_null().to_numpy()
        found.append(origins.astype(np.int64))
        for meta in table["meta"].drop_null().to_pylist():  # ponytail: only the rows that have one
            if '"bulk"' in meta:
                found.append(np.asarray(json.loads(meta)["bulk"], dtype=np.int64))
    for row in states.progress(project_db):
        found.append(states._table(project_db, row["kind"], row["key"])["shard"].to_numpy().astype(np.int64))
    return np.unique(np.concatenate(found)) if found else np.empty(0, dtype=np.int64)


def merge(project_db, generation_id: int | None, role: str) -> int:
    """The containers of ``(generation, role)`` here merged into as few as their bound allows
    (I18): the small ones and those with retired shards, their live shards copied as bytes (I12),
    the retired left out. Returns how many containers went.

    It changes shard ids, so it leaves as they are: a container one of whose shards something
    names (`referenced_shards`), one with a copy on another host (``ms_locations``) or brought
    from one (`locations.bring` keeps it as it is there), and a folder whose writer runs now (its
    lock). For when no job reads them: the caller sees to that.

    A kill leaves one readable copy: the new containers are whole before their rows exist, the
    rows swap in one transaction, and the old files go after it (`sweep` finds either leftover).
    """
    import uuid

    import numpy as np
    import pyarrow.ipc as ipc
    from sqlmodel import select

    from ms_flow.core.data import container
    from ms_flow.core.data.shard import SUFFIX
    from ms_flow.core.database.project_models import CatalogFile, RemoteLocation
    from ms_flow.engine.journal import Busy, own

    with project_db.get_session() as session:
        rows = list(session.exec(select(CatalogFile).where(
            CatalogFile.generation_id == generation_id, CatalogFile.role == str(role),
            CatalogFile.status.in_(READABLE), CatalogFile.location == "").order_by(CatalogFile.id)).all())
        elsewhere = set(session.exec(select(RemoteLocation.path)).all())
    project_dir = Path(getattr(project_db, "project_dir", "") or "")
    hosts = project_dir / "hosts"
    refs = referenced_shards(project_db)
    by_folder: dict[str, list] = {}
    for row in rows:
        path = Path(str(row.path))
        if not path.is_file() or str(path).startswith(f"{hosts}/"):
            continue
        try:
            inside = path.resolve().relative_to(project_dir.resolve()).as_posix() if project_dir.parts else None
        except ValueError:
            inside = None
        if inside is not None and inside in elsewhere:
            continue
        info, index = container.read_index(path)
        ids = np.asarray([shard_id(int(row.id), r["shard"]) for r in index], dtype=np.int64)
        if ids.size and np.isin(ids, refs).any():
            continue
        live = [r for r in index if not r["retired"]]
        if len(live) >= MAX_SHARDS and len(live) == len(index):
            continue  # full, nothing dead in it
        by_folder.setdefault(str(path.parent), []).append((row, path, info, live, len(index) - len(live)))
    went = 0
    name = __import__("re").sub(r"[^A-Za-z0-9._-]+", "_", str(role))
    for folder, members in by_folder.items():
        if len(members) < 2 and not any(dead for *_rest, dead in members):
            continue
        try:
            lock = own(_lock_path(folder, role, generation_id))
        except Busy:
            continue  # its writer appends now
        try:
            made, old = [], []
            by_head: dict[bytes, list] = {}
            for member in members:
                by_head.setdefault(bytes(container.read_head(member[1], member[2])), []).append(member)
            for head, group in by_head.items():
                bins, used, size = [[]], 0, 0
                for member in group:
                    length = sum(r["length"] for r in member[3])
                    if bins[-1] and (used + len(member[3]) > MAX_SHARDS or size + length > MAX_BYTES):
                        bins.append([])
                        used, size = 0, 0
                    bins[-1].append(member)
                    used, size = used + len(member[3]), size + length
                for members_bin in bins:
                    dest = Path(folder) / f"{name}.{uuid.uuid4().hex[:16]}{SUFFIX}"
                    appender = container.Appender.create(dest, ipc.read_schema(ipc.read_message(head)),
                                                         members_bin[0][2].get("app"))
                    appender.commit(({**r}, container._blocks(path, r)) for _row, path, _info, live, _d in members_bin
                                    for r in live)
                    made.append(dest)
                    old += [member[0] for member in members_bin]
            _step("merge:written")
            with project_db.get_session() as session:
                for dest in made:
                    counts = {k: v for k, v in _counts(project_db, dest).items() if k in _COUNTS}
                    session.add(CatalogFile(generation_id=generation_id, role=str(role), path=str(dest),
                                            status=READY, location="", **counts))
                for row in old:
                    session.delete(session.get(CatalogFile, int(row.id)))
                session.commit()  # the swap: either every old row or every new one
            _step("merge:swapped")
            for row in old:
                _unlink(row.path)
            went += len(old)
        finally:
            lock.close()
    return went


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
        rows = list(session.exec(select(CatalogFile).where(  # READY: these files are rewritten (see `compact`)
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
    alone — never while anything still runs on it. False too when a companion did not arrive (I14).
    ponytail: ``checksum`` covers the file, not its companions; a sealed index (I15) is read whole
    with its own crc on the first open."""
    from ms_flow.core.data.companions import companions

    if checksum and _sha256(path) != str(checksum) or not all(Path(c).is_file() for c in companions(path)):
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


def _overlaps(spans: list[tuple[int, int, int]], members: list) -> bool:
    """Does any of ``spans`` share an id with the shards of ``members``. ponytail: pairwise, a
    compaction sees tens of files; an interval tree if it ever sees thousands."""
    return any(lo < b_hi and b_lo < hi for lo, hi, _n in spans for m in members for b_lo, b_hi, _m in m[3])


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
        file_filters["status__in"] = list(READABLE)
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
        for group in file_groups(row["path"]):
            if group.retired:
                continue  # out of every read; its number stays taken
            index = group.shard
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
            "n_dropped": group.n_dropped, "present": group.present}


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
    except (OSError, IndexError, ValueError):
        return None
    if group.retired:
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
        field = getattr(CatalogFile, key.removesuffix("__in"))
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


def progress(project_db, generation_id: int, role: str) -> dict[str, Any]:
    """What a derived step has made of generation ``generation_id``, in the shape of
    `states.progress`: its index is its state. Done = source shards with a shard of ``role`` (the
    newest per origin), its records and its drops (the failed ones); totals = the source's. Reads
    the indexes of ``role``'s files and the file rows; no data file. A task that failed wrote
    nothing: its shards are not done and run again, so no shard counts as failed here."""
    import numpy as np
    import pyarrow as pa
    import pyarrow.compute as pc

    from ms_flow.core.data import container
    from ms_flow.core.data.shard import SUFFIX
    from ms_flow.core.data.states import COMPLETE, PARTIAL, summary

    made = [container.read_index_table(row["path"])[1]
            for row in files(project_db, shard_spec(generation_id=int(generation_id), role=str(role)))
            if str(row["path"]).endswith(SUFFIX)]
    done_ = records = dropped = 0
    if made:
        table = pa.concat_tables([t.select(["origin", "n_records", "n_dropped", "retired"]) for t in made])
        table = table.filter(pc.and_(pc.invert(table["retired"]), pc.is_valid(table["origin"])))
        origins = table["origin"].to_numpy()
        _, last = np.unique(origins[::-1], return_index=True)  # files in id order: the later wins
        newest = table.take(len(origins) - 1 - last)
        done_ = newest.num_rows
        records = int(pc.sum(newest["n_records"]).as_py() or 0)
        dropped = int(pc.sum(newest["n_dropped"]).as_py() or 0)
    source = shard_spec(generation_id=int(generation_id), role=SOURCE)
    shards_total, records_total = _sum(project_db, source, "n_shards"), _sum(project_db, source, "n_records")
    return summary(kind=str(role), key="", status=COMPLETE if done_ >= (shards_total or 0) else PARTIAL,
                   generation_id=int(generation_id), shards_done=done_, shards_failed=0,
                   shards_total=shards_total, records_done=records, records_failed=dropped,
                   records_total=records_total)


def has_files(project_db, spec: QuerySpec | None = None) -> bool:
    """Is there any file at all (being written or ready) in the active generation."""
    return next(files(project_db, spec, page_size=1), None) is not None


__all__ = [
    "ContainerWriter", "FIELDS", "GROUP_BITS", "MAX_BYTES", "MAX_SHARDS", "READABLE", "READY", "SEALED", "SOURCE", "StaleIndex", "TABLE", "WRITING", "activate_generation", "appender", "active_generation_id", "add_file", "add_remote", "adopt",
    "compact", "compact_tables", "count", "create_generation", "derived", "file_row", "files", "finish_file", "get_shard", "has_files",
    "progress", "recount", "record_count", "refresh_active", "remove", "seal", "roles", "shard_id", "shard_spec", "shards", "split_shard_id", "sweep",
    "underived",
    "update_file",
]
