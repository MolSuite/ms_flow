"""Where the files of a project are on other machines: one copy per host, under a folder there that
mirrors the project's, `<work directory>/projects/<uid>/<path in the project>`, so a location is
only "that host has it". A row of `ms_locations` says the file was there with that size and sha256
when it was last verified; a campaign checks again by size what is recorded, by sha256 what is not,
and sends only what is missing or not the same. Nothing over there is ever deleted from here: a
copy that went (a purged scratch) is found by `reconcile`, or by the next campaign that needs it.
"""
from __future__ import annotations

import json
import os
import re
import shlex
import uuid
from datetime import datetime
from pathlib import Path, PurePosixPath

from ms_flow.core.data.companions import whole


def _engine(project):
    return project.engine if project is not None and getattr(project, "project_dir", None) else None


def uid(project) -> str | None:
    """The name of the project's folder on other machines, made once: None for no project."""
    engine = _engine(project)
    if engine is None:
        return None
    with engine.begin() as conn:
        conn.exec_driver_sql("INSERT OR IGNORE INTO ms_project_meta (key, value) VALUES ('uid', ?)", (uuid.uuid4().hex,))
        return conn.exec_driver_sql("SELECT value FROM ms_project_meta WHERE key = 'uid'").scalar()


def host_of(remote) -> str:
    """How `ms_locations` names the place of a `RemoteExecutor`."""
    return f"{remote.remote.address or ''}:{remote.workdir}"


def root(remote, project) -> str:
    return f"{remote.workdir}/projects/{uid(project)}"


def inside(project, path) -> str | None:
    """`path` in the project, with "/", or None when it is not in the project's folder."""
    here = Path(project.project_dir).resolve()
    try:
        return PurePosixPath(Path(path).resolve().relative_to(here)).as_posix()
    except ValueError:
        return None


def known(project, host: str) -> dict[str, tuple[int, str]]:
    """`{path: (size, sha256)}` of what was verified on `host`."""
    engine = _engine(project)
    if engine is None:
        return {}
    with engine.connect() as conn:
        rows = conn.exec_driver_sql("SELECT path, size, sha256 FROM ms_locations WHERE host = ?", (host,))
        return {path: (size, sha) for path, size, sha in rows}


def record(project, host: str, verified: dict[str, tuple[int, str]]) -> None:
    """What was just verified on `host`, `{path: (size, sha256)}`. A file whose companions were
    not verified with it is not recorded: half of a container is not a copy (I14)."""
    engine = _engine(project)
    verified = {path: seen for path, seen in verified.items() if all(name in verified for name in whole(path))}
    if engine is None or not verified:
        return
    now = str(datetime.now())  # as SQLAlchemy writes a DateTime
    with engine.begin() as conn:
        for path, (size, sha) in verified.items():
            conn.exec_driver_sql(
                "INSERT INTO ms_locations (host, path, size, sha256, verified_at) VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT (host, path) DO UPDATE SET size = excluded.size, sha256 = excluded.sha256, "
                "verified_at = excluded.verified_at", (host, path, size, sha, now))


def forget(project, host: str, paths) -> None:
    """`paths` are not on `host`, and neither are their containers' other files."""
    engine = _engine(project)
    if engine is None:
        return
    with engine.begin() as conn:
        for path in {name for path in paths for name in whole(path)}:
            conn.exec_driver_sql("DELETE FROM ms_locations WHERE host = ? AND path = ?", (host, path))


def reconcile(project, remote) -> list[str]:
    """The files `ms_locations` says are on the host of `remote` that are not there any more, or
    not with their size: their rows go, with those of the rest of their container. One command
    over there, whatever their number."""
    host = host_of(remote)
    recorded = known(project, host)
    if not recorded:
        return []
    listing = "".join(f"{size} {path}\n" for path, (size, _) in recorded.items()).encode()
    said = remote.remote.run(f"cd {shlex.quote(root(remote, project))} 2>/dev/null || exec cat; "
                             # `stat -c` is GNU's, `-f %z` the BSD one (macOS).
                             "while read -r s p; do [ \"$(stat -c %s -- \"$p\" 2>/dev/null || stat -f %z -- \"$p\" 2>/dev/null)\" = \"$s\" ] || "
                             "printf '%s %s\\n' \"$s\" \"$p\"; done", listing).decode()
    gone = {name for line in said.splitlines() if line.strip() for name in whole(line.partition(" ")[2])}
    gone = sorted(gone & recorded.keys())
    forget(project, host, gone)
    return gone


# --- Containers a step made on another host (M11) ----------------------------------------------
#
# A container a campaign committed there is registered here as being there (`register_there`,
# `CatalogFile.location` = the executor's name). `bring` copies it here as far as its index there
# goes, a shard at least: the index first, then only the bytes not here yet, every new shard's crc
# checked, the index here last. Here it is sealed (only `bring` makes it grow) and readable; the
# copy there is recorded in `ms_locations`. `cleanup` removes copies there whose copy here is
# verified (the same index: shards, offsets, lengths, crcs), the rest only when asked to.


def _meta(project, key: str) -> dict:
    with _engine(project).connect() as conn:
        value = conn.exec_driver_sql("SELECT value FROM ms_project_meta WHERE key = ?", (key,)).scalar()
    return json.loads(value) if value else {}


def _save_meta(project, key: str, value: dict) -> None:
    with _engine(project).begin() as conn:
        conn.exec_driver_sql("INSERT INTO ms_project_meta (key, value) VALUES (?, ?) ON CONFLICT (key) "
                             "DO UPDATE SET value = excluded.value", (key, json.dumps(value, sort_keys=True)))


def mirror(project, location: str, there: str) -> Path:
    """Where the file at `there` on `location` is kept here."""
    return Path(project.project_dir) / "hosts" / re.sub(r"[^A-Za-z0-9._-]+", "_", location) / str(there).lstrip("/")


def _there_of(project, location: str, here: str) -> str | None:
    folder = mirror(project, location, "")
    try:
        return "/" + PurePosixPath(Path(here).relative_to(folder)).as_posix()
    except ValueError:
        return None


def register_there(project, location: str, rows) -> list[int]:
    """The containers a step committed on `location` (the rows of its catalog there, `id` among
    them), in the project's catalog as being there, with their counts from there: no data moves.
    One already brought here keeps its row; one registered before has its counts moved."""
    from ms_flow.core.data import catalog

    ids, far = [], _meta(project, f"far:{location}")
    for row in rows:
        counts = {key: value for key, value in row.items() if key in catalog._COUNTS}
        here = str(mirror(project, location, row["path"]))
        with project.get_session() as session:
            from sqlmodel import select

            from ms_flow.core.database.project_models import CatalogFile

            brought = session.exec(select(CatalogFile.id).where(CatalogFile.path == here)).first()
        if brought is not None:
            file_id = int(brought)
        else:
            file_id = catalog.add_remote(project, row["path"], location=location, role=row["role"], counts=counts,
                                         generation_id=row["generation_id"])
            catalog.update_file(project, file_id, **counts)
        if row.get("id") is not None:  # what its pairs there name it by
            far[f"{Path(row['path']).parent}|{int(row['id'])}"] = file_id
        ids.append(file_id)
    _save_meta(project, f"far:{location}", far)
    return ids


def _same(a: list, b: list) -> bool:
    keys = ("shard", "offset", "length", "crc")
    return [tuple(r[k] for k in keys) for r in a] == [tuple(r[k] for k in keys) for r in b]


def _start(here: Path, info: dict, rows: list) -> int:
    """How much of the copy here is the file there: what its own index committed, when that is a
    beginning of the index there; else (a cut copy) its head and the whole shards whose crc holds."""
    from ms_flow.core.data import container

    if not here.is_file():
        return 0
    try:
        _, mine = container.read_index(here)
        if _same(mine, rows[:len(mine)]):
            return min(here.stat().st_size, container.index_end(info, container.read_index_table(here)[1]))
    except (OSError, ValueError):
        pass
    try:
        container.read_head(here, info)
    except (OSError, ValueError):
        return 0
    at = info["head_length"]
    for row in sorted(rows, key=lambda r: r["offset"]):
        try:
            for _ in container._blocks(here, row):
                pass
        except (OSError, ValueError):
            break
        at = row["offset"] + row["length"]
    return at


def bring(project, remote, file_ids, *, location: str, piece: int = 64 << 20) -> list[int]:
    """The containers `file_ids` of `location` (the host of `remote`) brought here as far as their
    index there goes, in pieces of at most `piece` bytes; a cut copy goes on from what came. A
    pair's references to its other part (`meta["bulk"]`, ids of the catalog there) become this
    project's, so bring both parts. Returns the ids brought."""
    import hashlib

    from ms_flow.core.data import catalog, container
    from ms_flow.core.database.project_models import CatalogFile
    from ms_env import HostError

    far, done = _meta(project, f"far:{location}"), []
    for file_id in file_ids:
        with project.get_session() as session:
            row = session.get(CatalogFile, int(file_id))
        if row is None:
            continue
        there = row.path if row.location == location else _there_of(project, location, row.path) if not row.location else None
        if there is None:
            continue
        here = mirror(project, location, there)
        said = remote.remote.run(f"cat {shlex.quote(there + '.index')}")
        info, table = container.parse_index(said, there)
        rows, end = table.to_pylist(), container.index_end(info, table)
        here.parent.mkdir(parents=True, exist_ok=True)
        at = _start(here, info, rows)
        with open(here, "r+b" if here.exists() else "wb") as file:
            file.truncate(at)
        while at < end:
            n = min(int(piece), end - at)
            data = remote.remote.run(f"tail -c +{at + 1} {shlex.quote(there)} | head -c {n}")
            if len(data) != n:
                raise HostError(f"{there}: {len(data)} of {n} bytes came")
            with open(here, "ab") as file:
                file.write(data)
                file.flush()
                os.fsync(file.fileno())
            at += n
        container.read_head(here, info)
        for shard in rows:  # what came is what was committed there (I16)
            for _ in container._blocks(here, shard):
                pass
        folder = str(Path(there).parent)
        for shard in rows:
            meta = json.loads(shard["meta"] or "{}")
            if meta.get("bulk"):
                meta["bulk"] = [catalog.shard_id(far[f"{folder}|{f}"], n)
                                for f, n in map(catalog.split_shard_id, meta["bulk"])]
                shard["meta"] = json.dumps(meta, separators=(",", ":"))
        container._save_index(here, info, rows)  # the index last: it never names bytes not here
        if row.location:
            catalog.adopt(project, int(file_id), here)
            catalog.seal(project, int(file_id))  # only `bring` makes it grow
        else:
            catalog.update_file(project, int(file_id), **catalog._counts(project, here))
        sha = hashlib.sha256(said).hexdigest()
        record(project, host_of(remote), {there: (end, sha), there + ".index": (len(said), sha)})
        done.append(int(file_id))
    return done


def copies(project) -> dict[str, dict[str, int]]:
    """By the name of the executor that has them, how many containers of the project are only there
    (`there`) and how many are here with that copy too (`here_too`, brought by `bring`)."""
    from sqlmodel import select

    from ms_flow.core.database.project_models import CatalogFile

    with project.get_session() as session:
        rows = session.exec(select(CatalogFile.path, CatalogFile.location)).all()
    hosts, said = Path(project.project_dir) / "hosts", {}
    for path, location in rows:
        if location:
            said.setdefault(location, {"there": 0, "here_too": 0})["there"] += 1
        elif str(path).startswith(f"{hosts}/"):
            name = Path(path).relative_to(hosts).parts[0]
            said.setdefault(name, {"there": 0, "here_too": 0})["here_too"] += 1
    return said


def cleanup(project, remote, *, location: str, files=None, confirm: bool = False, dry: bool = False) -> list[dict]:
    """The copies of `location` (the host of `remote`) of this project's containers, or of `files`
    (catalog ids): each `{path, verified, removed}`. One whose copy here has the same index is
    removed there; one with no verified copy here only with `confirm`, and then, if it was only
    there, its catalog row goes too. `dry`: what would go, nothing removed."""
    from sqlmodel import select

    from ms_flow.core.data import container
    from ms_flow.core.database.project_models import CatalogFile

    host, wanted = host_of(remote), None if files is None else {int(f) for f in files}
    with project.get_session() as session:
        rows = session.exec(select(CatalogFile)).all()
    candidates = []
    for row in rows:
        if wanted is not None and int(row.id) not in wanted:
            continue
        if row.location == location:
            candidates.append((row, row.path, False))
        elif not row.location:
            there = _there_of(project, location, row.path)
            if there is not None and there in known(project, host):
                candidates.append((row, there, True))
    said = []
    for row, there, here_too in candidates:
        verified = False
        if here_too:
            try:
                _, theirs = container.parse_index(remote.remote.run(f"cat {shlex.quote(there + '.index')}"), there)
                verified = _same(container.read_index(row.path)[1], theirs.to_pylist())
            except (OSError, ValueError):
                verified = False
        removed = (verified or confirm) and not dry
        if removed:
            remote.remote.run(f"rm -f {shlex.quote(there)} {shlex.quote(there + '.index')}")
            forget(project, host, [there])
            if not here_too:
                with project.get_session() as session:
                    session.delete(session.get(CatalogFile, row.id))
                    session.commit()
        said.append({"path": there, "verified": verified, "removed": removed})
    return said


__all__ = ["bring", "cleanup", "copies", "forget", "host_of", "inside", "known", "mirror", "reconcile", "record",
           "register_there", "root", "uid"]
