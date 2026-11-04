"""Where the files of a project are on other machines: one copy per host, under a folder there that
mirrors the project's, `<work directory>/projects/<uid>/<path in the project>`, so a location is
only "that host has it". A row of `ms_locations` says the file was there with that size and sha256
when it was last verified; a campaign checks again by size what is recorded, by sha256 what is not,
and sends only what is missing or not the same. Nothing over there is ever deleted from here: a
copy that went (a purged scratch) is found by `reconcile`, or by the next campaign that needs it.
"""
from __future__ import annotations

import shlex
import uuid
from datetime import datetime
from pathlib import Path, PurePosixPath


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
    """What was just verified on `host`, `{path: (size, sha256)}`."""
    engine = _engine(project)
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
    engine = _engine(project)
    if engine is None:
        return
    with engine.begin() as conn:
        for path in paths:
            conn.exec_driver_sql("DELETE FROM ms_locations WHERE host = ? AND path = ?", (host, path))


def reconcile(project, remote) -> list[str]:
    """The files `ms_locations` says are on the host of `remote` that are not there any more, or
    not with their size: their rows go. One command over there, whatever their number."""
    host = host_of(remote)
    recorded = known(project, host)
    if not recorded:
        return []
    listing = "".join(f"{size} {path}\n" for path, (size, _) in recorded.items()).encode()
    said = remote.remote.run(f"cd {shlex.quote(root(remote, project))} 2>/dev/null || exec cat; "
                             "while read -r s p; do [ \"$(stat -c %s -- \"$p\" 2>/dev/null)\" = \"$s\" ] || "
                             "printf '%s %s\\n' \"$s\" \"$p\"; done", listing).decode()
    gone = [line.partition(" ")[2] for line in said.splitlines() if line.strip()]
    forget(project, host, gone)
    return gone


__all__ = ["forget", "host_of", "inside", "known", "reconcile", "record", "root", "uid"]
