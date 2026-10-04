"""External tools: find one that already works, or install it -- driven by a TOML file.

The app ships one TOML per tool: where to look for it, how to tell it works, how to
install it. MF owns the *where* -- this machine, or every host of a ray/HPC worker
over ssh -- and the *when*: an install runs on a daemon thread that belongs to the
app, not to a project (none has to be open), and jobs wait for it by listing the id
``install_tool`` returns in their ``depends_on``.

Lookup on a target, first candidate whose check exits 0 wins: paths the user
registered, the managed install under ``tools_home``, then ``PATH``.

TOML layout (every section but ``[tool]`` and ``[locate]`` is optional)::

    [tool]
    name = "openbabel"
    label = "OpenBabel"
    version = "3.1.1"                       # pinned here; never a user option
    prefix = "{tools_home}/{name}/{version}"  # the default

    [locate]
    paths = ["{prefix}/bin/obabel"]         # where the install puts it
    commands = ["obabel"]                   # looked up on PATH
    check = ["{exe}", "-V"]                 # {exe} = the candidate

    [env]                                   # a conda prefix at {prefix}
    conda = ["openbabel={version}"]
    pip = []

    [[install]]                             # plain steps, after [env]
    name = "download the model"
    argv = ["curl", "-fsSL", "-o", "{prefix}/model.pt", "https://..."]
    timeout = 1800

Every command runs through ``sh`` on the target, so targets are POSIX hosts.
"""
from __future__ import annotations

import shlex
import subprocess
import threading
import tomllib
from concurrent.futures import Future
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from ms_flow.core.executor.provisioning import (
    Step,
    StepFailed,
    _get,
    _worker_hosts,
    run_step,
    ssh_argv,
)

READY = "ready"
MISSING = "missing"
UNREACHABLE = "unreachable"
_SSH_CONNECT_FAILED = 255  # ssh's own exit code; anything else came from the remote command
_DEPENDENCY_PREFIX = "tool:"


# ----------------------------------------------------------------------------
# Spec (the TOML, still templated) -> Recipe (concrete for one target)
# ----------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class RecipeStep:
    name: str
    argv: tuple[str, ...]
    timeout: float = 1800.0


@dataclass(frozen=True, slots=True)
class Recipe:
    name: str
    prefix: str
    candidates: tuple[str, ...]  # executables tried first, in order
    commands: tuple[str, ...]  # then these names on PATH
    check: tuple[str, ...]  # "{exe}" marks the candidate
    steps: tuple[RecipeStep, ...]


@dataclass(frozen=True, slots=True)
class ToolSpec:
    name: str
    version: str
    info: dict[str, Any]  # the whole [tool] table: label, purpose, footprint...
    prefix: str
    paths: tuple[str, ...]
    commands: tuple[str, ...]
    check: tuple[str, ...]
    conda: tuple[str, ...] = ()
    pip: tuple[str, ...] = ()
    channels: tuple[str, ...] = ("conda-forge",)
    steps: tuple[RecipeStep, ...] = ()

    @classmethod
    def load(cls, path: Path | str) -> "ToolSpec":
        data = tomllib.loads(Path(path).read_text(encoding="utf-8"))
        tool, locate, env = data["tool"], data["locate"], data.get("env", {})
        return cls(
            name=str(tool["name"]),
            version=str(tool.get("version", "")),
            info=dict(tool),
            prefix=str(tool.get("prefix", "{tools_home}/{name}/{version}")),
            paths=tuple(locate.get("paths", ())),
            commands=tuple(locate.get("commands", ())),
            check=tuple(locate.get("check", ())),
            conda=tuple(env.get("conda", ())),
            pip=tuple(env.get("pip", ())),
            channels=tuple(env.get("channels", ("conda-forge",))),
            steps=tuple(
                RecipeStep(str(step["name"]), tuple(step["argv"]), float(step.get("timeout", 1800)))
                for step in data.get("install", ())
            ),
        )

    def recipe(
        self,
        tools_home: str,
        *,
        prefix: str = "",
        extra_paths: Iterable[str] = (),
        manager: str = "",
    ) -> Recipe:
        """Bind the templates to one target. ``tools_home``/``prefix`` are paths *on that target*.

        ``prefix`` is a user-registered install location (looked up and installed into instead
        of the managed one); ``extra_paths`` are user-registered executables; ``manager`` is a
        conda/mamba/micromamba known to exist there (otherwise one is searched, then bootstrapped).
        """
        values = {"tools_home": str(tools_home).rstrip("/"), "name": self.name, "version": self.version, "exe": "{exe}"}
        values["prefix"] = str(prefix or self.prefix.format_map(values)).rstrip("/")
        fill = lambda items: tuple(str(item).format_map(values) for item in items)  # noqa: E731
        steps = _env_steps(self.name, fill(self.conda), fill(self.pip), fill(self.channels), values, manager)
        steps += [RecipeStep(step.name, fill(step.argv), step.timeout) for step in self.steps]
        return Recipe(
            name=self.name,
            prefix=values["prefix"],
            candidates=(*map(str, extra_paths), *fill(self.paths)),
            commands=self.commands,
            check=fill(self.check),
            steps=tuple(steps),
        )


def _env_steps(
    name: str, conda: tuple[str, ...], pip: tuple[str, ...], channels: tuple[str, ...], values: dict, manager: str
) -> list[RecipeStep]:
    if not conda and not pip:
        return []
    prefix = values["prefix"]
    channel_args = [part for channel in channels for part in ("--channel", channel)]
    create = shlex.join(["create", "--yes", "--quiet", "--prefix", prefix, *channel_args, *conda])
    steps = [
        RecipeStep(
            f"create the {name} environment",
            ("sh", "-c", f'{_find_or_bootstrap_manager(manager, values["tools_home"])}\n"$M" {create}'),
        )
    ]
    if pip:
        steps.append(
            RecipeStep(
                f"pip install into {name}",
                (f"{prefix}/bin/python", "-m", "pip", "install", "--progress-bar", "off", *pip),
                timeout=3600.0,
            )
        )
    return steps


def _find_or_bootstrap_manager(hint: str, tools_home: str) -> str:
    """sh that leaves a conda-type manager in $M: the hint, PATH, or a micromamba it downloads."""
    root = shlex.quote(f"{tools_home}/micromamba")
    return f"""M={shlex.quote(str(hint or ""))}
[ -x "$M" ] || M=$(command -v micromamba || command -v mamba || command -v conda) || {{
  case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) p=linux-64 ;; Linux-aarch64) p=linux-aarch64 ;;
    Darwin-arm64) p=osx-arm64 ;; Darwin-x86_64) p=osx-64 ;;
    *) echo "no conda/mamba/micromamba here and no micromamba build for $(uname -sm)" >&2; exit 1 ;;
  esac
  M={root}/bin/micromamba
  if [ ! -x "$M" ]; then
    echo "no environment manager found: installing micromamba into {root}"
    mkdir -p {root} && curl -fsSL "https://micro.mamba.pm/api/micromamba/$p/latest" | tar -xj -C {root} bin/micromamba || exit 1
  fi
  export MAMBA_ROOT_PREFIX={root}
}}"""


# ----------------------------------------------------------------------------
# Targets: "" is this machine; a worker config is each of its remote hosts
# ----------------------------------------------------------------------------

def worker_targets(worker: Any = None) -> list[tuple[str, str]]:
    """(host, ssh user) pairs to act on. No worker, or one without remote hosts -> local."""
    if worker is None:
        return [("", "")]
    user = str(_get(worker, "ssh_user", "")).strip() or str(_get(worker, "user", "")).strip()
    return [(host, user) for host in _worker_hosts(worker)] or [("", "")]


def _argv_on(argv: Iterable[str], host: str, user: str) -> list[str]:
    return ssh_argv(host, shlex.join(argv), user=user) if host else list(argv)


def recipe_steps(recipe: Recipe, host: str = "", user: str = "") -> list[Step]:
    label = f"{host}: " if host else ""
    return [
        Step(label + step.name, argv=_argv_on(step.argv, host, user), timeout=step.timeout)
        for step in recipe.steps
    ]


def _locate_script(recipe: Recipe) -> str:
    check = " ".join('"$1"' if part == "{exe}" else shlex.quote(part) for part in recipe.check) or "true"
    return (
        f'ok() {{ [ -x "$1" ] && {check} >/dev/null 2>&1; }}\n'
        f"for p in {' '.join(map(shlex.quote, recipe.candidates))}; do"
        ' ok "$p" && { echo "$p"; exit 0; }; done\n'
        f"for c in {' '.join(map(shlex.quote, recipe.commands))}; do"
        ' p=$(command -v "$c") && ok "$p" && { echo "$p"; exit 0; }; done\n'
        "exit 1"
    )


def locate(recipe: Recipe, host: str = "", user: str = "", timeout: float = 60.0) -> tuple[str, str]:
    """(READY, executable) or (MISSING | UNREACHABLE, ""). Blocks on a subprocess: keep it off UI threads."""
    try:
        result = subprocess.run(
            _argv_on(("sh", "-c", _locate_script(recipe)), host, user),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return (UNREACHABLE if host else MISSING), ""
    found = result.stdout.strip().splitlines()
    if result.returncode == 0 and found:
        return READY, found[-1]
    return (UNREACHABLE if host and result.returncode == _SSH_CONNECT_FAILED else MISSING), ""


def tool_status(recipe: Recipe, worker: Any = None) -> str:
    """Worst state across the worker's hosts: a tool is ready only when every host has it."""
    states = {locate(recipe, host, user)[0] for host, user in worker_targets(worker)}
    if UNREACHABLE in states:
        return UNREACHABLE
    return READY if states == {READY} else MISSING


# ----------------------------------------------------------------------------
# Installs: app-level background threads that jobs can depend on
# ----------------------------------------------------------------------------

@dataclass
class Install:
    key: str
    future: Future = field(default_factory=Future)
    lines: list[str] = field(default_factory=list)  # live log, appended from the install thread


_INSTALLS: dict[str, Install] = {}
_LOCK = threading.Lock()


def tool_dependency(recipe: Recipe, worker: Any = None) -> str:
    """The ``depends_on`` id for this tool on this target."""
    target = str(_get(worker, "name", "")).strip() if worker is not None else ""
    return f"{_DEPENDENCY_PREFIX}{recipe.name}@{target or 'local'}"


def install_tool(recipe: Recipe, worker: Any = None) -> str:
    """Start installing (or join the install already running). Returns the ``depends_on`` id.

    Hosts where the tool is already usable are skipped, so this is also "make sure it's there".
    """
    key = tool_dependency(recipe, worker)
    with _LOCK:
        current = _INSTALLS.get(key)
        if current is not None and not current.future.done():
            return key
        install = _INSTALLS[key] = Install(key)
    # ponytail: one daemon thread per install, hosts one after another; a pool if installs pile up.
    # Daemon on purpose: quitting the app must not wait 30 min for a conda solve.
    threading.Thread(
        target=_run_install, args=(install, recipe, worker_targets(worker)), name=f"molsuite-{key}", daemon=True
    ).start()
    return key


def _run_install(install: Install, recipe: Recipe, targets: list[tuple[str, str]]) -> None:
    install.future.set_running_or_notify_cancel()
    try:
        for host, user in targets:
            where = host or "this machine"
            if locate(recipe, host, user)[0] == READY:
                install.lines.append(f"{recipe.name} is already usable on {where}.")
                continue
            for step in recipe_steps(recipe, host, user):
                run_step(step, install.lines.append)
            state, _exe = locate(recipe, host, user)
            if state != READY:
                raise StepFailed(f"{recipe.name}: every step ran but the tool is still {state} on {where}.")
    except Exception as exc:  # noqa: BLE001 -- whatever broke, dependents must see a failure
        install.lines.append(str(exc))
        install.future.set_exception(exc)
    else:
        install.future.set_result(None)


def get_install(key: str) -> Install | None:
    """The latest install for a ``depends_on`` id: its future and live log, for the UI."""
    return _INSTALLS.get(key)


def dependency_state(dependency_id: str) -> str | None:
    """Feeder hook. None: not a tool dependency. Otherwise "completed", "failed" or "running".

    No install known (e.g. after a restart) counts as completed: the app checked the tool
    when it submitted the job, and a tool that went missing fails the job loudly anyway.
    """
    if not str(dependency_id).startswith(_DEPENDENCY_PREFIX):
        return None
    install = _INSTALLS.get(str(dependency_id))
    if install is None:
        return "completed"
    if not install.future.done():
        return "running"
    return "failed" if install.future.exception() is not None else "completed"


__all__ = [
    "Install",
    "MISSING",
    "READY",
    "Recipe",
    "RecipeStep",
    "ToolSpec",
    "UNREACHABLE",
    "dependency_state",
    "get_install",
    "install_tool",
    "locate",
    "recipe_steps",
    "tool_dependency",
    "tool_status",
    "worker_targets",
]
