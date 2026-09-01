"""Reachability checks and provisioning for networked executors (ray / HPC).

Three rules shaped this module:

* The app process must never block on a remote host. Every probe is a socket or
  a subprocess with an explicit timeout.
* ``ray.init`` is NOT a reachability test: against a dead GCS it retries for
  ~60s. Doing that at project open froze the UI, so reachability is checked
  with a plain TCP connect first.
* We do not manage SSH credentials. Remote commands go through the system
  ``ssh`` with ``BatchMode=yes``: the OS/agent authenticates or the step fails
  immediately with a clear message. No keys, no passwords, no agent handling.

Everything here is plain stdlib and Qt-free; the UI only renders ``Step`` lists.
"""
from __future__ import annotations

import shlex
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

DEFAULT_TIMEOUT = 30.0
CONNECT_TIMEOUT = 2.0
RAY_GCS_PORT = 6379

# Command used to prove a scheduler answers on the remote side.
_SCHEDULER_PROBE = {
    "slurm": "sinfo --version",
    "pbs": "qstat --version",
    "torque": "qstat --version",
    "sge": "qstat -help",
    "lsf": "lsid",
}


class StepFailed(RuntimeError):
    pass


@dataclass(slots=True)
class Step:
    """One named unit of work: either a command line or an in-process call."""

    name: str
    argv: Optional[list[str]] = None
    call: Optional[Callable[[], str]] = None
    timeout: float = DEFAULT_TIMEOUT


@dataclass(slots=True)
class ProbeResult:
    ok: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)


# ----------------------------------------------------------------------------
# Address helpers
# ----------------------------------------------------------------------------

def split_address(address: str | None, *, default_port: int = RAY_GCS_PORT) -> tuple[str, int]:
    """``ray://h:p`` / ``h:p`` / ``h`` -> (host, port). Empty address -> ("", 0)."""
    text = str(address or "").strip()
    if not text:
        return "", 0
    for prefix in ("ray://", "tcp://", "http://", "https://"):
        if text.startswith(prefix):
            text = text[len(prefix):]
            break
    text = text.rstrip("/")
    if ":" in text:
        host, _, port = text.rpartition(":")
        try:
            return host.strip(), int(port)
        except ValueError:
            return text, default_port
    return text, default_port


def probe_tcp(host: str, port: int, timeout: float = CONNECT_TIMEOUT) -> str:
    """Return "" when the port accepts a connection, else a short error text."""
    if not host or port <= 0:
        return "No address configured."
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return ""
    except OSError as exc:
        return f"{host}:{port} unreachable ({exc.strerror or exc})"


def ray_preflight(address: str | None, timeout: float = CONNECT_TIMEOUT) -> str:
    """Guard for ``ray.init(address=...)``. "" means "go ahead"."""
    host, port = split_address(address)
    if not host:
        return ""
    return probe_tcp(host, port, timeout=timeout)


# ----------------------------------------------------------------------------
# Remote command construction (no credential handling — BatchMode only)
# ----------------------------------------------------------------------------

def ssh_argv(host: str, remote_command: str, *, user: str = "", connect_timeout: int = 5) -> list[str]:
    target = f"{user}@{host}" if user else host
    return [
        "ssh",
        "-o", "BatchMode=yes",              # never prompt: keys must already work
        "-o", f"ConnectTimeout={connect_timeout}",
        "-o", "StrictHostKeyChecking=accept-new",
        target,
        remote_command,
    ]


def conda_bootstrap_command(env_name: str, python_version: str = "") -> str:
    """Idempotent 'create the env if missing'. Empty env name -> no command."""
    env = str(env_name or "").strip()
    if not env:
        return ""
    version = str(python_version or "").strip() or f"{sys.version_info.major}.{sys.version_info.minor}"
    quoted = shlex.quote(env)
    # `conda env list` is the only listing that works for both named and path envs.
    return (
        f"conda env list | awk '{{print $1}}' | grep -qx {quoted} "
        f"|| conda create -y -n {quoted} python={version}"
    )


def in_env(command: str, env_name: str = "") -> str:
    """Wrap a command so it runs inside ``env_name`` (no-op without an env)."""
    env = str(env_name or "").strip()
    if not env:
        return command
    return f"conda run --no-capture-output -n {shlex.quote(env)} {command}"


# ----------------------------------------------------------------------------
# Worker introspection
# ----------------------------------------------------------------------------

def _get(worker: Any, key: str, default: Any = None) -> Any:
    value = getattr(worker, key, default)
    return default if value is None else value


def _worker_hosts(worker: Any) -> list[str]:
    """Remote hosts owned by this worker config (head only when it is remote)."""
    wtype = str(_get(worker, "type", "")).lower()
    if wtype == "hpc":
        host = str(_get(worker, "host", "")).strip()
        return [host] if host else []
    hosts = [str(ip).strip() for ip in _get(worker, "worker_ips", []) or [] if str(ip).strip()]
    head = str(_get(worker, "head_ip", "")).strip()
    if head and head not in hosts and str(_get(worker, "mode", "")).lower() != "local":
        hosts.insert(0, head)
    return hosts


def cluster_launch_needed(worker: Any) -> bool:
    """True when this ray worker owns remote nodes we must start ourselves."""
    return (
        str(_get(worker, "type", "")).lower() == "ray"
        and str(_get(worker, "mode", "")).lower() == "managed"
    )


# ----------------------------------------------------------------------------
# Steps: test / prepare / cluster up-down
# ----------------------------------------------------------------------------

def test_steps(worker: Any) -> list[Step]:
    wtype = str(_get(worker, "type", "")).lower()
    user = str(_get(worker, "ssh_user", "")).strip()
    env = str(_get(worker, "conda_env", "")).strip()
    steps: list[Step] = []

    if wtype == "ray":
        mode = str(_get(worker, "mode", "external")).lower()
        if mode != "local":
            address = str(_get(worker, "address", "")).strip()
            host, port = split_address(address)
            steps.append(
                Step(
                    f"TCP {host}:{port}",
                    call=lambda h=host, p=port: _ok_or_fail(probe_tcp(h, p), f"{h}:{p} reachable"),
                    timeout=CONNECT_TIMEOUT + 1,
                )
            )
            steps.append(Step("ray status", argv=["ray", "status", f"--address={host}:{port}"], timeout=20))
        else:
            steps.append(Step("ray installed", argv=["ray", "--version"], timeout=20))
        for host in [str(ip).strip() for ip in _get(worker, "worker_ips", []) or [] if str(ip).strip()]:
            steps.append(
                Step(
                    f"ssh {host}",
                    argv=ssh_argv(host, in_env("ray --version", env), user=user),
                    timeout=20,
                )
            )
        return steps

    if wtype == "hpc":
        host = str(_get(worker, "host", "")).strip()
        if not host:
            return [Step("host", call=lambda: _fail("No host configured for this HPC worker."))]
        user = str(_get(worker, "user", "")).strip() or user
        scheduler = str(_get(worker, "scheduler", "slurm")).lower()
        probe = _SCHEDULER_PROBE.get(scheduler, "true")
        steps.append(Step(f"ssh {host}", argv=ssh_argv(host, "echo ok", user=user), timeout=20))
        steps.append(Step(f"{scheduler} available", argv=ssh_argv(host, probe, user=user), timeout=20))
        if env:
            steps.append(
                Step(
                    f"conda env {env}",
                    argv=ssh_argv(host, in_env("python -V", env), user=user),
                    timeout=60,
                )
            )
        return steps

    return [Step("local executor", call=lambda: "Local executor: nothing to test.")]


def prepare_steps(worker: Any) -> list[Step]:
    """Ensure the conda env exists and run the user's setup commands on each host.

    Returns [] when there is nothing remote to prepare (local or externally
    managed executors) — the caller should say so instead of running nothing.
    """
    hosts = _worker_hosts(worker)
    if not hosts:
        return []
    user = str(_get(worker, "ssh_user", "")).strip() or str(_get(worker, "user", "")).strip()
    env = str(_get(worker, "conda_env", "")).strip()
    bootstrap = conda_bootstrap_command(env, str(_get(worker, "python_version", "")))
    commands = [str(c).strip() for c in _get(worker, "setup_commands", []) or [] if str(c).strip()]
    steps: list[Step] = []
    for host in hosts:
        if bootstrap:
            steps.append(
                Step(f"{host}: conda env {env}", argv=ssh_argv(host, bootstrap, user=user), timeout=900)
            )
        for command in commands:
            steps.append(
                Step(
                    f"{host}: {command[:48]}",
                    argv=ssh_argv(host, in_env(command, env), user=user),
                    timeout=1800,
                )
            )
    return steps


# ----------------------------------------------------------------------------
# Ray on-prem cluster launcher (`ray up`) — we generate the YAML, ray does the work
# ----------------------------------------------------------------------------

def cluster_config_path(name: str, root: Path | None = None) -> Path:
    base = Path(root) if root is not None else Path.home() / ".molsuite" / "clusters"
    return base / f"{name}.yaml"


def ray_cluster_yaml(worker: Any) -> str:
    """Cluster config for ray's on-prem ("local") provider.

    Only the fields ray needs; anything fancier belongs in a hand-written YAML
    the user points ray at directly.
    """
    name = str(_get(worker, "name", "cluster")).strip() or "cluster"
    head_ip = str(_get(worker, "head_ip", "")).strip() or _local_ip()
    worker_ips = [str(ip).strip() for ip in _get(worker, "worker_ips", []) or [] if str(ip).strip()]
    user = str(_get(worker, "ssh_user", "")).strip()
    env = str(_get(worker, "conda_env", "")).strip()
    bootstrap = conda_bootstrap_command(env, str(_get(worker, "python_version", "")))
    setup: list[str] = [bootstrap] if bootstrap else []  # creating the env can't run inside it
    setup += [in_env(c, env) for c in (str(c).strip() for c in _get(worker, "setup_commands", []) or []) if c]
    head_cpus = max(1, int(_get(worker, "cpus", 1) or 1))
    head_gpus = max(0, int(_get(worker, "gpus", 0) or 0))

    lines = [
        f"cluster_name: {name}",
        f"max_workers: {len(worker_ips)}",
        "provider:",
        "  type: local",
        f"  head_ip: {head_ip}",
        "  worker_ips: [" + ", ".join(worker_ips) + "]",
        "auth:",
        f"  ssh_user: {user or '$USER'}",
        "setup_commands:",
        *[f"  - {_yaml_scalar(command)}" for command in setup],
        "head_start_ray_commands:",
        f"  - {_yaml_scalar(in_env('ray stop', env))}",
        "  - " + _yaml_scalar(
            in_env(
                f"ray start --head --port={RAY_GCS_PORT} --num-cpus={head_cpus} "
                f"--num-gpus={head_gpus} --autoscaling-config=~/ray_bootstrap_config.yaml",
                env,
            )
        ),
        "worker_start_ray_commands:",
        f"  - {_yaml_scalar(in_env('ray stop', env))}",
        "  - " + _yaml_scalar(in_env(f"ray start --address=$RAY_HEAD_IP:{RAY_GCS_PORT}", env)),
        "",
    ]
    return "\n".join(lines)


def write_cluster_config(worker: Any, root: Path | None = None) -> Path:
    name = str(_get(worker, "name", "cluster")).strip() or "cluster"
    path = cluster_config_path(name, root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(ray_cluster_yaml(worker), encoding="utf-8")
    return path


def cluster_up_steps(worker: Any, root: Path | None = None) -> list[Step]:
    name = str(_get(worker, "name", "cluster")).strip() or "cluster"
    path = cluster_config_path(name, root)
    return [
        Step("write cluster config", call=lambda: f"Wrote {write_cluster_config(worker, root)}"),
        Step("ray up", argv=["ray", "up", "-y", "--no-config-cache", str(path)], timeout=3600),
    ]


def cluster_down_steps(worker: Any, root: Path | None = None) -> list[Step]:
    name = str(_get(worker, "name", "cluster")).strip() or "cluster"
    return [
        Step("ray down", argv=["ray", "down", "-y", str(cluster_config_path(name, root))], timeout=1800),
    ]


def cluster_address(worker: Any) -> str:
    head_ip = str(_get(worker, "head_ip", "")).strip() or _local_ip()
    return f"{head_ip}:{RAY_GCS_PORT}"


# ----------------------------------------------------------------------------
# Runner
# ----------------------------------------------------------------------------

def run_step(step: Step, on_line: Callable[[str], None] | None = None) -> None:
    """Run one step, streaming its output. Raises StepFailed on failure."""
    emit = on_line or (lambda _line: None)
    if step.call is not None:
        message = step.call()
        if message:
            emit(message)
        return
    if not step.argv:
        return
    emit("$ " + " ".join(step.argv))
    try:
        process = subprocess.Popen(
            step.argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
    except FileNotFoundError as exc:
        raise StepFailed(f"{step.name}: command not found ({exc.filename}).") from exc
    deadline = time.monotonic() + step.timeout
    assert process.stdout is not None
    for line in process.stdout:
        emit(line.rstrip())
        if time.monotonic() > deadline:
            process.kill()
            raise StepFailed(f"{step.name}: timed out after {step.timeout:.0f}s.")
    try:
        code = process.wait(timeout=max(1.0, deadline - time.monotonic()))
    except subprocess.TimeoutExpired:
        process.kill()
        raise StepFailed(f"{step.name}: timed out after {step.timeout:.0f}s.") from None
    if code != 0:
        raise StepFailed(f"{step.name}: exited with code {code}.")


def run_steps(
    steps: Iterable[Step],
    on_line: Callable[[str], None] | None = None,
    on_step: Callable[[int, Step], None] | None = None,
) -> None:
    for index, step in enumerate(steps):
        if on_step is not None:
            on_step(index, step)
        run_step(step, on_line)


# ----------------------------------------------------------------------------

def _ok_or_fail(error: str, ok_message: str) -> str:
    if error:
        raise StepFailed(error)
    return ok_message


def _fail(message: str) -> str:
    raise StepFailed(message)


def _local_ip() -> str:
    """Outbound-route IP, which is what ray reports for the head node."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))  # no packet is sent; just picks the route
            return str(sock.getsockname()[0])
    except OSError:
        return "127.0.0.1"


def _yaml_scalar(text: str) -> str:
    return '"' + str(text).replace("\\", "\\\\").replace('"', '\\"') + '"'


__all__ = [
    "CONNECT_TIMEOUT",
    "ProbeResult",
    "RAY_GCS_PORT",
    "Step",
    "StepFailed",
    "cluster_address",
    "cluster_config_path",
    "cluster_down_steps",
    "cluster_launch_needed",
    "cluster_up_steps",
    "conda_bootstrap_command",
    "in_env",
    "prepare_steps",
    "probe_tcp",
    "ray_cluster_yaml",
    "ray_preflight",
    "run_step",
    "run_steps",
    "split_address",
    "ssh_argv",
    "test_steps",
    "write_cluster_config",
]
