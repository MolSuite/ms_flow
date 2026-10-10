"""Steps that test a configured worker: can its hosts be reached, and do they answer.

* The app process must never block on a remote host. Every probe is a socket or
  a subprocess with an explicit timeout.
* ``ray.init`` is NOT a reachability test: against a dead GCS it retries for
  ~60s, so reachability is checked with a plain TCP connect first.
* No credential is handled: remote commands go through ``ms_env.Target``, the
  system ``ssh`` with ``BatchMode=yes``.

ponytail: what is left of the provisioning of the engine before this one. It tests
workers, not environments (those are ``ms_env``'s), and goes when the workers editor
is rebuilt on the executors of ``ms_flow.engine``.
"""
from __future__ import annotations

import shlex
import socket
from typing import Any

from ms_env import Step, StepFailed, Target

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


def _ssh_argv(host: str, remote_command: str, *, user: str = "") -> list[str]:
    # A test must answer soon, and of a host nobody logged in to before: the two options `Target` leaves out.
    ssh = ("ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=accept-new")
    return Target(f"{user}@{host}" if user else host, ssh=ssh).argv(remote_command)


def in_env(command: str, env_name: str = "") -> str:
    """Wrap a command so it runs inside ``env_name`` (no-op without an env)."""
    env = str(env_name or "").strip()
    if not env:
        return command
    return f"conda run --no-capture-output -n {shlex.quote(env)} {command}"


def _get(worker: Any, key: str, default: Any = None) -> Any:
    value = getattr(worker, key, default)
    return default if value is None else value


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
                    argv=_ssh_argv(host, in_env("ray --version", env), user=user),
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
        steps.append(Step(f"ssh {host}", argv=_ssh_argv(host, "echo ok", user=user), timeout=20))
        steps.append(Step(f"{scheduler} available", argv=_ssh_argv(host, probe, user=user), timeout=20))
        if env:
            steps.append(
                Step(
                    f"conda env {env}",
                    argv=_ssh_argv(host, in_env("python -V", env), user=user),
                    timeout=60,
                )
            )
        return steps

    return [Step("local executor", call=lambda: "Local executor: nothing to test.")]


def _ok_or_fail(error: str, ok_message: str) -> str:
    if error:
        raise StepFailed(error)
    return ok_message


def _fail(message: str) -> str:
    raise StepFailed(message)


__all__ = ["split_address", "test_steps"]
