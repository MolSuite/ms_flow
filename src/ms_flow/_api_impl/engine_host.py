"""The engine in a process of its own, which lives and dies with the application's.

`Workspace` with `engine_host="process"` keeps here, in a child process started at the engine's
first use, a `Workspace` of the same app that runs the engine, the project's writers and the
executors. The application's process keeps the projects, the settings and what reads the data;
what it asks of the engine travels through a pipe (`EngineProxy`). The child is no service: no
port, nothing left behind. It ends when the pipe closes, which is when the application closes or
dies (on Linux the kernel also kills it then: `PR_SET_PDEATHSIG`).

A child that dies leaves the application standing: what waits for it raises `EngineGone`, and
the next use of the engine starts another one, whose journal continues the jobs (`resume`).

What travels is references and values, with pickle: a job goes by its name (its function has
to be importable, as `resume` needs it anyway) and its params as they are; what it reads of the
project, the engine's process reads there. A callable the other side calls back goes as a
reference to it: `CALLBACKS` names those of the calls to the engine, and the arguments of a call
back are all of that kind.
"""
from __future__ import annotations

import itertools
import multiprocessing
import os
import pickle
import socket
import sys
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor

#: Arguments of a call to the engine that stay in the application: the engine calls them back.
CALLBACKS = frozenset({"on_terminal", "on_line"})


class EngineGone(RuntimeError):
    """The engine's process ended while something waited for it."""


_side = "app"  # "engine" in the child
_kept: dict[int, object] = {}  # what the other side may call back, by token
_tokens = itertools.count()
_peer = None  # this process's end of the pipe


def _kept_or_stub(side: str, token: int):
    return _kept[token] if side == _side else _Stub(side, token)


class _Stub:
    """A callable of the other process: calling it calls that one, and waits for what it returns."""

    def __init__(self, side: str, token: int):
        self.side, self.token = side, token

    def __call__(self, *args, **kwargs):
        args = tuple(callback(arg) if callable(arg) else arg for arg in args)
        kwargs = {name: callback(value) if callable(value) else value for name, value in kwargs.items()}
        return _peer.call(("call", self.token), args, kwargs)

    def __reduce__(self):
        return _kept_or_stub, (self.side, self.token)


def callback(fn):
    """`fn` as the other process takes it: a call there calls it here."""
    if isinstance(fn, _Stub):
        return fn
    # ponytail: kept for the session, one small entry for each callback sent; drop them once
    # called if a session ever sends millions.
    token = next(_tokens)
    _kept[token] = fn
    return _Stub(_side, token)


class _Peer:
    """One end of the pipe: calls the other end and answers its calls, each on a thread of its own."""

    def __init__(self, conn, serve, on_gone):
        self.conn, self.serve, self.on_gone = conn, serve, on_gone
        self._lock = threading.Lock()
        self._waiting: dict[int, list] = {}
        self._ids = itertools.count()
        self.gone = self.closed = False
        threading.Thread(target=self._read, name=f"msflow-host-{_side}", daemon=True).start()

    def _send(self, message) -> None:
        payload = pickle.dumps(message)
        with self._lock:
            self.conn.send_bytes(payload)

    def call(self, target, args=(), kwargs=None):
        rid, slot = next(self._ids), [threading.Event(), False, None]
        self._waiting[rid] = slot
        try:
            if self.gone:
                raise OSError
            self._send(("ask", rid, target, args, kwargs or {}))
        except (OSError, EOFError, ValueError):
            self._waiting.pop(rid, None)
            raise EngineGone("the engine's process has ended") from None
        slot[0].wait()
        if slot[1]:
            return slot[2]
        raise slot[2]

    def _read(self) -> None:
        try:
            while True:
                message = pickle.loads(self.conn.recv_bytes())
                if message[0] == "ask":
                    threading.Thread(target=self._answer, args=message[1:], daemon=True).start()
                else:
                    _, rid, ok, value = message
                    slot = self._waiting.pop(rid)
                    slot[1], slot[2] = ok, value
                    slot[0].set()
        except (EOFError, OSError):
            pass
        self.gone = True
        for slot in list(self._waiting.values()):
            slot[1], slot[2] = False, EngineGone("the engine's process has ended")
            slot[0].set()
        self._waiting.clear()
        self.on_gone()

    def _answer(self, rid, target, args, kwargs) -> None:
        try:
            value = _kept[target[1]](*args, **kwargs) if target[0] == "call" else self.serve(target, args, kwargs)
            reply = ("got", rid, True, value)
        except BaseException as error:  # noqa: BLE001 - it is raised on the other side
            error.add_note(f"in the {_side}'s process:\n{traceback.format_exc()}")
            reply = ("got", rid, False, error)
        try:
            self._send(reply)
        except (OSError, EOFError, ValueError):
            pass  # the other side is gone
        except Exception as error:  # noqa: BLE001 - what it returned or raised does not pickle
            self._send(("got", rid, False, RuntimeError(f"{type(error).__name__}: {error}\n{reply[3]!r}")))

    def close(self) -> None:
        """The other side reads the end of the pipe now: a `close` alone waits for this side's
        reading thread, which holds the socket while it waits."""
        self.closed = True
        end = socket.socket(fileno=self.conn.fileno())  # the pipe is a socketpair
        try:
            end.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        finally:
            end.detach()
        self.conn.close()


class EngineProxy:
    """`Engine`'s methods, run by the engine in the child process. `close` ends that process."""

    def __init__(self, process, peer):
        self.process, self.peer = process, peer

    @property
    def closed(self) -> bool:
        return self.peer.closed

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)

        def run(*args, **kwargs):
            kwargs = {key: callback(value) if key in CALLBACKS and callable(value) else value
                      for key, value in kwargs.items()}
            return self.peer.call(("engine", name), args, kwargs)
        return run

    def call(self, name: str, args=(), kwargs=None):
        """The child's `Workspace.<name>`."""
        kwargs = {key: callback(value) if key in CALLBACKS and callable(value) else value
                  for key, value in (kwargs or {}).items()}
        return self.peer.call(("workspace", name), args, kwargs)

    def close(self, timeout: float = 60) -> None:
        self.peer.close()  # the child sees the pipe close, closes its project and engine, and exits
        self.process.join(timeout)
        if self.process.is_alive():
            self.process.kill()
            self.process.join()


_STARTER = ThreadPoolExecutor(1, thread_name_prefix="msflow-engine-start")


def start(init: dict, on_gone) -> EngineProxy:
    """Start the child process with the `Workspace` that `init` describes."""
    global _peer
    context = multiprocessing.get_context("spawn")
    mine, theirs = context.Pipe()
    process = context.Process(target=_child, args=(theirs, pickle.dumps(init)), name="msflow-engine-host")
    # not a daemon: its executors start processes of their own. Started from a thread that lasts
    # as long as the application: `PR_SET_PDEATHSIG` kills the child when the thread that started
    # it ends, not only its process.
    _STARTER.submit(process.start).result()
    theirs.close()
    _peer = _Peer(mine, None, on_gone)
    return EngineProxy(process, _peer)


def _die_with_parent() -> None:
    if sys.platform.startswith("linux"):
        import ctypes
        import signal

        ctypes.CDLL(None).prctl(1, signal.SIGTERM)  # PR_SET_PDEATHSIG


def _child(conn, init: bytes) -> None:
    global _side, _peer
    _die_with_parent()
    _side = "engine"
    sys.setswitchinterval(0.001)  # what asks the engine is answered while the delivery writes
    init = pickle.loads(init)
    from ms_flow.main import Workspace

    workspace = Workspace(**init["workspace"], engine_host="inline")
    workspace.host_env, workspace.jobs_setup = init["host_env"], init["jobs_setup"]
    ended = threading.Event()

    def serve(target, args, kwargs):
        kind, name = target
        if kind == "engine":
            return getattr(workspace.engine, name)(*args, **kwargs)
        if name in ("_activate", "deactivate") or name.startswith(("save_", "remove_")):
            workspace.settings_manager.reload()  # the application may have changed them
        return getattr(workspace, name)(*args, **kwargs)

    _peer = _Peer(conn, serve, ended.set)
    ended.wait()
    try:
        workspace.shutdown()
    finally:
        os._exit(0)  # threads of executors may still wait: nothing is left for them
