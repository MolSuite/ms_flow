from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass
from queue import Empty, Full, Queue
from typing import Any


@dataclass(frozen=True)
class EngineCommand:
    kind: str
    payload: dict[str, Any]
    future: Future


class CommandInbox:
    def __init__(self, maxsize: int = 256):
        self._queue: Queue[EngineCommand] = Queue(maxsize=max(1, int(maxsize)))

    def publish(self, kind: str, payload: dict[str, Any]) -> Future:
        future: Future = Future()
        command = EngineCommand(kind=kind, payload=dict(payload), future=future)
        try:
            self._queue.put_nowait(command)
        except Full as exc:
            raise RuntimeError("The engine inbox is full.") from exc
        return future

    def drain(self) -> list[EngineCommand]:
        commands: list[EngineCommand] = []
        while True:
            try:
                commands.append(self._queue.get_nowait())
            except Empty:
                return commands

    def reject_pending(self, error: BaseException) -> None:
        for command in self.drain():
            if not command.future.done():
                command.future.set_exception(error)
