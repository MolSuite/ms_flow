"""An executor whose tasks go on without this process: what the engine tells it about a job."""
import time

from ms_flow.engine import Engine, content_key
from ms_flow.engine.executors import CANCELED, DONE


class _Far:
    """Runs nothing: the test says when a task ended."""

    capacity, uses_tokens, outlives = 4, False, True

    def __init__(self):
        self.tasks, self.said = [], []

    def bind(self, on_done):
        self.on_done = on_done

    def submit(self, fn, payload, tag, key=None, keys=()):
        self.tasks.append((tag, key, keys))
        return tag

    def cancel(self, handle):
        self.on_done(handle, CANCELED, None, None, 0)

    def ended(self, key, kept):
        self.said.append((key, kept))

    def shutdown(self):
        pass


def _until(check, seconds=10):
    deadline = time.monotonic() + seconds
    while not check() and time.monotonic() < deadline:
        time.sleep(0.01)
    return check()


def test_a_job_with_a_key_is_kept_there_only_when_interrupted(tmp_path):
    far = _Far()
    engine = Engine(cpus=2)
    engine.register("far", far)
    engine.attach("journal", tmp_path / "journal.db")

    done = engine.submit("far", print, [{"n": 1}], journal="journal", key="done")
    assert _until(lambda: far.tasks)
    tag, key, keys = far.tasks.pop()
    assert (key, keys) == ("done", [content_key({"n": 1})])
    far.on_done(tag, DONE, None, None, 0)
    assert engine.wait(done, 10).status == "completed" and far.said == [("done", False)]

    canceled = engine.submit("far", print, [{"n": 1}], journal="journal", key="canceled")
    assert _until(lambda: far.tasks)
    engine.cancel(canceled)
    assert engine.wait(canceled, 10).status == "canceled" and far.said[-1] == ("canceled", False)

    # Without a key there is nothing to ask for it again by: it is canceled, and that is all.
    engine.submit("far", print, [{"n": 2}], journal="journal")
    engine.submit("far", print, [{"n": 3}], journal="journal", key="left")
    assert _until(lambda: len(far.tasks) == 3)
    assert far.tasks[1][1] is None
    engine.close()
    assert far.said[2:] == [("left", True)]
