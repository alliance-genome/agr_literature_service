"""
Regression test for SCRUM-6106.

The `_current_user_id` ContextVar in `agr_literature_service.api.user`
must be isolated per asyncio task and per thread, so concurrent requests
cannot overwrite each other's identity before the SQLAlchemy
`before_update` listener stamps `updated_by`.

Before the fix, `_current_user_id` was a module-level global, and these
tests would fail: both tasks/threads would observe whichever set ran last.
"""

import asyncio
import threading
from typing import Dict, Optional

import anyio
import pytest

from agr_literature_service.api.user import (
    _current_user_id,
    get_global_user_id,
)


@pytest.fixture(autouse=True)
def _clear_global_user():
    """Ensure each test starts with no current user set."""
    token = _current_user_id.set(None)
    try:
        yield
    finally:
        _current_user_id.reset(token)


def test_contextvar_isolated_across_asyncio_tasks():
    """Two concurrent asyncio tasks see only their own user id."""

    async def _set_and_read(uid: str, hold_seconds: float) -> Optional[str]:
        _current_user_id.set(uid)
        # Yield to the event loop so the other task can run between the
        # set() and the get(). With a process-global, this is exactly the
        # window where the other coroutine would clobber the value.
        await asyncio.sleep(hold_seconds)
        return get_global_user_id()

    async def _run():
        return await asyncio.gather(
            _set_and_read("user_a", 0.05),
            _set_and_read("user_b", 0.01),
        )

    a, b = asyncio.run(_run())
    assert a == "user_a"
    assert b == "user_b"


def test_contextvar_isolated_across_threads():
    """Two raw threads see only their own user id.

    Raw `threading.Thread` does not share a Context across threads, so this
    test guards against re-introduction of a module-level global; it is not
    a model of FastAPI's threadpool dispatch (see the anyio test below for
    that).
    """
    results: Dict[str, Optional[str]] = {}
    barrier = threading.Barrier(2)

    def _worker(uid: str) -> None:
        _current_user_id.set(uid)
        # Wait for the other thread to also set its value before reading.
        # With a process-global, this is the window where one thread would
        # observe the other's user id.
        barrier.wait()
        results[uid] = get_global_user_id()

    t1 = threading.Thread(target=_worker, args=("user_a",))
    t2 = threading.Thread(target=_worker, args=("user_b",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert results == {"user_a": "user_a", "user_b": "user_b"}


def test_contextvar_does_not_leak_between_threadpool_calls():
    """Two sequential calls through anyio's threadpool — the actual
    dispatch path used by FastAPI for sync endpoints — must not see each
    other's user id, even when reused by the same worker thread.

    `anyio.to_thread.run_sync` wraps each call in
    `contextvars.copy_context().run(...)`, so a `.set()` inside the
    callable mutates only that copy and is discarded on return. With the
    pre-fix module global, the first call's identity would persist into
    the second.
    """

    def _set_user(uid: str) -> None:
        _current_user_id.set(uid)

    def _read_user() -> object:
        return get_global_user_id()

    async def _run():
        await anyio.to_thread.run_sync(_set_user, "user_a")
        # Second call: must not observe "user_a" set by the first call,
        # even if anyio reuses the same worker thread.
        return await anyio.to_thread.run_sync(_read_user)

    assert asyncio.run(_run()) is None
