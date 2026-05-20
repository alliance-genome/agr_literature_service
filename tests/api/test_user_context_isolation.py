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

from agr_literature_service.api.user import (
    _current_user_id,
    get_global_user_id,
)


def test_contextvar_isolated_across_asyncio_tasks():
    """Two concurrent asyncio tasks see only their own user id."""

    async def _set_and_read(uid: str, hold_seconds: float) -> str:
        _current_user_id.set(uid)
        # Yield to the event loop so the other task can run between the
        # set() and the get(). With a process-global, this is exactly the
        # window where the other coroutine would clobber the value.
        await asyncio.sleep(hold_seconds)
        result = get_global_user_id()
        assert result is not None
        return result

    async def _run():
        return await asyncio.gather(
            _set_and_read("user_a", 0.05),
            _set_and_read("user_b", 0.01),
        )

    a, b = asyncio.run(_run())
    assert a == "user_a"
    assert b == "user_b"


def test_contextvar_isolated_across_threads():
    """Two threads (simulating FastAPI's sync-endpoint threadpool)
    see only their own user id."""
    results: dict[str, str] = {}
    barrier = threading.Barrier(2)

    def _worker(uid: str) -> None:
        _current_user_id.set(uid)
        # Wait for the other thread to also set its value before reading,
        # which is the exact race produced by FastAPI's threadpool.
        barrier.wait()
        observed = get_global_user_id()
        assert observed is not None
        results[uid] = observed

    t1 = threading.Thread(target=_worker, args=("user_a",))
    t2 = threading.Thread(target=_worker, args=("user_b",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert results == {"user_a": "user_a", "user_b": "user_b"}
