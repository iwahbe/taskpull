import asyncio
import time

import pytest

from taskpull.engine_events import TaskName, WakeTask
from taskpull.state_manager import InMemoryStateManager
from taskpull.waker import AsyncWaker


def memory_factory(model: type) -> InMemoryStateManager:
    return InMemoryStateManager()


@pytest.mark.asyncio
async def test_schedule_fires_after_delay() -> None:
    queue: asyncio.Queue[WakeTask] = asyncio.Queue()
    waker = AsyncWaker(queue, memory_factory)
    await asyncio.sleep(0)

    waker.schedule(0.05, TaskName("task-a"))
    assert queue.empty()

    await asyncio.sleep(0.1)
    event = queue.get_nowait()
    assert event == WakeTask(name=TaskName("task-a"))


@pytest.mark.asyncio
async def test_schedule_replaces_previous() -> None:
    queue: asyncio.Queue[WakeTask] = asyncio.Queue()
    waker = AsyncWaker(queue, memory_factory)
    await asyncio.sleep(0)

    waker.schedule(0.05, TaskName("task-a"))
    waker.schedule(0.1, TaskName("task-a"))

    await asyncio.sleep(0.08)
    assert queue.empty(), "first timer should have been cancelled"

    await asyncio.sleep(0.05)
    event = queue.get_nowait()
    assert event == WakeTask(name=TaskName("task-a"))
    assert queue.empty()


@pytest.mark.asyncio
async def test_multiple_tasks_fire_independently() -> None:
    queue: asyncio.Queue[WakeTask] = asyncio.Queue()
    waker = AsyncWaker(queue, memory_factory)
    await asyncio.sleep(0)

    waker.schedule(0.1, TaskName("slow"))
    waker.schedule(0.05, TaskName("fast"))

    await asyncio.sleep(0.08)
    event = queue.get_nowait()
    assert event == WakeTask(name=TaskName("fast"))
    assert queue.empty()

    await asyncio.sleep(0.05)
    event = queue.get_nowait()
    assert event == WakeTask(name=TaskName("slow"))


@pytest.mark.asyncio
async def test_restore_fires_expired() -> None:
    from taskpull.waker import _WakerState

    expired_state = _WakerState(deadlines={TaskName("task-a"): time.time() - 100.0})
    manager = InMemoryStateManager()
    await manager.save(expired_state)

    def factory(model: type) -> InMemoryStateManager:
        return manager

    queue: asyncio.Queue[WakeTask] = asyncio.Queue()
    _waker = AsyncWaker(queue, factory)
    await asyncio.sleep(0)  # let _restore run

    event = queue.get_nowait()
    assert event == WakeTask(name=TaskName("task-a"))


@pytest.mark.asyncio
async def test_restore_schedules_future() -> None:
    shared_managers: dict[type, InMemoryStateManager] = {}

    def shared_factory(model: type) -> InMemoryStateManager:
        if model not in shared_managers:
            shared_managers[model] = InMemoryStateManager()
        return shared_managers[model]

    queue1: asyncio.Queue[WakeTask] = asyncio.Queue()
    waker1 = AsyncWaker(queue1, shared_factory)
    await asyncio.sleep(0)

    waker1.schedule(0.15, TaskName("task-a"))
    await asyncio.sleep(0)  # let persist run

    # Second waker restores — deadline is still in the future.
    queue2: asyncio.Queue[WakeTask] = asyncio.Queue()
    _waker2 = AsyncWaker(queue2, shared_factory)
    await asyncio.sleep(0)

    assert queue2.empty()
    await asyncio.sleep(0.2)
    event = queue2.get_nowait()
    assert event == WakeTask(name=TaskName("task-a"))
