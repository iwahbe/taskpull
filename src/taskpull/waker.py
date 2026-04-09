from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from pydantic import BaseModel

from taskpull.engine_events import TaskName, WakeTask
from taskpull.state_manager import StateFactory


class Waker(ABC):
    """Waker is responsible for ensuring that a task is woken up.

    For a production waker, this must survive loading.
    """

    @abstractmethod
    def schedule(self, wait: float, task: TaskName) -> None: ...


class _WakerState(BaseModel):
    deadlines: dict[TaskName, float] = {}


class AsyncWaker(Waker):
    """Production waker backed by asyncio timers with persistent deadlines."""

    def __init__(
        self,
        queue: asyncio.Queue[WakeTask],
        state_factory: StateFactory,
    ) -> None:
        self._queue = queue
        self._state_manager = state_factory(_WakerState)
        self._timers: dict[TaskName, asyncio.Task[None]] = {}
        self._deadlines: dict[TaskName, float] = {}
        asyncio.ensure_future(self._restore())

    async def _restore(self) -> None:
        state = await self._state_manager.load()
        if state is None:
            return
        now = time.time()
        for task_name, deadline in state.deadlines.items():
            remaining = deadline - now
            if remaining <= 0:
                await self._queue.put(WakeTask(name=task_name))
            else:
                self._deadlines[task_name] = deadline
                self._timers[task_name] = asyncio.create_task(
                    self._fire(task_name, remaining)
                )
        await self._persist()

    def schedule(self, wait: float, task: TaskName) -> None:
        existing = self._timers.pop(task, None)
        if existing is not None:
            existing.cancel()
        self._deadlines[task] = time.time() + wait
        self._timers[task] = asyncio.create_task(self._fire(task, wait))
        asyncio.ensure_future(self._persist())

    async def _fire(self, task: TaskName, delay: float) -> None:
        await asyncio.sleep(delay)
        self._timers.pop(task, None)
        self._deadlines.pop(task, None)
        await self._queue.put(WakeTask(name=task))
        await self._persist()

    async def _persist(self) -> None:
        await self._state_manager.save(_WakerState(deadlines=dict(self._deadlines)))
