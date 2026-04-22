"""Markdown task manager: watch a directory of task `.md` files and emit events.

Emits `NewTask` when a task file is created, modified, or moved-in, and
`RemoveTask` when it is deleted or moved-out.  An initial scan on start
emits `NewTask` for every existing file.

Runs the underlying watchdog observer on a background thread; events are
dispatched onto the asyncio queue using `loop.call_soon_threadsafe`.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from pathlib import Path
from types import TracebackType

from watchdog.events import (
    FileSystemEvent,
    FileSystemEventHandler,
)
from watchdog.observers import Observer
from watchdog.observers.api import BaseObserver

from taskpull.engine_events import (
    NewTask,
    RemoveTask,
    TaskGoal,
    TaskName,
    TaskSource,
)

logger = logging.getLogger(__name__)

_TASK_GLOB = "[!.]*.md"


class MdTaskParseError(ValueError):
    """Raised when a task markdown file cannot be parsed."""


def _parse_markdown(text: str) -> tuple[dict[str, str], str]:
    lines = text.split("\n")
    if not lines or lines[0].strip() != "---":
        raise MdTaskParseError("missing opening '---' delimiter")
    close_idx: int | None = None
    for i, line in enumerate(lines[1:], start=1):
        if line.strip() == "---":
            close_idx = i
            break
    if close_idx is None:
        raise MdTaskParseError("missing closing '---' delimiter")

    fields: dict[str, str] = {}
    for line in lines[1:close_idx]:
        stripped = line.strip()
        if not stripped:
            continue
        key, sep, value = stripped.partition(":")
        if not sep:
            raise MdTaskParseError(f"malformed frontmatter line: {stripped!r}")
        fields[key.strip()] = value.strip()

    prompt = "\n".join(lines[close_idx + 1 :]).strip()
    return fields, prompt


def parse_task_file(path: Path) -> NewTask:
    """Parse a markdown task file into a `NewTask` event."""
    fields, prompt = _parse_markdown(path.read_text())

    if "repo" not in fields:
        raise MdTaskParseError("missing required field 'repo'")

    goal_str = fields.get("goal", "pr")
    try:
        goal = TaskGoal(goal_str)
    except ValueError:
        valid = ", ".join(g.value for g in TaskGoal)
        raise MdTaskParseError(
            f"invalid goal {goal_str!r}, must be one of: {valid}"
        ) from None

    repeat_str = fields.get("repeat", "false").strip().lower()
    if repeat_str not in ("true", "false"):
        raise MdTaskParseError(
            f"invalid repeat {repeat_str!r}, must be 'true' or 'false'"
        )

    return NewTask(
        name=TaskName(path.stem),
        prompt=prompt,
        goal=goal,
        location=fields["repo"],
        key=fields.get("repo_lock"),
        repeat=repeat_str == "true",
        source=TaskSource.FILE,
    )


def _to_path(raw: str | bytes) -> Path:
    return Path(raw.decode() if isinstance(raw, bytes) else raw)


class _Handler(FileSystemEventHandler):
    def __init__(self, manager: MdTaskManager) -> None:
        self._manager = manager

    def on_created(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        self._manager._schedule_update(_to_path(event.src_path))

    def on_modified(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        self._manager._schedule_update(_to_path(event.src_path))

    def on_deleted(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        self._manager._schedule_remove(_to_path(event.src_path))

    def on_moved(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        self._manager._schedule_remove(_to_path(event.src_path))
        self._manager._schedule_update(_to_path(event.dest_path))


def _is_task_path(path: Path) -> bool:
    return path.suffix == ".md" and not path.name.startswith(".")


class MdTaskManager:
    """Watch a tasks directory and emit `NewTask` / `RemoveTask` to an asyncio queue.

    Usage (synchronous context manager, from inside an asyncio task):

        async with MdTaskManager(queue, path):
            ...

    or call `start()` / `stop()` directly.  Starting performs an initial scan
    and enqueues a `NewTask` for every existing file.
    """

    def __init__(
        self,
        queue: asyncio.Queue[NewTask | RemoveTask],
        path: Path,
    ) -> None:
        self._queue = queue
        self._path = path
        self._loop: asyncio.AbstractEventLoop | None = None
        self._observer: BaseObserver | None = None
        self._lock = threading.Lock()
        self._known: dict[TaskName, NewTask] = {}

    def current(self) -> dict[TaskName, NewTask]:
        """Return a snapshot of the currently-known tasks."""
        with self._lock:
            return dict(self._known)

    async def __aenter__(self) -> MdTaskManager:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.stop()

    async def start(self) -> None:
        if self._observer is not None:
            raise RuntimeError("MdTaskManager already started")
        self._loop = asyncio.get_running_loop()
        self._path.mkdir(parents=True, exist_ok=True)

        for p in sorted(self._path.glob(_TASK_GLOB)):
            self._emit_update(p)

        observer = Observer()
        observer.schedule(_Handler(self), str(self._path), recursive=False)
        observer.start()
        self._observer = observer

    async def stop(self) -> None:
        observer = self._observer
        if observer is None:
            return
        self._observer = None
        observer.stop()
        await asyncio.to_thread(observer.join)

    def _schedule_update(self, path: Path) -> None:
        if not _is_task_path(path):
            return
        loop = self._loop
        if loop is None:
            raise RuntimeError("MdTaskManager not started")
        loop.call_soon_threadsafe(self._emit_update, path)

    def _schedule_remove(self, path: Path) -> None:
        if not _is_task_path(path):
            return
        loop = self._loop
        if loop is None:
            raise RuntimeError("MdTaskManager not started")
        loop.call_soon_threadsafe(self._emit_remove, path)

    def _emit_update(self, path: Path) -> None:
        name = TaskName(path.stem)
        try:
            event = parse_task_file(path)
        except FileNotFoundError:
            self._emit_remove(path)
            return
        except MdTaskParseError as e:
            logger.warning("skipping task file %s: %s", path, e)
            return
        except OSError as e:
            logger.warning("failed to read task file %s: %s", path, e)
            return

        with self._lock:
            previous = self._known.get(name)
            if previous == event:
                return
            self._known[name] = event
        self._queue.put_nowait(event)

    def _emit_remove(self, path: Path) -> None:
        name = TaskName(path.stem)
        with self._lock:
            if name not in self._known:
                return
            del self._known[name]
        self._queue.put_nowait(RemoveTask(name=name))
