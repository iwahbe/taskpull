from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from taskpull.engine_events import (
    NewTask,
    RemoveTask,
    TaskGoal,
    TaskName,
    TaskSource,
)
from taskpull.markdown_v2 import MdTaskManager, MdTaskParseError, parse_task_file


def _write_task(
    path: Path,
    *,
    repo: str = "https://github.com/o/r",
    goal: str | None = None,
    repeat: str | None = None,
    repo_lock: str | None = None,
    prompt: str = "Do the thing.",
) -> None:
    fm = [f"repo: {repo}"]
    if goal is not None:
        fm.append(f"goal: {goal}")
    if repeat is not None:
        fm.append(f"repeat: {repeat}")
    if repo_lock is not None:
        fm.append(f"repo_lock: {repo_lock}")
    path.write_text("---\n" + "\n".join(fm) + "\n---\n" + prompt + "\n")


async def _drain(queue: asyncio.Queue, n: int, timeout: float = 2.0) -> list:
    events: list = []
    for _ in range(n):
        events.append(await asyncio.wait_for(queue.get(), timeout=timeout))
    return events


async def _wait_for_event(queue: asyncio.Queue, timeout: float = 2.0):
    return await asyncio.wait_for(queue.get(), timeout=timeout)


def test_parse_task_file_basic(tmp_path: Path) -> None:
    p = tmp_path / "my-task.md"
    _write_task(p)
    assert parse_task_file(p) == NewTask(
        name=TaskName("my-task"),
        prompt="Do the thing.",
        goal=TaskGoal.PR,
        location="https://github.com/o/r",
        key=None,
        repeat=False,
        source=TaskSource.FILE,
    )


def test_parse_task_file_all_fields(tmp_path: Path) -> None:
    p = tmp_path / "t.md"
    _write_task(
        p,
        repo="https://github.com/o/r",
        goal="issue",
        repeat="true",
        repo_lock="lane-a",
        prompt="Multi\n\nLine prompt",
    )
    assert parse_task_file(p) == NewTask(
        name=TaskName("t"),
        prompt="Multi\n\nLine prompt",
        goal=TaskGoal.ISSUE,
        location="https://github.com/o/r",
        key="lane-a",
        repeat=True,
        source=TaskSource.FILE,
    )


def test_parse_task_file_missing_opening_delimiter(tmp_path: Path) -> None:
    p = tmp_path / "bad.md"
    p.write_text("repo: x\n---\nPrompt\n")
    with pytest.raises(MdTaskParseError, match="missing opening"):
        parse_task_file(p)


def test_parse_task_file_missing_closing_delimiter(tmp_path: Path) -> None:
    p = tmp_path / "bad.md"
    p.write_text("---\nrepo: x\nPrompt\n")
    with pytest.raises(MdTaskParseError, match="missing closing"):
        parse_task_file(p)


def test_parse_task_file_missing_repo(tmp_path: Path) -> None:
    p = tmp_path / "bad.md"
    p.write_text("---\nrepeat: true\n---\nPrompt\n")
    with pytest.raises(MdTaskParseError, match="missing required field 'repo'"):
        parse_task_file(p)


def test_parse_task_file_malformed_frontmatter_line(tmp_path: Path) -> None:
    p = tmp_path / "bad.md"
    p.write_text("---\nnope\n---\nPrompt\n")
    with pytest.raises(MdTaskParseError, match="malformed frontmatter"):
        parse_task_file(p)


def test_parse_task_file_invalid_goal(tmp_path: Path) -> None:
    p = tmp_path / "bad.md"
    _write_task(p, goal="bogus")
    with pytest.raises(MdTaskParseError, match="invalid goal"):
        parse_task_file(p)


def test_parse_task_file_invalid_repeat(tmp_path: Path) -> None:
    p = tmp_path / "bad.md"
    _write_task(p, repeat="maybe")
    with pytest.raises(MdTaskParseError, match="invalid repeat"):
        parse_task_file(p)


@pytest.mark.asyncio
async def test_initial_scan_emits_new_task(tmp_path: Path) -> None:
    _write_task(tmp_path / "a.md", repo="https://github.com/o/a")
    _write_task(tmp_path / "b.md", repo="https://github.com/o/b")

    queue: asyncio.Queue = asyncio.Queue()
    async with MdTaskManager(queue, tmp_path):
        events = await _drain(queue, 2)

    names = sorted(e.name for e in events)
    assert names == [TaskName("a"), TaskName("b")]
    assert all(isinstance(e, NewTask) for e in events)


@pytest.mark.asyncio
async def test_initial_scan_ignores_dotfiles(tmp_path: Path) -> None:
    _write_task(tmp_path / ".hidden.md")
    _write_task(tmp_path / "visible.md")

    queue: asyncio.Queue = asyncio.Queue()
    async with MdTaskManager(queue, tmp_path):
        event = await _wait_for_event(queue)
        assert event.name == TaskName("visible")
        assert queue.empty()


@pytest.mark.asyncio
async def test_creates_missing_dir(tmp_path: Path) -> None:
    target = tmp_path / "nope"
    queue: asyncio.Queue = asyncio.Queue()
    async with MdTaskManager(queue, target):
        assert target.is_dir()
        assert queue.empty()


@pytest.mark.asyncio
async def test_file_created_emits_new_task(tmp_path: Path) -> None:
    queue: asyncio.Queue = asyncio.Queue()
    async with MdTaskManager(queue, tmp_path):
        _write_task(tmp_path / "new.md", repo="https://github.com/o/new")
        event = await _wait_for_event(queue)
        assert isinstance(event, NewTask)
        assert event.name == TaskName("new")
        assert event.location == "https://github.com/o/new"


@pytest.mark.asyncio
async def test_file_modified_emits_new_task(tmp_path: Path) -> None:
    path = tmp_path / "t.md"
    _write_task(path, repo="https://github.com/o/r1")

    queue: asyncio.Queue = asyncio.Queue()
    async with MdTaskManager(queue, tmp_path):
        first = await _wait_for_event(queue)
        assert first.location == "https://github.com/o/r1"

        _write_task(path, repo="https://github.com/o/r2")
        for _ in range(5):
            ev = await _wait_for_event(queue)
            if isinstance(ev, NewTask) and ev.location == "https://github.com/o/r2":
                break
        else:
            pytest.fail("did not observe updated NewTask")


@pytest.mark.asyncio
async def test_file_deleted_emits_remove_task(tmp_path: Path) -> None:
    path = tmp_path / "t.md"
    _write_task(path)

    queue: asyncio.Queue = asyncio.Queue()
    async with MdTaskManager(queue, tmp_path):
        first = await _wait_for_event(queue)
        assert isinstance(first, NewTask)

        path.unlink()
        for _ in range(5):
            ev = await _wait_for_event(queue)
            if isinstance(ev, RemoveTask) and ev.name == TaskName("t"):
                break
        else:
            pytest.fail("did not observe RemoveTask")


@pytest.mark.asyncio
async def test_invalid_file_logged_not_emitted(tmp_path: Path) -> None:
    bad = tmp_path / "bad.md"
    bad.write_text("no frontmatter here\n")

    queue: asyncio.Queue = asyncio.Queue()
    async with MdTaskManager(queue, tmp_path):
        await asyncio.sleep(0.05)
        assert queue.empty()


@pytest.mark.asyncio
async def test_current_reflects_state(tmp_path: Path) -> None:
    _write_task(tmp_path / "a.md", repo="https://github.com/o/a")
    _write_task(tmp_path / "b.md", repo="https://github.com/o/b")

    queue: asyncio.Queue = asyncio.Queue()
    async with MdTaskManager(queue, tmp_path) as mgr:
        await _drain(queue, 2)
        current = mgr.current()
        assert set(current.keys()) == {TaskName("a"), TaskName("b")}
        assert current[TaskName("a")].location == "https://github.com/o/a"


@pytest.mark.asyncio
async def test_duplicate_modify_not_reemitted(tmp_path: Path) -> None:
    path = tmp_path / "t.md"
    _write_task(path)

    queue: asyncio.Queue = asyncio.Queue()
    async with MdTaskManager(queue, tmp_path):
        first = await _wait_for_event(queue)
        assert isinstance(first, NewTask)

        _write_task(path)
        await asyncio.sleep(0.2)
        assert queue.empty()
