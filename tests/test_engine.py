from dataclasses import dataclass

import pytest

from taskpull.engine import (
    Active,
    Closed,
    Engine,
    Idle,
    Initializing,
)
from taskpull.engine_events import (
    IssueCreated,
    NewTask,
    PRClosed,
    PRCreated,
    RemoveTask,
    SessionID,
    SessionIdle,
    SessionTerminated,
    SessionWorking,
    TaskGoal,
    TaskName,
    TaskSource,
)
from taskpull.session_manager import SessionManager
from taskpull.state_manager import InMemoryStateManager
from taskpull.waker import Waker


@dataclass(frozen=True)
class Create:
    prompt: str
    location: str
    result: SessionID


@dataclass(frozen=True)
class Pause:
    session_id: SessionID


@dataclass(frozen=True)
class Resume:
    session_id: SessionID


@dataclass(frozen=True)
class Terminate:
    session_id: SessionID


@dataclass(frozen=True)
class Schedule:
    wait: float
    task: TaskName


Call = Create | Pause | Resume | Terminate | Schedule


class FakeSessionManager(SessionManager):
    def __init__(self, calls: list[Call]) -> None:
        self._calls = calls
        self._next_id = 0

    async def create(self, prompt: str, location: str) -> SessionID:
        sid = SessionID(f"session-{self._next_id}")
        self._next_id += 1
        self._calls.append(Create(prompt=prompt, location=location, result=sid))
        return sid

    async def pause(self, session: SessionID) -> None:
        self._calls.append(Pause(session_id=session))

    async def resume(self, session: SessionID) -> None:
        self._calls.append(Resume(session_id=session))

    async def terminate(self, session: SessionID) -> None:
        self._calls.append(Terminate(session_id=session))


class FakeWaker(Waker):
    def __init__(self, calls: list[Call]) -> None:
        self._calls = calls

    def schedule(self, wait: float, task: TaskName) -> None:
        self._calls.append(Schedule(wait=wait, task=task))


def _make_engine() -> tuple[Engine, list[Call]]:
    calls: list[Call] = []
    sessions = FakeSessionManager(calls)
    waker = FakeWaker(calls)
    engine = Engine(sessions, lambda model: InMemoryStateManager(), waker)
    return engine, calls


PR_PROMPT = "Fix the bug\n\nWhen your work is ready, create a descriptively named branch, push it, and open a PR:\n  git checkout -b your-descriptive-branch-name\n  git push -u origin HEAD\n  gh pr create --fill\n"

ISSUE_PROMPT = 'File a bug report\n\nWhen your work is ready, create a GitHub issue:\n  gh issue create --title "Your descriptive title" --body "Issue body"\n'


@pytest.mark.asyncio
async def test_adhoc_pr_lifecycle() -> None:
    engine, calls = _make_engine()
    await engine.enable()

    name = TaskName("fix-bug")
    sid = SessionID("session-0")

    await engine.handle(
        NewTask(
            name=name,
            prompt="Fix the bug",
            goal=TaskGoal.PR,
            location="/repo",
            key=None,
            repeat=False,
            source=TaskSource.ADHOC,
        )
    )

    assert calls == [
        Create(prompt=PR_PROMPT, location="/repo", result=sid),
    ]
    status = await engine.status()
    phase = status.tasks[name].phase
    assert isinstance(phase, Initializing)
    assert phase.session.session_id == sid

    await engine.handle(SessionWorking(session_id=sid))
    status = await engine.status()
    assert isinstance(status.tasks[name].phase, Active)

    await engine.handle(
        PRCreated(session_id=sid, pr_url="https://github.com/org/repo/pull/1")
    )
    status = await engine.status()
    phase = status.tasks[name].phase
    assert isinstance(phase, Active)
    assert len(phase.session.prs) == 1
    assert phase.session.prs[0].url == "https://github.com/org/repo/pull/1"

    await engine.handle(SessionIdle(session_id=sid))
    status = await engine.status()
    assert isinstance(status.tasks[name].phase, Idle)

    await engine.handle(PRClosed(pr_url="https://github.com/org/repo/pull/1"))
    assert calls == [
        Create(prompt=PR_PROMPT, location="/repo", result=sid),
        Terminate(session_id=sid),
    ]

    await engine.handle(SessionTerminated(session_id=sid))
    status = await engine.status()
    assert isinstance(status.tasks[name].phase, Closed)


@pytest.mark.asyncio
async def test_adhoc_issue_lifecycle() -> None:
    engine, calls = _make_engine()
    await engine.enable()

    name = TaskName("file-bug")
    sid = SessionID("session-0")

    await engine.handle(
        NewTask(
            name=name,
            prompt="File a bug report",
            goal=TaskGoal.ISSUE,
            location="/repo",
            key=None,
            repeat=False,
            source=TaskSource.ADHOC,
        )
    )

    assert calls == [
        Create(prompt=ISSUE_PROMPT, location="/repo", result=sid),
    ]
    status = await engine.status()
    assert isinstance(status.tasks[name].phase, Initializing)

    await engine.handle(SessionWorking(session_id=sid))
    status = await engine.status()
    assert isinstance(status.tasks[name].phase, Active)

    await engine.handle(
        IssueCreated(session_id=sid, issue_url="https://github.com/org/repo/issues/1")
    )
    status = await engine.status()
    phase = status.tasks[name].phase
    assert isinstance(phase, Active)
    assert len(phase.session.issues) == 1
    assert phase.session.issues[0].url == "https://github.com/org/repo/issues/1"

    # When an issue-goal task goes idle after creating an issue, the engine terminates it.
    await engine.handle(SessionIdle(session_id=sid))
    status = await engine.status()
    assert isinstance(status.tasks[name].phase, Idle)
    assert calls == [
        Create(prompt=ISSUE_PROMPT, location="/repo", result=sid),
        Terminate(session_id=sid),
    ]

    await engine.handle(SessionTerminated(session_id=sid))
    status = await engine.status()
    assert isinstance(status.tasks[name].phase, Closed)


@pytest.mark.asyncio
async def test_remove_task_mid_session() -> None:
    engine, calls = _make_engine()
    await engine.enable()

    name = TaskName("fix-bug")
    sid = SessionID("session-0")

    await engine.handle(
        NewTask(
            name=name,
            prompt="Fix the bug",
            goal=TaskGoal.PR,
            location="/repo",
            key=None,
            repeat=False,
            source=TaskSource.FILE,
        )
    )
    await engine.handle(SessionWorking(session_id=sid))
    await engine.handle(
        PRCreated(session_id=sid, pr_url="https://github.com/org/repo/pull/1")
    )

    # Remove the task while the session is active.
    await engine.handle(RemoveTask(name=name))

    # Task is still visible (session is running), no terminate yet.
    status = await engine.status()
    assert name in status.tasks
    assert isinstance(status.tasks[name].phase, Active)

    # PR closes, triggering the normal termination cycle.
    await engine.handle(PRClosed(pr_url="https://github.com/org/repo/pull/1"))
    status = await engine.status()
    assert isinstance(status.tasks[name].phase, Active)

    await engine.handle(SessionTerminated(session_id=sid))
    status = await engine.status()
    assert name not in status.tasks

    assert calls == [
        Create(prompt=PR_PROMPT, location="/repo", result=sid),
        Terminate(session_id=sid),
    ]


@pytest.mark.asyncio
async def test_update_non_repeating_task_has_no_effect() -> None:
    """Updating a non-repeat task while running doesn't matter — it closes after completion."""
    engine, calls = _make_engine()
    await engine.enable()

    name = TaskName("task")
    sid = SessionID("session-0")

    await engine.handle(
        NewTask(
            name=name,
            prompt="v1",
            goal=TaskGoal.PR,
            location="/repo",
            key=None,
            repeat=False,
            source=TaskSource.FILE,
        )
    )

    # Update goal to ISSUE while running.
    await engine.handle(
        NewTask(
            name=name,
            prompt="v2",
            goal=TaskGoal.ISSUE,
            location="/repo",
            key=None,
            repeat=False,
            source=TaskSource.FILE,
        )
    )

    # Session's goal is still PR.
    phase = (await engine.status()).tasks[name].phase
    assert isinstance(phase, Initializing)
    assert phase.session.goal == TaskGoal.PR

    # PR lifecycle completes — session goal is PR so PRClosed triggers terminate.
    await engine.handle(SessionWorking(session_id=sid))
    await engine.handle(
        PRCreated(session_id=sid, pr_url="https://github.com/org/repo/pull/1")
    )
    await engine.handle(SessionIdle(session_id=sid))
    await engine.handle(PRClosed(pr_url="https://github.com/org/repo/pull/1"))
    await engine.handle(SessionTerminated(session_id=sid))

    # Non-repeat → Closed. The goal update is lost.
    status = await engine.status()
    assert isinstance(status.tasks[name].phase, Closed)


@pytest.mark.asyncio
async def test_update_repeating_task_takes_effect_on_next_session() -> None:
    """Updating a repeat task while running takes effect on the next session."""
    engine, calls = _make_engine()
    await engine.enable()

    name = TaskName("task")
    sid0 = SessionID("session-0")
    sid1 = SessionID("session-1")

    await engine.handle(
        NewTask(
            name=name,
            prompt="v1",
            goal=TaskGoal.PR,
            location="/repo",
            key=None,
            repeat=True,
            source=TaskSource.FILE,
        )
    )

    # Update goal to ISSUE while running.
    await engine.handle(
        NewTask(
            name=name,
            prompt="v2",
            goal=TaskGoal.ISSUE,
            location="/repo",
            key=None,
            repeat=True,
            source=TaskSource.FILE,
        )
    )

    # Session's goal is still PR.
    phase = (await engine.status()).tasks[name].phase
    assert isinstance(phase, Initializing)
    assert phase.session.goal == TaskGoal.PR

    # PR lifecycle completes.
    await engine.handle(SessionWorking(session_id=sid0))
    await engine.handle(
        PRCreated(session_id=sid0, pr_url="https://github.com/org/repo/pull/1")
    )
    await engine.handle(SessionIdle(session_id=sid0))
    await engine.handle(PRClosed(pr_url="https://github.com/org/repo/pull/1"))
    await engine.handle(SessionTerminated(session_id=sid0))

    # Repeat → relaunches. New session picks up the updated ISSUE goal.
    status = await engine.status()
    phase = status.tasks[name].phase
    assert isinstance(phase, Initializing)
    assert phase.session.session_id == sid1
    assert phase.session.goal == TaskGoal.ISSUE


@pytest.mark.asyncio
async def test_update_non_repeating_to_repeating_reoccurs() -> None:
    """Changing repeat from False to True while running causes the task to reoccur."""
    engine, calls = _make_engine()
    await engine.enable()

    name = TaskName("task")
    sid0 = SessionID("session-0")
    sid1 = SessionID("session-1")

    await engine.handle(
        NewTask(
            name=name,
            prompt="v1",
            goal=TaskGoal.PR,
            location="/repo",
            key=None,
            repeat=False,
            source=TaskSource.FILE,
        )
    )

    # Change to repeating + new goal while running.
    await engine.handle(
        NewTask(
            name=name,
            prompt="v2",
            goal=TaskGoal.ISSUE,
            location="/repo",
            key=None,
            repeat=True,
            source=TaskSource.FILE,
        )
    )

    # PR lifecycle completes with original PR goal.
    await engine.handle(SessionWorking(session_id=sid0))
    await engine.handle(
        PRCreated(session_id=sid0, pr_url="https://github.com/org/repo/pull/1")
    )
    await engine.handle(SessionIdle(session_id=sid0))
    await engine.handle(PRClosed(pr_url="https://github.com/org/repo/pull/1"))
    await engine.handle(SessionTerminated(session_id=sid0))

    # Now repeat=True → relaunches with updated ISSUE goal.
    status = await engine.status()
    phase = status.tasks[name].phase
    assert isinstance(phase, Initializing)
    assert phase.session.session_id == sid1
    assert phase.session.goal == TaskGoal.ISSUE


@pytest.mark.asyncio
async def test_update_repeating_to_non_repeating_closes() -> None:
    """Changing repeat from True to False while running causes the task to close."""
    engine, calls = _make_engine()
    await engine.enable()

    name = TaskName("task")
    sid = SessionID("session-0")

    await engine.handle(
        NewTask(
            name=name,
            prompt="v1",
            goal=TaskGoal.PR,
            location="/repo",
            key=None,
            repeat=True,
            source=TaskSource.FILE,
        )
    )

    # Change to non-repeating while running.
    await engine.handle(
        NewTask(
            name=name,
            prompt="v1",
            goal=TaskGoal.PR,
            location="/repo",
            key=None,
            repeat=False,
            source=TaskSource.FILE,
        )
    )

    # PR lifecycle completes.
    await engine.handle(SessionWorking(session_id=sid))
    await engine.handle(
        PRCreated(session_id=sid, pr_url="https://github.com/org/repo/pull/1")
    )
    await engine.handle(SessionIdle(session_id=sid))
    await engine.handle(PRClosed(pr_url="https://github.com/org/repo/pull/1"))
    await engine.handle(SessionTerminated(session_id=sid))

    # Now repeat=False → Closed, does not relaunch.
    status = await engine.status()
    assert isinstance(status.tasks[name].phase, Closed)
