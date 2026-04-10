from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Discriminator, Tag

from taskpull.engine_events import (
    CIStatus,
    CiInfo,
    EngineEvent,
    ExhaustTask,
    IssueCreated,
    IssueClosed,
    NewTask,
    PRClosed,
    PRCreated,
    PauseSession,
    RemoveTask,
    ResumeSession,
    RestartSession,
    SessionID,
    SessionIdle,
    SessionPaused,
    SessionTerminated,
    SessionUnpaused,
    SessionWorking,
    TaskGoal,
    TaskName,
    TaskSource,
    WakeTask,
)
from taskpull.session_manager import SessionManager
from taskpull.state_manager import StateFactory
from taskpull.waker import Waker

logger = logging.getLogger(__name__)

PR_INSTRUCTIONS = """

When your work is ready, create a descriptively named branch, push it, and open a PR:
  git checkout -b your-descriptive-branch-name
  git push -u origin HEAD
  gh pr create --fill
"""

ISSUE_INSTRUCTIONS = """

When your work is ready, create a GitHub issue:
  gh issue create --title "Your descriptive title" --body "Issue body"
"""


def repeat_suffix(goal: TaskGoal) -> str:
    return f"""
If there is nothing left to do because the task is already completed, call
the task_exhausted MCP tool.  Do NOT call task_exhausted when you have
finished working on your {goal} — only call it when there is no work to do at all.
"""


SETUP_FAILURE_THRESHOLD = 3
EXHAUST_BACKOFF_CAP = 288
SETUP_BACKOFF_CAP = 24


# -- Session info (immutable snapshot of task config at launch time) --


class _PRInfo(BaseModel):
    url: str
    ci: CiInfo = CiInfo.UNKNOWN
    draft: bool = False
    approved: bool | None = None


class _IssueInfo(BaseModel):
    url: str


class SessionInfo(BaseModel):
    session_id: SessionID
    prompt: str
    goal: TaskGoal
    source: TaskSource
    prs: list[_PRInfo] = []
    issues: list[_IssueInfo] = []


# -- Phase types --


class Waiting(BaseModel):
    kind: Literal["waiting"] = "waiting"


class Initializing(BaseModel):
    kind: Literal["initializing"] = "initializing"
    session: SessionInfo


class Idle(BaseModel):
    kind: Literal["idle"] = "idle"
    session: SessionInfo


class Active(BaseModel):
    kind: Literal["active"] = "active"
    session: SessionInfo


class Paused(BaseModel):
    kind: Literal["paused"] = "paused"
    session: SessionInfo


class Closed(BaseModel):
    kind: Literal["closed"] = "closed"


Phase = Annotated[
    Union[
        Annotated[Waiting, Tag("waiting")],
        Annotated[Initializing, Tag("initializing")],
        Annotated[Idle, Tag("idle")],
        Annotated[Active, Tag("active")],
        Annotated[Paused, Tag("paused")],
        Annotated[Closed, Tag("closed")],
    ],
    Discriminator("kind"),
]


# -- Internal state models (Pydantic for persistence) --


class _TaskState(BaseModel):
    name: TaskName
    prompt: str
    goal: TaskGoal
    location: str
    key: str | None
    repeat: bool
    source: TaskSource

    phase: Phase = Waiting()
    removed: bool = False
    run_count: int = 0
    exhaust_count: int = 0
    setup_failure_count: int = 0
    last_launched_at: float = 0.0


class _EngineState(BaseModel):
    tasks: dict[TaskName, _TaskState] = {}


# -- Public view types (frozen dataclasses) --


@dataclass(frozen=True)
class PRView:
    url: str
    ci: CiInfo
    draft: bool
    approved: bool | None


@dataclass(frozen=True)
class IssueView:
    url: str


@dataclass(frozen=True)
class TaskView:
    """Read-only view of a task for external consumers."""

    name: TaskName
    phase: Phase
    goal: TaskGoal
    prompt: str
    location: str
    key: str | None
    repeat: bool
    source: TaskSource


@dataclass(frozen=True)
class Status:
    tasks: dict[TaskName, TaskView]
    enabled: bool


def _session_of(phase: Phase) -> SessionInfo | None:
    match phase:
        case (
            Initializing(session=s)
            | Idle(session=s)
            | Active(session=s)
            | Paused(session=s)
        ):
            return s
        case Waiting() | Closed():
            return None


def _session_id_of(phase: Phase) -> SessionID | None:
    s = _session_of(phase)
    return s.session_id if s is not None else None


def _is_running(phase: Phase) -> bool:
    return isinstance(phase, (Initializing, Idle, Active))


class Engine:
    def __init__(
        self,
        session_manager: SessionManager,
        state_factory: StateFactory,
        waker: Waker,
        poll_interval: float = 300.0,
    ) -> None:
        self._sessions = session_manager
        self._state_manager = state_factory(_EngineState)
        self._waker = waker
        self._poll_interval = poll_interval

        self._tasks: dict[TaskName, _TaskState] = {}
        self._session_to_task: dict[SessionID, TaskName] = {}
        self._enabled = False
        self._restored = False

    async def _ensure_restored(self) -> None:
        if self._restored:
            return
        self._restored = True
        state = await self._state_manager.load()
        if state is None:
            return
        self._tasks = dict(state.tasks)
        for ts in self._tasks.values():
            sid = _session_id_of(ts.phase)
            if sid is not None:
                self._session_to_task[sid] = ts.name

    async def status(self) -> Status:
        await self._ensure_restored()
        tasks = {}
        for name, ts in self._tasks.items():
            tasks[name] = TaskView(
                name=ts.name,
                phase=ts.phase,
                goal=ts.goal,
                prompt=ts.prompt,
                location=ts.location,
                key=ts.key,
                repeat=ts.repeat,
                source=ts.source,
            )
        return Status(tasks=tasks, enabled=self._enabled)

    async def enable(self) -> None:
        await self._ensure_restored()
        self._enabled = True
        await self._try_launch()
        await self._save()

    async def disable(self) -> None:
        await self._ensure_restored()
        self._enabled = False
        await self._save()

    async def handle(self, event: EngineEvent) -> None:
        await self._ensure_restored()
        match event:
            case NewTask():
                await self._handle_new_task(event)
            case RemoveTask():
                await self._handle_remove_task(event)
            case SessionPaused():
                self._handle_session_paused(event)
            case SessionUnpaused():
                self._handle_session_unpaused(event)
            case SessionWorking():
                self._handle_session_working(event)
            case SessionIdle():
                await self._handle_session_idle(event)
            case SessionTerminated():
                await self._handle_session_terminated(event)
            case PRCreated():
                self._handle_pr_created(event)
            case IssueCreated():
                self._handle_issue_created(event)
            case PRClosed():
                await self._handle_pr_closed(event)
            case IssueClosed():
                pass
            case CIStatus():
                self._handle_ci_status(event)
            case RestartSession():
                await self._handle_restart_session(event)
            case PauseSession():
                await self._handle_pause_session(event)
            case ResumeSession():
                await self._handle_resume_session(event)
            case ExhaustTask():
                await self._handle_exhaust_task(event)
            case WakeTask():
                await self._handle_wake_task(event)
        await self._save()

    # -- Event handlers --

    async def _handle_new_task(self, event: NewTask) -> None:
        existing = self._tasks.get(event.name)
        if existing is not None and _is_running(existing.phase):
            existing.prompt = event.prompt
            existing.goal = event.goal
            existing.location = event.location
            existing.key = event.key
            existing.repeat = event.repeat
            existing.source = event.source
            return
        self._tasks[event.name] = _TaskState(
            name=event.name,
            prompt=event.prompt,
            goal=event.goal,
            location=event.location,
            key=event.key,
            repeat=event.repeat,
            source=event.source,
        )
        await self._try_launch()

    async def _handle_remove_task(self, event: RemoveTask) -> None:
        ts = self._tasks.get(event.name)
        if ts is None:
            return
        sid = _session_id_of(ts.phase)
        if sid is not None:
            ts.removed = True
        else:
            del self._tasks[event.name]

    def _handle_session_paused(self, event: SessionPaused) -> None:
        ts = self._task_for_session(event.session_id)
        if ts is None:
            return
        session = _session_of(ts.phase)
        if session is None:
            return
        ts.phase = Paused(session=session)

    def _handle_session_unpaused(self, event: SessionUnpaused) -> None:
        ts = self._task_for_session(event.session_id)
        if ts is None:
            return
        session = _session_of(ts.phase)
        if session is None:
            return
        ts.phase = Active(session=session)

    def _handle_session_working(self, event: SessionWorking) -> None:
        ts = self._task_for_session(event.session_id)
        if ts is None:
            return
        session = _session_of(ts.phase)
        if session is None:
            return
        ts.phase = Active(session=session)

    async def _handle_session_idle(self, event: SessionIdle) -> None:
        ts = self._task_for_session(event.session_id)
        if ts is None:
            return
        session = _session_of(ts.phase)
        if session is None:
            return
        ts.phase = Idle(session=session)

        # If a session with goal == "issue" is idle and an issue has been created, that
        # session should be cleaned up: its goal has been reached.
        if session.goal == TaskGoal.ISSUE and len(session.issues) > 0:
            await self._sessions.terminate(event.session_id)

    async def _handle_session_terminated(self, event: SessionTerminated) -> None:
        ts = self._task_for_session(event.session_id)
        if ts is None:
            return
        self._session_to_task.pop(event.session_id, None)

        if ts.removed:
            del self._tasks[ts.name]
        else:
            match ts.phase:
                case Initializing():
                    # Never got past initializing — setup failure.
                    ts.setup_failure_count += 1
                    if ts.setup_failure_count >= SETUP_FAILURE_THRESHOLD:
                        ts.phase = Closed()
                    else:
                        ts.phase = Waiting()
                        self._schedule_backoff(ts)
                case _:
                    self._complete_task(ts)

        await self._try_launch()

    def _handle_pr_created(self, event: PRCreated) -> None:
        ts = self._task_for_session(event.session_id)
        if ts is None:
            return
        session = _session_of(ts.phase)
        if session is None:
            return
        session.prs.append(_PRInfo(url=event.pr_url))

    def _handle_issue_created(self, event: IssueCreated) -> None:
        ts = self._task_for_session(event.session_id)
        if ts is None:
            return
        session = _session_of(ts.phase)
        if session is None:
            return
        session.issues.append(_IssueInfo(url=event.issue_url))

    async def _handle_pr_closed(self, event: PRClosed) -> None:
        ts = self._task_for_pr(event.pr_url)
        if ts is None:
            return
        session = _session_of(ts.phase)
        if session is None:
            return
        if session.goal == TaskGoal.PR:
            await self._sessions.terminate(session.session_id)

    def _handle_ci_status(self, event: CIStatus) -> None:
        ts = self._task_for_pr(event.pr_url)
        if ts is None:
            return
        session = _session_of(ts.phase)
        if session is None:
            return
        for pr in session.prs:
            if pr.url == event.pr_url:
                pr.ci = event.info

    async def _handle_restart_session(self, event: RestartSession) -> None:
        ts = self._task_for_session(event.session_id)
        if ts is None:
            return
        await self._sessions.terminate(event.session_id)
        ts.phase = Waiting()
        ts.setup_failure_count = 0
        ts.exhaust_count = 0
        ts.last_launched_at = 0.0
        await self._try_launch()

    async def _handle_pause_session(self, event: PauseSession) -> None:
        ts = self._task_for_session(event.session_id)
        if ts is None:
            return
        if isinstance(ts.phase, (Paused, Closed)):
            return
        await self._sessions.pause(event.session_id)

    async def _handle_resume_session(self, event: ResumeSession) -> None:
        ts = self._task_for_session(event.session_id)
        if ts is None:
            return
        if not isinstance(ts.phase, Paused):
            return
        await self._sessions.resume(event.session_id)

    async def _handle_exhaust_task(self, event: ExhaustTask) -> None:
        ts = self._task_for_session(event.session_id)
        if ts is None:
            return
        await self._sessions.terminate(event.session_id)

        if ts.source == TaskSource.ADHOC and not ts.repeat:
            ts.phase = Closed()
        else:
            ts.exhaust_count += 1
            ts.phase = Waiting()
            ts.last_launched_at = time.time()
            self._schedule_backoff(ts)

    async def _handle_wake_task(self, event: WakeTask) -> None:
        await self._try_launch()

    # -- Internal helpers --

    def _task_for_session(self, session_id: SessionID) -> _TaskState | None:
        name = self._session_to_task.get(session_id)
        if name is None:
            return None
        return self._tasks.get(name)

    def _task_for_pr(self, pr_url: str) -> _TaskState | None:
        for ts in self._tasks.values():
            session = _session_of(ts.phase)
            if session is None:
                continue
            for pr in session.prs:
                if pr.url == pr_url:
                    return ts
        return None

    def _complete_task(self, ts: _TaskState) -> None:
        if ts.repeat:
            ts.exhaust_count = 0
            ts.phase = Waiting()
        else:
            ts.phase = Closed()

    def _lane_key(self, ts: _TaskState) -> tuple[str, str]:
        return (ts.location, ts.key if ts.key else ts.location)

    def _busy_lanes(self) -> set[tuple[str, str]]:
        lanes: set[tuple[str, str]] = set()
        for ts in self._tasks.values():
            if _is_running(ts.phase):
                lanes.add(self._lane_key(ts))
        return lanes

    def _exhaust_backoff(self, ts: _TaskState) -> float:
        if ts.exhaust_count <= 0:
            return 0.0
        multiplier = min(2**ts.exhaust_count, EXHAUST_BACKOFF_CAP)
        return multiplier * self._poll_interval

    def _setup_backoff(self, ts: _TaskState) -> float:
        if ts.setup_failure_count <= 1:
            return 0.0
        multiplier = min(2 ** (ts.setup_failure_count - 1), SETUP_BACKOFF_CAP)
        return multiplier * self._poll_interval

    def _backoff(self, ts: _TaskState) -> float:
        return max(self._exhaust_backoff(ts), self._setup_backoff(ts))

    def _schedule_backoff(self, ts: _TaskState) -> None:
        wait = self._backoff(ts)
        if wait > 0:
            self._waker.schedule(wait, ts.name)

    def _build_prompt(self, ts: _TaskState) -> str:
        prompt = ts.prompt
        if ts.goal == TaskGoal.PR:
            prompt += PR_INSTRUCTIONS
        elif ts.goal == TaskGoal.ISSUE:
            prompt += ISSUE_INSTRUCTIONS
        if ts.repeat:
            prompt += repeat_suffix(ts.goal)
        return prompt

    async def _try_launch(self) -> None:
        if not self._enabled:
            return
        busy = self._busy_lanes()
        lane_candidates: dict[tuple[str, str], list[_TaskState]] = {}
        for ts in self._tasks.values():
            if not isinstance(ts.phase, Waiting) or ts.removed:
                continue
            backoff = self._backoff(ts)
            if backoff > 0 and (time.time() - ts.last_launched_at) < backoff:
                continue
            lane = self._lane_key(ts)
            if lane in busy:
                continue
            lane_candidates.setdefault(lane, []).append(ts)

        for lane, candidates in lane_candidates.items():
            candidates.sort(key=lambda c: c.last_launched_at)
            ts = candidates[0]
            prompt = self._build_prompt(ts)
            session_id = await self._sessions.create(prompt, ts.location)
            session = SessionInfo(
                session_id=session_id,
                prompt=ts.prompt,
                goal=ts.goal,
                source=ts.source,
            )
            ts.phase = Initializing(session=session)
            ts.run_count += 1
            ts.last_launched_at = time.time()
            self._session_to_task[session_id] = ts.name
            busy.add(lane)

    async def _save(self) -> None:
        await self._state_manager.save(_EngineState(tasks=dict(self._tasks)))
