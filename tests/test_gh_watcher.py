from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import httpx
import pytest

from taskpull.engine import (
    Active,
    Idle,
    Initializing,
    SessionInfo,
    Status,
    TaskView,
    _IssueInfo,
    _PRInfo,
)
from taskpull.engine_events import (
    CIStatus,
    CiInfo,
    EngineEvent,
    IssueClosed,
    PRClosed,
    SessionID,
    TaskGoal,
    TaskName,
    TaskSource,
    WakeTask,
)
from taskpull.gh_watcher import (
    POLL_TASK,
    SYNC_TASK,
    GHWatcher,
    _WatcherState,
)
from taskpull.state_manager import InMemoryStateManager
from taskpull.waker import Waker


def mem_factory(model: type) -> InMemoryStateManager:
    return InMemoryStateManager()


# -- Fakes --------------------------------------------------------------------


@dataclass
class FakeStatusProvider:
    _status: Status = field(default_factory=lambda: Status(tasks={}, enabled=True))

    def set(self, status: Status) -> None:
        self._status = status

    async def status(self) -> Status:
        return self._status


@dataclass
class FakeWaker(Waker):
    scheduled: list[tuple[float, TaskName]] = field(default_factory=list)

    def schedule(self, wait: float, task: TaskName) -> None:
        self.scheduled.append((wait, task))


@dataclass
class _Stub:
    """Routable HTTP response stub for `httpx.MockTransport`."""

    pr_state: str = "open"
    pr_sha: str = "sha-1"
    ci_state: str | None = "success"  # None → endpoint returns 0 statuses
    issue_state: str = "open"
    status_code_pr: int = 200
    status_code_issue: int = 200
    calls: list[str] = field(default_factory=list)


def _transport(stubs: dict[str, _Stub]) -> httpx.MockTransport:
    """Build a transport that routes `api.github.com` paths to `stubs`.

    Keys look like `"org/repo/pulls/1"` or `"org/repo/issues/1"`.  The
    CI-status endpoint (`.../commits/<sha>/status`) is routed through the
    matching PR stub by SHA.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        path = str(request.url).removeprefix("https://api.github.com/repos/")
        parts = path.split("/")
        # commits/<sha>/status
        if len(parts) == 5 and parts[2] == "commits" and parts[4] == "status":
            sha = parts[3]
            for stub in stubs.values():
                if stub.pr_sha == sha:
                    stub.calls.append(str(request.url))
                    if stub.ci_state is None:
                        return httpx.Response(200, json={"state": "", "statuses": []})
                    return httpx.Response(
                        200,
                        json={
                            "state": stub.ci_state,
                            "statuses": [{"state": stub.ci_state}],
                        },
                    )
            return httpx.Response(404)

        key = "/".join(parts[:4])
        stub = stubs.get(key)
        if stub is None:
            return httpx.Response(404)
        stub.calls.append(str(request.url))
        if parts[2] == "pulls":
            if stub.status_code_pr != 200:
                return httpx.Response(stub.status_code_pr)
            return httpx.Response(
                200,
                json={"state": stub.pr_state, "head": {"sha": stub.pr_sha}},
            )
        if parts[2] == "issues":
            if stub.status_code_issue != 200:
                return httpx.Response(stub.status_code_issue)
            return httpx.Response(200, json={"state": stub.issue_state})
        return httpx.Response(404)

    return httpx.MockTransport(handler)


# -- Helpers ------------------------------------------------------------------


def _task(
    name: str,
    pr_urls: list[str] = [],
    issue_urls: list[str] = [],
    phase_kind: str = "active",
    location: str = "/repo",
) -> TaskView:
    session = SessionInfo(
        session_id=SessionID(f"session-{name}"),
        prompt="p",
        goal=TaskGoal.PR,
        source=TaskSource.ADHOC,
        prs=[_PRInfo(url=u) for u in pr_urls],
        issues=[_IssueInfo(url=u) for u in issue_urls],
    )
    phase = {
        "active": Active(session=session),
        "idle": Idle(session=session),
        "initializing": Initializing(session=session),
    }[phase_kind]
    return TaskView(
        name=TaskName(name),
        phase=phase,
        goal=TaskGoal.PR,
        prompt="p",
        location=location,
        key=None,
        repeat=False,
        source=TaskSource.ADHOC,
    )


def _status(*tasks: TaskView) -> Status:
    return Status(tasks={t.name: t for t in tasks}, enabled=True)


async def _make_watcher(
    status: FakeStatusProvider | None = None,
    stubs: dict[str, _Stub] | None = None,
    waker: FakeWaker | None = None,
    state_factory=mem_factory,
):
    status = status or FakeStatusProvider()
    stubs = stubs or {}
    waker = waker or FakeWaker()
    client = httpx.AsyncClient(transport=_transport(stubs))
    q: asyncio.Queue[EngineEvent] = asyncio.Queue()
    watcher = GHWatcher(
        status_provider=status,
        http_client=client,
        gh_token="test-token",
        waker=waker,
        engine_queue=q,
        state_factory=state_factory,
    )
    return watcher, status, stubs, waker, q, client


def _drain(q: asyncio.Queue[EngineEvent]) -> list[EngineEvent]:
    out: list[EngineEvent] = []
    while not q.empty():
        out.append(q.get_nowait())
    return out


PR_URL = "https://github.com/org/repo/pull/1"
PR_URL_2 = "https://github.com/org/repo/pull/2"
ISSUE_URL = "https://github.com/org/repo/issues/1"


# -- Watcher tests ------------------------------------------------------------


@pytest.mark.asyncio
async def test_enable_schedules_immediate_sync() -> None:
    watcher, _, _, waker, _, client = await _make_watcher()
    try:
        await watcher.enable()
        assert waker.scheduled == [(0.0, SYNC_TASK)]
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_sync_populates_tracked_urls_and_schedules_initial_poll() -> None:
    status = FakeStatusProvider()
    status.set(_status(_task("t", pr_urls=[PR_URL], issue_urls=[ISSUE_URL])))
    watcher, _, _, waker, _, client = await _make_watcher(status=status)
    try:
        await watcher.enable()
        waker.scheduled.clear()
        await watcher.handle(WakeTask(name=SYNC_TASK))

        assert (10.0, SYNC_TASK) in waker.scheduled
        assert (0.0, POLL_TASK) in waker.scheduled
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_sync_without_urls_does_not_schedule_poll() -> None:
    watcher, _, _, waker, _, client = await _make_watcher()
    try:
        await watcher.enable()
        waker.scheduled.clear()
        await watcher.handle(WakeTask(name=SYNC_TASK))
        assert all(task != POLL_TASK for _, task in waker.scheduled)
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_poll_emits_ci_status_on_first_poll() -> None:
    status = FakeStatusProvider()
    status.set(_status(_task("t", pr_urls=[PR_URL])))
    stubs = {"org/repo/pulls/1": _Stub(ci_state="pending")}
    watcher, _, _, waker, q, client = await _make_watcher(status=status, stubs=stubs)
    try:
        await watcher.enable()
        await watcher.handle(WakeTask(name=SYNC_TASK))
        await watcher.handle(WakeTask(name=POLL_TASK))

        assert _drain(q) == [CIStatus(pr_url=PR_URL, info=CiInfo.PENDING)]
        assert (200.0, POLL_TASK) in waker.scheduled
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_poll_does_not_re_emit_unchanged_ci() -> None:
    status = FakeStatusProvider()
    status.set(_status(_task("t", pr_urls=[PR_URL])))
    stubs = {"org/repo/pulls/1": _Stub(ci_state="success")}
    watcher, _, _, _, q, client = await _make_watcher(status=status, stubs=stubs)
    try:
        await watcher.enable()
        await watcher.handle(WakeTask(name=SYNC_TASK))
        await watcher.handle(WakeTask(name=POLL_TASK))
        _drain(q)
        await watcher.handle(WakeTask(name=POLL_TASK))
        assert _drain(q) == []
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_poll_emits_ci_change() -> None:
    status = FakeStatusProvider()
    status.set(_status(_task("t", pr_urls=[PR_URL])))
    stubs = {"org/repo/pulls/1": _Stub(ci_state="pending")}
    watcher, _, _, _, q, client = await _make_watcher(status=status, stubs=stubs)
    try:
        await watcher.enable()
        await watcher.handle(WakeTask(name=SYNC_TASK))
        await watcher.handle(WakeTask(name=POLL_TASK))
        _drain(q)

        stubs["org/repo/pulls/1"].ci_state = "success"
        await watcher.handle(WakeTask(name=POLL_TASK))

        assert _drain(q) == [CIStatus(pr_url=PR_URL, info=CiInfo.PASS)]
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_poll_emits_pr_closed_once() -> None:
    status = FakeStatusProvider()
    status.set(_status(_task("t", pr_urls=[PR_URL])))
    stubs = {"org/repo/pulls/1": _Stub(ci_state="success")}
    watcher, _, _, _, q, client = await _make_watcher(status=status, stubs=stubs)
    try:
        await watcher.enable()
        await watcher.handle(WakeTask(name=SYNC_TASK))
        await watcher.handle(WakeTask(name=POLL_TASK))
        _drain(q)

        stubs["org/repo/pulls/1"].pr_state = "closed"
        await watcher.handle(WakeTask(name=POLL_TASK))
        assert _drain(q) == [PRClosed(pr_url=PR_URL)]

        await watcher.handle(WakeTask(name=POLL_TASK))
        assert _drain(q) == []
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_poll_emits_issue_closed_once() -> None:
    status = FakeStatusProvider()
    status.set(_status(_task("t", issue_urls=[ISSUE_URL])))
    stubs = {"org/repo/issues/1": _Stub()}
    watcher, _, _, _, q, client = await _make_watcher(status=status, stubs=stubs)
    try:
        await watcher.enable()
        await watcher.handle(WakeTask(name=SYNC_TASK))
        await watcher.handle(WakeTask(name=POLL_TASK))
        _drain(q)

        stubs["org/repo/issues/1"].issue_state = "closed"
        await watcher.handle(WakeTask(name=POLL_TASK))
        assert _drain(q) == [IssueClosed(issue_url=ISSUE_URL)]

        await watcher.handle(WakeTask(name=POLL_TASK))
        assert _drain(q) == []
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_poll_skips_urls_when_gh_returns_error() -> None:
    status = FakeStatusProvider()
    status.set(_status(_task("t", pr_urls=[PR_URL])))
    stubs = {"org/repo/pulls/1": _Stub(status_code_pr=500)}
    watcher, _, _, _, q, client = await _make_watcher(status=status, stubs=stubs)
    try:
        await watcher.enable()
        await watcher.handle(WakeTask(name=SYNC_TASK))
        await watcher.handle(WakeTask(name=POLL_TASK))
        assert _drain(q) == []
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_sync_prunes_removed_urls() -> None:
    status = FakeStatusProvider()
    status.set(_status(_task("t", pr_urls=[PR_URL, PR_URL_2])))
    stubs = {
        "org/repo/pulls/1": _Stub(pr_sha="sha-1", ci_state="success"),
        "org/repo/pulls/2": _Stub(pr_sha="sha-2", ci_state="success"),
    }
    watcher, _, _, _, q, client = await _make_watcher(status=status, stubs=stubs)
    try:
        await watcher.enable()
        await watcher.handle(WakeTask(name=SYNC_TASK))
        await watcher.handle(WakeTask(name=POLL_TASK))
        _drain(q)

        status.set(_status(_task("t", pr_urls=[PR_URL])))
        await watcher.handle(WakeTask(name=SYNC_TASK))

        stubs["org/repo/pulls/1"].calls.clear()
        stubs["org/repo/pulls/2"].calls.clear()
        await watcher.handle(WakeTask(name=POLL_TASK))
        assert stubs["org/repo/pulls/1"].calls, "active PR should be polled"
        assert stubs["org/repo/pulls/2"].calls == [], "pruned PR should not be polled"
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_state_persists_across_restart_no_reemit() -> None:
    shared_state = InMemoryStateManager()

    def factory(model: type) -> InMemoryStateManager:
        if model is _WatcherState:
            return shared_state
        return InMemoryStateManager()

    status = FakeStatusProvider()
    status.set(_status(_task("t", pr_urls=[PR_URL])))
    stubs = {"org/repo/pulls/1": _Stub(pr_state="closed", ci_state="success")}

    w1, _, _, _, q1, c1 = await _make_watcher(
        status=status, stubs=stubs, state_factory=factory
    )
    try:
        await w1.enable()
        await w1.handle(WakeTask(name=SYNC_TASK))
        await w1.handle(WakeTask(name=POLL_TASK))
        assert PRClosed(pr_url=PR_URL) in _drain(q1)
    finally:
        await c1.aclose()

    w2, _, _, _, q2, c2 = await _make_watcher(
        status=status, stubs=stubs, state_factory=factory
    )
    try:
        await w2.enable()
        await w2.handle(WakeTask(name=SYNC_TASK))
        await w2.handle(WakeTask(name=POLL_TASK))
        assert _drain(q2) == []
    finally:
        await c2.aclose()


@pytest.mark.asyncio
async def test_only_watches_running_tasks() -> None:
    from taskpull.engine import Closed, Waiting

    status = FakeStatusProvider()
    active = _task("t1", pr_urls=[PR_URL])
    closed_task = TaskView(
        name=TaskName("t2"),
        phase=Closed(),
        goal=TaskGoal.PR,
        prompt="p",
        location="/repo",
        key=None,
        repeat=False,
        source=TaskSource.ADHOC,
    )
    waiting_task = TaskView(
        name=TaskName("t3"),
        phase=Waiting(),
        goal=TaskGoal.PR,
        prompt="p",
        location="/repo",
        key=None,
        repeat=False,
        source=TaskSource.ADHOC,
    )
    status.set(
        Status(
            tasks={t.name: t for t in [active, closed_task, waiting_task]}, enabled=True
        )
    )
    stubs = {"org/repo/pulls/1": _Stub(ci_state=None)}
    watcher, _, _, _, _, client = await _make_watcher(status=status, stubs=stubs)
    try:
        await watcher.enable()
        await watcher.handle(WakeTask(name=SYNC_TASK))
        await watcher.handle(WakeTask(name=POLL_TASK))
        # Only the single active task's PR should have been fetched.
        assert any("pulls/1" in c for c in stubs["org/repo/pulls/1"].calls)
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_auth_header_sent() -> None:
    status = FakeStatusProvider()
    status.set(_status(_task("t", pr_urls=[PR_URL])))
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        if "commits" in str(request.url):
            return httpx.Response(200, json={"state": "", "statuses": []})
        return httpx.Response(200, json={"state": "open", "head": {"sha": "abc"}})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    q: asyncio.Queue[EngineEvent] = asyncio.Queue()
    watcher = GHWatcher(
        status_provider=status,
        http_client=client,
        gh_token="secret-token",
        waker=FakeWaker(),
        engine_queue=q,
        state_factory=mem_factory,
    )
    try:
        await watcher.enable()
        await watcher.handle(WakeTask(name=SYNC_TASK))
        await watcher.handle(WakeTask(name=POLL_TASK))
        assert captured
        for req in captured:
            assert req.headers["authorization"] == "Bearer secret-token"
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_non_github_urls_skipped() -> None:
    status = FakeStatusProvider()
    status.set(_status(_task("t", pr_urls=["https://gitlab.com/org/repo/pull/1"])))
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(404)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    q: asyncio.Queue[EngineEvent] = asyncio.Queue()
    watcher = GHWatcher(
        status_provider=status,
        http_client=client,
        gh_token="t",
        waker=FakeWaker(),
        engine_queue=q,
        state_factory=mem_factory,
    )
    try:
        await watcher.enable()
        await watcher.handle(WakeTask(name=SYNC_TASK))
        await watcher.handle(WakeTask(name=POLL_TASK))
        assert calls == []
        assert _drain(q) == []
    finally:
        await client.aclose()
