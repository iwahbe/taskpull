"""GitHub watcher: poll GitHub for PR/issue state changes.

The watcher is event-driven.  It schedules two recurring wake-ups through
the engine's `Waker` interface, distinguished by reserved task names:

* `SYNC_TASK` — refresh the set of tracked URLs from an injected
  `StatusProvider` (the engine).
* `POLL_TASK` — query GitHub for the current state of each tracked URL
  and emit engine events when it changes.

All timing lives in the `Waker` which the caller constructs; the watcher
never sleeps directly.  Because the waker persists its deadlines and the
watcher persists the last-observed state of each URL, restarting the
daemon does not cause a flood of GitHub requests — the next poll fires
when the old deadline expires, and only transitions relative to the
persisted state are emitted as events.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Protocol

import httpx
from pydantic import BaseModel

from taskpull.engine import (
    Active,
    Idle,
    Initializing,
    Paused,
    Status,
)
from taskpull.engine_events import (
    CIStatus,
    CiInfo,
    EngineEvent,
    IssueClosed,
    PRClosed,
    TaskName,
    WakeTask,
)
from taskpull.state_manager import StateFactory
from taskpull.waker import Waker

log = logging.getLogger(__name__)


SYNC_TASK = TaskName("__gh_watcher_sync__")
POLL_TASK = TaskName("__gh_watcher_poll__")

SYNC_INTERVAL = 10.0
POLL_INTERVAL = 200.0


# -- Status provider ----------------------------------------------------------


class StatusProvider(Protocol):
    async def status(self) -> Status: ...


# -- Persisted per-URL state --------------------------------------------------


class _PRState(BaseModel):
    closed: bool
    ci: CiInfo


class _IssueState(BaseModel):
    closed: bool


class _TrackedPR(BaseModel):
    state: _PRState | None = None


class _TrackedIssue(BaseModel):
    state: _IssueState | None = None


class _WatcherState(BaseModel):
    prs: dict[str, _TrackedPR] = {}
    issues: dict[str, _TrackedIssue] = {}


# -- URL + CI parsing ---------------------------------------------------------


_CI_STATE_MAP: dict[str, CiInfo] = {
    "success": CiInfo.PASS,
    "failure": CiInfo.FAIL,
    "error": CiInfo.FAIL,
    "pending": CiInfo.PENDING,
}


@dataclass(frozen=True)
class _ParsedUrl:
    owner: str
    repo: str
    number: str


def _parse_html_url(url: str, expected_kind: str) -> _ParsedUrl | None:
    parts = url.removeprefix("https://").removeprefix("http://").split("/")
    if len(parts) != 5:
        return None
    host, owner, repo, kind, number = parts
    if host != "github.com" or kind != expected_kind:
        return None
    if not number.isdigit():
        return None
    return _ParsedUrl(owner=owner, repo=repo, number=number)


# -- Watcher ------------------------------------------------------------------


class GHWatcher:
    """Watches GitHub for state changes on PRs and issues tracked by the engine.

    The watcher never sleeps directly: it asks its `Waker` to schedule
    future wake-ups keyed by reserved task names (`SYNC_TASK`, `POLL_TASK`)
    and processes the resulting `WakeTask` events through `handle()`.
    Tests can drive wake-ups manually without any real waits.

    Last-observed state for every tracked URL is persisted via the injected
    `StateFactory`, so restarting the daemon does not re-emit `PRClosed` /
    `IssueClosed` for URLs that were already closed before.
    """

    def __init__(
        self,
        status_provider: StatusProvider,
        http_client: httpx.AsyncClient,
        gh_token: str,
        waker: Waker,
        engine_queue: asyncio.Queue[EngineEvent],
        state_factory: StateFactory,
        sync_interval: float = SYNC_INTERVAL,
        poll_interval: float = POLL_INTERVAL,
    ) -> None:
        self._status = status_provider
        self._http = http_client
        self._token = gh_token
        self._waker = waker
        self._queue = engine_queue
        self._state_manager = state_factory(_WatcherState)
        self._sync_interval = sync_interval
        self._poll_interval = poll_interval
        self._prs: dict[str, _TrackedPR] = {}
        self._issues: dict[str, _TrackedIssue] = {}
        self._loaded = False

    async def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        state = await self._state_manager.load()
        if state is not None:
            self._prs = dict(state.prs)
            self._issues = dict(state.issues)

    async def enable(self) -> None:
        """Prime the waker with an initial sync.

        Sync is scheduled immediately so the watcher starts from a fresh
        URL set.  The first poll is scheduled by `_sync` once there is at
        least one URL to watch.
        """
        await self._ensure_loaded()
        self._waker.schedule(0.0, SYNC_TASK)

    async def handle(self, event: WakeTask) -> None:
        await self._ensure_loaded()
        if event.name == SYNC_TASK:
            await self._sync()
            self._waker.schedule(self._sync_interval, SYNC_TASK)
        elif event.name == POLL_TASK:
            await self._poll()
            self._waker.schedule(self._poll_interval, POLL_TASK)

    async def _sync(self) -> None:
        status = await self._status.status()
        pr_urls: set[str] = set()
        issue_urls: set[str] = set()
        for task in status.tasks.values():
            phase = task.phase
            if not isinstance(phase, (Initializing, Idle, Active, Paused)):
                continue
            for pr in phase.session.prs:
                pr_urls.add(pr.url)
            for issue in phase.session.issues:
                issue_urls.add(issue.url)

        first_pr = not self._prs and pr_urls
        first_issue = not self._issues and issue_urls

        for url in pr_urls:
            self._prs.setdefault(url, _TrackedPR())
        for url in list(self._prs):
            if url not in pr_urls:
                del self._prs[url]

        for url in issue_urls:
            self._issues.setdefault(url, _TrackedIssue())
        for url in list(self._issues):
            if url not in issue_urls:
                del self._issues[url]

        await self._save()

        if first_pr or first_issue:
            self._waker.schedule(0.0, POLL_TASK)

    async def _poll(self) -> None:
        for url, tracked in list(self._prs.items()):
            new_state = await self._fetch_pr(url)
            if new_state is None:
                continue
            old_state = tracked.state
            if old_state is None or new_state.ci != old_state.ci:
                await self._queue.put(CIStatus(pr_url=url, info=new_state.ci))
            if new_state.closed and (old_state is None or not old_state.closed):
                await self._queue.put(PRClosed(pr_url=url))
            tracked.state = new_state

        for url, tracked in list(self._issues.items()):
            new_state = await self._fetch_issue(url)
            if new_state is None:
                continue
            old_state = tracked.state
            if new_state.closed and (old_state is None or not old_state.closed):
                await self._queue.put(IssueClosed(issue_url=url))
            tracked.state = new_state

        await self._save()

    async def _fetch_pr(self, url: str) -> _PRState | None:
        parsed = _parse_html_url(url, "pull")
        if parsed is None:
            return None
        try:
            resp = await self._http.get(
                f"https://api.github.com/repos/{parsed.owner}/{parsed.repo}/pulls/{parsed.number}",
                headers=self._auth(),
            )
        except httpx.HTTPError as e:
            log.warning("failed to fetch PR %s: %s", url, e)
            return None
        if resp.status_code != 200:
            log.info("PR %s fetch returned %s", url, resp.status_code)
            return None
        data = resp.json()
        closed = data.get("state") == "closed"
        sha = data.get("head", {}).get("sha")
        ci = CiInfo.UNKNOWN
        if sha:
            try:
                ci_resp = await self._http.get(
                    f"https://api.github.com/repos/{parsed.owner}/{parsed.repo}/commits/{sha}/status",
                    headers=self._auth(),
                )
            except httpx.HTTPError as e:
                log.warning("failed to fetch CI status for %s: %s", url, e)
                ci_resp = None
            if ci_resp is not None and ci_resp.status_code == 200:
                ci_data = ci_resp.json()
                if ci_data.get("total_count", len(ci_data.get("statuses", []))) == 0:
                    ci = CiInfo.NONE
                else:
                    ci = _CI_STATE_MAP.get(ci_data.get("state", ""), CiInfo.UNKNOWN)
        return _PRState(closed=closed, ci=ci)

    async def _fetch_issue(self, url: str) -> _IssueState | None:
        parsed = _parse_html_url(url, "issues")
        if parsed is None:
            return None
        try:
            resp = await self._http.get(
                f"https://api.github.com/repos/{parsed.owner}/{parsed.repo}/issues/{parsed.number}",
                headers=self._auth(),
            )
        except httpx.HTTPError as e:
            log.warning("failed to fetch issue %s: %s", url, e)
            return None
        if resp.status_code != 200:
            log.info("issue %s fetch returned %s", url, resp.status_code)
            return None
        data = resp.json()
        return _IssueState(closed=data.get("state") == "closed")

    def _auth(self) -> dict[str, str]:
        return {
            "authorization": f"Bearer {self._token}",
            "accept": "application/vnd.github+json",
        }

    async def _save(self) -> None:
        await self._state_manager.save(
            _WatcherState(prs=dict(self._prs), issues=dict(self._issues))
        )
