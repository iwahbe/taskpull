"""Session manager: owns Docker sessions, the GH-proxy HTTPS listener,
and the hook/MCP HTTP server.

This module is the boundary between the pure engine and the messy outside
world (Docker, GitHub, Claude).  It implements `SessionManager` by:

- translating `create / pause / resume / terminate` into docker operations;
- emitting `SessionWorking / SessionIdle / SessionPaused / SessionUnpaused
  / SessionTerminated / ExhaustTask` into an engine event queue;
- running a background poller that picks up container deaths missed by
  the hook path;
- on enter, unpausing exactly the sessions the manager itself paused on
  the previous exit — sessions a user paused stay paused;
- on exit, pausing every running session (blocking) so no events are
  missed while the daemon is down, then tearing down its servers.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import json
import logging
import os
import re
import secrets
from abc import ABC, abstractmethod
from pathlib import Path
from types import TracebackType
from typing import Any, AsyncIterator

import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from pydantic import BaseModel
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from taskpull.engine_events import (
    EngineEvent,
    ExhaustTask,
    SessionID,
    SessionIdle,
    SessionPaused,
    SessionTerminated,
    SessionUnpaused,
    SessionWorking,
)
from taskpull.gh_proxy_v2 import GitHubProxy, Permissions
from taskpull.session import (
    kill_session,
    launch_session,
    pause_session as _docker_pause,
    session_alive,
    session_exit_info,
    session_paused as _docker_is_paused,
    unpause_session as _docker_unpause,
)
from taskpull.state_manager import StateFactory, StateManager
from taskpull.workspace import (
    cleanup_workspace,
    clone_repo,
    is_repo_url,
    repo_url_to_owner_repo,
    resolve_local_path,
)

log = logging.getLogger(__name__)


DEFAULT_POLL_INTERVAL = 5.0


class SessionManager(ABC):
    @abstractmethod
    async def create(self, prompt: str, location: str) -> SessionID: ...

    @abstractmethod
    async def pause(self, session: SessionID) -> None: ...

    @abstractmethod
    async def resume(self, session: SessionID) -> None: ...

    @abstractmethod
    async def terminate(self, session: SessionID) -> None: ...


# -- State persisted across daemon restarts -----------------------------------


class _SessionRecord(BaseModel):
    session_id: SessionID
    location: str
    workspace: str
    owner_repo: str
    cloned: bool  # True when `workspace` is owned by us and may be deleted


class _SessionManagerState(BaseModel):
    active: dict[SessionID, _SessionRecord] = {}
    paused_on_exit: list[SessionID] = []


# -- Helpers ------------------------------------------------------------------


_NON_SLUG_CHAR = re.compile(r"[^a-zA-Z0-9]+")


def _sanitize_slug(raw: str) -> str:
    cleaned = _NON_SLUG_CHAR.sub("-", raw).strip("-").lower()
    return cleaned[:32] or "session"


def _slug_from_location(location: str) -> str:
    if is_repo_url(location):
        return _sanitize_slug(repo_url_to_owner_repo(location).replace("/", "-"))
    return _sanitize_slug(resolve_local_path(location).name)


async def _git_remote_url(repo: Path) -> str:
    proc = await asyncio.create_subprocess_exec(
        "git",
        "-C",
        str(repo),
        "remote",
        "get-url",
        "origin",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()
    return stdout.decode().strip()


def _write_hook_settings(
    workspace: Path, session_id: SessionID, http_port: int
) -> Path:
    """Write `.claude/settings.local.json` + `.claude/mcp.json` into the
    workspace for the container to pick up.  The URL path embeds
    ``session_id`` so the HTTP server can correlate callbacks back to a
    session without parsing the body."""
    base = f"http://host.docker.internal:{http_port}"
    notify_cmd = (
        f"curl -s --max-time 10 -X POST"
        f" -H 'Content-Type: application/json'"
        f" -d @- {base}/hooks/{session_id}/notify"
    )
    hook_entry = {
        "matcher": "",
        "hooks": [{"type": "command", "command": notify_cmd}],
    }
    settings = {
        "hooks": {
            "SessionStart": [hook_entry],
            "Stop": [hook_entry],
        }
    }
    mcp = {
        "mcpServers": {
            "taskpull": {
                "type": "http",
                "url": f"{base}/mcp/{session_id}",
            }
        }
    }
    claude_dir = workspace / ".claude"
    claude_dir.mkdir(parents=True, exist_ok=True)
    (claude_dir / "settings.local.json").write_text(
        json.dumps(settings, indent=2) + "\n"
    )
    mcp_path = claude_dir / "mcp.json"
    mcp_path.write_text(json.dumps(mcp, indent=2) + "\n")
    return mcp_path


# ContextVar used by the MCP endpoint to pass the session ID from the
# ASGI entry point into the tool handler without threading it through
# FastMCP's handler plumbing.
_current_session_id: contextvars.ContextVar[SessionID] = contextvars.ContextVar(
    "taskpull_session_id"
)


# -- Real implementation ------------------------------------------------------


class TmuxDockerSessionManager(SessionManager):
    """Production session manager backed by Docker and tmux.

    Use as an async context manager:

        async with TmuxDockerSessionManager(...) as sessions:
            await sessions.create(prompt, location)
            ...

    Entering starts the hook/MCP HTTP server, the GH-proxy HTTPS listener,
    and a background container poller.  Exiting pauses every running
    session, persists which ones *we* paused, then shuts down servers.
    """

    def __init__(
        self,
        queue: asyncio.Queue[EngineEvent],
        state_factory: StateFactory,
        gh_proxy: GitHubProxy,
        workspace_dir: Path,
        docker_image: str,
        claude_token: str,
        gh_proxy_port: int,
        gh_proxy_cert_path: Path,
        gh_proxy_key_path: Path,
        ca_cert_path: Path,
        http_port: int,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
    ) -> None:
        self._queue = queue
        self._state_manager: StateManager[_SessionManagerState] = state_factory(
            _SessionManagerState
        )
        self._gh_proxy = gh_proxy
        self._workspace_dir = workspace_dir
        self._docker_image = docker_image
        self._claude_token = claude_token
        self._gh_proxy_port = gh_proxy_port
        self._gh_proxy_cert_path = gh_proxy_cert_path
        self._gh_proxy_key_path = gh_proxy_key_path
        self._ca_cert_path = ca_cert_path
        self._http_port = http_port
        self._poll_interval = poll_interval

        self._sessions: dict[SessionID, _SessionRecord] = {}
        # Last paused state observed by the poller; used to emit edge
        # events when docker-state changes out-of-band (e.g. user runs
        # `docker pause` directly).
        self._last_paused: dict[SessionID, bool] = {}

        self._shutdown = asyncio.Event()
        self._hook_server: uvicorn.Server | None = None
        self._proxy_server: uvicorn.Server | None = None
        self._hook_task: asyncio.Task[None] | None = None
        self._proxy_task: asyncio.Task[None] | None = None
        self._poll_task: asyncio.Task[None] | None = None

    # -- context manager protocol --

    async def __aenter__(self) -> TmuxDockerSessionManager:
        state = await self._state_manager.load()
        to_resume: list[SessionID] = []
        if state is not None:
            self._sessions = dict(state.active)
            to_resume = list(state.paused_on_exit)

        await self._start_servers()
        await self._resume_paused(to_resume)
        await self._save()
        self._poll_task = asyncio.create_task(self._poll_loop())
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        # Stop the poller first so it doesn't fight our pause-on-exit.
        self._shutdown.set()
        if self._poll_task is not None:
            self._poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poll_task

        paused_now: list[SessionID] = []
        for sid in list(self._sessions):
            try:
                if await session_alive(sid) and not await _docker_is_paused(sid):
                    await _docker_pause(sid)
                    paused_now.append(sid)
            except Exception:
                log.exception("  %s: pause on shutdown failed", sid)

        await self._state_manager.save(
            _SessionManagerState(
                active=dict(self._sessions),
                paused_on_exit=paused_now,
            )
        )

        if self._hook_server is not None:
            self._hook_server.should_exit = True
        if self._proxy_server is not None:
            self._proxy_server.should_exit = True
        if self._hook_task is not None:
            with contextlib.suppress(Exception):
                await self._hook_task
        if self._proxy_task is not None:
            with contextlib.suppress(Exception):
                await self._proxy_task

    # -- SessionManager interface --

    async def create(self, prompt: str, location: str) -> SessionID:
        slug = _slug_from_location(location)
        session_id = SessionID(f"taskpull-{slug}-{secrets.token_hex(4)}")

        workspace, cloned, owner_repo = await self._prepare_workspace(
            session_id, location
        )

        try:
            token, _certs = await self._gh_proxy.create_proxy_session(
                session_id, Permissions(allowed_repo=owner_repo)
            )
        except Exception:
            if cloned:
                await cleanup_workspace(workspace)
            raise

        try:
            mcp_config = _write_hook_settings(workspace, session_id, self._http_port)
            env: dict[str, str] = {
                "CLAUDE_CODE_OAUTH_TOKEN": self._claude_token,
                "GITHUB_TOKEN": token,
                "MISE_VERBOSE": "1",
            }
            anthropic_base_url = os.environ.get("ANTHROPIC_BASE_URL")
            if anthropic_base_url:
                env["ANTHROPIC_BASE_URL"] = anthropic_base_url

            await launch_session(
                name=session_id,
                workspace=workspace,
                prompt=prompt,
                run_count=1,
                task_id=session_id,
                mcp_config=mcp_config,
                docker_image=self._docker_image,
                env=env,
                ca_cert=self._ca_cert_path,
                gh_proxy_port=self._gh_proxy_port,
                http_port=self._http_port,
            )
        except Exception:
            await self._gh_proxy.forget(session_id)
            if cloned:
                await cleanup_workspace(workspace)
            raise

        record = _SessionRecord(
            session_id=session_id,
            location=location,
            workspace=str(workspace),
            owner_repo=owner_repo,
            cloned=cloned,
        )
        self._sessions[session_id] = record
        self._last_paused[session_id] = False
        await self._save()
        log.info("  %s: session created (%s)", session_id, owner_repo)
        return session_id

    async def pause(self, session: SessionID) -> None:
        if session not in self._sessions:
            return
        if await _docker_is_paused(session):
            return
        await _docker_pause(session)
        self._last_paused[session] = True
        await self._queue.put(SessionPaused(session_id=session))

    async def resume(self, session: SessionID) -> None:
        if session not in self._sessions:
            return
        if not await _docker_is_paused(session):
            return
        await _docker_unpause(session)
        self._last_paused[session] = False
        await self._queue.put(SessionUnpaused(session_id=session))

    async def terminate(self, session: SessionID) -> None:
        record = self._sessions.pop(session, None)
        self._last_paused.pop(session, None)
        if record is None:
            # Already terminated. Still emit so engine cleans up if it
            # somehow still has state.
            await self._queue.put(SessionTerminated(session_id=session))
            return

        with contextlib.suppress(Exception):
            await kill_session(session)
        with contextlib.suppress(Exception):
            await self._gh_proxy.forget(session)
        if record.cloned:
            with contextlib.suppress(Exception):
                await cleanup_workspace(Path(record.workspace))

        await self._save()
        await self._queue.put(SessionTerminated(session_id=session))

    # -- TUI helper --

    def container_name(self, session: SessionID) -> str:
        """Return the docker container name that hosts a session.

        Session IDs are used directly as container names, so this is the
        identity function; it exists so that callers (the TUI) can use a
        named, meaningful API rather than relying on the coincidence.
        """
        return str(session)

    # -- Background poller --

    async def _poll_loop(self) -> None:
        try:
            while not self._shutdown.is_set():
                try:
                    await self._poll_once()
                except Exception:
                    log.exception("session poller iteration failed")
                try:
                    await asyncio.wait_for(
                        self._shutdown.wait(), timeout=self._poll_interval
                    )
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            pass

    async def _poll_once(self) -> None:
        for sid in list(self._sessions):
            if not await session_alive(sid):
                # `session_exit_info` is called purely so the logs line
                # appears in the journal; we don't use the content here
                # because we don't have anywhere to attach it in the
                # engine's event model.
                exit_code, _ = await session_exit_info(sid)
                log.info(
                    "  %s: container no longer alive (exit=%s), emitting terminated",
                    sid,
                    exit_code,
                )
                await self.terminate(sid)
                continue

            is_paused = await _docker_is_paused(sid)
            was_paused = self._last_paused.get(sid, False)
            if is_paused and not was_paused:
                self._last_paused[sid] = True
                await self._queue.put(SessionPaused(session_id=sid))
            elif not is_paused and was_paused:
                self._last_paused[sid] = False
                await self._queue.put(SessionUnpaused(session_id=sid))

    # -- Internal helpers --

    async def _prepare_workspace(
        self, session_id: SessionID, location: str
    ) -> tuple[Path, bool, str]:
        if is_repo_url(location):
            workspace = await clone_repo(
                self._workspace_dir, location, str(session_id), 1
            )
            return workspace, True, repo_url_to_owner_repo(location)

        local = resolve_local_path(location)
        if not local.exists():
            raise RuntimeError(f"local path does not exist: {local}")
        remote = await _git_remote_url(local)
        if not remote:
            raise RuntimeError(f"local repo has no origin remote: {local}")
        return local, False, repo_url_to_owner_repo(remote)

    async def _resume_paused(self, to_resume: list[SessionID]) -> None:
        for sid in to_resume:
            record = self._sessions.get(sid)
            if record is None:
                continue
            try:
                if not await session_alive(sid):
                    log.info(
                        "  %s: container missing on resume, emitting terminated",
                        sid,
                    )
                    self._sessions.pop(sid, None)
                    self._last_paused.pop(sid, None)
                    with contextlib.suppress(Exception):
                        await self._gh_proxy.forget(sid)
                    if record.cloned:
                        with contextlib.suppress(Exception):
                            await cleanup_workspace(Path(record.workspace))
                    await self._queue.put(SessionTerminated(session_id=sid))
                    continue
                if await _docker_is_paused(sid):
                    await _docker_unpause(sid)
                    self._last_paused[sid] = False
                    await self._queue.put(SessionUnpaused(session_id=sid))
            except Exception:
                log.exception("  %s: resume on enter failed", sid)

    async def _save(self) -> None:
        await self._state_manager.save(
            _SessionManagerState(
                active=dict(self._sessions),
                paused_on_exit=[],
            )
        )

    # -- Servers --

    async def _start_servers(self) -> None:
        hook_cfg = uvicorn.Config(
            self._build_hook_app(),
            host="0.0.0.0",
            port=self._http_port,
            log_level="warning",
        )
        proxy_cfg = uvicorn.Config(
            self._build_proxy_app(),
            host="0.0.0.0",
            port=self._gh_proxy_port,
            ssl_keyfile=str(self._gh_proxy_key_path),
            ssl_certfile=str(self._gh_proxy_cert_path),
            log_level="warning",
        )
        self._hook_server = uvicorn.Server(hook_cfg)
        self._proxy_server = uvicorn.Server(proxy_cfg)
        self._hook_task = asyncio.create_task(self._hook_server.serve())
        self._proxy_task = asyncio.create_task(self._proxy_server.serve())

        # Wait for both servers to be listening so the first container
        # we launch can actually reach them.
        for server in (self._hook_server, self._proxy_server):
            while not server.started:
                await asyncio.sleep(0.05)

    def _build_hook_app(self) -> Starlette:
        queue = self._queue
        terminate = self.terminate

        mcp = FastMCP("taskpull", stateless_http=True)

        @mcp.tool()
        async def task_exhausted() -> str:
            """Signal that this task is already complete — there is no work to do.

            Call ONLY when there is nothing to do: the task described has
            already been done, or is otherwise unnecessary.  Do NOT call
            this when you have finished working on a PR or issue — let
            the session end normally in that case.

            Calling this tool will terminate the current session.
            """
            sid = _current_session_id.get()
            log.info("MCP task_exhausted for %s", sid)
            await queue.put(ExhaustTask(session_id=sid))
            return "Task marked as exhausted. This session will be terminated."

        session_manager = StreamableHTTPSessionManager(
            app=mcp._mcp_server,
            stateless=True,
        )

        @contextlib.asynccontextmanager
        async def lifespan(_app: Starlette) -> AsyncIterator[None]:
            async with session_manager.run():
                yield

        async def hook_notify(request: Request) -> Response:
            sid = SessionID(request.path_params["session_id"])
            try:
                body = await request.json()
            except json.JSONDecodeError, ValueError:
                return JSONResponse(
                    {"status": "error", "message": "invalid JSON"}, status_code=400
                )
            event_name = body.get("hook_event_name", "")
            if event_name == "SessionStart":
                await queue.put(SessionWorking(session_id=sid))
            elif event_name == "Stop":
                await queue.put(SessionIdle(session_id=sid))
            elif event_name == "SetupFailed":
                log.info("  %s: setup-failed hook received", sid)
                await terminate(sid)
            return JSONResponse({"status": "ok"})

        class _McpEndpoint:
            def __init__(self, inner: StreamableHTTPSessionManager) -> None:
                self._inner = inner

            async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
                req = Request(scope, receive)
                _current_session_id.set(SessionID(req.path_params["session_id"]))
                await self._inner.handle_request(scope, receive, send)

        return Starlette(
            routes=[
                Route("/hooks/{session_id}/notify", hook_notify, methods=["POST"]),
                Route("/mcp/{session_id}", _McpEndpoint(session_manager)),
            ],
            lifespan=lifespan,
        )

    def _build_proxy_app(self) -> Starlette:
        async def handle(request: Request) -> Response:
            return await self._gh_proxy.handle(request)

        return Starlette(
            routes=[
                Route(
                    "/{rest:path}",
                    handle,
                    methods=["GET", "POST", "HEAD", "PUT", "PATCH", "DELETE"],
                ),
            ]
        )
