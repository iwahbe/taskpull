"""HTTP server providing hook notification and MCP endpoints for Docker containers."""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import json
import logging
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Awaitable, Callable

import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

log = logging.getLogger(__name__)

Handler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]

_current_task_id: contextvars.ContextVar[str] = contextvars.ContextVar("task_id")


def _build_mcp(handler: Handler) -> FastMCP:
    mcp = FastMCP("taskpull", stateless_http=True)

    @mcp.tool()
    async def task_exhausted() -> str:
        """Signal that this task has no work to do because the task is already completed.

        Call this ONLY when there is nothing to do — the work described by
        the task has already been done or is otherwise unnecessary.  Do NOT
        call this when you have finished working on a PR; just let the
        session end normally in that case.

        Calling this tool will terminate the current session.
        """
        task_id = _current_task_id.get()
        log.info("MCP task_exhausted called for task %s", task_id)
        response = await handler({"command": "task_exhausted", "task_id": task_id})
        log.info(
            "MCP task_exhausted handler returned for task %s: %s",
            task_id,
            response,
        )
        if response.get("status") == "ok":
            return "Task marked as exhausted. This session will be terminated."
        return f"Error: {response.get('message', 'unknown error')}"

    return mcp


async def _hook_notify(request: Request) -> Response:
    handler: Handler = request.app.state.ipc_handler
    task_id = request.path_params["task_id"]

    try:
        body = await request.json()
    except (json.JSONDecodeError, ValueError):
        return JSONResponse(
            {"status": "error", "message": "invalid JSON"}, status_code=400
        )

    event_name = body.get("hook_event_name", "")
    session_id = body.get("session_id", "")
    timestamp = datetime.now(timezone.utc).isoformat()

    event: dict[str, str] | None = None
    if event_name == "PreToolUse":
        event = {"type": "activity", "activity": "active", "timestamp": timestamp}
    elif event_name in ("Stop", "PostToolUseFailure"):
        event = {"type": "activity", "activity": "idle", "timestamp": timestamp}
    elif event_name == "SessionStart":
        event = {
            "type": "session_start",
            "session_id": session_id,
            "timestamp": timestamp,
        }
    elif event_name == "SetupFailed":
        event = {"type": "setup_failed", "timestamp": timestamp}

    if event is None:
        return JSONResponse({"status": "ok", "message": "ignored"})

    result = await handler(
        {"command": "notify_event", "task_id": task_id, "event": event}
    )
    return JSONResponse(result)


class _McpEndpoint:
    """ASGI app that sets the task_id context var before delegating to the MCP session manager."""

    def __init__(self, session_manager: StreamableHTTPSessionManager) -> None:
        self._session_manager = session_manager

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        request = Request(scope, receive)
        task_id = request.path_params["task_id"]
        _current_task_id.set(task_id)
        await self._session_manager.handle_request(scope, receive, send)


def _build_app(handler: Handler) -> Starlette:
    mcp = _build_mcp(handler)
    session_manager = StreamableHTTPSessionManager(
        app=mcp._mcp_server,
        stateless=True,
    )

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        async with session_manager.run():
            yield

    app = Starlette(
        routes=[
            Route("/hooks/{task_id}/notify", _hook_notify, methods=["POST"]),
            Route("/mcp/{task_id}", _McpEndpoint(session_manager)),
        ],
        lifespan=lifespan,
    )
    app.state.ipc_handler = handler
    return app


async def run_http_server(
    port: int,
    handler: Handler,
    shutdown_event: asyncio.Event,
) -> None:
    app = _build_app(handler)
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="warning")
    server = uvicorn.Server(config)

    serve_task = asyncio.create_task(server.serve())
    try:
        await shutdown_event.wait()
    finally:
        server.should_exit = True
        await serve_task
