from __future__ import annotations

import asyncio
from typing import Any

import pytest

from taskpull.ipc import run_ipc_server, send_command


@pytest.mark.asyncio
async def test_ipc_round_trip():
    received: list[dict[str, Any]] = []

    async def handler(request: dict[str, Any]) -> dict[str, Any]:
        received.append(request)
        return {"status": "ok", "echo": request.get("command")}

    shutdown = asyncio.Event()
    server_task = asyncio.create_task(run_ipc_server(0, handler, shutdown))

    # Give the server a moment to bind; then find the port.
    await asyncio.sleep(0.05)

    # run_ipc_server binds on port 0 which doesn't help us find the port.
    # We'll use a known port instead.
    shutdown.set()
    await server_task

    # Use a fixed port for the actual test.
    shutdown = asyncio.Event()
    port = 19599
    server_task = asyncio.create_task(run_ipc_server(port, handler, shutdown))
    await asyncio.sleep(0.05)

    try:
        response = await asyncio.to_thread(
            send_command, "127.0.0.1", port, "ping", value="hello"
        )
        assert response == {"status": "ok", "echo": "ping"}
        assert received[-1] == {"command": "ping", "value": "hello"}
    finally:
        shutdown.set()
        await server_task
