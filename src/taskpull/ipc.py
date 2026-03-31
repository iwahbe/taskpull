from __future__ import annotations

import asyncio
import json
import logging
import socket
from typing import Any, Awaitable, Callable

log = logging.getLogger(__name__)

Handler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


async def run_ipc_server(
    port: int,
    handler: Handler,
    shutdown_event: asyncio.Event,
) -> None:
    async def _on_connect(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=10)
            if not line:
                return
            request = json.loads(line)
            response = await handler(request)
            writer.write(json.dumps(response).encode() + b"\n")
            await writer.drain()
        except Exception:
            log.exception("IPC handler error")
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(_on_connect, "127.0.0.1", port)
    async with server:
        await shutdown_event.wait()


def send_command(
    host: str, port: int, command: str, timeout: float = 10, **kwargs: Any
) -> dict[str, Any]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))
        payload = {"command": command, **kwargs}
        sock.sendall(json.dumps(payload).encode() + b"\n")
        data = b""
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            data += chunk
            if b"\n" in data:
                break
        return json.loads(data.strip())
    finally:
        sock.close()
