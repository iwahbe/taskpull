from __future__ import annotations

import asyncio
import json
import logging
import socket
from pathlib import Path
from typing import Any, Awaitable, Callable

log = logging.getLogger(__name__)

Handler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


async def run_ipc_server(
    sock_path: Path,
    handler: Handler,
    shutdown_event: asyncio.Event,
) -> None:
    sock_path.unlink(missing_ok=True)

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

    server = await asyncio.start_unix_server(_on_connect, path=str(sock_path))
    async with server:
        await shutdown_event.wait()
    sock_path.unlink(missing_ok=True)


def send_command(sock_path: Path, command: str, timeout: float = 10) -> dict[str, Any]:
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect(str(sock_path))
        sock.sendall(json.dumps({"command": command}).encode() + b"\n")
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
