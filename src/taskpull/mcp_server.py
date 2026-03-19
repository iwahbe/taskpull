"""MCP server exposing a task_done tool for Claude Code to signal task completion."""

from __future__ import annotations

import argparse
import json
import socket
from pathlib import Path

from mcp.server.fastmcp import FastMCP


def _send_task_done(sock_path: Path, task_id: str) -> dict:
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(10)
    try:
        sock.connect(str(sock_path))
        payload = {"command": "task_done", "task_id": task_id}
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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sock", required=True)
    parser.add_argument("--task-id", required=True)
    args = parser.parse_args()

    sock_path = Path(args.sock)
    task_id = args.task_id

    mcp = FastMCP("taskpull")

    @mcp.tool()
    def task_done() -> str:
        """Signal that this repeating task has no more work to do."""
        try:
            response = _send_task_done(sock_path, task_id)
            if response.get("status") == "ok":
                return "Task marked as done."
            return f"Error: {response.get('message', 'unknown error')}"
        except Exception as e:
            return f"Failed to contact daemon: {e}"

    mcp.run()


if __name__ == "__main__":
    main()
