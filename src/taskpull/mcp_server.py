"""MCP server exposing a task_exhausted tool for Claude Code to signal there is no work to do."""

from __future__ import annotations

import json
import socket

from mcp.server.fastmcp import FastMCP


def _send_task_exhausted(host: str, port: int, task_id: str) -> dict:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    try:
        sock.connect((host, port))
        payload = {"command": "task_exhausted", "task_id": task_id}
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


def main(host: str, port: int, task_id: str) -> None:
    mcp = FastMCP("taskpull")

    @mcp.tool()
    def task_exhausted() -> str:
        """Signal that this task has no work to do because the task is already completed.

        Call this ONLY when there is nothing to do — the work described by
        the task has already been done or is otherwise unnecessary.  Do NOT
        call this when you have finished working on a PR; just let the
        session end normally in that case.

        Calling this tool will terminate the current session.
        """
        try:
            response = _send_task_exhausted(host, port, task_id)
            if response.get("status") == "ok":
                return "Task marked as exhausted. This session will be terminated."
            return f"Error: {response.get('message', 'unknown error')}"
        except Exception as e:
            return f"Failed to contact daemon: {e}"

    mcp.run()


if __name__ == "__main__":
    import argparse as _argparse

    _parser = _argparse.ArgumentParser()
    _parser.add_argument("--host", required=True)
    _parser.add_argument("--port", required=True, type=int)
    _parser.add_argument("--task-id", required=True)
    _args = _parser.parse_args()
    main(_args.host, _args.port, _args.task_id)
