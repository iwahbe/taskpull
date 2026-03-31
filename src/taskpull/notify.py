#!/usr/bin/env python3
"""Hook script invoked by Claude Code to report events back to taskpull.

Usage (configured in .claude/settings.local.json):
    taskpull for-task notify --host 127.0.0.1 --port PORT --task-id TASK_ID

Reads Claude Code hook JSON from stdin, extracts relevant events,
and sends them to the daemon as notify_event IPC commands over TCP.
"""

import json
import re
import socket
import sys
from datetime import datetime, timezone


def _send_event(host: str, port: int, task_id: str, event: dict) -> None:
    payload = {"command": "notify_event", "task_id": task_id, "event": event}
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    try:
        sock.connect((host, port))
        sock.sendall(json.dumps(payload).encode() + b"\n")
        data = b""
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            data += chunk
            if b"\n" in data:
                break
    finally:
        sock.close()


def main(host: str, port: int, task_id: str) -> None:
    try:
        hook_input = json.load(sys.stdin)
    except (json.JSONDecodeError, EOFError):
        sys.exit(0)

    event_name = hook_input.get("hook_event_name", "")
    session_id = hook_input.get("session_id", "")
    timestamp = datetime.now(timezone.utc).isoformat()

    event = None

    if event_name == "PreToolUse":
        event = {
            "type": "activity",
            "activity": "active",
            "timestamp": timestamp,
        }

    elif event_name == "Stop":
        event = {
            "type": "activity",
            "activity": "idle",
            "timestamp": timestamp,
        }

    elif event_name == "SessionStart":
        event = {
            "type": "session_start",
            "session_id": session_id,
            "timestamp": timestamp,
        }

    elif event_name == "PostToolUse":
        tool_input = hook_input.get("tool_input", {})
        tool_response = hook_input.get("tool_response", {})
        command = tool_input.get("command", "")
        stdout = tool_response.get("stdout", "")

        if "gh pr create" in command or "gh pr create" in str(tool_input):
            pr_url = _extract_pr_url(stdout)
            pr_number = _extract_pr_number(stdout, pr_url)
            if pr_url and pr_number is not None:
                event = {
                    "type": "pr_created",
                    "session_id": session_id,
                    "pr_url": pr_url,
                    "pr_number": pr_number,
                    "timestamp": timestamp,
                }

    if event is not None:
        _send_event(host, port, task_id, event)


def _extract_pr_url(text: str) -> str | None:
    match = re.search(r"https://github\.com/[^\s]+/pull/\d+", text)
    return match.group(0) if match else None


def _extract_pr_number(text: str, pr_url: str | None) -> int | None:
    if pr_url:
        match = re.search(r"/pull/(\d+)", pr_url)
        if match:
            return int(match.group(1))
    match = re.search(r"#(\d+)", text)
    return int(match.group(1)) if match else None


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--task-id", required=True)
    args = parser.parse_args()
    main(args.host, args.port, args.task_id)
