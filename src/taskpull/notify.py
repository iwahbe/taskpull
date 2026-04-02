#!/usr/bin/env python3
"""Hook script invoked by Claude Code to report events back to taskpull.

Usage (configured in .claude/settings.local.json):
    taskpull for-task notify --host 127.0.0.1 --port PORT --task-id TASK_ID

Reads Claude Code hook JSON from stdin, extracts relevant events,
and sends them to the daemon as notify_event IPC commands over TCP.
"""

import json
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

    if event is not None:
        _send_event(host, port, task_id, event)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--task-id", required=True)
    args = parser.parse_args()
    main(args.host, args.port, args.task_id)
