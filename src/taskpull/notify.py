#!/usr/bin/env python3
"""Hook script invoked by Claude Code to report events back to taskpull.

Usage (configured in .claude/settings.local.json):
    python3 /path/to/notify.py /path/to/events/task-id.jsonl

Reads Claude Code hook JSON from stdin, extracts relevant events,
and appends them as JSONL to the specified events file.
"""

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit(0)

    events_file = Path(sys.argv[1])
    events_file.parent.mkdir(parents=True, exist_ok=True)

    try:
        hook_input = json.load(sys.stdin)
    except (json.JSONDecodeError, EOFError):
        sys.exit(0)

    event_name = hook_input.get("hook_event_name", "")
    session_id = hook_input.get("session_id", "")
    timestamp = datetime.now(timezone.utc).isoformat()

    event = None

    if event_name == "SessionStart":
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
        with open(events_file, "a") as f:
            f.write(json.dumps(event) + "\n")


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
    main()
