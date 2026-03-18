from __future__ import annotations

import enum
import json
import logging
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)


class EventType(enum.Enum):
    SESSION_START = "session_start"
    PR_CREATED = "pr_created"


@dataclass(frozen=True)
class SessionStartEvent:
    session_id: str
    timestamp: str


@dataclass(frozen=True)
class PrCreatedEvent:
    session_id: str
    pr_url: str
    pr_number: int
    timestamp: str


Event = SessionStartEvent | PrCreatedEvent


def write_hooks_config(
    worktree: Path,
    task_id: str,
    events_dir: Path,
    notify_script: Path,
) -> None:
    events_file = (events_dir / f"{task_id}.jsonl").resolve()

    config = {
        "hooks": {
            "SessionStart": [
                {
                    "matcher": "",
                    "hooks": [
                        {
                            "type": "command",
                            "command": f"python3 {notify_script} {events_file}",
                        }
                    ],
                }
            ],
            "PostToolUse": [
                {
                    "matcher": "Bash",
                    "hooks": [
                        {
                            "type": "command",
                            "command": f"python3 {notify_script} {events_file}",
                        }
                    ],
                }
            ],
        }
    }

    claude_dir = worktree / ".claude"
    claude_dir.mkdir(parents=True, exist_ok=True)
    settings_path = claude_dir / "settings.local.json"
    with open(settings_path, "w") as f:
        json.dump(config, f, indent=2)
        f.write("\n")


def read_events(events_dir: Path, task_id: str) -> list[Event]:
    events_file = events_dir / f"{task_id}.jsonl"
    if not events_file.exists():
        return []

    events: list[Event] = []
    with open(events_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            raw = json.loads(line)
            event_type = EventType(raw["type"])
            if event_type == EventType.SESSION_START:
                events.append(
                    SessionStartEvent(
                        session_id=raw["session_id"],
                        timestamp=raw["timestamp"],
                    )
                )
            elif event_type == EventType.PR_CREATED:
                events.append(
                    PrCreatedEvent(
                        session_id=raw["session_id"],
                        pr_url=raw["pr_url"],
                        pr_number=raw["pr_number"],
                        timestamp=raw["timestamp"],
                    )
                )
    return events


def clear_events(events_dir: Path, task_id: str) -> None:
    events_file = events_dir / f"{task_id}.jsonl"
    if events_file.exists():
        events_file.unlink()
