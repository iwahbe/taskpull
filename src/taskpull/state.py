from __future__ import annotations

import enum
import json
import tempfile
import time
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any


class TaskStatus(enum.Enum):
    IDLE = "idle"
    ACTIVE = "active"
    PAUSED = "paused"
    DONE = "done"
    BROKEN = "broken"


class TaskGoal(enum.Enum):
    PR = "pr"
    NONE = "none"
    ISSUE = "issue"


class CiStatus(enum.Enum):
    UNKNOWN = "unknown"
    NONE = "none"
    PASS = "pass"
    FAIL = "fail"
    PENDING = "pending"


@dataclass
class TaskState:
    status: TaskStatus = TaskStatus.IDLE
    session_id: str | None = None
    session_name: str | None = None
    pr_number: int | None = None
    pr_url: str | None = None
    workspace: str | None = None
    repo: str | None = None
    run_count: int = 0
    exhaust_count: int = 0
    pr_draft: bool = False
    pr_approved: bool | None = None
    pr_ci: CiStatus = CiStatus.UNKNOWN
    activity: str | None = None
    proxy_secret: str | None = None
    last_launched_at: int = 0
    error_message: str | None = None
    setup_failure_count: int = 0
    # When set, this is an ad-hoc task created via `taskpull new` and the
    # value is the task prompt.  None means the task is file-based (prompt
    # comes from the .md file).
    adhoc: str | None = None
    repo_lock: str | None = None
    goal: TaskGoal = TaskGoal.PR
    issues: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["status"] = self.status.value
        d["goal"] = self.goal.value
        d["pr_ci"] = self.pr_ci.value
        return d

    def exhaust_backoff(self, poll_interval: int) -> float:
        if self.exhaust_count <= 0:
            return 0
        multiplier = min(2**self.exhaust_count, 288)
        return multiplier * poll_interval

    def setup_retry_backoff(self, poll_interval: int) -> float:
        if self.setup_failure_count <= 1:
            return 0
        multiplier = min(2 ** (self.setup_failure_count - 1), 24)
        return multiplier * poll_interval

    def seconds_since_launch(self) -> float:
        if self.last_launched_at <= 0:
            return float("inf")
        return time.time() - self.last_launched_at

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> TaskState:
        d = dict(d)
        raw_status = d.get("status", "idle")
        if raw_status == "pr_open":
            raw_status = "active"
        d["status"] = TaskStatus(raw_status)
        d["goal"] = TaskGoal(d.get("goal", "pr"))
        d["pr_ci"] = CiStatus(d.get("pr_ci", "unknown"))
        # Migrate legacy exhausted bool → exhaust_count.
        if d.pop("exhausted", False) and "exhaust_count" not in d:
            d["exhaust_count"] = 1
        # Migrate legacy worktree → workspace.
        if "worktree" in d and "workspace" not in d:
            d["workspace"] = d.pop("worktree")
        else:
            d.pop("worktree", None)
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in d.items() if k in known})


def load_state(path: Path) -> dict[str, TaskState]:
    if not path.exists():
        return {}
    with open(path) as f:
        raw = json.load(f)
    return {k: TaskState.from_dict(v) for k, v in raw.items()}


def save_state(path: Path, state: dict[str, TaskState]) -> None:
    raw = {k: v.to_dict() for k, v in state.items()}
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".json")
    try:
        with open(fd, "w") as f:
            json.dump(raw, f, indent=2)
            f.write("\n")
        Path(tmp).rename(path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
