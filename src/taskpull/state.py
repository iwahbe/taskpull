from __future__ import annotations

import enum
import json
import tempfile
from dataclasses import asdict, dataclass, fields
from pathlib import Path


class TaskStatus(enum.Enum):
    IDLE = "idle"
    ACTIVE = "active"
    DONE = "done"


@dataclass
class TaskState:
    status: TaskStatus = TaskStatus.IDLE
    session_id: str | None = None
    session_name: str | None = None
    pr_number: int | None = None
    worktree: str | None = None
    repo: str | None = None
    run_count: int = 0
    exhausted: bool = False
    pr_draft: bool = False
    activity: str | None = None
    last_launched_at: int = 0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["status"] = self.status.value
        return d

    @classmethod
    def from_dict(cls, d: dict) -> TaskState:
        d = dict(d)
        raw_status = d.get("status", "idle")
        if raw_status == "pr_open":
            raw_status = "active"
        d["status"] = TaskStatus(raw_status)
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
