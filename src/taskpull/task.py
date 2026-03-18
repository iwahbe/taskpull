from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TaskFile:
    repo: str
    branch_prefix: str
    repeat: bool
    prompt: str


def parse_task(path: Path) -> TaskFile:
    text = path.read_text()
    lines = text.split("\n")

    if not lines or lines[0].strip() != "---":
        raise ValueError(f"{path}: missing opening '---' delimiter")

    # Find closing delimiter.
    close_idx = None
    for i, line in enumerate(lines[1:], start=1):
        if line.strip() == "---":
            close_idx = i
            break
    if close_idx is None:
        raise ValueError(f"{path}: missing closing '---' delimiter")

    # Parse frontmatter fields.
    fields: dict[str, str] = {}
    for line in lines[1:close_idx]:
        line = line.strip()
        if not line:
            continue
        key, _, value = line.partition(":")
        if not _:
            raise ValueError(f"{path}: malformed frontmatter line: {line!r}")
        fields[key.strip()] = value.strip()

    for required in ("repo", "branch_prefix"):
        if required not in fields:
            raise ValueError(f"{path}: missing required field '{required}'")

    prompt = "\n".join(lines[close_idx + 1 :]).strip()

    return TaskFile(
        repo=fields["repo"],
        branch_prefix=fields["branch_prefix"],
        repeat=fields.get("repeat", "false").lower() == "true",
        prompt=prompt,
    )


def task_id_from_path(path: Path) -> str:
    return path.stem


def discover_tasks(tasks_dir: Path) -> dict[str, TaskFile]:
    result: dict[str, TaskFile] = {}
    if not tasks_dir.is_dir():
        return result
    for path in sorted(tasks_dir.glob("*.md")):
        task_id = task_id_from_path(path)
        result[task_id] = parse_task(path)
    return result
