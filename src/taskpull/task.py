from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from .state import TaskGoal
from .workspace import is_repo_url, resolve_local_path

if TYPE_CHECKING:
    from .state import TaskState


@dataclass(frozen=True)
class TaskFile:
    repo: str
    repeat: bool
    prompt: str
    repo_lock: str | None = None
    goal: TaskGoal = TaskGoal.PR

    @property
    def lane_key(self) -> tuple[str, str]:
        return (self.repo, self.repo_lock if self.repo_lock else self.repo)


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

    if "repo" not in fields:
        raise ValueError(f"{path}: missing required field 'repo'")

    prompt = "\n".join(lines[close_idx + 1 :]).strip()

    goal_str = fields.get("goal", "pr")
    try:
        goal = TaskGoal(goal_str)
    except ValueError:
        valid = ", ".join(g.value for g in TaskGoal)
        raise ValueError(f"{path}: invalid goal '{goal_str}', must be one of: {valid}")

    return TaskFile(
        repo=fields["repo"],
        repeat=fields.get("repeat", "false").lower() == "true",
        prompt=prompt,
        repo_lock=fields.get("repo_lock"),
        goal=goal,
    )


def task_id_from_path(path: Path) -> str:
    return path.stem


def discover_md_tasks(tasks_dir: Path) -> dict[str, TaskFile]:
    result: dict[str, TaskFile] = {}
    if not tasks_dir.is_dir():
        return result
    for path in sorted(tasks_dir.glob("[!.]*.md")):
        task_id = task_id_from_path(path)
        result[task_id] = parse_task(path)
    return result


def discover_tasks(tasks_dir: Path, state: dict[str, TaskState]) -> dict[str, TaskFile]:
    tasks = discover_md_tasks(tasks_dir)
    for task_id, ts in state.items():
        if ts.adhoc is not None and ts.repo is not None:
            tasks[task_id] = TaskFile(
                repo=ts.repo,
                repeat=False,
                prompt=ts.adhoc,
                repo_lock=ts.repo_lock,
                goal=ts.goal,
            )
    return tasks


@dataclass(frozen=True)
class ValidationResult:
    tasks: dict[str, TaskFile]
    errors: dict[str, str]


def validate_md_tasks(tasks_dir: Path) -> ValidationResult:
    tasks: dict[str, TaskFile] = {}
    errors: dict[str, str] = {}
    if not tasks_dir.is_dir():
        return ValidationResult(tasks=tasks, errors=errors)
    for path in sorted(tasks_dir.glob("[!.]*.md")):
        task_id = task_id_from_path(path)
        try:
            task = parse_task(path)
        except ValueError as e:
            errors[task_id] = str(e)
            continue
        if is_repo_url(task.repo):
            # URL repos are cloned at launch time; no local validation needed.
            pass
        else:
            repo = resolve_local_path(task.repo)
            if not repo.exists():
                errors[task_id] = f"{path}: repo does not exist: {repo}"
                continue
        tasks[task_id] = task
    return ValidationResult(tasks=tasks, errors=errors)
