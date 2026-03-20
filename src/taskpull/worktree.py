from __future__ import annotations

import asyncio
import logging
import shutil
from pathlib import Path

log = logging.getLogger(__name__)


async def _run(
    *args: str, cwd: Path | None = None, check: bool = True
) -> asyncio.subprocess.Process:
    proc = await asyncio.create_subprocess_exec(
        *args,
        cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await proc.wait()
    if check and proc.returncode != 0:
        stderr = (await proc.stderr.read()).decode() if proc.stderr else ""
        raise RuntimeError(f"command {args!r} failed (rc={proc.returncode}): {stderr}")
    return proc


def resolve_repo(raw: str) -> Path:
    return Path(raw).expanduser().resolve()


async def default_branch(repo: Path) -> str:
    proc = await _run(
        "git",
        "symbolic-ref",
        "refs/remotes/origin/HEAD",
        cwd=repo,
        check=False,
    )
    if proc.returncode == 0 and proc.stdout:
        ref = (await proc.stdout.read()).decode().strip()
        return ref.removeprefix("refs/remotes/origin/")
    return "main"


async def fetch_origin(repo: Path) -> None:
    await _run("git", "fetch", "origin", cwd=repo)


async def create_worktree(
    worktrees_dir: Path,
    repo: Path,
    task_id: str,
    run_count: int,
    base_ref: str,
) -> Path:
    wt = worktrees_dir / task_id / str(run_count)
    wt.parent.mkdir(parents=True, exist_ok=True)
    await _run(
        "git",
        "worktree",
        "add",
        "--detach",
        str(wt),
        base_ref,
        cwd=repo,
    )
    return wt


async def cleanup_worktree(repo: Path, worktree: Path) -> None:
    if worktree.exists():
        proc = await _run(
            "git",
            "worktree",
            "remove",
            "--force",
            str(worktree),
            cwd=repo,
            check=False,
        )
        if proc.returncode != 0:
            shutil.rmtree(worktree, ignore_errors=True)
