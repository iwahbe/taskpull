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
        raise RuntimeError(
            f"command {args!r} failed (rc={proc.returncode}): {stderr}"
        )
    return proc


def resolve_repo(raw: str) -> Path:
    return Path(raw).expanduser().resolve()


def worktree_path(worktrees_dir: Path, repo: Path, branch: str) -> Path:
    return worktrees_dir / repo.name / branch


async def default_branch(repo: Path) -> str:
    proc = await _run(
        "git", "symbolic-ref", "refs/remotes/origin/HEAD",
        cwd=repo, check=False,
    )
    if proc.returncode == 0 and proc.stdout:
        ref = (await proc.stdout.read()).decode().strip()
        return ref.removeprefix("refs/remotes/origin/")
    return "main"


async def fetch_origin(repo: Path) -> None:
    await _run("git", "fetch", "origin", cwd=repo)


async def create_worktree(
    worktrees_dir: Path, repo: Path, branch: str, base_ref: str,
) -> Path:
    wt = worktree_path(worktrees_dir, repo, branch)
    wt.parent.mkdir(parents=True, exist_ok=True)
    await _run(
        "git", "worktree", "add", str(wt), "-b", branch, base_ref,
        cwd=repo,
    )
    return wt


async def cleanup_worktree(
    worktrees_dir: Path, repo: Path, branch: str,
) -> None:
    wt = worktree_path(worktrees_dir, repo, branch)
    if wt.exists():
        proc = await _run(
            "git", "worktree", "remove", "--force", str(wt),
            cwd=repo, check=False,
        )
        if proc.returncode != 0:
            shutil.rmtree(wt, ignore_errors=True)
    await _run("git", "branch", "-D", branch, cwd=repo, check=False)
