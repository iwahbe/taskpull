from __future__ import annotations

import asyncio
import logging
import re
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


def is_repo_url(repo: str) -> bool:
    return repo.startswith("https://") or repo.startswith("git@")


def resolve_local_path(raw: str) -> Path:
    return Path(raw).expanduser().resolve()


def repo_url_to_owner_repo(url: str) -> str:
    """Extract 'owner/repo' from a GitHub URL.

    Handles both SSH (git@github.com:owner/repo.git) and
    HTTPS (https://github.com/owner/repo.git) formats.
    """
    m = re.match(r"git@github\.com:(.+?)(?:\.git)?$", url)
    if m:
        return m.group(1)
    m = re.match(r"https://github\.com/(.+?)(?:\.git)?$", url)
    if m:
        return m.group(1)
    raise ValueError(f"cannot parse GitHub owner/repo from: {url}")


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


async def clone_repo(
    workspace_dir: Path,
    repo_url: str,
    task_id: str,
    run_count: int,
) -> Path:
    dest = workspace_dir / task_id / str(run_count)
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        log.warning("workspace %s already exists, removing stale directory", dest)
        shutil.rmtree(dest)
    await _run("git", "clone", repo_url, str(dest))
    return dest


async def cleanup_workspace(workspace: Path | None) -> None:
    if workspace is None:
        return
    wp = Path(workspace)
    if wp.exists():
        shutil.rmtree(wp, ignore_errors=True)
