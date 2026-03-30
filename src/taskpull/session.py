from __future__ import annotations

import logging
import tempfile
from pathlib import Path

import libtmux

log = logging.getLogger(__name__)


def launch_session(
    server: libtmux.Server,
    name: str,
    worktree: Path,
    prompt: str,
    run_count: int,
    task_id: str,
    mcp_config: Path,
) -> str:
    fd, tmp = tempfile.mkstemp(prefix=f"taskpull-{task_id}-", suffix=".txt")
    prompt_file = Path(tmp)
    with open(fd, "w") as f:
        f.write(prompt)

    cmd = (
        f"cd {worktree!s} && "
        f"claude "
        f"--remote-control "
        f"--name '{task_id} (run {run_count})' "
        f"--allowedTools 'Bash,Read,Write,Edit,Glob,Grep,mcp__taskpull__task_exhausted' "
        f"--mcp-config {mcp_config!s} "
        f"< {prompt_file!s}; "
        f"rm -f {prompt_file!s}; "
        f"sleep 5"
    )

    session = server.new_session(
        session_name=name,
        window_command=cmd,
        attach=False,
    )
    return session.session_name  # type: ignore[return-value]


def resume_session(
    server: libtmux.Server,
    name: str,
    worktree: Path,
    session_id: str,
    run_count: int,
    task_id: str,
    mcp_config: Path,
) -> str:
    cmd = (
        f"cd {worktree!s} && "
        f"claude "
        f"--resume {session_id} "
        f"--remote-control "
        f"--name '{task_id} (run {run_count})' "
        f"--allowedTools 'Bash,Read,Write,Edit,Glob,Grep,mcp__taskpull__task_exhausted' "
        f"--mcp-config {mcp_config!s}; "
        f"sleep 5"
    )

    session = server.new_session(
        session_name=name,
        window_command=cmd,
        attach=False,
    )
    return session.session_name  # type: ignore[return-value]


def session_alive(server: libtmux.Server, name: str) -> bool:
    return any(s.session_name == name for s in server.sessions)


def kill_session(server: libtmux.Server, name: str) -> None:
    for s in server.sessions:
        if s.session_name == name:
            s.kill()
            return
