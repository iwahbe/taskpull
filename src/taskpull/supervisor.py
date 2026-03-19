from __future__ import annotations

import asyncio
import json
import logging
import signal
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import libtmux

from .config import Config
from .hooks import (
    PrCreatedEvent,
    SessionStartEvent,
    clear_events,
    read_events,
    write_hooks_config,
)
from .ipc import run_ipc_server
from .state import TaskState, TaskStatus, load_state, save_state
from .task import TaskFile, discover_tasks
from .worktree import (
    cleanup_worktree,
    create_worktree,
    default_branch,
    fetch_origin,
    resolve_repo,
)
from .session import kill_session, launch_session, session_alive

log = logging.getLogger(__name__)

PR_INSTRUCTIONS = """

When your work is ready, create a descriptively named branch, push it, and open a PR:
  git checkout -b your-descriptive-branch-name
  git push -u origin HEAD
  gh pr create --fill
"""

REPEAT_SUFFIX = """
If there is nothing left to do, call the task_done MCP tool and exit.
"""


@dataclass
class PrInfo:
    state: str
    is_draft: bool


async def _check_pr_state(repo_path: str, pr_number: int) -> PrInfo:
    proc = await asyncio.create_subprocess_exec(
        "gh",
        "pr",
        "view",
        str(pr_number),
        "--repo",
        repo_path,
        "--json",
        "state,isDraft",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()
    if proc.returncode != 0:
        return PrInfo(state="UNKNOWN", is_draft=False)
    data = json.loads(stdout.decode())
    return PrInfo(
        state=data.get("state", "UNKNOWN"), is_draft=data.get("isDraft", False)
    )


async def _get_remote_url(repo_path: str) -> str:
    proc = await asyncio.create_subprocess_exec(
        "git",
        "-C",
        repo_path,
        "remote",
        "get-url",
        "origin",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()
    return stdout.decode().strip()


def _build_prompt(task: TaskFile) -> str:
    prompt = task.prompt + PR_INSTRUCTIONS
    if task.repeat:
        prompt += REPEAT_SUFFIX
    return prompt


def _reset_task(ts: TaskState) -> None:
    ts.status = TaskStatus.IDLE
    ts.pr_number = None
    ts.pr_draft = False
    ts.worktree = None
    ts.session_id = None
    ts.session_name = None


async def run(config: Config) -> None:
    log.info(
        "taskpull starting (poll_interval=%ds)",
        config.poll_interval,
    )
    log.info("Tasks dir: %s", config.tasks_dir)
    log.info("State file: %s", config.state_file)

    config.events_dir.mkdir(parents=True, exist_ok=True)
    tmux_server = libtmux.Server()

    shutdown_event = asyncio.Event()
    refresh_event = asyncio.Event()

    current_state: dict[str, TaskState] = {}

    async def ipc_handler(request: dict[str, Any]) -> dict[str, Any]:
        command = request.get("command")
        if command == "refresh":
            refresh_event.set()
            return {"status": "ok"}
        if command == "stop":
            shutdown_event.set()
            return {"status": "ok"}
        if command == "list":
            return {
                "status": "ok",
                "tasks": {k: v.to_dict() for k, v in current_state.items()},
            }
        if command == "task_done":
            tid: str = request.get("task_id", "")
            ts = current_state.get(tid)
            if ts is None:
                return {"status": "error", "message": f"unknown task: {tid}"}
            ts.exhausted = True
            save_state(config.state_file, current_state)
            refresh_event.set()
            log.info("task_done received for %s", tid)
            return {"status": "ok"}
        return {"status": "error", "message": f"unknown command: {command}"}

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)

    ipc_task = asyncio.create_task(
        run_ipc_server(config.sock_file, ipc_handler, shutdown_event),
    )

    try:
        while not shutdown_event.is_set():
            log.info("--- Poll cycle ---")

            state = load_state(config.state_file)
            tasks = discover_tasks(config.tasks_dir)

            _phase1_process_events(config, state)
            await _phase2_check_prs(config, state, tasks, tmux_server)
            await _phase3_check_sessions(config, state, tmux_server)
            await _phase4_launch(config, state, tasks, tmux_server)

            save_state(config.state_file, state)
            current_state.clear()
            current_state.update(state)

            refresh_event.clear()
            try:
                await asyncio.wait_for(
                    refresh_event.wait(), timeout=config.poll_interval
                )
            except asyncio.TimeoutError:
                pass
    finally:
        shutdown_event.set()
        await ipc_task
        log.info("taskpull stopped")


def _phase1_process_events(
    config: Config,
    state: dict[str, TaskState],
) -> None:
    log.info("Phase 1: Processing events")
    for task_id, ts in state.items():
        if ts.status != TaskStatus.ACTIVE:
            continue

        events = read_events(config.events_dir, task_id)
        for event in events:
            if isinstance(event, SessionStartEvent):
                ts.session_id = event.session_id
                log.info("  %s: session_id=%s", task_id, event.session_id)
            elif isinstance(event, PrCreatedEvent):
                ts.pr_number = event.pr_number
                ts.status = TaskStatus.PR_OPEN
                log.info(
                    "  %s: PR #%d created (%s)",
                    task_id,
                    event.pr_number,
                    event.pr_url,
                )

        if events:
            clear_events(config.events_dir, task_id)


async def _cleanup_task(ts: TaskState, server: libtmux.Server) -> None:
    if ts.worktree and ts.repo:
        repo = resolve_repo(ts.repo)
        await cleanup_worktree(repo, Path(ts.worktree))
    if ts.session_name:
        kill_session(server, ts.session_name)


async def _phase2_check_prs(
    config: Config,
    state: dict[str, TaskState],
    tasks: dict[str, TaskFile],
    server: libtmux.Server,
) -> None:
    log.info("Phase 2: Checking PRs")
    for task_id, ts in list(state.items()):
        if ts.status != TaskStatus.PR_OPEN or ts.pr_number is None:
            continue

        task = tasks.get(task_id)
        if task is None or ts.repo is None:
            continue

        repo = resolve_repo(ts.repo)
        remote_url = await _get_remote_url(str(repo))
        pr_info = await _check_pr_state(remote_url, ts.pr_number)
        ts.pr_draft = pr_info.is_draft

        if pr_info.state == "MERGED":
            log.info("  %s: PR #%d merged", task_id, ts.pr_number)
            await _cleanup_task(ts, server)
            clear_events(config.events_dir, task_id)

            if task.repeat and not ts.exhausted:
                _reset_task(ts)
            else:
                ts.status = TaskStatus.DONE

        elif pr_info.state == "CLOSED":
            log.info("  %s: PR #%d closed without merge", task_id, ts.pr_number)
            await _cleanup_task(ts, server)
            clear_events(config.events_dir, task_id)
            _reset_task(ts)


async def _phase3_check_sessions(
    config: Config,
    state: dict[str, TaskState],
    server: libtmux.Server,
) -> None:
    log.info("Phase 3: Checking sessions")
    for task_id, ts in list(state.items()):
        if ts.status != TaskStatus.ACTIVE:
            continue
        if ts.session_name and session_alive(server, ts.session_name):
            continue

        log.info("  %s: session ended", task_id)

        if ts.pr_number is not None:
            ts.status = TaskStatus.PR_OPEN
            continue

        # Check events one more time for any last-moment PR creation.
        events = read_events(config.events_dir, task_id)
        pr_event = next(
            (e for e in events if isinstance(e, PrCreatedEvent)),
            None,
        )
        if pr_event:
            ts.pr_number = pr_event.pr_number
            ts.status = TaskStatus.PR_OPEN
            clear_events(config.events_dir, task_id)
            log.info(
                "  %s: late PR #%d detected",
                task_id,
                pr_event.pr_number,
            )
            continue

        log.info("  %s: no PR, resetting to idle", task_id)
        await _cleanup_task(ts, server)
        clear_events(config.events_dir, task_id)
        _reset_task(ts)


async def _phase4_launch(
    config: Config,
    state: dict[str, TaskState],
    tasks: dict[str, TaskFile],
    server: libtmux.Server,
) -> None:
    log.info("Phase 4: Launching new work")

    busy_locks: set[tuple[str, str]] = set()
    for tid, ts in state.items():
        if ts.status in (TaskStatus.ACTIVE, TaskStatus.PR_OPEN) and ts.repo:
            task = tasks.get(tid)
            lock = task.repo_lock if task and task.repo_lock else ts.repo
            busy_locks.add((ts.repo, lock))

    for task_id, task in tasks.items():
        ts = state.get(task_id)
        if ts is None:
            ts = TaskState()
            state[task_id] = ts

        if ts.status in (TaskStatus.ACTIVE, TaskStatus.PR_OPEN, TaskStatus.DONE):
            continue
        if ts.exhausted:
            continue
        lock = task.repo_lock if task.repo_lock else task.repo
        if (task.repo, lock) in busy_locks:
            continue

        repo = resolve_repo(task.repo)
        if not repo.exists():
            log.warning("  %s: repo %s does not exist, skipping", task_id, repo)
            continue

        ts.run_count += 1
        default_br = await default_branch(repo)

        log.info(
            "  %s: launching run %d on %s",
            task_id,
            ts.run_count,
            repo,
        )

        await fetch_origin(repo)
        wt = await create_worktree(
            config.worktrees_dir,
            repo,
            task_id,
            ts.run_count,
            f"origin/{default_br}",
        )

        write_hooks_config(
            wt,
            task_id,
            config.events_dir,
            config.notify_script,
            config.mcp_server_script,
            config.sock_file,
        )

        prompt = _build_prompt(task)
        session_name = f"taskpull-{task_id}"
        launch_session(server, session_name, wt, prompt, ts.run_count, task_id)

        ts.status = TaskStatus.ACTIVE
        ts.repo = task.repo
        ts.worktree = str(wt)
        ts.session_name = session_name
        ts.session_id = None
        ts.pr_number = None

        busy_locks.add((task.repo, lock))
