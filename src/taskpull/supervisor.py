from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import libtmux

from .config import Config
from .hooks import (
    ActivityEvent,
    PrCreatedEvent,
    SessionStartEvent,
    clear_events,
    read_events,
    write_hooks_config,
)
from .ipc import run_ipc_server
from .state import TaskState, TaskStatus, load_state, save_state
from .task import TaskFile, discover_tasks, validate_tasks
from .worktree import (
    cleanup_worktree,
    create_worktree,
    default_branch,
    fetch_origin,
    resolve_repo,
)
from .session import kill_session, launch_session, resume_session, session_alive

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
    ts.pr_url = None
    ts.pr_draft = False
    ts.worktree = None
    ts.session_id = None
    ts.session_name = None
    ts.activity = None


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
        if command == "status":
            result = validate_tasks(config.tasks_dir)
            return {
                "status": "ok",
                "tasks": {
                    tid: {
                        "repo": tf.repo,
                        "repeat": tf.repeat,
                        "repo_lock": tf.repo_lock,
                        "has_prompt": bool(tf.prompt),
                        "state": current_state[tid].to_dict()
                        if tid in current_state
                        else None,
                    }
                    for tid, tf in result.tasks.items()
                },
                "errors": result.errors,
            }
        if command == "task_done":
            tid: str = request.get("task_id", "")
            ts = current_state.get(tid)
            if ts is None:
                return {"status": "error", "message": f"unknown task: {tid}"}
            ts.exhaust_count += 1
            save_state(config.state_file, current_state)
            refresh_event.set()
            log.info("task_done received for %s", tid)
            return {"status": "ok"}
        return {"status": "error", "message": f"unknown command: {command}"}

    loop = asyncio.get_running_loop()

    def _shutdown():
        shutdown_event.set()
        refresh_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _shutdown)

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
        last_activity: ActivityEvent | None = None
        for event in events:
            if isinstance(event, SessionStartEvent):
                ts.session_id = event.session_id
                log.info("  %s: session_id=%s", task_id, event.session_id)
            elif isinstance(event, PrCreatedEvent):
                ts.pr_number = event.pr_number
                ts.pr_url = event.pr_url
                log.info(
                    "  %s: PR #%d created (%s)",
                    task_id,
                    event.pr_number,
                    event.pr_url,
                )
            elif isinstance(event, ActivityEvent):
                last_activity = event

        if last_activity is not None:
            ts.activity = last_activity.activity

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
        if ts.status != TaskStatus.ACTIVE or ts.pr_number is None:
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

            if task.repeat:
                ts.exhaust_count = 0
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

        log.info("  %s: session gone, restoring", task_id)

        if ts.session_id and ts.worktree and ts.session_name:
            log.info("  %s: resuming session %s", task_id, ts.session_id)
            resume_session(
                server,
                ts.session_name,
                Path(ts.worktree),
                ts.session_id,
                ts.run_count,
                task_id,
            )
        else:
            log.warning("  %s: no session_id to restore, resetting to idle", task_id)
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

    busy_lanes: set[tuple[str, str]] = set()
    for tid, ts in state.items():
        if ts.status == TaskStatus.ACTIVE and ts.repo:
            task = tasks.get(tid)
            lane = task.lane_key if task else (ts.repo, ts.repo)
            busy_lanes.add(lane)

    # Ensure all tasks have state entries.
    for task_id, task in tasks.items():
        if task_id not in state:
            state[task_id] = TaskState()

    # Group eligible tasks by lane.
    lane_candidates: dict[tuple[str, str], list[tuple[str, TaskFile, TaskState]]] = {}
    for task_id, task in tasks.items():
        ts = state[task_id]
        if ts.status in (TaskStatus.ACTIVE, TaskStatus.DONE):
            continue
        backoff = ts.exhaust_backoff(config.poll_interval)
        if backoff > 0 and ts.seconds_since_launch() < backoff:
            continue
        if task.lane_key in busy_lanes:
            continue
        lane_candidates.setdefault(task.lane_key, []).append((task_id, task, ts))

    # For each free lane, pick the task launched least recently (round-robin).
    for lane_key, candidates in lane_candidates.items():
        candidates.sort(key=lambda c: c[2].last_launched_at)

        for task_id, task, ts in candidates:
            repo = resolve_repo(task.repo)
            if not repo.exists():
                log.warning("  %s: repo %s does not exist, skipping", task_id, repo)
                continue

            ts.run_count += 1
            ts.last_launched_at = int(time.time())
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
            ts.pr_url = None
            ts.activity = "active"

            busy_lanes.add(lane_key)
            break
