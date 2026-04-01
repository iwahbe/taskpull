from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
from dataclasses import dataclass
from pathlib import Path
import os
from typing import Any

from .config import Config
from .gh_proxy import GHProxy, generate_certs, parse_github_repo
from .hooks import write_hooks_config
from .ipc import run_ipc_server
from .session import build_image, kill_session, launch_session, session_alive
from .state import TaskState, TaskStatus, load_state, save_state
from .task import TaskFile, discover_tasks, validate_tasks
from .worktree import (
    cleanup_worktree,
    create_worktree,
    default_branch,
    fetch_origin,
    resolve_repo,
)

log = logging.getLogger(__name__)

PR_INSTRUCTIONS = """

When your work is ready, create a descriptively named branch, push it, and open a PR:
  git checkout -b your-descriptive-branch-name
  git push -u origin HEAD
  gh pr create --fill
"""

REPEAT_SUFFIX = """
If there is nothing left to do because the task is already completed, call
the task_exhausted MCP tool.  Do NOT call task_exhausted when you have
finished working on a PR — only call it when there is no work to do at all.
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
    ts.proxy_secret = None


async def _get_gh_token() -> str:
    token = os.environ.get("GH_TOKEN")
    if token:
        return token
    proc = await asyncio.create_subprocess_exec(
        "gh",
        "auth",
        "token",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f"gh auth token failed (rc={proc.returncode}): {stderr.decode().strip()}"
        )
    return stdout.decode().strip()


async def run(config: Config, ready_fd: int, claude_token: str) -> None:
    log.info(
        "taskpull starting (poll_interval=%ds)",
        config.poll_interval,
    )
    log.info("Tasks dir: %s", config.tasks_dir)
    log.info("State file: %s", config.state_file)

    await build_image(config.docker_image)

    gh_token = await _get_gh_token()
    ca_cert, _ca_key, server_cert, server_key = generate_certs(config.certs_dir)
    gh_proxy = GHProxy(gh_token, ca_cert, server_cert, server_key)

    os.write(ready_fd, b"\x00")
    os.close(ready_fd)

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
        if command == "task_exhausted":
            tid: str = request.get("task_id", "")
            ts = current_state.get(tid)
            if ts is None:
                return {"status": "error", "message": f"unknown task: {tid}"}
            ts.exhaust_count += 1
            if ts.session_name:
                await kill_session(ts.session_name)
            save_state(config.state_file, current_state)
            refresh_event.set()
            log.info("task_exhausted received for %s", tid)
            return {"status": "ok"}
        if command == "restart":
            tid = request.get("task_id", "")
            ts = current_state.get(tid)
            if ts is None:
                return {"status": "error", "message": f"unknown task: {tid}"}
            await _cleanup_task(ts, gh_proxy)
            _reset_task(ts)
            save_state(config.state_file, current_state)
            refresh_event.set()
            log.info("restart received for %s", tid)
            return {"status": "ok"}
        if command == "notify_event":
            tid = request.get("task_id", "")
            event = request.get("event", {})
            ts = current_state.get(tid)
            if ts is None:
                return {"status": "error", "message": f"unknown task: {tid}"}
            event_type = event.get("type")
            if event_type == "session_start":
                ts.session_id = event["session_id"]
            elif event_type == "pr_created":
                ts.pr_number = event["pr_number"]
                ts.pr_url = event["pr_url"]
            elif event_type == "activity":
                ts.activity = event["activity"]
            save_state(config.state_file, current_state)
            return {"status": "ok"}
        return {"status": "error", "message": f"unknown command: {command}"}

    loop = asyncio.get_running_loop()

    def _shutdown():
        shutdown_event.set()
        refresh_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _shutdown)

    ipc_task = asyncio.create_task(
        run_ipc_server(config.ipc_port, ipc_handler, shutdown_event),
    )
    proxy_task = asyncio.create_task(
        gh_proxy.run(config.gh_proxy_port, shutdown_event),
    )

    try:
        while not shutdown_event.is_set():
            log.info("--- Poll cycle ---")

            state = load_state(config.state_file)
            tasks = discover_tasks(config.tasks_dir)

            await _phase2_check_prs(state, tasks, gh_proxy)
            await _phase3_check_sessions(state, gh_proxy)
            await _phase4_launch(config, state, tasks, claude_token, gh_proxy)

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
        await proxy_task
        log.info("taskpull stopped")


async def _cleanup_task(ts: TaskState, gh_proxy: GHProxy | None = None) -> None:
    if ts.proxy_secret and gh_proxy:
        gh_proxy.unregister_task(ts.proxy_secret)
    if ts.worktree and ts.repo:
        repo = resolve_repo(ts.repo)
        await cleanup_worktree(repo, Path(ts.worktree))
    if ts.session_name:
        await kill_session(ts.session_name)


async def _phase2_check_prs(
    state: dict[str, TaskState],
    tasks: dict[str, TaskFile],
    gh_proxy: GHProxy,
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
            await _cleanup_task(ts, gh_proxy)

            if task.repeat:
                ts.exhaust_count = 0
                _reset_task(ts)
            else:
                ts.status = TaskStatus.DONE

        elif pr_info.state == "CLOSED":
            log.info("  %s: PR #%d closed without merge", task_id, ts.pr_number)
            await _cleanup_task(ts, gh_proxy)
            _reset_task(ts)


async def _phase3_check_sessions(
    state: dict[str, TaskState],
    gh_proxy: GHProxy,
) -> None:
    log.info("Phase 3: Checking sessions")
    for task_id, ts in list(state.items()):
        if ts.status != TaskStatus.ACTIVE:
            continue
        if ts.session_name and await session_alive(ts.session_name):
            continue

        log.info("  %s: session gone, resetting to idle", task_id)
        await _cleanup_task(ts, gh_proxy)
        _reset_task(ts)


async def _phase4_launch(
    config: Config,
    state: dict[str, TaskState],
    tasks: dict[str, TaskFile],
    claude_token: str,
    gh_proxy: GHProxy,
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

            mcp_config = write_hooks_config(
                wt,
                task_id,
                config.ipc_port,
            )

            remote_url = await _get_remote_url(str(repo))
            owner_repo = parse_github_repo(remote_url)
            proxy_secret = gh_proxy.register_task(owner_repo)

            prompt = _build_prompt(task)
            session_name = f"taskpull-{task_id}"
            env: dict[str, str] = {
                "CLAUDE_CODE_OAUTH_TOKEN": claude_token,
                "GH_HOST": f"host.docker.internal:{config.gh_proxy_port}",
                "GH_TOKEN": proxy_secret,
            }
            anthropic_base_url = os.environ.get("ANTHROPIC_BASE_URL")
            if anthropic_base_url:
                env["ANTHROPIC_BASE_URL"] = anthropic_base_url
            await launch_session(
                session_name,
                wt,
                prompt,
                ts.run_count,
                task_id,
                mcp_config,
                config.docker_image,
                env,
                gh_proxy.ca_cert_path,
            )

            ts.status = TaskStatus.ACTIVE
            ts.repo = task.repo
            ts.worktree = str(wt)
            ts.session_name = session_name
            ts.session_id = None
            ts.pr_number = None
            ts.pr_url = None
            ts.activity = "active"
            ts.proxy_secret = proxy_secret

            busy_lanes.add(lane_key)
            break
