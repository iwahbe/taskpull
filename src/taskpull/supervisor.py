from __future__ import annotations

import asyncio
import logging

import libtmux

from .config import Config
from .hooks import (
    PrCreatedEvent,
    SessionStartEvent,
    clear_events,
    read_events,
    write_hooks_config,
)
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

When your work is ready, push your branch and create a PR:
  git push -u origin HEAD
  gh pr create --fill
"""

REPEAT_SUFFIX = """
If there is nothing left to do, output the exact text TASKPULL_DONE and exit.
"""


async def _check_pr_state(repo_path: str, pr_number: int) -> str:
    proc = await asyncio.create_subprocess_exec(
        "gh", "pr", "view", str(pr_number),
        "--repo", repo_path,
        "--json", "state", "-q", ".state",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()
    if proc.returncode != 0:
        return "UNKNOWN"
    return stdout.decode().strip()


async def _get_remote_url(repo_path: str) -> str:
    proc = await asyncio.create_subprocess_exec(
        "git", "-C", repo_path, "remote", "get-url", "origin",
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


async def run(config: Config, *, once: bool = False) -> None:
    log.info(
        "taskpull starting (poll_interval=%ds)", config.poll_interval,
    )
    log.info("Tasks dir: %s", config.tasks_dir)
    log.info("State file: %s", config.state_file)

    config.events_dir.mkdir(parents=True, exist_ok=True)
    server = libtmux.Server()

    while True:
        log.info("--- Poll cycle ---")

        state = load_state(config.state_file)
        tasks = discover_tasks(config.tasks_dir)

        _phase1_process_events(config, state)
        await _phase2_check_prs(config, state, tasks, server)
        await _phase3_check_sessions(config, state, tasks, server)
        await _phase4_launch(config, state, tasks, server)

        save_state(config.state_file, state)

        if once:
            log.info("Single cycle complete.")
            break

        log.info("Sleeping %ds", config.poll_interval)
        await asyncio.sleep(config.poll_interval)


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
                    task_id, event.pr_number, event.pr_url,
                )

        if events:
            clear_events(config.events_dir, task_id)


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
        pr_state = await _check_pr_state(remote_url, ts.pr_number)

        if pr_state == "MERGED":
            log.info("  %s: PR #%d merged", task_id, ts.pr_number)
            if ts.branch:
                await cleanup_worktree(config.worktrees_dir, repo, ts.branch)
            if ts.session_name:
                kill_session(server, ts.session_name)
            clear_events(config.events_dir, task_id)

            if task.repeat and not ts.exhausted:
                ts.status = TaskStatus.IDLE
                ts.pr_number = None
                ts.branch = None
                ts.worktree = None
                ts.session_id = None
                ts.session_name = None
            else:
                ts.status = TaskStatus.DONE

        elif pr_state == "CLOSED":
            log.info("  %s: PR #%d closed without merge", task_id, ts.pr_number)
            if ts.branch:
                await cleanup_worktree(config.worktrees_dir, repo, ts.branch)
            if ts.session_name:
                kill_session(server, ts.session_name)
            clear_events(config.events_dir, task_id)

            ts.status = TaskStatus.IDLE
            ts.pr_number = None
            ts.branch = None
            ts.worktree = None
            ts.session_id = None
            ts.session_name = None


async def _phase3_check_sessions(
    config: Config,
    state: dict[str, TaskState],
    tasks: dict[str, TaskFile],
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
            # PR was already created via hook; phase 2 handles it.
            ts.status = TaskStatus.PR_OPEN
            continue

        task = tasks.get(task_id)
        if task is None or ts.repo is None:
            continue

        repo = resolve_repo(ts.repo)

        # Check events one more time for any last-moment PR creation.
        events = read_events(config.events_dir, task_id)
        pr_event = next(
            (e for e in events if isinstance(e, PrCreatedEvent)), None,
        )
        if pr_event:
            ts.pr_number = pr_event.pr_number
            ts.status = TaskStatus.PR_OPEN
            clear_events(config.events_dir, task_id)
            log.info(
                "  %s: late PR #%d detected", task_id, pr_event.pr_number,
            )
            continue

        # No PR. Clean up and reset.
        log.info("  %s: no PR, resetting to idle", task_id)
        if ts.branch:
            await cleanup_worktree(config.worktrees_dir, repo, ts.branch)
        clear_events(config.events_dir, task_id)

        ts.status = TaskStatus.IDLE
        ts.branch = None
        ts.worktree = None
        ts.session_id = None
        ts.session_name = None


async def _phase4_launch(
    config: Config,
    state: dict[str, TaskState],
    tasks: dict[str, TaskFile],
    server: libtmux.Server,
) -> None:
    log.info("Phase 4: Launching new work")

    busy_repos: set[str] = set()
    for ts in state.values():
        if ts.status in (TaskStatus.ACTIVE, TaskStatus.PR_OPEN) and ts.repo:
            busy_repos.add(ts.repo)

    for task_id, task in tasks.items():
        ts = state.get(task_id)
        if ts is None:
            ts = TaskState()
            state[task_id] = ts

        if ts.status in (TaskStatus.ACTIVE, TaskStatus.PR_OPEN, TaskStatus.DONE):
            continue
        if ts.exhausted:
            continue
        if task.repo in busy_repos:
            continue

        repo = resolve_repo(task.repo)
        if not repo.exists():
            log.warning("  %s: repo %s does not exist, skipping", task_id, repo)
            continue

        ts.run_count += 1
        branch = f"{task.branch_prefix}-{ts.run_count}"
        default_br = await default_branch(repo)

        log.info(
            "  %s: launching run %d on %s (branch: %s)",
            task_id, ts.run_count, repo, branch,
        )

        await fetch_origin(repo)
        wt = await create_worktree(
            config.worktrees_dir, repo, branch, f"origin/{default_br}",
        )

        write_hooks_config(wt, task_id, config.events_dir, config.notify_script)

        prompt = _build_prompt(task)
        session_name = f"taskpull-{task_id}"
        launch_session(server, session_name, wt, prompt, ts.run_count, task_id)

        ts.status = TaskStatus.ACTIVE
        ts.repo = task.repo
        ts.branch = branch
        ts.worktree = str(wt)
        ts.session_name = session_name
        ts.session_id = None
        ts.pr_number = None

        busy_repos.add(task.repo)
