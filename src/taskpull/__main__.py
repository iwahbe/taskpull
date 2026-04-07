from __future__ import annotations

import argparse
import asyncio
import logging
import re
import setproctitle
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from .config import load_config
from .credentials import get_claude_token
from .daemon import daemonize, is_daemon_running, remove_pid, stop_daemon, write_pid
from .ipc import send_command
from .session import DockerBackend
from .supervisor import run


def cmd_start(config):
    running, pid = is_daemon_running(config)
    if running:
        print(f"daemon already running (PID {pid})")
        sys.exit(1)

    if pid is not None:
        remove_pid(config)

    config.user_dir.mkdir(parents=True, exist_ok=True)

    claude_token = get_claude_token()

    ready_fd = daemonize(config.log_file, config.pid_file)

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    write_pid(config)

    asyncio.run(run(config, ready_fd, claude_token, DockerBackend()))


def cmd_stop(config):
    stop_daemon(config)


def _task_status_label(info: dict[str, Any]) -> str:
    state = info.get("state")
    if not state:
        return "pending"
    status = state.get("status", "idle")
    if status == "broken":
        return "broken"
    if status == "active":
        activity = state.get("activity")
        if activity == "idle":
            return "idle"
        if activity == "initializing":
            return "initializing"
        return "working"
    if status == "done":
        return "done"
    return status


def cmd_status(config):
    running, pid = is_daemon_running(config)
    if not running:
        print("daemon is not running (start with `taskpull start`)")
        sys.exit(1)
    print(f"daemon is running (PID {pid})")

    try:
        response = send_command("127.0.0.1", config.ipc_port, "status")
    except ConnectionRefusedError:
        print("could not connect to daemon")
        sys.exit(1)

    errors = response.get("errors", {})
    tasks = response.get("tasks", {})

    if errors:
        print()
        print(f"Task errors ({len(errors)}):")
        for task_id, msg in sorted(errors.items()):
            print(f"  {task_id}: {msg}")

    if tasks:
        # Group tasks by lane.
        lanes: dict[tuple[str, str], list[tuple[str, dict]]] = {}
        for task_id, info in tasks.items():
            repo = info["repo"]
            lock = info.get("repo_lock") or repo
            lanes.setdefault((repo, lock), []).append((task_id, info))

        # Sort tasks within each lane: active first, then by last_launched_at.
        for lane_tasks in lanes.values():
            lane_tasks.sort(
                key=lambda t: (
                    0 if (t[1].get("state") or {}).get("status") == "active" else 1,
                    (t[1].get("state") or {}).get("last_launched_at", 0),
                )
            )

        print()
        print(f"Lanes ({len(lanes)}):")
        for (repo, lock), lane_tasks in sorted(lanes.items()):
            header = repo
            if lock != repo:
                header += f" [lock={lock}]"
            print(f"  {header}:")

            name_width = max(len(tid) for tid, _ in lane_tasks)
            for task_id, info in lane_tasks:
                state = info.get("state")
                is_active = state and state.get("status") == "active"
                marker = ">" if is_active else " "
                label = _task_status_label(info)
                runs = state.get("run_count", 0) if state else 0
                extras = []
                if info.get("repeat"):
                    extras.append("repeat")
                if not info.get("has_prompt"):
                    extras.append("NO PROMPT")
                suffix = f" ({', '.join(extras)})" if extras else ""
                print(
                    f"    {marker} {task_id:<{name_width}}  {label} (run {runs}){suffix}"
                )
                if state and state.get("status") == "broken":
                    error = state.get("error_message", "")
                    if error:
                        for line in error.splitlines()[-10:]:
                            print(f"      {line}")
            print()

    if not errors and not tasks:
        print("\nNo tasks found.")


def _require_daemon(config):
    running, _ = is_daemon_running(config)
    if not running:
        print("daemon is not running (start with `taskpull start`)")
        sys.exit(1)


def cmd_list(config):
    _require_daemon(config)
    try:
        response = send_command("127.0.0.1", config.ipc_port, "list")
    except ConnectionRefusedError:
        print("daemon is not running (start with `taskpull start`)")
        sys.exit(1)

    tasks = response.get("tasks", {})
    if not tasks:
        print("No tasks.")
        return

    rows = []
    for task_id, info in sorted(tasks.items()):
        pr = info.get("pr_url") or "-"
        repo = info.get("repo") or "-"
        runs = str(info.get("run_count", 0))
        status = info.get("status", "unknown")
        if info.get("pr_number") and info.get("pr_draft"):
            status = "pr_draft"
        elif info.get("pr_number"):
            status = "pr_open"
        elif info.get("exhaust_count", 0) > 0:
            status = "exhausted"
        elif status == "active":
            activity = info.get("activity")
            if activity == "idle":
                status = "idle"
            elif activity == "active":
                status = "working"

        draft = "yes" if info.get("pr_draft") else "-"
        pr_approved = info.get("pr_approved")
        if pr_approved is True:
            approved = "yes"
        elif pr_approved is False:
            approved = "no"
        else:
            approved = "-"

        pr_ci = info.get("pr_ci", "unknown")
        ci = "-" if pr_ci in ("unknown", "none") else pr_ci

        rows.append((task_id, status, pr, draft, approved, ci, repo, runs))

    headers = ("TASK", "STATUS", "PR", "DRAFT", "APPROVED", "CI", "DIR", "RUNS")
    widths = [max(len(h), max(len(r[i]) for r in rows)) for i, h in enumerate(headers)]
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*headers))
    for row in rows:
        print(fmt.format(*row))


def cmd_refresh(config):
    _require_daemon(config)
    try:
        send_command("127.0.0.1", config.ipc_port, "refresh")
    except ConnectionRefusedError:
        print("daemon is not running (start with `taskpull start`)")
        sys.exit(1)
    print("refresh triggered")


def cmd_restart(config, task_name):
    _require_daemon(config)
    try:
        response = send_command(
            "127.0.0.1", config.ipc_port, "restart", task_id=task_name
        )
    except ConnectionRefusedError:
        print("could not connect to daemon")
        sys.exit(1)
    if response.get("status") != "ok":
        print(f"error: {response.get('message', 'unknown error')}")
        sys.exit(1)
    print(f"task {task_name!r} restarted")


def _slug_from_repo(repo: str) -> str:
    """Derive a short slug from a repo URL or local path for use in task IDs."""
    from .workspace import is_repo_url

    if is_repo_url(repo):
        m = re.search(r"([^/:]+/[^/:]+?)(?:\.git)?$", repo)
        if m:
            return m.group(1).replace("/", "-")
    return Path(repo).resolve().name


def cmd_new(config, location: str, prompt: str, goal: str, repo_lock: str | None):
    from .workspace import is_repo_url, normalize_location, resolve_local_path

    if location == "git":
        result = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print("error: not in a git repository or no origin remote")
            sys.exit(1)
        repo = result.stdout.strip()
    else:
        repo = normalize_location(location)
        if not is_repo_url(repo):
            local = resolve_local_path(repo)
            if not local.exists():
                print(f"error: path does not exist: {local}")
                sys.exit(1)
            repo = str(local)

    task_id = f"adhoc-{_slug_from_repo(repo)}-{int(time.time())}"

    _require_daemon(config)
    try:
        response = send_command(
            "127.0.0.1",
            config.ipc_port,
            "new_task",
            task_id=task_id,
            repo=repo,
            prompt=prompt,
            goal=goal,
            repo_lock=repo_lock,
        )
    except ConnectionRefusedError:
        print("could not connect to daemon")
        sys.exit(1)
    if response.get("status") != "ok":
        print(f"error: {response.get('message', 'unknown error')}")
        sys.exit(1)
    print(f"task {task_id!r} created")


class _HelpFormatter(argparse.HelpFormatter):
    def _format_action(self, action: argparse.Action) -> str:
        if isinstance(action, argparse._SubParsersAction._ChoicesPseudoAction):
            if action.help == argparse.SUPPRESS:
                return ""
        return super()._format_action(action)


def main() -> None:
    setproctitle.setproctitle("taskpull")

    parser = argparse.ArgumentParser(
        prog="taskpull",
        description="Pull-based multi-repo Claude Code task runner",
        formatter_class=_HelpFormatter,
    )
    parser.add_argument(
        "--user-dir",
        type=Path,
        default=Path.home() / ".taskpull",
        help="User data directory (default: ~/.taskpull)",
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=False,
        metavar="{start,stop,status,list,refresh,restart,new}",
    )
    subparsers.add_parser("start", help="Start the daemon")
    subparsers.add_parser("stop", help="Stop the daemon")
    subparsers.add_parser("status", help="Show daemon status and validate tasks")
    subparsers.add_parser("list", help="Show tasks and their states")
    subparsers.add_parser("refresh", help="Trigger an immediate poll cycle")
    restart_parser = subparsers.add_parser(
        "restart", help="Kill a task's session and re-enqueue it"
    )
    restart_parser.add_argument("task_name", help="Name of the task to restart")

    new_parser = subparsers.add_parser("new", help="Create a one-time ad-hoc task")
    new_parser.add_argument(
        "--goal",
        choices=["none", "pr"],
        default="none",
        help="Task goal: 'pr' to terminate on PR close/merge, 'none' for open-ended (default: none)",
    )
    new_parser.add_argument(
        "--repo-lock",
        default=None,
        help="Concurrency key. Tasks with the same repo and repo-lock won't run simultaneously.",
    )
    new_parser.add_argument(
        "location",
        help="Git repo URL, local path, or 'git' for current repo's origin",
    )
    new_parser.add_argument("prompt", help="Task prompt")

    args = parser.parse_args()

    config = load_config(args.user_dir)

    if args.command == "restart":
        cmd_restart(config, args.task_name)
        return

    if args.command == "new":
        cmd_new(config, args.location, args.prompt, args.goal, args.repo_lock)
        return

    if args.command is None:
        _require_daemon(config)
        from .tui import launch_tui

        launch_tui(config)
        return

    commands = {
        "start": cmd_start,
        "stop": cmd_stop,
        "status": cmd_status,
        "list": cmd_list,
        "refresh": cmd_refresh,
    }
    commands[args.command](config)


if __name__ == "__main__":
    main()
