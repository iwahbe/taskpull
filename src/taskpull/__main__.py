from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from .config import load_config
from .daemon import daemonize, is_daemon_running, remove_pid, stop_daemon, write_pid
from .ipc import send_command
from .supervisor import run


def cmd_start(config):
    running, pid = is_daemon_running(config)
    if running:
        print(f"daemon already running (PID {pid})")
        sys.exit(1)

    if pid is not None:
        remove_pid(config)
    config.sock_file.unlink(missing_ok=True)

    config.user_dir.mkdir(parents=True, exist_ok=True)

    daemonize(config.log_file)

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    write_pid(config)
    try:
        asyncio.run(run(config))
    finally:
        remove_pid(config)
        config.sock_file.unlink(missing_ok=True)


def cmd_stop(config):
    stop_daemon(config)


def cmd_status(config):
    running, pid = is_daemon_running(config)
    if not running:
        print("daemon is not running (start with `taskpull start`)")
        sys.exit(1)
    print(f"daemon is running (PID {pid})")

    try:
        response = send_command(config.sock_file, "status")
    except (ConnectionRefusedError, FileNotFoundError):
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
        print()
        print(f"Tasks ({len(tasks)}):")
        for task_id, info in sorted(tasks.items()):
            parts = [info["repo"]]
            if info.get("repeat"):
                parts.append("repeat")
            if info.get("repo_lock"):
                parts.append(f"lock={info['repo_lock']}")
            if not info.get("has_prompt"):
                parts.append("NO PROMPT")
            state = info.get("state")
            if state:
                parts.append(state["status"])
            print(f"  {task_id}: {', '.join(parts)}")

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
        response = send_command(config.sock_file, "list")
    except (ConnectionRefusedError, FileNotFoundError):
        print("daemon is not running (start with `taskpull start`)")
        sys.exit(1)

    tasks = response.get("tasks", {})
    if not tasks:
        print("No tasks.")
        return

    rows = []
    for task_id, info in sorted(tasks.items()):
        pr = str(info.get("pr_number") or "-")
        repo = info.get("repo") or "-"
        runs = str(info.get("run_count", 0))
        status = info.get("status", "unknown")
        if info.get("pr_number") and info.get("pr_draft"):
            status = "pr_draft"
        elif info.get("pr_number"):
            status = "pr_open"
        rows.append((task_id, status, pr, repo, runs))

    headers = ("TASK", "STATUS", "PR", "REPO", "RUNS")
    widths = [max(len(h), max(len(r[i]) for r in rows)) for i, h in enumerate(headers)]
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*headers))
    for row in rows:
        print(fmt.format(*row))


def cmd_refresh(config):
    _require_daemon(config)
    try:
        send_command(config.sock_file, "refresh")
    except (ConnectionRefusedError, FileNotFoundError):
        print("daemon is not running (start with `taskpull start`)")
        sys.exit(1)
    print("refresh triggered")


class _HelpFormatter(argparse.HelpFormatter):
    def _format_action(self, action: argparse.Action) -> str:
        if isinstance(action, argparse._SubParsersAction._ChoicesPseudoAction):
            if action.help == argparse.SUPPRESS:
                return ""
        return super()._format_action(action)


def main() -> None:
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
        dest="command", required=True, metavar="{start,stop,status,list,refresh}"
    )
    subparsers.add_parser("start", help="Start the daemon")
    subparsers.add_parser("stop", help="Stop the daemon")
    subparsers.add_parser("status", help="Show daemon status and validate tasks")
    subparsers.add_parser("list", help="Show tasks and their states")
    subparsers.add_parser("refresh", help="Trigger an immediate poll cycle")

    ft_parser = subparsers.add_parser("for-task", help=argparse.SUPPRESS)
    ft_sub = ft_parser.add_subparsers(dest="for_task_command", required=True)

    ft_sub.add_parser("notify").add_argument("events_file", type=Path)

    mcp_parser = ft_sub.add_parser("mcp-server")
    mcp_parser.add_argument("--sock", required=True, type=Path)
    mcp_parser.add_argument("--task-id", required=True)

    args = parser.parse_args()

    if args.command == "for-task":
        if args.for_task_command == "notify":
            from .notify import main as notify_main

            notify_main(args.events_file)
        elif args.for_task_command == "mcp-server":
            from .mcp_server import main as mcp_server_main

            mcp_server_main(args.sock, args.task_id)
        return

    config = load_config(args.user_dir)

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
