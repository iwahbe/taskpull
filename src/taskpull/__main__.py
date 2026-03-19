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
        if status == "pr_open" and info.get("pr_draft"):
            status = "pr_draft"
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


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="taskpull",
        description="Pull-based multi-repo Claude Code task runner",
    )
    parser.add_argument(
        "--user-dir",
        type=Path,
        default=Path.home() / ".taskpull",
        help="User data directory (default: ~/.taskpull)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("start", help="Start the daemon")
    subparsers.add_parser("stop", help="Stop the daemon")
    subparsers.add_parser("status", help="Show whether daemon is running")
    subparsers.add_parser("list", help="Show tasks and their states")
    subparsers.add_parser("refresh", help="Trigger an immediate poll cycle")

    args = parser.parse_args()
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
