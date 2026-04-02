from __future__ import annotations

import curses
import shlex
import signal
import shutil
import subprocess
import sys
from typing import Any

from .config import Config
from .daemon import is_daemon_running
from .ipc import send_command

_SESSION_NAME = "taskpull-tui"


def _tmux(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["tmux", *args],
        capture_output=True,
        text=True,
    )


def _kill_session() -> None:
    _tmux("kill-session", "-t", _SESSION_NAME)


def _tmux_session_exists() -> bool:
    result = _tmux("has-session", "-t", _SESSION_NAME)
    return result.returncode == 0


def _fetch_tasks(ipc_port: int) -> dict[str, dict[str, Any]]:
    try:
        response = send_command("127.0.0.1", ipc_port, "list")
    except (ConnectionRefusedError, OSError):
        return {}
    return response.get("tasks", {})


def _attach_cmd(session_name: str) -> str:
    return f"docker exec -it {session_name} tmux attach -t claude"


def _right_pane_cmd(tasks: dict[str, dict[str, Any]]) -> str:
    for _tid, info in sorted(tasks.items()):
        if info.get("status") == "active" and info.get("session_name"):
            return _attach_cmd(info["session_name"])
    return "echo 'No active tasks. Select a task from the sidebar.'; read"


def launch_tui(config: Config) -> None:
    running, _ = is_daemon_running(config)
    if not running:
        print("daemon is not running (start with `taskpull start`)")
        sys.exit(1)

    if not shutil.which("tmux"):
        print("tmux is required (brew install tmux)")
        sys.exit(1)

    if _tmux_session_exists():
        _kill_session()

    tasks = _fetch_tasks(config.ipc_port)

    sidebar_cmd = (
        f"{sys.executable} -m taskpull for-task tui-sidebar --port {config.ipc_port}"
    )
    right_cmd = _right_pane_cmd(tasks)

    _tmux("new-session", "-d", "-s", _SESSION_NAME, sidebar_cmd)
    _tmux(
        "split-window",
        "-h",
        "-d",
        "-t",
        f"{_SESSION_NAME}:0",
        "-l",
        "80%",
        right_cmd,
    )

    # Make tmux invisible as a layout layer: no prefix key, no status bar,
    # mouse for pane switching only.
    for option, value in [
        ("mouse", "on"),
        ("status", "off"),
        ("prefix", "None"),
        ("prefix2", "None"),
    ]:
        _tmux("set-option", "-t", _SESSION_NAME, option, value)
    _tmux("set-option", "-w", "-t", f"{_SESSION_NAME}:0", "remain-on-exit", "on")
    # Option+h/l to switch between sidebar and session pane.
    # Option+j/k to change task selection from either pane.
    _tmux("bind-key", "-n", "M-h", "select-pane", "-t", f"{_SESSION_NAME}:0.0")
    _tmux("bind-key", "-n", "M-l", "select-pane", "-t", f"{_SESSION_NAME}:0.1")
    _tmux("bind-key", "-n", "M-j", "send-keys", "-t", f"{_SESSION_NAME}:0.0", "j")
    _tmux("bind-key", "-n", "M-k", "send-keys", "-t", f"{_SESSION_NAME}:0.0", "k")

    # Ensure cleanup on exit.
    prev_sigterm = signal.getsignal(signal.SIGTERM)
    prev_sighup = signal.getsignal(signal.SIGHUP)

    def _on_signal(signum: int, _frame: Any) -> None:
        _kill_session()
        sys.exit(128 + signum)

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGHUP, _on_signal)

    try:
        subprocess.run(["tmux", "attach", "-t", _SESSION_NAME])
    finally:
        _kill_session()
        signal.signal(signal.SIGTERM, prev_sigterm)
        signal.signal(signal.SIGHUP, prev_sighup)


def _sync_right_pane(info: dict[str, Any]) -> None:
    session_name = info.get("session_name")
    if info.get("status") == "broken":
        error = info.get("error_message") or "unknown error"
        cmd = f"echo {shlex.quote(error)}; read"
    elif info.get("status") == "paused":
        cmd = "echo 'Task is paused (r to resume)'; read"
    elif info.get("status") == "active" and session_name:
        cmd = _attach_cmd(session_name)
    else:
        cmd = "echo 'Task is not active'; read"
    subprocess.run(
        ["tmux", "respawn-pane", "-k", "-t", f"{_SESSION_NAME}:0.1", cmd],
        capture_output=True,
    )


def _status_label(info: dict[str, Any]) -> tuple[str, int]:
    """Return (label, curses_color_pair) for a task."""
    status = info.get("status", "idle")
    pr = info.get("pr_number")
    pr_draft = info.get("pr_draft", False)

    if status == "broken":
        return "broken", 6
    if status == "done":
        return "done", 3
    if status == "paused":
        return "paused", 4
    if pr and pr_draft:
        return f"PR #{pr} (draft)", 4
    if pr:
        return f"PR #{pr}", 2
    if status == "active":
        activity = info.get("activity")
        if activity == "idle":
            return "idle", 5
        return "working", 2
    if info.get("exhaust_count", 0) > 0:
        return "exhausted", 5
    return "pending", 5


def _pr_detail_lines(info: dict[str, Any]) -> list[tuple[str, int]]:
    """Return extra lines to display under a task that has an open PR.

    Each entry is (text, curses_color_pair).  Up to 2 lines:
      1. PR URL
      2. draft / approval status
    """
    pr_number = info.get("pr_number")
    if not pr_number:
        return []

    lines: list[tuple[str, int]] = []

    pr_url = info.get("pr_url")
    if pr_url:
        lines.append((f"     {pr_url}", 5))

    tags: list[str] = []
    pr_draft = info.get("pr_draft", False)
    pr_approved = info.get("pr_approved")

    if pr_draft:
        tags.append("draft")
    if pr_approved is True:
        tags.append("approved")
    elif pr_approved is False:
        tags.append("not approved")

    if tags:
        tag_str = ", ".join(tags)
        color = 4 if pr_draft else (2 if pr_approved else 5)
        lines.append((f"     [{tag_str}]", color))

    return lines


def _draw_sidebar(
    stdscr: curses.window,
    task_list: list[tuple[str, dict[str, Any]]],
    selected: int,
) -> None:
    stdscr.clear()
    max_y, max_x = stdscr.getmaxyx()

    # max_x - 1: curses raises an error when writing to the bottom-right
    # cell because the cursor would advance past the window boundary.
    usable_x = max_x - 1

    # Header
    stdscr.addnstr(0, 0, " taskpull ", usable_x, curses.A_BOLD | curses.color_pair(1))
    stdscr.addnstr(1, 0, "─" * usable_x, usable_x, curses.color_pair(5))

    # Task list
    row = 3
    for i, (tid, info) in enumerate(task_list):
        if row >= max_y - 2:
            break

        label, color = _status_label(info)
        runs = info.get("run_count", 0)

        marker = ">" if i == selected else " "
        attr = curses.A_BOLD if i == selected else 0

        line = f" {marker} {tid}"
        stdscr.addnstr(row, 0, line, usable_x, attr | curses.color_pair(1))

        status_str = f" {label} (run {runs})"
        status_col = len(line)
        if status_col + len(status_str) < usable_x:
            stdscr.addnstr(
                row,
                status_col,
                status_str,
                usable_x - status_col,
                curses.color_pair(color),
            )
        row += 1

        for detail_text, detail_color in _pr_detail_lines(info):
            if row >= max_y - 2:
                break
            stdscr.addnstr(
                row, 0, detail_text, usable_x, curses.color_pair(detail_color)
            )
            row += 1

    # Footer
    stdscr.addnstr(
        max_y - 1,
        0,
        " ⌥ j/k:sel  ⌥ h/l:pane  p:pause  r:resume  R:restart  q:quit",
        usable_x,
        curses.color_pair(5),
    )

    stdscr.refresh()


def run_sidebar(ipc_port: int) -> None:
    curses.wrapper(lambda stdscr: _sidebar_loop(stdscr, ipc_port))


def _sidebar_loop(stdscr: curses.window, ipc_port: int) -> None:
    curses.curs_set(0)
    curses.use_default_colors()

    # Color pairs: 1=header, 2=green(active), 3=blue(done), 4=yellow(draft),
    # 5=dim, 6=red(broken)
    curses.init_pair(1, curses.COLOR_WHITE, -1)
    curses.init_pair(2, curses.COLOR_GREEN, -1)
    curses.init_pair(3, curses.COLOR_BLUE, -1)
    curses.init_pair(4, curses.COLOR_YELLOW, -1)
    curses.init_pair(5, curses.COLOR_WHITE, -1)
    curses.init_pair(6, curses.COLOR_RED, -1)

    curses.halfdelay(20)  # 2 second timeout for getch

    selected = 0
    prev_selected = -1
    task_list: list[tuple[str, dict[str, Any]]] = []

    # Draw an empty screen immediately so there's no flash of shell content
    # before the first IPC fetch completes.
    stdscr.erase()
    stdscr.refresh()

    while True:
        tasks = _fetch_tasks(ipc_port)
        task_list = sorted(
            tasks.items(),
            key=lambda item: (
                item[1].get("status") == "broken"
                or item[1].get("exhaust_count", 0) > 0,
                item[0],
            ),
        )

        if selected >= len(task_list):
            selected = max(0, len(task_list) - 1)

        # Sync the right pane before drawing: respawn-pane may cause tmux
        # to redraw/resize, which corrupts the curses display.
        if task_list and selected != prev_selected:
            _sync_right_pane(task_list[selected][1])
            prev_selected = selected

        _draw_sidebar(stdscr, task_list, selected)

        try:
            key = stdscr.getch()
        except curses.error:
            continue

        if key == -1:
            continue

        if key == ord("q") or key == 3:  # 3 = Ctrl-C
            _kill_session()
            break

        if key == ord("j") or key == curses.KEY_DOWN:
            if task_list:
                selected = min(selected + 1, len(task_list) - 1)

        elif key == ord("k") or key == curses.KEY_UP:
            selected = max(selected - 1, 0)

        elif key == ord("r"):
            if task_list:
                tid, _info = task_list[selected]
                try:
                    send_command("127.0.0.1", ipc_port, "resume", task_id=tid)
                except (ConnectionRefusedError, OSError):
                    pass
                prev_selected = -1

        elif key == ord("p"):
            if task_list:
                tid, _info = task_list[selected]
                try:
                    send_command("127.0.0.1", ipc_port, "pause", task_id=tid)
                except (ConnectionRefusedError, OSError):
                    pass
                prev_selected = -1

        elif key == ord("R"):
            if task_list:
                tid, _info = task_list[selected]
                try:
                    send_command("127.0.0.1", ipc_port, "restart", task_id=tid)
                except (ConnectionRefusedError, OSError):
                    pass
                prev_selected = -1
