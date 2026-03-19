from __future__ import annotations

import os
import signal
import sys
import time
from pathlib import Path

from .config import Config
from .ipc import send_command


def read_pid(config: Config) -> int | None:
    try:
        return int(config.pid_file.read_text().strip())
    except (FileNotFoundError, ValueError):
        return None


def is_daemon_running(config: Config) -> tuple[bool, int | None]:
    pid = read_pid(config)
    if pid is None:
        return False, None
    try:
        os.kill(pid, 0)
    except OSError:
        return False, pid
    return True, pid


def write_pid(config: Config) -> None:
    config.pid_file.write_text(str(os.getpid()) + "\n")


def remove_pid(config: Config) -> None:
    config.pid_file.unlink(missing_ok=True)


def daemonize(log_file: Path) -> None:
    if os.fork() > 0:
        sys.exit(0)

    os.setsid()

    if os.fork() > 0:
        sys.exit(0)

    sys.stdout.flush()
    sys.stderr.flush()

    devnull = open(os.devnull, "r")
    os.dup2(devnull.fileno(), sys.stdin.fileno())

    log_fd = open(log_file, "a")
    os.dup2(log_fd.fileno(), sys.stdout.fileno())
    os.dup2(log_fd.fileno(), sys.stderr.fileno())


def stop_daemon(config: Config) -> None:
    running, pid = is_daemon_running(config)
    if not running:
        if pid is not None:
            # Stale PID file — process is already dead, just clean up.
            remove_pid(config)
            print(f"removed stale PID file (PID {pid})")
        else:
            print("daemon is not running (start with `taskpull start`)")
        sys.exit(1)

    assert pid is not None

    # Verify this is actually our daemon by checking the socket, not just the PID
    # (the PID could have been reused by an unrelated process).
    try:
        send_command(config.sock_file, "ping")
    except (OSError, ValueError):
        remove_pid(config)
        print(f"removed stale PID file (PID {pid} is not the taskpull daemon)")
        sys.exit(1)

    os.kill(pid, signal.SIGTERM)

    for _ in range(50):
        try:
            os.kill(pid, 0)
        except OSError:
            remove_pid(config)
            print(f"daemon (PID {pid}) stopped")
            return
        time.sleep(0.1)

    print(f"daemon (PID {pid}) did not stop within 5 seconds")
    sys.exit(1)
