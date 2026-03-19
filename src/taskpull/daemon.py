from __future__ import annotations

import os
import signal
import sys
import time
from pathlib import Path

from .config import Config


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
        print("daemon is not running (start with `taskpull start`)")
        sys.exit(1)

    assert pid is not None
    os.kill(pid, signal.SIGTERM)

    for _ in range(50):
        try:
            os.kill(pid, 0)
        except OSError:
            print(f"daemon (PID {pid}) stopped")
            return
        time.sleep(0.1)

    print(f"daemon (PID {pid}) did not stop within 5 seconds")
    sys.exit(1)
