from __future__ import annotations

import os
import signal
import sys
import time
from pathlib import Path

from .config import Config
from .ipc import send_command


def read_pid_file(pid_file: Path) -> int | None:
    try:
        return int(pid_file.read_text().strip())
    except FileNotFoundError, ValueError:
        return None


def read_pid(config: Config) -> int | None:
    return read_pid_file(config.pid_file)


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


def daemonize(log_file: Path, pid_file: Path) -> int:
    """Double-fork to daemonize.

    Returns a file descriptor that the daemon must write a byte to once it is
    ready.  The original (parent) process blocks on this fd and exits only
    after the daemon signals readiness — or exits non-zero if the pipe closes
    without a signal (daemon crashed before becoming ready).
    """
    read_fd, write_fd = os.pipe()

    if os.fork() > 0:
        os.close(write_fd)
        # Block until the daemon writes a readiness byte or the pipe closes.
        data = os.read(read_fd, 1)
        os.close(read_fd)
        if data:
            pid = read_pid_file(pid_file)
            print(f"daemon started (PID {pid})")
        sys.exit(0 if data else 1)

    os.setsid()

    if os.fork() > 0:
        os.close(read_fd)
        os.close(write_fd)
        os._exit(0)

    os.close(read_fd)

    sys.stdout.flush()
    sys.stderr.flush()

    devnull = open(os.devnull, "r")
    os.dup2(devnull.fileno(), sys.stdin.fileno())

    log_fd = open(log_file, "a")
    os.dup2(log_fd.fileno(), sys.stdout.fileno())
    os.dup2(log_fd.fileno(), sys.stderr.fileno())

    return write_fd


def stop_daemon(config: Config) -> None:
    running, pid = is_daemon_running(config)
    if not running:
        if pid is not None:
            # Stale PID file — process is already dead, just clean up.
            remove_pid(config)
            print(f"removed stale PID file (PID {pid})")
        else:
            print("daemon is not running (start with `taskpull daemon start`)")
        sys.exit(1)

    assert pid is not None

    # Verify this is actually our daemon by checking the socket, not just the PID
    # (the PID could have been reused by an unrelated process).
    try:
        send_command("127.0.0.1", config.ipc_port, "ping")
    except OSError, ValueError:
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
