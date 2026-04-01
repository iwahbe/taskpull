from __future__ import annotations

import asyncio
import importlib.resources
import json
import logging
import os
import shutil
import tempfile
from pathlib import Path

log = logging.getLogger(__name__)

_PROMPT_FILENAME = ".taskpull-prompt.txt"


_PROJECT_DIR = Path("/Users/ianwahbe/Projects/taskpull")


async def build_image(image_name: str) -> None:
    """Build the worker Docker image from the bundled Dockerfile and wheel."""
    pkg = importlib.resources.files("taskpull")
    with tempfile.TemporaryDirectory() as ctx_dir:
        ctx = Path(ctx_dir)
        shutil.copy2(str(pkg.joinpath("Dockerfile")), ctx / "Dockerfile")

        wheel_proc = await asyncio.create_subprocess_exec(
            "uv",
            "build",
            "--wheel",
            "--out-dir",
            str(ctx),
            str(_PROJECT_DIR),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        wheel_out, _ = await wheel_proc.communicate()
        if wheel_proc.returncode != 0:
            raise RuntimeError(
                f"uv build failed (rc={wheel_proc.returncode}):\n{wheel_out.decode()}"
            )

        build_env = {**os.environ, "BUILDX_BUILDER": ""}
        proc = await asyncio.create_subprocess_exec(
            "docker",
            "buildx",
            "build",
            "--load",
            "-t",
            image_name,
            str(ctx),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=build_env,
        )
        stdout, _ = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(
                f"docker build failed (rc={proc.returncode}):\n{stdout.decode()}"
            )
    log.info("built docker image %s", image_name)


_CLAUDE_SETTINGS = json.dumps(
    {
        "skipDangerousModePermissionPrompt": True,
    }
)


async def launch_session(
    name: str,
    worktree: Path,
    prompt: str,
    run_count: int,
    task_id: str,
    mcp_config: Path,
    docker_image: str,
    env: dict[str, str],
) -> str:
    prompt_file = worktree / _PROMPT_FILENAME
    prompt_file.write_text(prompt)

    home = Path.home()
    cmd = [
        "docker",
        "run",
        "-d",
        "-t",
        "--name",
        name,
        "-v",
        f"{worktree}:/workspace",
        "-v",
        f"{home / '.claude'}:/home/worker/.claude",
    ]
    for key, value in env.items():
        cmd.extend(["-e", f"{key}={value}"])
    cmd.append(docker_image)
    mcp_rel = mcp_config.relative_to(worktree)

    # Write the Claude launch script and tmux config as files inside the
    # container to avoid nested shell-quoting issues.
    claude_script = (
        "#!/bin/bash\n"
        "claude "
        "--dangerously-skip-permissions "
        f"--settings '{_CLAUDE_SETTINGS}' "
        # --remote-control doesn't work in Docker containers yet.
        # See: https://github.com/anthropics/claude-code/issues/27848
        "--remote-control "
        f"--name '{task_id} (run {run_count})' "
        f"--mcp-config /workspace/{mcp_rel!s} "
        f"< /workspace/{_PROMPT_FILENAME}\n"
        f"rm -f /workspace/{_PROMPT_FILENAME}\n"
    )
    claude_script_path = worktree / ".taskpull-run.sh"
    claude_script_path.write_text(claude_script)
    claude_script_path.chmod(0o755)

    tmux_conf = (
        "set-option -g prefix None\n"
        "unbind C-b\n"
        "set-option -g status off\n"
        "set-option -g remain-on-exit on\n"
        "set-option -g detach-on-destroy off\n"
    )
    tmux_conf_path = worktree / ".taskpull-tmux.conf"
    tmux_conf_path.write_text(tmux_conf)

    claude_json = json.dumps(
        {
            "hasCompletedOnboarding": True,
            "lastOnboardingVersion": "9.9.9",
            "theme": "dark",
            "projects": {
                "/workspace": {
                    "hasTrustDialogAccepted": True,
                    "hasCompletedProjectOnboarding": True,
                }
            },
        }
    )

    bash_script = (
        "cd /workspace && "
        f"echo '{claude_json}' > ~/.claude.json && "
        "cp /workspace/.taskpull-tmux.conf ~/.tmux.conf && "
        "tmux new-session -d -s claude /workspace/.taskpull-run.sh && "
        "tail -f /dev/null"
    )

    cmd.extend(["bash", "-c", bash_script])

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f"docker run failed (rc={proc.returncode}): {stderr.decode().strip()}"
        )
    log.info("launched container %s", name)
    return name


async def session_alive(name: str) -> bool:
    proc = await asyncio.create_subprocess_exec(
        "docker",
        "inspect",
        "--format",
        "{{.State.Running}}",
        name,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()
    return stdout.decode().strip() == "true"


async def kill_session(name: str) -> None:
    proc = await asyncio.create_subprocess_exec(
        "docker",
        "rm",
        "-f",
        name,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await proc.communicate()
