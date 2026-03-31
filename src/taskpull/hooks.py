from __future__ import annotations

import json
import logging
from pathlib import Path

log = logging.getLogger(__name__)


def write_hooks_config(
    worktree: Path,
    task_id: str,
    ipc_port: int,
) -> Path:
    notify_cmd = (
        f"taskpull for-task notify --host 127.0.0.1 --port {ipc_port}"
        f" --task-id {task_id}"
    )

    mcp_config = {
        "mcpServers": {
            "taskpull": {
                "command": "taskpull",
                "args": [
                    "for-task",
                    "mcp-server",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(ipc_port),
                    "--task-id",
                    task_id,
                ],
            },
        },
    }

    config = {
        "hooks": {
            "SessionStart": [
                {
                    "matcher": "",
                    "hooks": [
                        {
                            "type": "command",
                            "command": notify_cmd,
                        }
                    ],
                }
            ],
            "PreToolUse": [
                {
                    "matcher": "",
                    "hooks": [
                        {
                            "type": "command",
                            "command": notify_cmd,
                        }
                    ],
                }
            ],
            "PostToolUse": [
                {
                    "matcher": "Bash",
                    "hooks": [
                        {
                            "type": "command",
                            "command": notify_cmd,
                        }
                    ],
                }
            ],
            "Stop": [
                {
                    "matcher": "",
                    "hooks": [
                        {
                            "type": "command",
                            "command": notify_cmd,
                        }
                    ],
                }
            ],
        },
    }

    claude_dir = worktree / ".claude"
    claude_dir.mkdir(parents=True, exist_ok=True)
    settings_path = claude_dir / "settings.local.json"
    with open(settings_path, "w") as f:
        json.dump(config, f, indent=2)
        f.write("\n")

    mcp_path = claude_dir / "mcp.json"
    with open(mcp_path, "w") as f:
        json.dump(mcp_config, f, indent=2)
        f.write("\n")

    return mcp_path
