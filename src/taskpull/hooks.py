from __future__ import annotations

import json
import logging
from pathlib import Path

log = logging.getLogger(__name__)


def write_hooks_config(
    workspace: Path,
    task_id: str,
    http_port: int,
) -> Path:
    base_url = f"http://host.docker.internal:{http_port}"
    notify_cmd = (
        f"curl -s --max-time 10 -X POST"
        f" -H 'Content-Type: application/json'"
        f" -d @- {base_url}/hooks/{task_id}/notify"
    )

    mcp_config = {
        "mcpServers": {
            "taskpull": {
                "url": f"{base_url}/mcp/{task_id}",
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

    claude_dir = workspace / ".claude"
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
