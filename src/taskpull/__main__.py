from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path

from .config import load_config
from .supervisor import run


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="taskpull",
        description="Pull-based multi-repo Claude Code task runner",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single poll cycle then exit",
    )
    parser.add_argument(
        "--user-dir",
        type=Path,
        default=Path.home() / ".taskpull",
        help="User data directory (default: ~/.taskpull)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    config = load_config(args.user_dir)
    asyncio.run(run(config, once=args.once))


if __name__ == "__main__":
    main()
