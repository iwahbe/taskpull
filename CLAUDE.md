# taskpull

Pull-based multi-repo Claude Code task runner. Ensures each configured repository
always has at most one active Claude Code session working through a task list.
Tasks are either persistent markdown files with YAML frontmatter or ad-hoc tasks
created at runtime via `taskpull new`. When a repo lane becomes free the
supervisor picks the next eligible task, clones (or resolves) the repo, and
launches Claude inside an isolated Docker container.

## Architecture

All source lives under `src/taskpull/`.

| Module | Responsibility |
|---|---|
| `__main__.py` | CLI entry point, argument parsing, command dispatch |
| `supervisor.py` | Core async event loop — polling, PR/session checking, task launching |
| `task.py` | Task-file parsing (YAML frontmatter + prompt body) and discovery |
| `state.py` | `TaskState`/`TaskStatus`/`TaskGoal` dataclasses, JSON persistence |
| `session.py` | Docker container lifecycle — build image, launch, pause, kill, inspect |
| `gh_proxy.py` | HTTPS GitHub API proxy with per-task token isolation and mutation allowlisting |
| `hooks.py` | Generates `.claude/settings.local.json` + `mcp.json` for containers |
| `http_server.py` | Starlette/Uvicorn server for hook callbacks and the `task_exhausted` MCP tool |
| `ipc.py` | TCP/JSON IPC server for CLI ↔ daemon communication |
| `tui.py` | tmux-based terminal dashboard (curses sidebar + session pane) |
| `daemon.py` | Double-fork daemonisation, PID file management, signal handling |
| `credentials.py` | OAuth token retrieval from system keyring |
| `workspace.py` | Git clone, default-branch detection, local-path resolution |
| `Dockerfile` | Worker container image (Debian + mise, Claude CLI, gh, uv, ripgrep) |

## CLI commands

| Command | Effect |
|---|---|
| `taskpull daemon start` | Build Docker image, start daemon event loop |
| `taskpull daemon stop` | SIGTERM → graceful shutdown |
| `taskpull daemon restart` | Stop then start |
| `taskpull status` | Daemon PID + task states grouped by lane |
| `taskpull list` | Tabular view (task, status, PR#, CI, repo, runs) |
| `taskpull refresh` | Trigger an immediate poll cycle |
| `taskpull restart <task>` | Kill session, reset task to IDLE |
| `taskpull new <location> <prompt>` | Create an ad-hoc task (`--goal`, `--repo-lock`) |
| `taskpull` (no subcommand) | Launch tmux TUI dashboard |

## Key concepts

- **Lane** — `(repo, repo_lock)` tuple. At most one ACTIVE task per lane.
  Tasks targeting the same repo can run concurrently if they have distinct
  `repo_lock` values.
- **Task goal** — `pr` (done on merge/close), `issue` (done on creation), or `none`.
- **Repeating tasks** — re-queued after the goal is met.
- **Backoff** — exponential delay on exhaustion or setup failure before re-launch.
- **GH proxy** — per-task proxy tokens, GraphQL mutation allowlisting
  (`createPullRequest`, `createIssue`), REST write-path matching, and repo-node-ID
  caching. All Claude ↔ GitHub traffic routes through it.

## Tools

- **uv** for Python package management. Use `uv sync` to install dependencies and `uv run` to execute scripts.
- **ruff** for linting and formatting. Run `ruff check` and `ruff format` before committing.

## Code style

- `| None` is only for modeling genuinely nullable data. Do not use `X | None = None` as a fallback default when the caller can pass the value explicitly.
- Tests are function-based, not class-based. Write flat `def test_*` / `async def test_*` functions — do not group related tests under a `TestFoo` class.

## Reinstalling & restarting the daemon

After making changes, reinstall and restart so the user can test:

```
uv tool install --reinstall .
taskpull daemon restart
```

## Verifying UI changes

After making a TUI change, always verify it works by capturing the tmux pane:

```
tmux capture-pane -t taskpull-tui:0.0 -p
```

Do not consider a UI change complete until you have confirmed the output.
