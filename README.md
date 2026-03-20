# taskpull

> [!WARNING]
> This project is in alpha and under active development. Expect breaking changes.

Pull-based multi-repo Claude Code task runner. Ensures each configured repo always has one active Claude Code session working through a task list, visible in your Claude app via Remote Control.

## Requirements

- `claude` CLI >= 2.1.51, logged in (`claude /login`)
- `gh` CLI, authenticated
- `tmux`
- `uv` (Python 3.12+)

## Installing

```bash
uv tool install git+https://github.com/iwahbe/taskpull.git
```

## Setup

1. Create `~/.taskpull/` with your config and tasks:

```
~/.taskpull/
├── config.toml     # poll interval and other settings
├── tasks/          # task definition files
│   └── my-task.md
├── state.json      # managed by supervisor (gitignored equivalent)
├── events/         # hook event files (managed by supervisor)
└── worktrees/      # git worktrees for active tasks
```

2. Run `taskpull start`.

## Task file format

Create a `.md` file in `~/.taskpull/tasks/`. The filename (minus extension) is the task ID.

```markdown
---
repo: ~/src/my-repo
repeat: true
---

Your prompt to Claude goes here. This is passed verbatim.
```

### Fields

| Field       | Required | Description |
|-------------|----------|-------------|
| `repo`      | yes      | Path to the local repo clone |
| `repeat`    | no       | `true` to re-run after each PR merge until `TASKPULL_DONE`. Default `false` |
| `repo_lock` | no       | Concurrency key. Tasks with the same `repo` and `repo_lock` won't run simultaneously. Defaults to `repo` |

Claude chooses its own branch name when creating a PR.

## Configuration

`~/.taskpull/config.toml`:

```toml
# Seconds between poll cycles
poll_interval = 300
```

## How it works

The supervisor polls every `poll_interval` seconds and runs four phases:

1. **Process events** — read hook events from Claude sessions. When Claude creates a PR, the hook captures the PR number and associates it with the task.
2. **Check PRs** — if a taskpull PR was merged or closed, free that repo's slot.
3. **Check sessions** — if a Claude session exited without creating a PR, reset the task.
4. **Launch** — for each repo with no active/pr-open task, pick the next eligible task, create a worktree, configure hooks, and start a Claude session.

Claude is instructed to push its branch and create a PR via `gh pr create` when work is complete. A per-worktree `.claude/settings.local.json` configures hooks that report session starts and PR creation events back to the supervisor via JSONL event files.

### Constraints

- One active or pr-open task per repo at a time.
- Each run starts from a fresh worktree off `origin/main` (or whatever the default branch is).
- Worktrees live in `~/.taskpull/worktrees/<task-id>/<run-count>`.

## Interacting with sessions

All sessions register with Remote Control. Open the Claude app (iOS, Android, or claude.ai/code) and you'll see your running sessions listed by name. Tap any one to steer it, approve actions, or just watch.

## Commands

```bash
# Start the daemon
taskpull start

# Stop the daemon
taskpull stop

# Check whether the daemon is running
taskpull status

# Show tasks and their states
taskpull list

# Trigger an immediate poll cycle
taskpull refresh

# Use a different user directory
taskpull --user-dir /path/to/dir start
```

## State

`~/.taskpull/state.json` tracks active tasks, PR numbers, session IDs, and run counts. Managed by the supervisor. You can manually edit it to reset `exhausted` flags or change status.

`~/.taskpull/events/` contains per-task JSONL files written by Claude Code hooks. These are consumed and cleared by the supervisor during each poll cycle.
