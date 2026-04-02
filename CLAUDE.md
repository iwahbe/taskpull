# taskpull

Pull-based multi-repo Claude Code task runner.

## Tools

- **uv** for Python package management. Use `uv sync` to install dependencies and `uv run` to execute scripts.
- **ruff** for linting and formatting. Run `ruff check` and `ruff format` before committing.

## Code style

- `| None` is only for modeling genuinely nullable data. Do not use `X | None = None` as a fallback default when the caller can pass the value explicitly.

## Verifying UI changes

After making a TUI change, always verify it works by capturing the tmux pane:

```
tmux capture-pane -t taskpull-tui:0.0 -p
```

Do not consider a UI change complete until you have confirmed the output.
