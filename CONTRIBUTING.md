# Contributing

## Development setup

Install dependencies for local development:

```
uv sync
```

Run the tool from the local source:

```
uv run taskpull <command>
```

## Installing the tool globally

taskpull is installed as a uv tool. After making changes, reinstall with:

```
uv tool install --force --reinstall /path/to/taskpull
```

`--reinstall` is required to force uv to rebuild the package from source rather
than using a cached wheel.

After reinstalling, restart the daemon and relaunch the TUI:

```
taskpull daemon restart
taskpull tui
```
