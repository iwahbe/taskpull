from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
import tomllib

_DEFAULT_DIR = Path.home() / ".taskpull"


@dataclass(frozen=True)
class Config:
    poll_interval: int = 300
    user_dir: Path = _DEFAULT_DIR

    @property
    def tasks_dir(self) -> Path:
        return self.user_dir / "tasks"

    @property
    def state_file(self) -> Path:
        return self.user_dir / "state.json"

    @property
    def events_dir(self) -> Path:
        return self.user_dir / "events"

    @property
    def worktrees_dir(self) -> Path:
        return self.user_dir / "worktrees"

    @property
    def notify_script(self) -> Path:
        return Path(str(files("taskpull").joinpath("notify.py")))


def load_config(user_dir: Path = _DEFAULT_DIR) -> Config:
    config_file = user_dir / "config.toml"
    kwargs: dict = {"user_dir": user_dir}
    if config_file.exists():
        with open(config_file, "rb") as f:
            data = tomllib.load(f)
        if "poll_interval" in data:
            kwargs["poll_interval"] = int(data["poll_interval"])
    return Config(**kwargs)
