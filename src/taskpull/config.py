from dataclasses import dataclass
from pathlib import Path
from typing import Any
import tomllib

_DEFAULT_DIR = Path.home() / ".taskpull"


@dataclass(frozen=True)
class Config:
    poll_interval: int = 300
    ipc_port: int = 19471
    gh_proxy_port: int = 19472
    http_port: int = 19473
    user_dir: Path = _DEFAULT_DIR
    docker_image: str = "taskpull-worker"

    @property
    def tasks_dir(self) -> Path:
        return self.user_dir / "tasks"

    @property
    def state_file(self) -> Path:
        return self.user_dir / "state.json"

    @property
    def workspace_dir(self) -> Path:
        return self.user_dir / "workspace"

    @property
    def pid_file(self) -> Path:
        return self.user_dir / "daemon.pid"

    @property
    def certs_dir(self) -> Path:
        return self.user_dir / "certs"

    @property
    def log_file(self) -> Path:
        return self.user_dir / "daemon.log"


def load_config(user_dir: Path = _DEFAULT_DIR) -> Config:
    config_file = user_dir / "config.toml"
    kwargs: dict[str, Any] = {"user_dir": user_dir}
    if config_file.exists():
        with open(config_file, "rb") as f:
            data = tomllib.load(f)
        if "poll_interval" in data:
            kwargs["poll_interval"] = int(data["poll_interval"])
        if "ipc_port" in data:
            kwargs["ipc_port"] = int(data["ipc_port"])
        if "docker_image" in data:
            kwargs["docker_image"] = str(data["docker_image"])
        if "gh_proxy_port" in data:
            kwargs["gh_proxy_port"] = int(data["gh_proxy_port"])
        if "http_port" in data:
            kwargs["http_port"] = int(data["http_port"])
    return Config(**kwargs)
