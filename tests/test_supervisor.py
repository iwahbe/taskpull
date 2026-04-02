from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from taskpull.config import Config
from taskpull.gh_proxy import GHProxy
from taskpull.state import TaskState, TaskStatus
from taskpull.supervisor import (
    PrInfo,
    _build_prompt,
    _cleanup_task,
    _phase2_check_prs,
    _phase3_check_sessions,
    _phase4_launch,
    _reset_task,
)
from taskpull.task import TaskFile


@dataclass
class FakeBackend:
    """In-memory session backend that records all calls."""

    launched: list[dict[str, Any]] = field(default_factory=list)
    killed: list[str] = field(default_factory=list)
    paused: list[str] = field(default_factory=list)
    unpaused: list[str] = field(default_factory=list)

    alive_sessions: dict[str, bool] = field(default_factory=dict)
    claude_exited_sessions: dict[str, bool] = field(default_factory=dict)
    exit_info: dict[str, tuple[int | None, str]] = field(default_factory=dict)
    paused_sessions: dict[str, bool] = field(default_factory=dict)

    async def build_image(self, image_name: str) -> None:
        pass

    async def launch_session(
        self,
        name: str,
        workspace: Path,
        prompt: str,
        run_count: int,
        task_id: str,
        mcp_config: Path,
        docker_image: str,
        env: dict[str, str],
        ca_cert: Path | None,
        gh_proxy_port: int,
    ) -> str:
        self.launched.append(
            {
                "name": name,
                "workspace": workspace,
                "prompt": prompt,
                "run_count": run_count,
                "task_id": task_id,
                "mcp_config": mcp_config,
                "docker_image": docker_image,
                "env": env,
                "ca_cert": ca_cert,
                "gh_proxy_port": gh_proxy_port,
            }
        )
        return name

    async def session_alive(self, name: str) -> bool:
        return self.alive_sessions.get(name, False)

    async def session_claude_exited(self, name: str) -> bool:
        return self.claude_exited_sessions.get(name, False)

    async def session_exit_info(self, name: str) -> tuple[int | None, str]:
        return self.exit_info.get(name, (None, ""))

    async def kill_session(self, name: str) -> None:
        self.killed.append(name)

    async def pause_session(self, name: str) -> None:
        self.paused.append(name)

    async def unpause_session(self, name: str) -> None:
        self.unpaused.append(name)

    async def session_paused(self, name: str) -> bool:
        return self.paused_sessions.get(name, False)


def _make_gh_proxy(tmp_path: Path) -> GHProxy:
    """Create a GHProxy with dummy cert paths (never actually starts)."""
    cert = tmp_path / "dummy.pem"
    cert.write_text("dummy")
    return GHProxy("fake-token", cert, cert, cert)


class TestBuildPrompt:
    def test_non_repeat(self):
        task = TaskFile(
            repo="https://github.com/o/r", repeat=False, prompt="Fix the bug."
        )
        prompt = _build_prompt(task)
        assert "Fix the bug." in prompt
        assert "gh pr create" in prompt
        assert "task_exhausted" not in prompt

    def test_repeat(self):
        task = TaskFile(
            repo="https://github.com/o/r", repeat=True, prompt="Check logs."
        )
        prompt = _build_prompt(task)
        assert "Check logs." in prompt
        assert "task_exhausted" in prompt


class TestResetTask:
    def test_resets_all_fields(self):
        ts = TaskState(
            status=TaskStatus.ACTIVE,
            session_id="s",
            session_name="n",
            pr_number=1,
            pr_url="u",
            workspace="/w",
            pr_draft=True,
            pr_approved=True,
            activity="active",
            proxy_secret="ps",
            error_message="err",
        )
        _reset_task(ts)
        assert ts == TaskState(
            status=TaskStatus.IDLE,
            run_count=ts.run_count,
            repo=ts.repo,
            exhaust_count=ts.exhaust_count,
            last_launched_at=ts.last_launched_at,
        )


class TestCleanupTask:
    @pytest.mark.asyncio
    async def test_kills_session(self, tmp_path: Path):
        backend = FakeBackend()
        gh_proxy = _make_gh_proxy(tmp_path)
        ts = TaskState(session_name="taskpull-foo")
        await _cleanup_task(ts, gh_proxy, backend)
        assert backend.killed == ["taskpull-foo"]

    @pytest.mark.asyncio
    async def test_unregisters_proxy(self, tmp_path: Path):
        gh_proxy = _make_gh_proxy(tmp_path)
        secret = gh_proxy.register_task("owner/repo", "task-1")
        ts = TaskState(proxy_secret=secret)
        await _cleanup_task(ts, gh_proxy, FakeBackend())
        assert gh_proxy._token_map.get(secret) is None


class TestPhase3CheckSessions:
    @pytest.mark.asyncio
    async def test_dead_session_resets_to_idle(self, tmp_path: Path):
        backend = FakeBackend()
        gh_proxy = _make_gh_proxy(tmp_path)
        state = {
            "task-a": TaskState(
                status=TaskStatus.ACTIVE, session_name="taskpull-task-a"
            ),
        }

        await _phase3_check_sessions(state, gh_proxy, backend)

        assert state["task-a"].status == TaskStatus.IDLE
        assert state["task-a"].session_name is None
        assert backend.killed == ["taskpull-task-a"]

    @pytest.mark.asyncio
    async def test_alive_session_not_touched(self, tmp_path: Path):
        backend = FakeBackend()
        backend.alive_sessions["taskpull-task-a"] = True
        gh_proxy = _make_gh_proxy(tmp_path)
        state = {
            "task-a": TaskState(
                status=TaskStatus.ACTIVE, session_name="taskpull-task-a"
            ),
        }

        await _phase3_check_sessions(state, gh_proxy, backend)

        assert state["task-a"].status == TaskStatus.ACTIVE
        assert backend.killed == []

    @pytest.mark.asyncio
    async def test_claude_exited_resets_to_idle(self, tmp_path: Path):
        backend = FakeBackend()
        backend.alive_sessions["taskpull-task-a"] = True
        backend.claude_exited_sessions["taskpull-task-a"] = True
        gh_proxy = _make_gh_proxy(tmp_path)
        state = {
            "task-a": TaskState(
                status=TaskStatus.ACTIVE, session_name="taskpull-task-a"
            ),
        }

        await _phase3_check_sessions(state, gh_proxy, backend)

        assert state["task-a"].status == TaskStatus.IDLE
        assert backend.killed == ["taskpull-task-a"]

    @pytest.mark.asyncio
    async def test_nonzero_exit_marks_broken(self, tmp_path: Path):
        backend = FakeBackend()
        backend.exit_info["taskpull-task-a"] = (1, "segfault")
        gh_proxy = _make_gh_proxy(tmp_path)
        state = {
            "task-a": TaskState(
                status=TaskStatus.ACTIVE, session_name="taskpull-task-a"
            ),
        }

        await _phase3_check_sessions(state, gh_proxy, backend)

        assert state["task-a"].status == TaskStatus.BROKEN
        assert state["task-a"].error_message == "segfault"

    @pytest.mark.asyncio
    async def test_skips_non_active_tasks(self, tmp_path: Path):
        backend = FakeBackend()
        gh_proxy = _make_gh_proxy(tmp_path)
        state = {
            "task-a": TaskState(status=TaskStatus.IDLE),
            "task-b": TaskState(status=TaskStatus.DONE),
        }

        await _phase3_check_sessions(state, gh_proxy, backend)

        assert state["task-a"].status == TaskStatus.IDLE
        assert state["task-b"].status == TaskStatus.DONE


class TestPhase2CheckPrs:
    @pytest.mark.asyncio
    async def test_merged_repeat_resets_to_idle(self, tmp_path: Path):
        backend = FakeBackend()
        gh_proxy = _make_gh_proxy(tmp_path)
        state = {
            "task-a": TaskState(
                status=TaskStatus.ACTIVE,
                pr_number=42,
                repo="https://github.com/owner/repo",
                session_name="taskpull-task-a",
                exhaust_count=3,
            ),
        }
        tasks = {
            "task-a": TaskFile(
                repo="https://github.com/owner/repo",
                repeat=True,
                prompt="Do stuff",
            ),
        }

        with patch(
            "taskpull.supervisor._check_pr_state",
            return_value=PrInfo(state="MERGED", is_draft=False, approved=True),
        ):
            await _phase2_check_prs(state, tasks, gh_proxy, backend)

        assert state["task-a"].status == TaskStatus.IDLE
        assert state["task-a"].exhaust_count == 0
        assert backend.killed == ["taskpull-task-a"]

    @pytest.mark.asyncio
    async def test_merged_no_repeat_marks_done(self, tmp_path: Path):
        backend = FakeBackend()
        gh_proxy = _make_gh_proxy(tmp_path)
        state = {
            "task-a": TaskState(
                status=TaskStatus.ACTIVE,
                pr_number=10,
                repo="https://github.com/owner/repo",
                session_name="taskpull-task-a",
            ),
        }
        tasks = {
            "task-a": TaskFile(
                repo="https://github.com/owner/repo",
                repeat=False,
                prompt="Do stuff",
            ),
        }

        with patch(
            "taskpull.supervisor._check_pr_state",
            return_value=PrInfo(state="MERGED", is_draft=False, approved=None),
        ):
            await _phase2_check_prs(state, tasks, gh_proxy, backend)

        assert state["task-a"].status == TaskStatus.DONE

    @pytest.mark.asyncio
    async def test_closed_resets_to_idle(self, tmp_path: Path):
        backend = FakeBackend()
        gh_proxy = _make_gh_proxy(tmp_path)
        state = {
            "task-a": TaskState(
                status=TaskStatus.ACTIVE,
                pr_number=5,
                repo="https://github.com/owner/repo",
                session_name="taskpull-task-a",
            ),
        }
        tasks = {
            "task-a": TaskFile(
                repo="https://github.com/owner/repo",
                repeat=False,
                prompt="Do stuff",
            ),
        }

        with patch(
            "taskpull.supervisor._check_pr_state",
            return_value=PrInfo(state="CLOSED", is_draft=False, approved=None),
        ):
            await _phase2_check_prs(state, tasks, gh_proxy, backend)

        assert state["task-a"].status == TaskStatus.IDLE

    @pytest.mark.asyncio
    async def test_open_updates_draft_and_approved(self, tmp_path: Path):
        backend = FakeBackend()
        gh_proxy = _make_gh_proxy(tmp_path)
        state = {
            "task-a": TaskState(
                status=TaskStatus.ACTIVE,
                pr_number=7,
                repo="https://github.com/owner/repo",
            ),
        }
        tasks = {
            "task-a": TaskFile(
                repo="https://github.com/owner/repo",
                repeat=False,
                prompt="Do stuff",
            ),
        }

        with patch(
            "taskpull.supervisor._check_pr_state",
            return_value=PrInfo(state="OPEN", is_draft=True, approved=False),
        ):
            await _phase2_check_prs(state, tasks, gh_proxy, backend)

        assert state["task-a"].status == TaskStatus.ACTIVE
        assert state["task-a"].pr_draft is True
        assert state["task-a"].pr_approved is False


def _mock_clone_repo(workspace_dir: Path):
    """Return an async function that creates a local dir instead of cloning."""

    async def _clone(ws_dir: Path, repo_url: str, task_id: str, run_count: int) -> Path:
        dest = ws_dir / task_id / str(run_count)
        dest.mkdir(parents=True, exist_ok=True)
        return dest

    return _clone


class TestPhase4Launch:
    @pytest.mark.asyncio
    async def test_launches_idle_task(self, tmp_path: Path):
        backend = FakeBackend()
        gh_proxy = _make_gh_proxy(tmp_path)

        config = Config(
            poll_interval=300,
            ipc_port=19471,
            gh_proxy_port=19472,
            user_dir=tmp_path,
        )

        state: dict[str, TaskState] = {}
        tasks = {
            "task-a": TaskFile(
                repo="https://github.com/owner/repo",
                repeat=False,
                prompt="Fix bug",
            ),
        }

        with patch(
            "taskpull.supervisor.clone_repo",
            side_effect=_mock_clone_repo(config.workspace_dir),
        ):
            await _phase4_launch(
                config, state, tasks, "claude-token", gh_proxy, backend
            )

        assert "task-a" in state
        assert state["task-a"].status == TaskStatus.ACTIVE
        assert state["task-a"].run_count == 1
        assert len(backend.launched) == 1
        assert backend.launched[0]["name"] == "taskpull-task-a"
        assert backend.launched[0]["env"]["CLAUDE_CODE_OAUTH_TOKEN"] == "claude-token"

    @pytest.mark.asyncio
    async def test_skips_active_lane(self, tmp_path: Path):
        backend = FakeBackend()
        gh_proxy = _make_gh_proxy(tmp_path)

        config = Config(user_dir=tmp_path)

        repo = "https://github.com/owner/repo"
        state: dict[str, TaskState] = {
            "task-a": TaskState(status=TaskStatus.ACTIVE, repo=repo),
        }
        tasks = {
            "task-a": TaskFile(repo=repo, repeat=False, prompt="A"),
            "task-b": TaskFile(repo=repo, repeat=False, prompt="B"),
        }

        with patch(
            "taskpull.supervisor.clone_repo",
            side_effect=_mock_clone_repo(config.workspace_dir),
        ):
            await _phase4_launch(config, state, tasks, "token", gh_proxy, backend)

        assert state["task-b"].status == TaskStatus.IDLE
        assert len(backend.launched) == 0

    @pytest.mark.asyncio
    async def test_respects_exhaust_backoff(self, tmp_path: Path):
        backend = FakeBackend()
        gh_proxy = _make_gh_proxy(tmp_path)

        config = Config(user_dir=tmp_path, poll_interval=300)

        state: dict[str, TaskState] = {
            "task-a": TaskState(
                exhaust_count=1,
                last_launched_at=int(time.time()),
            ),
        }
        tasks = {
            "task-a": TaskFile(
                repo="https://github.com/owner/repo",
                repeat=True,
                prompt="Check",
            ),
        }

        with patch(
            "taskpull.supervisor.clone_repo",
            side_effect=_mock_clone_repo(config.workspace_dir),
        ):
            await _phase4_launch(config, state, tasks, "token", gh_proxy, backend)

        assert len(backend.launched) == 0

    @pytest.mark.asyncio
    async def test_round_robins_by_last_launched(self, tmp_path: Path):
        backend = FakeBackend()
        gh_proxy = _make_gh_proxy(tmp_path)

        config = Config(user_dir=tmp_path)

        repo = "https://github.com/owner/repo"
        state: dict[str, TaskState] = {
            "task-a": TaskState(last_launched_at=200),
            "task-b": TaskState(last_launched_at=100),
        }
        tasks = {
            "task-a": TaskFile(repo=repo, repeat=True, prompt="A"),
            "task-b": TaskFile(repo=repo, repeat=True, prompt="B"),
        }

        with patch(
            "taskpull.supervisor.clone_repo",
            side_effect=_mock_clone_repo(config.workspace_dir),
        ):
            await _phase4_launch(config, state, tasks, "token", gh_proxy, backend)

        assert len(backend.launched) == 1
        assert backend.launched[0]["name"] == "taskpull-task-b"
