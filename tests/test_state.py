from __future__ import annotations

import json
from pathlib import Path

from taskpull.state import TaskState, TaskStatus, load_state, save_state


class TestTaskStateRoundTrip:
    def test_default_round_trip(self):
        ts = TaskState()
        assert TaskState.from_dict(ts.to_dict()) == ts

    def test_populated_round_trip(self):
        ts = TaskState(
            status=TaskStatus.ACTIVE,
            session_id="sess-123",
            session_name="taskpull-foo",
            pr_number=42,
            pr_url="https://github.com/owner/repo/pull/42",
            workspace="/tmp/ws",
            repo="https://github.com/owner/repo",
            run_count=3,
            exhaust_count=2,
            pr_draft=True,
            pr_approved=False,
            activity="active",
            proxy_secret="secret-abc",
            last_launched_at=1700000000,
            error_message="some error",
            setup_failure_count=2,
        )
        assert TaskState.from_dict(ts.to_dict()) == ts

    def test_adhoc_round_trip(self):
        ts = TaskState(
            adhoc="do the thing",
            repo="https://github.com/o/r",
        )
        assert TaskState.from_dict(ts.to_dict()) == ts

    def test_status_serializes_as_string(self):
        ts = TaskState(status=TaskStatus.BROKEN)
        d = ts.to_dict()
        assert d["status"] == "broken"


class TestTaskStateMigration:
    def test_pr_open_migrated_to_active(self):
        ts = TaskState.from_dict({"status": "pr_open"})
        assert ts.status == TaskStatus.ACTIVE

    def test_exhausted_bool_migrated_to_count(self):
        ts = TaskState.from_dict({"status": "idle", "exhausted": True})
        assert ts.exhaust_count == 1

    def test_exhausted_bool_not_overridden_by_count(self):
        ts = TaskState.from_dict(
            {"status": "idle", "exhausted": True, "exhaust_count": 5}
        )
        assert ts.exhaust_count == 5

    def test_worktree_migrated_to_workspace(self):
        ts = TaskState.from_dict({"status": "idle", "worktree": "/old/path"})
        assert ts.workspace == "/old/path"

    def test_worktree_not_overridden_by_workspace(self):
        ts = TaskState.from_dict(
            {"status": "idle", "worktree": "/old", "workspace": "/new"}
        )
        assert ts.workspace == "/new"

    def test_unknown_fields_ignored(self):
        ts = TaskState.from_dict({"status": "idle", "unknown_field": "value"})
        assert ts.status == TaskStatus.IDLE


class TestExhaustBackoff:
    def test_zero_exhaust_count(self):
        ts = TaskState(exhaust_count=0)
        assert ts.exhaust_backoff(300) == 0

    def test_exhaust_count_one(self):
        ts = TaskState(exhaust_count=1)
        assert ts.exhaust_backoff(300) == 2 * 300

    def test_exhaust_count_three(self):
        ts = TaskState(exhaust_count=3)
        assert ts.exhaust_backoff(300) == 8 * 300

    def test_capped_at_288x(self):
        ts = TaskState(exhaust_count=100)
        assert ts.exhaust_backoff(300) == 288 * 300


class TestSetupRetryBackoff:
    def test_zero_failure_count(self):
        ts = TaskState(setup_failure_count=0)
        assert ts.setup_retry_backoff(300) == 0

    def test_failure_count_one(self):
        ts = TaskState(setup_failure_count=1)
        assert ts.setup_retry_backoff(300) == 0

    def test_failure_count_two(self):
        ts = TaskState(setup_failure_count=2)
        assert ts.setup_retry_backoff(300) == 2 * 300

    def test_capped_at_24x(self):
        ts = TaskState(setup_failure_count=100)
        assert ts.setup_retry_backoff(300) == 24 * 300


class TestLoadSaveState:
    def test_round_trip_through_file(self, tmp_path: Path):
        state_file = tmp_path / "state.json"
        state = {
            "task-a": TaskState(status=TaskStatus.ACTIVE, run_count=2),
            "task-b": TaskState(status=TaskStatus.DONE, pr_number=10),
        }
        save_state(state_file, state)
        loaded = load_state(state_file)
        assert loaded == state

    def test_load_missing_file(self, tmp_path: Path):
        assert load_state(tmp_path / "missing.json") == {}

    def test_save_creates_parent_dir(self, tmp_path: Path):
        state_file = tmp_path / "state.json"
        save_state(state_file, {"x": TaskState()})
        assert state_file.exists()

    def test_save_is_valid_json(self, tmp_path: Path):
        state_file = tmp_path / "state.json"
        save_state(state_file, {"t": TaskState(status=TaskStatus.IDLE)})
        data = json.loads(state_file.read_text())
        assert data["t"]["status"] == "idle"
