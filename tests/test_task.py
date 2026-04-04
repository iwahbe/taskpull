from __future__ import annotations

from pathlib import Path

import pytest

from taskpull.task import (
    TaskFile,
    discover_md_tasks,
    discover_tasks,
    parse_task,
    task_id_from_path,
)


class TestParseTask:
    def test_basic_task(self, tmp_path: Path):
        p = tmp_path / "my-task.md"
        p.write_text("---\nrepo: https://github.com/owner/repo\n---\nDo the thing.\n")
        task = parse_task(p)
        assert task == TaskFile(
            repo="https://github.com/owner/repo",
            repeat=False,
            prompt="Do the thing.",
        )

    def test_repeat_task(self, tmp_path: Path):
        p = tmp_path / "t.md"
        p.write_text("---\nrepo: https://github.com/o/r\nrepeat: true\n---\nPrompt\n")
        task = parse_task(p)
        assert task.repeat is True

    def test_repo_lock(self, tmp_path: Path):
        p = tmp_path / "t.md"
        p.write_text(
            "---\nrepo: https://github.com/o/r\nrepo_lock: shared\n---\nPrompt\n"
        )
        task = parse_task(p)
        assert task.repo_lock == "shared"

    def test_missing_opening_delimiter(self, tmp_path: Path):
        p = tmp_path / "bad.md"
        p.write_text("repo: https://github.com/o/r\n---\nPrompt\n")
        with pytest.raises(ValueError, match="missing opening"):
            parse_task(p)

    def test_missing_closing_delimiter(self, tmp_path: Path):
        p = tmp_path / "bad.md"
        p.write_text("---\nrepo: https://github.com/o/r\nPrompt\n")
        with pytest.raises(ValueError, match="missing closing"):
            parse_task(p)

    def test_missing_repo_field(self, tmp_path: Path):
        p = tmp_path / "bad.md"
        p.write_text("---\nrepeat: true\n---\nPrompt\n")
        with pytest.raises(ValueError, match="missing required field 'repo'"):
            parse_task(p)

    def test_malformed_frontmatter_line(self, tmp_path: Path):
        p = tmp_path / "bad.md"
        p.write_text("---\nno-colon-here\n---\nPrompt\n")
        with pytest.raises(ValueError, match="malformed frontmatter"):
            parse_task(p)

    def test_multiline_prompt(self, tmp_path: Path):
        p = tmp_path / "t.md"
        p.write_text("---\nrepo: https://github.com/o/r\n---\nLine 1\n\nLine 3\n")
        task = parse_task(p)
        assert task.prompt == "Line 1\n\nLine 3"


class TestDiscoverMdTasks:
    def test_finds_markdown_files(self, tmp_path: Path):
        (tmp_path / "a.md").write_text(
            "---\nrepo: https://github.com/o/a\n---\nPrompt A\n"
        )
        (tmp_path / "b.md").write_text(
            "---\nrepo: https://github.com/o/b\n---\nPrompt B\n"
        )
        tasks = discover_md_tasks(tmp_path)
        assert set(tasks.keys()) == {"a", "b"}

    def test_ignores_dotfiles(self, tmp_path: Path):
        (tmp_path / ".hidden.md").write_text(
            "---\nrepo: https://github.com/o/r\n---\nPrompt\n"
        )
        tasks = discover_md_tasks(tmp_path)
        assert tasks == {}

    def test_empty_dir(self, tmp_path: Path):
        assert discover_md_tasks(tmp_path) == {}

    def test_nonexistent_dir(self, tmp_path: Path):
        assert discover_md_tasks(tmp_path / "nope") == {}


class TestDiscoverTasks:
    def test_includes_md_tasks(self, tmp_path: Path):
        (tmp_path / "a.md").write_text(
            "---\nrepo: https://github.com/o/a\n---\nPrompt A\n"
        )
        tasks = discover_tasks(tmp_path, {})
        assert "a" in tasks

    def test_includes_adhoc_tasks(self, tmp_path: Path):
        from taskpull.state import TaskState

        state = {
            "adhoc-foo-123": TaskState(
                adhoc="do the thing",
                repo="https://github.com/o/r",
            ),
        }
        tasks = discover_tasks(tmp_path, state)
        assert tasks == {
            "adhoc-foo-123": TaskFile(
                repo="https://github.com/o/r",
                repeat=False,
                prompt="do the thing",
            ),
        }

    def test_skips_non_adhoc_state(self, tmp_path: Path):
        from taskpull.state import TaskState

        state = {"file-task": TaskState(repo="https://github.com/o/r")}
        tasks = discover_tasks(tmp_path, state)
        assert tasks == {}

    def test_merges_md_and_adhoc(self, tmp_path: Path):
        from taskpull.state import TaskState

        (tmp_path / "a.md").write_text(
            "---\nrepo: https://github.com/o/a\n---\nPrompt A\n"
        )
        state = {
            "adhoc-b-123": TaskState(
                adhoc="Prompt B",
                repo="https://github.com/o/b",
            ),
        }
        tasks = discover_tasks(tmp_path, state)
        assert set(tasks.keys()) == {"a", "adhoc-b-123"}


class TestTaskIdFromPath:
    def test_stem(self):
        assert task_id_from_path(Path("/some/dir/my-task.md")) == "my-task"


class TestLaneKey:
    def test_without_repo_lock(self):
        task = TaskFile(repo="https://github.com/o/r", repeat=False, prompt="p")
        assert task.lane_key == ("https://github.com/o/r", "https://github.com/o/r")

    def test_with_repo_lock(self):
        task = TaskFile(
            repo="https://github.com/o/r",
            repeat=False,
            prompt="p",
            repo_lock="shared",
        )
        assert task.lane_key == ("https://github.com/o/r", "shared")
