from __future__ import annotations

from pathlib import Path

import pytest

from taskpull.task import TaskFile, discover_tasks, parse_task, task_id_from_path


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


class TestDiscoverTasks:
    def test_finds_markdown_files(self, tmp_path: Path):
        (tmp_path / "a.md").write_text(
            "---\nrepo: https://github.com/o/a\n---\nPrompt A\n"
        )
        (tmp_path / "b.md").write_text(
            "---\nrepo: https://github.com/o/b\n---\nPrompt B\n"
        )
        tasks = discover_tasks(tmp_path)
        assert set(tasks.keys()) == {"a", "b"}

    def test_ignores_dotfiles(self, tmp_path: Path):
        (tmp_path / ".hidden.md").write_text(
            "---\nrepo: https://github.com/o/r\n---\nPrompt\n"
        )
        tasks = discover_tasks(tmp_path)
        assert tasks == {}

    def test_empty_dir(self, tmp_path: Path):
        assert discover_tasks(tmp_path) == {}

    def test_nonexistent_dir(self, tmp_path: Path):
        assert discover_tasks(tmp_path / "nope") == {}


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
