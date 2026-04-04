from __future__ import annotations

from taskpull.workspace import normalize_location


class TestNormalizeLocation:
    def test_https_url_passthrough(self):
        assert normalize_location("https://github.com/o/r") == "https://github.com/o/r"

    def test_ssh_url_passthrough(self):
        assert normalize_location("git@github.com:o/r") == "git@github.com:o/r"

    def test_bare_github_domain(self):
        assert (
            normalize_location("github.com/org/repo") == "https://github.com/org/repo"
        )

    def test_local_path_passthrough(self):
        assert normalize_location(".") == "."

    def test_home_path_passthrough(self):
        assert normalize_location("~/Projects/foo") == "~/Projects/foo"
