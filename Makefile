.PHONY: check

check:
	uv run ruff check
	uv run ruff format --check
	uv run ty check --error-on-warning
