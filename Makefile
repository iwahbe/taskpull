.PHONY: all check test

all: check test

check:
	uv run ruff check
	uv run ruff format --check
	uv run ty check --error-on-warning

test:
	uv run pytest tests/
