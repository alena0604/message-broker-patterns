.PHONY: install test unit-tests integration-tests lint-check lint-fix format-check format-fix pre-commit build publish

install:
	uv sync

unit-tests:
	uv run pytest tests/unit -q

integration-tests:
	uv run pytest tests/integration -q

test: unit-tests integration-tests

lint-check:
	uv run ruff check src tests

lint-fix:
	uv run ruff check --fix src tests

format-check:
	uv run ruff format --check src tests

format-fix:
	uv run ruff format src tests

pre-commit: format-check lint-check
	uv run pytest tests/unit -q

build:
	uv build

publish: build
	uv publish --token $(UV_PUBLISH_TOKEN)
