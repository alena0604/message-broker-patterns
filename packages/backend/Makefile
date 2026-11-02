.PHONY: redis-up redis-down redis-logs run run-all run-metrics-server run-outbox run-scatter-gather run-competing-consumers run-dlq run-claim-check run-event-sourcing run-priority-queue run-saga install test unit-tests integration-tests lint-check lint-fix format-check format-fix pre-commit bench build publish

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

REDIS_CONTAINER ?= mbp-redis
REDIS_PORT ?= 6379

redis-up:
	@docker inspect $(REDIS_CONTAINER) >/dev/null 2>&1 \
		&& docker start $(REDIS_CONTAINER) \
		|| docker run -d --name $(REDIS_CONTAINER) -p $(REDIS_PORT):6379 redis:7-alpine

redis-down:
	-docker rm -f $(REDIS_CONTAINER)

redis-logs:
	docker logs -f $(REDIS_CONTAINER)

run: run-all

run-all: run-outbox run-scatter-gather run-competing-consumers run-dlq run-claim-check run-event-sourcing run-priority-queue run-saga

run-outbox:
	uv run python scripts/run_outbox_pattern.py

run-scatter-gather:
	uv run python scripts/run_scatter_gather_pattern.py

run-competing-consumers:
	uv run python scripts/run_competing_consumers_pattern.py

run-dlq:
	uv run python scripts/run_dlq_pattern.py

run-claim-check:
	uv run python scripts/run_claim_check_pattern.py

run-event-sourcing:
	uv run python scripts/run_event_sourcing_pattern.py

run-priority-queue:
	uv run python scripts/run_priority_queue_pattern.py

run-saga:
	uv run python scripts/run_saga_pattern.py

run-metrics-server:
	uv run python scripts/run_metrics_server.py

bench:
	uv run python scripts/bench_outbox_pattern.py
	uv run python scripts/bench_scatter_gather_pattern.py
	uv run python scripts/bench_competing_consumers_pattern.py
	uv run python scripts/bench_dlq_pattern.py

build:
	uv build

publish: build
	uv publish --token $(UV_PUBLISH_TOKEN)
