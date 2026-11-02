## Root Makefile — delegator. Per-component logic lives under packages/<c>/Makefile.
.DEFAULT_GOAL := help

HAS_BACKEND := $(shell test -d packages/backend && echo yes)
HAS_FRONTEND := $(shell test -d packages/frontend && echo yes)

.PHONY: help install test unit-tests integration-tests lint-check lint-fix \
        format-check format-fix pre-commit bench build ci \
        install-backend \
        test-backend unit-tests-backend integration-tests-backend \
        lint-check-backend lint-fix-backend \
        format-check-backend format-fix-backend \
        pre-commit-backend bench-backend build-backend \
        redis-up redis-down redis-logs run run-all \
        install-frontend dev-frontend frontend preview-frontend build-frontend

## ---------- help ----------

help: ## Show this help
	@awk 'BEGIN { FS = ":.*?## " } /^[a-zA-Z_\/-]+:.*?## / { printf "  %-30s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""
	@echo "Enabled components:"
	@$(if $(HAS_BACKEND), echo "  - backend";)
	@$(if $(HAS_FRONTEND), echo "  - frontend";)

## ---------- aggregate ----------

install: $(if $(HAS_BACKEND),install-backend) $(if $(HAS_FRONTEND),install-frontend) ## Install all component deps
test: $(if $(HAS_BACKEND),test-backend) ## Run every component's test suite
unit-tests: $(if $(HAS_BACKEND),unit-tests-backend) ## Backend unit tests only
integration-tests: $(if $(HAS_BACKEND),integration-tests-backend) ## Backend integration tests only
lint-check: $(if $(HAS_BACKEND),lint-check-backend) ## Lint check all components
lint-fix: $(if $(HAS_BACKEND),lint-fix-backend) ## Apply lint fixes
format-check: $(if $(HAS_BACKEND),format-check-backend) ## Check formatting
format-fix: $(if $(HAS_BACKEND),format-fix-backend) ## Apply formatting fixes
pre-commit: $(if $(HAS_BACKEND),pre-commit-backend) ## Run pre-commit-equivalent checks
bench: $(if $(HAS_BACKEND),bench-backend) ## Run every component's benchmarks (not part of ci)
build: $(if $(HAS_BACKEND),build-backend) $(if $(HAS_FRONTEND),build-frontend) ## Build all deployable artifacts

ci: install test lint-check format-check pre-commit build ## Full pre-PR fan

## ---------- backend ----------

install-backend: ## Install backend Python deps via uv
	$(MAKE) -C packages/backend install

test-backend: ## Run backend tests (unit + integration)
	$(MAKE) -C packages/backend test

unit-tests-backend: ## Run backend unit tests only
	$(MAKE) -C packages/backend unit-tests

integration-tests-backend: ## Run backend integration tests only
	$(MAKE) -C packages/backend integration-tests

lint-check-backend:
	$(MAKE) -C packages/backend lint-check

lint-fix-backend:
	$(MAKE) -C packages/backend lint-fix

format-check-backend:
	$(MAKE) -C packages/backend format-check

format-fix-backend:
	$(MAKE) -C packages/backend format-fix

pre-commit-backend:
	$(MAKE) -C packages/backend pre-commit

bench-backend: ## Run backend benchmarks (scripts/bench_*.py; needs Redis)
	$(MAKE) -C packages/backend bench

build-backend: ## Build backend wheel/sdist
	$(MAKE) -C packages/backend build

redis-up: ## Start local Redis (docker, container mbp-redis)
	$(MAKE) -C packages/backend redis-up

redis-down: ## Stop and remove local Redis
	$(MAKE) -C packages/backend redis-down

redis-logs: ## Follow local Redis logs
	$(MAKE) -C packages/backend redis-logs

run run-all: ## Run every backend pattern demo in sequence (needs Redis)
	$(MAKE) -C packages/backend $@

run-%: ## Run one pattern demo, e.g. make run-dlq, run-outbox, run-saga
	$(MAKE) -C packages/backend $@

## ---------- frontend ----------

install-frontend: ## Install frontend Node deps
	npm --prefix packages/frontend install

dev-frontend: ## Start frontend dev server (vite)
	npm --prefix packages/frontend run dev

frontend: dev-frontend ## Alias for dev-frontend

preview-frontend: ## Preview the production frontend build (run build-frontend first)
	npm --prefix packages/frontend run preview

build-frontend: ## Build frontend for production
	npm --prefix packages/frontend run build
