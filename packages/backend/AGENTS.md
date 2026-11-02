# Backend — message_broker_patterns

Python library implementing message broker patterns (pub/sub, work queue, request/reply, fan-out, dead-letter). Patterns that need a real broker (e.g. Transactional Outbox) use **Redis Streams** (`redis[hiredis]>=5`); patterns that don't use stdlib primitives (`asyncio.Queue`, `queue.Queue`). Tests always use `fakeredis` — no real Redis needed for `make test`.

See the root [AGENTS.md](../../AGENTS.md) for repo-wide conventions, the agent team workflow, and the full tech stack.

## Quick commands

```bash
make install          # uv sync
make test             # unit + integration
make unit-tests       # fast — no external deps
make lint-fix         # ruff check --fix
make format-fix       # ruff format
make pre-commit       # format-check + lint-check + unit-tests
make build            # wheel + sdist into dist/
```

## Component layout

```
packages/backend/
├── pyproject.toml
├── Makefile
├── .env.example
├── src/
│   └── message_broker_patterns/
│       ├── __init__.py
│       ├── logging.py             # init_logger() — call before any project import
│       ├── config/settings.py     # pydantic-settings Settings singleton
│       ├── entities/              # shared types/enums used across pattern modules
│       └── <pattern>/             # one module per messaging pattern
├── scripts/
│   └── run_<pattern>.py           # demo entry points — call init_logger() at module level
└── tests/
    ├── conftest.py
    ├── unit/message_broker_patterns/
    └── integration/message_broker_patterns/
```
