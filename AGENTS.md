# AGENTS.md — OrangeCashMachine

Crypto market data lakehouse. Medallion (Bronze→Silver→Gold) + Iceberg + Dagster +
Hydra. Clean/Hexagonal with bounded contexts and ~40 import-linter contracts (BC-NN).

## Commands

    uv sync                           # prod deps
    uv sync --group dev               # dev deps (import-linter, mypy, ruff, bandit)
    uv run lint-imports               # ARCH CONTRACTS — GATE: broken = blocked merge
    uv run pytest tests/ -x -q        # tests, fail-fast
    uv run pytest tests/ -x -q -m integration  # integration tests (need infra)
    uv run ruff check .               # lint
    uv run ruff format . --check      # format check
    uv run mypy .                     # type check (excludes tests/, .venv/)
    uv run bandit .                   # security audit
    uv run ocm --cfg job              # validate/print Hydra config (no main.py at root)
    ./run.sh ocm                      # market data pipeline (same as uv run ocm)
    ./run.sh dagster                  # Dagster UI (port 3001)
    docker compose up -d              # infra: Redis, Kafka, Dagster, Prometheus

No main.py at repo root. CLI entrypoint: `uv run ocm` (via `app.cli.main`). Dagster
entrypoint: `dagster_defs.py` at root — do not move.

## CI order (fail-fast)

    architecture (import-linter) → tests + config-validation (parallel)

Broken contracts skip all downstream jobs. CI uses `uv sync --group dev` for
contracts, plain `uv sync` for tests+config.

## Pre-commit hooks

    ruff check --fix → ruff format

If hooks modify files: `git add -u && git commit -m <msg>`. Never skip.

## Dependency direction

    shared → ocm → domain → ports → application → adapters → infrastructure

- Never import infrastructure into domain.
- Never import bounded contexts directly across domains.
- Use ports/contracts instead.
- Composition Root = infrastructure/dagster/assets/ only.
- shared/ may only import stdlib and approved 3rd-party libs.

## Gotchas

- `import-linter 2.x`: use `uv run lint-imports` or `uv run python -c "from importlinter.cli import lint_imports; exit(lint_imports())"`. NEVER `python -m importlinter` (no `__main__.py` in 2.6). ⚠️ CI has this bug — `.github/workflows/ocm-ci.yml` runs `python -m importlinter` which will fail.
- E402 allowed only in files explicitly listed in pyproject.toml per-file-ignores (composition roots, entrypoints, tests). Not a global ignore.
- `type: ignore` requires an explanatory comment (non-default).
- `dry_run: true` = global default in `config/base.yaml`. Production overrides. Never reached production by omission.
- BC-35: all Kafka wire schemas live in `shared/kafka/schemas/` only.
- `ocm/config/env_vars.py` = SSOT for all `OCM_*` env var names. Do not define env var name strings anywhere else.
- `pytest.ini` adds `.` and `apps` to pythonpath, `asyncio_mode=auto`. Integration tests marked `@pytest.mark.integration`.
- `mypy` excludes `tests/` and `.venv/` by default (pyproject.toml config).
- Pinned deps with known reasons (always read comment before bumping):
  `pydantic==2.8.2`, `ccxt==4.3.58`, `loguru==0.7.2`, `pyyaml==6.0.2`,
  `aioresilience==0.2.1`, `pybreaker==1.4.1`.
- CD workflow is a placeholder (`workflow_dispatch` only, no automation).
- `uv run ocm --cfg job` exposes secrets in stdout (Hydra DictConfig pre-Pydantic). Never pipe to logs in production.
- Config validation: `OCM_VALIDATE_ONLY=1 uv run python -m app.cli.main` — validates Hydra+Pydantic bootstrap and exits.
- `tests/architecture/` contains additional structural invariants beyond import-linter (test_import_contracts.py, test_kafka_contracts.py). Run both.

## Package remapping (hatchling)

    packages/market_data   → market_data
    packages/trading       → trading
    packages/portfolio     → portfolio
    apps/app               → app
    apps/api               → api
    infrastructure/dagster → infrastructure.dagster
    shared/                → shared (no remap)

## Architecture

- `shared/` = lowest layer (stdlib + 3rd-party only). Types, kafka schemas, contracts (Protocols), exceptions, utils.
- `ocm/` = platform (config/runtime/observability), no business logic.
- `packages/market_data/` = Clean/Hexagonal: domain→ports→application→adapters→infrastructure.
- `packages/trading/` = engine in active development.
- `packages/portfolio/` = position management + rebalance.
- `infrastructure/dagster/assets/` = sole external Composition Root.
- `apps/api/` = FastAPI gateway, experimental. `apps/app/` = CLI entrypoints.
- `apps/research/` = read-only gold layer consumer for notebooks. Not importable as package.
- `pyproject.toml` = SSOT for build, deps, tools, and all BC-NN contracts.
- `config/` = Hydra YAML (layered: base→env→exchange→pipeline→CLI→env vars).
- `scripts/` = arch_metrics.py, arch_graph.py (dev tooling, not entrypoints).

## Active development / WIP

- `packages/trading/` — engine in active development
- `apps/api/` — FastAPI gateway, experimental
- `.opencode/plans/REFACTOR_PUBLISHER_DOMAIN.md` — current refactor plan

## Git workflow

- Branch: solo main.
- Conventional Commits: `feat(...)`, `fix(...)`, `chore(...)`, `refactor(...)`.
- Atomic commits: one logical change per commit.
- Never commit: `.coverage`, `.venv`, `.pytest_cache`, `uv.lock` changes without real dep change.
- Never `git push --force` on main.
- Run before push (domain logic changes):
  `uv run ruff check . && uv run lint-imports && uv run pytest tests/ -q`
