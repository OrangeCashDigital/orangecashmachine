# AGENTS.md — OrangeCashMachine

Crypto market data lakehouse. Medallion (Bronze→Silver→Gold) + Iceberg + Hydra.
Clean/Hexagonal with bounded contexts and 49 import-linter contracts (BC-NN; baseline F2.1, verificada en CI).

## Commands

    uv sync                           # prod deps
    uv sync --group dev               # dev deps (import-linter, mypy, ruff, bandit)
    uv run lint-imports --config architecture/importlinter.toml  # ARCH CONTRACTS — GATE: broken = blocked merge
    uv run pytest tests/ -x -q        # tests, fail-fast
    uv run pytest tests/ -x -q -m integration  # integration tests (need infra)
    uv run ruff check .               # lint
    uv run ruff format . --check      # format check
    uv run mypy .                     # type check (excludes tests/, .venv/)
    uv run bandit -r apps ocm packages shared infrastructure   # security audit
    uv run ocm --cfg job              # validate/print Hydra config (no main.py at root)
    uv run ocm-api                    # FastAPI gateway (experimental)
    uv run live                       # live trading — ⚠️ capital real
    uv run paper                      # paper trading
    ./run.sh ocm                      # market data pipeline (same as uv run ocm)
    docker compose up -d              # infra: Redis, Kafka, Prometheus

No main.py at repo root. CLI entrypoint: `uv run ocm` (via `app.cli.main`).

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
- Composition Root = por bounded context (ver ADR-0003, serie heredada: CR jerárquico en OCM — no confundir con `decisions/ADR-0003`, constructor angosto de trading) — todos los BCs tienen CR propio:
  `market_data.infrastructure.bootstrap.composition_root`, `trading.bootstrap.composition_root`,
  `portfolio.bootstrap.composition_root`.
- shared/ may only import stdlib and approved 3rd-party libs.

## Active migration: pandas → polars

Domain is 100% framework-agnostic (zero pandas/polars imports). Application and
infrastructure are hybrid during migration. Key facts:

- `ports/outbound/normalization.py` = SSOT of DataFrame transforms for persistence.
  Application and infrastructure both import this.
- The transient bridge (`application/processing/polars_interop.py`) was ELIMINATED.
  The single conversion boundary is now `application/use_cases/ohlcv_transformer.py`:
  `pl.from_pandas()` on entry, `.to_pandas()` on exit (see its module header).
- Remaining pandas usage lives only in adapters/ and infrastructure/ (fetchers,
  storages, quality checkers, some trading strategies). Domain is untouched.
- Already polars-native: `grid_alignment`, `ohlcv_schema`, `gap_scanner` (no pandas).
- Phase order: transformer → pandas_to_domain → storages (bridge already deleted).

## Gotchas

- `import-linter 2.x`: config moved to `architecture/importlinter.toml` (was `pyproject.toml`). Use `uv run lint-imports --config architecture/importlinter.toml`. NEVER `python -m importlinter` (no `__main__.py` in 2.6) and never bare `uv run lint-imports` without `--config` (pyproject.toml no longer has `[tool.importlinter]`, fails with "Could not read any configuration").
- CI bug (fix en `ocm-ci.yml`) — el job config-validation ejecutaba `OCM_VALIDATE_ONLY=1 uv run python main.py` (no existe main.py en la raíz). Ahora es `OCM_VALIDATE_ONLY=true uv run python -m app.cli.main`. Nota: `OCM_VALIDATE_ONLY` usa `BOOL_TRUE` = {true, yes, on} (`ocm/config/layers/coercion.py`) — `1` NO activa validate-only.
- E402 allowed only in files explicitly listed in pyproject.toml per-file-ignores (composition roots, entrypoints, tests). Not a global ignore.
- `type: ignore` requires an explanatory comment (non-default).
- `dry_run: true` = global default in `config/base.yaml`. Production overrides. Never reached production by omission.
- BC-35: all Kafka wire schemas live in `shared/kafka/schemas/` only.
- `ocm/config/env_vars.py` = SSOT for all `OCM_*` env var names. Do not define env var name strings anywhere else.
- `pytest.ini` adds `.` and `apps` to pythonpath. `asyncio_mode=auto` in pyproject.toml `[tool.pytest.ini_options]`. Integration tests marked `@pytest.mark.integration`.
- `mypy` excludes `tests/` and `.venv/` by default (pyproject.toml config).
- Pinned deps with known reasons in pyproject.toml (read comments before bumping):
  `pydantic==2.8.2`, `ccxt==4.3.58`, `loguru==0.7.2`, `pyyaml==6.0.2`,
  `aioresilience==0.2.1`, `pybreaker==1.4.1`.
- CD workflow is a placeholder (`workflow_dispatch` only, no automation).
- `uv run ocm --cfg job` exposes secrets in stdout (Hydra DictConfig pre-Pydantic). Never pipe to logs in production.
- Config validation: `OCM_VALIDATE_ONLY=true uv run python -m app.cli.main` — validates Hydra+Pydantic bootstrap and exits.
- Structural invariants beyond import-linter: `tests/architecture/` (import contracts, kafka contracts) and `tests/market_data/test_layer_contracts.py` (placeholder — BC-09 gobernado por import-linter). These supplement, not replace, the import-linter contracts in `architecture/importlinter.toml`.

## Package remapping (hatchling)

    packages/market_data   → market_data
    packages/trading       → trading
    packages/portfolio     → portfolio
    apps/app               → app
    apps/api               → api
    shared/                → shared (no remap)

## Architecture

- `shared/` = lowest layer (stdlib + 3rd-party only). Types, kafka schemas, contracts (Protocols), exceptions, utils.
- `ocm/` = platform (config/runtime/observability), no business logic.
- `packages/market_data/` = Clean/Hexagonal: domain→ports→application→adapters→infrastructure.
- `packages/trading/` = engine in active development.
- `packages/portfolio/` = position management + rebalance.
- `apps/api/` = FastAPI gateway, experimental. `apps/app/` = CLI entrypoints.
- `apps/research/` = read-only gold layer consumer for notebooks. Importable as package `research` (root_packages de importlinter); no expone ruta CLI.
- `pyproject.toml` = SSOT for build, deps, tools. BC-NN contracts live in `architecture/importlinter.toml`.
- `config/` = Hydra YAML (layered: base→env→exchange→pipeline→CLI→env vars).
- Import graph: `uv run pydeps <package> --max-bacon 4` (pydeps en grupo dev)

## Git workflow

- Branch: solo main.
- Conventional Commits: `feat(...)`, `fix(...)`, `chore(...)`, `refactor(...)`.
- Atomic commits: one logical change per commit.
- Never commit: `.coverage`, `.venv`, `.pytest_cache`, `uv.lock` changes without real dep change.
- Never `git push --force` on main.
- Run before push (domain logic changes):
  `uv run ruff check . && uv run lint-imports --config architecture/importlinter.toml && uv run pytest tests/ -q`

## Tool ownership

- `import-linter` → package boundaries and layer direction
- `Ruff` → style and hygiene
- `mypy` → typing contracts
- `pytest` → runtime and integration behavior
- `import-linter` BC-09 → technology governance (domain no importa frameworks de infra/datos)
