# AGENTS.md — OrangeCashMachine

Crypto market data lakehouse. Medallion (Bronze→Silver→Gold) + Iceberg + Hydra.
Clean/Hexagonal with bounded contexts and 49 import-linter contracts (BC-NN; baseline F2.1, verificada en CI).

## ⚠️ Autonomous Agent Audit & Compliance Protocol

**OBLIGATORIO:** Cualquier encargo de auditoría (ej. *"audita la repo"*, *"haz una auditoría"*), revalidación de findings o control de cumplimiento **debe seguir estrictamente** el protocolo formal definido en [`docs/governance/AUDIT_PROTOCOL.md`](docs/governance/AUDIT_PROTOCOL.md).

- **Principio Read-Only:** Durante una auditoría está prohibido modificar código, tests, CI, ADRs o tracking. La escritura se restringe a `docs/audits/`.
- **Control FAIL $\neq$ Finding Nuevo:** Todo fallo detectado (ej. por `architecture_linter`) debe contrastarse obligatoriamente contra `tracking.yaml`, ADRs y auditorías históricas. Si ya está registrado, se clasifica como `REVALIDADO`, nunca como `NUEVO`.
- **Modelo de Fuentes y Jerarquía:** Las fuentes externas de conocimiento (libros, papers, notas) no son normas. Toda obligación requiere la cadena de adopción: `Conocimiento` → `Decisión Humana` → `ADR / Governance` → `Control`.
- **Orden de Descubrimiento:** Plan Maestro (`docs/PLAN-Maestro-Ingenieria.md`) → Governance (`docs/architecture/GOVERNANCE.md`) → Tracking (`docs/plans/tracking.yaml`) → ADRs → CI/Linters.
- **Tooling mecánico primero:** Antes de emitir juicios, ejecuta `uv run python scripts/audit_validator.py` (reglas M1..M20, ver AUDIT_PROTOCOL §Q). El LLM solo juzga donde no hay validación mecánica. Comando canónico de cada control: AUDIT_PROTOCOL §R.

## Commands

    uv sync                           # prod deps
    uv sync --group dev               # dev deps (import-linter, mypy, ruff, bandit)
    uv run lint-imports --config architecture_linter/importlinter.toml  # ARCH CONTRACTS — GATE: broken = blocked merge
    uv run pytest tests/ -x -q        # tests, fail-fast
    uv run pytest tests/ -x -q -m integration  # integration tests (need infra)
    uv run ruff check .               # lint
    uv run ruff format . --check      # format check
    uv run mypy .                     # type check (excludes tests/, .venv/)
    uv run bandit -r apps ocm packages shared infrastructure   # security audit
    uv run ocm --cfg job              # validate/print Hydra config (no main.py at root)
    uv run python -m architecture_linter --root .  # Architecture Governance Linter (AST, stdlib-only)
    uv run python -m architecture_linter --root . --json  # salida JSON para CI (exit 1 si hay FAIL/PARTIAL)
    uv run pytest tests/architecture_linter/ -q        # tests del linter (por regla + golden contra OCM real)
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

## Architecture Governance Linter (`architecture_linter/`)

Herramienta independiente (stdlib-only, sin imports de OCM) que detecta
violaciones arquitectónicas por AST en 10 invariantes (ARCH-001..010). No
sustituye a import-linter (BC-NN) ni a los guards de `scripts/`; los
complementa. Detalles en `docs/audits/2026-08-16-architecture-linter.md`.

- Config: `architecture_linter/architecture_linter.toml` (severidad, allowlist por
  símbolo). Sin config → defaults y todas las reglas activas.
- `ARCH-009` reproduce el contrato de capas BC-08 de `architecture_linter/importlinter.toml`
  (incluye `ignore_imports`); no inventa capas.
- El linter NO analiza `architecture_linter/` ni `tests/` (roots configurados:
  `packages, shared, apps, ocm`).
- Exit codes: 0 = sin FAIL/PARTIAL; 1 = hay FAIL/PARTIAL; 2 = error de ejecución.

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

- `import-linter 2.x`: config moved to `architecture_linter/importlinter.toml` (was `pyproject.toml`). Use `uv run lint-imports --config architecture_linter/importlinter.toml`. NEVER `python -m importlinter` (no `__main__.py` in 2.6) and never bare `uv run lint-imports` without `--config` (pyproject.toml no longer has `[tool.importlinter]`, fails with "Could not read any configuration").
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
- Structural invariants beyond import-linter: `tests/architecture/` (import contracts, kafka contracts) and `tests/market_data/test_layer_contracts.py` (placeholder — BC-09 gobernado por import-linter). These supplement, not replace, the import-linter contracts in `architecture_linter/importlinter.toml`.

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
- `pyproject.toml` = SSOT for build, deps, tools. BC-NN contracts live in `architecture_linter/importlinter.toml`.
- `config/` = Hydra YAML (layered: base→env→exchange→pipeline→CLI→env vars).
- Import graph: `uv run pydeps <package> --max-bacon 4` (pydeps en grupo dev)

## Git workflow

- Branch: solo main.
- Conventional Commits: `feat(...)`, `fix(...)`, `chore(...)`, `refactor(...)`.
- Atomic commits: one logical change per commit.
- Never commit: `.coverage`, `.venv`, `.pytest_cache`, `uv.lock` changes without real dep change.
- Never `git push --force` on main.
- Run before push (domain logic changes):
  `uv run ruff check . && uv run lint-imports --config architecture_linter/importlinter.toml && uv run pytest tests/ -q`

## Tool ownership

- `import-linter` → package boundaries and layer direction
- `Ruff` → style and hygiene
- `mypy` → typing contracts
- `pytest` → runtime and integration behavior
- `import-linter` BC-09 → technology governance (domain no importa frameworks de infra/datos)

## Knowledge Base

1. Empieza siempre por `docs/knowledge/manifest.yaml`.
2. No recorras ciegamente todos los PDFs de `docs/knowledge/`.
3. Respeta `status` — metadata `needs_verification` no es un hecho confirmado.
4. Respeta `authority` — un TIER_3/TIER_4 no es autoridad técnica normativa.
5. No cites metadata no verificada como si fuera confirmada.
6. Distingue fuentes primarias (TIER_1) de históricas/draft (TIER_2/TIER_3).
7. Usa la KB como referencia, no como autoridad arquitectónica.
8. Ante conflicto entre la KB y código/ADR, el código/ADR gana siempre.

### Gobernanza de la KB (política normativa)

La Knowledge Base informa el razonamiento; NO gobierna OCM. Jerarquía de autoridad (mayor → menor):

- código y comportamiento ejecutable/tests;
- contratos e invariantes arquitectónicos (import-linter, ports, BC-NN);
- ADRs y decisiones arquitectónicas aprobadas;
- documentación oficial de las tecnologías/versiones en uso;
- documentación interna y Knowledge Base;
- literatura externa, libros, papers y referencias históricas.

Si una fuente de menor autoridad contradice una de mayor autoridad, prevalece la mayor.

**Libro ≠ contrato.** Un libro/paper/material externo no es un BC-NN, ni un ADR, ni un contrato; no modifica la arquitectura ni autoriza una implementación. Puede motivar una propuesta; el cambio formal pasa por su mecanismo (ADR/contrato/BC).

**Documentación oficial primero.** Para comportamiento actual, API, configuración, compatibilidad, límites, seguridad o versión de una dependencia, la doc oficial de la tecnología prevalece sobre libros/históricos. Los libros son fundamentos conceptuales, no SSOT del comportamiento vigente.

**Conocimiento ≠ evidencia de trading.** Una afirmación externa sobre estrategias, indicadores, patrones, alpha o microestructura es conocimiento/hipótesis, no evidencia de edge en OCM. Promover una hipótesis a estrategia candidata exige investigación reproducible y evidencia.

**Flujo de consulta.** fuente externa → conocimiento/hipótesis → verificación contra autoridades superiores → investigación/validación → evidencia → decisión formal (si cambia arquitectura/contrato) → implementación. Nunca saltarse pasos.

**Gaps.** Si la KB no cubre un asunto, declara el gap explícitamente; no rellenes con inferencias presentadas como hechos. Para tecnología actual, consulta la doc oficial.

**Trazabilidad.** Conserva la procedencia (fuente, contexto, fecha/versión/estado) cuando el sistema documental lo permita. Distingue: conocimiento externo / evidencia de research de OCM / decisión arquitectónica aprobada.

**No-sobrediseño.** La literatura no justifica introducir abstracciones, BCs, servicios, capas o patrones nuevos "por analogía". Toda complejidad nueva exige necesidad concreta + autoridad arquitectónica.

**Estado/vigencia.** Fuentes históricas, antiguas, no verificadas o potencialmente desactualizadas no se presentan como conocimiento operativo vigente. Diferencia referencia histórica/conceptual de documentación vigente.
