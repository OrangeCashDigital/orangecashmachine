## Reglas de seguridad no negociables

- El agente NUNCA ejecuta `git push`, `gh pr merge`, `git reset`,
  `git rebase`, `git stash`, `git clean`, `git commit --amend`,
  `git commit --no-verify`, `git branch -D`, `git branch -d`,
  `git push --force`, `git push -f`, `git push --delete`.
- El agente NUNCA usa `export`, `env`, `bash -c`, `sh -c`,
  `python -c`, `eval`, `exec`, `source`, `command`, `builtin`,
  `nohup`, `nice`, `time`, `xargs`, `script`, `setsid`, `curl`,
  `wget`, `ssh`, `scp`, `nc`, `rsync`.
- El agente NUNCA encadena comandos con `;`, `&&`, `||`, `|`,
  backticks o `$()`.
- El agente NUNCA intenta extraer credenciales de
  `credential.helper`, `.gitconfig`, `.env` ni ningún otro sitio.
- Si un comando está bloqueado por el entorno: PARAR. No sustituir,
  no probar variantes, no rodear. Pedir al humano que lo ejecute.
- Ante cualquier duda: proponer y esperar. El humano es el único
  autorizado a modificar el remoto (push, PR, merge).

---

# AGENTS.md — OrangeCashMachine

Crypto market data lakehouse. Medallion (Bronze→Silver→Gold) + Iceberg + Hydra.
Clean/Hexagonal with bounded contexts and 51 import-linter contracts (BC-NN; baseline F2.1, verificado en CI: 51 kept / 0 broken).

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

If hooks modify files: stop and report. The human re-stages and commits; agents may run `git add/commit` (never `add -A`/`add .`); `push` and `commit --amend` require human confirmation; agents never run `git reset/stash/rebase/merge`, force-push, `--no-verify` or `gh pr merge`. Never skip hooks.

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
complementa. Detalles en `docs/audits/AUDIT_OCM_architecture-linter_2026-08-16.md`.

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
  `pydantic==2.14.0`, `loguru==0.7.3`, `pyyaml==6.0.3`,
  `aioresilience==0.2.1`, `pybreaker==1.4.1`.
  Nota: `ccxt>=4.5.74` es un **rango** (no pin), no una dependencia fijada.
  Nota: `pyarrow>=17.0,<27.0` es un specifier estrecho acoplado a `uv.lock`; el guard
  `uv lock --check` (CI, 2 jobs) detecta divergencias pyproject ↔ uv.lock.
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

## Skills

Before modifying code under packages/, apps/, shared/, config/ or policies/, load the `ocm-context-bootstrap` skill and build a Context Pack. Not required for typos, formatting or docs-only edits.
The skill is procedural and never authority; ADR-0054 is accepted as documentation except MD-A4, which stays PROPOSED (docs/architecture/decisions/ADR-0054-persistent-engineering-context-decisions.md).

## Agent rules (OpenCode / OmO)

- Agent git policy (SSOT: `opencode.json`; patterns are a guardrail, not a guarantee): allow `fetch/switch/pull --ff-only/add <paths>/commit`, read-only `branch`; ask `push`, `commit --amend`, `gh pr create`; deny `add -A`/`add .`/`--all`/`-f`, `commit -n`/`--no-verify`, `switch -f/--discard-changes`, `reset/stash/rebase/merge`, force-push/delete, branch delete/force, `clean`, `gh pr merge`. The human merges PRs.
- Never read or print `.env` or secrets. Never run `uv run ocm --cfg job` (it prints secrets).
- Gate before reporting done: `uv run ruff check . && uv run lint-imports --config architecture_linter/importlinter.toml && uv run pytest tests/ -x -q`.
- Do not run `/init-deep` or create nested AGENTS.md files.
- No new pandas outside `adapters/` and `infrastructure/`; domain stays framework-agnostic.

## Setup de agentes

Guía de instalación y configuración: `docs/agents/opencode-omo-setup.md`.

### Limitación conocida de OpenCode (permission.edit/write)

`permission.edit` y `permission.write` NO están soportados en esta
versión de OpenCode. El agente PUEDE editar archivos con Edit/Write
sin restricción del entorno.

**Mitigación**:
- `.githooks/pre-commit` y `.githooks/pre-push` bloquean commits/pushes
  que toquen `opencode.json`, `AGENTS.md`, `CONTRIBUTING.md`,
  `.githooks/*` y `.github/**`.
- Override: `GIT_HUMAN_OVERRIDE=1` (variable de entorno, solo humano).
- El agente NUNCA debe ejecutar `git commit` ni `git push` con esa
  variable. Es exclusiva del humano.
- `git commit --no-verify` está en deny, así que el agente no puede
  saltarse los hooks.

---

## Reglas de operación con Git (post-incidente 2026-10-26)

Estas reglas son **obligatorias** y no admiten excepción. Fueron ratificadas
tras el incidente del PR #86 (squash que arrastró commits de feature branches
ajenas + `pull.rebase=true` que descartó commits del reflog).

### R-GIT-1 — Un tema por PR

- **Un PR = un tema** (deps / ADR / fix / test / docs). No mezclar.
- Si surge un segundo tema: `git stash push -m "wip: <tema 2>"` y abre rama nueva.
- Un PR con >500 líneas de diff o >5 ficheros modificados requiere justificación
  explícita en el cuerpo del PR.

### R-GIT-2 — Rama siempre desde `main` limpio

Antes de crear una rama nueva:

    git checkout main
    git pull origin main
    git status                          # DEBE decir "nada para hacer commit"
    git log --oneline -1                # DEBE ser HEAD de origin/main
    git checkout -b <tipo>/<descripcion-corta>

- **Prohibido** crear rama desde otra feature branch (el squash arrastra commits ajenos).
- **Prohibido** crear rama desde `main` con cambios sin commitear.

### R-GIT-3 — Verificar `git log main..mi-rama` antes de squash-mergear

Antes de `gh pr merge --squash`:

    git log --oneline main..HEAD
    git diff --stat main..HEAD

- Solo deben aparecer commits y ficheros del tema del PR.
- Si aparece un commit o fichero ajeno → **ABORTAR el merge**, reorganizar la
  rama (`git rebase -i main` + drop de los ajenos + `--force-with-lease`).

### R-GIT-4 — Squash solo de commits propios

- `--squash` es por defecto para PRs pequeños (1-3 commits).
- `--merge` (merge commit) si el PR tiene >3 commits con historia útil.
- **Prohibido** `--squash` si la rama contiene commits cuya autoría no es del
  agente/usuario que la creó.

### R-GIT-5 — Nunca `--admin` sobre checks rojos

- `--admin` solo si TODOS los checks verdes y el único bloqueante es "required
  reviews pendientes".
- Si hay un check `X` (rojo): resolverlo, no saltarlo.
- Si el check rojo es falso positivo documentado (ej. CVE no aplicable), ignorar
  con `--ignore-vuln` + justificación en el workflow.

### R-GIT-6 — Pull nunca con `rebase`

- Configurado globalmente: `pull.rebase=false`. No cambiar por sesión.
- Si necesitas rebase explícito: `git fetch origin && git rebase origin/main`,
  nunca `git pull --rebase`.
- Tras rebase: `git log --oneline origin/main..HEAD` para confirmar que los
  commits siguen ahí.

### R-GIT-7 — Commits con mensaje truthful

- El mensaje debe describir **todo** lo que el commit contiene.
- Si un "fix(deps)" contiene cambios de ADR/docs, **no es un fix(deps)**.
  Reorganizar en commits atómicos o cambiar el mensaje.
- Usar Conventional Commits: feat, fix, docs, test, style, refactor, chore.

### R-GIT-8 — Cero force-push a `main`

- `main` está protegido. Cualquier intento rebota.
- Force-push solo en feature branches propias, con `--force-with-lease`
  (nunca `--force`).

