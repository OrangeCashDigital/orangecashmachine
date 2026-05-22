# Plan — AGENTS.md y arquitectura de governance

## Estado actual

`AGENTS.md`: 103 líneas, 3 cambios ejecutados (pyright, WIP, structural invariants).
`PLAN_AGENTS.md`: este documento — arquitectura objetivo + hoja de ruta.

---

## Arquitectura objetivo

```
Qué rompiste
│
├── import-linter      → arquitectura — solo boundaries entre paquetes y capas
├── Ruff               → estilo
├── pyright            → tipos
├── pytest             → comportamiento
└── AST linter         → governance tecnológica — frameworks prohibidos
```

### Principios

- **import-linter contracts** definen solo architecture boundaries entre paquetes
  (dependency direction, capas, BC-NN). Nada de technology governance.
- **AST linter** (Ruff plugin o custom) define technology governance:
  qué frameworks están prohibidos en qué capas (domain↛pandas/ccxt/redis/...).
- Son dominios ortogonales. Nunca mezclarlos en la misma herramienta.
- `pyproject.toml` sigue siendo el hub de tooling; `[tool.importlinter]` solo
  tendrá architecture boundary contracts.
- **No mover nada solo porque semánticamente encaja.** Mover solo si mejora
  ownership, discoverability, simplifica CI o elimina confusión real.
  `scripts/` se queda donde está. `tests/architecture/` se queda donde está.
   `test_layer_contracts.py` delega al AST linter en scripts/forbidden_frameworks.py.

---

## Cambios ejecutados en AGENTS.md

### 1. Commands — añadir `uv run pyright` y `uv run python scripts/forbidden_frameworks.py`

```
    uv run pyright                    # optional type check (TS-style)
    uv run python scripts/forbidden_frameworks.py  # AST linter — domain no infra frameworks
```

### 2. WIP section — eliminar redundancia

`packages/trading/` y `apps/api/` ya están en Architecture. Se reemplaza por:

```
## Current refactor plan

- `.opencode/plans/REFACTOR_PUBLISHER_DOMAIN.md`
```

### 3. Gotcha de invariantes estructurales

**Antes**:
```
- `tests/architecture/` contains additional structural invariants beyond import-linter
  (test_import_contracts.py, test_kafka_contracts.py). Run both.
```

**Después**:
```
- Structural invariants beyond import-linter: `tests/architecture/` (import contracts,
  kafka contracts) and `tests/market_data/test_layer_contracts.py` (pytest wrapper for
  the AST linter). Run standalone: `uv run python scripts/forbidden_frameworks.py`.
  These supplement, not replace, the import-linter contracts in pyproject.toml.
```

### Resumen

| # | Operación | Neto |
|---|-----------|------|
| 1 | +2 líneas (pyright + forbidden_frameworks) | +2 |
| 2 | 4 → 2 (WIP) | -2 |
| 3 | 2 → 3 (structural tests) | +1 |
| **Total** | | **104 líneas** |

---

## Migración desde `test_layer_contracts.py`

El archivo mezcla tipos de reglas diferentes:

| Regla | Destino correcto | Estado hoy |
|---|---|---|
| `test_composition_root_is_sole_wiring_point` | import-linter (BC-38) | ✅ Ya existe |
| layer direction (domain↛infra, domain↛adapters) | import-linter (BC-03, BC-08) | ✅ Ya existe |
| forbidden frameworks (pandas, ccxt, redis...) | AST linter separado | ✅ Creado como scripts/forbidden_frameworks.py |
| `test_allowed_exceptions_still_exist` | AST linter (misma tool) | ✅ Migrado — test_layer_contracts delega |
| package boundaries (general) | import-linter | ✅ Ya cubierto |

### Timeline

1. ✅ **AST linter creado**: `scripts/forbidden_frameworks.py` — tool standalone
2. ✅ **Test delegado**: `test_layer_contracts.py` importa y llama al tool
3. ✅ **Composition root test eliminado**: BC-38 ya cubre
4. ✅ **AGENTS.md actualizado**: command + gotcha reflejan el nuevo tool

No mover nada más. El tool está en `scripts/` junto a los otros dev tools.

---

## Verificaciones realizadas

- pyproject.toml (898 líneas) — contratos BC-NN, ruff/mypy config, hatch remapping
- pytest.ini — pythonpath, markers de integración
- .pre-commit-config.yaml — ruff check --fix + ruff format
- dagster_defs.py — entrypoint fijo en raíz
- run.sh — SSOT entrypoints
- .env.example — variables de entorno
- ocm/config/env_vars.py — SSOT, guard de sincronía
- tests/conftest.py — solo helper `assert_no_inf_nan`
- tests/market_data/test_layer_contracts.py — 259 líneas, linter AST casero
- tests/architecture/ — test_import_contracts.py, test_kafka_contracts.py
- .github/workflows/ocm-ci.yml — CI order, bug: `python -m importlinter` falla
- .github/workflows/ocm-cd.yml — placeholder
- config/base.yaml — `dry_run: true` default global
- import-linter 2.6 — sin `__main__.py`
- apps/app/cli/main.py — entrypoint hydra_main con `--cfg job`

**No existen**: `opencode.json`, `CLAUDE.md`, `.cursor/`, `Makefile`, `main.py` en raíz.
