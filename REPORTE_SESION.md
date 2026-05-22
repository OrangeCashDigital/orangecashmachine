# Reporte de sesión — Mayo 2026

## Resumen

Refinamiento completo de `AGENTS.md`, migración del AST linter de governance
tecnológica a una herramienta standalone, y formalización de Tool Ownership
para evitar que las reglas vuelvan a mezclarse.

---

## 1. AGENTS.md — cambios aplicados

| # | Cambio | Línea |
|---|--------|-------|
| 1 | Añadido `uv run pyright` a Commands | 17 |
| 2 | Añadido `uv run python scripts/forbidden_frameworks.py` a Commands | 18 |
| 3 | WIP section reemplazada → `Current refactor plan` (solo ref plan) | 92-94 |
| 4 | Gotcha de invariantes estructurales ampliada: menciona tool standalone | 66 |
| 5 | Nueva sección **Tool Ownership** — dominio explícito de cada herramienta | 106-112 |

**Resultado**: 112 líneas.

---

## 2. AST linter — migración completa

### Creado: `scripts/forbidden_frameworks.py` (118 líneas)

Tool standalone de governance tecnológica. Verifica que `domain/` no importe
frameworks prohibidos (pandas, ccxt, redis, dagster, polars, loguru...).

**Capacidades**:
- `uv run python scripts/forbidden_frameworks.py` — output human-readable
- `uv run python scripts/forbidden_frameworks.py --json` — JSON para CI
- `uv run python scripts/forbidden_frameworks.py --strict` — exit 1 en stale exceptions
- `--domain-root <path>` — raíz alternativa para testing
- Importable: `from scripts.forbidden_frameworks import collect_violations`

**Exit codes**: 0 = clean, 1 = violations/strict failures

### Actualizado: `tests/market_data/test_layer_contracts.py` (259 → 39 líneas)

Wrapper pytest que delega al tool. Eliminado `test_composition_root_is_sole_wiring_point`
(redundante con BC-38).

### Comportamiento encontrado

El tool detecta **5 violaciones** preexistentes (loguru y polars en domain/) y
**3 stale exceptions** (pandas documentado como excepción pero ya no se importa).
Estos hallazgos existían antes de la migración.

---

## 3. Tool Ownership — formalización

Nueva sección en `AGENTS.md` que declara ownership explícito de cada herramienta:

| Herramienta | Dueña de |
|---|---|
| `import-linter` | package boundaries and layer direction |
| `Ruff` | style and hygiene |
| `pyright` / `mypy` | typing contracts |
| `pytest` | runtime and integration behavior |
| `scripts/forbidden_frameworks.py` | technology governance |

Esto cierra la posibilidad de que alguien vuelva a meter governance tecnológica
dentro de `tests/`. La frontera de cada herramienta queda documentada.

---

## 4. PLAN_AGENTS.md — alineado

125 líneas que reflejan: árbol de governance, tool ownership, principios de
separación de dominios, timeline de migración completada.

---

## 5. Arquitectura de governance resultante

```
Qué rompiste
│
├── import-linter (pyproject.toml) → boundaries entre paquetes y capas
├── Ruff                             → style / hygiene
├── pyright / mypy                   → typing / contracts
├── pytest (tests/)                  → runtime + integration + behavior
└── scripts/forbidden_frameworks.py  → technology governance
```

---

## Archivos tocados

| Archivo | Operación | Líneas |
|---------|-----------|--------|
| `AGENTS.md` | Editado (+3 líneas, -2 líneas, +1 gotcha, +1 sección) | 112 |
| `PLAN_AGENTS.md` | Editado extensivamente | 125 |
| `scripts/forbidden_frameworks.py` | **Creado** | 118 |
| `tests/market_data/test_layer_contracts.py` | Reescrito (259 → 39) | 39 |
| `REPORTE_SESION.md` | **Creado** / actualizado (este archivo) | — |
