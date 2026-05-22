# Reporte de sesión — Mayo 2026

## Resumen

Esta sesión ejecutó el plan completo de refinamiento de `AGENTS.md` y migración
del AST linter desde un test monolítico a una herramienta standalone.

---

## 1. AGENTS.md — 3 cambios aplicados

| # | Cambio | Línea |
|---|--------|-------|
| 1 | Añadido `uv run pyright` a Commands | 17 |
| 2 | Añadido `uv run python scripts/forbidden_frameworks.py` a Commands | 18 |
| 3 | WIP section reemplazada → `Current refactor plan` con solo el ref plan | 92-94 |
| 4 | Gotcha de invariantes estructurales ampliada: menciona `test_layer_contracts.py` + tool standalone | 66-67 |

**Resultado**: 104 líneas, todas verificadas contra el código fuente.

---

## 2. AST linter — migración completa

### Creado: `scripts/forbidden_frameworks.py`

Tool standalone de governance tecnológica. Verifica que `domain/` no importe
frameworks prohibidos (pandas, ccxt, redis, dagster, polars, loguru...).

**Capacidades**:
- `uv run python scripts/forbidden_frameworks.py` — output human-readable
- `uv run python scripts/forbidden_frameworks.py --json` — JSON para CI
- `uv run python scripts/forbidden_frameworks.py --strict` — exit 1 también en stale exceptions
- `--domain-root <path>` — para testeo con raíces alternativas
- Importable: `from scripts.forbidden_frameworks import collect_violations`

**Exit codes**: 0 = clean, 1 = violations/strict failures

### Actualizado: `tests/market_data/test_layer_contracts.py`

De 259 → 39 líneas. Ahora es un wrapper pytest que delega al tool.

**Eliminado**: `test_composition_root_is_sole_wiring_point` — redundante con BC-38

### Comportamiento encontrado

El tool detecta **5 violaciones** preexistentes (loguru y polars en domain/) y
**3 stale exceptions** (pandas ya no se importa donde se documentó como excepción).
Estos hallazgos existían antes de la migración — el test original ya fallaba igual.

---

## 3. PLAN_AGENTS.md — alineado con el estado real

Documento de 125 líneas que refleja:
- Árbol de governance final (import-linter / Ruff / pyright / pytest / AST linter)
- Principios: separación de dominios, no mover por estética
- Timeline de migración marcada como completada

---

## 4. Arquitectura de governance resultante

```
Qué rompiste
│
├── import-linter (pyproject.toml) → boundaries entre paquetes y capas
├── Ruff                             → estilo
├── pyright                          → tipos
├── pytest (tests/)                  → comportamiento, integración, runtime
└── scripts/forbidden_frameworks.py  → governance tecnológica
```

---

## Archivos tocados

| Archivo | Operación | Líneas |
|---------|-----------|--------|
| `AGENTS.md` | Editado (+2 líneas, -2 líneas, +1 línea) | 104 |
| `PLAN_AGENTS.md` | Editado extensivamente | 125 |
| `scripts/forbidden_frameworks.py` | **Creado** | 118 |
| `tests/market_data/test_layer_contracts.py` | Reesccrito (259 → 39) | 39 |
| `REPORTE_SESION.md` | **Creado** (este archivo) | — |
