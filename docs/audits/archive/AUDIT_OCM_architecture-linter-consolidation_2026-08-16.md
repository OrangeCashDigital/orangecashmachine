# Architecture Governance Consolidation — 2026-08-16

## 1. ARCHITECTURE_SOURCE

`docs/Clean Architecture A Craftsman Guide to Software Structure and Design.pdf` (Robert C. Martin). Fuente conceptual normativa; no SSOT operativo (KB: ADR/código/contratos prevalecen). Principios relevantes: Dependency Rule, 4 círculos, DIP en boundaries, "database is a detail".

## 2. Fuentes secundarias consultadas

- `docs/architecture/decisions/ADR-0014-diseno-interno-market-data.md` (layout de ports; nota de discrepancia `:273-287`).
- `docs/architecture/decisions/ADR-0009-eliminar-fillhandler-tradehistory-huerfanos.md` (precedente de eliminación de huérfanos).
- `docs/architecture/decisions/ADR-0029`, `ADR-0030` (PROPUESTA; no modificados).
- `docs/audits/2026-08-16-architecture-linter.md`, `2026-08-16-endurecimiento-architecture-linter.md`, `2026-08-16-architecture-remediation.md`.
- Bots de gerencia en `docs/` (referencia de diseño; no convertidos en reglas obligatorias).

## 3. Baseline

| Gate | Valor |
|---|---|
| Linter | 10 reglas, 19 filas, 17 violaciones reales, exit 1 |
| pytest `tests/architecture_linter/` | 41 passed |
| pytest completo (sin Kafka env) | 1150 passed |
| mypy | 0 |
| import-linter | 50 kept / 0 broken |
| ruff | 0 |
| bandit (severity Low) | 51 (preexistentes) |

## 4. Inventario de `architecture/`

| Fichero | Función | Consumidores |
|---|---|---|
| `architecture/importlinter.toml` | SSOT de contratos BC-NN (layers + forbidden, 50 contratos) | import-linter CLI; `architecture_linter/rules/arch_009.py:24` (SSOT) |
| `architecture/architecture_linter.toml` | Config del linter: roots, severidad, allowlist | `architecture_linter/config.py:16` (`DEFAULT_CONFIG_PATH`) |
| `architecture/metrics.json` | Snapshot de salud generado (contracts/mypy/pytest/vulns) | ninguno; generado por `scripts/metrics_report.py:65` |

`architecture/` NO contiene Python, ni engine, ni rules, ni CLI.

## 5. Inventario de `architecture_linter/`

| Componente | Ficheros | Función |
|---|---|---|
| Engine | `engine.py:161` (`LinterEngine`, `RepoContext`) | ejecución de reglas sobre AST |
| Rules | `rules/arch_001..010.py` + `rules/base.py` | 10 invariantes ARCH-001..010 |
| Models | `models.py` (`Status`, `Severity`, `Finding`, `Evidence`) | modelo único de findings |
| Config | `config.py` (`load_config`) | carga TOML, defaults de fallback |
| CLI | `cli.py`, `__main__.py` | entrypoint `python -m architecture_linter` |
| Analyzers | `analyzers/{ast_walk,behavior,mutable_state}.py` | análisis semántico/AST |
| Reporters | `reporters/__init__.py` | formato de salida |

## 6. Dependency / Import Analysis

- `architecture/` → sin imports (solo datos TOML/JSON).
- `architecture_linter/` → stdlib-only (`tomllib`); **no importa** `architecture/` como código; **lee** `architecture/importlinter.toml` en runtime como SSOT (`arch_009.py:24,52-80,83+`) y `architecture/architecture_linter.toml` como config (`config.py:16`).
- `scripts/metrics_report.py` genera `architecture/metrics.json` (comando `_run(["uv","run","lint-imports",...])`).
- import-linter (herramienta externa) consume `architecture/importlinter.toml` vía flag `--config`.
- Tests: `tests/architecture_linter/` (reglas + golden + adversarial); `tests/architecture/` (import contracts, kafka contracts — complementan, no duplican: AGENTS.md:101).

## 7. Duplicaciones encontradas

| Capacidad | ¿Duplicada? | Evidencia |
|---|---|---|
| Engine | NO | único `LinterEngine` (`engine.py:161`) |
| CLI | NO | único `__main__.py`/`cli.py` |
| Rules | NO | 10 reglas en Python; ARCH-009 **lee** importlinter.toml, no duplica las capas (`arch_009.py:52-80`) |
| Models | NO | único `models.py` |
| Config | NO | única fuente TOML (`architecture/architecture_linter.toml`); defaults de código son fallback (`config.py:21-27`) |
| Reporters | NO | único `reporters/` |
| Contracts | NO | SSOT único `architecture/importlinter.toml` (leído por import-linter Y por ARCH-009) |
| Metrics | PARCIAL (dato) | `architecture/metrics.json` es snapshot generado **stale** (43 kept vs 50 actuales; 748 pytest vs 1150; 56 vulns vs 51) — no es enforcement |

## 8. Clasificación

**A — No existe duplicación sustancial** (con nota menor en `metrics.json`).

`architecture/` = capa de **configuración normativa/contratos**. `architecture_linter/` = **mecanismo ejecutable de enforcement**. No hay dos engines, dos CLIs, dos reglas, dos modelos ni dos configuraciones contradictorias. El cruce ARCH-009→importlinter.toml es una referencia SSOT deliberada y documentada (AGENTS.md:63), no una regla duplicada.

## 9. Arquitectura objetivo

```
architecture/            → configuración normativa (contracts SSOT + config linter + datos)
architecture_linter/     → enforcement ejecutable (único engine/rules/models/CLI)
tests/architecture_linter/ → tests del linter (reglas + golden + adversarial)
docs/ (PDF + ADRs)       → define la arquitectura
```

## 10. Decisión de consolidación

**No consolidar estructuralmente.** `architecture/` es representación normativa/configuracional válida y NO un segundo linter; eliminarla rompería el gate CI de import-linter (`uv run lint-imports --config architecture/importlinter.toml`) y la fuente de verdad de ARCH-009. Se reafirma la separación de responsabilidades.

Acción recomendada única (UNKNOWN, no bloqueante): `architecture/metrics.json` está stale. Regenerar con `uv run python scripts/metrics_report.py` o eliminar del repo (en CI no se commitea; `scripts/metrics_report.py:5`).

## 11. Cambios realizados

Ninguno sobre `architecture/` ni `architecture_linter/` (decisión A). Los cambios de código de esta sesión corresponden a la remediación arquitectónica (`docs/audits/2026-08-16-architecture-remediation.md`), no a esta consolidación.

## 12. Reglas preservadas

ARCH-001..010 intactas, incluidas las 10 capacidades golden (position state, divergencia semántica, order state, balance state, freshness, orphan contracts, duplicate contracts, false capabilities, layer violations, duplicated mutable state).

## 13. Golden findings before/after

Sin cambios por esta tarea (no se tocó ninguna regla). Estado verificado: PASS=2 (ARCH-006, ARCH-009), FAIL=7, PARTIAL=1 (ARCH-003).

## 14. Tests

`uv run pytest tests/architecture_linter/ -q` → **41 passed** (por regla + golden contra OCM real + adversariales que reducen falsos positivos). `tests/architecture/` también en verde.

## 15. Gates

mypy 0 · import-linter 50/0 · ruff 0 · pytest 1150 passed (excl. 4 fallos ambientales Kafka) · bandit 51 Low preexistentes · linter exit 1 (FAIL/PARTIAL restantes, ver §16 de remediation report).

## 16. Riesgos

- Bajo: la decisión de no consolidar no toca código ejecutable.
- `metrics.json` stale puede inducir a error si alguien lo lee como estado actual; es un snapshot histórico (recomendación: regenerar/eliminar).

## 17. UNKNOWN / BLOCKED

- **UNKNOWN**: intención original de `architecture/metrics.json` en el repo (si debe mantenerse como snapshot comprometido o eliminarse). Se documenta; no bloquea.
- **BLOCKED**: ninguna. No hay contradicción PDF↔linter (`ARCHITECTURAL_CONFLICT` no detectado): ARCH-009 cumple Dependency Rule; ARCH-001/010 encajan con state ownership; ARCH-007 con business rules en círculo interior.

## 18. Git state

Sin cambios en esta tarea. Estado del repo: `M AGENTS.md` (preexistente) + cambios de la remediación (5 deletions, 9 modifications) + untracked de tareas previas (architecture_linter/, tests/architecture_linter/, architecture/architecture_linter.toml, docs/audits/*, PDF). Sin commits, sin push.

## 19. Veredicto

**NO CONSOLIDAR — responsabilidades inequívocas (clasificación A).** Existe una única implementación ejecutable del Architecture Governance Linter (`architecture_linter/`), una única fuente de configuración ejecutable (`architecture/architecture_linter.toml`), una única fuente de contratos (`architecture/importlinter.toml`, consumida por import-linter y por ARCH-009 como SSOT). No hay reglas/engines/CLIs/configuraciones duplicadas. `architecture/` es capa normativa/configuracional y se conserva; `architecture_linter/` es el enforcement. Los golden findings se siguen detectando, los tests adversariales siguen funcionando, y el PDF sigue siendo la referencia normativa.