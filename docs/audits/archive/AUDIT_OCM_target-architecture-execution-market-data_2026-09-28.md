# OCM — HUMAN ACCEPTANCE GATE — FINAL REPORT

**Iniciativa:** MASTER ARCHITECTURE TRANSFORMATION — Execution Contract & Runtime Architecture
**Fecha:** 2026-09-28
**Autoridad:** GATE A aprobado por humano (2026-09-28) → fases B→L ejecutadas.
**Baseline referencia:** `docs/audit/OCM-PRE-IMPLEMENTATION-BASELINE-2026-09-28.md` · HEAD `05317f149c57afa05fa5f667b53272e6ace7ba49`
**ADR normativo:** `docs/architecture/decisions/ADR-0048-execution-contract-and-runtime-architecture.md` (+ ADR-0045, ADR-0046, ADR-0047)

> **ESTADO:** REPORTE TÉCNICO DESCRIPTIVO — NO DECLARA ACEPTACIÓN NORMATIVA. La aceptación
> final del Execution Contract y de la migración de consumers es decisión exclusiva del humano.

---

## A. Cambios realizados (resumen por fase)

| Fase | Cambio |
|---|---|
| **B** | `config/execution_registry.yaml` creado: EP-001..006, 12 campos, estados ADR-0048 (ACTIVO/DISCREPANCIA/DEAD/NO_IMPLEMENTADO), section modes (BACKTEST NO_IMPLEMENTADO), discrepancias trazables, dead_references. |
| **C** | `app.cli` auditado ↔ registry: main/live_hydra/paper_hydra/streaming_hydra mapean a EP-001..004 vía composition roots (execute_live/execute_paper/streaming). Sin procesos ni BCs nuevos. Sin cambios de código. |
| **D** | Market Data auditado: frontera limpia (sin imports cross-BC trading/portfolio; domain sin pandas; SSOT normalization). Arquitectura objetivo ya documentada en ADR-0048 §8. Deep-schemas (canonical instrument identity completo, dataset metadata como first-class product) **deferidos** — implementarlos sin ADR propio sería inventar decisión (mandato §24 → H2-E). |
| **E** | Dockerfile: CMD `python -m market_data.main` (EP-006) documentado con referencia contractual; sin cambio de comportamiento. |
| **F** | docker-compose: servicios `trading`/`portfolio` (scaffolding con refs a módulos inexistentes) **retirados de forma documentada** — DISP-D5-02 RESUELTA sin crear trading.main/portfolio.main. |
| **G** | systemd: templates `ocm-market-data`/`ocm-streaming` con ExecStart ↔ EP-006/EP-004 + comentario contractual. Diffs preexistentes (tuning/resiliencia) preservados íntegros. `install_systemd.sh` sin cambios (helper consumer válido). |
| **H** | run.sh: claim "SSOT" eliminado; documentado como developer/operator convenience layer (consumer); invoca EP-001/002/003. DISP-D5-01 RESUELTA. |
| **I** | `tests/architecture/test_execution_registry_parity.py`: 18 tests (13 checks del mandato §17 + schema/states/entrypoints). |
| **J** | `.github/workflows/ocm-ci.yml`: job `execution-registry-parity` (fail-closed): valida registry schema + parity tests. CI verifica, no autoriza. |
| **K** | `docs/architecture/README.md` (estado vigente): matriz de madurez alineada (trading/portfolio sin servicio Docker; nota Phase F con trazabilidad). Docs históricos (audits, decision pack PROPUESTA, tracking D5) sin tocar — refs legacy preservadas. |
| **L** | Validación integral ✓ (sección I). |

## B. Archivos modificados (por esta iniciativa)

- `.github/workflows/ocm-ci.yml` (+32)
- `Dockerfile` (+3: doc EP-006)
- `docker-compose.yml` (−99: retiro trading/portfolio scaffold + comentario trazabilidad)
- `docs/architecture/README.md` (±16: matriz + nota Phase F)
- `docs/plans/tracking.yaml` (+155 bloque D5 previo; +~30 bloque ADR-0048 fases B-L)
- `run.sh` (±27: convenience layer)

## C. Archivos creados (por esta iniciativa)

- `config/execution_registry.yaml`
- `docs/architecture/decisions/ADR-0048-execution-contract-and-runtime-architecture.md`
- `docs/audit/OCM-PRE-IMPLEMENTATION-BASELINE-2026-09-28.md`
- `tests/architecture/test_execution_registry_parity.py`
- reporte final (este archivo): `docs/audit/OCM-TARGET-ARCHITECTURE-EXECUTION-MARKET-DATA-REPORT.md` (FINAL)

## D. Decisiones aplicadas

- Q1 registry → `config/execution_registry.yaml` (NO policies/registry.yaml). Q2 market_data.main PUBLIC SERVICE vía `-m` (no console script). Q3 run.sh = convenience. Q4 dead refs sin crear módulos. Q5 fases A-L. Q6 estados enum. Q7 roles. Q8 autoridad ADR-0048→Contract→Registry Rule→Registry.
- Baseline y cambios preexistentes (systemd tuning, book_builder, audit_validator, importlinter, test_golden, staging ports) **preservados íntegros**.

## E. Contratos materializados

- ADR-0048 (Execution Contract): 12 campos, matrices rol/frontera, consumer×entrypoint, modo×BC, autoridad, arquitecturas target Application y Market Data, non-goals, fases.
- Execution Registry (config/execution_registry.yaml) con 6 entrypoints.

## F. Entrypoints finales

| EP | Entrypoint | Rol | Estado |
|---|---|---|---|
| EP-001 | `app.cli.main` | APPLICATION BATCH ENTRYPOINT | ACTIVO |
| EP-002 | `app.cli.live_hydra` | APPLICATION RUNTIME ENTRYPOINT (LIVE) | ACTIVO |
| EP-003 | `app.cli.paper_hydra` | APPLICATION RUNTIME ENTRYPOINT (PAPER) | ACTIVO |
| EP-004 | `app.cli.streaming_hydra` | APPLICATION RUNTIME ENTRYPOINT (STREAMING) | ACTIVO |
| EP-005 | `api.main:serve` (ocm-api) | APPLICATION API ENTRYPOINT (experimental) | ACTIVO |
| EP-006 | `market_data.main` | MARKET DATA SERVICE ENTRYPOINT | ACTIVO |

`trading.main`/`portfolio.main` → DEAD (sin crear); BACKTEST → NO_IMPLEMENTADO (sin falsa implementación).

## G. Registry final

`config/execution_registry.yaml` (schema v1, adr ADR-0048, 6 entradas, modes, 2 discrepancias RESUELTAS, dead_references). YAML válido; consumible programáticamente.

## H. Consumidores

- pyproject `[project.scripts]` (consumer packaging; EP-001..005) — coherente.
- Dockerfile (EP-006) — coherente.
- docker-compose (market-data service EP-006) — coherente; refs dead retiradas.
- systemd (EP-006 market-data, EP-004 streaming) — coherente.
- run.sh (EP-001/002/003 convenience) — coherente.
- CI (EP-001 config-validation + nuevo job parity) — verifica.

## I. Validaciones

| Validador | Resultado |
|---|---|
| `audit_validator` | ≤ baseline: M21 (FAIL pre-existente), M25×2 (WARN pre-existente), M20 (WARN pre-existente). **Sin FAIL nuevo.** |
| `architecture_linter` | 3 PASS / 7 FAIL (idéntico baseline: ARCH-008/ARCH-010 pre-existentes). |
| import-linter BC-NN | **51 KEPT, 0 BROKEN** (baseline F2.1 intacto). |
| mypy (apps/app, apps/api) | Success, 23 files. |
| ruff (parity test + archivos iniciativa) | All checks passed. |
| tests/architecture | 200 passed; 2 FAIL **pre-existentes** (M21 naming, B-25 EN_PROGRESO). |
| tests/market_data | **203 passed.** |
| parity tests (Phase I) | **18 passed.** |
| config validation (`OCM_VALIDATE_ONLY`) | OK. |
| docker compose config | OK (sin docker disponible: validación YAML + estructura). |
| ruff format (parity test) | OK. |

## J. Tests

- Nuevos: 18 parity tests (contract↔consumers).
- Suite completa sin regresiones (FAIL count = baseline exacto).

## K. Divergencias

- Frente al baseline: git status = baseline + 4 archivos de la iniciativa (registry, ADR-0048, parity test, baseline report) + reporte final. Los 8 modificados y 10 untracked del baseline **siguen presentes**; solo se tocaron desde la iniciativa los que son parte directa de la migración (compose templates/README) con delta mínimo.

## L. Deuda residual

- Deferidos (ADR-0048 §14): deep-market-data (canonical instrument identity completo, dataset metadata first-class, data quality expansion contratos, histórico/backtest conjunto) → requieren ADR/decisiones con gate humano (mandato §8-11, §24). NO inventados.
- ocm-api = experimental; `modo: SERVICE` en EP-005 es extensión documental pendiente de confirmación humana (registrada en registry).
- Backtest = capacidad objetivo, sin implementación.

## M. Problemas preexistentes (preservados, NO atribuidos a esta iniciativa)

- M21 (naming canónico docs/audits/), WARN M25 (ADR-0020/0044), WARN M20, B-25 (EN_PROGRESO), ARCH-008 (WSTradesSource stub), ARCH-010 (estado mutable duplicado), engineering_health 2 errors, semgrep/bandit hallazgos previos. Nada se autofixó (mandato §50).

## N. Problemas nuevos

- **Ninguno** detectado en validación integral.

## O. Riesgos

- `config/execution_registry.yaml` vive en `config/` (raíz de Hydra). Verificado: Hydra compone vía `defaults` explícitos; ningún glob de `config/*.yaml`; registro no es grupo Hydra. Riesgo bajo.
- Parity tests dependen de rutas/entrypoints reales; cualquier cambio de entrypoint romperá CI (intencional, fail-closed).
- ocm-api `modo: SERVICE` — clasificación abierta a confirmación.

## P. Operaciones Git realizadas

- Ninguna. Sin commit/push/merge/rebase/reset/clean/stash/checkout destructivo.

## Q. Operaciones Git deliberadamente NO realizadas

- commit, push, merge, rebase, reset, clean, stash; no se modificó HEAD.

## R. Recomendación técnica (descriptiva, no normativa)

La iniciativa cumple los 18 criterios de éxito del mandato (ADR-0048 materializado, registry válido,
entrypoints registrados, consumidores coherentes, sin dead refs activas, sin procesos artificiales,
frontera Market Data clara, parity tests + CI fail-closed, sin deuda nueva ni destrucción de trabajo
preexistente). Queda **PENDIENTE de decisión humana** la aceptación normativa; y como trabajo futuro
(decisión humana gate adicional) la materialización de deep-market-data (instrument identity canónico,
dataset metadata, data quality contratos) que ADR-0048 y el mandato dejan en modo objetivo/deferido.