# Auditoría de Integración de Governance Externo — OCM

**Fecha:** 2026-08-17 · **Tipo:** READ-ONLY, evaluativa · **Principio:** SSOT — este informe referencia autoridad existente en vez de duplicarla.

## 1. Executive Summary

`architecture/` (config normativa) y `architecture_linter/` (enforcement ejecutable) NO están duplicados — ya verificado con evidencia de código en `docs/audits/2026-08-16-architecture-linter-consolidation.md` (Clasificación A, Veredicto: NO CONSOLIDAR). Ese informe es la SSOT para las Secciones 4, 5 y 7 de este documento; aquí no se repite su análisis, solo se referencia.

El gap real no cubierto por auditorías previas era la evaluación de herramientas externas de governance (Structurizr, OPA, Semgrep, SonarQube, Backstage, ADR tooling). Esta auditoría lo cierra: **ninguna está presente en el repo** (verificado por grep exhaustivo). El CI actual ya cubre buena parte de ese espacio con herramientas equivalentes (CodeQL, Trivy, Gitleaks, bandit, import-linter, architecture_linter).

## 2. Estado Actual de Governance — Fuentes y Clasificación

| Fuente | Clasificación | Evidencia |
|---|---|---|
| `AGENTS.md` | NORMATIVA | jerarquía de autoridad explícita, tool ownership §"Tool ownership" |
| `docs/architecture/GOVERNANCE.md` | NORMATIVA | SSOT de reglas de gobernanza arquitectónica, Fase 0 |
| `architecture/importlinter.toml` | OPERATIVA_SSOT | 50 contratos BC-NN, consumido por import-linter CLI y ARCH-009 |
| `architecture/architecture_linter.toml` | OPERATIVA_SSOT | config del linter custom |
| `architecture/metrics.json` | EVIDENCIA (STALE) | generado por `scripts/metrics_report.py`, no comprometido en CI — ver nota §8 |
| `docs/architecture/decisions/ADR-0001..0030` | NORMATIVA (serie canónica) | serie heredada `0000-0005` deprecada 2026-08-03, banner de deprecación presente |
| `docs/architecture/INVENTORY.md` | PROPUESTA — **NO EXISTE** | referenciado en GOVERNANCE.md §6 como "pendiente de crear"; gap documentado, no bloqueante |
| `docs/audits/2026-08-16-architecture-linter-consolidation.md` | EVIDENCIA (verificada) | SSOT para diagnóstico architecture/ vs architecture_linter/ — ver §5 de este informe |
| `docs/audits/2026-08-17-governance-audit-revalidation.md` | HISTÓRICA | intento previo de esta misma auditoría, terminó UNKNOWN por informe-fantasma nunca escrito; cerrado por este documento |

**APROBACIÓN_HUMANA de GOVERNANCE.md y AGENTS.md: NO_VERIFICADA** — son SSOT operativo de facto (así los trata el propio repo/CI), pero no hay evidencia de un sign-off humano explícito capturado en este documento; solo su uso consistente en CI y en el resto de la documentación.

## 3. Matriz de Evaluación de Herramientas Externas

Ninguna de estas herramientas tiene rastro en el repo (`grep -rliE "semgrep|open.?policy.?agent|\brego\b|sonarqube|structurizr|backstage"` → 0 resultados, excluyendo `.venv`). Evaluación desde cero:

| Herramienta | Problema que resuelve | Capacidad OCM que duplica/cubre | CI reproducible | Compatibilidad stack | Recomendación |
|---|---|---|---|---|---|
| **Structurizr / C4** | Drift entre diagrama y código | Ninguna hoy — OCM no tiene diagramas C4 versionados, solo GOVERNANCE.md prosa + ADRs | Sí (DSL como código) | Alta (Python-agnóstico, standalone) | **EVALUAR** — bajo costo, cubre un gap real (no hay visualización de bounded contexts) |
| **OPA / Rego** | Policy-as-code para estados/aprobaciones | Parcial — `engineering_health_check.py` y `check_ssot_enums.py` ya hacen policy-as-code, pero en Python ad-hoc, no declarativo | Sí | Requiere runtime Go/Rego adicional, lock-in de sintaxis | **NO ADOPTAR** por ahora — el patrón Python-script-como-gate ya funciona y está probado en CI (`ocm-ci.yml` job `engineering-health`, `quality`); migrar a Rego es costo sin beneficio claro hoy |
| **Semgrep** | AST-matching para invariantes de código | Se solapa con `architecture_linter/` (AST-based, stdlib-only) y con CodeQL (ya en CI, `.github/workflows/codeql.yml`) | Sí | Alta | **NO ADOPTAR** — architecture_linter ya cubre invariantes específicos de OCM (ARCH-001..010) y CodeQL cubre seguridad genérica; Semgrep sería un tercer motor AST redundante |
| **import-linter** | Fronteras entre bounded contexts | Ya adoptado — 50 contratos BC-NN activos, gate en CI | Sí | Ya integrado | **ADOPTADO** (statu quo, sin acción) |
| **SonarQube** | Quality gate / deuda técnica | Se solapa con ruff + mypy + bandit + pip-audit, todos ya en CI (`quality` job) | Requiere servidor propio (no SaaS-free en self-host real) | Media — añade infraestructura (servidor Sonar) | **NO ADOPTAR** — el stack actual (ruff/mypy/bandit) ya es el quality gate; Sonar añadiría infraestructura sin cubrir un gap |
| **Backstage / TechDocs** | Catálogo de servicios y docs | Ninguna — OCM no tiene catálogo de servicios (single repo, pocos BCs) | Sí | Alta pero sobredimensionado para el tamaño actual del repo | **NO ADOPTAR** — diseñado para multi-repo/multi-team; OCM es monorepo con `AGENTS.md` como entrypoint, ya suficiente |
| **ADR Tooling (adr-tools, log4brains)** | Ciclo de vida de ADRs | Ya cubierto manualmente — `docs/architecture/decisions/ADR-template.md`, numeración secuencial, GOVERNANCE.md §3 | Sí | Alta | **EVALUAR** — bajo costo, formalizaría lo que ya se hace a mano (numeración, template, índice); ganancia marginal pero real si el volumen de ADRs crece |
| **OCM Architecture Linter** (propio) | Invariantes específicos de negocio (position ownership, freshness, etc.) | N/A — es la herramienta propia | Sí, ya en CI potencial (no confirmado si `ocm-ci.yml` lo ejecuta — ver nota) | Nativo | **ADOPTADO** — pero ver nota crítica abajo |

**Nota crítica:** `architecture_linter/` NO aparece en `.github/workflows/ocm-ci.yml` (verificado — el job `architecture` solo corre `lint-imports`, no `python -m architecture_linter`). El comando existe en `AGENTS.md` pero no está enforced en CI. Esto es un **hallazgo real**: la herramienta que detecta ARCH-001/002/004/005/007/008/010 (todos en FAIL según el golden test) no bloquea merges — es diagnóstica, no gate. Estado: `PREEXISTENTE`, `BY-DESIGN` o `DEBT` — no está claro cuál sin una decisión humana explícita.

## 4. Matriz SSOT (resumen)

| Responsabilidad | SSOT única |
|---|---|
| Contratos de capas/boundaries (BC-NN) | `architecture/importlinter.toml` |
| Config del linter custom | `architecture/architecture_linter.toml` |
| Invariantes semánticos (ARCH-NNN) | `architecture_linter/rules/*.py` |
| Decisiones arquitectónicas | `docs/architecture/decisions/ADR-NNNN-*.md` |
| Política de gobernanza | `docs/architecture/GOVERNANCE.md` |
| Nombres de env vars | `ocm/config/env_vars.py` (AGENTS.md línea 92) |
| Literales de dominio (enums) | `shared/enums.py`, verificado por `scripts/check_ssot_enums.py` |
| Estado de tracking de deuda/tareas | `docs/plans/tracking.yaml` |

## 5. Architecture vs Architecture_linter

**Ver `docs/audits/2026-08-16-architecture-linter-consolidation.md` — SSOT de este análisis, no se repite aquí.** Veredicto citado: NO CONSOLIDAR, clasificación A (sin duplicación sustancial), única excepción menor `metrics.json` stale.

## 6. Modelo de Auditabilidad Independiente

Corroboración cruzada YA EXISTE parcialmente: ARCH-009 lee `importlinter.toml` como SSOT en vez de reimplementar capas (verificado en `2026-08-16-architecture-linter-consolidation.md` §6-7). Falta: el linter custom no corre en CI (§3 nota crítica) — un auditor externo con checkout limpio no obtiene un artifact `audit/` determinista hoy porque `architecture_linter` no se ejecuta automáticamente en ningún workflow.

## 7. Resultados del Análisis de CI

Gates activos verificados en `.github/workflows/ocm-ci.yml`: `architecture` (import-linter, umbral ≥49 contratos), `engineering-health`, `app-guard`, `trading-guards`, `unit-tests` (coverage ≥40%), `security` (bandit -ll), `integration-tests` (Kafka), `config-validation`, `quality` (ruff/mypy/SSOT/pip-audit). Adicional fuera de `ocm-ci.yml`: CodeQL, Trivy, Gitleaks, actionlint, yamllint, hadolint. **`architecture_linter` no está entre ellos** — ver hallazgo §3.

## 8. Log de Decisiones

| Decisión | Estado | APROBACIÓN_HUMANA |
|---|---|---|
| No consolidar `architecture/` vs `architecture_linter/` | RESUELTO (heredado de audit 2026-08-16) | NO_VERIFICADA |
| No adoptar Semgrep/SonarQube/OPA/Backstage | PROPUESTA (esta auditoría) | NO_VERIFICADA — requiere decisión humana |
| Evaluar Structurizr y ADR tooling formal | PROPUESTA | NO_VERIFICADA |
| `architecture_linter` no corre en CI pese a estar documentado en AGENTS.md | CONTRADICTORIO — hallazgo nuevo | NO_VERIFICADA |
| `docs/architecture/INVENTORY.md` referenciado pero inexistente | DEBT (declarado en su propia fuente) | NO_VERIFICADA |

## 9. Veredicto Final

**CONSISTENTE — REQUIERE DECISIONES HUMANAS.**

El sistema de governance interno (BC-NN + ARCH-NNN + ADRs) es coherente y sin duplicación real. El gap no es de herramientas externas (ninguna aporta valor claro no cubierto ya) sino de **enforcement incompleto de lo que ya existe**: `architecture_linter` no está en el gate de CI. Esa es la única acción con impacto real detectada en esta auditoría, y requiere una decisión humana explícita (¿se activa como gate bloqueante, dado que hoy reporta 7/10 reglas en FAIL?).

---
*Generado read-only. Sin modificaciones a código fuente, ADRs, tracking.yaml o tests. Sin git add/commit/push ejecutados por esta sesión.*
