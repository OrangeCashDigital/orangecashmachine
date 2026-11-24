# OCM — AUDIT FINDINGS REGISTER (B-20 / H-11 — git_hash injection + R11)

**Ejecución de auditoría:** 2026-08-18 (baseline `a4d8298`, branch `main`)
**Alcance:** Validación formal del cierre de B-20 (H-11): inyección de `git_hash` desde el
composition root, eliminación de `subprocess` de domain/application y activación del guard
R11 en CI. Read-only estricto (escritura solo en `docs/audits/`).
**Fuente primaria:** diff real del working tree, `docs/plans/tracking.yaml` (B-20, R11),
commit `49b63a4`, ADR-0006, `docs/audits/2026-08-auditoria-integral.md` H-11, checks mecánicos
(ruff, mypy, lint-imports, pytest, health, golden, arch-linter, bandit, pip-audit, yamllint).
**Estado de este registro:** OPEN (nuevo, creado durante la auditoría de cierre de B-20).

Resumen: CRITICAL 0 · HIGH 0 · MEDIUM 0 · LOW 3 · INFO 0 · **total 3**.

Clasificación (taxonomía del protocolo de auditoría de OCM):
- NUEVO: 0
- REVALIDADO: 0
- REGRESIÓN: 0
- CERRADO: 1 — F-B20-01
- CONTRADICCIÓN: 1 — F-B20-02
- RECOMENDACIÓN: 1 — F-B20-03
- NO_VERIFICADO: 0

Deduplicación (regla §11):
- F-B20-01 (regresión de formato) no aparece en tracking/ADRs previos; se registra como
  REGRESIÓN introducida por la edición B-20 de esta sesión y se CERRÓ en la propia auditoría
  (remediación autorizada).
- F-B20-02 (referencia ADR-0006 inválida) contrastado contra el contenido real de ADR-0006
  ("Portfolio es el único dueño del estado de posiciones") — el ADR NO gobierna pureza de
  dominio/subprocess; la norma real es BC-09 (contrato import-linter) + H-11. No estaba
  registrado en tracking → CONTRADICCIÓN nueva de trazabilidad.
- F-B20-03 (backtest R11 sin script histórico) contrastado contra R12-R16 (que usan
  `scripts/backtest_app_guard.py` + CI `fetch-depth: 0`). No es un incumplimiento normativo
  directo (el backtest "positivo+negativo" de R11 está commitado y el Plan §6 lo admite) →
  RECOMENDACIÓN de alineación.

Verificación matemática: total 3

---

## F-B20-01 — Regresión de formato en archivos de B-20 (docstring `run_id` + archivos nuevos sin `ruff format`)

Severity: LOW
Status: CERRADO
Classification: CERRADO
Control: RUFF_FORMAT — `uv run ruff format . --check`
Source: packages/market_data/application/quality/pipeline.py (docstring `run()`),
scripts/domain_subprocess_guard.py, tests/architecture/test_domain_subprocess_guard.py

Evidence:
- `uv run ruff format --check` → `Would reformat: pipeline.py, domain_subprocess_guard.py,
  test_domain_subprocess_guard.py` (3 archivos).
- `git show HEAD:...pipeline.py | ruff format --check` → HEAD pasaba (regresión introducida
  por la edición B-20: línea `run_id` del docstring perdió la alineación).
- `ci_head.yml` (HEAD) → yamllint exit 0; el error de yamllint del repo (`deploy/monitoring/
  alerts.yml:66:162` sin newline al final) es pre-existente y NO relacionado con B-20.
- Remedio aplicado en auditoría: `uv run ruff format` sobre los 3 archivos + re-alineación
  manual del docstring. Verificado: `ruff format . --check` → 498 files already formatted (exit 0);
  `ruff check .` → All checks passed; tests del guard → 8 passed; snapshot pre-fix → 1 violación detectada.

Impact:
- El gate CI `ruff format . --check` (job quality, ocm-ci.yml:317-318) habría roto el merge.
  Sin impacto funcional en runtime.

Required human decision:
- Ninguna — cerrado y verificado (remediación aplicada en la propia auditoría).

Recommended remediation:
- N/A (aplicada). Mantener los archivos B-20 bajo `ruff format . --check`.

Verification required:
- `uv run ruff format . --check` → PASS (exit 0).

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: edición B-20 2026-08-18 ·
  Tests: guard 8 passed, suite 1230 passed · CI: ruff format --check exit 0 ·
  Evidence: diff + ruff format --diff · Closure: CERRADO 2026-08-18

---

## F-B20-02 — B-20 cita ADR-0006 como norma de pureza de dominio, pero ADR-0006 es de portfolio/posiciones

Severity: LOW
Status: OPEN
Classification: CONTRADICCIÓN
Control: TRAZABILIDAD — cadena `Hallazgo → Control → Requisito → Fuente Normativa → Tracking → ADR`
Source: docs/plans/tracking.yaml (B-20: `adr_relacionado: ADR-0006` y cadena.adr.referencia),
commit `49b63a4` (mensaje "Cumple ADR-0006: dominio no depende de infraestructura"),
comentarios en pipeline.py, quality_consumer.py, ge_checker.py, pipeline_factory.py,
domain_subprocess_guard.py, ocm-ci.yml (job domain-guard)

Evidence:
- `docs/architecture/decisions/ADR-0006-portfolio-owns-position-state.md` → ADR-0006 trata
  "Portfolio es el único dueño del estado de posiciones" (BC-13, BC-43). NO menciona
  subprocess, git_hash ni pureza de dominio.
- `docs/audits/2026-08-auditoria-integral.md` H-11 → "¿Contradice algún ADR? No."
- Norma real de pureza de dominio: BC-09 (`architecture_linter/importlinter.toml:234`:
  "market_data.domain does not import infrastructure third-party libs"), que prohíbe
  bibliotecas de infra (pyiceberg/redis/ccxt/polars/...) — `subprocess` (stdlib) no está
  en esa lista; la motivación normativa de B-20 es el hallazgo H-11 + el principio de
  dominio framework-agnostic de AGENTS.md, NO ADR-0006.
- `rg -n "subprocess|pureza|git_hash" docs/architecture/GOVERNANCE.md` → 0 hits.

Impact:
- Trazabilidad inválida: la cadena del Plan Maestro registra un ADR que no soporta la
  decisión. No afecta el runtime ni la validez técnica de B-20 (el código es correcto),
  pero viola la cadena de adopción Conocimiento→Decisión→ADR→Control.

Required human decision:
- D-B20-1: ¿corregir la referencia ADR en tracking.yaml + comentarios (→ BC-09 / null /
  "H-11 + dominio framework-agnostic"), o crear ADR propio? (ver informe §5).

Recommended remediation:
- Corregir `adr_relacionado` y cadena.adr de B-20 en tracking.yaml (→ null o BC-09) y los
  comentarios "ADR-0006, B-20" en código/CI a una referencia normativa válida. Requiere
  Decisión Humana (protocolo §M: el agente no modifica tracking/ADR por iniciativa propia).

Verification required:
- Tras decisión: `uv run python scripts/engineering_health_check.py` → PASS.

Traceability:
- Tracking: B-20 (adr_relacionado ADR-0006) · ADR: ADR-0006 existe pero es de otro dominio ·
  Implementation: 49b63a4 + 2026-08-18 · Tests: N/A · CI: N/A ·
  Evidence: contenido ADR-0006 vs mensaje commit · Closure: PENDIENTE Decisión Humana

---

## F-B20-03 — R11 sin backtest histórico automatizado (a diferencia de R12-R16)

Severity: LOW
Status: OPEN
Classification: RECOMENDACIÓN
Control: R11 — scripts/domain_subprocess_guard.py + tests/architecture/test_domain_subprocess_guard.py
Source: docs/plans/tracking.yaml (R11), scripts/backtest_app_guard.py (R12-R16),
.github/workflows/ocm-ci.yml (jobs app-guard vs domain-guard)

Evidence:
- R12-R16: `scripts/backtest_app_guard.py` extrae snapshots pre-fix (39687e7, cdd7e7e) vía
  `git archive` y el job `app-guard` lo ejecuta con `fetch-depth: 0` (ocm-ci.yml:89-105).
- R11: job `domain-guard` (ocm-ci.yml:116-133) solo ejecuta
  `pytest tests/architecture/test_domain_subprocess_guard.py` (8 tests: positivo árbol real
  + negativos anti-patrón). No hay script de backtest contra snapshot pre-fix commitado.
- Verificación manual de esta sesión: `guard_domain_subprocess` sobre `49b63a4~1` (snapshot
  pre-fix) → detecta la violación en `domain/quality/types.py` (1 violación). El guard SÍ
  captura el caso real.
- Plan Maestro §6: "Backtest contra snapshot pre-fix" (eslabón Evidencia). R11 lo cumple
  parcialmente: el backtest positivo+negativo está commitado, pero no hay script histórico
  automatizado como en R12-R16.

Impact:
- Riesgo de debilitamiento silencioso del guard sin detección histórica. Sin impacto en
  runtime; R11 ya detecta la violación original (verificado).

Required human decision:
- D-B20-2: ¿añadir `scripts/backtest_domain_guard.py` (análogo a backtest_app_guard.py)
  para automatizar la evidencia contra `49b63a4~1`? (ver informe §5).

Recommended remediation:
- Opcional: crear script de backtest histórico de R11 + `fetch-depth: 0` en el job
  `domain-guard`, alineando con el estándar R12-R16.

Verification required:
- N/A (recomendación; la regla ya está activada y su backtest positivo/negativo pasa).

Traceability:
- Tracking: R11 (backtest: ok, activada_en_ci: true) · ADR: NOT_TRACED ·
  Implementation: domain_subprocess_guard.py · Tests: 8 passed ·
  CI: job domain-guard · Evidence: guard sobre snapshot 49b63a4~1 → 1 violación ·
  Closure: PENDIENTE Decisión Humana (mejora opcional)
