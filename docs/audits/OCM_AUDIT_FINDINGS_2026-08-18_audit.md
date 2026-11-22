# OCM — AUDIT FINDINGS REGISTER

**Ejecución de auditoría:** 2026-08-18 (baseline `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283`)
**Fuente primaria:** `docs/audits/AUDIT_OCM_TECHNICAL_COMPLIANCE_2026-08-18.md` + artefactos en `/tmp/ocm-audit-20260818/` y `/tmp/ocm-audit-20260818-s2/` (revalidación de esta sesión)
**Estado de este registro:** OPEN (se actualiza conforme los findings se resuelven o se toma decisión humana)
**Nota de revalidación (2026-08-18T02:05Z+):** todos los hallazgos se re-verificaron en esta sesión — CI remoto vía `gh` (runs 32069832325 FAIL, 32069832475 FAIL), `architecture_linter` standalone (E-101, exit 1, 7/1/2), golden/adversarial (E-102/E-103, 4/12 passed), pip-audit (E-105, exit 1, 4 vulns), yamllint (E-106, exit 1). Las referencias de evidencia E-0xx del informe canónico son la SSOT de trazabilidad.

Resumen: CRITICAL 1 · HIGH 4 · MEDIUM 5 · LOW 5 · INFO 1 · **total 16**.

Clasificación (taxonomía del protocolo de auditoría de OCM):
- NUEVO: 4 — F-CI-01, F-CI-02, F-ARCH-04, F-GOV-01
- REVALIDADO: 5 — F-ARCH-01, F-ARCH-02, F-ARCH-03, F-ARCH-05, F-ARCH-06
- REGRESIÓN: 0
- CERRADO: 0
- CONTRADICCIÓN: 1 — F-GOV-05
- RECOMENDACIÓN: 6 — F-CI-03, F-GOV-02, F-GOV-03, F-GOV-04, F-SC-01, F-SC-02
- NO_VERIFICADO: 0

Deduplicación (regla §11):
- ARCH-001 + ARCH-002 + ARCH-010 → findings separados pero **mismo dominio: Position State Ownership** (F-ARCH-01).
- pip-audit (F-CI-01) → **un único finding** con 4 advisories (mismo root cause: dependency gate).
- Los 4 controles FAIL (pip-audit, yamllint, secret scanning nativo, pipeline CI/CD) → 2 findings NUEVOS (F-CI-01, F-CI-02); secret scanning nativo y pipeline CI/CD no generan finding adicional (misma causa raíz / práctica no adoptada por governance).

---

## F-CI-01 — pip-audit bloquea Quality Gate

Severity: CRITICAL
Status: OPEN
Classification: NUEVO
Control: Dependency Security
Source: ocm-ci.yml / job `quality`

Evidence:
- GitHub Actions run: 32069832325 (conclusion FAILURE, step `Vulnerabilidades (pip-audit)` exit 1)
- Local exact CI command: `uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325` → exit 1
- aiohttp 3.14.1 (transitiva vía ccxt directo)
- PYSEC-2026-3545 → fix 3.14.3
- PYSEC-2026-3546 → fix 3.14.2
- PYSEC-2026-3547 → fix 3.14.2
- cryptography 49.0.0 (transitiva vía python-jose[cryptography], coincurve)
- PYSEC-2026-3552 → fix 50.0.0
- existing ignores: PYSEC-2026-113 (pyarrow 19.0.1), PYSEC-2026-1325 (ecdsa 0.19.2) — risk-accept documentado 2026-08-03

Impact:
- Gate de seguridad de CI rojo permanente en el commit auditado; merge de main bloqueado por el job `quality` (semántica fail). Las 4 vulns quedan sin mitigar ni aceptar formalmente. aiohttp expone superficie de red (client HTTP).

Required human decision:
- D1: bump `aiohttp` ≥3.14.3 y `cryptography` ≥50.0.0 (validando staging contra ccxt/python-jose), **o** risk acceptance formal (ADR + tracking) sin ocultar el finding. Prohibido ampliar el ignore-list sin aprobación.

Recommended remediation:
- Actualizar dependencias y validar staging; si no es viable, formalizar risk-accept con evidencia de exploitabilidad y plan de bump.

Verification required:
- `uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325` → exit 0; run GitHub Actions en nuevo commit → SUCCESS.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: NOT_TRACED · Tests: NOT_TRACED · CI: run 32069832325 (FAIL) · Evidence: E-012, E-031 · Closure: OPEN

---

## F-ARCH-01 — Multi-owner del estado de posición

Severity: HIGH
Status: OPEN
Classification: REVALIDADO
Control: Architecture — Position State Ownership (dominio: ARCH-001 + ARCH-002 + ARCH-010)
Source: `architecture_linter` standalone

Evidence:
- `uv run python -m architecture_linter --root . --json` → exit 1
- ARCH-001 FAIL: posición gestionada por 6 owners mutables además del SSOT portfolio (TradeTracker._open_positions, OMS._orders, OMS._open, OMS._entry_positions, RiskManager._open_positions, RiskManager._positions)
- ARCH-002 FAIL (2 findings): divergencia semántica — WAC/acumulación vs reemplazo sin leer previo; SELL reduce vs pop incondicional
- ARCH-010 FAIL (2 findings): estado mutable `position` duplicado en 7 almacenes; `order` en 4
- Relacionado con B-15 / ADR-0021 (PROPUESTA) / ADR-0006

Impact:
- Riesgo financiero en live: divergencia de estado de posición entre owners; mitigado parcialmente por `dry_run:true` default y mitigación de observabilidad en fill_sync.

Required human decision:
- D3: prioridad/aprobación/implementación de ADR-0021 (single-owner en PortfolioService).

Recommended remediation:
- Aprobar ADR-0021, unificar el estado de posición en PortfolioService, consolidar los stores duplicados, eliminar la divergencia WAC/reemplazo/pop.

Verification required:
- ARCH-001/002/010 → PASS; golden test actualizado; G10 (invariante single-owner) en CI.

Traceability:
- Tracking: B-15 (EN_CURSO) · ADR: ADR-0021 (PROPUESTA) + ADR-0006 · Implementation: PARCIAL (fill_sync) · Tests: PARCIAL (test_fill_sync_close_divergence) · CI: PENDIENTE (G10) · Evidence: E-005, E-022 · Closure: OPEN

---

## F-ARCH-02 — Ausencia de loop periódico de órdenes

Severity: HIGH
Status: OPEN
Classification: REVALIDADO
Control: Architecture — Order Reconciliation (ARCH-003)
Source: `architecture_linter` standalone

Evidence:
- ARCH-003 PARTIAL: existe reconciliación puntual submit-time (fetch_state), pero NO hay loop periódico `fetch_open_orders`/`manage_open_orders`. Órdenes sin fill durante downtime solo se recuperan en el siguiente submit del mismo símbolo.
- Relacionado con B-MD-008 / ADR-0029 (ACEPTADA, implementación PENDIENTE).

Impact:
- Órdenes abiertas no gestionadas durante downtime; riesgo operacional y financiero en ejecución real.

Required human decision:
- D4: implementación del loop de órdenes abiertas (ADR-0029 ya aceptada).

Recommended remediation:
- Implementar el mecanismo periódico de gestión de órdenes abiertas definido en ADR-0029; tests de reconciliación.

Verification required:
- ARCH-003 → PASS (o transición a FULL); tests de loop periódico; job CI correspondiente.

Traceability:
- Tracking: B-MD-008 (PENDIENTE) · ADR: ADR-0029 (ACEPTADA) · Implementation: PENDIENTE · Tests: PENDIENTE · CI: PENDIENTE · Evidence: E-005, E-022 · Closure: OPEN

---

## F-ARCH-03 — Ausencia de balance real

Severity: HIGH
Status: OPEN
Classification: REVALIDADO
Control: Architecture — Real Balance / Patrimonial (ARCH-004)
Source: `architecture_linter` standalone

Evidence:
- ARCH-004 FAIL: no existe `fetch_balance`/`get_balance`/BalancePort ni fuente estructural currency→amount; Risk/Execution computa sizing/drawdown contra `capital_usd` configurado (capital estático ≠ balance del exchange).
- Relacionado con B-MD-009 / ADR-0030 (ACEPTADA, implementación PENDIENTE).

Impact:
- Sizing y drawdown calculados contra capital configurado, no contra el balance real del exchange; riesgo patrimonial en ejecución real.

Required human decision:
- D5: implementación del balance real (ADR-0030 ya aceptada).

Recommended remediation:
- Implementar BalancePort + fetch_balance real y reconciliación patrimonial según ADR-0030.

Verification required:
- ARCH-004 → PASS; tests de reconciliación patrimonial; job CI correspondiente.

Traceability:
- Tracking: B-MD-009 (PENDIENTE) · ADR: ADR-0030 (ACEPTADA) · Implementation: PENDIENTE · Tests: PENDIENTE · CI: PENDIENTE · Evidence: E-005, E-022 · Closure: OPEN

---

## F-GOV-05 — Inconsistencia de licencia

Severity: HIGH
Status: OPEN
Classification: CONTRADICCIÓN
Control: Governance / Legal
Source: LICENSE / pyproject.toml / README

Evidence:
- `LICENSE` (74 líneas) = **PolyForm Noncommercial 1.0.0**
- `pyproject.toml:31` = `license = "MIT"` (SPDX)
- `README.md:14,386` = declara MIT
- CONTRADICCIÓN documentada: tres declaraciones, dos valores (PolyForm vs MIT). El auditor NO decide cuál es la correcta.

Impact:
- Discrepancia legal en la distribución del software; inconsistencia en metadata de paquete y documentación pública.

Required human decision:
- D2: resolver la licencia real (PolyForm Noncommercial o MIT) y unificar las tres declaraciones.

Recommended remediation:
- Decisión humana + actualización de LICENSE/pyproject/README/classifiers de forma coherente.

Verification required:
- LICENSE, pyproject.toml y README consistentes entre sí.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: NOT_TRACED · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-016 · Closure: OPEN

---

## F-CI-02 — yamllint falla en deploy/monitoring/alerts.yml

Severity: MEDIUM
Status: OPEN
Classification: NUEVO
Control: CI — Yamllint workflow
Source: `.github/workflows/yamllint.yml` / `deploy/monitoring/alerts.yml`

Evidence:
- GitHub Actions run: 32069832475 (conclusion FAILURE)
- `uvx yamllint -c .yamllint .` local → exit 1
- `deploy/monitoring/alerts.yml:66:162` — `error new-line-at-end-of-file` (no new line character at the end of file)
- Warnings adicionales (no bloqueantes) en alertmanager.yml, historical.yaml, manifest.yaml

Impact:
- Workflow `yamllint` rojo en el commit auditado; bloquea merge de ese workflow (no afecta runtime).

Required human decision:
- Ninguna bloqueante; corregir el archivo YAML (requiere editar deploy/monitoring/alerts.yml — fuera del alcance documental de esta auditoría).

Recommended remediation:
- Añadir newline final a `deploy/monitoring/alerts.yml:66`; revisar warnings de line-length.

Verification required:
- `uvx yamllint -c .yamllint .` → exit 0; run GitHub Actions → SUCCESS.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: NOT_TRACED · Tests: NOT_TRACED · CI: run 32069832475 (FAIL) · Evidence: E-032 · Closure: OPEN

---

## F-ARCH-04 — Freshness

Severity: MEDIUM
Status: OPEN
Classification: NUEVO
Control: Architecture — Data Freshness Boundary (ARCH-005)
Source: `architecture_linter` standalone

Evidence:
- ARCH-005 FAIL: detección/recovery de silencio presentes (niveles 1–2); AUSENTES niveles 3–6: estado consultable en port, contrato (boundaries/ports), propagación a trading/portfolio, enforcement pre-orden.

Impact:
- Posible ejecución de órdenes sobre datos no frescos (sin enforcement de freshness en la cadena Market Data → Strategy → Risk → Execution).

Required human decision:
- No requiere decisión puntual de aprobación; priorizarla en roadmap P2.

Recommended remediation:
- Completar los niveles 3–6 de la frontera de freshness (estado consultable + contrato + propagación + enforcement).

Verification required:
- ARCH-005 → PASS.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: NOT_TRACED · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-005 · Closure: OPEN

---

## F-ARCH-05 — Duplicidad de contratos

Severity: MEDIUM
Status: OPEN
Classification: REVALIDADO
Control: Architecture — Contract Uniqueness (ARCH-007)
Source: `architecture_linter` standalone

Evidence:
- ARCH-007 FAIL, 8 findings: contratos duplicados (misma semántica) u homónimos (semántica distinta):
  - AnomalyRegistryPort (ports/outbound/anomaly_registry.py vs quality.py)
  - OrderStatus (execution/order.py vs transport.py)
  - PipelineContext (application/context.py vs pipeline/runtime.py)
  - QualityPipelineResult (application/quality/pipeline.py vs ports/outbound/quality_pipeline.py)
  - RetryExhaustedError (adapters/outbound/exchange/resilience.py vs domain/exceptions)
  - SchemaVersionError (domain/exceptions vs shared/kafka/schemas/_base.py)
  - StorageFactoryPort (ports/outbound/storage.py vs storage_factory.py)
  - _TransientProxy (application/pipeline/runtime.py vs domain/policies/base.py)
- Allowlist actual: CompositionRoot, RiskConfig, ConfigurationError, CursorStore (justificados por BC)

Impact:
- Riesgo de drift semántico entre definiciones paralelas; ambigüedad en contratos de frontera.

Required human decision:
- No requiere decisión puntual; consolidación en roadmap P2.

Recommended remediation:
- Unificar cada par a una única definición o renombrar para disambiguar (revisar allowlist).

Verification required:
- ARCH-007 → PASS (o con hallazgos reducidos y justificados).

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: NOT_TRACED · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-005 · Closure: OPEN

---

## F-ARCH-06 — Stub WSTradesSource

Severity: MEDIUM
Status: OPEN
Classification: REVALIDADO
Control: Architecture — Production Stubs (ARCH-008)
Source: `architecture_linter` standalone

Evidence:
- ARCH-008 FAIL: `WSTradesSource` (packages/market_data/adapters/inbound/websocket/ws_trades_source.py:32) expone capacidad sin ejecutarla: `__anext__` termina de inmediato (StopAsyncIteration), `_running` nunca True.
- Diseño con fallback REST documentado (honesto), pero la capacidad WS no se ejecuta.

Impact:
- Capacidad anunciada sin implementación real; riesgo de que un consumidor asuma streaming WS cuando no ocurre.

Required human decision:
- No requiere decisión puntual; resolución en roadmap P2.

Recommended remediation:
- Implementar el streaming real, o eliminar la capacidad WS y dejar solo el fallback REST declarado.

Verification required:
- ARCH-008 → PASS.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: NOT_TRACED · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-005 · Closure: OPEN

---

## F-SC-02 — Ausencia de SBOM, provenance y firma de artefactos

Severity: MEDIUM
Status: OPEN
Classification: RECOMENDACIÓN
Control: Supply Chain — SBOM / Provenance / Signing
Source: búsqueda sistemática en repo y workflows

Evidence:
- Sin archivos SBOM en el repo (find -iname "*sbom*" → vacío)
- Sin provenance/SLSA: grep en `.github/workflows/` → sin references
- Sin cosign/syft/grype configurados
- Sin firma de artefactos de build
- Dockerfile no produce firmas ni atestaciones
- Nota: `shared/kafka/provenance.py` es proveniencia de datos (no build provenance)

Impact:
- Sin trazabilidad criptográfica de la cadena de suministro; auditoría de supply chain de terceros limitada. No bloquea operación actual.

Required human decision:
- D6: decidir introducir SBOM/provenance/artifact signing (Syft/Cosign/SLSA) en CI.

Recommended remediation:
- Generar SBOM por release, atestaciones SLSA y firma de imágenes/artefactos (Cosign), pin por SHA de Actions.

Verification required:
- SBOM presente en release; firma verificable.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: NOT_TRACED · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-019 · Closure: OPEN

---

## F-GOV-01 — INVENTORY.md ausente aunque referenciado

Severity: LOW
Status: OPEN
Classification: NUEVO
Control: Documentation
Source: `docs/architecture/GOVERNANCE.md`

Evidence:
- `GOVERNANCE.md:48` referencia `docs/architecture/INVENTORY.md` ("pendiente de crear")
- `ls docs/architecture/INVENTORY.md` → no existe
- Deuda declarada en la propia fuente (DOC DRIFT)

Impact:
- Referencia documental rota; sin impacto funcional.

Required human decision:
- Ninguna; crear el archivo (fuera del alcance documental de auditoría).

Recommended remediation:
- Crear INVENTORY.md con el inventario de bounded contexts o eliminar la referencia.

Verification required:
- La referencia apunta a un archivo existente.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: NOT_TRACED · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-020 · Closure: OPEN

---

## F-GOV-02 — Drift de documentación de ccxt

Severity: LOW
Status: OPEN
Classification: RECOMENDACIÓN
Control: Documentation
Source: pyproject.toml / AGENTS.md

Evidence:
- `pyproject.toml:97` comentario: "ccxt: pinneado en 4.3.58"
- `AGENTS.md:96`: `ccxt==4.3.58`
- `pyproject.toml:100`: pin real `ccxt==4.5.70`
- Instalado/verificado: ccxt 4.5.70

Impact:
- Documentación normativa con versión desactualizada; riesgo de malinterpretación en bump futuro.

Required human decision:
- Ninguna; corregir la documentación.

Recommended remediation:
- Actualizar comentario y AGENTS.md al pin real 4.5.70 (o actualizar el pin si procede).

Verification required:
- Comentario/AGENTS.md coherentes con pyproject.toml.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: NOT_TRACED · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-017 · Closure: OPEN

---

## F-GOV-03 — Docstrings con rutas architecture/ obsoletas

Severity: LOW
Status: OPEN
Classification: RECOMENDACIÓN
Control: Documentation
Source: docstrings en código

Evidence:
- Referencias a `architecture/importlinter.toml` en:
  - `shared/__init__.py:17`
  - `packages/market_data/infrastructure/bootstrap/pipeline_factory.py:16`
  - `packages/portfolio/bootstrap/composition_root.py:36`
  - `apps/research/data/composition_root.py:24`
- Ruta real actual: `architecture_linter/importlinter.toml` (tras fusión `architecture/`→`architecture_linter/` en `314285a`)

Impact:
- Docstrings con ruta obsoleta (DOC DRIFT); sin impacto funcional.

Required human decision:
- Ninguna; corregir docstrings.

Recommended remediation:
- Actualizar las 4 referencias a `architecture_linter/importlinter.toml`.

Verification required:
- grep de `architecture/importlinter.toml` en docstrings → sin coincidencias.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: NOT_TRACED · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-033 · Closure: OPEN

---

## F-GOV-04 — metrics_report.py referencia/escribe en ruta muerta

Severity: LOW
Status: OPEN
Classification: RECOMENDACIÓN
Control: Documentation
Source: scripts/metrics_report.py / .gitignore

Evidence:
- `scripts/metrics_report.py:4` docstring: "Genera architecture/metrics.json"
- `scripts/metrics_report.py:65`: `Path("architecture/metrics.json").write_text(...)` — el directorio `architecture/` **no existe** en HEAD (post-fusión); el script fallaría en escritura
- `.gitignore:58`: `architecture/metrics.json` (ruta obsoleta)
- El script NO corre en CI

Impact:
- Ruta muerta (dead path); el script no es ejecutable tal como está. Sin impacto en runtime.

Required human decision:
- Ninguna; corregir ruta o eliminar script.

Recommended remediation:
- Apuntar a `architecture_linter/metrics.json` (o directorio válido) o retirar el script si no se usa.

Verification required:
- `metrics_report.py` escribe sin error, o script eliminado.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: NOT_TRACED · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-020 · Closure: OPEN

---

## F-SC-01 — Pinning mixto de GitHub Actions

Severity: LOW
Status: OPEN
Classification: RECOMENDACIÓN
Control: Supply Chain — Action Pinning
Source: .github/workflows/

Evidence:
- Pin por SHA completo: `reviewdog/action-actionlint`, `ludeeus/action-shellcheck`, `aquasecurity/trivy-action`
- Pin por tag sin SHA: `actions/checkout@v4`, `github/codeql-action@v3`, `astral-sh/setup-uv@v4`, `hadolint/hadolint-action@v3.4.0`, `gacts/gitleaks@v1`

Impact:
- Tags mutables = vector de supply-chain teórico (la tag puede apuntar a un commit distinto en el tiempo). Riesgo bajo-moderado, mitigado por que son repos mayoritariamente oficiales.

Required human decision:
- D6: decidir política de pinning por SHA para terceros.

Recommended remediation:
- Pin por SHA completo en todas las Actions de terceros; mantener renovación documentada (Dependabot ya configurado).

Verification required:
- grep `uses:` en workflows → todas con SHA de 40 chars o @vN justificado.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: NOT_TRACED · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-018 · Closure: OPEN

---

## F-CI-03 — architecture_linter standalone no es gate CI

Severity: INFO
Status: OPEN
Classification: RECOMENDACIÓN
Control: CI — Architecture Governance
Source: .github/workflows/ocm-ci.yml / tests/architecture_linter/test_golden.py

Evidence:
- Estado real del linter standalone (`uv run python -m architecture_linter --root . --json` → exit 1):
  - ARCH-001 FAIL · ARCH-002 FAIL · ARCH-003 PARTIAL · ARCH-004 FAIL · ARCH-005 FAIL · ARCH-006 PASS · ARCH-007 FAIL · ARCH-008 FAIL · ARCH-009 PASS · ARCH-010 FAIL
- Golden test (4 passed) **fija el estado esperado del detector**, incluyendo los FAIL/PARTIAL; un golden PASS **NO significa reglas resueltas**.
- El standalone `python -m architecture_linter` **NO aparece en ningún workflow**; solo el `lint-imports` corre en CI.
- Los golden/adversarial corren dentro del job `unit-tests` (vía `pytest tests/ -m "not integration"`).

Impact:
- La deuda de arquitectura (19 findings) no tiene presión de CI para remediarse; el golden legitima el estado actual como esperado. Distinción crítica: detector vs gate.

Required human decision:
- D7: decidir si los findings del linter pasan a ser gate obligatorio de CI (política "no aumentar findings sin ADR/tracking") o permanecen como detector documental.

Recommended remediation:
- Si se activa como gate: job CI propio con `--json`, umbral de failed_findings y procedimiento de excepción vía ADR/tracking.

Verification required:
- Job CI con linter standalone configurado (si se aprueba D7); golden actualizado tras cada remediación.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: NOT_TRACED · Tests: golden/adversarial (PASS, vía pytest) · CI: NOT_TRACED (standalone no configurado) · Evidence: E-005, E-024 · Closure: OPEN

---

*Registro canónico de findings. Fuente: AUDIT_OCM_TECHNICAL_COMPLIANCE_2026-08-18.md. No se modificó código, tests, CI, ADRs ni tracking.yaml. git add/commit/push: NO ejecutados.*
