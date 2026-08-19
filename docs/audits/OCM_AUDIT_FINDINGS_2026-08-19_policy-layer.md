# OCM — AUDIT FINDINGS REGISTER — Policy Layer Feasibility

**Ejecución de auditoría:** 2026-08-19 (baseline `a4d82983f629ef933a155ee7863ab5b2d3a56ae9`, branch `main`)
**Fuente primaria:** `docs/audits/AUDIT_OCM_POLICY_LAYER_FEASIBILITY_2026-08-19.md`
**Alcance:** viabilidad técnica, arquitectónica y operativa de una Policy Layer formal (FASE 1..13 del encargo) sobre infraestructura self-hosted OrangeHouse.
**Estado de este registro:** OPEN

Resumen: CRITICAL 1 · HIGH 1 · MEDIUM 5 · LOW 4 · INFO 0 · **total 11**.

Clasificación (taxonomía del protocolo de auditoría de OCM):
- NUEVO: 1 — F-PL-10
- REVALIDADO: 5 — F-PL-01, F-PL-02, F-PL-03, F-PL-05, F-PL-11
- REGRESIÓN: 0
- CERRADO: 0
- CONTRADICCIÓN: 1 — F-PL-04
- RECOMENDACIÓN: 4 — F-PL-06, F-PL-07, F-PL-08, F-PL-09
- NO_VERIFICADO: 0

Deduplicación (regla §H):
- Control FAIL ≠ Finding NUEVO: pip-audit (F-PL-02 = F-CI-01 de 2026-08-18), yamllint (F-PL-03 = F-CI-02), architecture_linter (F-PL-01 = F-ARCH-01..06), drift ccxt (F-PL-05 = F-GOV-02), systemd NO_VERIFICADO (F-PL-11 = tracking.yaml `systemd_reinicia_correctamente`).
- Los 7 FAIL de architecture_linter → un único finding REVALIDADO (misma causa raíz: deuda arquitectónica gobernada por golden).

---

## F-PL-01 — Architecture Linter: 7/10 reglas en FAIL (deuda gobernada)

Severity: HIGH
Status: OPEN
Classification: REVALIDADO
Control: Architecture Linter
Source: architecture_linter (AST, stdlib-only) — `uv run python -m architecture_linter --root . --json`

Evidence:
- `architecture_linter --root . --json` → summary: total 10, passed 3, failed 7, findings 19, failed_findings 16
- Reglas FAIL: ARCH-001 (multi-owner posición), ARCH-002 (divergencia semántica), ARCH-004 (balance real), ARCH-005 (freshness), ARCH-007 (homónimos), ARCH-008 (stub WSTradesSource), ARCH-010 (estado mutable duplicado)
- Reglas PASS: ARCH-003 (reconciliación órdenes), ARCH-006 (sin ports huérfanos), ARCH-009 (capas)
- Golden fija el estado esperado con FAIL/PARTIAL (deuda legítima, no-regresión): `tests/architecture_linter/test_golden.py` — 4 passed
- Ya registrado: F-ARCH-01..06 en `OCM_AUDIT_FINDINGS_2026-08-18.md` (misma deuda)

Impact:
- No bloquea merge (el linter no es gate CI — ver F-CI-03 en registro 2026-08-18); la deuda es conocida, gobernada por golden y en backlog (B-21, ADR-0021/0030).

Required human decision:
- Mantener como deuda gobernada o priorizar su resolución en F4/F5; no es hallazgo nuevo.

Recommended remediation:
- Ninguna para esta auditoría (viabilidad); el Policy Registry propuesto (F-PL-06) debe declarar estas reglas como `expected_fail: golden` con evidence.

Verification required:
- `uv run pytest tests/architecture_linter/test_golden.py -q --no-cov` → 4 passed.

Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0021, ADR-0030 · Implementation: architecture_linter/ · Tests: tests/architecture_linter/test_golden.py · CI: golden job · Evidence: E-PL-01 · Closure: OPEN

---

## F-PL-02 — pip-audit: 4 vulnerabilidades activas

Severity: CRITICAL
Status: OPEN
Classification: REVALIDADO
Control: Dependency Security
Source: ocm-ci.yml / job `quality` — comando canónico del CI

Evidence:
- `uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325` → exit 1
- aiohttp 3.14.1 → PYSEC-2026-3545 (fix 3.14.3), PYSEC-2026-3546 (fix 3.14.2), PYSEC-2026-3547 (fix 3.14.2)
- cryptography 49.0.0 → PYSEC-2026-3552 (fix 50.0.0)
- Los 2 ignores del risk-accept documentado (2026-08-03) no cubren estas 4 vulns
- Ya registrado: F-CI-01 en `OCM_AUDIT_FINDINGS_2026-08-18.md` (misma causa raíz)

Impact:
- Gate de seguridad de CI rojo (merge bloqueado); superficie de red HTTP (aiohttp) y crypto (cryptography) sin mitigar ni risk-accept formal.

Required human decision:
- D-PL-01: bump `aiohttp` ≥3.14.3 y `cryptography` ≥50.0.0 validando staging, o risk-accept formal (ADR + tracking). Prohibido ampliar ignore-list sin aprobación.

Recommended remediation:
- Actualizar dependencias y validar staging; formalizar risk-accept si no es viable de inmediato.

Verification required:
- `uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325` → exit 0.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: pyproject.toml · Tests: NOT_TRACED · CI: quality job · Evidence: E-PL-02, E-012, E-031 · Closure: OPEN

---

## F-PL-03 — yamllint: error en deploy/monitoring/alerts.yml

Severity: LOW
Status: OPEN
Classification: REVALIDADO
Control: YAML Lint
Source: `.github/workflows/yamllint.yml` / `deploy/monitoring/alerts.yml`

Evidence:
- `uvx yamllint -c .yamllint .` → exit 1
- `deploy/monitoring/alerts.yml:66:162` — error `new-line-at-end-of-file`
- Ya registrado: F-CI-02 en `OCM_AUDIT_FINDINGS_2026-08-18.md` (mismo archivo/línea)

Impact:
- Job de CI rojo; brecha menor de higiene de archivo.

Required human decision:
- Añadir newline al final de `deploy/monitoring/alerts.yml` (1-liner, no requiere ADR).

Recommended remediation:
- Corregir el newline; si reaparece, añadir autofix en pre-commit.

Verification required:
- `uvx yamllint -c .yamllint .` → exit 0.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: deploy/monitoring/alerts.yml · Tests: NOT_TRACED · CI: yamllint job · Evidence: E-PL-03, E-106 · Closure: OPEN

---

## F-PL-04 — `check_production_gates.py` referenciado como ejecutable pero inexistente

Severity: MEDIUM
Status: OPEN
Classification: CONTRADICCIÓN
Control: Production Gate
Source: `docs/PLAN-Maestro-Ingenieria.md` vs filesystem

Evidence:
- Plan Maestro §1:38 "Ejecutar `scripts/check_production_gates.py` (veredicto binario PASS/FAIL)" — presentado como flujo rápido operativo
- Plan Maestro §10:94 (métrica madurez) y §6:374 (veredicto binario G1..G11) — descrito como mecanismo existente
- Plan Maestro F1:120 "Criterio de salida: `scripts/check_production_gates.py` → G1–G4 PASS"
- `ls scripts/` → NO existe `check_production_gates.py` (8 scripts reales listados)
- El gate G1..G11 no tiene veredicto binario ejecutable; el `engineering_health_check.py` (F2.0) cubre coherencia normativa pero NO el Production Gate de release completo

Impact:
- El Production Gate (ADR-0020, §6 del Plan) no tiene su veredicto binario declarado; un agente de IA no puede demostrar "production-ready" con un comando. Debilita el caso de uso del Policy Registry.

Required human decision:
- D-PL-02: implementar `scripts/check_production_gates.py` (veredicto binario G1..G11 sobre evidencia mecánica) o actualizar el Plan para reflejar que `engineering_health_check.py` + jobs CI son el mecanismo vigente.

Recommended remediation:
- Como parte de la Policy Layer: implementar el script como primer gate de evidencia del Policy Registry (piloto F-PL-06).

Verification required:
- `scripts/check_production_gates.py` existe y devuelve PASS/FAIL binario sobre los 11 checks G1..G11.

Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0020 · Implementation: scripts/ (ausente) · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-PL-04 · Closure: OPEN

---

## F-PL-05 — Drift de documentación: pin ccxt (comentario vs valor)

Severity: LOW
Status: OPEN
Classification: REVALIDADO
Control: Documentation
Source: `pyproject.toml`

Evidence:
- `pyproject.toml:100` → `ccxt==4.5.70` (valor vigente) vs comentario `:97` "pinneado en 4.3.58" (obsoleto)
- `AGENTS.md` cita `ccxt==4.3.58` como pin
- Ya registrado: F-GOV-02 en `OCM_AUDIT_FINDINGS_2026-08-18.md` (drift de documentación de ccxt)

Impact:
- Riesgo de bump incorrecto: un agente/ingeniero que confíe en el comentario fijaría 4.3.58, degradando la resolución actual.

Required human decision:
- Corregir comentario en pyproject.toml y AGENTS.md (no requiere ADR).

Recommended remediation:
- Corrección documental; el Policy Registry puede añadir una regla `pin-comment-drift` para detectarlo.

Verification required:
- `grep -n "ccxt" pyproject.toml AGENTS.md` → comentario y valor consistentes.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: pyproject.toml · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-PL-05 · Closure: OPEN

---

## F-PL-06 — Viabilidad: formalizar un OCM Policy Registry (YAML) como SSOT de reglas

Severity: MEDIUM
Status: OPEN
Classification: RECOMENDACIÓN
Control: Policy Governance
Source: análisis FASE 4, 9, 10, 11 del informe canónico

Evidence:
- Ya existe la semilla: `tracking.yaml` reglas R1..R16 con `backtest` + `activada_en_ci` (SSOT consumible por máquina, validado por `engineering_health_check.py`)
- ADR-0020 convierte `activada_en_ci` en normativo (gate de release); ADR-0015 formaliza el patrón guard AST + tests pos/neg + backtest + CI
- `scripts/audit_validator.py` (M1..M20) ya valida registros de findings con enums cerrados y reconciliación matemática — infraestructura reutilizable
- No existe policy engine externo (OPA/Conftest/Semgrep/Sonar/CodeQL-as-gate — NO ENCONTRADO); los gates son import-linter + AST guards + CI

Impact:
- OCM ya tiene ~70% de la Policy Layer en forma embrionaria (tracking.yaml + guards + CI + health). Formalizarla como "Policy Registry" es evolución incremental, no arquitectura nueva: bajo riesgo, alto valor para el control de agentes de IA.

Required human decision:
- D-PL-03: adoptar YAML como fuente de verdad del registry (recomendado — compatible con tracking.yaml/audit_validator, sin infraestructura nueva), con esquema `id/name/description/scope/severity/owner/enforcement/tests/evidence/ci/master_plan/adr`.

Recommended remediation:
- Evolucionar el bloque `reglas:` de tracking.yaml hacia el esquema completo del registry (o módulo YAML dedicado `policies/`), manteniendo `audit_validator` como motor M1..M20 + reglas nuevas M21.. (duplicados, dead rules, waivers expirados).

Verification required:
- registry YAML válido, parseado por `audit_validator` con reglas M21+ verdes.

Traceability:
- Tracking: R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12, R13, R14, R15, R16 · ADR: ADR-0020, ADR-0015 · Implementation: scripts/audit_validator.py · Tests: tests/architecture/test_audit_validator.py · CI: engineering-health · Evidence: E-PL-06 · Closure: OPEN

---

## F-PL-07 — No introducir HashiCorp stack (Terraform/Vault/Consul/Nomad/Packer/Boundary)

Severity: LOW
Status: OPEN
Classification: RECOMENDACIÓN
Control: Infrastructure
Source: análisis FASE 2, 3 del informe canónico

Evidence:
- Infraestructura real = Docker Compose single-host (docker-compose.yml, 621 líneas, 10+ servicios base), sin VMs, sin inventario de hosts, sin self-hosted runners
- Secretos: `.env` gitignored (nunca versionado, verificado) + SecretStr Pydantic + `REDIS_PASSWORD:?...` fail-fast + `.dockerignore`; sin Vault, sin secret manager (alertmanager.yml:11 "Reabrir cuando exista un secret manager")
- Despliegue: `docker compose up -d`; CD placeholder deshabilitado; deploy manual vía SSH sin script
- F2.6 (CERRADA): proceso único systemd + Kafka local suficiente; canary 30min CPU 0.00% / RAM 40.4MB
- 0 matches de Terraform/Vault/Consul/Nomad/Packer en todo el repo

Impact:
- Terraform/Vault/Consul/Nomad/Packer/Boundary resolverían problemas que OCM no tiene hoy (multi-host, HA, discovery, secret rotation, scheduler, imágenes, acceso admin). Su coste operativo (un proceso nuevo por herramienta en un solo host, TLS/HA, backup de seal) supera el beneficio. Introducirlos violaría el principio no-sobrediseño y F2.6d.

Required human decision:
- D-PL-04: no introducir el stack HashiCorp; mantener Docker Compose + `.env` + systemd (ADR-0022) + Git como mecanismos vigentes.

Recommended remediation:
- Solo si el inventario de OrangeHouse crece (multi-host) re-evaluar; registrar la decisión en el Plan.

Verification required:
- Ninguna (decisión de no-cambio).

Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0022, ADR-0024 · Implementation: docker-compose.yml · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-PL-07 · Closure: OPEN

---

## F-PL-08 — No introducir OPA/Conftest/Semgrep/SonarQube como capa de policy

Severity: LOW
Status: OPEN
Classification: RECOMENDACIÓN
Control: Security Tooling
Source: análisis FASE 5, 6 del informe canónico

Evidence:
- import-linter (50 BC) + AST guards (R11..R16) + Bandit + ruff + mypy + pip-audit + CodeQL + Trivy + Gitleaks ya cubren: boundaries, invariantes estructurales, APIs peligrosas, vulns, taint, secrets
- 0 matches de Semgrep/Sonar/OPA/Conftest en el repo
- CodeQL ya está en CI (`codeql.yml`, schedule semanal) con dataflow/taint que Semgrep no superaría marginalmente para Python en un repo de este tamaño
- SonarQube duplicaría ruff+mypy+pytest (complexity/duplication/coverage ya cubiertos) sin valor marginal para un single-host

Impact:
- Herramientas nuevas añaden superficie, cómputo y deps sin resolver un problema real que las actuales no cubran. El hueco real (policy de reglas con SSOT + evidencia) se llena con el Policy Registry (F-PL-06), no con más scanners.

Required human decision:
- D-PL-05: no introducir Semgrep/SonarQube/OPA/Conftest; la IaC policy se limita a `docker-compose config` + Hadolint + rules de hardening ya presentes (test_docker_hardening.py).

Recommended remediation:
- Re-evaluar Semgrep solo si los AST guards no cubren un patrón real de seguridad (no anticipado).

Verification required:
- Ninguna (decisión de no-cambio).

Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0015 (patrón guard) · Implementation: codeql.yml, trivy.yml, test_docker_hardening.py · Tests: NOT_TRACED · CI: codeql/trivy jobs · Evidence: E-PL-08 · Closure: OPEN

---

## F-PL-09 — Separación de privilegios para agentes de IA (threat model)

Severity: MEDIUM
Status: OPEN
Classification: RECOMENDACIÓN
Control: AI Governance
Source: análisis FASE 7 del informe canónico

Evidence:
- Los guards (scripts/domain_subprocess_guard.py, app_layer_guard.py) y sus tests viven en el MISMO repo y rama que el código protegido — un agente con acceso de escritura a main puede modificar guard+tests+CI+tracking simultáneamente
- El audit_validator valida el registro/informe pero NO el contenido de los guards
- No existe firma de evidencia: un agente puede regenerar evidencia falsa (git_hash inyectado desde composition root, B-20) y manipular tracking.yaml
- No hay protección de branch (branch protection rules no declaradas en repo) ni separación de roles CI/prod

Impact:
- Riesgo real de captura del sistema de validación por el agente que la propia validación debería contener. La pregunta central del encargo ("¿puede un agente modificar simultáneamente el código y las reglas?") tiene respuesta afirmativa con la configuración actual.

Required human decision:
- D-PL-06: aplicar separación de privilegios: (a) branch protection en main (required reviews + status checks); (b) firmar evidencia con hash de guards+config (no solo git_hash); (c) tratar los guards como código sagrado (cambios de guard requieren ADR + revisión humana); (d) separar el policy registry de la PR de feature (config as code reviewado por humano).

Recommended remediation:
- Documentar en ADR el threat model y las reglas de separación; el Policy Registry (F-PL-06) debe declarar ownership humano de las reglas.

Verification required:
- Branch protection activa en main; verificación de integridad de guards en CI (hash de los scripts + tracking comparado contra registro inmutable).

Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0015, ADR-0020 (contexto) · Implementation: NOT_TRACED · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-PL-09 · Closure: OPEN

---

## F-PL-10 — Grafana provisioning/dashboards vacíos y gitignored (reproducibilidad rota)

Severity: MEDIUM
Status: OPEN
Classification: NUEVO
Control: Infrastructure Reproducibility
Source: docker-compose.yml + .gitignore

Evidence:
- docker-compose.yml:237-238 monta `./deploy/monitoring/grafana/provisioning` y `./dashboards`
- `.gitignore:72-73` → ambos directorios están gitignored
- `ls deploy/monitoring/grafana/` → `provisioning/` y `dashboards/` vacíos (creados ago 9)
- README.md:267/274 referencia dashboards "provisionados desde deploy/" — no reproducible desde el repo
- No registrado en tracking ni en registros de findings previos (2026-08-18)

Impact:
- El stack de observabilidad Grafana no es reproducible: un clon limpio del repo obtiene Grafana sin dashboards ni datasources. Violación del principio "qué partes son reproducibles" (FASE 2, pregunta 10) y de la reproducibilidad que exige la evidencia de auditoría.

Required human decision:
- D-PL-07: versionar el provisioning de Grafana (dashboards JSON + datasources) o eliminar el montaje de directorios vacíos y documentar el setup manual.

Recommended remediation:
- Añadir dashboards JSON (p.ej. pipeline health, kafka) al repo y quitar del .gitignore; o declarar Grafana "sin provisioning" y eliminar montajes.

Verification required:
- `deploy/monitoring/grafana/provisioning/` y `dashboards/` versionados con contenido real, o montajes eliminados.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: docker-compose.yml · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-PL-10 · Closure: OPEN

---

## F-PL-11 — Unit systemd de `streaming` NO_VERIFICADA

Severity: LOW
Status: OPEN
Classification: REVALIDADO
Control: Ops Lifecycle
Source: ADR-0022 + tracking.yaml

Evidence:
- tracking.yaml:2573 → `systemd_reinicia_correctamente: NO_VERIFICADO`
- ADR-0022:138-140 → la unidad systemd de streaming "queda supervisada por su propia unidad"; ADR-0022:323 → "Sin unit systemd activo (systemctl vacío para market_data/streaming)"
- Canary F2.6c arrancado manualmente (fase3.5c-capacity-empirico.md:94)
- Ya registrado en tracking como criterio NO_VERIFICADO (deuda conocida)

Impact:
- El modelo operativo declarado (systemd supervisando streaming) no está verificado; un reinicio de OrangeHouse no está cubierto por evidencia.

Required human decision:
- D-PL-08: verificar la unidad systemd o declarar el modelo de lifecycle alternativo (Docker restart: unless-stopped ya cubre los servicios de compose).

Recommended remediation:
- Crear la unit systemd y verificar reinicio, o documentar que streaming corre bajo Docker/systemd del host según ADR-0022.

Verification required:
- `systemctl status streaming` → active; test de reinicio documentado.

Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0022 · Implementation: NOT_TRACED · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-PL-11 · Closure: OPEN