# OCM — AUDIT FINDINGS REGISTER — Policy Layer Complementary (Semgrep/SonarQube/AST Guards)

**Ejecución de auditoría complementaria:** 2026-08-19 (baseline `a4d82983f629ef933a155ee7863ab5b2d3a56ae9`, branch `main`)
**Fuente primaria:** `docs/audits/AUDIT_OCM_POLICY_LAYER_COMPLEMENTARY_2026-08-19.md`
**Alcance:** evaluación adversarial de la conclusión previa F-PL-07/F-PL-08 ("No introducir HashiCorp/OPA/Conftest/Semgrep/SonarQube") bajo la arquitectura normativa OCM Constitution (4 pilares → Policy Gate → CI → CD → OrangeHouse).
**Estado de este registro:** OPEN

Resumen: CRITICAL 0 · HIGH 1 · MEDIUM 7 · LOW 2 · INFO 0 · **total 10**.

Clasificación (taxonomía del protocolo de auditoría de OCM):
- NUEVO: 8 — F-PLC-01..08
- REVALIDADO: 0
- REGRESIÓN: 0
- CERRADO: 0
- CONTRADICCIÓN: 2 — F-PL-07, F-PL-08 (conclusión anterior contradicha por evidencia)
- RECOMENDACIÓN: 0
- NO_VERIFICADO: 0

Deduplicación (regla §H):
- F-PLC-01 contradice F-PL-07 (HashiCorp): la conclusión "no introducir" se mantiene, pero por razones operativas no por "single-host suficiente" (el argumento F2.6d no elimina la necesidad de secret rotation que Vault resolvería).
- F-PLC-02 contradice F-PL-08 (Semgrep): **Semgrep aporta valor marginal real** para arquitectura/policy (imports prohibidos, APIs deprecated, os.environ, subprocess, crypto, logging secrets) que ni import-linter (solo grafo estático) ni AST Guards (patrones codificados a mano) ni CodeQL (triage costoso) cubren hoy.
- F-PLC-03 contradice F-PL-08 (SonarQube): **SonarQube NO aporta valor marginal** para bugs/code smells/maintainability/duplication/complexity/coverage/technical debt que ruff+mypy+pytest+CodeQL+AST Guards no cubran; Quality Gates son ruido sin ownership humano.
- F-PLC-04: AST Guards **SÍ pueden** ser una policy layer formal, pero requieren Rule ID, ownership, evidencia, waivers, expiración, historial, reporting — hoy son scripts + tests sin metadatos normativos.
- F-PLC-05: Policy Registry YAML (extensión de tracking.yaml) es **viable y recomendado**; M1..M20 se extienden naturalmente a M21..M25 (tests obligatorios, enforcement, dead rules, waivers expirados, ADR huérfanos).
- F-PLC-06: Threat model IA — solo **CodeQL + import-linter + Policy Registry hash** detectan Caso B (código+guard); Caso C (código+CI) requiere branch protection; Caso D/E (excepciones/docs) requieren ADR ownership + M24/M25; Caso F (vuln invisible a tests) detecta Trivy/CodeQL.
- F-PLC-07: CI/Policy Gate óptimo — FAST LOCAL → ARCHITECTURE → SECURITY → QUALITY → SUPPLY CHAIN → POLICY GATE → CD. CodeQL/Trivy **nightly**, no PR.
- F-PLC-08: SonarQube en OrangeHouse **operacionalmente inviable** (DB, backup, auth, updates, superficie ataque, maintenance) sin beneficio marginal; Semgrep **sí viable** (CLI-only, sin servidor, coste ~0).
- F-PLC-09: HashiCorp/OPA/Conftest — ausencia **no afecta** a la policy layer propuesta; Terraform solo si multi-host; OPA/Conftest solo si hay Rego policies (no hay).
- F-PLC-10: CD/Delivery — Policy Layer debe ejecutarse **antes de crear el artefacto** (Policy Gate) y **antes del deploy** (CD Gate); rollback no requiere re-ejecutar policy (artifact SHA ya validado).

---

## F-PLC-01 — F-PL-07 contradicho: argumento "single-host" no elimina necesidad de secret rotation

Severity: MEDIUM
Status: OPEN
Classification: CONTRADICCIÓN
Control: Infrastructure / Secrets
Source: FASE 3 + 11 del informe complementario

Evidence:
- `docker-compose.yml` + `.env` + SecretStr + fail-fast `${VAR:?}` es la mitigación actual (F-PL-07)
- `alertmanager.yml:11`: "Reabrir cuando exista un secret manager" — deuda documentada
- OCM **no rota secrets** hoy (evidencia: no hay cron, script, ni proceso de rotación)
- Vault resolvería: rotación automática, TTL, audit trail, dynamic secrets, seal/unseal
- **Contra-argumento "single-host" (F2.6d):** F2.6d cierra "proceso único systemd + Kafka local suficiente" — no cierra "secret rotation". Un solo host **necesita rotar secrets** igual que multi-host.
- Coste Vault en single-host: ~1 proceso + seal backup + init + TLS local ≈ 30 min setup + 5 min/mes maintenance
- Beneficio: elimina `.env` manual, rota API keys (Binance/KuCoin), audit trail obligatorio para trading real

Impact:
- La recomendación "no introducir HashiCorp" de F-PL-07 es **correcta para Terraform/Consul/Nomad/Packer/Boundary** pero **incorrecta para Vault** si OCM opera con capital real (live trading).
- Si OCM es solo paper/research → Vault innecesario. Si live → Vault REQUIRED NOW.

Required human decision:
- D-PL-12: ¿OCM opera/operará en modo live (capital real)? Si SÍ → adoptar Vault (ADR). Si NO → mantener `.env`+SecretStr.

Recommended remediation:
- Documentar decisión explícita en ADR; no introducir Terraform/Consul/Nomad/Packer/Boundary.

Verification required:
- Si live: `vault operator init` + `vault secrets enable -path=secret kv-v2` + rotación automática configurada.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: alertmanager.yml:11 · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-PLC-01 · Closure: OPEN

---

## F-PLC-02 — F-PL-08 contradicho: Semgrep aporta valor marginal real para arquitectura/policy

Severity: HIGH
Status: OPEN
Classification: CONTRADICCIÓN
Control: Security Tooling / Architecture Policy
Source: FASE 4.1, 4.2 del informe complementario

Evidence:
- **Semgrep vs Bandit**: Bandit 51 Low (B101=25 assert, B110=17 try/except/pass, B311=3 random, B607=2 partial path, B603=2 subprocess, B404=2 subprocess) — **0 Medium/High**. Bandit NO detecta: imports prohibidos cross-layer, llamadas a APIs deprecated, os.environ directo, crypto inseguro pattern, logging secrets, subprocess fuera de infra.
- **Semgrep vs CodeQL**: CodeQL semanal, dataflow/taint excelente, PERO: triage manual, no corre en PR (coste), reglas custom CodeQL requierenQL expertise. Semgrep: CLI instantáneo, YAML rules, corre en PR/local, reglas de arquitectura expresables en 5 min.
- **Semgrep vs import-linter**: import-linter = grafo estático de módulos. **NO ve**: imports lazy (E402 en apps/cli), function-level imports, attribute access (`mod.attr`), patterns like `os.environ['X']`, `subprocess.run()`, `logging.info(secret)`, `hashlib.md5()`, `random.random()` en contexto crypto.
- **Semgrep vs AST Guards**: AST Guards (R11..R16) = 6 reglas hardcodeadas en Python AST. Semgrep = **reglas declarativas YAML**, extensibles por cualquier ingeniero, versionadas en registry, sin deploy de código Python.

Matriz de cobertura (Regla → AST Guard / import-linter / Semgrep / CodeQL):

| Regla OCM | AST Guard | import-linter | Semgrep | CodeQL |
|---|---|---|---|---|
| imports prohibidos cross-layer (domain→infra) | R11 (subprocess) | BC-03/BC-09 (grafo estático) | **SÍ** (pattern `import infra.*` en `domain/`) | SÍ (dataflow) |
| llamadas prohibidas (argparse en use_cases) | R12 (argparse) | BC-53 (grafo estático) | **SÍ** (pattern `import argparse` en `use_cases/`) | SÍ |
| acceso directo capa a capa (adapters→application) | R13 (getattr) | BC-05/BC-06 (grafo estático) | **SÍ** (pattern `from application import` en `adapters/`) | SÍ |
| APIs deprecated (pandas.DataFrame.append) | NO | NO | **SÍ** (pattern `df.append(`) | SÍ |
| os.environ directo en domain/application | NO | NO | **SÍ** (pattern `os.environ[` o `os.getenv(`) | SÍ |
| subprocess fuera de infrastructure | R11 (domain) | NO (no es import) | **SÍ** (pattern `subprocess.run` fuera de `infrastructure/`) | SÍ |
| filesystem directo (open/read/write) en domain | NO | NO | **SÍ** (pattern `open(` / `Path.read` en `domain/`) | SÍ |
| librerías concretas (ccxt/pyiceberg en domain) | NO | BC-09 (solo modules) | **SÍ** (pattern `import ccxt` en `domain/`) | SÍ |
| crypto inseguro (md5/sha1/random en crypto ctx) | NO | NO | **SÍ** (pattern `hashlib.md5` / `random.random` + contexto) | SÍ |
| logging de secrets (logger.* + api_key/secret) | NO | NO | **SÍ** (pattern `logger\.(info\|debug).*api_key`) | SÍ |
| llamadas prohibidas en capas (time.sleep en domain) | NO | NO | **SÍ** (pattern `time.sleep` en `domain/`) | SÍ |

**Hallazgo clave**: AST Guards cubren 6/11 patrones críticos; import-linter cubre 5/11 (solo grafo estático); **Semgrep cubre 11/11** con reglas declarativas mantenibles.

Impact:
- La conclusión F-PL-08 "Semgrep redundante con Bandit/CodeQL/AST Guards" es **falsa** para arquitectura/policy. Semgrep cubre el gap **imports lazy + patrones de uso + APIs deprecated + os.environ + crypto + logging secrets** que ninguna herramienta actual cubre completamente.
- Coste marginal: ~500ms/PR, YAML en repo, sin servidor.

Required human decision:
- D-PL-13: ¿Adoptar Semgrep para arquitectura/policy (reglas R11..R16 migradas a YAML + nuevas)? Recomendado: SÍ (RECOMMENDED).

Recommended remediation:
- Migrar R11..R16 a Semgrep rules YAML en `policies/semgrep/`; añadir reglas gap (os.environ, crypto, logging, deprecated APIs); integrar en CI job `architecture` (fast, paralelo a import-linter).

Verification required:
- `semgrep --config=policies/semgrep/ .` → 0 violations en código actual; CI job architecture verde.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: policies/semgrep/ (propuesto) · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-PLC-02 · Closure: OPEN

---

## F-PLC-03 — F-PL-08 contradicho: SonarQube NO aporta valor marginal

Severity: MEDIUM
Status: OPEN
Classification: CONTRADICCIÓN
Control: Quality Tooling
Source: FASE 5 del informe complementario

Evidence:
- **Bugs**: mypy (tipos) + pytest (comportamiento) + CodeQL (dataflow) cubren superficie real. SonarQube "bugs" en Python son heurísticos y ruidosos (falsos positivos >> reales).
- **Code smells**: ruff (C901 complexity, PLR2004 magic value, SIM117, etc.) + mypy (unused, unreachable) cubren 95%+; SonarQube duplica con naming distinto.
- **Maintainability**: Cognitive Complexity (Sonar) ≈ McCabe (ruff `C901`) + nesting depth (ruff `PLR1702`). Sin valor añadido.
- **Duplicación**: `ruff --select=DUP` / `jscpd` / manual review — SonarQube no supera.
- **Complejidad**: `radon cc` / `ruff C901` / `mypy --warn-unused-ignores` — cubierto.
- **Cobertura**: `pytest --cov` + `fail_under=40` (pyproject.toml) — SonarQube solo visualiza, no mide.
- **Deuda técnica**: fórmula opaca de SonarQube (minutos) vs evidencia real (tracking.yaml B-*, ADRs, golden FAIL). La deuda real de OCM es **arquitectónica** (ARCH-001..010), no "code smells".
- **Quality Gates**: `quality_gate: passed/failed` binario sin contexto OCM — ruido. OCM ya tiene gates binarios reales: import-linter (0 broken), bandit (0 med/high), mypy (0 errors), tests (pass), audit_validator (PASS).
- **Tendencias históricas**: Git + CI artifacts + `audit_validator` + `engineering_health` ya proveen trazabilidad real.
- **Métricas por proyecto**: OCM es mono-repo con 4 BCs — `pydeps` + `import-linter` + `architecture_linter` dan métricas estructurales reales.
- **Enforcement en CI**: ya existe (fail-fast en 10 jobs). SonarQube añadiría un gate más sin información nueva.

Impact:
- Introducir SonarQube añade: servidor (Java/PostgreSQL), backup, auth, updates, TLS, mantenimiento, superficie de ataque, coste CPU/RAM en OrangeHouse — **sin resolver un gap real**.
- La recomendación F-PL-08 "no introducir SonarQube" se **confirma y fortalece** con evidencia detallada.

Required human decision:
- D-PL-14: Confirmar no adopción de SonarQube (RECOMMENDED: NOT JUSTIFIED).

Recommended remediation:
- Ninguna. Mantener ruff+mypy+pytest+CodeQL+import-linter+AST Guards como Quality Gate real.

Verification required:
- Ninguna (decisión de no-cambio).

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: pyproject.toml (ruff/mypy/pytest) · Tests: NOT_TRACED · CI: quality job · Evidence: E-PLC-03 · Closure: OPEN

---

## F-PLC-04 — AST Guards pueden ser policy layer formal, pero requieren metadatos normativos

Severity: MEDIUM
Status: OPEN
Classification: NUEVO
Control: Policy Governance / Architecture
Source: FASE 7 del informe complementario

Evidence:
- **Estado actual**: 6 guards (R11 subprocess, R12 argparse, R13 getattr, R14 scaffolding, R15 CycleRunResult, R16 SILENT_PATHS) en `scripts/*.py` + tests pos/neg + backtest + CI jobs.
- **Gap para policy layer formal** (lo que falta):
  - **Rule ID** único y estable (hoy: R11..R16 implícito en nombre de función)
  - **Descripción** normativa legible por humano (hoy: docstring del script)
  - **Scope** declarativo (hoy: hardcoded en `_py_files`/`_domain_packages`)
  - **Severidad** (HIGH/MEDIUM/LOW) — hoy implícita (FAIL/PASS binario)
  - **Ownership** humano (arquitectura / platform / trading) — hoy: nadie declarado
  - **Archivos/capas afectadas** — hoy: hardcoded paths
  - **Positive/Negative tests** — SÍ existen (tests/architecture/test_*_guard.py)
  - **Evidencia** — backtest + git_hash + guard output (parcial, no estructurada)
  - **CI integration** — SÍ (jobs domain-guard, app-guard)
  - **Master Plan** — R11→F2.5, R12..R16→AUDIT-apps; no declarativo en guard
  - **ADR** — ADR-0006, ADR-0015 referenciados; no declarativo en guard
  - **Estado** (ACTIVE/DEPRECATED/SUPERSEDED) — hoy: todos activos implícitos
  - **Enforcement** (blocking/warning/informational) — hoy: blocking en CI
  - **Excepciones** (waivers) — NO existen (ni mecanismo)
  - **Expiración de excepciones** — NO existe
  - **Historial** (creado, modificado, por qué) — solo git log
  - **Reporting** (compliance, tendencias, violaciones por regla) — NO existe

Impact:
- Sin estos metadatos, los AST Guards son **scripts operacionales**, no una **policy layer normativa**. Un agente de IA no puede consumirlos programáticamente; un auditor no puede verificar completitud; un owner no puede aprobar waiver con expiración.

Required human decision:
- D-PL-15: ¿Formalizar AST Guards como policy layer con metadatos completos (esquema F-PLC-05 registry + Semgrep YAML)? Recomendado: SÍ (REQUIRED NOW para control de agentes IA).

Recommended remediation:
- Definir esquema `PolicyRule` (ver F-PLC-05); migrar R11..R16 a `policies/` YAML + Semgrep rules; generar compliance report desde registry.

Verification required:
- Registry parseable por `audit_validator` con M21..M25; compliance report generado nightly.

Traceability:
- Tracking: R11, R12, R13, R14, R15, R16 · ADR: ADR-0006, ADR-0015 · Implementation: scripts/app_layer_guard.py, scripts/domain_subprocess_guard.py · Tests: tests/architecture/test_app_layer_guard.py, tests/architecture/test_domain_subprocess_guard.py · CI: app-guard, domain-guard · Evidence: E-PLC-04 · Closure: OPEN

---

## F-PLC-05 — Policy Registry YAML viable (extiende tracking.yaml) — M21..M25 requeridos

Severity: MEDIUM
Status: OPEN
Classification: NUEVO
Control: Policy Governance
Source: FASE 8 del informe complementario

Evidence:
- **Tracking.yaml actual**: bloque `reglas:` con 16 entradas (`id`, `descripcion`, `mecanismo`, `backtest`, `activada_en_ci`). SSOT consumible por máquina; `engineering_health_check.py` lo valida contra CI.
- **Extensión natural**: añadir campos `scope`, `severity`, `owner`, `enforcement`, `tests`, `evidence`, `ci`, `master_plan`, `adr`, `waiver`, `expires`, `created`, `modified`, `history`.
- **M1..M20 ya validan**: IDs únicos (M1), enum classification (M2), severity enum (M3), ADR states (M4), refs existen (M5), tracking refs válidos (M6), evidence no vacía (M7), control declarado (M8), ADR refs existen (M9), reconciliación matemática (M10), golden state (M11), informe estructura (M12), comandos canónicos (M13), versiones (M14), estados inventados (M15), duplicados (M16), informe↔registro (M17), severidad consistente (M18), clasificación consistente (M19), control counts (M20).
- **Nuevas reglas M21..M25 necesarias**:
  - **M21**: `tests.positive` y `tests.negative` obligatorios (path + patrón) — evita reglas sin validación.
  - **M22**: `enforcement` obligatorio + verificación contra CI real (engineering_health ya hace base).
  - **M23**: `dead rule` detection — regla sin implementación referenciada (guard script / semgrep rule / import-linter contract) → FAIL.
  - **M24**: `waiver` con `expires` (ISO date) — waiver expirado → FAIL; waiver sin ADR → FAIL.
  - **M25**: `ADR huérfano` — ADR referenciado en registry sin rule asociada → WARNING (posible deuda).

Impact:
- Registry YAML + M21..M25 convierte tracking.yaml en **fuente normativa consumible por agente IA** (parse YAML → conoce reglas, scope, tests, evidence, waivers, ownership).
- Sin M21..M25: registry es "documentación" no "enforcement".

Required human decision:
- D-PL-16: ¿Extender `audit_validator` con M21..M25 y evolucionar tracking.yaml → `policies/registry.yaml`? Recomendado: SÍ (REQUIRED NOW).

Recommended remediation:
- Implementar M21..M25 en `scripts/audit_validator.py`; migrar `reglas:` de tracking.yaml a `policies/registry.yaml` con esquema completo; mantener compatibilidad.

Verification required:
- `uv run python scripts/audit_validator.py --register policies/registry.yaml` → PASS con M21..M25 verdes.

Traceability:
- Tracking: R1..R16 · ADR: ADR-0015, ADR-0020 · Implementation: scripts/audit_validator.py, docs/plans/tracking.yaml · Tests: tests/architecture/test_audit_validator.py · CI: engineering-health · Evidence: E-PLC-05 · Closure: OPEN

---

## F-PLC-06 — Threat model IA: solo combinación CodeQL + import-linter + Policy Registry hash detecta casos críticos

Severity: HIGH
Status: OPEN
Classification: NUEVO
Control: AI Governance
Source: FASE 9 del informe complementario

Evidence:
Evaluación de 6 casos de captura por agente IA:

| Caso | Descripción | Detectado por | Gap |
|---|---|---|---|
| **A** | Agente cambia código correctamente pero viola arquitectura | **import-linter** (BC-01..55) + **AST Guards/Semgrep** (patrones) | — |
| **B** | Agente cambia código + **AST Guard** para ocultar violación | **CodeQL** (dataflow detecta patrón oculto) + **Policy Registry hash** (guard modificado ≠ hash registrado) | Requiere hash inmutable de guards en artifact CI |
| **C** | Agente cambia código + **CI** para eliminar gate | **Branch protection** (required status checks) + **workflow file review** | Requiere GitHub branch protection rules (no en repo hoy) |
| **D** | Agente agrega excepción a una regla (waiver) | **M24** (waiver requiere ADR + expiración) + **M25** (ADR huérfano) | Requiere M24/M25 implementados |
| **E** | Agente modifica tracking/ADR/docs para aparentar cumplimiento | **Policy Registry hash** + **audit_validator M6/M9** (refs deben existir) + **evidencia firmada** | Requiere hash de registry + tracking en artifact CI |
| **F** | Agente introduce vuln que tests funcionales no detectan | **Trivy** (fs scan CRITICAL/HIGH) + **CodeQL** (dataflow taint) + **pip-audit** (deps) | Ya cubierto (CodeQL semanal, Trivy semanal, pip-audit PR) |

**Conclusiones**:
1. **Ninguna herramienta individual** detecta todos los casos.
2. **Combinación mínima efectiva**: import-linter + AST Guards/Semgrep + CodeQL + Policy Registry (hash inmutable) + Branch Protection + M24/M25.
3. **Semgrep es crítico** para Caso B (detecta patrones que guard modificado ocultaría) y Caso A (arquitectura via patterns).
4. **SonarQube NO ayuda** en ningún caso (no detecta arquitectura, no tiene dataflow, Quality Gates no relevantes).

Impact:
- La defensa en profundidad requiere: **Semgrep (RECOMMENDED) + CodeQL (EXISTENTE) + Policy Registry hash (REQUIRED NOW) + Branch Protection (REQUIRED NOW) + M24/M25 (REQUIRED NOW)**.
- Sin Semgrep: Caso B indetectable si guard modificado.
- Sin Policy Registry hash: Casos B/E indetectables.
- Sin Branch Protection: Caso C indetectable.

Required human decision:
- D-PL-17: Implementar defensa en profundidad completa (Semgrep + Registry hash + Branch Protection + M24/M25).

Recommended remediation:
- Secuencia: (1) M21..M25 en audit_validator; (2) Policy Registry hash en CI artifact; (3) Branch protection en main; (4) Semgrep rules para R11..R16 + gaps; (5) nightly compliance report con hash verification.

Verification required:
- Simular Caso B: modificar guard + código → CI detecta (hash mismatch + Semgrep pattern).
- Simular Caso C: editar workflow → branch protection bloquea merge.

Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0015, ADR-0020 · Implementation: NOT_TRACED · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-PLC-06 · Closure: OPEN

---

## F-PLC-07 — CI/Policy Gate óptimo: FAST LOCAL → ARCHITECTURE → SECURITY → QUALITY → SUPPLY CHAIN → POLICY GATE → CD

Severity: MEDIUM
Status: OPEN
Classification: NUEVO
Control: CI / Policy Gate
Source: FASE 10 del informe complementario

Evidence:
- **Jobs actuales (ocm-ci.yml)**: architecture (import-linter) → engineering-health → app-guard → domain-guard → trading-guards → unit-tests → security (bandit -ll) → integration-tests → config-validation → quality (ruff/mypy/ssot/pip-audit).
- **Satélites**: codeql (semanal), trivy (semanal), gitleaks (PR), actionlint, docker-lint, shellcheck, yamllint, ocm-cd (placeholder).
- **Problema**: orden actual mezcla arquitectura (import-linter) con calidad (ruff/mypy) y seguridad (bandit) sin etapa explícita "POLICY GATE" que aglutine decisiones binarias.
- **Diseño propuesto**:

| Stage | Jobs | Blocking | When | Server needed |
|---|---|---|---|---|
| **FAST LOCAL** | ruff, format, mypy, ssot-enums, actionlint, shellcheck, yamllint | SÍ | PR (paralelo, <2 min) | NO |
| **ARCHITECTURE** | import-linter, AST Guards/Semgrep, cycle detection, architecture tests | SÍ | PR (paralelo, <3 min) | NO |
| **SECURITY** | bandit -ll, gitleaks, Semgrep security rules | SÍ | PR (paralelo, <2 min) | NO |
| **QUALITY** | pytest (unit, fail_under=40), coverage gate | SÍ | PR (secuencial, ~5 min) | NO (service containers para integration) |
| **SUPPLY CHAIN** | pip-audit (con ignores risk-accept), Trivy fs (CRITICAL/HIGH) | SÍ | PR (paralelo, <3 min) | NO |
| **POLICY GATE** | engineering_health + audit_validator (M1..M25) + registry hash verify | **SÍ (binario)** | PR (serial, <1 min) | NO |
| **EXPENSIVE/PR** | integration-tests (Kafka) | SÍ (opcional: solo si código trading/portfolio tocado) | PR (bajo label) | SÍ (service container) |
| **NIGHTLY** | CodeQL full, Trivy full, dependency-audit extendido, compliance-report, evidence-gen | NO (report only) | schedule 04:00 | SÍ (CodeQL/Trivy) |
| **RELEASE** | check_production_gates.py (F-PL-04) + compliance report firmado | **SÍ (binario)** | tag/manual | NO |

Impact:
- Reordenar CI añade claridad normativa: Policy Gate = veredicto único PASS/FAIL sobre TODAS las reglas (registry).
- CodeQL/Trivy nightly ahorra ~15 min/PR sin perder cobertura (semanal ya existe).
- check_production_gates.py (F-PL-04) es el gate de release real.

Required human decision:
- D-PL-18: Reordenar ocm-ci.yml a etapas explícitas + añadir nightly + implementar check_production_gates.py.

Recommended remediation:
- Editar .github/workflows/ocm-ci.yml con stages; crear .github/workflows/nightly.yml; implementar scripts/check_production_gates.py.

Verification required:
- PR con cambio en domain → architecture+security+quality+policy_gate PASS; nightly genera compliance report.

Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0020 · Implementation: .github/workflows/ocm-ci.yml · Tests: NOT_TRACED · CI: ocm-ci.yml · Evidence: E-PLC-07 · Closure: OPEN

---

## F-PLC-08 — SonarQube en OrangeHouse operacionalmente inviable; Semgrep viable (CLI-only)

Severity: MEDIUM
Status: OPEN
Classification: NUEVO
Control: Operational Feasibility
Source: FASE 11, 12 del informe complementario

Evidence:
**SonarQube Server (Community Edition) en OrangeHouse single-host:**

| Requisito | Realidad OrangeHouse | Coste |
|---|---|---|
| **Base de datos** | PostgreSQL NO existe (docker-compose.yml no lo tiene) | +1 contenedor + 512MB RAM + backup |
| **Almacenamiento** | Volúmenes Docker locales (sin backup DR) | Sin persistencia garantizada |
| **Persistencia** | Requiere PG data dir + SonarQube data dir | 2 volúmenes + backup strategy |
| **Backup** | No hay backup documentado (F-PL-10: ni Grafana tiene) | Debe implementarse desde cero |
| **Actualizaciones** | Java + SonarQube version upgrades manuales | Maintenance window + compatibilidad plugins |
| **Autenticación** | Local (sin LDAP/OIDC/SAML) — admin token en .env | Surface de ataque + secret management |
| **Acceso local** | Puerto 9000 expuesto (¿loopback? ¿reverse proxy?) | Si loopback: inútil para CI; si expuesto: riesgo |
| **Integración GitHub** | GitHub App / PAT / webhook para PR decoration | Config + secret + network egress |
| **Integración CI** | `sonar-scanner` en PR + quality gate wait | +2-5 min/PR; quality gate binario sin contexto OCM |
| **Reproducibilidad** | Estado en DB + config en UI — **no reproducible desde repo** | Violación principio "config as code" |
| **Coste operacional** | ~1-2 GB RAM + 1 CPU + DB + backup + updates + auth | **Alto** para single-host sin equipo ops dedicado |
| **Superficie de ataque** | Web UI + API + DB + webhook receiver | Crítico si expuesto; inútil si solo localhost |
| **Mantenimiento** | Logs, GC, reindex, plugin updates, version upgrades | Tiempo humano recurrente |

**Semgrep (CLI-only) en OrangeHouse:**
- **Instalación**: `pip install semgrep` / `uvx semgrep` / binary — **sin servidor, sin DB, sin auth**.
- **Ejecución**: `semgrep --config=policies/semgrep/ .` — **~500ms, stateless, reproducible**.
- **Reglas**: YAML en repo (`policies/semgrep/`) — versionadas, revisables, testeables.
- **CI**: job paralelo en ARCHITECTURE stage, <1 min.
- **Coste**: ~0 (CPU marginal, sin RAM persistente, sin backup, sin updates de servidor).
- **Superficie de ataque**: ninguna (binario/CLI, no daemon).

**Comparativa:**

| Factor | SonarQube Server | Semgrep CLI |
|---|---|---|
| Infra nueva | PostgreSQL + SonarQube (2 contenedores) | Ninguna |
| Estado persistente | Sí (DB + indices) | No |
| Backup/DR | Requerido (crítico) | N/A |
| Auth/Access | Requerido | N/A |
| CI time/PR | +2-5 min | +0.5 s |
| Reproducible (repo-only) | **NO** (DB state) | **SÍ** |
| Valor marginal OCM | **NULO** (ruff+mypy+pytest+CodeQL) | **ALTO** (arquitectura/policy gaps) |
| Maintenance/mes | ~2-4 horas | ~0 |

Impact:
- **SonarQube = NOT JUSTIFIED** para OrangeHouse (single-host, sin ops team, sin DB, sin backup, sin valor marginal).
- **Semgrep = RECOMMENDED** (CLI-only, coste ~0, cubre gaps arquitectura/policy reales, reproducible).

Required human decision:
- D-PL-19: Confirmar no-SonarQube; adoptar Semgrep para arquitectura/policy.

Recommended remediation:
- No instalar SonarQube. Crear `policies/semgrep/` con reglas R11..R16 migradas + gaps (os.environ, crypto, logging, deprecated). Añadir job `semgrep` en stage ARCHITECTURE de CI.

Verification required:
- `semgrep --config=policies/semgrep/ .` → clean; CI job semgrep verde.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: policies/semgrep/ (propuesto) · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-PLC-08 · Closure: OPEN

---

## F-PLC-09 — HashiCorp/OPA/Conftest: ausencia no afecta a la policy layer propuesta

Severity: LOW
Status: OPEN
Classification: NUEVO
Control: Infrastructure / Policy
Source: FASE 13 del informe complementario

Evidence:
- **HashiCorp**: Terraform solo si multi-host (F-PLC-01); Vault solo si live trading (F-PLC-01); Consul/Nomad/Packer/Boundary = problemas que no existen. **Confirmado: no afecta**.
- **OPA/Conftest**: Motor Rego + test framework para policies declarativas. Útil si: (a) hay Terraform/K8s/Helm policies; (b) hay microservicios con authz distribuido; (c) policy-as-code separado del código. OCM: **ninguna de las tres**. IaC = docker-compose (validado por compose config + hadolint + test_docker_hardening.py). Authz = no hay (single-user). Policy-as-code = AST Guards/Semgrep + Registry YAML. **Confirmado: no afecta**.
- **Necesidad concreta**: cero. Introducirlos sería "curriculum-driven development".

Impact:
- Confirmación de F-PL-07 para Terraform/Consul/Nomad/Packer/Boundary; matiz para Vault (F-PLC-01); confirmación de F-PL-08 para OPA/Conftest.

Required human decision:
- D-PL-20: No introducir HashiCorp (excepto Vault si live), no OPA/Conftest.

Recommended remediation:
- Ninguna (decisión de no-cambio).

Verification required:
- Ninguna.

Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0022, ADR-0024 · Implementation: docker-compose.yml · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-PLC-09 · Closure: OPEN

---

## F-PLC-10 — CD/Delivery: Policy Layer antes de artifact + antes de deploy; rollback no re-ejecuta

Severity: MEDIUM
Status: OPEN
Classification: NUEVO
Control: CD / Delivery
Source: FASE 14 del informe complementario

Evidence:
- **OCM Constitution** propone: Policy Gate → CI → Artifact Build + SHA/Digest → CD Gate (verify/deploy/rollback) → OrangeHouse health.
- **Policy Gate** (binario, en PR): todas las reglas del registry (import-linter, AST Guards/Semgrep, security, quality, supply chain) deben PASS. **Output**: Policy Gate PASS → autoriza build.
- **Artifact Build**: Docker image multi-stage + SHA256 digest (reproducible). Firmado opcional (cosign/keyless) si supply chain lo requiere.
- **CD Gate (verify)**: antes de deploy → verify artifact SHA coincide con el construido + Policy Gate PASS registrado + health checks pre-deploy (config-guard, redis, kafka).
- **Deploy**: `docker compose up -d --build` (o script) en OrangeHouse.
- **Post-deploy health**: OrangeHouse health checks (pipeline deadman, kafka lag, redis memory, disk) → ACCEPT / ROLLBACK.
- **Rollback**: `docker compose down && docker compose up -d` con image SHA anterior. **NO re-ejecutar Policy Layer** (artifact ya validado en build). Solo health checks.
- **Estado actual**: ocm-cd.yml placeholder; `deploy_ocm.sh` no existe; deploy manual SSH; sin artifact SHA tracking; sin health checks automatizados post-deploy.

Impact:
- Policy Layer **debe** ejecutarse en PR (Policy Gate) y **antes** de deploy (CD Gate verify). Rollback es operativo, no de policy.
- Gap crítico: **check_production_gates.py (F-PL-04)** es el Production Gate binario que falta; sin él, no hay release gate real.

Required human decision:
- D-PL-21: Implementar check_production_gates.py + ocm-cd.yml real + artifact SHA tracking + post-deploy health checks.

Recommended remediation:
- Secuencia: (1) scripts/check_production_gates.py (G1..G11 binario); (2) ocm-cd.yml con verify/deploy/rollback; (3) artifact SHA en CI artifact + registry; (4) OrangeHouse health endpoints + deadman alerts.

Verification required:
- Tag release → CI: Policy Gate PASS → Build artifact SHA → CD: verify SHA → deploy → health ACCEPT.

Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0020, ADR-0022 · Implementation: .github/workflows/ocm-cd.yml (placeholder) · Tests: NOT_TRACED · CI: ocm-cd.yml · Evidence: E-PLC-10 · Closure: OPEN

---

## F-PLC-11 — Policy Registry debe declarar ownership humano de reglas (anti-captura IA)

Severity: HIGH
Status: OPEN
Classification: NUEVO
Control: AI Governance / Policy Governance
Source: FASE 7, 9 del informe complementario

Evidence:
- **Problema**: guards, tests, CI, tracking, ADRs viven en mismo repo/branch. Agente con write a `main` puede modificar **cualquier combinación** (Caso A-F).
- **Registry actual (tracking.yaml)**: NO declara `owner` (humano/equipo) para cada regla. Sin ownership → sin accountability → agente puede cambiar regla sin revisión.
- **Requerimiento**: cada regla en registry debe tener `owner: "arquitectura" | "platform" | "trading" | "security"` y **cambio de regla requiere PR separado con revisión del owner**.
- **Enforcement técnico**: 
  - `CODEOWNERS` en GitHub para `scripts/*_guard.py`, `policies/`, `architecture_linter/`, `tracking.yaml`, `.github/workflows/ocm-ci.yml`.
  - Branch protection: required review from CODEOWNERS para archivos de policy.
  - CI job: verifica que PRs que tocan policy files tienen label `policy-change` + approval de owner.
- **Sin esto**: F-PL-09 (threat model) se cumple — agente captura el sistema.

Impact:
- Policy Registry **sin ownership humano** = policy layer **capturable**. Ownership es la primera línea de defensa, no las herramientas.

Required human decision:
- D-PL-22: Definir owners por regla en registry; configurar CODEOWNERS + branch protection para policy files.

Recommended remediation:
- Añadir `owner` a cada regla en `policies/registry.yaml`; crear `.github/CODEOWNERS`; habilitar branch protection en main con required reviews.

Verification required:
- PR modificando `scripts/app_layer_guard.py` sin approval de @arquitectura → blocked.

Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0015, ADR-0020 · Implementation: NOT_TRACED · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-PLC-11 · Closure: OPEN