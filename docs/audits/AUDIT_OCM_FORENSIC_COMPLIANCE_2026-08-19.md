# AUDIT — OCM Policy Layer Feasibility

**Fecha:** 2026-08-19
**Rol solicitado:** Principal Software Architect + DevSecOps Architect + Infrastructure Architect + Python Architecture Auditor
**Contexto normativo:** infraestructura self-hosted `OrangeHouse` (single-host, bare-metal/local) — NO cloud.

---

## 1. Executive Summary

OCM ya posee ~70 % de lo que el encargo describe como "Policy Layer formal", pero distribuido e implícito en lugar de declarado. Los componentes reales verificados son: `tracking.yaml` (SSOT de reglas R1..R16 con `backtest` + `activada_en_ci`), AST guards (R11..R16), `architecture_linter` (ARCH-001..010), `scripts/audit_validator.py` (M1..M20), `scripts/engineering_health_check.py` (F2.0), import-linter (50 BC), 9 workflows CI con 10 jobs fail-fast, y 8 workflows satélite (CodeQL, Trivy, Gitleaks, Hadolint, ShellCheck, yamllint, actionlint).

La conclusión central de la viabilidad es **ALTA**: la formalización de una Policy Layer es evolución incremental sobre infraestructura existente, no arquitectura nueva. La fuente de verdad recomendada es **YAML** (continuidad con tracking.yaml y el motor M1..M20), NO un registry Python, TOML, JSON ni Markdown como fuente primaria.

La infraestructura OrangeHouse real es **Docker Compose single-host** (sin VMs, sin self-hosted runners, sin TLS, sin backup, sin secret manager). Para este contexto, **no se recomienda introducir** el stack HashiCorp (Terraform/Vault/Consul/Nomad/Packer/Boundary) ni OPA/Conftest/Semgrep/SonarQube: resolverían problemas que OCM no tiene hoy, y su coste operativo violaría el principio no-sobrediseño y la decisión F2.6d (proceso único suficiente).

El riesgo más relevante del encargo — **¿puede un agente de IA modificar simultáneamente el código y las reglas que lo protegen?** — tiene respuesta **afirmativa** con la configuración actual: guards, tests, CI y tracking viven en el mismo repo/branch y no hay branch protection declarada. La separación de privilegios es el entregable crítico, no la introducción de más scanners.

**Post-scriptum del propietario (2026-08-19) — OCM Constitution:** el propietario aportó la Constitution como arquitectura objetivo (4 pilares → Policy Gate → CI → Artifact SHA/Digest → CD Gate verify/deploy/rollback → OrangeHouse health). La auditoría la integra en FASE 13: los 4 pilares son mayormente EXISTENTES; Semgrep/SonarQube (nuevos en la Constitution) requieren decisión humana con ADR (D-PL-09/D-PL-10, la auditoría los desaconseja por solapamiento); el CD Gate con verify/deploy/rollback y la firma de artefacto son las adiciones genuinamente nuevas (D-PL-11), implementables sin infraestructura nueva.

Hallazgos de esta auditoría: **11** (1 NUEVO, 5 REVALIDADO, 1 CONTRADICCIÓN, 4 RECOMENDACIÓN; severidad: 1 CRITICAL, 1 HIGH, 5 MEDIUM, 4 LOW). El control FAIL ≠ finding NUEVO (regla §H) se aplicó sistemáticamente: pip-audit (F-CI-01), yamllint (F-CI-02), arch-linter (F-ARCH-*), drift ccxt (F-GOV-02) y systemd (tracking) son revalidaciones.

---

## 2. Scope y Metodología

- **Alcance:** viabilidad de una Policy Layer formal para OCM sobre infraestructura self-hosted. Análisis de las 13 fases del encargo. Read-only estricto (protocolo §K): única escritura en `docs/audits/`.
- **Metodología:** orden de descubrimiento §C (AGENTS → AUDIT_PROTOCOL → PLAN → GOVERNANCE → tracking → ADRs → audits → architecture_linter → CI); tooling mecánico primero §Q (M1..M20); taxonomía estricta §G; reconciliación §H.
- **Ejecución mecánica previa al juicio LLM:** los comandos canónicos §R se ejecutaron en el commit auditado; el LLM juzga solo L1..L4.

### 2.1 Governance Baseline

```
REPRODUCIBILIDAD
- commit: a4d82983f629ef933a155ee7863ab5b2d3a56ae9
- branch: main
- fecha: 2026-08-19
- protocolo: AUDIT_PROTOCOL v2.1
- agente/modelo: opencode/deepseek-v4-flash-free
- herramientas: ruff 0.15.10 · mypy 1.19.1 · bandit 1.9.4 · pytest 8.4.2 · yamllint 1.38.0 · pip-audit 2.10.1
- comandos:
    - uv run lint-imports --config architecture_linter/importlinter.toml   (ARCH_CONTRACTS)
    - uv run python scripts/engineering_health_check.py                     (ENGINEERING_HEALTH)
    - uv run python -m architecture_linter --root . --json                  (ARCH_LINTER)
    - uv run pytest tests/architecture_linter/test_golden.py -q --no-cov    (GOLDEN)
    - uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325  (DEPENDENCY_AUDIT)
    - uvx yamllint -c .yamllint .                                           (YAMLLINT)
    - uv run python scripts/audit_validator.py                              (M1..M20)
- golden: PASS (4 tests, deuda gobernada con FAIL/PARTIAL legítimo)
- resultado: validador PASS (16 findings registrados previos + 11 de esta auditoría)
```

---

## 3. FASE 1 — Reconocimiento (estado del repo)

Resultados factuales (EXISTENTE/PROPUESTO/INFERIDO/NO ENCONTRADO):

| Artefacto | Estado | Evidencia |
|---|---|---|
| pyproject.toml | EXISTENTE | deps de calidad en `dependency-groups.dev` (ruff, mypy, import-linter, bandit, pip-audit, pydeps, vulture, pytest); pins comentados; fail_under=40 |
| uv.lock | EXISTENTE | herramientas resueltas: bandit 1.9.4, import-linter 2.6, mypy 1.19.1, pip-audit 2.10.1, ruff 0.15.10 |
| .github/workflows/ (9) | EXISTENTE | ocm-ci.yml (10 jobs fail-fast), codeql.yml, trivy.yml, gitleaks.yml, actionlint.yml, docker-lint.yml, shellcheck.yml, yamllint.yml, ocm-cd.yml (placeholder) |
| scripts/ (8) | EXISTENTE | audit_validator, engineering_health_check, domain_subprocess_guard, app_layer_guard, backtest_app_guard, check_ssot_enums, metrics_report, provision_kafka_topics |
| `check_production_gates.py` | **PROPUESTO (referenciado, NO implementado)** | Plan Maestro §1/§6/§10 lo cita como ejecutable; no existe en filesystem → F-PL-04 |
| tests/architecture/* (13) | EXISTENTE | guards R11..R16, import contracts, kafka contracts, golden, audit_validator, no-vacuo, docker hardening |
| architecture_linter/ | EXISTENTE | 10 reglas ARCH-001..010, config toml, golden con 7 FAIL gobernados |
| docker-compose.yml | EXISTENTE | 621 líneas, 10+ servicios base (single-host) |
| Dockerfile | EXISTENTE | multi-stage python:3.11-slim, USER appuser |
| Makefile/Taskfile | NO ENCONTRADO | — (run.sh es el entrypoint dev) |
| .pre-commit-config.yaml | EXISTENTE | ruff, format, gitleaks, readme-size-guard, import-linter, mypy-shared, ssot-enums, bandit, pytest-pre-push |
| Terraform/Vault/Consul/Nomad/Packer/Boundary | NO ENCONTRADO | 0 matches en todo el repo |
| OPA/Conftest/Semgrep/Sonar | NO ENCONTRADO | 0 matches |
| CodeQL/Trivy | EXISTENTE | codeql.yml (semanal), trivy.yml (semanal) |
| systemd | DOCUMENTADO (sin units) | ADR-0022; `systemd_reinicia_correctamente: NO_VERIFICADO` |
| Kubernetes/Helm | NO ENCONTRADO | solo menciones en comentarios |

---

## 4. FASE 2 — Inventario de OrangeHouse

Infraestructura real (no inferida):

1. **Servicios existentes** (docker-compose.yml): config-guard, redis 7.2, pushgateway, prometheus 2.51.2, alertmanager 0.27.0, grafana 10.4.2, loki 3.0.0, promtail 3.0.0, zookeeper, kafka 7.6.1 (single-broker, replication 1, PLAINTEXT) + kafka-ui (profile monitoring). Microservicios market-data/trading/portfolio = profile `microservices` (trading/portfolio scaffolding no ejecutable — ADR-0024). **PostgreSQL NO existe como servicio.**
2. **Dónde viven:** todos en Docker Compose en la raíz del repo; procesos de app (ocm/live/paper) corren en el host fuera de Docker por defecto.
3. **Cómo se despliegan:** `docker compose up -d`; CD placeholder deshabilitado; deploy manual vía SSH sin script (`deploy_ocm.sh` no existe).
4. **Cómo se configuran:** `.env` (gitignored) + Hydra `config/*.yaml` (capas base→env→exchange→pipeline→CLI→env vars); protocolo `OCM_SECTION__KEY`.
5. **Secretos:** `.env` local (nunca versionado, verificado) + SecretStr Pydantic + fail-fast `${VAR:?}` + `.dockerignore`. **Sin Vault/secret manager** (alertmanager.yml:11 lo bloquea explícitamente hasta que exista).
6. **Cómo se actualizan:** `git pull` + `docker compose up -d --build`; Dependabot para deps. Sin runbook de update/rollback.
7. **Recuperación tras fallo:** `restart: unless-stopped` + healthchecks + config-guard fail-fast + Redis AOF + deadman alert `PipelineDown`. **Sin backup/snapshots/DR documentados.**
8. **Cómo se auditan:** protocolo AUDIT_PROTOCOL + audit_validator (M1..M20) + engineering_health + 9 workflows. **Nota:** auditorías previas NO auditaban infra en ejecución.
9. **Manuales:** `.env` con secretos, `docker compose up`, arranque de streaming (canary manual, sin unit systemd), deploy SSH sin script, provisioning de topics Kafka (script manual), registro de hardware (pendiente).
10. **Reproducibles:** stack compose completo, configs Hydra, provisioning Kafka idempotente, imagen Docker multi-stage, CI, canary F2.6c. **Brechas:** Grafana provisioning/dashboards vacíos+gitignored (F-PL-10), hardware no inventariado, sin playbook de deploy.

**Conclusiones FASE 2:** host único `orangehouse`, single-node, Docker Compose como orquestador de facto, decisión F2.6d documentada (proceso único systemd + Kafka local suficiente; canary: CPU 0.00 %, RAM 40.4 MB). **Terraform no debe administrar "todo"; la infraestructura no escala horizontalmente y no lo requiere.**

---

## 5. FASE 3 — Evaluación HashiCorp (recomendación: no introducir)

| Herramienta | Problema que resolvería | Problema que NO resolvería | Coste/complejidad | Verdicto |
|---|---|---|---|---|
| **Terraform** | Declarar hosts/red/almacenamiento | El problema real (compose single-host + secrets) | Estado remoto, proveedores, learning | **NO** — `docker-compose.yml` ya es IaC declarativo para este alcance; `docker compose config` + Hadolint cubren validación |
| **Vault** | Secret management centralizado | El riesgo real (exposición de `.env`) que ya se mitiga con `.env` gitignored + SecretStr + fail-fast | Seal/unseal, HA, backup de unseal, bootstrap en single-host | **NO** — caso de uso no demostrado: OCM no rota secrets, no hay multi-host, `.env`+SecretStr satisfacen (F-PL-07) |
| **Consul** | Service discovery/config distribuida | Nada — DNS interno de Docker + config Hydra ya resuelven | Un proceso más + gossip | **NO** — recomendado explícitamente no introducir |
| **Nomad** | Scheduler/orquestador adicional | Nada — Docker Compose + systemd satisfacen (F2.6d) | Un proceso más + job specs | **NO** — recomendado explícitamente no introducir |
| **Packer** | Imágenes reproducibles | No hay imágenes de host que construir (solo Docker image, ya reproducible) | Build pipeline | **NO** |
| **Boundary** | Acceso admin seguro | No hay superficie admin remota (single-user local, sin reverse proxy) | Puerta de enlace + credenciales | **NO** |

Regla que debe permanecer en cada capa: **Docker Compose** (infra de servicios) · **systemd** (lifecycle de streaming, ADR-0022) · **shell** (`run.sh`, scripts de provisioning) · **Git + CI** (despliegue/cambio). Terraform solo sería reevaluable si el inventario de OrangeHouse creciera a multi-host (fuera de alcance).

---

## 6. FASE 4 — OCM Policy Layer: viabilidad y formato

**Veredicto: VIABLE — evolución incremental, ALTA prioridad.**

Estado actual (lo que ya es "policy layer"):
- `tracking.yaml` reglas R1..R16: `descripcion / mecanismo / backtest / activada_en_ci` — **YA es un registry embrionario**.
- ADR-0020: `activada_en_ci` es normativo (gate de release); engineering_health lo verifica contra CI.
- ADR-0015: patrón guard AST + tests pos/neg + backtest histórico + job CI.
- `scripts/audit_validator.py`: motor M1..M20 con enums cerrados, reconciliación y golden — infraestructura lista para extender a reglas de policy.

**Fuente de verdad recomendada: YAML.** Justificación:
- Continuidad: `tracking.yaml` ya es el SSOT consumible por máquina; el motor M1..M20 ya parsea YAML y enums.
- TOML: atractivo para tooling Python, pero rompería el SSOT unificado con tracking y no añade valor de schema frente a YAML.
- Python registry: executable = más poder pero rompe el principio "config es datos" (`test_config_dir_is_data_only.py` ya lo exige para config/) y mezcla reglas con código mutable.
- JSON: sin comentarios ni composición; menos legible para revisión humana de reglas.
- Markdown metadata: ya está como informe/registro; no es parseable de forma fiable (el validador ya sufre con el parse).

Esquema propuesto (compatible con tracking.yaml actual):
```yaml
policy:
  id: R11
  name: domain-purity
  description: domain no ejecuta subprocess
  scope: packages/*/domain/
  severity: HIGH
  owner: architecture
  enforcement: AST guard + CI job domain-guard
  tests:
    positive: test_domain_subprocess_guard.py (árbol real limpio)
    negative: anti-patrones import/alias/from/lazy
  evidence: backtest + git_hash + guard output
  ci: ocm-ci.yml job domain-guard
  master_plan: F2.5/§2
  adr: ADR-0006, ADR-0015
  waiver: null
```

---

## 7. FASE 5 — Responsabilidad de cada herramienta

| Herramienta | Debe manejar (exclusivo) | Estado real |
|---|---|---|
| **import-linter** | boundaries de capa, dirección de dependencias, forbidden/independence | EXISTENTE (50 BC, gate CI) |
| **AST Guards** | invariantes estructurales de OCM (R11..R16): subprocess en domain, argparse en use_cases, getattr-default, scaffolding CLI, SILENT_PATHS, CycleRunResult | EXISTENTE (scripts + tests + backtest + jobs CI) |
| **Semgrep** | APIs peligrosas/anti-patrones no estructurales | **NO RECOMENDADO** — Bandit+ruff+CodeQL+AST guards cubren el espacio real; añadirlo no resuelve un hueco |
| **SonarQube** | complexity/duplication/maintainability | **NO RECOMENDADO** — duplica ruff+mypy+pytest+coverage sin valor marginal para single-host (F-PL-08) |
| **CodeQL** | dataflow/taint/source→sink (lo que Semgrep/Bandit verían de forma limitada) | EXISTENTE (codeql.yml semanal, SARIF) |
| **Bandit** | APIs inseguras, secretos, subprocess en app | EXISTENTE (CI + pre-commit, -ll) |
| **mypy/ruff/pytest** | tipos, estilo, comportamiento | EXISTENTE (gates CI) |

**CodeQL sí aporta valor marginal** sobre Semgrep/Bandit para dataflow/taint; ya está integrado. Semgrep y SonarQube no aportan valor marginal suficiente para su coste → no introducir (F-PL-08).

---

## 8. FASE 6 — IaC Policy

Evaluación de OPA/Conftest: **NO NECESARIOS.** Justificación por infraestructura REAL:
- No hay Terraform → no hay plan para policy-test.
- La "IaC" real es `docker-compose.yml` → validación ya cubierta por `docker compose config` (implicito), Hadolint (Dockerfile), `test_docker_hardening.py` (bindings loopback, kafka-ui auth, grafana password, .dockerignore), config-guard init container (fail-fast antes de arrancar).
- Reglas candidatas solo las justificadas por la infra REAL: no-plaintext-secrets (ya: `.env` gitignored + SecretStr + fail-fast), no-puertos-expuestos-innecesarios (ya: grafana/kafka-ui loopback; bindings `0.0.0.0` ya señalados en H-14/B-05), no-almacenamiento-sin-retención (ya: Kafka 168h, Redis AOF).
- Conftest/OPA añadirían un motor Rego para validar YAML de compose que ya se valida con herramientas existentes → sobrediseño.

**Conclusión:** la IaC policy de OCM es "compose + hardening tests + hadolint", y así debe permanecer hasta que exista Terraform (que no se recomienda).

---

## 9. FASE 7 — AI Agent Threat Model

Análisis de vectores de captura del sistema de validación:

| Vector | ¿Posible hoy? | Mitigación propuesta |
|---|---|---|
| Modificar el AST guard | **SÍ** (mismo repo/branch) | Tratar guards como código sagrado: cambio requiere ADR + revisión humana; hash de guards en CI comparado contra registro inmutable |
| Modificar los tests | **SÍ** | Tests pos/neg son parte del guard; backtest contra git history detecta falsos negativos |
| Modificar el Policy Registry | **SÍ** | Registry en YAML revisado por humano; M21+ valida IDs únicos, dead rules, waivers |
| Desactivar CI | SÍ (editar workflow) | Branch protection + `workflow` file review; audit_validator verifica que las reglas activas tienen mecanismo en CI (engineering_health) |
| Bajar severity / waiver permanente | SÍ (editar tracking) | Waiver requiere ADR + fecha de expiración; M-regla valida waivers vigentes |
| Mover código de capa | Parcial (import-linter + AST guards lo detectan) | ya cubierto |
| Wrappers / reflection / dynamic imports | Parcial | AST guards ven lazy imports (R11/R12); reflection/dynamic fuera del alcance actual — documentado en ADR-0015 deuda |
| Modificar Terraform/Vault policies | N/A (no existen) | — |

**Respuesta a la pregunta fundamental:** **SÍ, un agente puede modificar simultáneamente el código y las reglas que lo protegen** con la configuración actual. La separación de privilegios requerida (F-PL-09):
1. Branch protection en `main` (required status checks + PR review).
2. Los guards/policy-registry NO se cambian en PRs de feature — PRs separados con revisión humana obligatoria.
3. Evidencia firmada: hash de guards+config+tracking en un artefacto inmutable (CI artifact / registro), no solo git_hash inyectado (B-20).
4. Auditoría de integridad: verificación periódica de que los guards y el registry no difieren de un snapshot firmado.

---

## 10. FASE 8 — CI

Diseño óptimo del pipeline (basado en lo REAL, sin ejecutar herramientas caras innecesariamente):

| Etapa | Jobs | Coste | Existente hoy |
|---|---|---|---|
| **PR-fast** | ruff, format, mypy, import-linter, ssot-enums, actionlint, shellcheck, gitleaks, yamllint | bajo (paralelo) | ✔ (jobs en ocm-ci + workflows satélite) |
| **PR-required** | tests unit (fail_under 40), AST guards (R11..R16 + backtest), config-validation, engineering-health, bandit | medio | ✔ |
| **PR-expensive** | integration (Kafka service container), CodeQL, Trivy | alto | ✔ (integration en ocm-ci; CodeQL/Trivy **semanal** — correcto: no correr en cada PR) |
| **nightly** | CodeQL+Trivy completos, dependency-audit extendido, compliance-report, evidence-generación | alto | ✖ (no existe — recomendar agregar) |
| **release** | Production Gate binario (F-PL-04) + compliance report firmado | medio | ✖ (check_production_gates.py ausente) |

**Reglas de paralelización:** `architecture` + `engineering-health` como raíz fail-fast (ya), los demás dependientes en paralelo (ya). CodeQL/Trivy no deben moverse a cada PR (coste sin valor). La adición propuesta es el **nightly** con generación de compliance report y evidencia, y el **release** con el Production Gate binario.

---

## 11. FASE 9 — Policy Registry

**Viabilidad: ALTA.** La relación R11 → description → implementation → guard → tests → CI → evidence → Master Plan → ADR ya existe en tracking.yaml para las 16 reglas; falta formalizar el esquema y los anti-drift:

| Riesgo de drift | Mecanismo propuesto (extensión M1..M20) |
|---|---|
| IDs duplicados | M1 (ya existe) |
| Reglas sin tests | Nueva regla M21: `tests.positive` y `tests.negative` obligatorios |
| Reglas sin enforcement | M-delta: `enforcement` obligatorio + verificación contra CI (engineering_health ya) |
| Reglas sin evidence | M-delta: `evidence` no vacía (M7 patrón) |
| Dead rules | M-delta: regla sin implementación referenciada → warning/FAIL |
| ADRs huérfanos | M9 extendido: ADR referenciado sin rule → warning |
| Waivers expirados | M-delta: waiver con `expires` en el pasado → FAIL |
| Policy drift (registry ≠ código) | hash del registry en artifact CI + verificación nocturna |

**No introducir un registry Python**: rompe `test_config_dir_is_data_only.py` y el principio "reglas = datos revisables". YAML (extensión del bloque `reglas:` de tracking.yaml o módulo `policies/*.yaml`) es la opción recomendada.

---

## 12. FASE 10 — Caso piloto R11 (Domain Purity)

Análisis de dónde debe implementarse R11 exactamente:

```
R11 — Domain Purity
 ├── import-linter   → BC-09 / capas (domain no importa infra de datos)
 ├── AST Guard       → scripts/domain_subprocess_guard.py (R11, backtest ok, CI true)
 ├── CI              → job domain-guard (ocm-ci.yml:133)
 ├── ADR             → ADR-0006 (dominio no depende de infraestructura)
 ├── Master Plan     → F2.5, §2 (B-20 inyección de git_hash)
 └── Registry        → tracking.yaml R11 (ya completo)
```

**Conclusión:** R11 está correctamente implementado con **import-linter + AST Guard + CI** — y nada más. No requiere Semgrep, ni CodeQL, ni policy engine. Es exactamente el patrón "un piloto que NO se fuerza a todas las herramientas". El único delta de la Policy Layer es declarar R11 en el registry con el esquema completo de §6 y añadir su hash de evidencia al registro inmutable.

Datos R11 verificados: `tracking.yaml` R11 → `subprocess` en domain, mecanismo AST guard tipo BC-09, backtest ok, activada_en_ci true. Evidencia: `tests/architecture/test_domain_subprocess_guard.py` (8 tests pos/neg).

---

## 13. FASE 11 — Modelo de Compliance

Evaluación de `OCM Policy Compliance Report`:

- **Tiene valor real SI es generado desde evidencia mecánica** (re-ejecución de gates + registry + hash), no si es un documento estático escrito a mano.
- El precedente ya existe: `engineering_health_check.py` → PASS/FAIL binario con evidencia por cheque; `audit_validator.py` → PASS con reconciliación matemática.
- El report propuesto es una **proyección** de los mismos motores: iterar el registry y ejecutar cada gate, agregando PASS/FAIL por categoría (Architecture/Security/Infrastructure/Quality/Traceability).
- **Valor real:** 1) bloquea merges con una vista única; 2) da la línea de defensa contra agentes que solo miran "tests pasan"; 3) responde la pregunta de evidencia (§14). **Riesgo cosmético:** si el report no se deriva de los gates reales, es documentación decorativa → debe ser un *artifact generado* en CI (nightly), nunca un .md mantenido a mano.

Recomendación: generar el compliance report como derivado automático del Policy Registry + gates CI (nightly), con firma de evidencia.

---

## 14. FASE 12 — Evidence

Modelo de almacenamiento de evidencia:

| Pregunta | Respuesta propuesta |
|---|---|
| ¿Por qué sabemos que OCM cumplía R11 en el commit X? | Guard AST ejecutado en CI del commit X + artefacto (JSON) + hash del árbol en el registro — R11 ya tiene backtest ok + activada_en_ci |
| ¿Qué reglas estaban vigentes en el commit X? | Snapshot del registry (tracking.yaml) en el commit X — git ya lo garantiza |

Mecanismos:
- **CI artifacts:** JSON de cada gate (ya: `metrics_report.py` → metrics.json no commiteado, artifact CI).
- **SARIF:** CodeQL/Trivy ya suben SARIF al Security tab.
- **Registro de findings:** `OCM_AUDIT_FINDINGS_*.md` validado por M1..M20 (ya).
- **Policy results:** salida del registry + gates (nuevo).
- **Audit reports:** `docs/audits/` (ya, inmutables por convención).
- **Firma:** hash de guards+config+tracking en artifact inmutable + verificación nocturna (nuevo, F-PL-09).

---

## 15. FASE 13 — Arquitectura objetivo

### 15.1 OCM Constitution (visión del propietario — 2026-08-19)

El propietario propone la **OCM Constitution** como marco objetivo completo, incluyendo pila de CD (verify/deploy/rollback) y firma de artefactos que la auditoría no había considerado en el diagrama inicial:

```
OCM CONSTITUTION
                                  │
          ┌───────────────────────┼────────────────────────┐
          │                       │                        │
    ARCHITECTURE               SECURITY              SUPPLY CHAIN
          │                       │                        │
    import-linter             Gitleaks                   uv.lock
    AST Guards                Bandit                     pip-audit
    cycle detection           Semgrep                    Trivy
    architecture tests        CodeQL                     Dependabot
          │                       │                        │
          └───────────────────────┼────────────────────────┘
                                  │
                               QUALITY
                                  │
             ┌────────────────────┼────────────────────┐
             │                    │                    │
           Ruff                 mypy               SonarQube
             │                    │                    │
          lint/style          type safety       bugs/code smells
          formatting          contracts         duplication
                                                  complexity
                                                  maintainability
                                                  coverage
                                                        │
                                                        │
                                                     pytest
                                                        │
                                                     tests
                                  │
                                  ▼
                            POLICY GATE
                                  │
                                  ▼
                                CI
                                  │
                         ┌────────┴────────┐
                         │                 │
                       BLOCK              PASS
                                           │
                                           ▼
                                   ARTIFACT BUILD
                                           │
                                     SHA / DIGEST
                                           │
                                           ▼
                                      CD GATE
                                           │
                              ┌────────────┼────────────┐
                              │            │            │
                           verify       deploy       rollback
                              │            │            │
                              └────────────┼────────────┘
                                           │
                                           ▼
                                      OrangeHouse
                                           │
                                     health checks
                                           │
                                  ┌────────┴────────┐
                                  │                 │
                               HEALTHY          UNHEALTHY
                                  │                 │
                               ACCEPT           ROLLBACK
```

**Análisis de viabilidad de la Constitution (read-only):**

| Componente | Estado real | Veredicto |
|---|---|---|
| import-linter | EXISTENTE (50 BC, gate CI) | ✔ |
| AST Guards / architecture tests / cycle detection | EXISTENTE (R11..R16, ARCH-001..010, golden) | ✔ |
| Gitleaks | EXISTENTE (workflow + pre-commit) | ✔ |
| Bandit | EXISTENTE (CI + pre-commit, -ll) | ✔ |
| **Semgrep** | NO ENCONTRADO | **Reevaluar** — la Constitution lo coloca en SECURITY; la auditoría FASE 5 recomendó no introducirlo por solapamiento con Bandit+CodeQL+AST. Si el propietario decide adoptarlo, debe justificarse con un patrón de seguridad real no cubierto y registrarse como ADR (no es una obligación de esta auditoría) |
| CodeQL | EXISTENTE (semanal, SARIF) | ✔ |
| uv.lock | EXISTENTE | ✔ (SSOT de deps) |
| pip-audit | EXISTENTE (gate CI) | **FAIL** (F-PL-02) — necesario resolver |
| Trivy | EXISTENTE (semanal, SARIF) | ✔ |
| Dependabot | EXISTENTE (semanal) | ✔ |
| Ruff / mypy | EXISTENTE (gates CI) | ✔ |
| **SonarQube** | NO ENCONTRADO | **Reevaluar** — la Constitution lo coloca en QUALITY; la auditoría FASE 5 lo desaconsejó (duplica ruff+mypy+pytest+coverage). Mismo criterio: decisión del propietario con ADR, no obligación de esta auditoría |
| pytest / tests | EXISTENTE (fail_under 40, jobs CI) | ✔ |
| **POLICY GATE** | Embrionario (tracking.yaml + health check + audit_validator) | ✔ evolución incremental (F-PL-06) |
| CI | EXISTENTE (10 jobs + 8 workflows satélite) | ✔ |
| **ARTIFACT BUILD + SHA/DIGEST** | NO EXISTE | **Nuevo** — no hay firma de artefacto (F-PL-09: evidencia firmada); el build Docker es reproducible pero no se firma el digest |
| **CD GATE (verify/deploy/rollback)** | Placeholder (ocm-cd.yml deshabilitado, `deploy_ocm.sh` no existe) | **Nuevo** — el CD es manual vía SSH; verify/deploy/rollback requieren implementación + `deploy_ocm.sh` + runbook |
| OrangeHouse health checks | Parcial (healthchecks Docker + deadman alert `PipelineDown` + config-guard) | ✔ parcial — sin sistema formal de HEALTHY/UNHEALTHY/ACCEPT/ROLLBACK |

**Conclusiones Constitution:**
1. **Los 4 pilares (Architecture/Security/Supply Chain/Quality) son viables y mayormente EXISTENTES.** La Constitution formaliza lo que ya está en CI, añadiendo Semgrep y SonarQube como componentes del propietario.
2. **Semgrep y SonarQube contradicen la recomendación F-PL-08** (no introducir por solapamiento). Al ser visión del propietario, se registran como DECISIÓN HUMANA pendiente (D-PL-09/D-PL-10): adoptarlos requiere ADR con justificación del patrón real que cubren, respetando la cadena de adopción (§B).
3. **El CD Gate (verify/deploy/rollback) y la firma de artefacto son las adiciones genuinamente nuevas** de la Constitution. Son coherentes con F-PL-09 (separación de privilegios) y F-PL-04 (gate de release), y requieren: `deploy_ocm.sh`, runbook, digest firmado, health checks formales. No dependen de infraestructura nueva (Docker Compose single-host es suficiente).
4. La Constitution **no requiere** Terraform/Vault/Consul/Nomad (F-PL-07 se mantiene): verify/deploy/rollback se implementan con shell + systemd + Docker Compose en OrangeHouse.

### 15.2 Diagrama objetivo consolidado (auditoría + Constitution)

Evolución (no reescritura), integrando la Constitution:

```
CODE ──► import-linter (BC-NN) ──► AST Guards (R11..R16) ──► CodeQL/Trivy (semanal, SARIF)
INFRA ──► docker-compose config + Hadolint + test_docker_hardening (sin OPA/Conftest/Terraform)
                 │
                 ▼
        OCM Policy Registry (YAML — extensión de tracking.yaml)
                 │
                 ▼
   engineering_health_check (F2.0) + check_production_gates (binario, a implementar)
                 │
                 ▼
              OCM Compliance Gate (CI) = POLICY GATE
                 │
                 ▼
              PASS / BLOCKED
                 │
                 ▼ (PASS)
           ARTIFACT BUILD → SHA/DIGEST firmado        ← nuevo (F-PL-09)
                 │
                 ▼
              CD GATE: verify → deploy → rollback     ← nuevo (placeholder hoy)
                 │
                 ▼
              OrangeHouse → health checks → ACCEPT / ROLLBACK
```

Principios rectores de la arquitectura objetivo:
1. **SSOT YAML** para reglas (registry), motor M1..M20+ para validación mecánica.
2. **Sin infraestructura nueva** en OrangeHouse (no HashiCorp, no OPA/Conftest/Terraform; Semgrep/SonarQube solo si decisión humana con ADR — D-PL-09/D-PL-10).
3. **Separación de privilegios** para agentes (branch protection + guards sagrados + evidencia firmada).
4. **Compliance report como artifact generado** (nunca a mano).
5. **R11 como piloto** ya completo con import-linter + AST Guard + CI.
6. `check_production_gates.py` implementado como primer gate de evidencia del registry (cierra F-PL-04).
7. **CD Gate con verify/deploy/rollback** como evolución del CD placeholder (ocm-cd.yml), con `deploy_ocm.sh` + runbook + digest firmado + health checks formales (Constitution).

---

## 16. Matriz de Findings

| Finding | Severidad | Clasificación |
|---|---|---|
| F-PL-01 | HIGH | REVALIDADO |
| F-PL-02 | CRITICAL | REVALIDADO |
| F-PL-03 | LOW | REVALIDADO |
| F-PL-04 | MEDIUM | CONTRADICCIÓN |
| F-PL-05 | LOW | REVALIDADO |
| F-PL-06 | MEDIUM | RECOMENDACIÓN |
| F-PL-07 | LOW | RECOMENDACIÓN |
| F-PL-08 | LOW | RECOMENDACIÓN |
| F-PL-09 | MEDIUM | RECOMENDACIÓN |
| F-PL-10 | MEDIUM | NUEVO |
| F-PL-11 | LOW | REVALIDADO |

Reconciliación: 11 findings = 1 NUEVO + 5 REVALIDADO + 1 CONTRADICCIÓN + 4 RECOMENDACIÓN = 1 CRITICAL + 1 HIGH + 5 MEDIUM + 4 LOW. ✓

## 17. Matriz de Controles

| Control | Comando canónico | Exit esperado | Resultado real |
|---|---|---|---|
| ARCH_CONTRACTS | `uv run lint-imports --config architecture_linter/importlinter.toml` | 0 | **PASS** (50 kept, 0 broken) |
| ENGINEERING_HEALTH | `uv run python scripts/engineering_health_check.py` | 0 | **PASS** |
| ARCH_LINTER | `uv run python -m architecture_linter --root . --json` | 1 si FAIL/PARTIAL | **FAIL gobernado** (7/10, golden) → F-PL-01 |
| GOLDEN | `uv run pytest tests/architecture_linter/test_golden.py -q --no-cov` | 0 | **PASS** (4) |
| DEPENDENCY_AUDIT | `uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325` | 0 | **FAIL** (4 vulns) → F-PL-02 |
| YAMLLINT | `uvx yamllint -c .yamllint .` | 0 | **FAIL** (alerts.yml) → F-PL-03 |
| AUDIT_VALIDATOR | `uv run python scripts/audit_validator.py` | 0 | **PASS** (M1..M20, 16 previos + warnings) |

Total de controles: 7. PASS 4, FAIL gobernado/revalidado 3. Ningún FAIL genera finding NUEVO sin dedup (§H).

## 18. Matriz de Decisiones

| D-ID | Pregunta | Opciones | Consecuencias | Finding |
|---|---|---|---|---|
| D-PL-01 | Mitigar 4 vulns de deps | (a) bump aiohttp≥3.14.3 + cryptography≥50.0.0 (validar staging) · (b) risk-accept formal (ADR+tracking) | (a) cierra CI, riesgo de regresión ccxt/python-jose · (b) deja gate rojo gobernado | F-PL-02 |
| D-PL-02 | `check_production_gates.py` ausente | (a) implementar binario G1..G11 · (b) actualizar Plan para declarar health-check como mecanismo | (a) da el veredicto binario que el Plan promete · (b) alinea docs pero pierde el gate de release explícito | F-PL-04 |
| D-PL-03 | Fuente de verdad del Policy Registry | (a) YAML (extensión tracking) · (b) TOML · (c) Python · (d) JSON · (e) Markdown | (a) continuidad con M1..M20 y SSOT unificado — RECOMENDADA | F-PL-06 |
| D-PL-04 | Introducir stack HashiCorp | (a) no introducir · (b) introducir Vault/Terraform | (a) mantiene single-host simple (F2.6d) · (b) coste operativo sin caso de uso real | F-PL-07 |
| D-PL-05 | Introducir Semgrep/Sonar/OPA/Conftest | (a) no introducir · (b) añadir | (a) sin valor marginal sobre AST guards+CodeQL+Bandit · (b) superficie y cómputo extra | F-PL-08 |
| D-PL-06 | Separación de privilegios para agentes | (a) branch protection + guards sagrados + evidencia firmada · (b) mantener estado actual | (a) mitiga la captura del sistema de validación · (b) riesgo afirmativo (FASE 7) | F-PL-09 |
| D-PL-07 | Grafana no reproducible | (a) versionar provisioning/dashboards · (b) eliminar montajes vacíos | (a) observabilidad reproducible · (b) declara setup manual | F-PL-10 |
| D-PL-08 | Unit systemd de streaming | (a) crear unit + verificar reinicio · (b) declarar Docker restart como modelo | (a) cumple ADR-0022 · (b) alternativa documentada | F-PL-11 |
| D-PL-09 | Adoptar Semgrep en SECURITY (Constitution) | (a) no adoptar (recomendación auditoría F-PL-08) · (b) adoptar con ADR y patrón real justificado | (a) sin superficie nueva · (b) cubre un hueco de seguridad real si existe — requiere cadena de adopción (§B) | F-PL-08 |
| D-PL-10 | Adoptar SonarQube en QUALITY (Constitution) | (a) no adoptar (duplica ruff+mypy+pytest+coverage) · (b) adoptar con ADR | (a) sin cómputo extra · (b) agrega maintainability si se decide — requiere ADR | F-PL-08 |
| D-PL-11 | CD Gate verify/deploy/rollback (Constitution) | (a) implementar `deploy_ocm.sh` + digest firmado + runbook + health checks · (b) mantener CD placeholder | (a) completa la Constitution y cierra F-PL-04/F-PL-09 · (b) deploy manual vía SSH sin rollback automatizado | F-PL-04, F-PL-09 |

## 19. Integridad

- Read-only estricto (protocolo §K): no se modificó código, tests, CI, ADRs, tracking ni dependencias. Única escritura: `docs/audits/` (este informe + registro de findings).
- Orden de descubrimiento §C cumplido.
- Contraste de cada FAIL contra tracking.yaml y ADRs (§H): los 5 controles FAIL son revalidaciones de deuda conocida (F-CI-01, F-CI-02, F-ARCH-*, F-GOV-02, systemd).
- Contadores reconciliados matemáticamente (§16-17).
- No se inventó infraestructura: clasificación EXISTENTE/PROPUESTO/INFERIDO/NO ENCONTRADO aplicada en FASE 1-2.
- Veredicto global: **VIABLE** — la Policy Layer es evolución incremental; el entregable crítico es la separación de privilegios (F-PL-09), no más tooling.

## 20. Roadmap propuesto (fuera del alcance read-only; requiere decisión humana)

1. Aprobar decisiones D-PL-01..11.
2. Implementar `check_production_gates.py` (F-PL-04) como primer gate de evidencia.
3. Formalizar el Policy Registry YAML (extensión de tracking.yaml) + reglas M21+ en audit_validator.
4. Versionar Grafana provisioning (F-PL-10) y resolver systemd (F-PL-11).
5. Aplicar separación de privilegios (branch protection + guards sagrados + evidencia firmada).
6. Nightly job: compliance report + evidence artifacts.
7. Decidir Semgrep/SonarQube (D-PL-09/D-PL-10) según la Constitution — con ADR si se adoptan.
8. Implementar CD Gate de la Constitution (D-PL-11): `deploy_ocm.sh` + digest firmado + runbook verify/deploy/rollback + health checks formales (evolución de ocm-cd.yml placeholder).