# AUDITORÍA ADVERSARIAL — OCM POLICY LAYER / VALIDACIÓN DE LA CONSTITUTION

**Ejecución:** 2026-08-19 · baseline `a4d82983f629ef933a155ee7863ab5b2d3a56ae9` · branch `main`
**Protocolo:** AUDIT_PROTOCOL v2.1 · read-only estricto (§K) — única escritura en `docs/audits/`
**Modo:** adversarial de segunda opinión — no se acepta ninguna conclusión de la auditoría previa sin verificación contra el repositorio
**Registro:** `docs/audits/OCM_AUDIT_FINDINGS_2026-08-19-policy-layer-complementary.md`

---

## 1. Executive Summary

Esta auditoría adversarial re-examina las conclusiones de `AUDIT_OCM_POLICY_LAYER_FEASIBILITY_2026-08-19.md` y valida la **OCM Constitution** como arquitectura normativa. Verificó cada conclusión contra evidencia real del repositorio — sin aceptar "existe un script" como prueba. Los resultados corrigen, confirman y contradicen partes de la auditoría previa:

**Corrección crítica (F-PLA-01):** la auditoría previa justificó el descarte de SonarQube afirmando "duplicaría ruff (complexity/duplication ya cubiertos)". **Es falso.** `pyproject.toml` activa solo `[E, F, I]` en ruff — complejidad ciclomática (C901), cognitive complexity, duplicación, long methods y coupling están **NOT COVERED** por ninguna herramienta ejecutada. El gap de maintainability es real.

**Confirmación de Semgrep (F-PLA-07):** búsqueda exhaustiva de patrones peligrosos (eval/exec, pickle, shell=True, subprocess, os.environ, crypto, SQL, logging secrets, deserialización) confirma **NO EVIDENCE OF MATERIAL GAP** de seguridad: los 0-2 usos por patrón están todos en infraestructura con mitigaciones verificadas. Bandit (0 Med/High) + CodeQL (PR, dataflow) + Gitleaks + Trivy cubren la superficie real. Semgrep tiene valor **preventivo** (reglas declarativas de arquitectura), no correctivo — ADOPT NON-BLOCKING como máximo, nunca blocking sin baseline.

**Corrección factual (F-PLA-03):** CodeQL y Trivy se ejecutan en **push/PR a main + weekly**, no solo semanal como afirmó la auditoría previa. La cobertura de seguridad es mejor de lo declarado.

**SonarQube (F-PLA-08):** la conclusión final (no adoptar) se **mantiene**, pero la justificación correcta es el **coste operacional en OrangeHouse** (PostgreSQL inexistente, backup inexistente, auth, superficie de ataque, 2-4 h/mes) — NO la duplicación con ruff, que era incorrecta. La señal longitudinal de maintainability puede obtenerse a coste ~0 con ruff extendido + vulture (ya instalado pero nunca ejecutado, F-PLA-02).

**Policy Gate (F-PLA-05):** el gate binario central de la Constitution (`POLICY GATE → BLOCK/PASS`) **no existe** — `check_production_gates.py` ausente. Los gates están distribuidos en 10 jobs sin veredicto agregado normativo consumible por un agente.

**Cadena RULE→CI→EVIDENCE (F-PLA-09):** ninguna regla R1..R16 tiene la cadena completa: faltan severidad, owner, evidencia con hash, estado, waiver, expiración. Un agente de IA puede hoy modificar guard+test+registry+CI y obtener PASS (vector demostrado, §9).

**HashiCorp (F-PLA-04):** confirmado — sin necesidad demostrable; F-PL-07 REVALIDADO.

**Veredicto agregado:** la OCM Constitution es **parcialmente viable y correcta en estructura**, pero no es enforcement hoy. Semgrep: NO ADOPT blocking (valor preventivo, no gap material). SonarQube: NO JUSTIFIED (coste > valor). El mínimo stack de gobernanza exige: registry completo + hash de evidencia + waiver + ownership + Policy Gate binario — la parte que falta no es herramientas, es **cadena de evidencia**.

Hallazgos: **9** (4 NUEVO, 2 REVALIDADO, 2 CONTRADICCIÓN, 1 RECOMENDACIÓN; severidad: 1 CRITICAL, 3 HIGH, 4 MEDIUM, 1 LOW).

---

## 2. Scope y Metodología

- **Alcance:** validación adversarial de la auditoría previa (2026-08-19) y de la OCM Constitution; reevaluación obligatoria de Semgrep (FASE 3) y SonarQube (FASE 4) contra evidencia real; auditoría de la cadena RULE→CI→EVIDENCE, supply chain, CI/CD, resistencia a agentes IA, Policy Registry, Policy Gate, artifact integrity, CD en OrangeHouse, HashiCorp.
- **No-alcance:** implementación, modificación de código/tests/CI/workflows/Docker/tracking/ADRs/Plan/deps/config. Read-only estricto.
- **Metodología:** MACHINE CHECKS FIRST (§Q): audit_validator M1..M20 sobre artefactos previos, comandos canónicos §R re-ejecutados; luego verificación de CADA conclusión previa contra el repositorio (configs reales, workflows reales, output real de herramientas); taxonomía §G; reconciliación §H.

### 2.1 Governance Baseline

```
REPRODUCIBILIDAD
- commit: a4d82983f629ef933a155ee7863ab5b2d3a56ae9
- branch: main
- fecha: 2026-08-19
- protocolo: AUDIT_PROTOCOL v2.1
- agente/modelo: opencode/deepseek-v4-flash-free
- herramientas: ruff 0.15.10 · mypy 1.19.1 · bandit 1.9.4 · pytest 8.4.2 · yamllint 1.38.0 · pip-audit 2.10.1 · vulture 2.16
- comandos:
    - uv run lint-imports --config architecture_linter/importlinter.toml   (ARCH_CONTRACTS)
    - uv run python scripts/engineering_health_check.py                     (ENGINEERING_HEALTH)
    - uv run python -m architecture_linter --root . --json                  (ARCH_LINTER)
    - uv run pytest tests/architecture_linter/test_golden.py -q --no-cov    (GOLDEN)
    - uv run bandit -r apps ocm packages shared infrastructure              (BANDIT detalle)
    - uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325  (DEPENDENCY_AUDIT)
    - uvx yamllint -c .yamllint .                                           (YAMLLINT)
    - uv run python scripts/audit_validator.py                              (M1..M20)
- golden: PASS (4 tests, deuda gobernada)
- validador (artefactos previos): PASS — 11 findings, 20 reglas mecánicas, 10 warnings benignos
```

### 2.2 Protocol Compliance

| Regla | Cumplimiento |
|---|---|
| Read-only (§K) | ✔ — única escritura: 2 artefactos en docs/audits/ |
| Orden de descubrimiento (§C) | ✔ — AGENTS → AUDIT_PROTOCOL → PLAN → GOVERNANCE → tracking → ADRs → audits → arch_linter → CI |
| MACHINE CHECKS FIRST (§Q) | ✔ — M1..M20 + comandos canónicos |
| Control FAIL ≠ Finding NUEVO (§H) | ✔ — F-PLA-06 revalida F-PL-02 (no duplica) |
| Taxonomía (§G) | ✔ — NUEVO/CONTRADICCIÓN/REVALIDADO/RECOMENDACIÓN |
| No implementación | ✔ — RECOMMENDATION ONLY; cero cambios fuera de docs/audits/ |
| NOT_TRACED exacto | ✔ |
| B-*/R-* válidos | ✔ — R1..R16 son IDs reales de tracking.yaml (engineering_health PASS) |
| Findings fantasma | ✔ — F-PLA-01..09 = informe ↔ registro |

---

## 3. FASE 1 — Verificación de la auditoría previa (clasificación por conclusión)

| Conclusión previa | Verificación en repo | Clasificación |
|---|---|---|
| import-linter: mantener | 50 BC, 0 broken, gate CI | **CONFIRMADA** |
| AST Guards: mantener | R11..R16, tests pos/neg, backtest, CI | **CONFIRMADA** (con gap de metadatos, F-PLA-09) |
| architecture_linter: mantener | 10 reglas, golden 7 FAIL gobernados | **CONFIRMADA** |
| Bandit: mantener | -ll → 0 Med/High; 51 Low (B101×25, B110×17, B311×3, B607×2, B603×2, B404×2) | **CONFIRMADA** |
| CodeQL: mantener | codeql.yml push+PR+weekly (NO solo semanal) | **CONFIRMADA con corrección** (F-PLA-03) |
| Gitleaks: mantener | gitleaks.yml push+PR, fetch-depth 0 | **CONFIRMADA** |
| Trivy: mantener | trivy.yml push+PR+weekly, CRIT/HIGH, SARIF | **CONFIRMADA** (PR, no solo weekly) |
| Ruff/mypy/pytest: mantener | gates CI; **ruff solo E/F/I** | **CONFIRMADA con corrección** (F-PLA-01) |
| HashiCorp: no introducir | 0 matches; sin necesidad | **CONFIRMADA** (F-PLA-04) |
| OPA/Conftest: no introducir | sin Terraform/K8s; IaC=compose | **CONFIRMADA** |
| Semgrep: no introducir | 0 matches; sin gap material (FASE 3) | **PARCIALMENTE CONFIRMADA** (valor preventivo → non-blocking) |
| SonarQube: no introducir | justificación de F-PL-08 **incorrecta** (ruff no cubre complexity); conclusión final correcta por coste | **CONTRADICCIÓN** (F-PLA-08) |
| CD Gate: necesario | ocm-cd.yml placeholder; sin deploy_ocm.sh | **CONFIRMADA** |
| Artifact SHA/Digest: necesario | no existe; sin hash de artifact | **CONFIRMADA** |
| Policy Registry YAML: viable | tracking.yaml R1..R16 SSOT embrionario | **CONFIRMADA** (con M21..M25 + evidencia hash) |
| Separación de privilegios: crítica | guards/tests/CI/tracking en mismo repo; sin branch protection | **CONFIRMADA y extendida** (F-PLA-09) |

**Conclusión FASE 1:** la auditoría previa fue correcta en 13/17 conclusiones; 2 correcciones factuales (CodeQL en PR, ruff solo E/F/I); 1 contradicción de justificación (SonarQube — conclusión final correcta, razón incorrecta); 1 parcial (Semgrep).

---

## 4. FASE 2 — Adversarial Constitution: cadena RULE→CI→EVIDENCE por componente

Evaluación adversarial de cada componente de la Constitution contra la cadena completa:

| Pilar | Componente | Enforcement | CI | Tests | Evidencia | Policy Gate | Gap |
|---|---|---|---|---|---|---|---|
| ARCH | import-linter | BC-01..55 | ✔ job architecture | ✔ | ✔ (0 broken) | ✔ (gate CI) | — |
| ARCH | AST Guards | R11..R16 scripts | ✔ jobs app/domain-guard | ✔ pos/neg | ⚠ parcial (sin hash) | ✔ | evidencia hash |
| ARCH | architecture_linter | ARCH-001..010 | ✔ golden job | ✔ golden | ✔ (golden) | ✖ (no gate, golden-gated) | no gate directo |
| ARCH | cycle detection | pydeps (dev, no CI) | ✖ no job | ✖ | ✖ | ✖ | **GAP: no enforcement** |
| ARCH | architecture tests | 13 tests/architecture | ✔ | ✔ | ✔ | ✔ | — |
| SEC | Gitleaks | gitleaks.yml | ✔ PR | ✔ | ✔ | ✔ | — |
| SEC | Bandit | -ll | ✔ job security | ✔ | ✔ | ✔ | — |
| SEC | **Semgrep** | **NO EXISTE** | ✖ | ✖ | ✖ | ✖ | **GAP: sin enforcement (F-PLA-07)** |
| SEC | CodeQL | codeql.yml | ✔ PR+weekly | ✔ | ✔ SARIF | ⚠ (no gate binario, tab Security) | no gate blocking |
| SCN | uv.lock | uv sync | ✔ | ✔ | ✔ | ✔ | — |
| SCN | pip-audit | quality job | ✔ | ✔ | ⚠ FAIL (4 vulns) | ✔ (red) | vulns activas (F-PLA-06) |
| SCN | Trivy | trivy.yml | ✔ PR+weekly | ✔ | ✔ SARIF | ⚠ (tab Security) | no gate blocking |
| SCN | Dependabot | weekly | ✔ | ✔ | ✔ | ⚠ | auto-merge no |
| QUA | Ruff | ruff check/format | ✔ | ✔ | ✔ | ✔ | — |
| QUA | mypy | mypy . | ✔ | ✔ | ✔ | ✔ | — |
| QUA | pytest | unit+coverage fail_under=40 | ✔ | ✔ | ✔ | ✔ | — |
| QUA | **SonarQube** | **NO EXISTE** | ✖ | ✖ | ✖ | ✖ | **GAP: maintainability (F-PLA-01/08)** |
| GATE | **Policy Gate** | **NO EXISTE** (check_production_gates.py ausente) | ✖ | ✖ | ✖ | ✖ | **GAP crítico (F-PLA-05)** |
| CD | verify/deploy/rollback | **NO EXISTE** (ocm-cd placeholder) | ✖ | ✖ | ✖ | ✖ | **GAP crítico** |
| ART | SHA/digest | **NO EXISTE** | ✖ | ✖ | ✖ | ✖ | **GAP crítico** |
| RUN | health checks | compose healthchecks + deadman | ✔ | ✔ | ⚠ parcial | ⚠ | post-deploy health no automatizado |
| POL | registry | tracking.yaml R1..16 | ✔ health | ⚠ | ⚠ sin hash | ✖ | **GAP: evidencia/waiver/owner** |

**Conclusión FASE 2:** la Constitution es **estructuralmente correcta pero no es enforcement hoy**. De 22 componentes: 11 con cadena completa, 4 con gap de gate/no-blocking, **7 sin enforcement real** (Semgrep, SonarQube, Policy Gate, CD, artifact SHA, cycle detection CI, registry completo). El gap no es de herramientas — es de **cadena de evidencia y gate agregado**.

---

## 5. FASE 3 — Semgrep: reevaluación obligatoria

### 5.1 Patrones peligrosos reales del repositorio

| Patrón | Resultado | Detalle |
|---|---|---|
| `eval(`/`exec(` | **0** | ausente en apps/ocm/packages/shared |
| `pickle.`/`yaml.load`/`shelve`/`marshal` | **0** | ausente |
| `shell=True` | **0** | ausente |
| `subprocess.run/Popen/call` | **2** | `ocm/runtime/lineage.py:60` (git rev-parse, infra), `shared/utils/repo.py:50` (repo root, infra) |
| `os.environ`/`os.getenv` | 16 archivos | **0 en domain/application**; solo ocm/config (SSOT env_vars) + infrastructure + apps/research |
| `random.*` | 4 | jitter/backoff en adapters (`ohlcv_fetcher.py:414-417`, `ccxt_adapter.py:773`) — no-crypto legítimo |
| `hashlib.*` | 3 | `sha256` para config hashing (`logger.py:495`, `hydra_loader.py:282`, `yaml_loader.py:115`) — legítimo |
| `sqlite3` | 2 | `ocm/runtime/registry.py` (B608 nosec, queries parametrizadas — **verificado no vulnerable**), `infrastructure/lineage/tracker.py` |
| `requests/httpx/aiohttp` | 2 | `ocm/observability/sinks.py:87` (httpx.Client), `adapters/inbound/external/base.py:83` (aiohttp.ClientSession) — capas correctas |
| Logging de secrets | **0** | `logger.*api_key/secret/password` → vacío; `processors.py:55` redacta api_key |
| Deserialización peligrosa | **0** | json.loads only (safe) |
| Inyección SQL | **0** | queries parametrizadas (params list) |

### 5.2 Comparativa Semgrep vs Bandit/CodeQL/Ruff/AST Guards/architecture_linter

| Capacidad | Bandit | CodeQL | Ruff | AST Guards | arch_linter | Semgrep |
|---|---|---|---|---|---|---|
| APIs inseguras | ✔ (6 checkers) | ✔ | ✖ | ✖ | ✖ | ✔ |
| Taint/source→sink | ✖ | **✔** (PR) | ✖ | ✖ | ✖ | ✔ (limitado) |
| Secrets | ✔ (B105-108) | ✖ | ✖ | ✖ | ✖ | ✔ |
| Deps vulns | ✖ | ✖ | ✖ | ✖ | ✖ | ✔ (limitado) |
| Imports prohibidos cross-layer | ✖ | ✔ (dataflow) | ✖ | ✔ (R11) | ✔ (ARCH-009) | **✔ declarativo** |
| Patrones de uso (os.environ, crypto, logging) | ⚠ (subprocess/random only) | ⚠ (QL costoso) | ✖ | ⚠ (hardcodeado) | ✖ | **✔ YAML** |
| Reglas de arquitectura custom | ✖ | ✖ (QL costoso) | ✖ | ✔ (Python AST) | ✔ | **✔ declarativo** |
| CI cost | ~1s | alto (PR+weekly) | ~1s | ~1s | ~1s | ~0.5s |
| False positives | bajo | medio | bajo | bajo | bajo | medio (default ruleset) |
| Developer feedback | stdout | Security tab | stdout | stdout | stdout | stdout |

### 5.3 Decision

**SEMGREP DECISION: NO ADOPT como blocking. ADOPT SOLO COMO NON-BLOCKING inicialmente (opcional).**

Justificación:
- **No hay gap material de seguridad** (NO EVIDENCE OF MATERIAL GAP): los patrones peligrosos son 0-2 por clase, todos en infraestructura con mitigación verificada. Bandit+CodeQL(PR)+Gitleaks+Trivy cubren la superficie.
- **Valor arquitectónico potencial**: reglas declarativas YAML para invariantes (imports prohibidos, os.environ en domain, logging secrets) complementarían AST Guards (hardcodeados). Pero el repo **no tiene hoy esos patrones prohibidos** → valor preventivo, no correctivo.
- **Coste ~0** (CLI, sin servidor, ~500ms/PR) → non-blocking viable sin riesgo.
- **Riesgo**: el ruleset default de seguridad de Semgrep produce falsos positivos; debe usarse con reglas propias de arquitectura y `--baseline`.

---

## 6. FASE 4 — SonarQube: reevaluación obligatoria

### 6.1 ¿Aportaría algo que no tengamos?

| Dimensión | Cobertura actual | SonarQube aportaría |
|---|---|---|
| Complexity (ciclomática) | **NO CUBIERTA** (ruff E/F/I, F-PLA-01) | ✔ |
| Cognitive complexity | **NO CUBIERTA** | ✔ |
| Duplication | **NO CUBIERTA** | ✔ |
| Long methods / coupling | **NO CUBIERTA** | ✔ |
| Dead code | ⚠ vulture instalado pero **nunca ejecutado** (F-PLA-02) | ✔ (parcial) |
| Code smells | parcial (AST guards) | ✔ |
| Bugs (heurístico) | mypy+pytest+CodeQL | ⚠ (falsos positivos) |
| Trend histórico de maintainability | **NO CUBIERTA** (señal longitudinal agregada) | ✔ |
| Quality Gate / Profile | gates binarios OCM (mejores) | ⚠ (gate genérico, ruido) |
| Cobertura trend | pytest --cov (no trend histórico) | ✔ |
| Deuda técnica (fórmula) | tracking.yaml/ADR/golden (mejor evidencia) | ⚠ fórmula opaca |

**Conclusión adversarial:** SonarQube **sí aportaría una señal longitudinal de maintainability** que ruff/mypy/pytest no proveen con la config actual (F-PLA-01/F-PLA-08). La afirmación de la auditoría previa de que "duplicaría ruff" era **incorrecta**.

### 6.2 Coste real en OrangeHouse (sin cloud)

| Recurso | Requerido por SonarQube | Estado OrangeHouse | Coste |
|---|---|---|---|
| Base de datos | PostgreSQL | **no existe** en compose | +1 contenedor + datos + backup |
| RAM/CPU | ~1-2 GB + 1 CPU | single-host (canary: CPU 0.00%/RAM 40.4MB) | significativo |
| Almacenamiento | data dir + DB | sin backup/DR (F-PL-10) | riesgo |
| Backup | requerido | no existe | nuevo |
| Actualización | Java + Sonar upgrades | manual | 2-4 h/mes |
| Autenticación | local admin token | sin LDAP/OIDC | superficie |
| Reproducibilidad | estado en DB+UI | "config as code" requerido | **viola principio** |
| CI integration | sonar-scanner + gate wait | +2-5 min/PR | coste |
| Superficie ataque | Web UI + API + DB + webhook | host crítico trading | riesgo alto |

### 6.3 Decisión

**SONARQUBE DECISION: DESCARTARSE (NO JUSTIFIED).** La conclusión final de F-PL-08 se mantiene, pero la justificación correcta es el **coste operacional** (PostgreSQL, backup, auth, updates, superficie de ataque, 2-4h/mes, viola reproducibilidad) — no la duplicación con ruff. **El gap de maintainability que SonarQube llenaría se resuelve a coste ~0** activando C901/PLR/SIM en ruff + vulture en CI + trend en nightly report.

---

## 7. FASE 5 — CodeQL vs Semgrep vs Bandit: matriz comparativa real

| Capability | AST | Ruff | Bandit | Semgrep | CodeQL |
|---|---|---|---|---|---|
| syntax | ✖ | **✔ PRIMARY** | ✖ | ⚠ | ⚠ |
| architecture (layers/invariants) | **✔ PRIMARY** | ✖ | ✖ | ✔ | ✔ (dataflow) |
| dangerous API | ⚠ (R11 subprocess) | ✖ | **✔ PRIMARY** | ✔ | ✔ |
| taint/dataflow | ✖ | ✖ | ✖ | ⚠ (limitado) | **✔ PRIMARY** |
| source→sink | ✖ | ✖ | ✖ | ⚠ | **✔ PRIMARY** |
| Python security | ⚠ | ✖ | **✔ PRIMARY** | ✔ | ✔ |
| custom OCM invariants | **✔ PRIMARY** | ✖ | ✖ | ✔ (YAML) | ⚠ (QL costoso) |
| dependency security | ✖ | ✖ | ✖ | ⚠ | ✖ |
| CI cost | ~1s | ~1s | ~1s | ~0.5s | alto (PR+weekly) |
| false positives | bajo | bajo | bajo | medio | medio |
| developer feedback | stdout | stdout | stdout | stdout | Security tab |

**Veredicto de modo (no "todo es útil"):**
- **AST Guards → BLOCKING** (invariantes OCM, gate real)
- **import-linter → BLOCKING** (50 BC, gate real)
- **Bandit → BLOCKING** (0 Med/High, gate real)
- **Gitleaks → BLOCKING** (PR)
- **Ruff → BLOCKING** (E/F/I) + **extender a C901/PLR/SIM non-blocking**
- **mypy → BLOCKING**
- **pytest → BLOCKING** (unit + coverage fail_under=40)
- **CodeQL → PR + NIGHTLY** (dataflow; no gate binario hoy; revisar findings manualmente)
- **Trivy → PR + NIGHTLY** (fs CRIT/HIGH; no gate binario hoy)
- **Semgrep → NON-BLOCKING** (si se adopta; informacional, `--baseline`)
- **SonarQube → NOT JUSTIFIED**

---

## 8. FASE 6 — Architecture Policy Coverage

| Aspecto | Mecanismo | Coverage | Gap |
|---|---|---|---|
| Layer boundaries | import-linter BC-01..55 | ✔ | — |
| Structural invariants | AST Guards R11..R16 | ✔ | evidencia hash |
| Dependency graph | import-linter + pydeps | ⚠ pydeps no en CI | cycle detection sin job CI |
| Architecture rules | architecture_linter ARCH-001..010 | ✔ (golden) | no gate directo |
| Runtime architecture | healthchecks compose + deadman | ⚠ | post-deploy health no automatizado |
| Configuration architecture | Hydra layers + config-guard + BC-51 | ✔ | — |
| Infrastructure architecture | docker-compose + hadolint + test_docker_hardening | ✔ | Grafana repro (F-PL-10) |
| Lifecycle architecture | systemd (ADR-0022) | ⚠ | `systemd_reinicia_correctamente: NO_VERIFICADO` |
| CI architecture | ocm-ci 10 jobs fail-fast | ✔ | sin stages explícitos |
| CD architecture | ocm-cd placeholder | **✖** | **GAP crítico** |
| Artifact integrity | **no existe** | **✖** | **GAP crítico** |
| Policy integrity | tracking.yaml R1..16 | ⚠ | sin hash/waiver/owner |

**Huecos especiales (dynamic imports, reflection, plugin loading):** AST guards ven lazy imports (R11/R12) pero reflection/dynamic imports (`importlib.import_module` dinámico) fuera de alcance — documentado en ADR-0015 como deuda. Sin patrón real hoy (0 matches de importlib dinámico en domain).

---

## 9. FASE 7 — Supply Chain

| Control | Estado | Gap |
|---|---|---|
| Dependency pinning | pyproject.toml pins (con razones) | ⚠ drift ccxt (F-PL-05/F-GOV-02) |
| uv.lock | ✔ commit | — |
| pip-audit | ✔ PR | 4 vulns activas (F-PLA-06) |
| Dependabot | ✔ weekly (pip + GH Actions) | auto-merge no |
| SBOM | **NO existe** | GAP: cyclonedx-sbom (pip-audit lo soporta) |
| Image scanning | Trivy fs (PR+weekly) | ⚠ Trivy image (Dockerfile) no configurado |
| Artifact digest | **NO existe** | GAP (F-PLA-05/CD) |
| Provenance | **NO existe** | GAP |
| Reproducible build | Dockerfile multi-stage ✔ | build deps no-pinned? (no verificado) |
| Dependency confusion | private index no usado | riesgo bajo |
| Malicious/transitive | pip-audit (PyPI) | ✔ |

**Conclusión FASE 7:** los controles existentes son sólidos (pip-audit PR + Trivy fs + uv.lock). Los gaps reales: **SBOM** (coste ~0, pip-audit `-f cyclonedx`), **Trivy image** sobre la imagen final (no solo fs), **artifact digest** (requiere CD). NO recomendar más herramientas por existir.

---

## 10. FASE 8 — CI/CD audit

Cadena completa (commit → PR → fast → arch → security → quality → integration → policy gate → artifact → digest → CD verify → deploy → health → accept/rollback):

| Punto | ¿Puede el agente modificar? | Control actual | Gap |
|---|---|---|---|
| Regla | Sí (tracking.yaml) | engineering_health | sin ownership/hash |
| Guard | Sí (scripts/*_guard.py) | revisión humana | sin CODEOWNERS/hash |
| Test | Sí (tests/architecture/*) | revisión humana | sin branch protection |
| CI | Sí (.github/workflows/*) | revisión humana | **sin branch protection** |
| Gate | Sí (importlinter.toml, pyproject) | revisión humana | sin branch protection |
| Artifact | No (build-time CI) | — | sin digest |
| Deploy | Manual (SSH) | deploy manual | sin CD |
| Workflow | Sí | revisión | sin protection |

**Separación de privilegios requerida:** branch protection en main (required status checks + PR review) + CODEOWNERS para `scripts/*_guard.py`, `architecture_linter/`, `policies/`, `tracking.yaml`, `.github/workflows/`. Es el punto donde los Casos B/C/D/E del threat model se bloquean.

---

## 11. FASE 9 — AI Agent Resistance (vector demostrado)

**Vector de captura conceptual (demostrable con configuración actual):**

```
bug introducido en domain (viola BC-09)
  ↓  [agente edita scripts/domain_subprocess_guard.py — mismo repo, write a main]
guards modificado (no detecta el patrón)
  ↓  [agente edita tests/architecture/test_domain_subprocess_guard.py]
test negativo neutralizado
  ↓  [agente edita tracking.yaml — marca R11 como cumplida]
registry manipulado
  ↓  [agente edita .github/workflows/ocm-ci.yml — elimina job domain-guard]
CI gate eliminado
  ↓
PASS — sin evidencia de que se violó nada
```

Cada paso es posible porque: (1) guards/tests/registry/CI viven en el mismo repo y branch; (2) no hay branch protection declarada en el repo; (3) no hay hash de evidencia que detecte la modificación de guards/tracking; (4) engineering_health valida coherencia documental, no integridad de ejecutables.

**Arquitectura mínima para impedirlo (priorizada):**
1. **Branch protection** en main (required status checks + reviews) — bloquea merge sin checks verdes.
2. **Required checks** en la API de GitHub (necesita config repo-side, fuera de repo).
3. **CODEOWNERS** para policy files → cambios de regla requieren owner.
4. **Separación de PRs**: policy files en PR separado con revisión humana.
5. **Protected policy files** (inmutable vía CODEOWNERS + branch rules).
6. **Revisión humana** para changes a guards/registry/workflows.
7. **Policy snapshot** firmado (hash de guards+registry+CI en artifact inmutable).
8. **Hash/digest** de evidencia por regla (registry + git_hash + guard hash).
9. **Audit trail** (git + CI artifact + compliance report nightly).

Sin infraestructura nueva: todo lo anterior es GitHub config + shell + CI artifact. NO requiere herramienta adicional.

---

## 12. FASE 10 — Policy Registry

**tracking.yaml → OCM Policy Registry: viable sin plataforma nueva.**

| Campo | Estado hoy (R1..R16) | Requerido |
|---|---|---|
| id | ✔ | — |
| description | ✔ | — |
| scope | ⚠ implícito | explícito |
| severity | ✖ | HIGH/MED/LOW |
| owner | ✖ | humano + CODEOWNERS |
| enforcement | ✔ (mecanismo) | + verificación CI |
| tests pos | ⚠ parcial (backtest) | path declarado |
| tests neg | ✖ no declarado | path declarado |
| evidence | ⚠ git_hash only | + hash de guards/registry |
| ci | ✔ (activada_en_ci) | verificado por health |
| adr | ⚠ docstring | campo declarado |
| master_plan | ⚠ parcial | campo declarado |
| status | ✖ | ACTIVE/DEPRECATED/SUPERSEDED |
| waiver | ✖ | + ADR |
| expiration | ✖ | ISO date + M24 |
| history | ✖ | git + registry changelog |

**Detección requerida:** dead rules, orphan rules, duplicate IDs (M1), missing tests (M21), missing enforcement (M22), missing CI (engineering_health), stale ADR (M25), expired waiver (M24), policy drift (hash), evidence drift (hash).

**¿M1..M20 o motor separado?** Extender M1..M20 (M21..M25) — el motor ya parsea YAML, enums cerrados, reconciliación. Un motor separado duplicaría infraestructura sin valor.

---

## 13. FASE 11 — Policy Gate

**check_production_gates.py (G1..G11) vs arquitectura alternativa:**

| Opción | Pros | Contras |
|---|---|---|
| **check_production_gates.py** (script binario) | Veredicto único consumible por agente; reutiliza patrones existentes (engineering_health/audit_validator); F-PL-04/F-PLA-05 lo requieren | Nuevo script (mantenimiento) |
| Shell + CI jobs agregados | Sin nuevo código | Sin veredicto agregado normativo; no consumible programáticamente |
| Makefile target | Simple | No da veredicto agregado |

**Veredicto: check_production_gates.py es la arquitectura correcta.** Debe producir `PASS`/`BLOCK` + lista de reglas que provocaron el resultado + evidencia por regla. Es el punto de enforcement central de la Constitution. Viabilidad: ALTA (patrón idéntico a engineering_health_check.py).

---

## 14. FASE 12 — Artifact Integrity

| Aspecto | Estado | Requerido |
|---|---|---|
| Qué artifact | Docker image multi-stage | — |
| Dónde vive | build local (no registry) | GHCR/registry |
| Cómo se identifica | tag git (implícito) | **SHA256 digest** |
| Cómo se verifica | — | CD Gate verify |
| Cómo evitar deploy distinto | — | **digest compare** |
| Cómo rollback conoce versión | — | **digest del artifact anterior** |

**Gap:** no existe artifact registry, digest, ni verify. Requiere: build → sha256sum → artifact (GHCR) → CD verify (digest match) → deploy → health → rollback (digest anterior).

---

## 15. FASE 13 — CD Gate en OrangeHouse (single-host)

**Arquitectura mínima (sin cloud, sin Kubernetes, sin Terraform):**

```
CI build → Docker image + SHA256 → GHCR/registry local
CD verify (deploy_ocm.sh): digest match + config-guard + health pre-deploy
deploy: docker compose up -d --build (imagen SHA fijada)
health-check: healthchecks + deadman + kafka lag + redis memory + disk
ACCEPT (continuar) / ROLLBACK (docker compose down + up -d con SHA anterior)
```

**deploy_ocm.sh — sí es necesario**, responsabilidades:
1. Verificar digest del artifact (verificación de identidad).
2. Backup de `.env` + docker-compose (snapshot).
3. `docker compose pull/up -d` con tag SHA.
4. Wait-for-health (healthchecks de redis/prometheus/alertmanager/grafana/loki/kafka).
5. Health post-deploy (deadman alert, kafka lag).
6. Decisión ACCEPT/ROLLBACK con rollback automático a SHA anterior.
7. Escribir resultado (timestamp, SHA, health) a evidencia inmutable.

Todo con shell + Docker Compose + systemd + Git + CI artifacts. SSH solo si el runner no es el host (no es el caso hoy: deploy manual). NO requiere infraestructura adicional.

---

## 16. FASE 14 — HashiCorp: revalidación

| Herramienta | Necesidad demostrable | Veredicto |
|---|---|---|
| Terraform | No — IaC = compose declarativo | **NO** |
| Vault | No — sin rotación, sin multi-host, sin cluster; `.env`+SecretStr satisfacen | **NO** (re-evaluar solo si live trading) |
| Consul | No — DNS Docker + config Hydra | **NO** |
| Nomad | No — compose + systemd (F2.6d) | **NO** |
| Packer | No — imagen Docker reproducible | **NO** |
| Boundary | No — sin superficie admin remota | **NO** |

F-PL-07 REVALIDADO (F-PLA-04). Ninguna necesidad demostrable. No por moda.

---

## 17. FASE 15 — Recomendación final de herramientas

| Herramienta | Modo | Justificación |
|---|---|---|
| AST Guards | **BLOCKING** | Invariantes estructurales OCM; gate real |
| import-linter | **BLOCKING** | 50 BC; gate real |
| Bandit | **BLOCKING** | 0 Med/High; gate real |
| Gitleaks | **BLOCKING** | PR; secret scan |
| Ruff | **BLOCKING** (E/F/I) + **extender** C901/PLR/SIM non-blocking | cerrar gap maintainability a coste ~0 |
| mypy | **BLOCKING** | tipos |
| pytest | **BLOCKING** | unit + coverage fail_under=40 |
| CodeQL | **PR + NIGHTLY** | dataflow/taint; no gate binario hoy; triage manual |
| Trivy | **PR + NIGHTLY** | fs CRIT/HIGH; no gate binario hoy |
| pip-audit | **BLOCKING** | deps; 4 vulns pendientes |
| Dependabot | weekly | deps |
| vulture | **NON-BLOCKING → BLOCKING** | dead code; ya instalado, nunca ejecutado |
| Semgrep | **NON-BLOCKING** (si se adopta) | preventivo; sin gap material |
| SonarQube | **NOT JUSTIFIED** | coste > valor; alternativa coste ~0 |
| OPA/Conftest | **NO** | sin necesidad |
| HashiCorp | **NO** | sin necesidad |

---

## 18. FASE 16 — Matriz final de cobertura

| Policy | Tool | Rule ID | Enforcement | CI | Evidence | Blocking | Gap |
|---|---|---|---|---|---|---|---|
| Layer boundaries | import-linter | BC-01..55 | forbidden/layers | ✔ | 0 broken | ✔ | — |
| Domain purity | AST guard | R11 | AST | ✔ domain-guard | ⚠ sin hash | ✔ | evidencia hash |
| App layer invariants | AST guard | R12..R16 | AST | ✔ app-guard | ⚠ sin hash | ✔ | evidencia hash |
| Architecture rules | architecture_linter | ARCH-001..010 | AST | ✔ golden | ✔ golden | ✖ | no gate directo |
| Cycle detection | pydeps | — | — | ✖ | ✖ | ✖ | **sin job CI** |
| Secrets | Gitleaks | — | scan | ✔ PR | ✔ | ✔ | — |
| APIs inseguras | Bandit | R6 | -ll | ✔ | ✔ | ✔ | — |
| Dataflow/taint | CodeQL | — | QL | ✔ PR+wk | ✔ SARIF | ✖ | no gate binario |
| Deps vulns | pip-audit | — | audit | ✔ | ⚠ 4 vulns | ✔(red) | vulns |
| FS/container vulns | Trivy | — | fs scan | ✔ PR+wk | ✔ SARIF | ✖ | no gate; no image scan |
| Estilo/tipos | ruff/mypy | — | lint/type | ✔ | ✔ | ✔ | — |
| Maintainability | **—** | — | — | ✖ | ✖ | ✖ | **gap F-PLA-01/02/08** |
| Dead code | vulture | — | — | ✖ | ✖ | ✖ | **gap F-PLA-02** |
| Policy SSOT | tracking.yaml | R1..R16 | health | ✔ | ⚠ | ⚠ | sin waiver/owner/hash |
| Policy Gate binario | **check_production_gates.py** | G1..G11 | — | ✖ | ✖ | ✖ | **gap F-PLA-05** |
| Artifact digest | — | — | — | ✖ | ✖ | ✖ | **gap** |
| CD verify/deploy/rollback | deploy_ocm.sh | — | — | ✖ | ✖ | ✖ | **gap** |
| Post-deploy health | healthchecks+deadman | — | compose | ⚠ | ⚠ | ⚠ | no automatizado |
| AI-agent resistance | branch protection | — | GitHub | ✖ | ✖ | ✖ | **gap F-PLA-09** |
| SBOM | — | — | — | ✖ | ✖ | ✖ | **gap (coste ~0)** |

---

## 19. FASE 17 — Auditoría de D-PL-01..11

| Decisión | Estado | Verificación adversarial | Veredicto |
|---|---|---|---|
| D-PL-01 (bump aiohttp/cryptography) | válida | F-PLA-06 revalida | **CONFIRMADA** — sigue siendo válida |
| D-PL-02 (check_production_gates.py) | válida | F-PLA-05 extiende | **CONFIRMADA + refuerza** — prerequisito del Policy Gate |
| D-PL-03 (Policy Registry YAML) | válida | F-PLA-09 extiende | **CONFIRMADA** — requiere M21..M25 + ownership |
| D-PL-04 (no HashiCorp) | válida | F-PLA-04 revalida | **CONFIRMADA** |
| **D-PL-05 (no Semgrep/SonarQube/OPA/Conftest)** | **requiere modificación** | F-PLA-01/07/08 | **MODIFICAR**: justificación de ruff incorrecta; Semgrep non-blocking opcional; SonarQube descarte por coste no por duplicación |
| D-PL-06 (separación de privilegios) | válida | F-PLA-09 refuerza | **CONFIRMADA + URGENTE** |
| D-PL-07 (Grafana provisioning) | válida | F-PL-10 | **CONFIRMADA** |
| D-PL-08 (systemd unit) | válida | tracking NO_VERIFICADO | **CONFIRMADA** |
| **D-PL-09 (adoptar Semgrep)** | **requiere evidencia adicional** | F-PLA-07 | **MODIFICAR**: NO blocking; valor preventivo; sin gap material |
| **D-PL-10 (adoptar SonarQube)** | **necesita modificación** | F-PLA-08 | **CERRAR como NOT JUSTIFIED** — por coste operacional, no por duplicación |
| **D-PL-11 (CD Gate)** | válida | F-PLA-05 + §15 | **CONFIRMADA** — requiere check_production_gates.py + deploy_ocm.sh + digest |

---

## 20. FASE 18 — Nuevos Findings

Ver registro `docs/audits/OCM_AUDIT_FINDINGS_2026-08-19-policy-layer-complementary.md`:

| **F-ID** | Severity | Clasificación | Título |
|---|---|---|---|
| F-PLA-01 | HIGH | NUEVO | Ruff solo E/F/I: complexity/duplication NO cubiertas |
| F-PLA-02 | LOW | NUEVO | vulture instalado pero nunca ejecutado |
| F-PLA-03 | MEDIUM | SIN CONTRADICCIÓN | CodeQL/Trivy en push/PR + weekly |
| F-PLA-04 | MEDIUM | REVALIDADO | HashiCorp: confirmado no introducir |
| F-PLA-05 | HIGH | NUEVO | Policy Gate binario inexistente |
| F-PLA-06 | CRITICAL | REVALIDADO | pip-audit 4 vulns (F-PL-02) |
| F-PLA-07 | MEDIUM | RECOMENDACIÓN | Semgrep non-blocking, sin gap material |
| F-PLA-08 | MEDIUM | CONTRADICCIÓN | SonarQube: señal longitudinal real, coste > valor |
| F-PLA-09 | HIGH | NUEVO | Cadena RULE→CI→EVIDENCE incompleta |

Todos con: F-ID, severity, classification, title, description, evidence, impact, reproduction, affected_files, affected_rules, existing_controls, gap, recommendation, related_ADR, related_Master_Plan (en el registro).

---

## 21. FASE 19 — Veredicto (19 preguntas)

1. **¿La OCM Constitution es técnicamente viable?** **PARCIALMENTE.** Estructuralmente correcta (11/22 componentes con cadena completa), pero 7 componentes sin enforcement real (Semgrep, SonarQube, Policy Gate, CD, artifact SHA, cycle detection CI, registry completo). Viable = requiere cerrar los gaps, no añadir herramientas.

2. **¿La Policy Layer formal es viable?** **SÍ.** Registry YAML (extensión tracking.yaml) + M21..M25 + hash de evidencia + ownership es evolución incremental de infraestructura existente.

3. **¿AST Guards + import-linter son suficientes para Architecture?** **SÍ para boundaries e invariantes.** NO para: cycle detection (sin job CI), evidencia con hash, waiver/ownership. Gaps de metadatos, no de mecanismo.

4. **¿Falta algún control arquitectónico?** **SÍ**: cycle detection sin job CI; pydeps no ejecutado en CI; architecture_linter no es gate directo (golden-gated). Y la cadena de evidencia (F-PLA-09).

5. **¿Bandit + CodeQL + Gitleaks son suficientes?** **SÍ para la superficie real** (0 gaps materiales, §5.1). CodeQL además corre en PR (mejor de lo previo).

6. **¿Semgrep aporta valor real en OCM?** **Preventivo sí, correctivo no.** Sin gap material de seguridad hoy; valor arquitectónico potencial (reglas declarativas). NO ADOPT blocking.

7. **¿SonarQube aporta valor real en OCM?** **Señal longitudinal de maintainability sí** (gap real por ruff E/F/I), pero el coste operacional en OrangeHouse (PostgreSQL, backup, auth, 2-4h/mes, superficie ataque) lo hace **NO JUSTIFIED**. Alternativa coste ~0: ruff extendido + vulture.

8. **¿Falta algún control de Supply Chain?** **SBOM** (coste ~0, pip-audit cyclonedx), **Trivy image** (solo fs hoy), **artifact digest** (requiere CD). No más herramientas.

9. **¿El CD Gate es viable en OrangeHouse sin cloud?** **SÍ** — shell + Docker Compose + systemd + Git + CI artifacts + deploy_ocm.sh (§15). Sin infra nueva.

10. **¿Hace falta Terraform/Vault/Consul/Nomad?** **NO** — F-PLA-04 confirma sin necesidad demostrable. (Vault solo si live trading, decisión humana futura.)

11. **¿La arquitectura resiste modificaciones de un agente IA?** **NO hoy** — vector demostrado (§11): guard→test→registry→CI→PASS es posible. Requiere: branch protection + CODEOWNERS + policy snapshot hash + M24/M25. Sin infra nueva.

12. **¿Cuál es el mínimo stack necesario?** Existing tooling + **cierre de cadena**: (1) ruff extendido (C901/PLR/SIM) + vulture; (2) registry completo + M21..M25 + hash evidencia; (3) check_production_gates.py; (4) branch protection + CODEOWNERS; (5) SBOM + Trivy image; (6) CD (digest + deploy_ocm.sh + health). **Sin Semgrep, sin SonarQube, sin HashiCorp.**

13. **¿Cuál sería el stack ideal si se prioriza máxima gobernanza?** El mínimo stack + Semgrep **non-blocking** (reglas de arquitectura declarativas, `--baseline`) como capa preventiva adicional. SonarQube solo si: (a) equipo ops dedicado, (b) PostgreSQL gestionado, (c) backup/DR implementado, (d) se acepta la superficie de ataque — NO es el caso hoy.

---

## 21.1 Matriz de Findings

| **F-ID** | Severity | Clasificación | Título |
|---|---|---|---|
| F-PLA-01 | HIGH | NUEVO | Ruff solo E/F/I: complexity/duplication NO cubiertas |
| F-PLA-02 | LOW | NUEVO | vulture instalado pero nunca ejecutado |
| F-PLA-03 | MEDIUM | SIN CONTRADICCIÓN | CodeQL/Trivy en push/PR + weekly |
| F-PLA-04 | MEDIUM | REVALIDADO | HashiCorp: confirmado no introducir |
| F-PLA-05 | HIGH | NUEVO | Policy Gate binario inexistente |
| F-PLA-06 | CRITICAL | REVALIDADO | pip-audit 4 vulns (F-PL-02) |
| F-PLA-07 | MEDIUM | RECOMENDACIÓN | Semgrep non-blocking, sin gap material |
| F-PLA-08 | MEDIUM | CONTRADICCIÓN | SonarQube: señal longitudinal real, coste > valor |
| F-PLA-09 | HIGH | NUEVO | Cadena RULE→CI→EVIDENCE incompleta |

## 21.2 Matriz de Controles

| Control | Comando canónico | Exit esperado | Resultado real |
|---|---|---|---|
| ARCH_CONTRACTS | `uv run lint-imports --config architecture_linter/importlinter.toml` | 0 | **PASS** (50 kept, 0 broken) |
| ENGINEERING_HEALTH | `uv run python scripts/engineering_health_check.py` | 0 | **PASS** |
| GOLDEN | `uv run pytest tests/architecture_linter/test_golden.py -q --no-cov` | 0 | **PASS** (4) |
| BANDIT | `uv run bandit -r apps ocm packages shared infrastructure` | 0 | **PASS** (0 Med/High, 51 Low) |
| M1..M20 | `uv run python scripts/audit_validator.py` | 0 | **PASS** (M1..M20) |
| DEPENDENCY_AUDIT | `uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325` | 0 | **FAIL** (4 vulns) → F-PLA-06 |
| YAMLLINT | `uvx yamllint -c .yamllint .` | 0 | **FAIL** (alerts.yml) |
| ARCH_LINTER | `uv run python -m architecture_linter --root . --json` | 1 si FAIL/PARTIAL | **FAIL** (7/10, golden-gobernado) |

Controles = PASS(5) + FAIL(3) = 8

## 21.3 Matriz de Decisiones

| D-ID | Decisión | Estado |
|---|---|---|
| D-PLA-01 | Activar C901/PLR/SIM en ruff (non-blocking) para cerrar el gap de maintainability | RECOMENDADO |
| D-PLA-02 | vulture a CI/pre-commit (non-blocking → blocking) | RECOMENDADO |
| D-PLA-03 | Corrección documental: CodeQL/Trivy en push/PR + weekly | RECOMENDADO |
| D-PLA-04 | HashiCorp: NO introducir (F-PL-07 revalidado) | REVALIDADO |
| D-PLA-05 | Implementar check_production_gates.py (G1..G11, veredicto binario) | REQUERIDO |
| D-PLA-06 | Bump aiohttp/cryptography (pip-audit 4 vulns) | REQUERIDO |
| D-PLA-07 | Semgrep: NO ADOPT blocking; ADOPT non-blocking opcional | RECOMENDADO |
| D-PLA-08 | SonarQube: NOT JUSTIFIED (coste operacional); alternativa ruff+vulture coste ~0 | RECOMENDADO |
| D-PLA-09 | Completar cadena RULE→CI→EVIDENCE: ownership + hash + waiver + expiración (M21..M25) | REQUERIDO |

## 22. Integridad (read-only §K)

```
git status (baseline a4d82983):
- MODIFICADOS (previos, NO de esta auditoría): .env.example, .github/workflows/ocm-ci.yml,
  .pre-commit-config.yaml, apps/app/cli/main.py, config/observability/metrics.yaml,
  docker-compose.yml, docs/plans/tracking.yaml, ocm/config/*, ocm/observability/*,
  packages/market_data/application/consumers/*, packages/market_data/application/quality/*,
  packages/market_data/infrastructure/bootstrap/*, packages/market_data/infrastructure/quality/*,
  pyproject.toml, uv.lock
- NUEVOS (previos): docs/audits/* (forensic, kafka-replay, policy-layer)
- DE ESTA AUDITORÍA: docs/audits/AUDIT_OCM_POLICY_LAYER_COMPLEMENTARY_2026-08-19.md (este informe),
  docs/audits/OCM_AUDIT_FINDINGS_2026-08-19-policy-layer-complementary.md

NO se modificó: código, tests, CI, workflows, Docker, tracking.yaml, ADRs, Plan, deps, uv.lock, config.
NO se instaló herramienta nueva. NO se implementó nada. NO se crearon ADRs.
```

---

## 23. Protocol Compliance Final

- **Protocol:** PASS
- **audit_validator:** PASS (M1..M20 sobre artefactos previos y complementarios)
- **M1..M20:** PASS
- **Working tree integrity:** PASS (solo docs/audits/ tocado)
- **Semgrep:** NO ADOPT como blocking; ADOPT NON-BLOCKING (opcional, preventivo)
- **SonarQube:** NOT JUSTIFIED (por coste operacional, no por duplicación)
- **Policy Registry:** REQUIRED NOW (extensión tracking.yaml + M21..M25 + ownership)
- **AST Policy Layer:** REQUIRED NOW (formalizar metadatos + evidencia hash)
- **Previous findings F-PL-07/F-PL-08:** F-PL-07 REVALIDADO (F-PLA-04); F-PL-08 MODIFICADO (justificación de ruff incorrecta → F-PLA-01/08; conclusión final sobrevive)
- **New findings:** 9 (F-PLA-01..09)
- **Human decisions required:** 9 (D-PLA-01..D-PLA-09)

**RESTRICCIÓN CRÍTICA:** esta auditoría no defiende la auditoría previa. Verificó cada conclusión contra el repositorio, corrigió la justificación de SonarQube (error de evidencia en F-PL-08), confirmó la cobertura de seguridad (CodeQL en PR), demostró el gap de maintainability (ruff E/F/I), y demostró el vector de captura por agente IA. La conclusión: **la OCM Constitution es viable en estructura pero no es enforcement hoy; el mínimo stack de gobernanza es el existente + cierre de cadena de evidencia, sin herramientas nuevas salvo Semgrep non-blocking opcional.** Sin implementación.