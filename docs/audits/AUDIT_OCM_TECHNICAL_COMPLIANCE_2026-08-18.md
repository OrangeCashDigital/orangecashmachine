# AUDIT OCM — TECHNICAL COMPLIANCE & AUDITABILITY ASSESSMENT

**Fecha de auditoría:** 2026-08-18 (ejecución UTC 01:41–02:20)
**Repositorio:** `/home/orangemusic/trading/orangecashmachine`
**Rol:** Auditor técnico forense y custodio de evidencia — read-only
**Baseline (E-001):** `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283` · branch `main` · sincronizado con `origin/main`
**Evidencia:** `/tmp/ocm-audit-20260818/` (ejecución previa) y `/tmp/ocm-audit-20260818-s2/` (revalidación de esta sesión) + logs remotos GitHub Actions vía `gh api`
**Veredicto:** **AUDIT_READY_WITH_FINDINGS**

> **Nota de concurrencia (§14):** en `docs/audits/` existen 5 informes sobre auditorías relacionadas:
> `AUDIT_OCM_COMPLIANCE_AND_GOVERNANCE.md` (16:47), `AUDIT_OCM_COMPLIANCE_GOVERNANCE_ARCHITECTURE_2026-08-17.md` (20:44),
> `AUDIT_FORENSE_COMPLIANCE_2026-08-18.md` (20:51), `AUDIT_OCM_FORENSIC_COMPLIANCE_2026-08-18.md` (20:58) y
> este informe. Generados por sesiones concurrentes al repo; NO son fuentes de este informe salvo como
> [HISTÓRICA] y se reconcilian en la Sección 3.1. Ninguno fue sobrescrito ni borrado.

---

## 1. Executive Summary

OCM es un sistema con **gobernanza de ingeniería inusualmente madura**: 50 contratos import-linter sin ruptura, mypy/ruff limpios, 1164 tests unitarios pasando con coverage 51.46% (umbral 40), health check normativo PASS, y un linter de arquitectura propio con tests adversariales que demuestran semántica estructural (no heurística de nombres).

Sin embargo, esta auditoría encontró **2 fallos verificables de CI remoto** en el commit HEAD:

1. **`OrangeCashMachine CI` run `32069832325` = FAILURE** — el job **`Quality gates`** falló en el step `Vulnerabilidades (pip-audit)` (exit 1) por **4 CVEs no mitigados**: `aiohttp 3.14.1` (PYSEC-2026-3545/3546/3547, fix 3.14.2/3.14.3) y `cryptography 49.0.0` (PYSEC-2026-3552, fix 50.0.0). Reproducido localmente con el comando exacto de CI → exit 1.
2. **`yamllint` run `32069832475` = FAILURE** — error en `deploy/monitoring/alerts.yml:66:162` `new-line-at-end-of-file`. Reproducido localmente con el comando exacto de CI → exit 1.

Los demás jobs de `OrangeCashMachine CI` (architecture, engineering-health, integration-tests, trading-guards, security, config-validation, app-guard, unit-tests) **concluyeron SUCCESS** en el commit HEAD. CodeQL, Trivy, Gitleaks, Actionlint concluyeron SUCCESS; yamllint FAIL.

**Distinción obligatoria (§4):** el **golden test** del architecture_linter pasa (4 passed) pero **NO es un gate de arquitectura** — fija el estado actual (que incluye 7 reglas en FAIL y 1 en PARTIAL). El standalone `python -m architecture_linter` **NO corre en CI**. No existe ningún gate que exija findings=0. Un "golden PASS" no equivale a "reglas satisfechas".

**Deuda de arquitectura formalmente trazada:** 7 reglas FAIL + 1 PARTIAL con 19 findings (16 failed), todos rastreables a ADR/tracking: ARCH-001/002/010 (multi-owner de posición → B-15/ADR-0021 PROPUESTA), ARCH-003 (sin loop periódico de órdenes → B-MD-008/ADR-0029 ACEPTADA, impl PENDIENTE), ARCH-004 (sin balance real → B-MD-009/ADR-0030 ACEPTADA, impl PENDIENTE), ARCH-005 (freshness incompleta), ARCH-007 (8 contratos duplicados/homónimos), ARCH-008 (stub `WSTradesSource`).

**Discrepancia legal:** `LICENSE` = PolyForm Noncommercial 1.0.0 vs `pyproject.toml:31` y README = MIT. Requiere decisión humana.

**Registro estructurado:** el detalle one-by-one de todos los findings (16) vive en
[`docs/audits/OCM_AUDIT_FINDINGS_2026-08-18.md`](OCM_AUDIT_FINDINGS_2026-08-18.md), documento canónico de referencia.

---

## 2. Scope

- Código fuente (packages, shared, ocm, apps, scripts, architecture_linter).
- Configuración (config/, workflows, pre-commit, Docker, dependencias).
- Documentación normativa y de auditoría (docs/, ADRs, tracking.yaml).
- CI remoto GitHub Actions (verificado con `gh`, cuenta `OrangeCashDigital`).
- Supply chain (uv.lock, pinning Actions, SBOM, firma).
- **Excluido:** ejecución de servicios infra en local (Kafka/Redis no levantados), deploy en producción, funcionalidad de trading con capital real, infraestructura de producción (salvo evidencia en repo).

## 3. Methodology

- Read-only estricto: prohibido `git add/commit/push/reset/checkout/restore/clean`, editar código/tests/CI/ADRs/tracking/config, instalar deps, cambiar config.
- Cada claim clasificado: [EVIDENCIA] / [ARCHIVO] / [CI] / [HISTÓRICA] / [INFERENCIA] / [NO_VERIFICADO] / [CONTRADICCIÓN].
- Comandos ejecutados con los comandos SSOT de AGENTS.md; salidas guardadas en `/tmp/ocm-audit-20260818-s2/` (revalidación de esta sesión).
- Evidencia remota obtenida vía `gh api` (runs, jobs, logs GitHub Actions) durante ESTA sesión — [CI].
- Sin invocación de "contexto fantasma": todo resultado re-ejecutado o re-verificado en esta sesión (2026-08-18T02:05Z en adelante).

### 3.1 Matriz de reconciliación de informes concurrentes (§3)

Discrepancias entre este informe y los 4 informes concurrentes. Resolución: re-ejecutar el comando cuando fue posible; clasificar el resultado.

| Afirmación | FORENSE (20:51) | OCM_FORENSIC (20:58) | COMP_GOV_ARCH (20:44) | COMP_AND_GOV (16:47) | Este informe (canónico) | Revalidación esta sesión | Resolución |
|---|---|---|---|---|---|---|---|
| pip-audit vulnerabilidades | 6 en 4 paquetes | 6 en 4 paquetes | 6 en 4 paquetes | 4 sin mitigar (aiohttp×3, cryptography×1) | 4 sin mitigar (exit 1) | exit 1, 4 vulns (aiohttp×3, cryptography×1) | **CONFIRMADO: 4 sin mitigar** con el comando exacto de CI (ignora pyarrow/ecdsa por risk-accept). Los 3 informes que reportan 6 ejecutaron `pip-audit .` **sin** los `--ignore-vuln` del workflow. Comando distinto, no contradicción de hecho. |
| architecture_linter | 7 FAIL / 1 PARTIAL / 2 PASS | 7 FAIL / 1 PARTIAL / 2 PASS | 7 FAIL / 1 PARTIAL / 2 PASS | 7 FAIL / 1 PARTIAL / 2 PASS | 7 FAIL / 1 PARTIAL / 2 PASS | exit 1, 7/1/2, 19 findings (16 failed) | **CONFIRMADO — los 5 informes coinciden.** |
| Nº de ADRs | 30 | 30 | 30 | 27 archivos | 27 ADRs con estado | 27 archivos `ADR-*.md` (26 ADR + template) | **CONFIRMADO: 27 archivos.** "30 ADRs" de los concurrentes es conteo incorrecto (numeración hasta 0030, no archivos existentes). |
| tracking.yaml hallazgos | — | — | — | — | 48 (35 HECHO / 12 PENDIENTE / 1 EN_CURSO; 47 CONFIRMADO / 1 PARCIAL) | 48 hallazgos + 16 reglas (13 en CI, 3 no) | **CONFIRMADO** |
| CI remoto | REMOTE_UNVERIFIED | NO_VERIFICADO | — (solo config) | NO_VERIFICADO | FAIL: quality (pip-audit) + yamllint | FAIL confirmado vía `gh` (runs 32069832325, 32069832475) | **CONFIRMADO: CI remoto FAIL.** Los concurrentes no tenían acceso `gh`; este informe sí. |
| golden test | PASSED | PASSED | PASSED | 4 passed | 4 passed | 4 passed (exit 0 con `--no-cov`; exit 1 con `--cov` global por fail_under en subconjunto — ambiental) | **CONFIRMADO: 4 passed.** La aparente discrepancia de exit code es ambiental (addopts `--cov` + fail_under 40 al correr subconjunto). |
| gate CI del linter | no gate | "No (Informativo/Local)" | delegado a import-linter | no gate | NO es gate CI | standalone NO en workflows; solo lint-imports | **CONFIRMADO** |
| Severidad de findings | F-SEC-01 HIGH, F-ARCH-01 MEDIUM (agrupados) | idem | FINDING-01/02/03 | gaps sin IDs | F-CI-01 CRITICAL + 16 desglosados | — | **RECONCILIACIÓN:** los concurrentes agrupan en 2–3 findings; el canónico desglosa por control (regla §8: un ID por problema). La severidad CRITICAL de F-CI-01 (vs HIGH agrupada) está justificada en §21: gate de CI roto de forma verificada local y remota. |
| ARCH-006 | PASS | PASS | PASS (alerta previa AMBIENTAL por caché, resuelta) | PASS | PASS | PASS | **CONFIRMADO** |
| bandit | 0 M/H (51 Low) | 0 M/H (51 Low) | 0 M/H (51 Low) | 0 M/H (51 Low) | 0 M/H, 51 Low | exit 0, severidad Low 51 / Medium 0 / High 0 | **CONFIRMADO** |

Conclusión de reconciliación: **sin contradicciones de hecho entre informes**; las diferencias son de comando ejecutado (pip-audit), acceso a CI remoto, granularidad de findings y un error de conteo de ADRs en los concurrentes. Este informe es la fuente [OPERATIVA_SSOT] de la auditoría.

## 4. Baseline / Commit

| Aspecto | Resultado | Evidencia |
|---|---|---|
| HEAD | `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283` | E-001 |
| Branch | `main` (sync `origin/main`) | E-001 |
| Working tree inicial | 6 archivos untracked en `docs/audits/` (informes previos/concurrentes + este informe + registro) | E-001 |
| Fecha sesión | 2026-08-18T02:05:41Z (baseline re-capturado) | E-001 |
| Caches versionadas | ninguna (salvo `timestamp_cache.py` que es código, no caché) | E-036 |

## 5. Repository Integrity

`git status --short` final (E-034):

```
?? docs/audits/AUDIT_FORENSE_COMPLIANCE_2026-08-18.md
?? docs/audits/AUDIT_OCM_COMPLIANCE_AND_GOVERNANCE.md
?? docs/audits/AUDIT_OCM_COMPLIANCE_GOVERNANCE_ARCHITECTURE_2026-08-17.md
?? docs/audits/AUDIT_OCM_FORENSIC_COMPLIANCE_2026-08-18.md
?? docs/audits/AUDIT_OCM_TECHNICAL_COMPLIANCE_2026-08-18.md
?? docs/audits/OCM_AUDIT_FINDINGS_2026-08-18.md
```

- `git diff --stat HEAD` → **vacío** (cero modificaciones a archivos trackeados).
- Los 4 untracked de `docs/audits/` NO son creados por esta sesión excepto `AUDIT_OCM_TECHNICAL_COMPLIANCE_2026-08-18.md` (este informe) y `OCM_AUDIT_FINDINGS_2026-08-18.md` (registro) — autorizados por el encargo.
- **Ningún archivo de código, tests, CI, ADR, tracking.yaml o configuración fue modificado.** No se ejecutó ningún `git add/commit/push/reset/checkout/restore/clean`.

## 6. Inventory

| Área | Contenido | Evidencia |
|---|---|---|
| Estructura | `apps/`, `architecture_linter/`, `config/`, `deploy/`, `docs/`, `infrastructure/`, `ocm/`, `packages/`, `scripts/`, `shared/`, `tests/` | E-035 |
| Archivos trackeados | 629 | E-035 |
| Líneas Python | 76.942 | E-035 |
| Paquetes | `market_data`, `trading`, `portfolio` (Clean/Hexagonal) + `shared` + `ocm` | E-035 |
| Apps | `app` (CLI), `api` (FastAPI), `research` | E-035 |
| Workflows | 9 (ocm-ci, codeql, trivy, gitleaks, actionlint, docker-lint, shellcheck, yamllint, ocm-cd) | E-024, E-018 |
| ADRs | 27 archivos `ADR-*.md` + template (26 ADR) | E-023 |
| Informes de auditoría | 40+ en `docs/audits/` | E-035 |

## 7. Lint

| Control | Resultado | Evidencia |
|---|---|---|
| ruff check | PASS (`All checks passed!`, exit 0) | E-008, E-108 |
| ruff format --check | PASS (490 files) | E-008 |
| bandit -ll | PASS (severidad: 51 Low / 0 Medium / 0 High) | E-013 |
| yamllint | **FAIL** local y remoto (`alerts.yml:66:162` new-line-at-end-of-file) | E-032 |
| actionlint / shellcheck / hadolint | configurados; en HEAD: Actionlint SUCCESS; Hadolint/ShellCheck NO_VERIFICADO (path-filter, sin run) | E-030 |

## 8. Architecture Contracts

| Contrato | Resultado | Evidencia |
|---|---|---|
| import-linter (BC-NN) | **50 kept / 0 broken** (baseline mínimo 49 en CI) | E-004 |
| Config SSOT | `architecture_linter/importlinter.toml` (50 contratos activos) | E-004 |
| ARCH-009 (capas) | PASS (4 layer contracts + 46 forbidden) | E-005 |
| Kafka wire schemas | SSOT en `shared/kafka/schemas/` | E-037 |
| Layer direction | domain NO importa infraestructura (verificado por linter + adversariales) | E-007 |
## 9. Architecture Linter

### 9.1 Ejecución standalone (E-005, revalidado E-101)

`uv run python -m architecture_linter --root . --json` → **exit 1**

| Regla | Estado | # findings | Resumen del hallazgo | ADR/Tracking |
|---|---|---|---|---|
| ARCH-001 | **FAIL** | 1 | Posición gestionada por 6 owners mutables además del SSOT portfolio (TradeTracker, OMS._orders/_open/_entry_positions, RiskManager._open_positions/_positions) | B-15 / ADR-0021 PROPUESTA |
| ARCH-002 | **FAIL** | 2 | Divergencia semántica: WAC/acumulación vs reemplazo sin leer previo; SELL reduce vs pop incondicional | B-15 / ADR-0021 |
| ARCH-003 | **PARTIAL** | 1 | Reconciliación submit-time presente; sin loop periódico `fetch_open_orders/manage_open_orders` | B-MD-008 / ADR-0029 ACEPTADA |
| ARCH-004 | **FAIL** | 1 | Sin `fetch_balance/BalancePort`; sizing/drawdown contra `capital_usd` configurado (≠ balance exchange) | B-MD-009 / ADR-0030 ACEPTADA |
| ARCH-005 | **FAIL** | 1 | Freshness: detección/recovery presentes; ausentes estado consultable, contrato, propagación y enforcement pre-orden | — |
| ARCH-006 | **PASS** | 0 | Ningún port/contract huérfano (remediación efectiva) | bee9fb5 |
| ARCH-007 | **FAIL** | 8 | 8 contratos duplicados/homónimos (AnomalyRegistryPort, OrderStatus, PipelineContext, QualityPipelineResult, RetryExhaustedError, SchemaVersionError, StorageFactoryPort, _TransientProxy) | — |
| ARCH-008 | **FAIL** | 1 | Stub de producción `WSTradesSource` (`__anext__` termina de inmediato; `_running` nunca True) | — |
| ARCH-009 | **PASS** | 0 | Capas BC-08 respetadas (4 layer contracts + 46 forbidden contracts) | BC-08 |
| ARCH-010 | **FAIL** | 2 | Estado mutable `position` duplicado en 7 almacenes; `order` en 4 | B-15 |

Total: **7 FAIL + 1 PARTIAL + 2 PASS**, 19 findings (16 failed). Se graba literalmente; no se reduce a "7 reglas pendientes".

### 9.2 Golden test — NO es gate de arquitectura (§4)

`uv run pytest tests/architecture_linter/test_golden.py --no-cov` → **4 passed, exit 0** (E-006/E-102)

- `GOLDEN_EXPECTED` **codifica explícitamente** `ARCH-001: FAIL, ARCH-002: FAIL, ARCH-003: PARTIAL, ARCH-004: FAIL, ARCH-005: FAIL, ARCH-007: FAIL, ARCH-008: FAIL, ARCH-010: FAIL`. Un golden PASS significa únicamente "el estado esperado del linter no cambió".
- **Ambigüedad de exit code resuelta (§3.1):** con el `addopts --cov` global de `pyproject.toml` (fail_under=40), correr solo el subconjunto golden da `exit 1` por coverage 0% del subconjunto — condición **AMBIENTAL**, no fallo. Con `--no-cov`: exit 0, 4 passed. En CI corre dentro del job `unit-tests` con la suite completa (coverage 51.46%).
- En CI, los golden tests corren **dentro del job `unit-tests`** (`pytest tests/ -m "not integration"` recolecta `tests/architecture_linter/`), no como job propio.
- El standalone `python -m architecture_linter` **NO está en ningún workflow** (E-024: grep en `.github/workflows/` solo encuentra el `lint-imports`).
- **No existe gate que exija findings=0.** El golden solo detecta regresión (cambio de estado).

### 9.3 Tests adversariales y de reglas (E-007, revalidado E-103)

- `test_adversarial.py --no-cov` → 12 passed, exit 0. Cubren: mutaciones de renombrado (atributos, variables WAC, clases) que **no cambian el veredicto** (semántica estructural, no nombres); FP conocidos (null-objects, streams legítimos, single-write) → PASS; corregido single-owner → PASS; PARTIAL/UNKNOWN; violaciones de capa y forbidden → FAIL; global state → FAIL; orphan port → FAIL.
- `tests/architecture_linter/` completo `--no-cov` → 41 passed, exit 0 (golden 4 + adversarial 12 + rules 25).

### 9.4 Auditoría del auditor (§7)

| Dimensión | Evaluación |
|---|---|
| Independencia técnica | `architecture_linter/` es stdlib-only; **cero imports de OCM** (E-025) |
| Independencia de datos | Golden y adversarial usan fixtures sintéticas (`make_repo`), no el repo como input de entrenamiento |
| Independencia de implementación | Corroboración cruzada por `tests/architecture/` (AST policies: `test_import_contracts.py`, `test_import_linter_no_vacuo.py`) e import-linter para ARCH-009 (lee `importlinter.toml` como SSOT) |
| Independencia organizacional | El autor del linter (documentado en `docs/audits/2026-08-16-*`) es el mismo equipo OCM → **JUEZ=PARTE organizacionalmente**, mitigado por adversariales y golden, pero no es un tercero independiente |

Riesgo residual: golden **fija el estado actual como esperado** (incluyendo 7 FAIL). Si el equipo "consolida" deuda sin remediarla, el golden la legitima. No se puede afirmar independencia organizacional plena.

## 10. Tests

| Suite | Comando | Resultado | Evidencia |
|---|---|---|---|
| Unit (gate CI) | `pytest tests/ -m "not integration" --cov=...` | **1164 passed, 4 deselected**, coverage **51.46%** (threshold 40), exit 0 | E-008, E-113 |
| Golden | `pytest tests/architecture_linter/test_golden.py --no-cov` | 4 passed, exit 0 | E-006, E-102 |
| Adversarial | `pytest tests/architecture_linter/test_adversarial.py --no-cov` | 12 passed, exit 0 | E-007, E-103 |
| Arch linter completo | `pytest tests/architecture_linter/ --no-cov` | 41 passed, exit 0 | E-007, E-104 |
| Integración (local) | `pytest -m integration --no-cov` | 4 failed (Kafka ausente local), 164s | E-009 |
| Integración (CI remoto) | idem + service container | SUCCESS | E-030 |

Clasificación de los 4 fallos locales de integración: **FAIL POR INFRAESTRUCTURA** (broker ausente en `localhost:9093`), no fallo de código. Los 4 tests `C1/C2...` fallan únicamente por conexión a Kafka.

## 11. Config Validation

| Control | Comando | Resultado | Evidencia |
|---|---|---|---|
| Hydra + Pydantic bootstrap | `OCM_VALIDATE_ONLY=true uv run python -m app.cli.main` | **PASS** — `environment_validation_passed` + `validation_complete`, exit 0 | E-114 |
| CI remoto | job `config-validation` @HEAD | SUCCESS | E-030 |
| Nota | `OCM_VALIDATE_ONLY` usa `BOOL_TRUE` = {true, yes, on}; `1` NO activa validate-only (AGENTS.md gotcha) | — |

## 12. Dependency / Security

### 12.1 Versiones instaladas (E-014, E-015, E-029)
- `aiohttp 3.14.1` (transitiva vía `ccxt` directo).
- `cryptography 49.0.0` (transitiva vía `python-jose[cryptography]`, `coincurve`).
- `pyarrow 19.0.1` (directo, `<20.0`), `ecdsa 0.19.2` (transitiva de python-jose).
- `ccxt 4.5.70` (pin directo `==4.5.70`).

### 12.2 Vulnerabilidades con el comando exacto de CI (E-012, revalidado E-105)

`uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325` → **exit 1**

| Paquete | Versión | ID | Fix | En ignore-list CI |
|---|---|---|---|---|
| aiohttp | 3.14.1 | PYSEC-2026-3545 | 3.14.3 | NO |
| aiohttp | 3.14.1 | PYSEC-2026-3546 | 3.14.2 | NO |
| aiohttp | 3.14.1 | PYSEC-2026-3547 | 3.14.2 | NO |
| cryptography | 49.0.0 | PYSEC-2026-3552 | 50.0.0 | NO |
| pyarrow | 19.0.1 | PYSEC-2026-113 | — | SÍ (risk-accept documentado 2026-08-03) |
| ecdsa | 0.19.2 | PYSEC-2026-1325 | — | SÍ (risk-accept documentado) |

**Local quality gate = FAIL** (exit 1, 4 vulns sin cubrir por el risk-accept actual).
**Remote CI = FAIL** (run 32069832325, job `Quality gates`, step `Vulnerabilidades (pip-audit)`, log descargado E-031).

**Regla aplicada (§5):** NO se agrega ninguna vulnerabilidad al ignore-list. NO se oculta el finding. El risk-accept documentado solo cubre pyarrow/ecdsa; aiohttp y cryptography quedan **sin mitigar ni aceptar**.

## 13. Supply Chain

| Elemento | Estado | Evidencia |
|---|---|---|
| uv.lock | presente (641K, hashes sha256 en sdist/wheels), `requires-python >=3.11,<3.14` | E-029 |
| Pinning directo | pinneado con razón documentada (pydantic, ccxt, loguru, pyyaml, aioresilience, pybreaker) | E-017 |
| Pinning GitHub Actions | **mixto** — SHA: actionlint, shellcheck, trivy; **tag sin SHA**: `checkout@v4`, `codeql@v3`, `setup-uv@v4`, `hadolint@v3.4.0`, `gitleaks@v1` | E-018 |
| SBOM | **ausente** (búsqueda sistemática → sin archivos) | E-019 |
| Provenance / SLSA | **ausente** (sin references en workflows) | E-019 |
| Firmas / Cosign | **ausente** | E-019 |
| Syft / Grype | **ausente** | E-019 |
| Dependabot | **configurado** (pip + github-actions, weekly, con groups) | E-026 |
| Dockerfile | multi-stage, build tools no llegan a runtime, `uv==0.11.14` pinneado, `USER appuser`, no-root | E-027 |
| .dockerignore | excluye `.env`, caches, data_platform, logs | E-028 |

Nota: `shared/kafka/provenance.py` y `tests/kafka/test_schema_provenance.py` contienen "provenance" pero es **proveniencia de datos** (línea temporal), no SBOM/provenance de build. No confundir.

## 14. CI/CD

### 14.1 `ocm-ci.yml` — tabla de jobs (leído completo, E-024)

| Job | Needs | Comandos | Puede fallar? | Gate real | Resultado remoto @HEAD |
|---|---|---|---|---|---|
| `architecture` | — | `lint-imports --config architecture_linter/importlinter.toml` + guard conteo ≥49 | Sí | Merge blocker (contrato broken) | SUCCESS |
| `engineering-health` | — | `python scripts/engineering_health_check.py` | Sí | Merge blocker (coherencia normativa) | SUCCESS |
| `app-guard` | architecture, engineering-health | pytest app_layer_guard + backtest + mypy apps | Sí | Merge blocker | SUCCESS |
| `trading-guards` | architecture, engineering-health | pytest test_live_executor + test_transport_mapping | Sí | Merge blocker (R9/R10) | SUCCESS |
| `unit-tests` | architecture, engineering-health | `pytest tests/ -x -m "not integration" --cov ...` | Sí | Merge blocker (coverage ≥40) | SUCCESS |
| `security` | architecture, engineering-health | `bandit -r apps ocm packages shared infrastructure -ll` | Sí | Merge blocker | SUCCESS |
| `integration-tests` | architecture, engineering-health | `pytest tests/ -m integration --no-cov` (Kafka service container) | Sí | Merge blocker | SUCCESS |
| `config-validation` | architecture, engineering-health | `OCM_VALIDATE_ONLY=true python -m app.cli.main` | Sí | Merge blocker | SUCCESS |
| `quality` | architecture, engineering-health | ruff check, ruff format, mypy ., SSOT, **pip-audit** | Sí | Merge blocker | **FAILURE** (pip-audit exit 1) |

**Preguntas de control (§5):**
- ¿Se ejecuta? Sí, los 9 jobs en push/PR a main.
- ¿Puede fallar? Sí — `quality` **falló** en el run remoto `32069832325` (step `Vulnerabilidades (pip-audit)`, exit 1).
- ¿El job depende de él? `architecture`/`engineering-health` son raíz; el resto `needs` ambos (fail-fast).
- ¿Bloquea merge? Configuración de branch protection **NO_VERIFICADO** (requiere acceso a settings de GitHub no disponible). La semántica del workflow (jobs fail) bloquea el merge por defecto salvo bypass.
- ¿Comprueba la propiedad real o un proxy? `architecture` comprueba el conteo (proxy: guard ≥49 para evitar no-vacuo); `quality` comprueba la propiedad real (pip-audit sobre el árbol de deps).

### 14.2 Otros workflows

| Workflow | Trigger | Resultado remoto @HEAD | Nota |
|---|---|---|---|
| CodeQL | push/PR/schedule | SUCCESS | `github/codeql-action@v3` (tag, no SHA) |
| Trivy fs | push/PR/schedule | SUCCESS | `@a9c7b0f0...` (SHA) |
| Gitleaks | push/PR | SUCCESS | `gacts/gitleaks@v1` (tag) |
| Actionlint | push/PR (paths workflows) | SUCCESS | `@a5524e1c...` (SHA) |
| Hadolint | push/PR (Dockerfile) | NO_VERIFICADO (sin run en HEAD; path-filter) | `hadolint-action@v3.4.0` (tag) |
| ShellCheck | push/PR (paths .sh) | NO_VERIFICADO | `@00cae500...` (SHA) |
| yamllint | push/PR (paths yml/yaml) | **FAILURE** (run 32069832475) | `deploy/monitoring/alerts.yml:66:162` new-line-at-end-of-file |
| ocm-cd | workflow_dispatch | NO_VERIFICADO (placeholder) | Solo manual, documentado |

### 14.3 CONTRADICCIÓN detectada (§21)

- **Informe local:** `pytest -m integration` → **4 FAILED** (tests/kafka/test_integration_kafka.py) por `Unable connect to "localhost:9093"`.
- **CI remoto:** job `integration-tests` → **SUCCESS** (Kafka service container `cp-kafka:7.6.0` levantado por CI).
- **Resolución:** NO es contradicción de código. Causa: **ausencia del broker Kafka en el entorno local** durante la ejecución. Clasificación: `FAIL POR INFRAESTRUCTURA`, no "tests fallan". Estado verificado: **integración Kafka verde en CI, no reproducible localmente sin broker**.

## 15. Secret Scanning

| Elemento | Estado | Evidencia |
|---|---|---|
| Gitleaks (CI) | configurado (push/PR); **SUCCESS** remoto @HEAD run 32069832371 | E-030 |
| Gitleaks action | `gacts/gitleaks@v1` (tag, no SHA) | E-018 |
| Scan local | NO hay script local de secret scanning (`scripts/` sin gitleaks/secret) | E-115 |
| Detección de secretos de GitHub | repo setting `security_and_analysis.secret_scanning` = **disabled** (vía `gh api repos/...`) | E-116 |
| Push protection | repo setting `secret_scanning_push_protection` = **disabled** | E-116 |

Nota: la ejecución de Gitleaks en CI es SUCCESS, pero el escaneo nativo de secretos de GitHub está desactivado a nivel de repo; la mitigación depende del workflow Gitleaks.

## 16. Docker Security

| Elemento | Estado | Evidencia |
|---|---|---|
| Hadolint (CI) | configurado (`docker-lint.yml`), path-filter en Dockerfile; NO_VERIFICADO (sin run en HEAD) | E-024 |
| Hadolint config | `failure-threshold: warning`, `ignore: DL3008` | E-024 |
| Trivy fs (CI) | **SUCCESS** remoto @HEAD run 32069832381; `aquasecurity/trivy-action` pineado por SHA | E-030 |
| Dockerfile | multi-stage: builder con build-essential+git que **no llegan a runtime**; `USER appuser` (no-root); `uv==0.11.14` pinneado | E-027 |
| Imágenes base | `python:3.11-slim-bookworm` (slim) en builder y runtime | E-027 |
| .dockerignore | excluye `.env`, caches, data_platform, logs | E-028 |
| Escaneo de imagen (CI) | no hay job de escaneo de imagen en ejecución; Trivy es filesystem scan | E-024 |

## 17. Governance

| Control | Estado | Evidencia |
|---|---|---|
| AGENTS.md normativo | Presente y actualizado (migración pandas→polars, gotchas, jerarquía KB) | E-003 |
| GOVERNANCE.md política | Presente; referencia `docs/architecture/INVENTORY.md` **inexistente** (F-GOV-01) | E-020 |
| Knowledge Base manifest | 30 recursos con `status`/`authority` (TIER_1..4), gobernanza explícita | E-021 |
| tracking.yaml | 48 hallazgos: 35 HECHO / 12 PENDIENTE / 1 EN_CURSO; 47 CONFIRMADO / 1 PARCIAL; 16 reglas (13 activa_en_ci, 3 no) | E-022, E-117 |
| ADR registry | **27 archivos** `ADR-*.md` (26 ADR + template); estados Aceptado/Propuesto/Reemplazado | E-023 |
| engineering_health_check | `[EngineeringHealth] PASS` local + job remoto SUCCESS | E-010, E-030 |
| SSOT enums | `OK: todos los literales viven en shared/enums.py` | E-011 |
| SSOT env vars | `ocm/config/env_vars.py` sin cadenas OCM_* fuera de él | E-037 |

**Estado de ADRs relevantes (leídos en esta sesión):**
- ADR-0021 (single-owner posición): **PROPUESTA** — B-15 EN_CURSO, implementación PARCIAL.
- ADR-0029 (cancelación real, B-MD-008): **ACEPTADA** — implementación **PENDIENTE** (cadena: hallazgo HECHO, backlog HECHO, ADR HECHO, implementación PENDIENTE, tests PENDIENTE, CI PENDIENTE).
- ADR-0030 (balance real, B-MD-009): **ACEPTADA** — implementación **PENDIENTE** (idem).
- ADR-0022 / ADR-0024: **PROPUESTA** (formato `## Estado` distinto al `**Estado:**` del resto).

**Conclusión governance (§7):** un ADR **Aceptado NO implica implementación**. ADR-0029/0030 están aceptados pero sin implementar — la cadena de trazabilidad lo refleja correctamente en tracking.yaml. Estados separados: PROPUESTO ≠ APROBADO/ACEPTADO ≠ IMPLEMENTADO ≠ VERIFICADO.

## 18. Traceability

Cadena evaluada (§8) para los hallazgos de mayor prioridad:

- **B-12 (H-01, ADR-0016, CRÍTICA):** HECHO completo — hallazgo→backlog→ADR→implementación→tests (14)→CI (trading-guards)→evidencia reproducible→cierre. [DOCUMENTAL verificado contra tracking E-022]
- **B-15 (H-09, ADR-0006 + ADR-0021 PROPUESTA, ALTA):** EN_CURSO — ADR PROPUESTA (cadena adr=PENDIENTE), implementación PARCIAL, tests PARCIAL, CI PENDIENTE, cierre PENDIENTE.
- **B-MD-008 (ADR-0029 ACEPTADA, CRÍTICA):** PENDIENTE — cadena: hallazgo HECHO, backlog HECHO, ADR HECHO, implementación PENDIENTE, tests PENDIENTE, CI PENDIENTE, cierre PENDIENTE. **Aceptado ≠ implementado.**
- **B-MD-009 (ADR-0030 ACEPTADA, CRÍTICA):** idem.

**Estado de la cadena:** la estructura hallazgo→tracking→ADR→implementación→tests→CI→evidencia→cierre es correcta y completa para hallazgos cerrados (B-12, B-16). Para los abiertos, cada eslabón reporta su estado real sin enmascarar deuda.

### Trazabilidad por finding (§10)

| Finding | → Tracking | → ADR | → Implementation | → Tests | → CI | → Evidence | → Closure |
|---|---|---|---|---|---|---|---|
| F-CI-01 | NOT_TRACED | NOT_TRACED | NOT_TRACED | NOT_TRACED | run 32069832325 (job quality, FAIL) | E-012, E-031 | OPEN |
| F-ARCH-01 | B-15 (EN_CURSO) | ADR-0021 (PROPUESTA) + ADR-0006 | PARCIAL (fill_sync) | PARCIAL (test_fill_sync_close_divergence) | PENDIENTE (G10) | E-005 | OPEN |
| F-ARCH-02 | B-MD-008 (PENDIENTE) | ADR-0029 (ACEPTADA) | PENDIENTE | PENDIENTE | PENDIENTE | E-005, E-022 | OPEN |
| F-ARCH-03 | B-MD-009 (PENDIENTE) | ADR-0030 (ACEPTADA) | PENDIENTE | PENDIENTE | PENDIENTE | E-005, E-022 | OPEN |
| F-GOV-05 | NOT_TRACED | NOT_TRACED | NOT_TRACED | NOT_TRACED | NOT_TRACED | E-016 | OPEN |
| F-CI-02 | NOT_TRACED | NOT_TRACED | NOT_TRACED | NOT_TRACED | run 32069832475 (yamllint, FAIL) | E-032 | OPEN |
| F-ARCH-04 | NOT_TRACED | NOT_TRACED | NOT_TRACED | NOT_TRACED | NOT_TRACED | E-005 | OPEN |
| F-ARCH-05 | NOT_TRACED | NOT_TRACED | NOT_TRACED | NOT_TRACED | NOT_TRACED | E-005 | OPEN |
| F-ARCH-06 | NOT_TRACED | NOT_TRACED | NOT_TRACED | NOT_TRACED | NOT_TRACED | E-005 | OPEN |
| F-SC-02 | NOT_TRACED | NOT_TRACED | NOT_TRACED | NOT_TRACED | NOT_TRACED | E-019 | OPEN |
| F-GOV-01 | NOT_TRACED | NOT_TRACED | NOT_TRACED | NOT_TRACED | NOT_TRACED | E-020 | OPEN |
| F-GOV-02 | NOT_TRACED | NOT_TRACED | NOT_TRACED | NOT_TRACED | NOT_TRACED | E-017 | OPEN |
| F-GOV-03 | NOT_TRACED | NOT_TRACED | NOT_TRACED | NOT_TRACED | NOT_TRACED | E-033 | OPEN |
| F-GOV-04 | NOT_TRACED | NOT_TRACED | NOT_TRACED | NOT_TRACED | NOT_TRACED | E-020 | OPEN |
| F-SC-01 | NOT_TRACED | NOT_TRACED | NOT_TRACED | NOT_TRACED | NOT_TRACED | E-018 | OPEN |
| F-CI-03 | NOT_TRACED | NOT_TRACED | NOT_TRACED | NOT_TRACED | NOT_TRACED | E-024 | OPEN |

`NOT_TRACED` = no existe relación documentada en tracking.yaml/ADRs para ese eslabón (no se inventan relaciones).

## 19. Auditor Independence

- El linter es técnicamente independiente (stdlib-only, sin imports de OCM) — E-025.
- Corroboración cruzada por `tests/architecture/` (linter dinámico AST de policies) — E-033.
- **Limitación:** organizacionalmente JUEZ=PARTE (mismo equipo). Los adversariales mitigan parcialmente, pero no puede afirmarse independencia de terceros.
- Sección 9.4 evalúa la independencia del propio linter (auditoría del auditor).

## 20. Documentation

DRIFT documental detectado (§12):

| Ref | Ubicación | Problema | Clasificación |
|---|---|---|---|
| `INVENTORY.md` | `GOVERNANCE.md:48` | Referenciado "pendiente de crear"; **no existe** | DOC DRIFT (deuda declarada) — F-GOV-01 |
| `architecture/metrics.json` | `.gitignore:58` | Ruta obsoleta (dir `architecture/` no existe en HEAD) | DOC DRIFT — F-GOV-04 |
| `architecture/metrics.json` | `scripts/metrics_report.py:4,65` | Escritura a ruta inexistente (`Path("architecture/metrics.json").write_text`) — script no corre en CI | DOC DRIFT / dead path — F-GOV-04 |
| `architecture/importlinter.toml` | `shared/__init__.py:17`, `pipeline_factory.py:16`, `composition_root.py:36`, `research/data/composition_root.py:24` | Ruta obsoleta tras fusión `architecture/`→`architecture_linter/` | DOC DRIFT — F-GOV-03 |
| `ccxt==4.3.58` | `pyproject.toml:97` comentario y `AGENTS.md:96` | Pin real = `4.5.70` (pyproject.toml:100) | DOC DRIFT — F-GOV-02 |

Ninguno es un problema arquitectónico; son referencias documentales obsoletas.

## 21. Findings

| ID | Severidad | Classification | Categoría | Claim | Evidencia |
|---|---|---|---|---|---|
| F-CI-01 | **CRITICAL** | **NUEVO** | CI / Security | Job `quality` de CI remoto **FAIL** (pip-audit exit 1): 4 CVEs sin mitigar (aiohttp ×3, cryptography ×1); LOCAL QUALITY GATE = FAIL, REMOTE CI = FAIL | E-012, E-030, E-031, E-105 |
| F-CI-02 | MEDIUM | **NUEVO** | CI | Workflow `yamllint` remoto **FAIL**: `deploy/monitoring/alerts.yml:66:162` new-line-at-end-of-file; reproducido local (exit 1) | E-030, E-032, E-106 |
| F-ARCH-01 | HIGH | **REVALIDADO** | Architecture | Multi-owner de estado de posición + divergencia semántica (ARCH-001/002/010); ADR-0021 PROPUESTA, B-15 EN_CURSO | E-005, E-101 |
| F-ARCH-02 | HIGH | **REVALIDADO** | Architecture | Sin loop periódico de gestión de órdenes abiertas (ARCH-003 PARTIAL); B-MD-008, ADR-0029 ACEPTADA sin implementar | E-005, E-022 |
| F-ARCH-03 | HIGH | **REVALIDADO** | Architecture | Sin balance real del exchange (ARCH-004); B-MD-009, ADR-0030 ACEPTADA sin implementar | E-005, E-022 |
| F-ARCH-04 | MEDIUM | **NUEVO** | Architecture | Cadena de freshness incompleta niveles 3–6 (ARCH-005); sin tracking ni ADR | E-005 |
| F-ARCH-05 | MEDIUM | **REVALIDADO** | Architecture | 8 contratos duplicados/homónimos (ARCH-007); deuda fijada en golden | E-005 |
| F-ARCH-06 | MEDIUM | **REVALIDADO** | Architecture | Stub de producción `WSTradesSource` (ARCH-008); deuda fijada en golden | E-005 |
| F-GOV-05 | HIGH | **CONTRADICCIÓN** | Governance/Legal | Discrepancia de licencia: `LICENSE`=PolyForm Noncommercial vs `pyproject.toml:31`=MIT vs README=MIT | E-016 |
| F-GOV-01 | LOW | **NUEVO** | Documentation | `INVENTORY.md` referenciado en GOVERNANCE §6 y ausente; sin tracking | E-020 |
| F-GOV-02 | LOW | **RECOMENDACIÓN** | Documentation | Drift `ccxt 4.3.58` (docs/comentario) vs pin `4.5.70` | E-017 |
| F-GOV-03 | LOW | **RECOMENDACIÓN** | Documentation | 4 docstrings referencian `architecture/importlinter.toml` (ruta obsoleta) | E-033 |
| F-GOV-04 | LOW | **RECOMENDACIÓN** | Documentation | `metrics_report.py` escribe a ruta inexistente `architecture/metrics.json` | E-020 |
| F-SC-01 | LOW | **RECOMENDACIÓN** | Supply Chain | Pinning mixto de GitHub Actions (5 Actions de terceros por tag sin SHA) | E-018 |
| F-SC-02 | MEDIUM | **RECOMENDACIÓN** | Supply Chain | Sin SBOM, sin provenance, sin firma de artefactos (ausencia confirmada por búsqueda) | E-019 |
| F-CI-03 | INFO | **RECOMENDACIÓN** | CI | Linter standalone `architecture_linter` NO es gate CI; golden solo fija estado | E-024 |

**Nota de deduplicación (§11):** ARCH-001 + ARCH-002 + ARCH-010 pertenecen al mismo dominio **Position State Ownership** y se reportan como findings separados por control distinto; pip-audit (F-CI-01) es **un único finding** con root cause único (dependency gate), aunque incluya 4 advisories. Ver registro `OCM_AUDIT_FINDINGS_2026-08-18.md`.

## 22. Risk Matrix

| Finding | Probabilidad | Impacto | Riesgo | Nota operacional |
|---|---|---|---|---|
| F-CI-01 (CVEs aiohttp/cryptography) | MEDIUM | HIGH | **CRITICAL** | Explotabilidad no evaluada (sin PoC); el riesgo es la **pérdida de gate de seguridad** (CI rojo permanente) + superficie de red de aiohttp |
| F-ARCH-01 (multi-owner posición) | MEDIUM | CRITICAL (financial) | **HIGH** | Divergencia de estado de posición en live = riesgo financiero; mitigado por `dry_run:true` default y ADR-0006 |
| F-ARCH-02 (sin loop órdenes abiertas) | MEDIUM | HIGH (financial) | **HIGH** | Órdenes sin fill durante downtime solo se recuperan en siguiente submit del mismo símbolo |
| F-ARCH-03 (sin balance real) | MEDIUM | HIGH (financial) | **HIGH** | Sizing/drawdown contra capital estático ≠ balance exchange |
| F-GOV-05 (licencia) | LOW | MEDIUM (legal) | MEDIUM | Discrepancia declarativa; decisión humana requerida |
| F-ARCH-04 (freshness) | MEDIUM | MEDIUM | MEDIUM | Enforcement pre-orden ausente |
| F-ARCH-05 (duplicados) | MEDIUM | LOW | MEDIUM | Riesgo de drift semántico; no es error en runtime |
| F-ARCH-06 (stub WSTradesSource) | LOW | MEDIUM | LOW | Fallback REST documentado; capacidad honestamente no ejecutada |
| F-SC-01 (tags mutables) | MEDIUM | MEDIUM | MEDIUM | Supply-chain: tag mutables = vector de compromiso teórico |
| F-SC-02 (sin SBOM/provenance) | LOW | MEDIUM | LOW | Mejora de madurez; no bloquea |
| F-CI-02 (yamllint) | HIGH | LOW | LOW | Lint, no afecta runtime; bloquea merge de workflow yamllint |
| F-CI-03 (linter no gate) | HIGH | MEDIUM | MEDIUM | Deuda de arquitectura sin presión de CI para remediar |

## 23. Control Matrix

Formato §11: | Control | Tool | Source | Command | Result | Status | Evidence |

| Control | Tool | Source | Command | Result | Status | Evidence |
|---|---|---|---|---|---|---|
| Boundaries de paquetes | import-linter | `architecture_linter/importlinter.toml` | `uv run lint-imports --config architecture_linter/importlinter.toml` | 50 kept / 0 broken | PASS | E-004, E-107 |
| Invariantes semánticos | architecture_linter (standalone) | `architecture_linter/` | `uv run python -m architecture_linter --root . --json` | 7 FAIL / 1 PARTIAL / 2 PASS, 19 findings; coincide exactamente con GOLDEN_EXPECTED (no-regresión) | PARTIAL | E-005, E-101, E-102 |
| Golden regression | pytest | `tests/architecture_linter/test_golden.py` | `uv run pytest tests/architecture_linter/test_golden.py --no-cov` | 4 passed | PASS | E-006, E-102 |
| Adversarial | pytest | `tests/architecture_linter/test_adversarial.py` | `uv run pytest tests/architecture_linter/test_adversarial.py --no-cov` | 12 passed | PASS | E-007, E-103 |
| Suite arch linter | pytest | `tests/architecture_linter/` | `uv run pytest tests/architecture_linter/ --no-cov` | 41 passed | PASS | E-007, E-104 |
| Tipado estático | mypy | `pyproject.toml` | `uv run mypy .` | 0 issues / 377 files | PASS | E-008, E-109 |
| Lint | ruff | `pyproject.toml` | `uv run ruff check .` | All checks passed | PASS | E-008, E-108 |
| Formato | ruff format | `pyproject.toml` | `uv run ruff format . --check` | 490 files | PASS | E-008 |
| Unit tests | pytest | `tests/` | `uv run pytest tests/ -m "not integration" --cov=...` | 1164 passed, coverage 51.46% | PASS | E-008, E-113 |
| Coverage | pytest-cov | `pyproject.toml` (fail_under=40) | idem | 51.46% ≥ 40 | PASS | E-008 |
| SAST propio | bandit | `.bandit` config | `uv run bandit -r apps ocm packages shared infrastructure -ll` | 0 Medium/High (51 Low) | PASS | E-013, E-110 |
| Dependencias | pip-audit | `ocm-ci.yml` (job quality) | `uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325` | 4 vulns sin mitigar, exit 1 | **FAIL** | E-012, E-105, E-031 |
| Health normativo | engineering_health_check | `scripts/engineering_health_check.py` | `uv run python scripts/engineering_health_check.py` | PASS | PASS | E-010, E-111 |
| SSOT enums | check_ssot_enums | `scripts/check_ssot_enums.py` | `uv run python scripts/check_ssot_enums.py` | OK | PASS | E-011, E-112 |
| Config validation | Hydra + Pydantic | `ocm/config/` | `OCM_VALIDATE_ONLY=true uv run python -m app.cli.main` | validation_complete | PASS | E-114 |
| YAML lint | yamllint | `.yamllint` | `uvx yamllint -c .yamllint .` | alerts.yml:66:162 error | **FAIL** | E-032, E-106 |
| Secret scanning | Gitleaks (CI) | `.github/workflows/gitleaks.yml` | workflow | SUCCESS remoto | PASS | E-030 |
| Secret scanning nativo | GitHub | repo settings | `gh api repos/...` | disabled | FAIL | E-116 |
| Docker lint | Hadolint (CI) | `.github/workflows/docker-lint.yml` | workflow (path-filter) | sin run en HEAD | NO_VERIFICADO | E-024 |
| Filesystem scan | Trivy (CI) | `.github/workflows/trivy.yml` | workflow | SUCCESS remoto | PASS | E-030 |
| CI/CD pipeline | GitHub Actions | `.github/workflows/ocm-ci.yml` | runs @HEAD | 1 job FAIL (quality), 8 SUCCESS | FAIL | E-030, E-031 |
| Integración local | pytest | `tests/kafka/test_integration_kafka.py` | `uv run pytest tests/ -m integration --no-cov` | 4 failed (Kafka ausente) | INFRA_FAILURE | E-009 |
| Integración CI | pytest + Kafka | idem (service container) | job `integration-tests` | SUCCESS remoto | PASS | E-030 |

Resumen controles (23 filas, estados canónicos §11): **PASS 16 · FAIL 4 (pip-audit, yamllint, secret scanning nativo, pipeline CI/CD) · PARTIAL 1 (architecture_linter) · NO_VERIFICADO 1 (hadolint) · INFRA_FAILURE 1 (integración local)**.

> **CONTROL STATUS ≠ FINDING STATUS (regla de gobierno):** un control FAIL no genera automáticamente un finding NUEVO. Los 4 controles FAIL se reconcilian así: pip-audit → F-CI-01 (NUEVO, sin tracking/ADR); yamllint → F-CI-02 (NUEVO, sin tracking/ADR); secret scanning nativo → deuda no gobernada (parte de F-SC-01/F-SC-02, RECOMENDACIÓN — GitHub no es fuente de gobierno adoptada); pipeline CI/CD → consecuencia de F-CI-01+F-CI-02 (misma causa raíz, sin finding adicional). El control PARTIAL (architecture_linter) refleja deuda gobernada en golden → REVALIDADO, no NUEVO.

## 24. Human Decisions

Las decisiones NO las toma el auditor. Se separan estrictamente:

### D1 — Dependencias vulnerables
Decidir: bump de `aiohttp` ≥3.14.3 / `cryptography` ≥50.0.0, **o** risk acceptance formal (ADR + tracking, sin ocultar el finding). Necesario para restaurar CI verde. **Prohibido ampliar el ignore-list sin aprobación.**

### D2 — Licencia
Resolver: `LICENSE` = PolyForm Noncommercial vs `pyproject.toml:31`/README = MIT. Unificar las tres declaraciones tras elegir.

### D3 — ADR-0021
Decidir prioridad/aprobación/implementación del single-owner de posición (B-15, F-ARCH-01).

### D4 — ADR-0029
Decidir la implementación del loop periódico de gestión de órdenes abiertas (B-MD-008, F-ARCH-02). ADR aceptada, implementación pendiente.

### D5 — ADR-0030
Decidir la implementación del balance real del exchange (B-MD-009, F-ARCH-03). ADR aceptada, implementación pendiente.

### D6 — Supply chain
Decidir SBOM / provenance / artifact signing (F-SC-02) y SHA pinning de Actions (F-SC-01).

### D7 — Architecture linter
Decidir si los findings del `architecture_linter` deben convertirse en **gate obligatorio** de CI o permanecer como **detector documental** (F-CI-03). Actualmente NO es gate: el golden test solo fija el estado esperado.

## 25. Remediation Roadmap

Solo se documenta prioridad; NO se modifica código.

- **P0 (inmediato):** F-CI-01 (pip-audit), F-GOV-05 (licencia).
- **P1 (corto plazo):** F-ARCH-01/002/003/004 (dominio Position State Ownership + órdenes + balance: ARCH-001/002/003/004).
- **P2 (medio plazo):** F-ARCH-05 (ARCH-007 duplicados), F-ARCH-06 (ARCH-008 stub), F-CI-02 (yamllint alerts.yml).
- **P3 (largo plazo):** F-SC-02 (SBOM/provenance/firma), F-SC-01 (SHA pinning Actions), F-GOV-01/02/03/04 (documentation drift).

## 26. Evidence Index

```
[E-001]  git rev-parse HEAD / status / date            → bee9fb5a..., main, 6 untracked, 2026-08-18T02:05:41Z
[E-002]  git status --short (final)                    → repo no alterado
[E-003]  cat AGENTS.md                                 → normativo, actualizado
[E-004]  uv run lint-imports --config architecture_linter/importlinter.toml → 50 kept / 0 broken, exit 0
[E-005]  uv run python -m architecture_linter --root . --json → exit 1, 7 FAIL + 1 PARTIAL + 2 PASS, 19 findings
[E-006]  uv run pytest tests/architecture_linter/test_golden.py --no-cov → 4 passed, exit 0
[E-007]  uv run pytest tests/architecture_linter/ --no-cov → 41 passed (golden 4 + adversarial 12 + rules 25)
[E-008]  uv run pytest tests/ -m "not integration" --cov=... → 1164 passed, coverage 51.46%
[E-009]  uv run pytest tests/ -m integration --no-cov   → 4 failed (Kafka ausente local, 164s)
[E-010]  uv run python scripts/engineering_health_check.py → PASS
[E-011]  uv run python scripts/check_ssot_enums.py      → OK
[E-012]  uv run pip-audit . --ignore-vuln 113 --ignore-vuln 1325 → exit 1, 4 vulns (aiohttp ×3, cryptography)
[E-013]  uv run bandit -r apps ocm packages shared infrastructure -ll → exit 0, severidad 0 M / 0 H / 51 L
[E-014]  uv run python -c "import ccxt,aiohttp,cryptography" → 4.5.70 / 3.14.1 / 49.0.0
[E-015]  grep uv.lock → versions confirmadas
[E-016]  head LICENSE / grep license pyproject.toml      → PolyForm vs MIT (CONTRADICCIÓN)
[E-017]  grep ccxt pyproject.toml AGENTS.md              → 4.5.70 vs 4.3.58 (DRIFT)
[E-018]  grep "uses:" .github/workflows/*.yml            → pinning mixto (SHA vs tag)
[E-019]  find/grep SBOM/provenance/cosign/syft/grype      → ausente
[E-020]  grep INVENTORY.md / ls docs/architecture/INVENTORY.md → inexistente; metrics_report.py rutas
[E-021]  manifest.yaml parse                              → 30 recursos, status/authority
[E-022]  tracking.yaml parse (yaml.safe_load)            → 48 hallazgos, 16 reglas, distribución estados
[E-023]  grep Estado ADR-*.md                            → 27 archivos ADR con estados
[E-024]  cat .github/workflows/*.yml + grep architecture_linter → linter standalone NO en CI
[E-025]  grep "import market_data|trading|ocm|shared" architecture_linter/ → cero imports OCM
[E-026]  cat .github/dependabot.yml                       → pip + actions, weekly
[E-027]  cat Dockerfile                                   → multi-stage, non-root, uv pin
[E-028]  cat .dockerignore                                → secrets/caches excluidos
[E-029]  ls -lh uv.lock + head                           → lock con hashes
[E-030]  gh run list / gh run view @HEAD bee9fb5a         → CI run 32069832325 FAILURE (quality), yamllint 32069832475 FAILURE, CodeQL/Trivy/Gitleaks/Actionlint SUCCESS
[E-031]  gh api jobs/95510406862/logs                    → step pip-audit exit 1 (aiohttp+cryptography)
[E-032]  gh api jobs/95510292513/logs + uvx yamllint local → alerts.yml:66:162 new-line-at-end-of-file (remoto y local FAIL)
[E-033]  grep architecture/importlinter.toml shared/ packages/ apps/ → 4 refs docstring obsoletas
[E-034]  git status --short (final)                      → repo no alterado por esta auditoría
[E-035]  git ls-files | wc -l / wc -l *.py               → 629 archivos, 76.942 líneas
[E-036]  git ls-files | grep cache                        → sin caches versionadas (solo timestamp_cache.py código)
[E-037]  ocm/config/env_vars.py SSOT                      → sin cadenas OCM_* externas
[E-101]  uv run python -m architecture_linter --root . --json (S2) → revalidado, exit 1, 7/1/2, 19 findings
[E-102]  uv run pytest tests/architecture_linter/test_golden.py --no-cov (S2) → 4 passed, exit 0
[E-103]  uv run pytest tests/architecture_linter/test_adversarial.py --no-cov (S2) → 12 passed, exit 0
[E-104]  uv run pytest tests/architecture_linter/ --no-cov (S2) → 41 passed, exit 0
[E-105]  uv run pip-audit . --ignore-vuln 113 --ignore-vuln 1325 (S2) → exit 1, 4 vulns
[E-106]  uvx yamllint -c .yamllint . (S2)               → exit 1, alerts.yml:66:162
[E-107]  uv run lint-imports --config architecture_linter/importlinter.toml (S2) → 50 kept / 0 broken
[E-108]  uv run ruff check . (S2)                        → All checks passed
[E-109]  uv run mypy . (S2)                              → Success, 0 issues / 377 files
[E-110]  uv run bandit -r apps ocm packages shared infrastructure -ll (S2) → exit 0, 0 M / 0 H / 51 L
[E-111]  uv run python scripts/engineering_health_check.py (S2) → PASS
[E-112]  uv run python scripts/check_ssot_enums.py (S2)  → OK
[E-113]  uv run pytest tests/ -m "not integration" --cov=... (S2) → 1164 passed, coverage 51.46%
[E-114]  OCM_VALIDATE_ONLY=true uv run python -m app.cli.main (S2) → PASS, validation_complete
[E-115]  ls scripts/ | grep secret/gitleaks              → sin scan local de secretos
[E-116]  gh api repos/OrangeCashDigital/orangecashmachine → secret_scanning disabled, push_protection disabled
[E-117]  python yaml.safe_load tracking.yaml (S2)        → 48 hallazgos, 16 reglas, 13 en CI
```

Artefactos (S2 = revalidación esta sesión): `/tmp/ocm-audit-20260818-s2/` — `architecture_linter_standalone.json`, `pytest_golden.txt`, `pytest_golden_nocov.txt`, `pytest_adversarial.txt`, `pytest_adversarial_nocov.txt`, `pytest_arch_linter_all_nocov.txt`, `pip_audit_ci.txt`, `yamllint.txt`, `lint_imports.txt`, `ruff_check.txt`, `mypy.txt`, `bandit_ll.txt`, `health_check.txt`, `ssot.txt`, `pytest_unit_tests.txt`, `config_validation.txt`, `quality_job.log`, `yamllint_job.log`.
Artefactos (ejecución previa): `/tmp/ocm-audit-20260818/` — 18 archivos.

## 27. Repository Integrity (final)

- `git diff --stat HEAD` → **vacío** (cero modificaciones a archivos trackeados).
- HEAD intacto: `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283`.
- Untracked finales (todos documentales, dentro de `docs/audits/`): 4 informes concurrentes/históricos + este informe + registro de findings.
- **No se ejecutó ningún `git add/commit/push/reset/checkout/restore/clean`.**
- Documentación generada por esta auditoría (autorizada por el encargo): informe canónico `docs/audits/AUDIT_OCM_TECHNICAL_COMPLIANCE_2026-08-18.md` y registro estructurado `docs/audits/OCM_AUDIT_FINDINGS_2026-08-18.md`.

---

*Auditoría forense read-only. Evidencia en `/tmp/ocm-audit-20260818/` y `/tmp/ocm-audit-20260818-s2/`. No se modificó código, tests, CI, ADRs, tracking.yaml ni configuraciones. Precisión y trazabilidad por encima de favorabilidad.*
