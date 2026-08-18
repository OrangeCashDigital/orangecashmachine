# OCM — Forensic Compliance & Governance Audit (Consolidación Final)

**Fecha de consolidación:** 2026-08-18
**Commit auditado:** `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283`
**Branch:** `main`
**Alcance:** Consolidación forense final de TODAS las auditorías producidas sobre el commit `bee9fb5a` (producto + sistema de auditoría), reconciliada de forma determinista, trazable y read-only, para dejar una única verdad operativa y permitir continuar el Plan Maestro.
**Metodología:** Read-only estricto (escritura solo en `docs/audits/`). Discovery normativo → baseline → reconciliación de informes → verificación selectiva en vivo (incl. CI remoto vía `gh`) → clasificación → veredicto.

---

## 1. Executive Summary

El repositorio OCM en el commit `bee9fb5a` presenta una **gobernanza documental sólida y verificable** (Plan Maestro N1–N7, tracking.yaml v2 con enums SSOT, 26 ADRs + template, 50 contratos import-linter, engineering_health_check, golden + adversarial tests). La disciplina de estados es real: deuda arquitectónica correctamente trazada (7/10 reglas ARCH en FAIL, todas con ADR/tracking), sin claims falsos ni deuda oculta.

Se consolidan **27 findings** (16 de producto + 11 del sistema de auditoría) de todas las familias de informes sobre el mismo commit. Las divergencias observadas entre informes (2/3/6/16 findings; pip-audit 4 vs 6; severidad HIGH vs CRITICAL; ADR 27 vs 30) quedan **explicadas por causa raíz y reconciliadas** — son el síntoma de un sistema de auditoría `PARTIALLY_DETERMINISTIC`, no de un producto inconsistente.

**Hallazgo crítico operativo:** el gate de CI `quality` está **roto en vivo** — el paso `Vulnerabilidades (pip-audit)` del job `Quality gates` falla en el último push @HEAD (run 32069832325, 2026-08-17 21:11), y `yamllint` falla por separado (run 32069832475, `deploy/monitoring/alerts.yml:66:162`). Esto bloquea merges a `main` hasta decisión humana.

**Veredicto:** `AUDIT_READY_WITH_FINDINGS` — determinismo del sistema: `PARTIALLY_DETERMINISTIC`. El Plan Maestro puede continuar en los frentes no bloqueados; el bloqueo es operativo (gate CI) + decisiones humanas pendientes.

---

## 2. Commit Auditado

- **Commit:** `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283` (HEAD, `main`)
- **Sincronización:** 0 ahead / 0 behind `origin/main` (verificado en informes previos)
- **Working tree inicial:** solo archivos de auditoría sin seguimiento en `docs/audits/` + `docs/governance/`
- **Cambios concurrentes detectados (otra sesión, NO míos):** `AGENTS.md` (modificado 22:09), `docs/governance/AUDIT_PROTOCOL.md` (v1.0 21:53 → v2.0 22:06). Documentados, no sobrescritos.

---

## 3. Alcance

| Incluye | Excluye |
|---|---|
| Sistema de producto: código, tests, CI, ADRs, tracking, linters, golden, seguridad, supply chain | Entorno live con capital real |
| Sistema de auditoría: AGENTS.md, AUDIT_PROTOCOL.md, jerarquía normativa, comandos, determinismo | Modificaciones de cualquier tipo |
| Reconciliación de TODAS las auditorías sobre `bee9fb5a` | — |
| Verificación remota CI (gh CLI) | — |

**Read-only:** no se modificó código, tests, CI, ADRs, tracking, AGENTS.md ni AUDIT_PROTOCOL.md. Escritura: solo `docs/audits/AUDIT_OCM_FORENSIC_COMPLIANCE_2026-08-18.md`.

---

## 4. Fuentes Normativas (descubrimiento)

| Fuente | Encontrada | Consultada | Autoridad |
|---|---|---|---|
| `docs/PLAN-Maestro-Ingenieria.md` | ✅ | ✅ | N1 — SSOT documental |
| `docs/architecture/GOVERNANCE.md` | ✅ | ✅ | Normativa |
| `AGENTS.md` | ✅ | ✅ | Normativa de agentes (modificado concurrentemente) |
| `docs/governance/AUDIT_PROTOCOL.md` | ✅ | ✅ | Protocolo de auditoría v2.0 |
| `docs/plans/tracking.yaml` | ✅ | ✅ | N2 — estado (48 hallazgos, 16 reglas) |
| `docs/architecture/decisions/` (26 ADR + template) | ✅ | ✅ | N3 — decisiones |
| `architecture_linter/importlinter.toml` | ✅ | ✅ | N4 — 50 contratos |
| `tests/architecture/` + `scripts/` | ✅ | ✅ | N5 — contratos de código |
| `.github/workflows/` | ✅ | ✅ | N6 — CI |
| `docs/audits/` (todas las auditorías) | ✅ | ✅ | N7 — histórico |
| `docs/knowledge/manifest.yaml` | ✅ | ✅ | KB (30 entradas, TIER_0-4) |
| PDFs/zips de referencia (Clean Architecture, freqtrade, hummingbot, nautilus) | ✅ | ✅ | Conocimiento externo |

**No encontrado:** ningún AUDIT_PROTOCOL.md previo a 21:53; ningún tooling de validación de auditorías; ningún comando canónico documentado por control.

---

## 5. Fuentes de Conocimiento

`docs/knowledge/manifest.yaml` (30 entradas): 3 TIER_1, 7 TIER_2, 2 TIER_3, 18 TIER_4; 22 `active`, 5 `needs_verification`, 1 `historical`, 1 `needs_attribution_review`, 1 `needs_legal_review`. Regla KB explícita: **conocimiento ≠ norma**; TIER_1–4 no son autoridad normativa; metadata no verificada no es hecho.

**Evaluación:** correctamente gobernado por AGENTS.md KB y AUDIT_PROTOCOL §B v2.0. Gap residual: no hay categoría de clasificación `CONOCIMIENTO_EXTERNO` en la taxonomía de findings (→ F-SYS-10/RECOMENDACIÓN).

---

## 6. Jerarquía Normativa (verificada)

Fuentes que la definen (coherentes entre sí salvo 1 punto):

1. **Plan §12:** `Plan (N1) → Tracking (N2) → ADR (N3) → Contratos (N4/N5) → Código → Tests → CI → Release`. Reglas: N2 gana a N1 para estado de backlog; N3 gana al código (→ CONTRADICCIÓN si divergen).
2. **AUDIT_PROTOCOL §B v2.0:** `LEVEL 4 Normative Governance > LEVEL 3 Decisiones Humanas (ADR) > LEVEL 2 Estado e Implementación (tracking) > LEVEL 1 Conocimiento interno > LEVEL 0 Conocimiento externo`.

**Discrepancia (F-SYS-06):** la posición relativa de tracking↔ADR se invierte entre ambas fuentes (Plan: N2>N3; protocolo: ADR>tracking). Resolución propuesta (D-AS-11): distinguir "estado" (tracking manda) de "decisión" (ADR manda).

**Cadena de adopción (protocolo §B):** `KNOWLEDGE → PROPOSAL → HUMAN DECISION → ADR/GOVERNANCE/PLAN → TRACKED STATE → ENFORCEABLE CONTROL → AUDIT`. Conocimiento externo sin cadena = RECOMENDACIÓN, nunca FAIL.

---

## 7. Estado de AGENTS.md

Modificado concurrentemente (22:09) añadiendo la sección **"Autonomous Agent Audit & Compliance Protocol"** con:
- Referencia obligatoria a `docs/governance/AUDIT_PROTOCOL.md` ✅
- Principio read-only (escritura solo `docs/audits/`) ✅
- `CONTROL FAIL ≠ FINDING NUEVO` ✅
- Cadena de adopción `Conocimiento → Decisión Humana → ADR/Governance → Control` ✅
- Orden de descubrimiento Plan→Governance→Tracking→ADRs→CI/Linters ✅

**Evaluación:** correctamente dimensionado (activadores en AGENTS; operativa en protocolo; mecánica en tooling). No requiere cambios.

---

## 8. Estado de AUDIT_PROTOCOL.md

v2.0 (124 líneas, secciones A–P), creado concurrentemente (21:53 v1.0 → 22:06 v2.0 con Knowledge Governance). Evaluación:

| Sección | Evaluación |
|---|---|
| A Scope / C Discovery / D Baseline / E Golden / F Controls / K Read-Only / L Linter-Golden / M Contradicción / N Human / P Checklist | ✅ Correctas |
| B Jerarquía + Cadena de Adopción | ✅ Fuerte (añadida v2.0) — salvo posición tracking↔ADR (F-SYS-06) |
| G Taxonomía | ⚠️ Prosa, sin árbol de decisión (F-SYS-03) |
| H Reconciliación/Dedup | ⚠️ Regla clave ✅ pero sin procedimiento mecánico ni matriz (F-SYS-04) |
| I Trazabilidad | ✅ Cadena + NOT_TRACED |
| J Severidad | ⚠️ Sin tabla ni reglas (F-SYS-02) |
| O Informe canónico | ⚠️ Exige `AUDIT_OCM_FORENSIC_COMPLIANCE_*`, pero coexisten 8+ nombres (F-SYS-08) |

**Gaps:** sin comandos canónicos (F-SYS-01), sin versionado interno (F-SYS-07), reproducibilidad incompleta (F-SYS-05).

---

## 9. Estado de Governance

`docs/architecture/GOVERNANCE.md`: sistema de ADR (cuándo requiere ADR), gate de CI, scripts de gobernanza (`scripts/`), contratos del kernel (shared), SafeOps, series de ADR canónica vs heredada. **Evaluación: CONFORME.**

---

## 10. Estado del Plan Maestro

- F0/F1 cerradas (2026-08-06); F2 en curso (F2.0 Engineering Health gate ✅ en CI, F2.1 blindaje calidad, F2.2 gobernanza documental, F2.4 alineación backlog, F2.5 Protocol Discovery).
- **F2.5 (Protocol Discovery)** = gate normativo antes de capital (relacionado con el sistema de auditoría aquí consolidado).
- Regla suprema: todo `main` pasa Engineering Health Check.

**Estado: continuable** en los frentes no bloqueados (§23).

---

## 11. Estado de tracking

- schema_version 2; **48 hallazgos** (35 HECHO / 12 PENDIENTE / 1 EN_CURSO; 47 CONFIRMADO / 1 PARCIALMENTE_CONFIRMADO); **16 reglas** (13 `activada_en_ci`, 13 `backtest: ok`).
- Enums SSOT en `scripts/engineering_health_check.py:36-45`: estados {PENDIENTE, EN_CURSO, HECHO, VERIFICACION, RECHAZADO}, auditoría {CONFIRMADO, NO_CONFIRMADO, PARCIALMENTE_CONFIRMADO, REFORMULADO}, prioridad {CRITICA, ALTA, MEDIA, BAJA}, fases {F1..F5}.
- `engineering_health_check.py`: gate de CI `engineering-health` = PASS remoto @HEAD.

**Evaluación: CONFORME.**

---

## 12. Estado de ADRs

- **26 ADR + template** (ADR-0003..0030, faltan 0001/0002/0018/0019). Conteo "30" en informes previos = numeración máxima, no archivos → reconciliado (F-SYS-08 / comando de conteo no canónico).
- Estados reales: 20 Aceptado (+variantes con fecha), 3 Propuesto (ADR-0014, 0021, 0028), 1 Reemplazado (ADR-0005→0012), 2 con formato alternativo `## Estado` (ADR-0022, 0024). **"Superado" NO existe** (estado inventado por un LLM → F-SYS-09).
- ADR-0021 PROPUESTA; ADR-0029/0030 **ACEPTADAS sin implementar** (B-MD-008/B-MD-009 PENDIENTE) = decisión gobernada, no contradicción.
- 2 formatos de header coexisten (`**Estado:**` vs `## Estado`) → F-SYS-09.

---

## 13. Estado de architecture_linter

- 10 reglas ARCH-001..010, standalone `--json` con salida determinista.
- **GOLDEN_EXPECTED = 7 FAIL / 1 PARTIAL / 2 PASS** (deuda gobernada; ARCH-006 y ARCH-009 PASS).
- Golden test: **4 passed** (verificado en vivo, tras purgar cachés). Adversarial: **12 passed**.
- Config: roots `packages, shared, apps, ocm`; allow ARCH-007 (CompositionRoot, RiskConfig, ConfigurationError, CursorStore).

**Semántica golden verificada correcta:** GOLDEN PASS ≠ arquitectura conforme; es no-regresión de estado (§14). Un informe concurrente la interpretó como conformidad → corregido en la reconciliación.

---

## 14. Estado de tests / golden

- **Unit:** 1164 passed, 4 deselected (`-m "not integration"`); coverage 51.46% (umbral 40%).
- **Golden:** 4 passed (test_golden_statuses_repo_actual valida que resultado == GOLDEN_EXPECTED).
- **Adversarial:** 12 passed (mitiga riesgo JUEZ=PARTE).
- **Integración local:** 4 failed (Kafka ausente) = **INFRA_FAILURE**, NO product FAIL; job remoto `integration-tests` = SUCCESS (service container).

---

## 15. Estado de CI/CD

Declaración (`.github/workflows/ocm-ci.yml`, 10 jobs + 5 workflows de seguridad) vs ejecución remota @HEAD (verificado vía `gh`):

| Workflow / Job | Declarado | Remoto @HEAD | Estado consolidado |
|---|---|---|---|
| OrangeCashMachine CI (pipeline) | ✅ | **FAILURE** (run 32069832325) | **FAIL** |
| ├─ Architecture contracts (import-linter) | ✅ | success | PASS |
| ├─ Engineering Health (F2.0) | ✅ | success | PASS |
| ├─ Unit tests | ✅ | success | PASS |
| ├─ Quality gates (ruff/mypy/SSOT/**pip-audit**) | ✅ | **failure** (paso `Vulnerabilidades`) | **FAIL** |
| ├─ Security (bandit) | ✅ | success | PASS |
| ├─ Integration tests (Kafka) | ✅ | success | PASS |
| ├─ App layer guard / Trading guards / Config validation | ✅ | success | PASS |
| yamllint | ✅ | **FAILURE** (run 32069832475) | **FAIL** |
| Gitleaks | ✅ | success | PASS |
| Actionlint | ✅ | success | PASS |
| Trivy | ✅ | success | PASS |
| CodeQL | ✅ | success | PASS |
| hadolint (docker-lint) | ⚠️ path-filter, sin run @HEAD | NO_VERIFICADO | **NO_VERIFICADO** |

**Distingo declarado vs ejecutado:** el gate `quality` está roto en vivo (pip-audit). Esto es la causa raíz de que cualquier PR nuevo quede bloqueado.

---

## 16. Seguridad y Supply Chain

| Control | Comando | Resultado | Estado |
|---|---|---|---|
| SAST propio | `bandit -r ... -ll` | 0 Med/High (51 Low) | PASS |
| CVEs deps | `pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325` | **4 vulns** (aiohttp x3, cryptography x1) | **FAIL** |
| CVEs deps (sin ignores) | `pip-audit .` | 6 vulns (aiohttp x3, cryptography, pyarrow, ecdsa) | (reconciliado — ver §17) |
| Secret scanning | Gitleaks Action | success remoto | PASS |
| Filesystem/deps | Trivy Action | success remoto | PASS |
| SAST profundo | CodeQL Action | success remoto | PASS |
| Docker lint | Hadolint Action | sin run @HEAD | NO_VERIFICADO |
| Lockfile | `uv.lock` (656K) | reproducible | PASS |
| SBOM / provenance / firma | — | **no implementado** | F-SC-02 (RECOMENDACIÓN) |
| Pinning Actions | `ocm-ci.yml` | mixto (`@v4` + SHA) | F-SC-01 (RECOMENDACIÓN) |
| Yamllint | `uvx yamllint -c .yamllint .` | `alerts.yml:66:162 error` (reproducido local = CI) | **FAIL** |

---

## 17. Reconciliación de Auditorías Previas

Matriz de reconciliación de TODAS las familias sobre `bee9fb5a`:

| Dimensión | FORENSE (20:51) | COMPLIANCE_AND_GOV (16:47) | COMPLIANCE_GOV_ARCH (20:44) | FORENSIC_COMPLIANCE (21:25, canónico previo) | TECHNICAL_COMPLIANCE (21:46) + REGISTER (21:47) | MODEL_INDEPENDENCE (22:15) | Sistema (AGENT_AUDIT_SYSTEM 22:24) | **Consolidado** |
|---|---|---|---|---|---|---|---|---|
| Findings | 2 | 3 gaps | 3 | 2 | 16 | 1 | 11 (F-SYS) | **27** |
| pip-audit | 6 CVEs | 4 (con ignores CI) | 6 CVEs | 6 CVEs | 4 (CI) = F-CI-01 | — | F-SYS-01 (comando no canónico) | **4** = comando canónico CI; 6 = comando desnudo (explicado, no error) |
| Findings pip-audit | F-SEC-01 HIGH | gap ALTA | FINDING-02 HIGH | F-SEC-01 HIGH NUEVO | F-CI-01 **CRITICAL** NUEVO | — | F-SYS-01 | **F-CI-01** (gate roto = CRITICAL; severidad elevada por bloqueo de CI) |
| Linter ARCH | F-ARCH-01 MEDIUM | 7/10 FAIL deuda | FINDING-01 MEDIUM | F-ARCH-01 MEDIUM REVALIDADO | F-ARCH-01..06 (5 REVALIDADO + 1 NUEVO) | — | — | **F-ARCH-01..06** |
| Severidad CVE | HIGH | — | HIGH | HIGH | CRITICAL (F-CI-01) | — | F-SYS-02 (sin tabla) | CRITICAL (gate roto) — F-SYS-02 explica el desacuerdo |
| ADR count | 30 | 27 archivos | 30 | — | 27 | — | F-SYS-08 (conteo no canónico) | 26 ADR + template (filesystem) |
| Estado "Superado" | **inventado** | no existe | **inventado** | no existe | no existe | — | F-SYS-09 | **NO EXISTE** — enum {Propuesto, Aceptado, Reemplazado, Obsoleto} |
| CI remoto | NO_VERIFICADO | NO_VERIFICADO | PASS (declarado) | NO_VERIFICADO | FAIL (ejecutado) | — | — | **FAIL** (ejecutado vía gh) |
| Golden | 7 FAIL deuda | consistente | coincide | REVALIDADO | correcto | GOLDEN PASS≠conforme | — | 7 FAIL gobernado; un informe lo malinterpretó (corregido) |
| yamllint | — | — | — | — | F-CI-02 | — | — | **F-CI-02** FAIL (reproducido) |

**Regla aplicada:** `CONTROL FAIL ≠ FINDING NUEVO`. Los 6 CVEs del comando desnudo NO son un finding distinto de F-CI-01 (misma causa raíz: deps vulnerables sin mitigar); la diferencia es el comando. Cardinalidad = causas raíz gobernables, no mensajes de scanner.

**Reconciliación de severidad de F-CI-01:** HIGH (informes sin verificación CI) vs CRITICAL (informe con CI ejecutado + impacto de bloqueo de merges). Estado consolidado: **CRITICAL** — el gate de CI está roto en vivo (evidencia: run 32069832325, paso pip-audit FAIL), lo que bloquea el merge de cualquier PR. F-SYS-02 (sin tabla de severidad) explica por qué dos agentes discreparon.

---

## 18. Matriz de Findings (Consolidada — 27)

### A. Findings de Producto (16) — del registro canónico `OCM_AUDIT_FINDINGS_2026-08-18.md`

| ID | Severity | Classification | Descripción |
|---|---|---|---|
| F-CI-01 | CRITICAL | NUEVO | pip-audit bloquea Quality Gate (gate CI roto en vivo) |
| F-ARCH-01 | HIGH | REVALIDADO | Multi-owner del estado de posición (ADR-0021, B-15) |
| F-ARCH-02 | HIGH | REVALIDADO | Ausencia de loop periódico de órdenes (ADR-0029, B-MD-008) |
| F-ARCH-03 | HIGH | REVALIDADO | Ausencia de balance real (ADR-0030, B-MD-009) |
| F-GOV-05 | HIGH | CONTRADICCIÓN | Inconsistencia de licencia (PolyForm vs MIT) |
| F-CI-02 | MEDIUM | NUEVO | yamllint falla en deploy/monitoring/alerts.yml (verificado) |
| F-ARCH-04 | MEDIUM | NUEVO | Freshness sin tracking previo |
| F-ARCH-05 | MEDIUM | REVALIDADO | Duplicidad de contratos (ARCH-007) |
| F-ARCH-06 | MEDIUM | REVALIDADO | Stub WSTradesSource (ARCH-008) |
| F-SC-02 | MEDIUM | RECOMENDACIÓN | Ausencia de SBOM/provenance/firma |
| F-GOV-01 | LOW | NUEVO | INVENTORY.md ausente aunque referenciado |
| F-GOV-02 | LOW | RECOMENDACIÓN | Drift de documentación ccxt |
| F-GOV-03 | LOW | RECOMENDACIÓN | Docstrings con rutas architecture/ obsoletas |
| F-GOV-04 | LOW | RECOMENDACIÓN | metrics_report.py ref escribe en ruta muerta |
| F-SC-01 | LOW | RECOMENDACIÓN | Pinning mixto de GitHub Actions |
| F-CI-03 | INFO | RECOMENDACIÓN | architecture_linter standalone no es gate CI |

### B. Findings del Sistema de Auditoría (11) — del informe `AUDIT_AGENT_AUDIT_SYSTEM_2026-08-18.md`

| ID | Severity | Classification | Descripción |
|---|---|---|---|
| F-SYS-01 | HIGH | NUEVO | Sin comandos canónicos fijados (causa de divergencia pip-audit 4 vs 6) |
| F-SYS-02 | HIGH | NUEVO | Sin tabla normativa de severidad (causa de HIGH vs CRITICAL) |
| F-SYS-03 | HIGH | NUEVO | Clasificación sin árbol mecánico (causa de 2/3/6/16 findings) |
| F-SYS-04 | MEDIUM | NUEVO | Sin tooling M1–M12 (IDs, conteos, dedup, reconciliación, trazabilidad) |
| F-SYS-05 | MEDIUM | NUEVO | Sin bloque de reproducibilidad (agente/modelo/versión herramientas) |
| F-SYS-06 | MEDIUM | CONTRADICCIÓN | Precedencia tracking↔ADR invertida entre Plan §12 y protocolo §B |
| F-SYS-07 | MEDIUM | NUEVO | Protocolo sin versionado interno (evolucionó v1.0→v2.0 en vivo) |
| F-SYS-08 | MEDIUM | CONTRADICCIÓN | Nomenclatura de informes no unificada (8+ nombres vs §O) |
| F-SYS-09 | LOW | NUEVO | Estados de ADR sin gate ("Superado" inventado; 2 formatos header) |
| F-SYS-10 | LOW | RECOMENDACIÓN | Mapeo KB TIER↔autoridad + categoría CONOCIMIENTO_EXTERNO |
| F-SYS-11 | LOW | NUEVO | NO_VERIFICADO sin distinción "no ejecuté" vs "no pude" |

### Verificación matemática

```
Total = NUEVO(4+8) + REVALIDADO(5) + REGRESIÓN(0) + CERRADO(0) + CONTRADICCIÓN(1+2) + RECOMENDACIÓN(6+1) + NO_VERIFICADO(0)
      = 12 + 5 + 0 + 0 + 3 + 7 + 0 = 27 ✅

Severidades = CRITICAL(1) + HIGH(4+3) + MEDIUM(5+5) + LOW(5+3) + INFO(1) = 1+7+10+8+1 = 27 ✅
```

---

## 19. Matriz de Controles (23)

| Control | Comando canónico | Resultado | Estado |
|---|---|---|---|
| Boundaries de paquetes | `uv run lint-imports --config architecture_linter/importlinter.toml` | 50 kept / 0 broken | **PASS** |
| Invariantes semánticos | `uv run python -m architecture_linter --root . --json` | 7 FAIL / 1 PARTIAL / 2 PASS = GOLDEN_EXPECTED | **PARTIAL** (deuda gobernada) |
| Golden regression | `uv run pytest tests/architecture_linter/test_golden.py --no-cov` | 4 passed | **PASS** |
| Adversarial | `uv run pytest tests/architecture_linter/test_adversarial.py --no-cov` | 12 passed | **PASS** |
| Suite arch linter | `uv run pytest tests/architecture_linter/ --no-cov` | 41 passed | **PASS** |
| Tipado estático | `uv run mypy .` | 0 issues / 377 files | **PASS** |
| Lint | `uv run ruff check .` | All checks passed | **PASS** |
| Formato | `uv run ruff format . --check` | 490 files | **PASS** |
| Unit tests | `uv run pytest tests/ -m "not integration"` | 1164 passed, cov 51.46% | **PASS** |
| Coverage | fail_under=40 | 51.46% ≥ 40 | **PASS** |
| SAST propio | `uv run bandit -r ... -ll` | 0 Med/High | **PASS** |
| Dependencias | `uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325` | 4 vulns, exit 1 | **FAIL** |
| Health normativo | `uv run python scripts/engineering_health_check.py` | PASS | **PASS** |
| SSOT enums | `uv run python scripts/check_ssot_enums.py` | OK | **PASS** |
| Config validation | `OCM_VALIDATE_ONLY=true uv run python -m app.cli.main` | validation_complete | **PASS** |
| YAML lint | `uvx yamllint -c .yamllint .` | alerts.yml:66:162 error | **FAIL** |
| Secret scanning | Gitleaks (workflow) | SUCCESS remoto | **PASS** |
| Secret scanning nativo | GitHub repo settings | disabled | **FAIL** |
| Docker lint | Hadolint (workflow, path-filter) | sin run @HEAD | **NO_VERIFICADO** |
| Filesystem scan | Trivy (workflow) | SUCCESS remoto | **PASS** |
| CI/CD pipeline | GitHub Actions `ocm-ci.yml` | 1 job FAIL (quality), 8 SUCCESS | **FAIL** |
| Integración local | pytest `-m integration` | 4 failed (Kafka ausente) | **INFRA_FAILURE** |
| Integración CI | pytest + Kafka (service container) | SUCCESS remoto | **PASS** |

```
Controles = PASS(16) + FAIL(4) + PARTIAL(1) + NO_VERIFICADO(1) + INFRA_FAILURE(1) = 23 ✅
```

**Nota:** 4 controles FAIL → 3 findings NUEVO (F-CI-01, F-CI-02, F-SYS-01) + causa raíz compartida; no se contó cada FAIL como NUEVO (regla `CONTROL FAIL ≠ FINDING NUEVO`).

---

## 20. Matriz de Decisiones (Consolidada — sin duplicar)

### Producto (BLOCKING: 1)

| ID | Problema | Evidencia | Opciones | Recomendación | Bloquea |
|---|---|---|---|---|---|
| **D-P1** | Gate pip-audit roto: 4 CVEs activas (aiohttp x3, cryptography x1) | run 32069832325 (pip-audit FAIL); `pip-audit . --ignore-vuln...` = 4 | A) bump aiohttp≥3.14.3, cryptography≥50.0.0; B) risk acceptance formal (ADR+tracking) | A (remediar) | ✅ BLOCKING |
| **D-P2** | Licencia: PolyForm vs MIT | `LICENSE` vs `pyproject.toml:31`/README | A) unificar MIT; B) unificar PolyForm | Decisión del owner | ❌ |
| **D-P3** | ADR-0021 (single-owner posición, B-15) | ARCH-001, F-ARCH-01 | A) aprobar/priorizar; B) diferir | A | ❌ |
| **D-P4** | ADR-0029 (cancelación real, B-MD-008) | ARCH-002/003, F-ARCH-02 | A) implementar; B) diferir | A (aceptada, implementar) | ❌ |
| **D-P5** | ADR-0030 (balance real, B-MD-009) | ARCH-004, F-ARCH-03 | A) implementar; B) diferir | A (aceptada, implementar) | ❌ |
| **D-P6** | Supply chain (SBOM/provenance/firma) + SHA pinning | F-SC-01, F-SC-02 | A) adoptar; B) diferir | A gradual | ❌ |
| **D-P7** | Linter standalone como gate CI | F-CI-03 | A) gate; B) detector documental | B (por ahora) | ❌ |
| **D-P8** | yamllint alerts.yml:66:162 | run 32069832475; local reproducido | A) fix; B) excluir | A | ❌ |

### Sistema de Auditoría (BLOCKING: 3)

| ID | Problema | Evidencia | Opciones | Recomendación | Bloquea |
|---|---|---|---|---|---|
| **D-AS-1** | Congelar/versionar AUDIT_PROTOCOL.md | v1.0→v2.0 en vivo (21:53→22:06) | A) versionar+commitear; B) seguir iterando | A | ✅ BLOCKING |
| **D-AS-2** | Comando canónico pip-audit = CI (con ignores) | 4 vs 6 CVEs reproducido | A) fijar CI; B) otro | A | ✅ BLOCKING |
| **D-AS-3** | Tabla de severidad mecánica | HIGH vs CRITICAL | A) adoptar tabla; B) no | A | ❌ |
| **D-AS-4** | Árbol de clasificación mecánico | 2/3/6/16 findings | A) adoptar; B) no | A | ❌ |
| **D-AS-5** | Matriz de reconciliación obligatoria | 8+ informes paralelos | A) sí; B) solo conflictos | A | ❌ |
| **D-AS-6** | Categoría CONOCIMIENTO_EXTERNO | F-SYS-10 | A) añadir; B) no | A | ❌ |
| **D-AS-7** | Frontmatter ADR único + gate enum | "Superado" inventado; 2 formatos | A) unificar; B) tolerar | A | ❌ |
| **D-AS-8** | Tooling M1–M12 (audit_validator) | F-SYS-04 | A) fase 1 básica; B) completa | A→B secuencial | ✅ BLOCKING |
| **D-AS-9** | Bloque de reproducibilidad obligatorio | F-SYS-05 | A) sí; B) no | A | ❌ |
| **D-AS-10** | Unificar nombre informe canónico | F-SYS-08 | A) sí; B) libre | A | ❌ |
| **D-AS-11** | Precedencia tracking↔ADR (Plan vs protocolo) | F-SYS-06 | A) ADR>tracking; B) tracking>ADR; C) matizar | C | ❌ |

```
Decisiones = BLOCKING(4: D-P1, D-AS-1, D-AS-2, D-AS-8) + NON_BLOCKING(15) = 19 ✅
```

---

## 21. Determinismo del Sistema

**Estado: `PARTIALLY_DETERMINISTIC`.**

| Criterio | Estado |
|---|---|
| Reglas definidas por el proyecto | ✅ |
| Comandos fijados | ❌ (F-SYS-01) |
| Evidencia reproducible | ⚠️ |
| Estados con enums claros | ⚠️ (tracking sí; ADR no) |
| Reconciliación definida | ❌ (F-SYS-04) |
| Contadores verificables mecánicamente | ❌ |
| Severidad con criterios explícitos | ❌ (F-SYS-02) |
| Golden State semántica inequívoca | ✅ texto / ⚠️ aplicación |
| Conocimiento externo ≠ norma | ✅ |
| Decisiones humanas separadas | ✅ |
| LLM solo donde hay juicio | ❌ |
| Otro modelo ejecuta sin inventar reglas | ❌ ("Superado" inventado) |

Matriz de operaciones:

```
MECÁNICO (hoy manual, debe ser tooling): IDs únicos, conteos, severidades válidas,
enums, tracking/ADR refs, golden, reproducibilidad, comandos canónicos, trazabilidad,
integridad documental.
LLM JUDGMENT (legítimo): causa raíz, impacto, equivalencia semántica, contradicciones sutiles.
HUMANO (separado): aceptar riesgo, ADR, excepción, gate.
```

**Divergencias observadas explicadas:** pip-audit 4 vs 6 = comando; HIGH vs CRITICAL = sin tabla; 2/3/6/16 = granularidad; ADR 27 vs 30 = conteo; "Superado" = enum no validado; CI PASS vs FAIL = declarado vs ejecutado.

---

## 22. Gaps Restantes

1. Comandos canónicos no fijados (F-SYS-01).
2. Tabla de severidad ausente (F-SYS-02).
3. Clasificación sin árbol mecánico (F-SYS-03).
4. Tooling M1–M12 inexistente (F-SYS-04).
5. Reproducibilidad incompleta en protocolo (F-SYS-05).
6. Precedencia tracking↔ADR ambigua (F-SYS-06).
7. Protocolo sin versionar (F-SYS-07).
8. Nomenclatura informes no unificada (F-SYS-08).
9. Estados ADR sin gate (F-SYS-09).
10. Mapeo KB TIER↔autoridad (F-SYS-10).
11. NO_VERIFICADO no distingue "no ejecuté"/"no pude" (F-SYS-11).
12. Gate CI roto (F-CI-01) + yamllint (F-CI-02) — operativos.

---

## 23. Readiness para continuar el Plan Maestro

**A) ¿El sistema de auditoría es suficientemente determinista?** NO. Es `PARTIALLY_DETERMINISTIC`: la capa normativa es sólida, la capa operativa (comandos, severidad, clasificación, dedup, tooling) depende del LLM.

**B) ¿Qué debe resolverse antes de continuar?**
- D-P1 (gate pip-audit roto) — bloquea merges.
- D-AS-1, D-AS-2, D-AS-8 — bloquean el determinismo del sistema.

**C) ¿Qué puede ser deuda conocida?**
- F-ARCH-01..06 (gobernados por ADR/tracking — ya REVALIDADO).
- F-SC-01/02, F-GOV-02/03/04, F-CI-03 (RECOMENDACIÓN, sin obligación incumplida).
- F-ARCH-04, F-GOV-01 (NUEVO, severidad media/baja, no bloqueantes).

**D) ¿Qué decisiones humanas son bloqueantes?** D-P1, D-AS-1, D-AS-2, D-AS-8.

**E) ¿Qué puede continuar inmediatamente?**
- F2.4 alineación de backlog, F2.5 Protocol Discovery (con D-AS-1/2 aprobadas).
- Trabajo en F3 (funcionalidades) siempre que los gates locales pasen; el merge a `main` queda condicionado a resolver D-P1.
- Implementación de ADR-0029/0030 (B-MD-008/B-MD-009) — aceptadas.

**F) ¿Qué NO debe tocarse todavía?**
- ADRs no aprobadas (ADR-0021 PROPUESTA).
- La deuda ARCH gobernada (no "arreglarla" a la fuerza en esta fase).
- AGENTS.md / AUDIT_PROTOCOL.md hasta decisión humana D-AS-1 (congelar y versionar primero).
- Los informes históricos de `docs/audits/` (N7 inmutable — no se editan).

---

## 24. Integridad

| Ítem | Estado |
|---|---|
| HEAD | `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283` — **idéntico** |
| Código | Intacto |
| Tests | Intactos |
| CI | Intacto |
| ADRs | Intactos |
| tracking.yaml | Intacto |
| AGENTS.md / AUDIT_PROTOCOL.md | Intactos (cambios concurrentes de otra sesión, no sobrescritos) |
| Escrituras propias | Solo `docs/audits/AUDIT_OCM_FORENSIC_COMPLIANCE_2026-08-18.md` (consolidación) |
| git add/commit/push | NO ejecutados |

---

## 25. Conclusión

El repositorio OCM tiene una **gobernanza documental ejemplar** y un **sistema de auditoría con cimientos normativos correctos**, pero con una **capa operativa no determinista** que produce resultados divergentes entre agentes (todo explicado y reconciliado en este informe). El bloqueo operativo real es el gate CI `quality` (pip-audit) roto en vivo. La consolidación deja **27 findings** (12 NUEVO, 5 REVALIDADO, 3 CONTRADICCIÓN, 7 RECOMENDACIÓN, 0 REGRESIÓN/CERRADO/NO_VERIFICADO) y **19 decisiones humanas** (4 bloqueantes). Con las decisiones D-P1, D-AS-1, D-AS-2, D-AS-8 tomadas, el sistema queda en condiciones de continuar el Plan Maestro de forma determinista y trazable.

**Veredicto final:** `AUDIT_READY_WITH_FINDINGS` — `AUDIT_SYSTEM_STATE: PARTIALLY_DETERMINISTIC`.
