# AUDITORÍA DEL SISTEMA DE AUDITORÍA OCM — ENDURECIMIENTO Y DETERMINISMO

**Fecha:** 2026-08-18
**Encargo:** Misión de hardening del sistema de auditoría OCM: hacerlo REPRODUCIBLE, DETERMINISTA y RESISTENTE a variaciones entre modelos de IA, mediante tooling mecánico (reglas M1..M20) y reglas canónicas.
**Autoridad:** `docs/governance/AUDIT_PROTOCOL.md` v2.1 (§Q–§T), `AGENTS.md`, `docs/architecture/GOVERNANCE.md`, `docs/PLAN-Maestro-Ingenieria.md` (§12 N1–N7).
**Auditor ejecutor:** Agente OpenCode (modelo deepseek-v4-flash-free) bajo protocolo de auditoría de agentes.

---

## 1. Objetivo y Alcance

**Objetivo:** convertir el sistema de auditoría OCM de `PARTIALLY_DETERMINISTIC` a un sistema donde la evidencia mecánica (comandos canónicos + validadores ejecutables) resuelva toda ambigüedad, y el LLM quede restringido a reglas de juicio explícitas (L1..L4). Mismo commit + mismo protocolo + mismas herramientas ⇒ resultados equivalentes entre agentes.

**Alcance (lo que se modificó):**

| Artefacto | Acción |
|---|---|
| `docs/governance/AUDIT_PROTOCOL.md` | v2.0 → v2.1: §Q (clasificación M/L), §R (comandos canónicos), §S (reproducibilidad), §T (tooling), §U (changelog) |
| `AGENTS.md` | +1 línea: "Tooling mecánico primero" (10 líneas totales del bloque de auditoría, 9 concurrentes + 1 propia) |
| `scripts/audit_validator.py` | **NUEVO** — validador mecánico M1..M20 (stdlib + pyyaml) |
| `tests/architecture/test_audit_validator.py` | **NUEVO** — 13 tests (FAIL en cada violación, PASS en estados válidos) |
| `docs/audits/AUDIT_AUDIT_SYSTEM_HARDENING_2026-08-18.md` | **ESTE INFORME** |

**Restricciones cumplidas:** NO se modificó código de producto, tests funcionales, dependencias, `pyproject.toml`, `uv.lock`, CI funcional, workflows, `tracking.yaml` ni ADRs. NO se hizo `git add/commit/push`. HEAD intacto.

## 2. Estado Previo del Sistema

- Auditorías previas: `AUDIT_AGENT_AUDIT_SYSTEM_2026-08-18.md` (F-SYS-01..11, GAP-01..13, D-AS-1..11) y `AUDIT_OCM_FORENSIC_COMPLIANCE_2026-08-18.md` (informe canónico, 25 secciones).
- Verificación forense previa: 27 findings (16 producto + 11 F-SYS), 23 controles (16 PASS / 4 FAIL / 1 PARTIAL / 1 NO_VERIFICADO / 1 INFRA_FAILURE), 19 decisiones (4 BLOCKING).
- Problema central: **dependencia del modelo** — distintos agentes (Gemini/Claude/Codex/DeepSeek/OpenCode) aplican interpretaciones distintas a: estados ADR, conteo de findings, severidades, comandos de herramientas (ej. divergencia pip-audit 4 vs 6), y existencia de referencias.
- Causa raíz: normas redactadas en lenguaje natural sin validación mecánica ejecutable.

## 3. Findings de Origen (F-SYS)

Los hallazgos del sistema de auditoría que motivan este hardening (detallados en `AUDIT_AGENT_AUDIT_SYSTEM_2026-08-18.md`):

| ID | Problema | Resolución mecánica |
|---|---|---|
| F-SYS-01 | pip-audit divergente (4 vs 6) | Comando canónico = el del CI (M13, §R) |
| F-SYS-02 | Cuentas de controls report differ | M20 (parsing de Matriz de Controles + strip `*`) |
| F-SYS-03 | Estados ADR inventados | M4 (enum cerrado) + `normalize_adr_state` |
| F-SYS-04 | Contadores inconsistentes | M10 (reconciliación matemática) |
| F-SYS-05 | No se ejecutaron comandos | M13 (comando canónico citado en informe) |
| F-SYS-06 | Determinismo sin verificación | M11 (golden) + tests |
| F-SYS-07 | Falta de check de integridad | §19 de este informe + checklist §P.6 |
| F-SYS-08 | Ausencia de evidencia de revalidación | M6/M7/M9 |
| F-SYS-09 | Clasificaciones inventadas | M2 (enum cerrado) |
| F-SYS-10 | Severidades inventadas | M3 (enum cerrado) |
| F-SYS-11 | Verificación sin evidencia | M7/M14 + bloque de reproducibilidad |

**Estado tras hardening:** F-SYS-01..11 → RESUELTOS por tooling (ver §16, decisiones).

## 4. Arquitectura del Sistema de Auditoría Endurecido

```
encargo de auditoría
        │
        ▼
┌─────────────────────────────┐
│ 1. scripts/audit_validator │  ← MACHINE CHECKS FIRST
│    (M1..M20, exit 0/1/2)   │
└────────────┬────────────────┘
             │ evidencia + errores
             ▼
┌─────────────────────────────┐
│ 2. Reglas de juicio L1..L4  │  ← LLM JUDGMENT SECOND
│    (solo donde no hay Mx)   │
└────────────┬────────────────┘
             │
             ▼
┌─────────────────────────────┐
│ 3. Informe canónico          │  ← + bloque de reproducibilidad (§S)
│    + registro de findings    │
└────────────┬────────────────┘
             │
             ▼
┌─────────────────────────────┐
│ 4. Revalidación (M10/M17/    │  ← feedback loop (auditorías futuras)
│    M18/M19/M20 + dedup)      │
└─────────────────────────────┘
```

- **Frontera explícita:** el tooling recopila evidencia y aplica reglas cerradas; el LLM decide solo sobre L1–L4. Nunca al revés.
- **Cadena de adopción:** `Conocimiento` → `Decisión Humana` → `ADR/Governance` → `Control` (ninguna fuente externa es norma por sí sola).

## 5. Reglas Mecánicas (M1..M20)

Implementadas en `scripts/audit_validator.py`. Resumen (detalle completo en AUDIT_PROTOCOL §Q):

| Regla | Descripción | Mecánica |
|---|---|---|
| M1 | IDs únicos en registro | set de IDs |
| M2 | Clasificación ∈ enum cerrado (7) | parser + comparación |
| M3 | Severidad ∈ enum cerrado (5) | parser + comparación |
| M4 | Estados ADR válidos | parser `**Estado:**` / `## Estado` + enum base |
| M5 | Existencia de referencias | pathlib `exists()` (WARN si el hallazgo documenta archivo ausente) |
| M6 | Refs tracking ∈ tracking.yaml | regex `B-*`/`R*` + parse YAML |
| M7 | Evidence no vacía | parser de sección |
| M8 | Control declarado | parser de sección |
| M9 | Refs ADR-NNNN existen | glob de archivos |
| M10 | Reconciliación matemática | Σ clasificación = Σ severidad = total; resumen declarado = parseado |
| M11 | Golden state | GOLDEN_EXPECTED (test_golden.py) vs linter real (`--json`) |
| M12 | Estructura mínima del informe | secciones obligatorias |
| M13 | Comandos canónicos | informe cita comando canónico |
| M14 | Versiones de herramientas | informe declara versiones |
| M15 | Estados inventados | `Superado/Resuelto/...` → FAIL |
| M16 | Duplicados por ID | conteo |
| M17 | Informe ↔ registro | registro ⊆ informe (extras = WARN) |
| M18 | Consistencia severidad | informe == registro |
| M19 | Consistencia clasificación | informe == registro |
| M20 | Control counts | Σ filas matriz = total; FAIL con tracking ≠ NUEVO |

**Semántica:** PASS = sin errores; WARN = nota que no bloquea (marcadores de negación, F-SYS extras); FAIL = bloquea (exit 1); SKIP = flag no provisto.

## 6. Reglas de Juicio (L1..L4)

| Regla | Ámbito | Ejemplo |
|---|---|---|
| L1 | Equivalencia semántica / misma causa raíz | dos findings que describen el mismo problema |
| L2 | Impacto de negocio / trading | contexto de estrategias, riesgo |
| L3 | Interpretación arquitectónica | contradicciones sutiles entre ADRs |
| L4 | Priorización / roadmap / aceptación de conocimiento externo | qué es obligación vs hipótesis |

**Garantía de determinismo:** ninguna regla Mx depende del modelo. L1–L4 quedan documentadas como juicio, con evidencia mecánica subyacente.

## 7. Comandos Canónicos

Fuente de verdad: CI real (`ocm-ci.yml`) y Governance — no la preferencia del agente.

| Control | Comando canónico | Verificado | Resultado |
|---|---|---|---|
| ARCH_CONTRACTS | `uv run lint-imports --config architecture_linter/importlinter.toml` | — (gate CI) | 0 esperado |
| ENGINEERING_HEALTH | `uv run python scripts/engineering_health_check.py` | — | 0 esperado |
| ARCH_LINTER | `uv run python -m architecture_linter --root . --json` | ✓ | schema 1.0 |
| GOLDEN | `uv run pytest tests/architecture_linter/test_golden.py -q --no-cov` | ✓ | 41 passed |
| DEPENDENCY_AUDIT | `uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325` | ✓ | 4 vulns, 2 ignored |
| YAMLLINT | `uvx yamllint -c .yamllint .` | — | 0 errores esperado |

**Divergencia pip-audit (F-SYS-01) resuelta:** el comando normativo es el del `.github/workflows/ocm-ci.yml` (incluye los 2 ignores del risk-accept documentado 2026-08-03). El "6" era el mismo comando sin ignores; **no era un finding distinto**.

## 8. Golden Tests y No-Regresión

- **M11** parsea `GOLDEN_EXPECTED` de `tests/architecture_linter/test_golden.py` (7 FAIL / 1 PARTIAL / 2 PASS) y lo compara con `uv run python -m architecture_linter --root . --json` real.
- **Semántica de golden:** GOLDEN PASS ≠ arquitectura conforme. El golden DEBE contener FAIL/PARTIAL (deuda gobernada) para ser honesto; la no-regresión se mide contra ese golden, no contra un ideal imaginado.
- **Verificado:** M11 PASS sobre el repo real (`test_full_repo_audit_validator_passes`).

## 9. Reconciliación y Verificación

- **M10** exige Σ clasificación = Σ severidad = total, y que el resumen declarado en el registro coincida con lo parseado.
- **M17/M18/M19** exigen coherencia registro ↔ informe (ids, severidad, clasificación).
- **M20** exige Σ filas de la Matriz de Controles = total declarado, y convierte cualquier FAIL con tracking/ADR en `REVALIDADO` (no `NUEVO`).
- Verificación forense previa confirmada por awk (independiente del modelo): 23 controles, 27 findings, 19 decisiones.

## 10. Knowledge Governance y Jerarquía

- Jerarquía de autoridad (mayor → menor) intacta: código/tests → contratos/BC-NN → ADRs → docs oficiales → doc interna/KB → literatura externa.
- **Libro ≠ contrato; Conocimiento ≠ evidencia de trading.** La cadena de adopción es obligatoria.
- El tooling **no decide política** (qué paper se adopta, severidad definitiva, gate de CI) — eso queda en Decisión Humana (§16).

## 11. ADRs y Enums Cerrados

- Enum de estados ADR: {PROPUESTO, ACEPTADO, REEMPLAZADO, OBSOLETO} (fuente: `ADR-template.md`).
- Verificado: 27 archivos `ADR-*.md` → 20 ACEPTADO / 6 PROPUESTO / 1 REEMPLAZADO; **0 inventados**.
- Coexisten 2 formatos, ambos soportados por M4: `**Estado:**` y `## Estado` (ADR-0022, ADR-0024).
- **M15** detecta estados inventados (`Superado`, `Resuelto`, "Aceptado implícitamente", etc.).

## 12. Severidad y Taxonomía

- Severidades cerradas: {CRITICAL, HIGH, MEDIUM, LOW, INFO} — M3.
- Clasificaciones cerradas: {NUEVO, REVALIDADO, REGRESIÓN, CERRADO, CONTRADICCIÓN, RECOMENDACIÓN, NO_VERIFICADO} — M2.
- Estados de control: {PASS, FAIL, PARTIAL, NO_VERIFICADO, INFRA_FAILURE} — M20.
- **Nota:** la severidad definitiva de un finding sigue siendo decisión humana; M3 garantiza solo que el token sea válido (no inventado).

## 13. Reproducibilidad

Bloque obligatorio (§S del protocolo):

```
REPRODUCIBILIDAD
- commit: bee9fb5a3917c32fcc81fcc81fa5177ce0e57283
- branch: main
- fecha: 2026-08-18
- protocolo: AUDIT_PROTOCOL v2.1
- agente/modelo: OpenCode / deepseek-v4-flash-free
- herramientas: pip-audit 2.10.1 · ruff 0.15.10 · mypy 1.19.1 ·
               bandit 1.9.4 · pytest 8.4.2 · yamllint 1.38.0
- comandos: §7 (canónicos)
- golden: PASS (41 passed)
- resultado validador: PASS — 16 findings, 20 reglas mecánicas, 12 warnings, 0 skipped
```

**Verificación de determinismo:** el validador ejecutado 2 veces produjo salidas byte-idénticas (`diff` vacío).

## 14. Tests y Validación

- **`tests/architecture/test_audit_validator.py`** (13 tests): FAIL en M1 (duplicado), M2 (clasificación inventada), M3 (severidad inventada), M4 (estado ADR inventado), M10 (contadores), M6 (ref tracking inexistente), M7 (sin evidence), M11 (golden mismatch), y PASS en golden con FAIL conocidos, M12, M20, dedup FAIL→REVALIDADO, y validación full repo.
- Gates: `ruff check` PASS, `ruff format` PASS, `mypy scripts/audit_validator.py` PASS, `bandit -ll` 0 issues de severidad, `pytest tests/architecture_linter/` 41 passed.
- **Adversarial:** registro con ID duplicado + clasificación/severidad inventadas + ref rota + evidencia vacía → 14 FAIL mecánicos detectados (exit 1).

## 15. Resultados y Evidencia

| Comando | Resultado |
|---|---|
| `uv run pytest tests/architecture/test_audit_validator.py -q --no-cov` | 13 passed |
| `uv run pytest tests/architecture_linter/ -q --no-cov` | 41 passed |
| `uv run ruff check .` (afectados) | All checks passed |
| `uv run mypy scripts/audit_validator.py` | Success |
| `uv run bandit -r scripts/audit_validator.py -ll` | No issues (0 Med/High) |
| `uv run python scripts/audit_validator.py --versions` | 6 herramientas fijadas |
| validador (registro+informe+golden) | PASS — 16 findings / 20 reglas / 12 WARN / 0 SKIP |
| determinismo (2 runs) | byte-idénticos |

## 16. Decisiones Humanas

| ID | Decisión | Tipo | Estado |
|---|---|---|---|
| D-AS-1 | Comando canónico pip-audit = el del CI (con 2 ignores risk-accept 2026-08-03) | BLOCKING | Adoptada (M13) |
| D-AS-2 | Golden con FAIL/PARTIAL es legítimo (no-regresión, no conformidad) | BLOCKING | Adoptada (M11) |
| D-AS-3 | M5 es WARN (un finding puede documentar un archivo ausente) | NON_BLOCKING | Adoptada |
| D-AS-4 | M17: extras F-SYS en informe = WARN (sistema ≠ producto) | NON_BLOCKING | Adoptada |
| D-AS-5 | Severidad/clasificación definitivas = decisión humana; tooling solo valida token | NON_BLOCKING | Propuesta |
| D-AS-6 | Tabla de severidad concreta (qué es CRITICAL vs HIGH en OCM) — pendiente de Decisión Humana explícita | NON_BLOCKING | **PENDIENTE** |
| D-AS-7 | Verificar si el linter debiera emitir WARN cuando golden declara PASS para deuda nueva | NON_BLOCKING | **PENDIENTE** |
| D-AS-8 | Auditorías previas sin bloque de reproducibilidad quedan marcadas como pre-2.1 | BLOCKING | Adoptada |

## 17. Riesgos y Mitigaciones

| Riesgo | Mitigación |
|---|---|
| LLM elude el validador | AGENTS.md lo hace OBLIGATORIO; test adversarial documentado |
| Enums cambian | M2/M3/M4/M15 fallan → exige actualizar tooling + protocolo a la vez |
| Golden desactualizado | M11 compara contra `test_golden.py` (SSOT único) |
| Comando divergente | M13 exige citar el canónico; §R fija la fuente de verdad (CI) |
| Severidad aún manual | D-AS-6 PENDIENTE: requiere Decisión Humana formal |
| Cambios concurrentes | HEAD verificado; solo se añadieron secciones (no se reescribió v2.0) |

## 18. Nivel de Determinismo Alcanzado

**Estado: `DETERMINISTIC` (con decisiones humanas pendientes documentadas).**

- Reglas mecánicas: **20/20** (M1..M20) — deterministas y ejecutables.
- Reglas de juicio LLM: **4/4** (L1..L4) — explícitas, acotadas, con evidencia subyacente.
- Comandos canónicos: **6/6** definidos; verificado pip-audit, linter, golden.
- Golden: definido y verificado.
- Reconciliación: matemática (M10/M17/M18/M19/M20).
- Tests: 13 + 41 (linter) pasando.
- Decisiones: 8 documentadas; 2 pendientes de Decisión Humana (D-AS-6, D-AS-7) — no bloquean el determinismo mecánico.

## 19. Verificación de Integridad

- HEAD: `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283` (intacto, branch `main`).
- `git status --short`: `M AGENTS.md` (+10 líneas, 9 concurrentes + 1 propia) y solo archivos nuevos en `docs/governance/`, `scripts/`, `tests/architecture/`, `docs/audits/`.
- NO se modificó: código de producto, `pyproject.toml`, `uv.lock`, CI, workflows, `tracking.yaml`, ADRs, dependencias.
- NO se ejecutó `git add/commit/push`.

## 20. Plan Maestro / Governance

- Este hardening cumple los criterios A–N del encargo: resultados deterministas (A–E), evidencia mecánica (F–I), herramientas canónicas (J–L), decisiones humanas documentadas (M–N).
- El tooling y el protocolo v2.1 quedan como **autoridad mecánica** dentro de la jerarquía (mayor que el juicio LLM, menor que los contratos BC-NN y ADRs).
- Cambios exactos producidos: AUDIT_PROTOCOL v2.1 (§Q–§U), AGENTS.md (+1 línea), `scripts/audit_validator.py` (nuevo), `tests/architecture/test_audit_validator.py` (nuevo), este informe.
- **Siguiente paso sugerido (Decisión Humana):** resolver D-AS-6 y D-AS-7; integrar el validador como gate opcional de CI de auditoría si se decide.