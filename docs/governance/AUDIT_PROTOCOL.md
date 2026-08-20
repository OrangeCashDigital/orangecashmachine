# OCM — Autonomous Agent Audit Protocol (`AUDIT_PROTOCOL.md`)

**Versión:** 2.2 (Audit Tooling Determinista — M1..M26)
**Autoridad Normativa:** Plan Maestro de Ingeniería (`docs/PLAN-Maestro-Ingenieria.md`) y Governance Oficial (`docs/architecture/GOVERNANCE.md`).  
**Aplicabilidad:** Obligatorio para cualquier agente de IA (`Gemini`, `Claude`, `Codex`, `DeepSeek`, `OpenCode`, etc.) que reciba instrucciones de auditar el repositorio OrangeCashMachine (OCM).

> **Cambio normativo (v2.1):** El sistema de auditoría añade un validador mecánico ejecutable — `scripts/audit_validator.py` (reglas M1..M26) — que debe ejecutarse ANTES de cualquier juicio de LLM (`MACHINE CHECKS FIRST. LLM JUDGMENT SECOND.`). Se definen comandos canónicos y un bloque de reproducibilidad obligatorio. Las reglas M1..M26 eliminan ambigüedad mecánica; el LLM queda restringido a las reglas L1..L4 (juicio). Ver §Q–§T.

---

## A. Scope (Alcance)
Este protocolo regula estrictamente la ejecución de auditorías técnicas, de cumplimiento, arquitectónicas y de gobernanza en el repositorio OCM. Su propósito es garantizar que cualquier agente opere de forma determinista, respetando el sistema normativo preexistente y evitando falsos positivos, duplicación de hallazgos y metodologías genéricas ajenas al proyecto.

## B. Normative Hierarchy & Knowledge Governance (Jerarquía Normativa y Fuentes de Verdad)
Una auditoría en OCM distingue estrictamente entre **Conocimiento**, **Decisión** y **Norma**. Una fuente externa de conocimiento (libros, papers, artículos, blogs, notas, research o estándares externos no adoptados) **no constituye por sí misma una norma del proyecto**.

La jerarquía de autoridad y clasificación de fuentes se rige por los siguientes niveles (de mayor a menor autoridad):

1. **LEVEL 4 — NORMATIVE GOVERNANCE (Normas Obligatorias):** Plan Maestro de Ingeniería (`docs/PLAN-Maestro-Ingenieria.md`), Governance Oficial (`docs/architecture/GOVERNANCE.md`), AGENTS.md y este `AUDIT_PROTOCOL.md`.
2. **LEVEL 3 — DECISIONES HUMANAS (Acuerdos Formalizados):** ADRs aprobados (`docs/architecture/decisions/`) y decisiones humanas formalizadas.
3. **LEVEL 2 — ESTADO E IMPLEMENTACIÓN (Estado Verificable):** Tracking SSOT (`docs/plans/tracking.yaml`), código ejecutable (`packages/`, `apps/`, `shared/`, `ocm/`), suite de tests, contratos de linter (`architecture_linter/`, `importlinter.toml`) y configuración CI/CD.
4. **LEVEL 1 — CONOCIMIENTO DEL PROYECTO (Fundamentos Internos):** Notas de ingeniería internas, investigaciones, documentos técnicos y análisis históricos (`docs/knowledge/`, `docs/architecture/recovered/`).
5. **LEVEL 0 — CONOCIMIENTO EXTERNO (Literatura y Referencias):** Libros (ej. *Clean Architecture*), papers, artículos, blogs, repositorios de referencia externos (zips de Hummingbot, Freqtrade, Nautilus) y estándares externos.

### Cadena de Adopción Requerida:
`KNOWLEDGE (Level 0/1)` $\rightarrow$ `PROPOSAL` $\rightarrow$ `HUMAN DECISION` $\rightarrow$ `ADR / GOVERNANCE / PLAN (Level 3/4)` $\rightarrow$ `TRACKED STATE (Level 2)` $\rightarrow$ `ENFORCEABLE CONTROL (Level 2)`.  
Si una recomendación de conocimiento externo no ha completado esta cadena mediante un ADR o regla de tracking, el agente **no puede** convertirla en un `FAIL` o finding de incumplimiento; debe clasificarla como `RECOMENDACIÓN` o `CONOCIMIENTO EXTERNO`.

## C. Discovery Order (Orden de Descubrimiento Obligatorio)
Antes de emitir cualquier juicio o finding, el agente debe inspeccionar en orden:
1. `AGENTS.md` (Punto de entrada general)
2. `docs/governance/AUDIT_PROTOCOL.md` (Protocolo de auditoría)
3. `docs/PLAN-Maestro-Ingenieria.md` (Hitos y fases de ingeniería)
4. `docs/architecture/GOVERNANCE.md` (Principios transversales)
5. `docs/plans/tracking.yaml` (Estado de hallazgos y deuda conocida)
6. `docs/architecture/decisions/` (ADRs relevantes)
7. `docs/audits/` (Informes históricos y de revalidación)
8. `architecture_linter/` y `architecture_linter/architecture_linter.toml`
9. `tests/architecture_linter/test_golden.py`
10. `.github/workflows/ocm-ci.yml`

## D. Baseline Construction (Construcción del Baseline)
Toda auditoría debe registrar explícitamente:
- Commit SHA (`git rev-parse HEAD`)
- Branch actual (`git branch --show-current`)
- Estado del working tree (`git status --short`)
- Baseline documental y referencias previas.

## E. Expected / Golden State (Estado Esperado y Golden)
- **GOLDEN PASS $\neq$ Arquitectura Conforme.** Un golden test en `tests/architecture_linter/test_golden.py` demuestra únicamente que el linter reproduce de forma determinista y estable el estado esperado (incluyendo deudas conocidas). No convalida que la arquitectura esté exenta de deuda técnica.

## F. Control States (Estados de Control)
Los controles evaluados deben clasificarse estrictamente en:
- `PASS`: Control verificado y conforme.
- `FAIL`: Desviación no gobernada respecto al estándar normativo del proyecto.
- `PARTIAL`: Cumplimiento parcial.
- `NO_VERIFICADO`: Imposible de comprobar por falta de acceso o entorno.
- `INFRA_FAILURE`: Fallo originado por infraestructura externa (Kafka, Redis, red, Docker) y no por defecto del producto.

## G. Finding Taxonomy (Taxonomía de Hallazgos)
Todo hallazgo debe clasificarse normativamente como exactamente uno de:
- `NUEVO`: Incumplimiento de una obligación vigente no registrado previamente.
- `REVALIDADO`: Deuda técnica conocida, documentada en tracking/ADRs y confirmada en la auditoría actual (`CONTROL FAIL $\neq$ FINDING NUEVO`).
- `REGRESIÓN`: Problema previamente resuelto que reaparece.
- `CERRADO`: Hallazgo histórico que ya no reproduce.
- `CONTRADICCIÓN`: Desalineación entre código, ADRs, tracking, Governance o Plan Maestro.
- `RECOMENDACIÓN`: Propuesta u observación derivada de conocimiento externo sin incumplimiento normativo directo.
- `NO_VERIFICADO`: Evidencia insuficiente para clasificar.

## H. Historical Reconciliation & Deduplication (Reconciliación y Deduplicación)
**REGLA FUNDAMENTAL:** `CONTROL FAIL $\neq$ FINDING NUEVO`.  
Antes de declarar un finding como `NUEVO`, el agente está obligado a contrastarlo contra:
1. `tracking.yaml`
2. ADRs (`docs/architecture/decisions/`)
3. Informes históricos en `docs/audits/`
4. Plan Maestro y contratos arquitectónicos.
Si el problema ya existe, debe clasificarse obligatoriamente como `REVALIDADO` vinculando su ID de tracking o ADR original.

## I. Traceability & Evidence (Trazabilidad y Evidencia)
Todo finding debe vincularse mediante la cadena completa:  
`Hallazgo $\rightarrow$ Evidencia (Comando/Archivo/Línea) $\rightarrow$ Control $\rightarrow$ Requisito / Obligación $\rightarrow$ Fuente Normativa $\rightarrow$ Tracking $\rightarrow$ ADR $\rightarrow$ Implementación`.  
Si algún eslabón no existe o no puede probarse, debe marcarse como `NOT_TRACED` o `NO_VERIFICADO`. No se permite rellenar huecos mediante inferencias.

## J. Severity Model (Modelo de Severidad)
La severidad se clasifica en: `CRITICAL`, `HIGH`, `MEDIUM`, `LOW`, `INFO`.  
Debe derivarse del impacto real de negocio y trading, separando la **Severidad Técnica** de la **Aceptación de Riesgo de Governance** (un riesgo conocido y documentado en ADR mantiene su severidad técnica pero su estado normativo es `GOVERNED`).

## K. Read-Only Audit Boundary (Límite Read-Only)
Durante una auditoría, el agente tiene **prohibido absoluto** modificar:
- Código fuente (`packages/`, `apps/`, `shared/`, `ocm/`, `infrastructure/`, `architecture_linter/`)
- Tests (`tests/`)
- Configuración de CI/CD (`.github/workflows/`)
- ADRs (`docs/architecture/decisions/`)
- tracking (`docs/plans/tracking.yaml`)
- Governance y Plan Maestro
- Dependencias (`pyproject.toml`, `uv.lock`)

La única escritura permitida se restringe a crear o actualizar el informe canónico en `docs/audits/`.  
Queda prohibido ejecutar `git add`, `git commit` o `git push`.

## L. Architecture-Linter & Golden Semantics
- Las reglas de `architecture_linter` (ARCH-001 a ARCH-010) detectan invariantes semánticos. Un resultado `FAIL` en una regla con soporte en ADR/tracking constituye un hallazgo `REVALIDADO`, no un defecto `NUEVO`.

## M. Contradiction Handling (Manejo de Contradicciones)
- Una fuente de menor autoridad (ej. nota histórica o literatura externa) no puede invalidar una fuente de mayor autoridad (ej. ADR vigente o Governance).
- Si el código operativo contradice un ADR vigente o tracking, se clasifica como `CONTRADICCIÓN`. El agente nunca debe modificar el código ni el tracking por iniciativa propia para resolver la contradicción; debe registrar una **Decisión Humana** pendiente.

## N. Human Decisions (Decisiones Humanas)
Las decisiones que incumben exclusivamente al propietario del proyecto deben registrarse bajo la estructura:
- `D-XXX`: Pregunta / Opciones / Consecuencias / Evidencia / Finding relacionado.

## O. Audit Report Requirements (Requisitos del Informe Canónico)
Toda auditoría debe generar un informe estructurado en `docs/audits/AUDIT_OCM_FORENSIC_COMPLIANCE_YYYY-MM-DD.md` conteniendo:
- Executive Summary, Scope, Methodology, Governance Baseline.
- Detailed Finding Cards (con Taxonomía obligatoria y Ficha Estricta).
- Control Matrix y Risk Matrix.
- Decisiones Humanas y Roadmap.
- Reconciliación matemática exacta de contadores.

## P. Final Verification Checklist (Lista de Verificación de Cierre)
Antes de declarar `AUDITORÍA TERMINADA`, el agente debe verificar:
1. ¿Se descubrió el Plan Maestro y GOVERNANCE.md?
2. ¿Se contrastó cada FAIL contra `tracking.yaml` y ADRs?
3. ¿Se aplicó la taxonomía estricta (`NUEVO`, `REVALIDADO`, etc.)?
4. ¿Se reconciliaron exactamente los contadores de findings y controles?
5. ¿Se preservó la integridad del working tree (solo `docs/audits/` modificado)?
6. ¿Ejecutó `uv run python scripts/audit_validator.py` y obtuvo PASS (o errores resueltos)?

## Q. Rule Classification (Clasificación de Reglas: Máquina vs LLM)

### M1..M26 — Reglas MECÁNICAS (validación ejecutable en `scripts/audit_validator.py`)

| Regla | Descripción | Validación |
|---|---|---|
| M1 | IDs únicos | registro sin IDs duplicados |
| M2 | Clasificación dentro de enum cerrado | `NUEVO/REVALIDADO/REGRESIÓN/CERRADO/CONTRADICCIÓN/RECOMENDACIÓN/NO_VERIFICADO` |
| M3 | Severidad dentro de enum cerrado | `CRITICAL/HIGH/MEDIUM/LOW/INFO` |
| M4 | Estados ADR válidos | base ∈ `Propuesto/Aceptado/Reemplazado/Obsoleto` |
| M5 | Existencia de referencias | archivos citados existen (WARN si el hallazgo documenta un archivo ausente) |
| M6 | Findings ↔ tracking | refs `B-*`/`R*` existen en `tracking.yaml` |
| M7 | Finding ↔ evidence | toda ficha con `Evidence` no vacía |
| M8 | Finding ↔ control | toda ficha declara `Control` |
| M9 | Finding ↔ ADR | refs `ADR-NNNN` existen como archivos |
| M10 | Reconciliación matemática | Σ clasificación = Σ severidad = total; resumen declarado = parseado |
| M11 | Golden state | `GOLDEN_EXPECTED` == resultado real del linter; golden con FAIL/PARTIAL es legítimo (no-regresión) |
| M12 | Estructura mínima del informe | secciones obligatorias presentes |
| M13 | Comandos canónicos | informe cita el comando canónico de controles críticos |
| M14 | Versiones de herramientas | informe declara versiones (reproducibilidad) |
| M15 | Estados inventados | `Superado/Resuelto/...` → FAIL |
| M16 | Duplicados por ID | mismo ID en más de una ficha |
| M17 | Informe ↔ registro | findings del registro presentes en el informe |
| M18 | Consistencia severidad | misma severidad en informe y registro |
| M19 | Consistencia clasificación | misma clasificación en informe y registro |
| M20 | Control counts | Σ filas de la matriz de controles = total declarado; FAIL no genera NUEVO sin dedup |
| M21 | Naming canónico en docs/audits | todo `*.md` en `docs/audits/` cumple `AUDIT_OCM_<slug>_<date>[_<NN>].md` o `OCM_AUDIT_FINDINGS_<date>_<slug>[_<NN>].md` |
| M22 | Policy Registry: tests según `mechanism_type` | `guard_script`/`import_linter` exigen `tests.positive`+`tests.negative` resolubles (AST); `tool_gate` exige `ci.job`+`ci.command`; `absence_gate` exige `evidence`; con waiver → WARN, sin waiver → FAIL |
| M23 | Policy Registry: `enforcement` + CI | enum `blocking\|warning\|informational`; `blocking` exige gate de CI (`ci.job`+`ci.command`), salvo `absence_gate` |
| M24 | Policy Registry: regla muerta | `DEPRECATED` exige `absence_gate` y prohibe waiver; `status` ∈ `ACTIVE\|DEPRECATED` |
| M25 | Policy Registry: semántica de waiver | `allowed:true` + `expires` ISO + `motivo` + `adr` autorizante + `ticket` en `tracking.yaml`; expirado → FAIL; vigente → WARN |
| M26 | Policy Registry: ADRs existentes | ADR referenciado en registry tiene archivo en `docs/architecture/decisions/` |

**Ejecución:** `uv run python scripts/audit_validator.py [--register ...] [--report ...] [--golden ...]` — exit 0 = PASS, 1 = FAIL, 2 = error de ejecución.

### L1..L4 — Reglas de JUICIO (LLM humano-supervisado)

| Regla | Ámbito |
|---|---|
| L1 | Misma causa raíz / equivalencia semántica entre hallazgos |
| L2 | Impacto de negocio y trading (contexto) |
| L3 | Interpretación arquitectónica y contradicciones sutiles |
| L4 | Priorización / roadmap / aceptación de conocimiento externo como obligación |

**Frontera explícita:** el tooling recopila evidencia y aplica reglas cerradas; el LLM decide solo sobre L1–L4. Nunca al revés.

## R. Canonical Commands (Comandos Canónicos)

La fuente de verdad para cada comando es el **CI real** (`ocm-ci.yml` y workflows asociados) y la **Governance** — no la preferencia del agente. El comando canónico de cada control crítico:

| Control | Comando canónico | Exit esperado | Interpretación |
|---|---|---|---|
| ARCH_CONTRACTS | `uv run lint-imports --config architecture_linter/importlinter.toml` | 0 | broken = merge bloqueado |
| ENGINEERING_HEALTH | `uv run python scripts/engineering_health_check.py` | 0 | F2.0: Plan↔tracking↔ADR↔contratos↔CI |
| ARCH_LINTER | `uv run python -m architecture_linter --root . --json` | 1 si FAIL/PARTIAL | detector; golden fija estado esperado |
| GOLDEN | `uv run pytest tests/architecture_linter/test_golden.py -q --no-cov` | 0 | no-regresión |
| DEPENDENCY_AUDIT | `uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325` | 0 | **comando canónico = el del CI**; sin ignores → 6 vulns ≠ finding distinto |
| YAMLLINT | `uvx yamllint -c .yamllint .` | 0 | 0 errores YAML |

**Divergencia pip-audit resuelta (4 vs 6):** el comando normativo es el de `.github/workflows/ocm-ci.yml` (incluye los 2 ignores del risk-accept documentado el 2026-08-03). Cualquier informe que use otro comando debe citar cuál y por qué; si la decisión de ampliar/reducir ignores requiere aprobación, se registra como Decisión Humana.

## S. Reproducibility Block (Bloque de Reproducibilidad Obligatorio)

Todo informe canónico debe declarar un bloque de reproducibilidad:

```
REPRODUCIBILIDAD
- commit: <sha>
- branch: <branch>
- fecha: <fecha>
- protocolo: AUDIT_PROTOCOL v2.1
- agente/modelo: <modelo>
- herramientas: <versiones via `uv run python scripts/audit_validator.py --versions`>
- comandos: <lista de comandos canónicos usados>
- golden: PASS/FAIL
- resultado: PASS/FAIL del validador
```

Objetivo: `mismo commit + mismo protocolo + mismas herramientas + mismos comandos ⇒ resultados equivalentes` entre agentes.

## T. Audit Tooling (Tooling de Auditoría)

- **`scripts/audit_validator.py`** — validador mecánico M1..M26 (stdlib + pyyaml). Referencia de implementación: `scripts/engineering_health_check.py`.
- **`tests/architecture/test_audit_validator.py`** — 13 tests que demuestran FAIL en cada violación y PASS en estados válidos (golden con FAIL/PARTIAL incluido).
- El tooling NO decide política (severidad definitiva, qué paper se adopta, gate de CI). Eso queda para Decisión Humana (§N).

## U. Changelog (Historial de Versiones)

| Versión | Fecha | Motivo | Impacto |
|---|---|---|---|
| 1.0 | 2026-08-18T21:53 | Creación del protocolo de auditoría de agentes | Base normativa |
| 2.0 | 2026-08-18T22:06 | Knowledge Governance Integration (§B: jerarquía 5 niveles + cadena de adopción) | Jerarquía y taxonomía cerradas |
| 2.1 | 2026-08-18 | Audit Tooling Determinista: validador M1..M20 (§Q), comandos canónicos (§R), bloque de reproducibilidad (§S), tooling (§T) | Reduce divergencia entre modelos; elimina ambigüedad mecánica |
| 2.2 | 2026-08-19 | Policy Registry (ADR-0031): reglas M22..M26 (§Q) — tests por mechanism_type, enforcement+CI, regla muerta, semántica de waiver, ADR huérfano | Cierra gaps de enforcement del Policy Registry; renumeración por M21 ocupado |
