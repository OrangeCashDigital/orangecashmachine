# OCM — Forensic Audit of AGENTS.md Protocol & Governance Alignment

**Fecha de auditoría:** 2026-08-18  
**Commit Auditado:** `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283`  
**Branch:** `main`  
**Archivo Evaluado:** `AGENTS.md` (raíz del repositorio).

---

## 1. Executive Summary

El archivo `AGENTS.md` actual en OCM es un excelente manual operativo orientado a la construcción, testeo local, migración de datos (pandas a polars) y estructura de bounded contexts para desarrolladores humanos e IAs. Sin embargo, tras una auditoría forense rigurosa evaluando su capacidad para gobernar un encargo de **auditoría automatizada** (ej. "audita la repo"), se concluye que **AGENTS.md es insuficiente y carece de un protocolo formal de auditoría**. 

Si un agente recibe hoy la orden de auditar OCM basándose exclusivamente en `AGENTS.md`, operará con metodología genérica, tratará los fallos del `architecture_linter` como bugs críticos nuevos, ignorará `tracking.yaml` y los ADRs, y violará el principio read-only, poniendo en riesgo la integridad de la gobernanza del proyecto.

---

## 2. Fuentes Normativas Descubiertas (Jerarquía de Autoridad)

1. **Plan Maestro de Ingeniería:** `docs/PLAN-Maestro-Ingenieria.md` (Tier 1)
2. **Governance Oficial:** `docs/architecture/GOVERNANCE.md` (Tier 1)
3. **Tracking SSOT:** `docs/plans/tracking.yaml` v2 (Tier 2)
4. **ADRs:** `docs/architecture/decisions/` (Tier 2)
5. **Architecture Linter & Contracts:** `architecture_linter/` y `architecture_linter/importlinter.toml` (Tier 2)
6. **Tests y Golden States:** `tests/architecture_linter/test_golden.py` (Tier 2)
7. **CI/CD:** `.github/workflows/ocm-ci.yml` (Tier 2)

---

## 3. Evaluación de los 12 Puntos del Protocolo en AGENTS.md

1. **Qué reglas ya existen:** Comandos de CI, reglas de import-linter, dirección de dependencias, estructura de bounded contexts, migración polars.
2. **Qué reglas faltan:** Toda la directriz metodológica para ejecutar auditorías (reconciliación, clasificación taxonómica, trazabilidad estricta, modo read-only de auditoría).
3. **Qué reglas son ambiguas:** La referencia a la arquitectura y linter carece de distinción entre "deuda conocida y gobernada" y "regresión nueva".
4. **Permite metodología propia:** **SÍ**. Al no definir un protocolo de auditoría, deja libertad al agente para aplicar checklists externas (OWASP, ISO, etc.).
5. **Permite duplicar findings:** **SÍ**. No obliga a buscar en `tracking.yaml` ni en auditorías previas antes de reportar un problema.
6. **No obliga a consultar tracking/ADRs/Plan Maestro:** **SÍ**. Aunque menciona ADRs ocasionalmente, no establece un orden de descubrimiento obligatorio.
7. **No establece baseline:** **SÍ**. Omite la obligación de fijar el commit, rama y working tree.
8. **No establece reconciliación:** **SÍ**. No define el proceso para reconciliar contadores ni clasificaciones taxonómicas.
9. **No establece read-only:** **SÍ**. No prohíbe explícitamente modificar código, tests o tracking durante una auditoría.
10. **No establece clasificación de findings:** **SÍ**. No define las categorías (`NUEVO`, `REVALIDADO`, `REGRESIÓN`, `CERRADO`, `CONTRADICCIÓN`, `RECOMENDACIÓN`, `NO_VERIFICADO`).
11. **No establece trazabilidad:** **SÍ**. No exige la cadena finding → tracking → ADR → CI.
12. **Permite confundir FAIL de herramienta con defecto nuevo:** **SÍ**. Un agente sin el protocolo governance-first reportará los 7 `FAIL` del `architecture_linter` como fallos críticos de código ignorando que son deuda gobernada.

---

## 4. Gaps Concretos y Severidad

| GAP ID | Descripción del Gap | Severidad | Impacto |
|---|---|---|---|
| G-01 | Ausencia de Protocolo de Auditoría y Modo Read-Only | HIGH | El agente podría modificar código o tracking durante una auditoría. |
| G-02 | Omisión de Taxonomía de Reconciliación (`NUEVO`, `REVALIDADO`, etc.) | MEDIUM | Duplicación masiva de findings ya registrados en `tracking.yaml`. |
| G-03 | Falta de Jerarquía de Descubrimiento de Gobernanza | MEDIUM | El agente prioriza herramientas externas o código fuente sobre el Plan Maestro y ADRs. |
| G-04 | Confusión entre `FAIL` de linter y Defecto Nuevo | HIGH | Falsas alarmas sobre deudas arquitectónicas explícitamente aceptadas en ADRs. |

---

## 5. Recomendaciones de Diseño y Estructura Propuesta

Para resolver los gaps detectados sin sobrecargar `AGENTS.md` (manteniéndolo ágil como guía de desarrollo), se recomienda añadir una sección formal o un documento vinculado: `docs/architecture/AUDIT_PROTOCOL.md` (o sección **"Autonomous Audit & Compliance Protocol"** en `AGENTS.md`).

### Estructura Propuesta para el Protocolo de Agentes:
1. **Mandate Governance-First:** Descubrimiento obligatorio en orden (Plan Maestro → Governance → Tracking → ADRs).
2. **Taxonomía Obligatoria:** Uso exclusivo de `NUEVO`, `REVALIDADO`, `REGRESIÓN`, `CERRADO`, `CONTRADICCIÓN`, `RECOMENDACIÓN`, `NO_VERIFICADO`.
3. **Principio Read-Only:** Bloqueo de escritura en código, tests, CI y tracking durante encargos de auditoría; escritura permitida únicamente en `docs/audits/`.
4. **Reconciliación de Deuda:** Obligación de contrastar cualquier `FAIL` de linter contra `tracking.yaml` y ADRs antes de catalogarlo como finding nuevo.

---

## 6. Decisiones Humanas Requeridas

- **D-AGENTS-PROTOCOL-1:** Aprobar la creación y adopción formal de un `AUDIT_PROTOCOL.md` (o sección en `AGENTS.md`) que regule de manera determinista el comportamiento de los agentes ante encargos de auditoría.

---

## 7. Evidencia Utilizada

- Lectura y análisis de `/home/orangemusic/trading/orangecashmachine/AGENTS.md` (**[EVIDENCIA]**).
- Inspección de los documentos de governance y tracking del repositorio (**[EVIDENCIA]**).

---

## 8. Verificación de Integridad

- Working tree preservado intacto. Ningún archivo modificado fuera del directorio autorizado `docs/audits/`.
