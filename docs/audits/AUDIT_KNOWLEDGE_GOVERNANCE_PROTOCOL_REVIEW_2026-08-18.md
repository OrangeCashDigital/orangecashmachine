# OCM — Diagnostic Review: Knowledge, Governance & State Protocol

**Fecha de revisión:** 2026-08-18  
**Commit Auditado:** `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283`  
**Branch:** `main`  
**Objetivo:** Auditar el modelo de fuentes de verdad, conocimiento, estado e historial en OCM, evaluando cómo se articulan los libros, papers, notas y bitácoras frente al protocolo formal de auditoría (`docs/governance/AUDIT_PROTOCOL.md`).

---

## 1. Executive Summary

OrangeCashMachine (OCM) cuenta con una base de conocimiento rica y diversificada (`docs/knowledge/`, `docs/architecture/`, `docs/audits/`, además de libros de referencia y zips de repositorios benchmark como Nautilus, Freqtrade y Hummingbot). Sin embargo, se detecta un riesgo conceptual importante: **un agente sin directrices estrictas podría confundir fundamentos teóricos o históricos (libros, papers, notas) con obligaciones normativas vigentes**, reportando falsos incumplimientos (findings) basados en diferencias de diseño o buenas prácticas genéricas.

Este informe presenta la auditoría y propuesta de formalización de un **Modelo Unificado de Fuentes de Verdad y Conocimiento** para OCM.

---

## 2. Clasificación y Auditoría de Fuentes en el Repositorio

Tras una inspección exhaustiva del repositorio, se categorizan las fuentes reales encontradas:

### A. FUENTES NORMATIVAS (Tier 1)
- Plan Maestro de Ingeniería (`docs/PLAN-Maestro-Ingenieria.md`)
- Governance Oficial (`docs/architecture/GOVERNANCE.md`)
- AGENTS.md y `docs/governance/AUDIT_PROTOCOL.md`

### B. FUENTES DE ESTADO (Tier 2)
- Tracking SSOT (`docs/plans/tracking.yaml`)
- Backlog priorizado y milestones de ingeniería

### C. FUENTES DE DECISIÓN (Tier 2)
- ADRs aprobados (`docs/architecture/decisions/`)

### D. FUENTES DE IMPLEMENTACIÓN (Tier 2)
- Código fuente ejecutable (`packages/`, `apps/`, `shared/`, `ocm/`)
- Suite de tests y contratos import-linter / architecture_linter
- Configuración (Hydra YAML) y CI/CD (`.github/workflows/ocm-ci.yml`)

### E. FUENTES DE CONOCIMIENTO (Tier 3)
- Notas técnicas (`docs/knowledge/notes/`)
- Documentación de dominios (`docs/DOMAIN.md`, `docs/architecture/feed-model.md`)
- Libros y referencias externas (`docs/Clean Architecture...pdf`)
- Repositorios de referencia externos (`nautilus_trader-develop.zip`, `freqtrade-develop.zip`, `hummingbot-master.zip`)

### F. FUENTES HISTÓRICAS / EVIDENCIA (Tier 4)
- Informes de auditorías anteriores (`docs/audits/`)
- Logs, bitácoras y análisis forenses históricos (`docs/architecture/recovered/`, `docs/architecture/logs/`)

---

## 3. Principios de Promoción y Precedencia Normativa

1. **Una Fuente de Conocimiento NO es una Obligación Normativa:** Un libro (ej. *Clean Architecture*), un paper o una nota de investigación (`docs/knowledge/`) proporcionan fundamentos conceptuales o hipótesis, pero **nunca** constituyen por sí mismos un contrato o requisito auditable en OCM.
2. **La Cadena de Promoción Requerida:**  
   `Fuente Externa / Conocimiento` $\rightarrow$ `Investigación / Hipótesis` $\rightarrow$ `Decisión del Proyecto` $\rightarrow$ `ADR / Governance / Plan Maestro` $\rightarrow$ `Obligación Auditable`.  
   Si esta cadena no está cerrada formalmente mediante un ADR o regla en `tracking.yaml`, cualquier divergencia detectada frente a dicha fuente debe clasificarse como **RECOMENDACIÓN** o **CONOCIMIENTO EXTERNO**, nunca como un incumplimiento (finding).
3. **Precedencia de Fuentes Vigentes frente a Históricas:** Ante contradicciones entre un informe de auditoría histórico y una ADR vigente o el estado actual en `tracking.yaml`, **prevalece siempre la fuente vigente** (ADR / Tracking / Código).

---

## 4. Gaps Detectados en el Protocolo Actual (`AUDIT_PROTOCOL.md`)

1. **Ambigüedad en el tratamiento de literatura y referencias externas:** El protocolo actual no explicita cómo un agente debe tratar los materiales de `docs/knowledge/` o libros externos.
2. **Riesgo de Falsos Positivos por Buenas Prácticas Genéricas:** Un agente podría intentar auditar contra frameworks externos (ej. SLSA Level 3 o ISO 27001) que OCM no ha adoptado formalmente en su Plan Maestro.

---

## 5. Recomendaciones y Propuesta de Extensión para `AUDIT_PROTOCOL.md`

Se recomienda incorporar al protocolo de gobernanza un apéndice normativo titulado **"Modelo de Autoridad y Tratamiento de Fuentes de Conocimiento"** que establezca:
- La distinción estricta entre fuentes normativas, de estado, de decisión, de implementación, de conocimiento e históricas.
- La prohibición absoluta de convertir fuentes de conocimiento (libros, papers, notas) en findings normativos sin mediación de un ADR o regla de tracking.

---

## 6. Decisiones Humanas Requeridas

- **D-KNOWLEDGE-GOV-1:** Aprobar la formalización de la jerarquía de fuentes de conocimiento e histórica en el protocolo de auditoría (`AUDIT_PROTOCOL.md`).

---

## 7. Evidencia Utilizada

- Inspección de directorios `docs/knowledge/`, `docs/architecture/`, `docs/audits/` (**[EVIDENCIA]**).
- Lectura de `docs/governance/AUDIT_PROTOCOL.md` y `AGENTS.md` (**[EVIDENCIA]**).

---

## 8. Verificación de Integridad

- Working tree preservado intacto. Único archivo creado: `docs/audits/AUDIT_KNOWLEDGE_GOVERNANCE_PROTOCOL_REVIEW_2026-08-18.md`.
