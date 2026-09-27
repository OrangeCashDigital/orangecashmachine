# ADR-0046: Decision Records & Materialization (H3)

**Estado:** Propuesto
**Decisión humana asociada:** S6-CA-H3 = H3-CUSTOM/HYBRID — ADOPTADA 2026-09-27
**Materialización:** REALIZADA VÍA H3-M1 — PENDIENTE DE HUMAN REVIEW
**Fecha:** 2026-09-27
**Bounded context(s) afectado(s):** ocm (plataforma), governance, CI/CD (sin cambios técnicos en esta fase)

## Contexto

El Human Decision Gate H3 fue completado: el humano adoptó explícitamente el
contrato H3-CUSTOM/HYBRID (cláusulas H3-H1..H3-H12) y autorizó de forma
exclusiva GATE H3-M1 para materializar esa decisión.

Estado de la cadena de gobernanza en el momento de la materialización:

- H1 = DECIDED — CUSTOM/HYBRID AUTHORITY MODEL (MODEL D + B + C).
- H2 = DECIDED — H2-E CUSTOM (artefacto: ADR-0045, Estado Propuesto,
  Materialización PENDIENTE DE HUMAN REVIEW).
- H3 = DECIDED — HUMAN (S6-CA-H3, 2026-09-27).
- H4 = NOT DECIDED / NOT STARTED.
- H5 = NOT DECIDED / NOT STARTED.

Principios vinculantes de esta decisión:

- DECIDED ≠ MATERIALIZED ≠ ACCEPTED.
- COMMIT ≠ HUMAN DECISION.
- MATERIALIZATION ≠ AUTHORITY.
- EVIDENCE ≠ SSOT.
- CI ≠ AUTHORITY.
- IMPLEMENTATION ≠ ARCHITECTURAL APPROVAL.
- Ningún agente puede inventar precedencia ni autoridad por inferencia.

Este ADR es el registro normativo (Decision Record) de S6-CA-H3. Materializa
la decisión humana; no la sustituye, no la amplía ni la reinterpreta.

## Decisión

La decisión humana adopta el siguiente contrato H3-CUSTOM/HYBRID:

### H3-H1 — Identificación de decisiones normativas

Toda decisión humana que establezca, modifique o retire una regla, autoridad,
contrato de gobernanza o frontera normativa de OCM debe poder ser identificada
y trazada cuando tenga efecto normativo.

### H3-H2 — No todo requiere un ADR independiente

H3 no obliga a crear un ADR separado para cada decisión. El mecanismo de
registro será proporcional a la naturaleza y alcance de la decisión y podrá
consistir, según corresponda, en:

- ADR;
- registro de decisión identificado;
- actualización formal de un artefacto normativo existente;
- otro mecanismo expresamente aprobado.

### H3-H3 — Identidad de decisión

Cuando una decisión requiera trazabilidad individual podrá utilizar un
identificador estable, por ejemplo D-XXX. El identificador es un mecanismo de
trazabilidad y no constituye autoridad por sí mismo.

### H3-H4 — Definición de materialización

Una decisión está materializada cuando existe un registro persistente y
verificable de ella dentro del sistema de gobernanza de OCM. Una conversación,
mensaje temporal o intención no materializada no constituye por sí sola
materialización.

### H3-H5 — Commit no crea autoridad

Un commit puede demostrar persistencia técnica de un artefacto, pero no crea,
concede ni modifica autoridad normativa.

### H3-H6 — Accepted no equivale automáticamente a autoridad

El estado Accepted de un ADR no constituye por sí mismo una regla universal de
precedencia o autoridad. Solo tendrá dicho efecto cuando exista una regla
normativa aprobada que establezca explícitamente esa relación, dominio y
alcance.

### H3-H7 — Autoridad explícita

La autoridad normativa debe estar establecida explícitamente mediante una regla
aprobada. No debe inferirse autoridad a partir de:

- existencia del archivo;
- fecha;
- orden de creación;
- nombre del archivo;
- commit;
- estado Accepted;
- Registry;
- CI;
- Evidence;
- implementación existente;
- referencia cruzada;
- mención de un agente.

### H3-H8 — SSOT especializados

Se mantiene el modelo H1 de Specialized SSOT Authority. No se crea un SSOT
universal que gobierne automáticamente todos los dominios de OCM.

### H3-H9 — Registry no es autoridad universal

registry.yaml mantiene autoridad únicamente dentro de los dominios y usos
formalmente establecidos. Su existencia no le concede autoridad automática
sobre ADRs, decisiones humanas, Agent Governance, Evidence u otros dominios.

### H3-H10 — Evidence es atestación derivada

Evidence constituye una atestación derivada. Puede demostrar que determinados
hechos o verificaciones fueron producidos, pero no crea autoridad normativa ni
sustituye al SSOT correspondiente.

### H3-H11 — CI y enforcement no crean autoridad

CI, linters, validators y mecanismos de enforcement verifican o hacen cumplir
reglas existentes. No pueden crear autoridad normativa por sí mismos.

### H3-H12 — Prohibición de autoridad por inferencia

Ningún agente o proceso automático puede concluir que un artefacto posee
autoridad simplemente porque:

- existe;
- es más reciente;
- está materializado;
- está en Git;
- está Accepted;
- está referenciado;
- está implementado;
- pasa CI;
- aparece en Evidence.

Cuando no exista una regla aprobada que determine inequívocamente la
autoridad, debe aplicarse H2-E:

FAIL-CLOSED → HUMAN DECISION GATE.

## Relación con H1

H1 (decisión humana S6-CA-H1, 2026-09-26) estableció el modelo de autoridad
CUSTOM / HYBRID con tres dimensiones:

- MODEL D — layered normative authority / jerarquía normativa refinada.
- MODEL B — specialized SSOT por dominio; no existe un SSOT universal de OCM.
- MODEL C — fronteras de gobernanza CODE / AGENT / EXTERNAL.

H3 opera dentro de ese marco. Este ADR no reinterpreta ni reemplaza H1 y no
constituye una nueva decisión H1.

## Relación con H2-E y ADR-0045

H3 no reemplaza ni modifica H2-E. ADR-0045 es el artefacto que materializa el
mecanismo H2-E. Ante un conflicto se aplica la cadena:

CONFLICT → ¿regla aprobada? (NO) → ¿autoridad de dominio formalmente
determinada? (NO) → FAIL-CLOSED → HUMAN DECISION GATE.

Cuando no exista una regla aprobada que determine inequívocamente la autoridad,
el comportamiento es FAIL-CLOSED y la resolución pasa al humano. El agente no
puede resolver unilateralmente una situación indeterminada.

Este ADR no modifica ADR-0045 ni H2-E.

## Alcance y límites

Esta decisión H3 NO decide:

- el significado definitivo de `adr:null`;
- precedencia Registry ↔ ADR;
- destino de tracking.reglas;
- Evidence ↔ CI;
- M26;
- B4/B5;
- Fact Model;
- H4;
- H5;
- enforcement de Agent Governance;
- TCB;
- cambios de CI;
- migraciones;
- cambios de arquitectura del software;
- nuevas reglas de precedencia no expresamente incluidas en H3-H1..H3-H12.

Ninguna de estas cuestiones se resuelve ni se infiere a partir de la adopción
de H3. Las cuestiones D-002 (estado/sincronización) y D-006 (staged-port
enforcement) permanecen pendientes y no se tratan en esta fase. Este ADR no
declara que ADR-0045 tenga autoridad universal, que Accepted cree autoridad,
que registry sea SSOT universal, ni que Evidence, CI, implementación o commit
creen autoridad.

## Referencias

- Decisor humano: S6-CA-H3 = H3-CUSTOM/HYBRID — ADOPTADA 2026-09-27 (GATE H3-M1).
- Decisión humana S6-CA-H1 = CUSTOM/HYBRID (D+B+C) — 2026-09-26.
- Decisión humana S6-CA-H2 = H2-E CUSTOM — 2026-09-26.
- ADR-0045: Authority Conflict Resolution — Hybrid (H2-E Custom) — materializa el mecanismo H2-E.
- ADR-0031: Policy Registry YAML.
- ADR-0032: AI Agent Governance.
- AUDIT_PROTOCOL.md: cadena de adopción KNOWLEDGE → ... → ENFORCEABLE CONTROL; formato D-XXX.
- GOVERNANCE.md: SSOT de reglas de proceso; cuándo un cambio requiere ADR.