# ADR-0054: Decisiones de contexto de ingeniería persistente (H-CTX-1/2 + MD-A1..A5)

**Estado:** Propuesto
**Decisión humana asociada:** HUMAN DECISION CLOSURE + MATERIALIZATION GATE — 2026-10-08:
H-CTX-1 = OPTION A; H-CTX-2 = OPTION B; MD-A1 = fail-closed `adr:null`;
MD-A2 = precedencia formal de autoridad; MD-A3 = separación Evidencia/Provenance/Memoria/Autoridad/Decisión;
MD-A4 = base arquitectónica D20b confirmada (implementación NO autorizada);
MD-A5 = MVES + Human Authority, Agent Engineering Autonomy.
**Materialización:** REALIZADA VÍA HUMAN DECISION CLOSURE GATE — PENDIENTE DE HUMAN REVIEW
**Fecha:** 2026-10-08
**Bounded context(s) afectado(s):** ocm (plataforma), governance (sin cambios técnicos en esta fase)

## Contexto

Los gates de investigación de 2026-10-08 (*Persistent Engineering Context &
Codebase Memory Research Gate*, CTX-1, CTX-DECIDE y consolidación) establecieron
que OCM carece de un mecanismo para que un agente autorizado recupere el
contexto de ingeniería suficiente (decisiones, rationale, invariantes,
restricciones, fallos conocidos, rechazos, estado histórico) sin redescubrir
decisiones ya asentadas, y que cualquier memoria persistente debe conservar la
separación **CONTEXT ≠ AUTHORITY**, **MEMORY ≠ AUTHORITY**,
**RECOMMENDATION ≠ DECISION**, **AGENT JUDGEMENT ≠ HUMAN ARCHITECTURAL AUTHORITY**.

El Human Owner revisó la evidencia y cerró las siete decisiones pendientes con
base en el principio rector **Human Authority, Agent Engineering Autonomy** y
una lectura *best-of-world* = arquitectura mínima suficiente basada en la
evidencia más fuerte disponible, *fail-closed* ante ambigüedad de autoridad,
reversible donde sea posible, sin infraestructura prematura.

Evidencia de partida (verificable en repo): `policies/registry.yaml` (R2..R8
con `adr: null`), ADR-0045 (CX-14, CX-15/CX-16 enrutados a gate humano),
ADR-0046 (H3-H9, H3-H10, H3-H12), ADR-0049 (bloque de estado D20b),
`docs/audits/D19_HUMAN_DECISION_PACK_2026-09-30.md` y
`docs/audits/AUDIT_OCM_high-impact-improvements_2026-10-07.md` (F-OCM-01/D-OCM-01).

## Decisiones humanas cerradas

### H-CTX-1 — OPTION A: modelo de contexto mínimo suficiente

Se adopta el modelo **documentary / minimum sufficient engineering context**,
no infraestructura pesada de memoria. El contexto debe representar, como
mínimo: identidad del proyecto; contexto arquitectónico; decisiones; rationale;
requisitos; invariantes; restricciones; fallos conocidos; enfoques rechazados;
preguntas abiertas; hallazgos de research; relaciones de código; historial Git;
handoff de agente/sesión.

Todo ítem de memoria con apariencia autoritativa debe llevar provenance y
clasificación semántica: `FACT`, `DECISION`, `REQUIREMENT`, `CONSTRAINT`,
`RECOMMENDATION`, `INFERENCE`, `OPEN QUESTION`, `REJECTED OPTION`,
`HISTORICAL STATE`.

Regla arquitectónica: **MEMORY ≠ AUTHORITY**, **CONTEXT ≠ AUTHORITY**,
**RECOMMENDATION ≠ DECISION**. La fuente autoritativa sigue siendo el artefacto
de autoridad OCM correspondiente (ADRs, contratos, decisiones humanas).

*Clasificación: HUMAN DECISION — ACCEPTED RECOMMENDATION (opción A tal como
investigada).*

### H-CTX-2 — OPTION B: política de escritura controlada y auditable

La ingeniería persistente puede actualizarse por un mecanismo controlado, pero
la memoria nunca se convierte en autoridad normativa. La política distingue
**AUTHORITATIVE SOURCE** vs **DERIVED CONTEXT** vs **RESEARCH** vs
**INFERENCE** vs **HISTORICAL STATE**. Un ítem de memoria nunca se promueve
silenciosamente a requisito arquitectónico.

Si la memoria entra en conflicto con un ADR, contrato o decisión humana
explícita: **prevalece la fuente autoritativa**; el conflicto debe **ponerse de manifiesto
en lugar de reconciliarse en silencio** (FAIL-CLOSED → reporte →
decisión humana cuando corresponda). Las actualizaciones deben ser
atribuibles, con provenance, revisables, recuperables, auditables y reversibles
cuando sea práctico.

Esta decisión **no** autoriza implementar el sistema de memoria.

*Clasificación: HUMAN DECISION — ACCEPTED RECOMMENDATION (opción B tal como
investigada).*

### MD-A1 — semántica fail-closed de `adr:null`

`adr:null` significa que la regla **no tiene autoridad ADR establecida** y por
tanto **no debe interpretarse como portadora de autoridad arquitectónica
autónoma**. La ausencia de referencia ADR **no crea autoridad**.

Para las siete reglas afectadas (`R2`..`R8` de `policies/registry.yaml`):

- preservar las reglas actuales;
- no inventar referencias ADR ni poblar `adr:null`;
- no modificar `docs/plans/tracking.yaml`;
- no inferir autoridad por implementación ni por convención histórica.

Donde una regla requiera autoridad normativa y no exista fuente válida:
**FAIL-CLOSED → HUMAN DECISION REQUIRED**. Los detalles técnicos posteriores
pueden resolverse por evidencia de ingeniería una vez establecido el límite de
autoridad.

*Clasificación: HUMAN DECISION — ACCEPTED RECOMMENDATION (normalizado desde la
opción investigada (b)+(c)).*

### MD-A2 — precedencia Registry ↔ ADR

Modelo de autoridad adoptado:

```
EXPLICIT HUMAN DECISION
        ↓
FORMAL ADR / CONTRACT
        ↓
GOVERNANCE / AUTHORIZED POLICY
        ↓
REGISTRY / DERIVED ENGINEERING CONTEXT
        ↓
IMPLEMENTATION
```

- Un Registry **no puede** prevalecer sobre un ADR aplicable.
- Conflicto Registry vs ADR: **FAIL-CLOSED → REPORT CONFLICT → NO AUTO-RESOLVE
  → HUMAN DECISION REQUIRED**.
- El Registry puede operacionalizar o dar contexto estructurado a una decisión
  arquitectónica establecida; **no puede redefinirla en silencio**.
- Artefactos de tracking/planificación no prevalecen en silencio sobre la
  autoridad arquitectónica formal: **ADR / FORMAL AUTHORITY > TRACKING / PLAN**;
  la discrepancia se corrige por el proceso gobernado apropiado.

*Clasificación: HUMAN DECISION — ACCEPTED RECOMMENDATION (opción (B)
normalizada con la cadena de precedencia explícita; sin cambios a ADR-0045/0046
ni a `registry.yaml`).*

### MD-A3 — frontera H5: Evidencia / Provenance / Memoria / Autoridad / Decisión

Separación estricta y definiciones:

- **Evidence:** información que sostiene o contradice una afirmación.
- **Provenance:** información sobre origen y linaje (fuente, autoridad declarada,
  método, contexto); **no establece por sí misma autoridad normativa**.
- **Memory:** información contextual persistente para continuidad de ingeniería.
- **Authority:** fuente formalmente reconocida capaz de establecer una regla
  normativa o decisión arquitectónica.
- **Decision:** elección formalmente aceptada que establece arquitectura,
  política, alcance o restricciones.

Por tanto: la evidence puede soportar una decisión pero no se convierte en
autoridad; la memoria puede preservar evidence y rationale pero no se vuelve
autoridad por ser persistente. Donde los límites de autoridad
TCB/security-sensitive sean ambiguos: **FAIL-CLOSED → NO ASSUME TRUST → NO
PROMOTE MEMORY TO AUTHORITY → ESCALATE**.

*Clasificación: HUMAN DECISION — ACCEPTED RECOMMENDATION (opción (A) de TCB
mínimo, normalizada con definiciones y regla de escalamiento).*

### MD-A4 — D20b: base arquitectónica confirmada; implementación NO autorizada

Se confirma:

```
D20b = DECIDED
D20b = MATERIALIZED
D20b = VERIFIED
```

distinto de autorización de implementación. D20b sigue siendo la base
arquitectónica de futura implementación; **esta decisión no autoriza
implementación**:

```
D20b → ARCHITECTURAL AUTHORITY → FUTURE IMPLEMENTATION DESIGN
     → SEPARATE IMPLEMENTATION AUTHORIZATION
```

No se implementan esquemas PostgreSQL, tablas de Outbox, Kafka, esquemas
Iceberg, event envelopes, migraciones, adapters ni infraestructura: pertenecen
a gates de implementación posteriores. Resuelve la fuente normativa del
conflicto F-OCM-01/D-OCM-01 a favor de ADR-0049 sin conceder autorización.

*Clasificación: HUMAN DECISION (normalización dentro del espacio de opciones
investigado (a)+(b)).*

### MD-A5 — MVES + Human Authority, Agent Engineering Autonomy

Se adopta el modelo operativo **Minimum Viable Engineering System (MVES)** con
el principio **Human Authority, Agent Engineering Autonomy**, bajo el ciclo:

```
DECIDE → SPECIFY → SCOPE → RESEARCH → DESIGN → PLAN → AUTHORIZE
       → IMPLEMENT → VERIFY → CONVERGE → ATTEST
```

Modelo gobernante de ingeniería asistida por agentes. El humano define
intención, arquitectura normativa, alcance estratégico, límites de autoridad,
riesgos no aceptables, elecciones arquitectónicas sin resolver y
autorización explícita de implementación. El agente resuelve autónomamente
decisiones técnicas reversibles, detalles de implementación, refactoring,
estrategia de tests, nombrado interno, algoritmos, mecanismos técnicos y
elecciones de ingeniería suficientemente determinadas por la evidencia.

*Clasificación: HUMAN DECISION — ACCEPTED RECOMMENDATION (adopción tal como
recomendada).*

## Alternativas evaluadas

1. **H-CTX-1:** A (contexto mínimo nativo, elegida) / B (adopción externa —
   evidencia insuficiente, requeriría CTX-3) / C (diferir — P3 aceptado).
2. **H-CTX-2:** A solo-humanos / B proponer→aprobar (elegida) / C auto-FACTs
   (diferido) / D escritura libre (rechazado).
3. **MD-A1:** (a) sin ADR intencional / (b)+(c) provenance no establecida
   (elegida) — ver §MD-A1.
4. **MD-A2:** (A) ADR>Registry / (B) dual con fail-closed (elegida, cadena
   explícita) / (C) Registry>ADR (contradice H3-H9).
5. **MD-A3:** (A) TCB mínimo (elegida) / (B) extendido / (C) bifurcado.
6. **MD-A4:** (a) ADR-0049 vigente + autorizar / (b) diferir / (c) alinear
   KB-CONTRACT — combinados: base ADR-0049 confirmada, implementación diferida.

## Arquitectura de contexto (principio)

```
                    OCM AUTHORITY
                         │
              ┌──────────┴──────────┐
              │                     │
             ADRs              Contracts
              │                     │
              └──────────┬──────────┘
                         │
                    GOVERNANCE
                         │
                         ▼
              ENGINEERING CONTEXT
                         │
       ┌─────────────────┼─────────────────┐
       │                 │                 │
   Decisions          Evidence          Research
   Rationale          Provenance         History
   Constraints        Invariants         Git
   Rejections         Code relations     Handoff
       │                 │                 │
       └─────────────────┼─────────────────┘
                         │
                         ▼
                   CONTEXT PACK
                         │
                         ▼
                       AGENT

The Agent consumes context.
The Agent does not become the authority.
```

## Política de escalamiento (E1–E8)

Los agentes **no deben** escalar por: múltiples implementaciones posibles;
preferencia de confirmación; complejidad; refactor multarchivo; opciones
estándar de ingeniería; o práctica *best-of-world* con respuesta clara.

Antes de escalar: **E1** ADR existente · **E2** especificación/contrato ·
**E3** governance/AGENTS · **E4** evidencia del repo · **E5** KB/documentos ·
**E6** estándar formal/fuente primaria · **E7** práctica superior establecida ·
**E8** decisión humana normativa/estratégica/autoridad genuina.

Solo **E1–E7 agotados AND E8 = YES** justifica `HUMAN DECISION REQUIRED`;
si no, el agente decide autónomamente, documenta su rationale y procede.

## Best-of-world — adopciones rechazadas prematuramente

"Best-of-world" = la arquitectura más simple que satisface requisitos,
invariantes, fiabilidad, auditabilidad, seguridad, mantenibilidad y evolución
de OCM con la evidencia más fuerte disponible. Se rechazan de forma explícita
hasta nueva evidencia: bases de datos vectoriales; embeddings; Neo4j; knowledge
graphs temporales; transcript mining; daemons de memoria distribuidos;
agentes de memoria especializados; RAG pesado; infraestructura SCIP/LSIF;
MCP servers innecesarios; SaaS externo innecesario; microservicios
innecesarios. Pueden justificarse luego; **no son requeridos por esta decisión**.

## Justificación técnica

- La evidencia del research gate demostró que docs estructurados + metadata +
  git-aware retrieval + ripgrep + Context Pack + Skill de bootstrap resuelven
  P1/P2 con cero componentes nuevos; el store solo aporta el residual P3 bajo
  control humano (opción A).
- Opción B carecía de evidencia de adopción suficiente (solo README) → no
  seleccionable responsablemente.
- La precedencia ADR > Registry > Tracking formaliza H3-H9 + H2-E sin inventar
  jerarquías nuevas (la cadena ya estaba implícita en ADR-0045/0046).
- El veto a poblar `adr:null` evita crear autoridad por edición (regla de
  silencio de §2 de los gates).

## Consecuencias

- Queda más fácil: recuperación de contexto por cualquier agente autorizado;
  escalamiento objetivo (E1–E8); auditoría de toda escritura de memoria.
- Deuda aceptada conscientemente: sin store hasta gate de implementación;
  `adr:null` permanece sin resolver a nivel de dato (fail-closed operativo);
  P3 residual asumido hasta CTX-2.
- Autoridad preservada: memoria/contexto nunca son autoridad; fail-closed
  intacto; D20b es autoridad arquitectónica con implementación NO autorizada.
- Sin contratos BC-NN nuevos; el enforcement existente (import-linter,
  architecture_linter, CI) queda intacto.
- **NO autorizado por esta gate:** infraestructura de memoria, Skills, Agents,
  configuración OpenCode, bases de datos/indexos/vector stores, embeddings,
  graph databases, MCP memory servers, indexers, esquemas PostgreSQL/Outbox/
  Kafka/Iceberg, infraestructura de ejecución, integraciones broker/exchange,
  `docs/plans/tracking.yaml`, `uv.lock`.

## Trazabilidad

```
Decision (H-CTX-1/2, MD-A1..A5 — Human Owner, 2026-10-08)
   ↓
Authority (Human Authority, Agent Engineering Autonomy; jerarquía §2 gates)
   ↓
Rationale (research gate + CTX-1 + consolidación: evidencia mínima suficiente)
   ↓
Evidence (policies/registry.yaml R2..R8; ADR-0045 CX-14/CX-15/CX-16;
          ADR-0046 H3-H9/H3-H10/H3-H12; ADR-0049 §estado D20b;
          D19_HUMAN_DECISION_PACK_2026-09-30.md; AUDIT_OCM…2026-10-07.md F-OCM-01/D-OCM-01)
   ↓
Affected Governance / ADR (ADR-0045, ADR-0046, ADR-0049, AGENTS.md, governance)
```

No se crean enlaces de trazabilidad cuya fuente no existe.

## Referencias

- Código: `policies/registry.yaml` (R2..R8, `adr: null` — sin modificar)
- ADRs relacionados: ADR-0045 (CX-14/CX-15/CX-16), ADR-0046 (H3-H9/H3-H10/H3-H12),
  ADR-0049 (estado D20b), ADR-0032 (revisión humana de cambios)
- Auditoría: `docs/audits/AUDIT_OCM_high-impact-improvements_2026-10-07.md` (F-OCM-01, D-OCM-01)
- Decisiones D19: `docs/audits/D19_HUMAN_DECISION_PACK_2026-09-30.md`
- Governance: `AGENTS.md`, `docs/architecture/GOVERNANCE.md`
