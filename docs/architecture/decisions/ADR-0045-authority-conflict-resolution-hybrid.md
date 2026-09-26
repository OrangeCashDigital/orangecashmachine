# ADR-0045: Authority Conflict Resolution — Hybrid (H2-E Custom)

**Estado:** Propuesto
**Decisión humana asociada:** S6-CA-H2 = H2-E CUSTOM — DECIDIDA 2026-09-26
**Materialización:** PENDIENTE DE HUMAN REVIEW
**Fecha:** 2026-09-26
**Bounded context(s) afectado(s):** ocm (plataforma), shared, packages (todos), CI/CD

## Contexto

La auditoría S6-CA (Consolidación de Autoridad y Resolución de Conflictos)
identificó que OCM contiene múltiples fuentes de autoridad (plan maestro,
GOVERNANCE.md, AGENTS.md, AUDIT_PROTOCOL.md, ADRs, registry.rules,
tracking.reglas, CODEOWNERS, CI, Evidence, artefactos de enforcement y
configuración externa de GitHub) entre las que pueden existir afirmaciones
o reglas incompatibles, sin un mecanismo aprobado para determinar cuál
prevalece.

No existe una precedencia universal configurada. Los conflictos reales
(CX-1/CX-2, CX-3, CX-6, CX-8/CX-9, CX-11/CX-12, CX-13, CX-14, CX-15/CX-16,
S5-F12) quedaron documentados en S6-CA sin resolución.

La decisión humana S6-CA-H1 (2026-09-26) estableció el modelo de autoridad
de OCM como CUSTOM / HYBRID AUTHORITY MODEL, con exactamente tres
dimensiones:

- MODEL D — layered normative authority / jerarquía normativa refinada §B.
- MODEL B — specialized SSOT por dominio; no existe un SSOT universal de OCM.
- MODEL C — fronteras de gobernanza CODE / AGENT / EXTERNAL.

MODEL C no es un modelo de SSOT; delimita fronteras de gobernanza.

Este ADR registra el mecanismo de resolución de conflictos de autoridad
dentro de ese marco.

## Problema

Dos fuentes de autoridad de OCM pueden contener reglas o afirmaciones
incompatibles, y no existe un mecanismo aprobado que determine cuál
prevalece ni cuándo el conflicto debe bloquearse y escalarse al humano.
La ausencia de mecanismo fuerza hoy a decidir caso a caso, sin regla.

## Alternativas evaluadas

1. **H2-A — Fixed Deterministic Precedence** — Ventaja: máximo
   determinismo. Coste/riesgo: exige definir la jerarquía completa de una
   vez; en el brownfield actual no hay niveles formalmente documentados y
   definirla implicaría inferir autoridad (prohibido).
2. **H2-B — Domain-Scoped Precedence** — Ventaja: coherente con SSOT por
   dominio (H1-B). Coste/riesgo: exige dominios formalmente completos
   (hoy `adr: null` lo vulnera en R2/R7); no resuelve conflictos
   inter-dominio sin regla adicional.
3. **H2-C — Human Resolution by Default** — Ventaja: no inventa autoridad.
   Coste/riesgo: todo conflicto sin regla escala al humano; coste
   operacional alto y riesgo de bloqueos.
4. **H2-D — Hybrid** — Ventaja: automatiza lo definido y bloquea el resto.
   Coste/riesgo: dependía de criterios de "claramente definido" que no
   existían aún.
5. **H2-E — CUSTOM híbrido (adoptada por decisión humana)** — regla
   aprobada → autoridad de dominio → FAIL-CLOSED + HUMAN DECISION GATE,
   con reglas normativas que prohíben inferir precedencia.

## Decisión

OCM adopta **H2-E — Hybrid Authority Conflict Resolution** como único
mecanismo de este ADR:

```
CONFLICT
   ▼
¿Existe una regla de precedencia formalmente aprobada?
   ├── YES → APLICAR LA REGLA DETERMINISTA APROBADA
   └── NO
        ▼
¿La autoridad del dominio está formalmente determinada?
        ├── YES → APLICAR LA AUTORIDAD DEL DOMINIO
        └── NO
             ▼
          FAIL-CLOSED
             ▼
       HUMAN DECISION GATE
             ▼
   NUEVA DECISIÓN / REGLA
```

El mecanismo completo es:

CONFLICT → APPROVED PRECEDENCE RULE → DOMAIN AUTHORITY IF FORMALLY ESTABLISHED → FAIL-CLOSED → HUMAN DECISION GATE

### Reglas normativas de H2-E

1. No existe una precedencia universal implícita.
2. La precedencia automática requiere una regla formalmente aprobada.
3. La autoridad especializada del dominio solo puede utilizarse cuando esté
   formalmente determinada.
4. La existencia material de un archivo no concede automáticamente autoridad.
5. Evidence no es SSOT; es attestación derivada.
6. CI verifica/enforces, pero no define por sí misma la autoridad normativa.
7. Los artefactos legacy no adquieren autoridad solo por existir.
8. Una implementación existente no demuestra que la decisión arquitectónica
   o normativa haya sido aprobada.
9. Un agente no puede inventar precedencia ni resolver por inferencia un
   conflicto indeterminado.
10. Si la autoridad no puede determinarse inequívocamente (sin regla
    aprobada y sin dominio formalmente determinado), el comportamiento es
    FAIL-CLOSED + HUMAN DECISION GATE.
11. Una decisión humana posterior no se interpretará retroactivamente como
    si hubiera sido una regla aprobada antes de esa decisión.

### Aclaraciones normativas de H2-E (invariantes explícitas)

Estas aclaraciones especifican las reglas 1–11; no constituyen decisiones
nuevas ni precedencias concretas entre artefactos:

- Un documento no adquiere autoridad por aparecer primero.
- Ser más reciente no confiere autoridad automáticamente.
- Estar materializado en el repositorio no confiere autoridad
  automáticamente.
- Ser ejecutable no confiere autoridad normativa.
- Ser verificado por CI no confiere autoridad normativa.
- Aparecer en Evidence no confiere autoridad normativa.
- Ser mencionado por un agente no confiere autoridad.
- Los agentes no pueden inventar precedencia.
- Si existe una regla formalmente aprobada, esa regla se aplica.
- Si no existe regla aprobada pero la autoridad de dominio está
  formalmente determinada, se aplica esa autoridad.
- Si ninguna de las dos condiciones existe, se falla cerrado
  (FAIL-CLOSED) y se pasa a Human Decision Gate.
- Durante el estado indeterminado no se debe inferir, sobrescribir,
  reconciliar ni implementar una decisión.

### Tratamiento normativo de `adr: null`

`adr: null` permanece semánticamente indeterminado mientras no exista una
regla formal aprobada que determine su significado. No se interpreta como
autoridad superior, autoridad inferior, inválido, legacy, permitido,
prohibido ni excepción.

Cualquier conflicto relacionado con `adr: null` sigue el mecanismo H2-E y,
si no existe una autoridad de dominio formalmente determinada, termina en
FAIL-CLOSED → HUMAN DECISION GATE.

### Tratamiento conceptual de los conflictos H2 relevantes

El ADR no resuelve ningún caso; solo los clasifica según el mecanismo H2-E.

| Caso | ¿Regla de precedencia aprobada? | ¿Autoridad de dominio formalmente determinada? | Tratamiento H2-E |
|---|---|---|---|
| CX-1/CX-2 — `tracking.reglas ↔ registry.rules` | No | No determinada formalmente | FAIL-CLOSED → HUMAN DECISION GATE |
| CX-3 — `ADR ↔ CODEOWNERS` | No | No (la materialización no confiere autoridad: regla 4) | FAIL-CLOSED → HUMAN DECISION GATE |
| CX-6 — `Evidence ↔ CI` | No | No (ambas conservan sus roles: reglas 5 y 6) | La discrepancia se clasifica; si exige decisión de autoridad → FAIL-CLOSED → HUMAN DECISION GATE |
| CX-8/CX-9 — `M26 / staged-port governance` | No | No | FAIL-CLOSED → HUMAN DECISION GATE (este ADR no altera M26) |
| CX-11/CX-12 — `Agent Governance` | No | No | FAIL-CLOSED → HUMAN DECISION GATE (la instrucción de agente no desplaza autoridad normativa; enforcement es H4) |
| CX-13 — `human decision ↔ materialized authority` | No | No | Decisión de gate ≠ decisión materializada; la materialización es H3 |
| CX-14 — `TCB / Evidence` | No | No | La clasificación de TCB es H5 |
| CX-15/CX-16 — `registry.rules ↔ ADR / adr:null` | No | No (`adr:null` indeterminado) | FAIL-CLOSED → HUMAN DECISION GATE |
| S5-F12 — `AGENTS.md / §B / KB hierarchy` | No | No | No se inventa jerarquía; la jerarquía faltante se resuelve por la autoridad correspondiente en decisión posterior |

## Justificación técnica

- H2-E permite operar sin inventar una jerarquía que el brownfield no
  documenta.
- Mantiene el determinismo como objetivo alcanzable mediante decisiones
  formales, no por inferencia.
- Conserva las restricciones de H1: Evidence derivado, CI verifier,
  GitHub como assurance externo, SSOT por dominio y separación
  CODE/AGENT/EXTERNAL.
- El fail-closed convierte "no decidido" en bloqueo auditable, coherente
  con la cadena de adopción (Conocimiento → Decisión Humana →
  ADR/Governance → Control).

## Consecuencias

- **Más fácil:** clasificar cualquier conflicto futuro en una de las tres
  ramas del mecanismo y decidir si requiere human gate.
- **Deuda aceptada:** mientras no existan reglas de precedencia aprobadas
  ni dominios formalmente determinados, los conflictos indeterminados se
  bloquean (fail-closed) y escalan al humano.
- **Contratos:** ninguno nuevo se crea con este ADR; el enforcement
  normativo de decisiones materializadas se define en H3/H4/H5.
- **M26:** permanece FROZEN / ARCHITECTURALLY NOT ACCEPTED; este ADR no lo
  promueve ni lo rechaza.

## Scope

- Define el mecanismo general de resolución de conflictos de autoridad
  (H2-E) para todo OCM.
- Clasifica conceptualmente el tratamiento de los conflictos reales ya
  identificados en S6-CA, sin resolverlos.
- Establece los límites de autoridad que deben respetar CODE Governance,
  AGENT Governance y EXTERNAL Governance ante un conflicto.

## Non-scope (decisiones posteriores)

Quedan explícitamente fuera de este ADR y pertenecen a decisiones futuras:

- decidir si registry.rules tiene precedencia sobre ADR o viceversa,
  mediante una decisión formal posterior que establezca dicha precedencia;
- el destino definitivo de `tracking.reglas`;
- el destino definitivo de `adr:null` (sigue indeterminado);
- cómo registrar formalmente las futuras decisiones humanas del human gate
  (H3);
- cómo implementar Agent Governance técnicamente (H4);
- qué pertenece al TCB definitivo (H5);
- cómo integrar Evidence con CI;
- qué hacer con M26 (promoción o rechazo);
- B4, B5 y Fact Model;
- staged-port enforcement;
- migraciones y cambios de CI.

## Relación con H1

H1 ya fue decidido por el humano como CUSTOM / HYBRID AUTHORITY MODEL:

- MODEL D — layered normative authority (jerarquía refinada §B).
- MODEL B — specialized SSOT por dominio (no un SSOT universal de OCM).
- MODEL C — fronteras de gobernanza CODE / AGENT / EXTERNAL.

H2-E es el mecanismo conceptual de resolución de conflictos **dentro** de ese
modelo: respeta las capas (D), la autoridad por dominio (B) y las fronteras
de gobernanza (C). Este ADR no redefine H1.

## Relación con H3 / H4 / H5

Cadena de decisión:

- H1 → modelo de autoridad (layered + SSOT por dominio + fronteras).
- H2 → resolución de conflictos (regla aprobada → dominio → fail-closed →
  human gate).
- H3 → materialización / frontera de registro de decisiones.
- H4 → enforcement técnico de Agent Governance.
- H5 → frontera TCB / Evidence.

Estado de la cadena en este ADR:

- H1 = DECIDIDA — CUSTOM/HYBRID D+B+C.
- H2 = DECIDIDA — H2-E CUSTOM.
- H3 = NOT DECIDED / NOT STARTED.
- H4 = NOT DECIDED / NOT STARTED.
- H5 = NOT DECIDED / NOT STARTED.

Este ADR no adelanta ninguna decisión de H3, H4 ni H5.

## Referencias

- Auditoría: S6-CA — Consolidación de Autoridad y Resolución de Conflictos
  (conflictos CX-1..CX-16; S5-F12; matrices DMAE/TCB/Boundary).
- Decisiones humanas: S6-CA-H1 = CUSTOM/HYBRID (D+B+C); S6-CA-H2 = H2-E
  CUSTOM — 2026-09-26.
- ADRs relacionados: ADR-0031 (policy registry), ADR-0032 (AI agent
  governance), ADR-0033 (production gate), ADR-0044 (staged ports).
- Artefactos (sin cambio de estatus en este ADR):
  `docs/architecture/GOVERNANCE.md`, `docs/governance/AUDIT_PROTOCOL.md`,
  `policies/registry.yaml`, `policies/evidence.json`, `docs/plans/tracking.yaml`,
  `AGENTS.md`.