> **NOTA EDITORIAL (estado final):** Título **aprobado** · Ruta **aprobada**
> (0055 verificado libre) · Cinco puntos de Q1: **conservados literales** ·
> Fecha: **formulación triple conservada** · Correcciones **F-01, F-02 y F-04
> ratificadas y aplicadas** · Fila F-03 de Evidencia: **ratificada por el
> propietario humano** (condición documental confirmada). **Pendiente
> únicamente:** clasificación `Bounded context(s)` = `PENDIENTE DE DECISIÓN
> HUMANA`. La aprobación del contenido no constituye autorización de escritura.

# ADR-0055: Registrar la autoridad normativa de D19-01 y D20b-01..06 (D-OCM-01 Q1)

**Estado:** ACEPTADO — aceptación documental autorizada 2026-10-09 (decisión humana D-OCM-01 Q1, adoptada 2026-10-08; no declara vigencia normativa ni autoriza implementación)
**Decisión humana asociada:** D-OCM-01 Q1 = OPCIÓN A — decidida en conversación
  el 2026-10-08 (fecha atribuida a la decisión en conversación según el registro
  de esta conversación; registrada en el repositorio con la materialización de
  este ADR el 2026-10-09)
**Fecha:** 2026-10-08 = fecha atribuida a la decisión humana Q1 en conversación.
  Fecha de creación de este documento: **2026-10-09** (materializado vía gate de
  escritura autorizado). Fecha de aceptación formal: **2026-10-09** (aceptación
  documental autorizada; distinta de la fecha de decisión Q1 y de la de
  creación; no implica vigencia normativa).
**Materialización:** REALIZADA EL 2026-10-09 VÍA GATE DE ESCRITURA AUTORIZADO;
  REVISIÓN HUMANA RESUELTA — ACEPTACIÓN DOCUMENTAL REALIZADA EL 2026-10-09 (ver
  campo `Estado`). La aceptación documental no declara vigencia normativa ni
  autoriza implementación.
**Bounded context(s) afectado(s):** PENDIENTE DE DECISIÓN HUMANA — valor
  sugerido por el agente: `market_data | ocm` (**no es una decisión humana**; la
  decisión humana no especificó bounded contexts; el campo figura por convención
  de plantilla, ADR-template.md:5).

## Contexto

F-OCM-01 (AUDIT_OCM_high-impact-improvements_2026-10-07.md:98-130) registró una
contradicción normativa sobre el estado de D19-01 y D20b-01..06: ADR-0049
(**Estado:** ACEPTADO, L3) y el pack D19 los declaran DECIDED (D19-01 = C+),
mientras INDUSTRY (2026-10-03) y KN_BASE declaran HUMAN DECISION REQUIRED /
PENDING. Como evidencia normativa relevante: AUDIT_PROTOCOL §B (L17-26) ubica
"ADRs aprobados … y decisiones humanas formalizadas" en LEVEL 3 y
`docs/knowledge/` en LEVEL 1; AGENTS.md L162 establece "Ante conflicto entre la
KB y código/ADR, el código/ADR gana siempre". Ambos documentos son LEVEL 4; **la
relación de precedencia entre ellos no se declara en este documento — cuestión
abierta (U4)**. El 2026-10-08, en conversación, el propietario humano resolvió
D-OCM-01 parte 1 (Q1) = Opción A. Este documento es el registro de esa decisión,
materializado el 2026-10-09 vía gate de escritura con autorización explícita
(H3-H4, ADR-0046:64-69 — la conversación por sí sola no materializa).

## Pregunta (D-OCM-01 parte 1)

¿Qué fuente gobierna el estado de D19-01 y D20b-01..06, y qué mecanismo de
reconciliación documental se ordena?

## Decisión humana efectiva — Q1 = OPCIÓN A (2026-10-08, registro conversacional)

Registrada sin ampliarla:

- ADR-0049 y los registros de decisiones humanas son fuentes normativas para
  D19-01 y D20b-01..06.
- INDUSTRY y KN_BASE son fuentes subordinadas de investigación y conocimiento
  para este asunto.
- Las contradicciones documentales entre fuentes deben reconciliarse sin borrar
  la evidencia histórica ni alterar retroactivamente su contenido.
- Esta decisión no concede autorización de implementación.
- La decisión está tomada en conversación; su registro normativo en el
  repositorio todavía debe materializarse y verificarse por separado.

## Justificación documental (elaboración del agente — no es texto del decisor)

- La Opción A se presentó en el informe de decisión F-OCM-01/D-OCM-01 y su
  coherencia con AUDIT_PROTOCOL §B (L17-26, L105-107) y AGENTS.md L162/L166-173
  fue verificada como evidencia relevante **sin declarar una jerarquía universal
  entre ambos documentos (U4 permanece abierta)**.
- Las opciones B y C fueron presentadas al decisor con su análisis documental
  (B: sin fundamento previo verificado y en tensión con las autorenuncias
  INDUSTRY L3-11 y KN_BASE L5; C: mantenía F-OCM-01 abierto). La selección de A
  corresponde a la decisión humana; los argumentos de evaluación pertenecen al
  informe del agente, no al decisor.

## Cuestiones normativas que permanecen sin resolver (no tratadas aquí)

U4 (precedencia AGENTS.md ↔ AUDIT_PROTOCOL), relación MD-A4/ADR-0054-PEC con Q1,
U1 (cronología del contenido C+), reconciliación material de INDUSTRY/KN_BASE
(fuera de alcance), colisión ADR-0054, F-OCM-02/M4, CX-1/CX-2, CX-15/CX-16,
S5-F12, GATE C §17.3/§17.4, B-MD-005, persistencia del pack D19, Q2. Ver
"Límites".

## Estado documental por dimensiones (sin «DECISION REGISTERED» global)

1. Decisión tomada en conversación: **SÍ — 2026-10-08** (registro conversacional).
2. Decisión registrada en un documento normativo específico: **SÍ** — ADR-0055,
   creado el 2026-10-09 y con aceptación documental formalizada el 2026-10-09
   (`Estado: ACEPTADO`; sin declaración de vigencia normativa).
3. Decisión registrada en `docs/plans/tracking.yaml`: **NO** — 0 entradas
   D19/HD/D-OCM (verificado; sin cambios en este alcance).
4. Decisión materializada y verificada en el repositorio mediante ADR-0055:
   **SÍ — 2026-10-09** (verificada en el gate de corrección ADF-01/ADF-02); la
   materialización no implica aceptación formal ni autorización de
   implementación.

Preparar o llegar a crear este archivo con `Estado: Propuesto` **no** lo declara
aceptado, aprobado formalmente ni vigente (H3-H5/H3-H6/H3-H12, ADR-0046:71-81,
123-141). La aceptación documental se realizó el 2026-10-09; la vigencia
normativa no queda declarada por esa aceptación.

## Evidencia (verificada en esta sesión)

| Fuente | Referencia | Declaración |
|---|---|---|
| AUDIT_PROTOCOL | §B L17-26; L105-107; §N L108-110 | jerarquía de niveles; "menor no invalida mayor"; estructura `D-XXX` |
| AGENTS.md | L162; L166-173 | "el código/ADR gana siempre"; "la KB … NO gobierna OCM" |
| ADR-0049 | L3; L542; L555-560; L561-562 | ACEPTADO; D19-01 = DECIDED (C+); D20b-01..06 = DECIDED; D21–D24 = BLOCKED; IMPLEMENTATION = NOT AUTHORIZED |
| Pack D19 | L40; L836; L2270; L2291 | HUMAN DECISION MADE; D19-01 = DECIDED — C+; IMPLEMENTATION: NOT AUTHORIZED; decision registration |
| INDUSTRY | L3-11 (prohibiciones en L10-11); L33-35; L167-169 | [síntesis L3-9] metadata y modo de ejecución; [L10-11] - NO implementación; - NO registrar decisiones; [L33-35] - D19-01 = HUMAN DECISION REQUIRED; - IMPLEMENTATION = NOT AUTHORIZED; - DECISION REGISTERED = NO; [L167-169] - D19-01: HUMAN DECISION REQUIRED; - DECISION: NOT YET DECIDED; - D19: PENDING / BLOCKED; [síntesis] contradicción histórica preservada |
| KN_BASE | L5; L164-167 | "NO decide ninguna cuestión de dominio"; snapshot §12 con D19-01 = HUMAN DECISION REQUIRED (contradicción histórica preservada) |
| Audit 10-07 | L98; L306; L342 | F-OCM-01 — CRITICAL · CONTRADICCIÓN; fila D-OCM-01; "único bloqueo crítico" |
| tracking.yaml | md5 c4886bd3…; grep 0 | dimensión 3 = NO |
| ADR-0046 | L64-69; L71-81; L123-141 | H3-H4 materialización; H3-H5/H3-H6; H3-H12 |
| ADR-0045 / ADR-0046 | frontmatter L1-6 | precedente: cabeceras `Decisión humana asociada` + `Estado: Propuesto` + `PENDIENTE DE HUMAN REVIEW` |
| ADR-template / GOVERNANCE | template L5; GOVERNANCE L30-32 | campos de plantilla; ubicación y numeración secuencial (0055 libre, verificado) |
| Conversación | gate Q1 (recibido ≤ 2026-10-08 21:55, preflight 21:55:41) | Opción A + redacción de los bullets (única evidencia primaria de la fecha) |

## Consecuencias documentales

- Fija —dentro de este registro propuesto— la dirección de autoridad para
  D19-01 y D20b-01..06 en los términos de Q1.
- La contradicción material **NO queda reconciliada**: INDUSTRY y KN_BASE **no
  se modifican** en este alcance; conservan su contenido histórico. Toda
  reconciliación futura debe preservar la evidencia histórica y no alterar
  contenido retroactivamente.
- No se altera `tracking.yaml`, ADR-0049, el pack D19 ni ningún otro archivo.
- **Este ADR-0055 NO resuelve:** Q2; la persistencia del pack D19; las
  anotaciones/reconciliaciones B1/B2; el tracking y GATE C; §17.3/§17.4 y
  B-MD-005; F-OCM-02/M4; la colisión de numeración ADR-0054; CX-1/CX-2 y
  CX-15/CX-16; S5-F12; U1 y la cronología histórica no demostrada; U4 y la
  jerarquía general entre documentos de gobernanza; la implementación de D20b.

## Límites de autorización

- Q2 (materialización de D20b-01..06) **NO decidida**.
- **IMPLEMENTATION = NOT AUTHORIZED** (ADR-0049 L562; pack L2270): registrar
  esta decisión **no autoriza** código, persistencia, DefinitionVersion,
  VenueConstraints, Outbox, lanes, adapters, consumers, wiring, cambios de
  tracking ni cambios de CI.
- Alcance = registro de Q1 únicamente (Q-c): sin anotaciones INDUSTRY/KN_BASE,
  sin tracking, sin persistencia del pack, sin operaciones Git por este conducto.
- **MD-A4 / ADR-0054-PEC:** la relación con Q1 permanece **SIN ACLARAR** por
  decisión humana. Este documento no declara que sean la misma decisión ni que
  sean definitivamente distintas en intención. La cláusula de PEC
  (docs/architecture/decisions/ADR-0054-persistent-engineering-context-decisions.md:172-173)
  no sustituye esa aclaración; el `Estado: Propuesto` de PEC no le concede
  autoridad normativa por sí mismo.
- Cronología U1 (aparición del contenido C+ en el pack): **BLOCKED**; el commit
  único de ADR-0049 (3c4aa4a3) no prueba evolución previa.

## Finding relacionado

F-OCM-01 — CRITICAL · CONTRADICCIÓN (audit 10-07 L98-130); D-OCM-01 (L306).

## Referencias

- docs/architecture/decisions/ADR-0049-canonical-instrument-identity-g1.md (L3, L542, L555-562)
- docs/audits/D19_HUMAN_DECISION_PACK_2026-09-30.md (L40, L836, L2270, L2291)
- docs/audits/D19-01_INDUSTRY_BOW_ARCHITECTURE_REVIEW_2026-10-03.md (L3-11, L33-35, L167-169)
- docs/knowledge/KNOWLEDGE_BASE_CONTRACT.md (L5, L164-167)
- docs/audits/AUDIT_OCM_high-impact-improvements_2026-10-07.md (L98-130, L306, L342)
- docs/governance/AUDIT_PROTOCOL.md (§B L17-26, L105-107; §N L108-110)
- AGENTS.md (L8, L162, L166-173); docs/architecture/GOVERNANCE.md (§2, §3 L30-32)
- docs/architecture/decisions/ADR-0045-authority-conflict-resolution-hybrid.md (precedente)
- docs/architecture/decisions/ADR-0046-decision-records-materialization.md (H3-H2, H3-H4, H3-H5, H3-H6, H3-H12)
- docs/architecture/decisions/ADR-template.md (estructura de campos)
- docs/architecture/decisions/ADR-0054-persistent-engineering-context-decisions.md (MD-A4 — relación sin aclarar)
- Decisión humana D-OCM-01 Q1 = OPCIÓN A, 2026-10-08 (registro de conversación)
