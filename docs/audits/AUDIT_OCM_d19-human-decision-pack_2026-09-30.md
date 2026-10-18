# D19 HUMAN DECISION PACK — Physical Materialization Decisions (D19-01..D19-10)

**Fecha:** 2026-09-30
**Destino:** `docs/audits/` (artefacto documental de gobernanza/auditoría)
**Modo:** DOCUMENTATION/GOVERNANCE ONLY — READ-ONLY SCOPE
**Restricciones:** No modifica código, tests, ADR-0049, tracking.yaml, GOVERNANCE.md, CI, schemas, ports, adapters, repositories, APIs. No registra decisiones humanas. No autoriza implementación. No realiza Git mutations.

> **Propósito:** Materializar de forma persistente y auditable el paquete de decisión humana para las decisiones físicas D19-01…D19-10. Este documento NO toma decisiones, NO recomienda alternativas y NO cambia estados. Presenta únicamente alternativas, restricciones, invariantes, consecuencias arquitectónicas y dependencias para deliberación humana.

**Estado de entrada:**
- HD-1..HD-8: DECIDED
- HD-VC-1..HD-VC-7: DECIDED
- D12: DECIDED — Alternative 5 + Alternative 3
- D19: PENDING/BLOCKED
- GATE C: BLOCKED
- IMPLEMENTATION: NOT AUTHORIZED

## 1. PRINCIPIOS TRANSVERSALES

Este pack parte obligatoriamente de decisiones ya cerradas (HD-1..HD-8, HD-VC-1..HD-VC-7, D12). Debe preservarse estrictamente:

- **IDENTITY ≠ DEFINITION ≠ REPRESENTATION ≠ VENUE CONSTRAINTS ≠ EXCHANGE QUIRKS ≠ RISK POLICY**
- **DefinitionVersion y VenueConstraints son ejes independientes.** Relación **informativa, NO estructural, NO composicional.**
- **Sin coupling temporal obligatorio.** Un cambio de VenueConstraints no crea automáticamente DefinitionVersion. Un cambio de DefinitionVersion no crea automáticamente VenueConstraints. **No existe ventana temporal conjunta obligatoria.**
- **Sin segundo SSOT.** Un valor contractual no puede ser simultáneamente término de ContractTerms/DefinitionVersion y VenueConstraint.
- **HD-VC-1 A+:** admisión directa por evidencia + clasificación/routing semántico (nunca inferencia numérica) + clasificación no determinista → FAIL-CLOSED. Procedencia ≠ Admisibilidad.
- **Evidence ≠ Provenance.** `effective_at ≠ observed_at ≠ recorded_at`. `SOURCE ≠ AUTHORITY`. Evidencia nunca se destruye destructivamente.
- **Resolución determinista + FAIL-CLOSED.** Sin `latest-wins`, `source-wins`, `API-wins`, `observed-wins`. Sin interpolación. Sin reconstrucción histórica por inferencia.
- **Ownership (HD-4, HD-VC-7):** `DOMAIN = DECIDE`, `APPLICATION = ORCHESTRATE`, `PORT/REGISTRY = INFORM`, `ADAPTER = TRANSLATE / PROVIDE EVIDENCE`, `Consumers = CONSUME`.
- **ExchangeQuirks = únicamente transporte/API/connector.** RiskPolicy separado de VenueConstraints. SymbolBinding separado.
- **Catálogo gobernado, explícito y no exhaustivo** (D12-I, HD-3).
- **Distinción transversal obligatoria:** `LOGICAL DOMAIN ≠ RUNTIME MATERIALIZATION ≠ HISTORICAL PERSISTENCE ≠ TRANSPORT`

No se reabre D12. Las referencias históricas `D12 = OPEN` se tratan como **snapshots históricos** del proceso deliberativo. El estado registrado es **D12 = DECIDED — Alternative 5 + Alternative 3**.

---

## D19-01 — Physical Identity IDs

**STATUS:** HUMAN DECISION MADE

**QUESTION:** ¿Cómo debe materializarse físicamente una identidad estable independiente de `native_symbol` para `ListingIdentity`?

**WHY THIS DECISION EXISTS:** Garantizar que ListingIdentity tenga un identificador físico estable, independiente de representación (native_symbol/SymbolBinding), permitiendo continuidad histórica ante cambios de símbolo y relaciones tipadas (SUCCESSOR_OF/REPLACED_BY), preservando `Identity ≠ Representation`.

**PREVIOUS DECISIONS THAT CONSTRAIN IT:** D01, D02, D03, HD-1, HD-VC-7, D12 (Alt. 5+3), HD-VC-1 A+.

**NON-NEGOTIABLE INVARIANTS:** `Identity ≠ Representation`, estabilidad de identidad, sin derivación desde `native_symbol`, sin segundo SSOT.

### HUMAN DECISION

**SELECTED ALTERNATIVE:** **C+ — Hybrid Controlled Identity**

La arquitectura seleccionada combina:

**Physical Canonical Identity**

`identity_id`

Características obligatorias:

* opaco;
* estable;
* inmutable;
* independiente de atributos semánticos;
* independiente de `native_symbol`;
* independiente de Representation;
* independiente de VenueConstraints;
* independiente de DefinitionVersion;
* único como identidad física canónica.

`identity_id` será la única referencia canónica para:

* PK;
* FK;
* event identity/key cuando se requiera identidad canónica;
* historical reference;
* replay;
* audit;
* Iceberg identity/reference;
* cross-service canonical reference.

**LOGICAL IDENTITY KEY**

`logical_identity_key`

como:

clave lógica determinística y DERIVADA, utilizada como mecanismo auxiliar de lookup, reconciliación y validación de unicidad.

Debe derivarse exclusivamente de:

atributos declarados explícitamente como identity-defining e immutable por el contrato de identidad.

No puede incorporar campos de:

* Representation;
* native symbol;
* SymbolBinding contextual;
* VenueConstraints;
* ExchangeQuirks;
* RiskPolicy;
* datos operacionales;
* timestamps de observación;
* provenance mutable;
* lifecycle mutable;
* DefinitionVersion mutable.

### REGLA ABSOLUTA CONTRA SECOND SSOT

`logical_identity_key` NO es una segunda identidad.

Nunca podrá sustituir `identity_id` como:

* PK canónica;
* FK canónica;
* event identity;
* historical identity;
* Iceberg identity;
* replay identity;
* cross-service canonical reference.

La relación conceptual obligatoria es:

identity_id
    ↓
CANONICAL PHYSICAL IDENTITY
logical_identity_key
    ↓
DERIVED LOGICAL LOOKUP / RECONCILIATION KEY

Si existe cualquier documento actual que contradiga esta regla:

NO lo corrijas silenciosamente.

Repórtalo como:

GOVERNANCE INCONSISTENCY / HUMAN REVIEW REQUIRED

### CANONICALIZATION CONTRACT

Debe quedar explícitamente registrado que `logical_identity_key` requiere un contrato formal de canonicalización.

El contrato futuro deberá definir, como mínimo:

1. conjunto exacto de campos identity-defining;
2. inmutabilidad de dichos campos;
3. orden canónico;
4. representación canónica de cada valor;
5. tratamiento de null/optional;
6. tratamiento de enums;
7. normalización de unidades;
7. normalización de números;
8. serialización determinística;
9. algoritmo/hash, si corresponde;
10. versión del canonicalizador;
11. collision handling;
12. ambiguity handling;
13. backward compatibility;
14. historical reproducibility;
15. replay reproducibility;
16. migration compatibility.

IMPORTANTE:

El canonicalizador NO puede decidir por sí mismo qué campos son identity-defining.

La cadena normativa debe ser:

Identity Contract
        ↓
Identity-defining immutable attributes
        ↓
Canonicalization Contract
        ↓
logical_identity_key

### VERSIONADO DEL CANONICALIZADOR

Debe quedar como requisito normativo:

`logical_identity_key` debe estar asociado a una versión explícita del esquema/regla de canonicalización.

Conceptualmente:

canonicalization_version
+
canonical representation
+
logical_identity_key

No asumir que un algoritmo futuro puede reinterpretar retroactivamente claves históricas.

La evolución futura debe preservar:

* reproducibilidad;
* replay;
* histórico;
* auditoría;
* migración controlada.

NO implementar esto todavía.

Solo documentarlo como requisito del Implementation Gate.

### IMPACT ON OTHER D19 DECISIONS

Afecta D19-02,03,05,06,07,10 (estabilidad/lookup).

**IMPACT ON D12:** C+: COHERENTE. (A,C: COHERENTE. B: REQUIERE CONDICIÓN (*).)

**IMPACT ON HD-VC-1..7:** C+: COHERENTE. (A,C: COHERENTE. B: REQUIERE CONDICIÓN.)

**IMPLEMENTATION CONSEQUENCES:** Consecuencias físicas (referencia/lookup/mapping), **NO implementación de código**.

**HUMAN DECISION MADE:** YES  
**DECISION:** C+ — HYBRID CONTROLLED IDENTITY

**Condición (*) — B:** Solo partes **identity-defining inmutables**. **Ningún campo representation-bound**. No deriva identidad de representación. Estabilidad absoluta ante cambios de representación. No fuerza composite keys que acoplen representación/filtren por mutables. Preserva `Identity ≠ Representation`. Permite SUCCESSOR_OF/REPLACED_BY. No fusiona ejes. Si falla → INCOMPATIBLE.

---

## D19-02 — Physical ListingIdentity Model

**STATUS:** DECIDED — A (Pure Domain Entity / Entidad Propia)  
**QUESTION:** ¿Cómo debe materializarse físicamente `ListingIdentity` (venue-scoped), preservando `ListingIdentity ≠ native_symbol ≠ SymbolBinding ≠ DefinitionVersion`?

**WHY THIS DECISION EXISTS:** Delimitar boundary físico vs representación temporal (SymbolBinding) vs evolución definicional (DefinitionVersion), evitando forzar composición entre ejes (D12).

**PREVIOUS DECISIONS THAT CONSTRAIN IT:** D01–D03, HD-4, HD-VC-7, D12, D19-01.

**NON-NEGOTIABLE INVARIANTS:** Separación Identity/Representation/Definition. Relación informativa/no estructural/no composicional. Sin coupling temporal.

### HUMAN DECISION

**SELECTED ALTERNATIVE:** **A — Pure Domain Entity / Entidad Propia**

`ListingIdentity` será una **entidad propia del dominio Market Data / Instrument Identity**.

Su identidad física se determina mediante:

```text
identity_id
```

Las referencias al instrumento deben utilizar `identity_id`.

La entidad no debe convertirse en un Aggregate Root ni introducir una estructura de Aggregate artificial que contradiga D12.

La decisión debe preservar explícitamente:

```text
Identity
    ↓
ListingIdentity
    ↓
identity_id
```

como concepto de identidad canónica.

### WHAT THIS DECISION MEANS

1. `ListingIdentity` tiene existencia propia como entidad de dominio.
2. Su identidad canónica es `identity_id`.
3. No depende de `native_symbol` para existir.
4. No depende de `VenueConstraints`.
5. No depende de `DefinitionVersion`.
6. No depende de Representation.
7. `logical_identity_key` solamente proporciona lookup/reconciliación auxiliar.
8. El dominio no necesita introducir un Aggregate Root artificial para representar `ListingIdentity`.
9. La persistencia futura podrá implementar repositorios/adapters sin convertir la persistencia en una segunda fuente de verdad.
10. Las proyecciones históricas, Kafka, Iceberg y vistas/materializaciones futuras NO convierten automáticamente esas estructuras en otro estado canónico de identidad.

### ALTERNATIVES EVALUATION

#### Alternative A — Pure Domain Entity / Entidad Propia

**ACEPTADA / DECIDIDA**

Motivos principales:

* máxima coherencia con D12;
* mantiene Identity separada de Definition y Representation;
* compatible directamente con D19-01 C+;
* evita introducir un Aggregate Root artificial;
* evita introducir un estado runtime duplicado;
* evita sincronización entre registry runtime e histórico;
* evita complejidad operacional prematura;
* permite persistencia y repositorios sin convertirlos en una segunda SSOT;
* permite proyecciones históricas sin convertirlas en la identidad canónica;
* facilita evolución futura.

### Alternative B — Aggregate Root / Agregado

**NORMATIVAMENTE ELIMINADA / RECHAZADA**

Motivo:

La alternativa B introduce una estructura de Aggregate que contradice la separación arquitectónica establecida por D12 y no resulta necesaria para representar la identidad canónica.

NO reabrir esta alternativa.

### Alternative C — Runtime Registry + Historical Projection

**NO ELEGIDA / DIFERIDA**

No significa que sea imposible en el futuro.

Significa que actualmente no existe una necesidad arquitectónica demostrada que justifique introducir:

```text
Runtime Registry
        +
Historical Projection
```

como arquitectura estructural de identidad.

La posibilidad futura debe quedar explícitamente condicionada a una necesidad real y demostrable, por ejemplo:

* requisito de rendimiento medido;
* patrón operativo que requiera materialización especializada;
* necesidad de reconstrucción/lookup que justifique una proyección;
* requisitos de disponibilidad o escala que no puedan resolverse mediante la arquitectura A.

IMPORTANTE:

NO convertir umbrales arbitrarios como:

* P99 > X ms;
* X eventos/segundo;
* X FTE;
* X número de instrumentos;

en requisitos normativos de OCM.

La decisión futura debe basarse en evidencia real del sistema.

### INCOMPATIBLE ALTERNATIVES

**B — Aggregate Root / Agregado**

**WHY INCOMPATIBLE:** Aggregate root tiende estructuralmente a componer/embeber DefinitionVersion y/o VenueConstraints → transforma relación informativa (D12-B) en estructural/composicional, crea acoplamiento temporal (D12-C–G). No subsanable por disciplina (riesgo estructural).

**IMPACT ON OTHER D19 DECISIONS:** Afecta 03,04,05,06,07,10.

**IMPACT ON D12:** A,C: COHERENTE. B: INCOMPATIBLE.

**IMPACT ON HD-VC-1..7:** A,C: COHERENTE. B: INCOMPATIBLE (fusiona carriles/ventanas).

**IMPLEMENTATION CONSEQUENCES:** Límites de entidad/agregado, referencias lógicas vs composición. NO implementación.

**HUMAN DECISION MADE:** YES  
**DECISION:** A — Pure Domain Entity / Entidad Propia

---

## D19-03 — Physical DefinitionVersion / ContractTerms Model

**STATUS:** DECIDED — A (Pure Domain Entity + Effective Intervals / Entidad Propia + Intervalos Efectivos)  
**QUESTION:** ¿Cómo se materializan físicamente `DefinitionVersion` + `ContractTerms`, manteniendo separación respecto de ListingIdentity y preservando D12?

**WHY THIS DECISION EXISTS:** Modelar evolución definicional sin introducir VenueConstraints por conveniencia y sin fusionar ventanas con VenueConstraints.

**PREVIOUS DECISIONS THAT CONSTRAIN IT:** D05–D08, D12, HD-3, HD-VC-1 A+, D19-01,02.

**NON-NEGOTIABLE INVARIANTS:** Ejes independientes, informativa/no estructural/no composicional, sin coupling temporal. Prohibido introducir `tick_size,lot_size,min_notional`.

### HUMAN DECISION

**SELECTED ALTERNATIVE:** **A — Pure Domain Entity + Effective Intervals / Entidad Propia + Intervalos Efectivos**

`DefinitionVersion` se materializa como **entidad de dominio propia**.

`ContractTerms` pertenece al eje **Definition**.

Las versiones se distinguen mediante `definition_version`.

La vigencia se representa mediante un intervalo propio:

`[effective_from, effective_to)`

Los intervalos de Definition son **independientes** de `VenueConstraints`.

La Definition referencia `ListingIdentity` mediante el `identity_id` canónico.

`logical_identity_key` puede utilizarse únicamente como mecanismo auxiliar de lookup, nunca como sustituto de `identity_id`.

No se adopta Event Sourcing como mecanismo normativo de materialización de DefinitionVersion.

No se introduce un agregado que fusione Identity + Definition + VenueConstraints.

No se crea ninguna dependencia estructural Definition ↔ VenueConstraints.

### ALTERNATIVES EVALUATION

#### Alternative A — Pure Domain Entity + Effective Intervals / Entidad Propia + Intervalos Efectivos

**ACEPTADA / DECIDIDA**

Motivos principales:

* máxima coherencia con D12;
* DefinitionVersion como entidad propia con ContractTerms y vigencia temporal propia;
* intervalos de Definition independientes de VenueConstraints;
* Definition referencia ListingIdentity mediante identity_id canónico;
* logical_identity_key como lookup auxiliar (no sustituye identity_id);
* no Event Sourcing como mecanismo normativo;
* no agregado que fusione Identity + Definition + VenueConstraints;
* no dependencia estructural Definition ↔ VenueConstraints.

### Alternative B — Nested/Aggregate with ListingIdentity

**NORMATIVAMENTE ELIMINADA / RECHAZADA**

Motivo:

Anidada en agregado con ListingIdentity fuerza presión estructural a componer VenueConstraints → viola D12-B (no estructural/no composicional) y D12-C–G (sin coupling temporal, sin ventana conjunta obligatoria). Estructuralmente no subsanable.

NO reabrir esta alternativa.

### Alternative C — Event-Sourced (DefinitionChanged + Snapshots)

**NO ELEGIDA / DIFERIDA**

No significa que sea imposible en el futuro.

Significa que actualmente no existe una necesidad arquitectónica demostrada que justifique introducir:

```text
Event Sourcing
        +
Snapshots
```

como mecanismo normativo de materialización de DefinitionVersion.

La posibilidad futura debe quedar explícitamente condicionada a una necesidad real y demostrable, por ejemplo:

* requisito de auditoría forense completa demostrado;
* patrón operativo que requiera event sourcing nativo;
* necesidad de reconstrucción/lookup que justifique event sourcing;
* requisitos de disponibilidad o escala que no puedan resolverse mediante la arquitectura A.

IMPORTANTE:

NO convertir umbrales arbitrarios como:

* P99 > X ms;
* X eventos/segundo;
* X FTE;
* X número de instrumentos;

en requisitos normativos de OCM.

La decisión futura debe basarse en evidencia real del sistema.

### INCOMPATIBLE ALTERNATIVES

**B — Nested/Aggregate with ListingIdentity**

**WHY INCOMPATIBLE:** Anidado en agregado fuerza presión estructural a componer VenueConstraints → viola D12-B (no estructural/no composicional) y D12-C–G (sin coupling temporal, sin ventana conjunta obligatoria). Estructuralmente no subsanable.

**IMPACT ON OTHER D19 DECISIONS:** Afecta 06–10.

**IMPACT ON D12:** A: COHERENTE. B: INCOMPATIBLE. C: COHERENTE (con condición *).

**IMPACT ON HD-VC-1..7:** A,C: COHERENTE (C con condición *). B: INCOMPATIBLE.

**IMPLEMENTATION CONSEQUENCES:** Modelo de entidades/eventos vs agregado, ventanas independientes. NO implementación.

**HUMAN DECISION MADE:** YES  
**DECISION:** A — Pure Domain Entity + Effective Intervals / Entidad Propia + Intervalos Efectivos

**Condición (*) — C:** Eventos Definition vs VenueConstraints separados por carril semántico. Nunca fusionar ejes en mismo evento/agregado.

---

## D19-04 — Physical ValidityWindow

**STATUS:** DECIDED — B (Embedded Fields per Concept / Campos Embebidos Específicos por Concepto)  
**QUESTION:** ¿Cómo se materializa físicamente `ValidityWindow` y `SymbolBinding`, respetando HD-5 (`[from,to)`, `from<to`, `to=None` permitido, `from=None` no soportado, `from==to/to<from` inválidos)? ¿Reutilizar `ValidityWindow` para VenueConstraints?

**WHY THIS DECISION EXISTS:** Delimitar representación temporal (SymbolBinding+ValidityWindow) vs restricciones operativas (VenueConstraints). Reutilización para VenueConstraints **no decidida**.

**PREVIOUS DECISIONS THAT CONSTRAIN IT:** HD-5, D10, HD-8, D19-02,03. DSP §9,132: reutilización cuestión futura.

**NON-NEGOTIABLE INVARIANTS:** Reglas HD-5. D10 gobierna validez temporal de SymbolBinding, NO de VenueConstraints. Independencia de carriles.

### HUMAN DECISION

**SELECTED ALTERNATIVE:** **B — Embedded Fields per Concept / Campos Embebidos Específicos por Concepto**

`SymbolBinding` materializa su propia validez temporal mediante `valid_from` y `valid_to`, bajo semántica `[from,to)`, manteniendo independencia estructural respecto de la temporalidad de `DefinitionVersion` y de `VenueConstraints`.

Las reglas de intervalo pueden ser comunes como invariantes, pero no existe un `ValidityWindow` de dominio transversal que fuerce la reutilización estructural entre carriles arquitectónicos.

No se crea una entidad independiente `ValidityWindow`.

D19-05 conserva autoridad exclusiva sobre la materialización temporal de `VenueConstraints`.

### WHAT THIS DECISION MEANS

1. `SymbolBinding` materializa su propia validez temporal mediante `valid_from` y `valid_to`, bajo semántica `[from,to)`, manteniendo independencia estructural respecto de la temporalidad de `DefinitionVersion` y de `VenueConstraints`.
2. Las reglas de intervalo pueden ser comunes como invariantes, pero no existe un `ValidityWindow` de dominio transversal que fuerce la reutilización estructural entre carriles arquitectónicos.
3. No se crea una entidad independiente `ValidityWindow`.
4. D19-05 conserva autoridad exclusiva sobre la materialización temporal de `VenueConstraints`.

### ALTERNATIVES EVALUATION

#### Alternative A — Shared VO / VO Reutilizable
- **Definition:** VO reutilizable (compartido) para ventanas temporales.
- **Advantages:** DRY, consistencia reglas intervalo.
- **Disadvantages:** Riesgo acoplamiento entre carriles (DefinitionVersion/VenueConstraints).
- **Risks:** Medio.
- **Dependencies:** D19-02,03.
- **Architectural consequences:** Tipo común; riesgo fusionar ventanas.

#### Alternative B — Embedded Fields per Concept / Campos Embebidos Específicos por Concepto
- **Definition:** Campos embebidos (específicos por concepto), sin forzar abstracción común.
- **Advantages:** Evita acoplamiento entre carriles. Respeta semánticas distintas.
- **Disadvantages:** Ligero duplicado (justificado).
- **Risks:** Bajo.
- **Dependencies:** D19-02,03.
- **Architectural consequences:** Intervalos específicos por carril.

#### Alternative C
- **Definition:** DIFERIDO (post-D19/fase 2). No decidir reutilización ahora.
- **Advantages:** Reduce scope, evita decidir prematuramente.
- **Disadvantages:** Diferido.
- **Risks:** Bajo.
- **Dependencies:** D19-02,03.
- **Architectural consequences:** Aplazamiento controlado.

**INCOMPATIBLE ALTERNATIVES:** Ninguna.

**WHY INCOMPATIBLE:** N/A.

**IMPACT ON OTHER D19 DECISIONS:** Afecta 05,07,10.

**IMPACT ON D12:** A: REQUIERE CONDICIÓN (*). B,C: COHERENTE.

**IMPACT ON HD-VC-1..7:** A: REQUIERE CONDICIÓN. B,C: COHERENTE.

**IMPLEMENTATION CONSEQUENCES:** Abstracción vs duplicación, sin forzar ventana conjunta. NO implementación.

**HUMAN DECISION MADE:** YES  
**DECISION:** B — Campos Embebidos Específicos por Concepto

**Condición (*) — A:** Reutilización **no implica ni fuerza acoplamiento temporal obligatorio** entre DefinitionVersion y VenueConstraints (D12-G). No fusionar ventanas. Respetar D12-A–G. Si fuerza ventana conjunta obligatoria → deja de ser coherente.

---

## D19-05 — Physical VenueConstraints Model

**STATUS:** DECIDED — A (Independent VenueConstraints Domain Entity / Entidad Independiente de VenueConstraints)  
**QUESTION:** ¿Cómo se materializa físicamente `VenueConstraints` como eje independiente (`ListingIdentity + venue`), preservando HD-VC-1 A+, HD-VC-2–HD-VC-7, D12?

**WHY THIS DECISION EXISTS:** Materializar PRICE/SIZE/NOTIONAL (precision/increment/limits) con temporalidad propia `effective_at/effective_to`, evidence+provenance obligatorios, PIT+FAIL-CLOSED, sin componer con DefinitionVersion.

**PREVIOUS DECISIONS THAT CONSTRAIN IT:** HD-VC-1 A+, HD-VC-2–7, D12, D19-01,02,04.

**NON-NEGOTIABLE INVARIANTS:** `precision≠increment≠limits`, `min_notional≠min_order_usd`, ExchangeQuirks solo transporte. Temporalidad propia independiente. Sin segundo SSOT. Sin composición. Sin coupling temporal.

### HUMAN DECISION

**SELECTED ALTERNATIVE:** **A — Independent VenueConstraints Domain Entity / Entidad Independiente de VenueConstraints**

`VenueConstraints` se materializa como **entidad de dominio independiente**.

Su scope canónico es:

```text
(ListingIdentity, venue)
```

Su temporalidad operacional es independiente:

```text
[effective_at, effective_to)
```

con las invariantes temporales aplicables:

```text
effective_at < effective_to
```

cuando ambos extremos están presentes.

No se introduce en esta fase una decisión sobre:

* esquema físico;
* persistencia;
* backend;
* event sourcing;
* Kafka;
* registry;
* ports;
* APIs;
* evidencia/provenance;
* IDs;
* version counters;
* storage layout.

Esos asuntos permanecen para D19-06..D19-10.

### 6. D12 BOUNDARY

La materialización debe preservar explícitamente:

```text
Identity
    ≠
Definition
    ≠
Representation
    ≠
VenueConstraints
```

No fusionar estos conceptos.

La relación conceptual debe quedar documentada como:

```text
ListingIdentity
    ├── DefinitionVersion
    │      └── ContractTerms
    │
    ├── SymbolBinding
    │      └── valid_from / valid_to
    │
    └── VenueConstraints
           └── effective_at / effective_to
```

Esto es un modelo conceptual, NO una instrucción de implementación física.

### 7. DEFINITION BOUNDARY

Documentar que:

DefinitionVersion / ContractTerms

es responsable de términos contractuales/económicos de la definición del instrumento, incluyendo conceptualmente los elementos ya establecidos por las decisiones anteriores.

VenueConstraints

es responsable de restricciones operacionales propias de una venue.

Ejemplos conceptuales de VenueConstraints:

* tick size;
* quantity increment / lot size;
* min/max quantity;
* min/max notional;
* precision/increment constraints;
* límites operacionales específicos de la venue.

No congelar todavía una lista exhaustiva de campos.

### 8. SYMBOLBINDING BOUNDARY

Documentar explícitamente:

VenueConstraints is not embedded in, subordinated to, or temporally coupled to SymbolBinding.

SymbolBinding conserva su responsabilidad sobre la representación/símbolo de un listing en una venue y su propia validez temporal.

VenueConstraints conserva su propia dimensión operacional y temporal.

No crear un ValidityWindow compartido.

No fusionar:

valid_from / valid_to

con:

effective_at / effective_to

### 10. INDEPENDENT TEMPORAL AXES

La documentación debe preservar tres ejes temporales conceptualmente independientes:

DefinitionVersion
    effective_from / effective_to
SymbolBinding
    valid_from / valid_to
VenueConstraints
    effective_at / effective_to

No introducir un Value Object común ValidityWindow.

Las invariantes comunes pueden describirse conceptualmente, pero cada concepto conserva su propia semántica y campos temporales.

### 10. IMPORTANT SEMANTIC RULE

No escribir que VenueConstraints tiene una relación "cero" o "ninguna" con DefinitionVersion/SymbolBinding.

La formulación normativa correcta es:

VenueConstraints is conceptually related to the instrument model, but it is not embedded in, subordinated to, or temporally coupled to DefinitionVersion or SymbolBinding.

### 11. D19-06..D19-10

Después de materializar D19-05, mantener:

D19-06 = HUMAN DECISION REQUIRED / BLOCKED
D19-07 = HUMAN DECISION REQUIRED / BLOCKED
D19-08 = HUMAN DECISION REQUIRED / BLOCKED
D19-09 = HUMAN DECISION REQUIRED / BLOCKED
D19-10 = HUMAN DECISION REQUIRED / BLOCKED

NO anticipar ninguna de estas decisiones.

En particular:

D19-06

No decidir todavía el modelo definitivo de evidence/provenance.

D19-07

No decidir todavía persistence scope.

D19-08

No decidir backend/storage.

D19-09

No decidir eventos/Kafka.

D19-10

No decidir domain/application/port/registry boundary de implementación.

⸻

12. DOCUMENTOS A MATERIALIZAR

Primero localizar los documentos normativos reales utilizados por el pack D19.

Como mínimo revisar:

* docs/audits/D19_HUMAN_DECISION_PACK_2026-09-30.md
* docs/architecture/decisions/ADR-0049-canonical-instrument-identity-g1.md

y el tracking normativo correspondiente.

No asumir rutas adicionales.

Leer el contenido actual antes de editar.

Modificar únicamente los documentos estrictamente necesarios para materializar D19-05.

⸻

13. AUDIT PACK

Actualizar el Human Decision Pack para que registre inequívocamente:

D19-05 = DECIDED — A

y las cinco respuestas:

H-01 = YES
H-02 = YES
H-03 = YES
H-04 = YES
H-05 = YES

Debe quedar claramente distinguido:

Recommended Option

de:

Human Decision

La recomendación anterior deja de ser meramente recomendación porque ahora existe una decisión humana explícita.

⸻

14. ADR-0049

Materializar D19-05 en ADR-0049 únicamente en la medida necesaria para que el ADR refleje la decisión normativa.

Debe quedar documentado:

D19-05 = A — Independent VenueConstraints Domain Entity

con:

scope = (ListingIdentity, venue)
temporal axis = [effective_at, effective_to)

y la separación respecto a:

DefinitionVersion
SymbolBinding

No introducir implementación.

⸻

15. TRACKING

Actualizar el tracking normativo correspondiente para reflejar:

D19-05 = DECIDED

No modificar el estado de:

D19-06
D19-07
D19-08
D19-09
D19-10

salvo que el tracking existente requiera una actualización estrictamente derivada de la transición de D19-05.

No modificar Gate C a OPEN.

No autorizar implementación.

⸻

16. FINAL STATE

El estado normativo esperado después de la materialización es:

D19-01 = DECIDED — C+
D19-02 = DECIDED — A
D19-03 = DECIDED — A
D19-04 = DECIDED — B
D19-05 = DECIDED — A
D19-06..D19-10 = HUMAN DECISION REQUIRED / BLOCKED
GATE C = BLOCKED
IMPLEMENTATION = NOT AUTHORIZED

IMPORTANTE:

D19-05 decidido NO significa implementación autorizada.

La implementación permanece explícitamente bloqueada hasta completar las decisiones posteriores requeridas por el proceso.

⸻

16. VALIDACIÓN

Después de las modificaciones documentales:

* revisar diff de los archivos modificados;
* verificar que no haya cambios fuera del alcance;
* verificar que no se haya modificado código;
* verificar que no se hayan modificado tests;
* verificar que no se hayan alterado decisiones frozen;
* verificar que D19-06..D19-10 permanecen pendientes/bloqueados;
* verificar que Gate C permanece BLOCKED;
* verificar que Implementation permanezca NOT AUTHORIZED.

No ejecutar una batería de tests de código porque esta fase no debe modificar código.

⸻

18. SAFEOPS

Después de las modificaciones ejecutar únicamente validaciones read-only.

Confirmar:

* commit = NO
* push = NO
* merge = NO
* rebase = NO
* reset = NO
* clean = NO
* stash = NO

Si cualquier operación requerida implicara modificar la repository:

DETENERSE y reportarlo.

No intentar corregirlo automáticamente.

No intentar ocultarlo.

⸻

19. INFORME FINAL OBLIGATORIO

Entregar exclusivamente un reporte de materialización que incluya:

A. Decision Materialized

D19-05 = DECIDED — A

Human Answers

H-01 = YES
H-02 = YES
H-03 = YES
H-04 = YES
H-05 = YES

Files Modified

Enumerar únicamente los archivos realmente modificados.

Files Not Modified

Confirmar explícitamente:

Code = 0
Tests = 0
Schemas = 0
Kafka = 0
Iceberg = 0
Adapters = 0
APIs = 0

Governance State

D19-05 = DECIDED
D19-06..D19-10 = PENDING / BLOCKED
GATE C = BLOCKED
IMPLEMENTATION = NOT AUTHORIZED

SafeOps

Confirmar:

Commit = NO
Push = NO
Merge = NO
Rebase = NO
Reset = NO
Clean = NO
Stash = NO

⸻

20. STOP

Al terminar:

DETENERSE.

No implementar VenueConstraints.

No crear clases.

No crear schemas.

No crear ports.

No crear registry.

No crear eventos.

No modificar adapters.

No modificar Kafka/Iceberg.

No hacer commit.

No hacer push.

No hacer merge.

No hacer rebase.

No hacer reset.

No hacer clean.

No hacer stash.

La siguiente etapa será un nuevo Human Decision Pack para D19-06, y únicamente después de una decisión humana explícita.

STOP — WAIT FOR NEXT HUMAN INSTRUCTION.

---

## D19-06 — Evidence / Provenance / Identity Relations

**STATUS:** DECIDED — A (Independent Concepts / Conceptos Independientes)  
**QUESTION:** ¿Cómo se materializan físicamente `Evidence`, `Provenance` e `Identity Relations` (SUCCESSOR_OF/REPLACED_BY), preservando `Evidence≠Provenance`, `effective_at≠observed_at≠recorded_at`, `SOURCE≠AUTHORITY` y conservación no destructiva?

**WHY THIS DECISION EXISTS:** Garantizar trazabilidad auditable (HD-VC-5/6), sin destruir evidencia (conflict/correction/supersession/retraction). Relaciones tipadas con evidencia+provenance+temporalidad.

**PREVIOUS DECISIONS THAT CONSTRAIN IT:** HD-VC-5,6, D13,14, D19-01–05.

**NON-NEGOTIABLE INVARIANTS:** Evidence≠Provenance, tiempos distintos, SOURCE≠AUTHORITY, conservación no destructiva, FAIL-CLOSED.

### VALID ALTERNATIVES

#### Alternative A
- **Definition:** Campos embebidos por versión (VenueConstraints/DefinitionVersion).
- **Advantages:** Sencillo, as-of directo.
- **Disadvantages:** Reutilización limitada entre versiones/entidades.
- **Risks:** Bajo–Medio.
- **Dependencies:** D19-01–05.
- **Architectural consequences:** Embebido, as-of directo.

#### Alternative B
- **Definition:** Entidad `Evidence` separada + referencias (referenciable/reutilizable). Relations = edge con atributos (evidence+provenance+temporalidad+direccionalidad).
- **Advantages:** Normalizado, reutilizable, trazabilidad. Clara separación. Edge para Relations.
- **Disadvantages:** Join adicional.
- **Risks:** Bajo.
- **Dependencies:** D19-01–05.
- **Architectural consequences:** Entidades separadas + edges. Mejor trazabilidad.

#### Alternative C
- **Definition:** Append-only audit/event log (inmutable) por carril semántico. **Condición (*):** separación estricta por carriles (Definition vs VenueConstraints). Nunca fusionar ejes.
- **Advantages:** Auditoría forense, append-only, no destructivo (HD-VC-6). Soporta correcciones.
- **Disadvantages:** Proyección vs estado vigente.
- **Risks:** Medio.
- **Dependencies:** D19-01–05.
- **Architectural consequences:** Log append-only por carril + proyecciones.

**INCOMPATIBLE ALTERNATIVES:** Ninguna.

**WHY INCOMPATIBLE:** N/A.

**IMPACT ON OTHER D19 DECISIONS:** Afecta 07–10.

**IMPACT ON D12:** A,B,C: COHERENTE (C con condición *).

**IMPACT ON HD-VC-1..7:** A,B,C: COHERENTE (C con condición *).

**IMPLEMENTATION CONSEQUENCES:** Modelo embebido vs entidad separada vs log append-only. NO implementación.

### HUMAN DECISION

**SELECTED ALTERNATIVE:** **A — Independent Concepts / Conceptos Independientes**

Evidence y Provenance se modelan como **conceptos independientes** de soporte y auditoría. IdentityRelation es una **relación explícita de dominio** que representa SUCCESSOR_OF y REPLACED_BY con evidencia y provenance obligatorias.

Principio **Best of Both Worlds**:

conceptos independientes
        +
relaciones explícitas
        +
auditabilidad
        +
fail-closed
        +
separación estricta de responsabilidades.

NO interpretar "independiente" como "sin relación". La arquitectura correcta es:

ListingIdentity
      │
      ├──────────── DefinitionVersion
      ├──────────── SymbolBinding
      └──────────── VenueConstraints
Evidence ──────────┐
                   ├── sustenta / audita ── ListingIdentity
Provenance ────────┘
IdentityRelation
      │
      ├── SUCCESSOR_OF
      └── REPLACED_BY

Los conceptos pueden relacionarse, pero no deben fusionarse en un mega-agregado ni intercambiar responsabilidades.

**H-01 = YES** — Evidence y Provenance deben permanecer conceptualmente separados. Evidence representa los hechos, datos o elementos que sustentan una afirmación. Provenance representa el origen, lineage, fuente, autoridad, método y contexto de esos elementos. No deben fusionarse conceptualmente en un único concepto.

**H-02 = YES** — Evidence y Provenance deben permanecer conceptualmente independientes de ListingIdentity. Pueden relacionarse explícitamente con ListingIdentity para sustentar, justificar y auditar decisiones de identidad. NO convertir ListingIdentity + Evidence + Provenance en un único agregado conceptual. La decisión NO prescribe todavía persistencia, backend, esquema físico ni estructura de almacenamiento.

**H-03 = YES** — Ante evidencia faltante, evidencia insuficiente, evidencia conflictiva, provenance desconocida, autoridades conflictivas, información ambigua o relaciones de identidad contradictorias, la resolución de identidad debe permanecer fail-closed. No se debe degradar silenciosamente a una interpretación arbitraria. Conceptualmente deben poder existir estados como UNKNOWN, CONFLICT, BLOCKED según corresponda al caso. No diseñar todavía enums, clases, tablas, APIs ni código para estos estados.

**H-04 = YES** — IdentityRelation debe ser un concepto explícito de dominio. Debe representar explícitamente las relaciones ya establecidas: SUCCESSOR_OF y REPLACED_BY. NO inferir automáticamente una IdentityRelation a partir de cambios de DefinitionVersion, cambios de SymbolBinding o cambios de VenueConstraints. Una modificación contractual, simbólica u operativa NO constituye automáticamente un cambio de identidad. NO agregar nuevos tipos de IdentityRelation.

**H-05 = YES** — Evidence, Provenance e IdentityRelation deben permanecer semánticamente separados de DefinitionVersion, SymbolBinding y VenueConstraints. Los cambios contractuales pertenecen a DefinitionVersion / ContractTerms. Los cambios simbólicos pertenecen a SymbolBinding. Los cambios operativos de venue pertenecen a VenueConstraints. Las relaciones históricas de identidad pertenecen a IdentityRelation. Evidence y Provenance sirven para sustentar y auditar conclusiones; no sustituyen esos conceptos.

**NO decidido en D19-06 (permanecen para gates posteriores):**
- Persistencia, backend, esquema físico, storage layout, wire format.
- Diseño físico de estados UNKNOWN/CONFLICT/BLOCKED (enums, clases, tablas, APIs).
- D19-07 (Persistence Scope), D19-08 (Backend Strategy), D19-09 (Kafka/Events), D19-10 (Domain/App/Port boundaries).
- D15 EconomicGroup / cross-venue / economic equivalence.

**DECISION:** DECIDED — A (human decision, H-01..H-05 = YES)

**Condición (*) — C:** Separación estricta por carriles semánticos (DefinitionEvents vs VenueConstraintEvents). Nunca fusionar ejes/ventanas. Evidence≠Provenance mantenidos. Conservación no destructiva.

---

## D19-07 — Persistence Scope

**STATUS:** DECIDED — A — Durable Canonical State  
**QUESTION:** ¿Qué partes del modelo requieren conceptualmente persistencia frente a `runtime-only/cache/derived state/historical record/registry`? (Sin elegir backend D19-08)

**WHY THIS DECISION EXISTS:** Determinar alcance (necesidad) por concepto, diferenciando `LOGICAL DOMAIN ≠ RUNTIME MATERIALIZATION ≠ HISTORICAL PERSISTENCE ≠ TRANSPORT`.

**PREVIOUS DECISIONS THAT CONSTRAIN IT:** HD-4, HD-VC-7, HD-VC-4/5/6, D19-01–06.

**NON-NEGOTIABLE INVARIANTS:** No asumir 1:1 clase=tabla, entidad=tópico. Diferenciación obligatoria de capas.

### VALID ALTERNATIVES

#### Alternative A
- **Definition:** Runtime-only (Registry in-memory) + audit log histórico append-only.
- **Advantages:** Separación runtime/histórico, capas. Evidence append-only (no destructivo).
- **Disadvantages:** Restauración desde log.
- **Risks:** Medio.
- **Dependencies:** D19-01–06.
- **Architectural consequences:** Split runtime/audit. Evidence histórico preferente.

#### Alternative B
- **Definition:** Persistencia histórica completa (PIT obligatorio) para DefinitionVersion + VenueConstraints + Evidence (+ relacionados).
- **Advantages:** PIT completo, auditoría robusta, reconstrucción histórica.
- **Disadvantages:** Alcance mayor.
- **Risks:** Medio.
- **Dependencies:** D19-01–06.
- **Architectural consequences:** Histórico completo para conceptos con evolución+evidence.

#### Alternative C
- **Definition:** Híbrido (operativo caliente + histórico frío).
- **Advantages:** Equilibra rendimiento vs auditoría/PIT.
- **Disadvantages:** Proyecciones/sincronización.
- **Risks:** Medio.
- **Dependencies:** D19-01–06.
- **Architectural consequences:** Split por criticidad.

#### Alternative D
- **Definition:** DIFERIDO (no decidir alcance ahora).
- **Advantages:** Reduce scope.
- **Disadvantages:** Diferido.
- **Risks:** Bajo.
- **Dependencies:** D19-01–06.
- **Architectural consequences:** Aplazamiento controlado.

**INCOMPATIBLE ALTERNATIVES:** Ninguna.

**WHY INCOMPATIBLE:** N/A.

**Necesidad conceptual (sin selección):** ListingIdentity: depende (puede runtime+proyección). DefinitionVersion: probable histórico. VenueConstraints: probable histórico (PIT+evidence). SymbolBinding: depende. Evidence/Provenance/Relations: crítico (histórico preferente, append-only).

**IMPACT ON OTHER D19 DECISIONS:** Afecta 08–10.

**IMPACT ON D12:** A–D: COHERENTE.

**IMPACT ON HD-VC-1..7:** A–C: COHERENTE. D: COHERENTE.

**IMPLEMENTATION CONSEQUENCES:** Alcance por concepto (runtime vs histórico), no backend. NO implementación.

### HUMAN DECISION

**SELECTED ALTERNATIVE:** **A — Durable Canonical State**

Los siguientes siete conceptos forman parte del estado durable persistente del modelo canónico de identidad:

1. ListingIdentity
2. DefinitionVersion
3. SymbolBinding
4. VenueConstraints
5. Evidence
6. Provenance
7. IdentityRelation

Ninguno de estos conceptos debe depender exclusivamente de memoria/runtime si su pérdida impediría reconstruir, auditar o validar correctamente el estado canónico.

**H-07-01 = YES** — Durable Canonical State. Los siete conceptos forman parte del estado durable persistente del modelo canónico de identidad.

**H-07-02 = YES** — Independencia semántica. Los siete conceptos mantienen sus responsabilidades y ciclos de vida conceptualmente independientes. Identity ≠ Definition ≠ Representation ≠ VenueConstraints; Evidence ≠ Provenance; IdentityRelation ≠ DefinitionVersion/SymbolBinding/VenueConstraints. **Independencia semántica NO significa siete almacenes físicos, siete bases de datos, siete tablas, siete repositorios ni siete mecanismos de persistencia.** D19-07 decide el scope semántico de lo que debe ser durable, no la descomposición física del almacenamiento.

**H-07-03 = YES** — Durabilidad histórica y reconstruibilidad. La persistencia durable debe conservar suficiente información histórica para permitir reconstrucción del estado canónico, auditoría histórica, resolución Point-in-Time (PIT) donde corresponda, trazabilidad y comportamiento fail-closed. Deben preservarse los ejes temporales ya decididos: DefinitionVersion `[effective_from, effective_to)`; SymbolBinding `[valid_from, valid_to)`; VenueConstraints `[effective_at, effective_to)`. NO crear un `ValidityWindow` compartido. Los timestamps de Evidence/Provenance (`observed_at`, `recorded_at`, etc.) no sustituyen los ejes temporales anteriores.

**H-07-04 = YES** — Alcance tecnológico fuera de D19-07. D19-07 NO decide: PostgreSQL, Iceberg, DuckDB, Kafka, Redis, Event Sourcing, CQRS, ORM, tablas físicas, schemas físicos, columnas, índices, particiones, repositorios concretos, APIs, serialización, topics, consumer groups, adapters, deployment, infraestructura, implementación Python. Todo lo anterior queda para las decisiones posteriores correspondientes, especialmente D19-08, D19-09 y D19-10.

### PRECISIONES NORMATIVAS POR CONCEPTO

**ListingIdentity:** Debe tener persistencia durable. `identity_id` continúa siendo la referencia canónica estable definida en D19-01. No fusionar ListingIdentity con DefinitionVersion, SymbolBinding, VenueConstraints, Evidence, Provenance, IdentityRelation.

**DefinitionVersion:** Debe tener persistencia durable independiente. Debe conservar sus intervalos `[effective_from, effective_to)`. Una nueva DefinitionVersion NO implica automáticamente una nueva ListingIdentity.

**SymbolBinding:** Debe tener persistencia durable. Debe conservar su propio eje `[valid_from, valid_to)`. Un cambio de símbolo NO implica automáticamente cambio de identidad. No fusionar su temporalidad con DefinitionVersion.

**VenueConstraints:** Debe tener persistencia durable independiente. Scope conceptual: `(ListingIdentity, venue)`. Debe conservar `[effective_at, effective_to)`. VenueConstraints sigue siendo independiente de DefinitionVersion y SymbolBinding. No trasladar ContractTerms a VenueConstraints.

**Evidence:** Debe tener persistencia durable como concepto independiente. Evidence debe poder respaldar afirmaciones relacionadas con ListingIdentity, DefinitionVersion, SymbolBinding, VenueConstraints, IdentityRelation. No convertir Evidence en un simple campo de ListingIdentity. No fusionar Evidence con Provenance.

**Provenance:** Debe tener persistencia durable como concepto independiente. Debe preservar información de origen/linaje/autoridad/contexto/método según las decisiones previas. Mantener: Evidence ≠ Provenance. No convertir Provenance en un simple atributo de ListingIdentity.

**IdentityRelation:** Debe tener persistencia durable explícita. Solo están autorizadas conceptualmente estas relaciones: `SUCCESSOR_OF`, `REPLACED_BY`. Deben poder conservarse y consultarse como relaciones explícitas entre ListingIdentity. NO inferir IdentityRelation automáticamente a partir de: cambio de DefinitionVersion, cambio de SymbolBinding, cambio de VenueConstraints. No agregar nuevos tipos de relación.

### FAIL-CLOSED

La materialización debe conservar el principio D19-06: la pérdida, ausencia, insuficiencia o conflicto de evidencia/provenance no debe interpretarse silenciosamente como una identidad válida. Los estados conceptuales: UNKNOWN, CONFLICT, BLOCKED continúan siendo conceptuales. NO diseñar todavía enums, clases, tablas, schemas ni APIs para ellos.

**DECISION:** DECIDED — A — Durable Canonical State (human decision, H-07-01..H-07-04 = YES)

---

## D19-08 — Persistence / Backend Strategy

**STATUS:** DECIDED — D — Hybrid Relational + Iceberg  
**QUESTION:** ¿Qué alternativas de materialización física persistente (abstraídas vía Ports/Repositories), separando transporte/almacenamiento histórico/consulta/runtime registry/cache? (Sin imponer selección)

**WHY THIS DECISION EXISTS:** Identificar alternativas físicas sin forzar leak tecnológico. Principio dominio framework-agnostic.

**PREVIOUS DECISIONS THAT CONSTRAIN IT:** HD-4, BC-35, D19-07.

**NON-NEGOTIABLE INVARIANTS:** Abstraer vía Ports/Repositories. Diferenciar storage≠transport≠cache≠domain model. No asumir 1:1.

### VALID ALTERNATIVES

#### Alternative A
- **Definition:** Relacional (DB) + interval tables (effective windows).
- **Advantages:** PIT interval, ACID.
- **Disadvantages:** Menor time-travel masivo vs lakehouse.
- **Risks:** Bajo.
- **Dependencies:** D19-07.
- **Architectural consequences:** Interval tables, ACID, abstracción vía ports.

#### Alternative B
- **Definition:** Iceberg (lakehouse) + snapshots/time-travel/PIT.
- **Advantages:** Inmutable, time-travel, auditoría, favorece no destructivo (HD-VC-6).
- **Disadvantages:** Complejidad ops (no obligatorio).
- **Risks:** Medio.
- **Dependencies:** D19-07.
- **Architectural consequences:** Lakehouse, inmutabilidad, time-travel.

#### Alternative C
- **Definition:** Híbrido (A operativo caliente + B histórico/frío/auditoría).
- **Advantages:** Caliente/frío, equilibra rendimiento/auditoría.
- **Disadvantages:** Sincronización/proyecciones.
- **Risks:** Medio.
- **Dependencies:** D19-07.
- **Architectural consequences:** Split por uso, proyecciones.

#### Alternative D
- **Definition:** Registry in-memory + append-only audit log (archival) — abstraído vía Ports. No impone backend concreto.
- **Advantages:** Máxima abstracción (no fuerza leak). Compatible D19-07 A.
- **Disadvantages:** Restauración depende de log.
- **Risks:** Bajo.
- **Dependencies:** D19-07.
- **Architectural consequences:** Máxima abstracción, diferenciación de funciones.

**INCOMPATIBLE ALTERNATIVES:** Ninguna.

**WHY INCOMPATIBLE:** N/A.

**Separación por función (referencia):** Transporte: Kafka opcional (D19-09); Almacenamiento histórico: Iceberg/objeto/DB append-only; Consulta (PIT/as-of): interval/time-travel; Runtime Registry: in-memory; Cache: derivado (nunca SSOT).

**IMPACT ON OTHER D19 DECISIONS:** Afecta 09–10.

**IMPACT ON D12:** A–D: COHERENTE.

**IMPACT ON HD-VC-1..7:** A–D: COHERENTE.

**IMPLEMENTATION CONSEQUENCES:** Abstracción Ports/Repositories, sin imponer tecnología. NO implementación.

### HUMAN DECISION

**SELECTED ALTERNATIVE:** **C — Hybrid Relational + Iceberg** (referida como "D19-08 = D" en la decisión humana para alineación con nomenclatura de gates)

PostgreSQL será la **autoridad canónica operacional (canonical operational authority / SSOT)** del estado durable de identidad. Su responsabilidad conceptual es responder: "¿Cuál es el estado canónico vigente que OCM debe resolver operacionalmente?" Debe soportar la resolución operacional de: ListingIdentity, DefinitionVersion, SymbolBinding, VenueConstraints, Evidence, Provenance, IdentityRelation, respetando sus ejes temporales independientes.

Iceberg será una **representación derivada histórica/analítica del estado canónico**. Su responsabilidad conceptual es soportar: histórico, Point-in-Time analysis, auditoría, investigación, analytics, reconstrucción analítica, consultas históricas. **Iceberg NO es la autoridad canónica ni un domain write path independiente.** Los consumidores de dominio no deben escribir directamente en Iceberg para establecer identidad canónica.

Kafka será **transporte/eventing/propagación**, no canonical authority. Kafka puede transportar cambios, eventos, actualizaciones, intención de propagación, información necesaria para proyectar el estado hacia otros sistemas. **Kafka NO es el Source of Truth de identidad.** No se convierte D19-08 en una decisión de Event Sourcing.

DuckDB queda conceptualmente como **herramienta local de análisis, investigación y testing**. No será canonical authority. No será durable SSOT.

**Principio fundamental:** Single Source of Truth no significa Single Storage System. La arquitectura puede utilizar varios sistemas siempre que cada uno tenga una responsabilidad inequívoca:

| Sistema    | Responde principalmente                          |
| ---------- | ------------------------------------------------ |
| PostgreSQL | ¿Cuál es el estado canónico operacional?         |
| Kafka      | ¿Qué cambio/evento debe propagarse?              |
| Iceberg    | ¿Qué estado histórico/analítico puedo consultar? |
| DuckDB     | ¿Cómo analizo/investigo localmente esos datos?   |

**No dual-write:** El dominio no debe escribir independientemente el mismo estado canónico tanto en PostgreSQL como en Iceberg. La arquitectura conceptual es:

```text
                ┌──────────────────────┐
                │ Canonical Domain     │
                │ Instrument Identity  │
                └──────────┬───────────┘
                           │
                           ▼
                ┌──────────────────────┐
                │ PostgreSQL           │
                │ Canonical Authority  │
                └──────────┬───────────┘
                           │
                     propagation
                           │
                           ▼
                ┌──────────────────────┐
                │ Kafka                │
                │ Transport / Events   │
                └──────────┬───────────┘
                           │
                     projection
                           │
                           ▼
                ┌──────────────────────┐
                │ Iceberg              │
                │ Historical / Analytic│
                └──────────────────────┘

                ┌──────────────────────┐
                │ DuckDB               │
                │ Local Analysis/Test  │
                └──────────────────────┘
```

**Fail-closed responsibility:** PostgreSQL proporciona primitivas de persistencia, atomicidad y consistencia que soportan la estrategia canónica, mientras que el comportamiento fail-closed pertenece a las reglas de dominio, resolución y acceso al registry. Integridad física/backend ≠ fail-closed semántico; constraints de DB ≠ resolución de identidad; PostgreSQL no decide identidad; el registry/resolver mantiene las reglas de dominio.

**Temporalidad — Distinción obligatoria:**
- **Domain Time:** Los conceptos de D19 poseen ejes temporales propios: DefinitionVersion `effective_from / effective_to`; SymbolBinding `valid_from / valid_to`; VenueConstraints `effective_at / effective_to`. Representan tiempo semántico/empresarial del instrumento.
- **Storage Time:** Iceberg puede proporcionar snapshot/time-travel del almacenamiento. **Iceberg Time Travel NO equivale al Point-in-Time semántico del dominio.** No mezclar ambos conceptos.

**Benchmark externo (rationale, no decisiones copiadas):**
- **CME Security Definition:** La identidad/definición debe basarse en información estructurada de definición del instrumento proporcionada por la autoridad de mercado, no en inferencias desde un símbolo.
- **FIX:** Un estándar de intercambio puede normalizar la representación y distribución de definiciones, pero el protocolo de transporte no debe convertirse automáticamente en la autoridad canónica de identidad de OCM.
- **OpenFIGI:** OCM debe poder relacionar identificadores externos con su identidad canónica. FIGI NO debe convertirse automáticamente en ListingIdentity. ExternalIdentifier / identifier mapping queda identificado como futura decisión arquitectónica.
- **NautilusTrader:** La separación entre identidad interna, venue y símbolo nativo es una referencia válida para un sistema multi-venue. Compatible con ListingIdentity, SymbolBinding, Venue, native_symbol.
- **QuantConnect / LEAN:** Un identificador interno estable puede ser muy útil para evitar que el sistema dependa de representaciones externas cambiantes. Relacionado con el carácter opaco/estable de listing_id.
- **Freqtrade:** Los símbolos/pair names son útiles para routing y compatibilidad con exchanges, pero no constituyen por sí solos una identidad canónica universal. Refuerza: Identity ≠ Representation.

OCM combina sus mejores principios manteniendo una separación más estricta: Identity ≠ Definition ≠ Representation ≠ Venue Constraints ≠ Evidence ≠ Provenance. IdentityRelation permanece como concepto explícito.

**Future multi-market:** La estrategia de backend debe soportar un registry canónico multi-market sin depender de la semántica particular de un exchange, broker o asset class. Compatible conceptualmente con: crypto, spot, perpetuals, futures, options, forex, equities, indices, commodities, otros instrumentos negociables.

**D15 exclusion:** NO incorporar EconomicGroup, cross-venue equivalence, economic equivalence, underlying equivalence, FIGI-based equivalence, automatic successor inference. D15 permanece fuera de alcance.

**Outbox / CDC:** La propagación desde la autoridad canónica hacia sistemas derivados deberá poseer garantías de entrega durable y consistencia operacional. Transactional Outbox / Debezium / CDC / Kafka Connect / Flink son patrones candidatos para decisión posterior (D19-09/D19-10). NO se decide mecanismo concreto aquí.

**DECISION:** DECIDED — D — Hybrid Relational + Iceberg (human decision, Alternative C selected)

---

## D19-09 — Event Stream / Kafka Materialization

**STATUS:** DECIDED — B — Domain Events by Semantic Lane with Optional Transport  
**QUESTION:** ¿Requiere G1 necesariamente eventos/Kafka/topics? Diferenciar `dominio lógico ≠ transporte ≠ persistencia histórica ≠ runtime`.

**WHY THIS DECISION EXISTS:** Evitar asumir entidad=tópico, dominio=>Kafka, persistencia=>Kafka. Evaluar necesidad preservando separación carriles (D12).

**PREVIOUS DECISIONS THAT CONSTRAIN IT:** BC-35, HD-VC-6, D19-06,07,08.

**NON-NEGOTIABLE INVARIANTS:** Nunca forzar 1:1 dominio→topic. Si eventos, separación estricta por carriles (Definition vs VenueConstraints). Kafka opcional (no obligatorio).

### VALID ALTERNATIVES

#### Alternative A
- **Definition:** NO obligatorio (solo dominio+persistencia) — snapshots + append-only audit log.
- **Advantages:** Mínimo acoplamiento, simplicidad, capas HD-4. No asume entidad=tópico.
- **Disadvantages:** Menor granularidad event-level si no requerido.
- **Risks:** Bajo.
- **Dependencies:** D19-06,07,08.
- **Architectural consequences:** Sin event stream obligatorio, snapshots+log.

#### Alternative B
- **Definition:** Domain events por carriles separados (DefinitionEvents vs VenueConstraintEvents) + Kafka opcional. **Condición (*):** separados por carril, Kafka opcional/no obligatorio, nunca 1:1 dominio→topic, schemas por carril (BC-35), nunca fusionar ejes/ventanas.
- **Advantages:** Trazabilidad (HD-VC-6), replay/PIT. Protege independencia D12. Kafka opcional.
- **Disadvantages:** Complejidad (taxonomy/proyecciones/ordering). Riesgo imponer obligatoriedad.
- **Risks:** Medio.
- **Dependencies:** D19-06,07,08.
- **Architectural consequences:** Event stream opcional por carriles separados.

#### Alternative C
- **Definition:** DIFERIDO (no decidir ahora).
- **Advantages:** Reduce scope, evita sobredimensionamiento.
- **Disadvantages:** Diferido.
- **Risks:** Bajo.
- **Dependencies:** D19-06,07,08.
- **Architectural consequences:** Aplazamiento controlado.

**INCOMPATIBLE ALTERNATIVES:** Ninguna.

**WHY INCOMPATIBLE:** N/A.

**IMPACT ON OTHER D19 DECISIONS:** Afecta 10.

**IMPACT ON D12:** A,C: COHERENTE. B: COHERENTE solo con condición (*).

**IMPACT ON HD-VC-1..7:** A,C: COHERENTE. B: COHERENTE solo con condición (*).

**IMPLEMENTATION CONSEQUENCES:** Transporte opcional vs obligatorio, separación carriles. NO implementación.

### HUMAN DECISION

**SELECTED ALTERNATIVE:** **B — Domain Events by Semantic Lane with Optional Transport**

OCM modela cambios canónicos relevantes mediante **Domain Events** como conceptos del dominio, organizados por responsabilidad semántica.

Definition y VenueConstraints permanecen semánticamente separados, consistentes con D12 y D19-05. IdentityRelation permanece como concepto semántico separado consistente con D19-06.

Los Domain Events son conceptos del dominio y **no dependen semánticamente** de Kafka ni de ningún mecanismo de transporte específico.

Kafka puede utilizarse como mecanismo de transporte/eventing cuando sea requerido, pero **Kafka no es la autoridad canónica** y **no es obligatorio** para la existencia o significado de un Domain Event.

PostgreSQL continúa siendo la autoridad operacional canónica según D19-08.

Iceberg continúa siendo una representación histórica/analítica derivada.

La implementación física del mecanismo de propagación (Transactional Outbox, CDC, Dual Write, etc.), los schemas físicos de eventos, la topología de topics, el particionamiento, la implementación de delivery, la implementación de idempotencia, la estrategia de reconciliación y los parámetros operacionales **quedan diferidos a decisiones arquitectónicas posteriores** (D19-10 y gates posteriores).

**NO se decide en D19-09:**
- Transactional Outbox / CDC / Dual Write / Event Sourcing
- Kafka topics, partitioning, consumer groups
- Schemas físicos (Avro, Protobuf, JSON), Schema Registry
- Event envelope físico, event ID concreto (UUID v7, etc.)
- Delivery implementation, idempotency implementation
- Retry policies, reconciliation mechanism
- SLAs, RPO, RTO
- Infraestructura concreta

Todo lo anterior queda diferido a decisiones posteriores, particularmente D19-10 y gates posteriores.

**Caractéres normativos de la decisión:**

1. **Domain Events** existen como conceptos del dominio para cambios canónicos relevantes.
2. **Carriles semánticos**: Definition, VenueConstraints e IdentityRelation mantienen separación semántica (Definition ≠ VenueConstraints ≠ IdentityRelation).
3. **Kafka es opcional** — transporte pluggable, no requisito ni autoridad.
4. **PostgreSQL = Canonical Operational Authority** (D19-08).
5. **Iceberg = Derived Historical/Analytical Representation** (D19-08).
6. **Mecanismo físico de propagación** (Transactional Outbox, CDC, etc.) = D19-10 y gates posteriores.

**DECISION:** DECIDED — B — Domain Events by Semantic Lane with Optional Transport (human decision)

**Condición (*) — B:** Separación estricta por carriles semánticos (Definition vs VenueConstraints ≠ IdentityRelation). Nunca fusionar ejes/ventanas (D12). Kafka **opcional, no obligatorio**. **Nunca forzar 1:1** dominio→topic. Schemas por carril semántico (BC-35). Si falla → INCOMPATIBLE.

---

## D19-10 — Domain / Application / Port / Registry Boundary

**STATUS:** DECIDED — C — Hybrid Core Registry + Lane-Specific Extensions  
**QUESTION:** ¿Extender `instrument_identity_registry` existente o crear port outbound específico para VenueConstraints? ¿Cómo preservar HD-4 (`PORT/REGISTRY = INFORM`)?

**WHY THIS DECISION EXISTS:** Delimitar boundaries físicos sin crear/renombrar prematuramente, manteniendo PORT/REGISTRY = INFORM (sin decisiones semánticas).

**PREVIOUS DECISIONS THAT CONSTRAIN IT:** HD-4, HD-VC-7, registry existente `packages/market_data/ports/outbound/instrument_identity_registry.py`, D19-01–09.

**NON-NEGOTIABLE INVARIANTS:** `DOMAIN=DECIDE`, `APPLICATION=ORCHESTRATE`, `PORT/REGISTRY=INFORM`, `ADAPTER=TRANSLATE/PROVIDE EVIDENCE`, `Consumers=CONSUME`. Adapter no interpreta identidad, Application no redefine política semántica.

### VALID ALTERNATIVES

#### Alternative A
- **Definition:** Extender registry/port existente (`instrument_identity_registry`) (mínimo cambio conceptual). **Condición (*):** mantener estrictamente PORT/REGISTRY=INFORM, **no fusionar responsabilidades de carriles** (Identity/Definition vs VenueConstraints), no convertir port en agregado de carriles, no tomar decisiones semánticas.
- **Advantages:** Mínimo movimiento, cohesión con identidad.
- **Disadvantages:** Riesgo expandir alcance mezclando carriles si sin disciplina.
- **Risks:** Bajo–Medio.
- **Dependencies:** D19-01–09.
- **Architectural consequences:** Extensión controlada con disciplina estricta.

#### Alternative B
- **Definition:** Separar port outbound específico para VenueConstraints (claridad de frontera de carriles).
- **Advantages:** Refuerza independencia ejes D12. Claridad de frontera carriles. Reduce mezclar responsabilidades.
- **Disadvantages:** Proliferación controlada de ports.
- **Risks:** Bajo.
- **Dependencies:** D19-01–09.
- **Architectural consequences:** Separación por frontera de carriles.

#### Alternative C
- **Definition:** DIFERIDO (no decidir ahora).
- **Advantages:** Respeta “nombres definitivos no cerrados”. Evita crear/renombrar prematuramente.
- **Disadvantages:** Diferido.
- **Risks:** Bajo.
- **Dependencies:** D19-01–09.
- **Architectural consequences:** Aplazamiento controlado.

**INCOMPATIBLE ALTERNATIVES:** Ninguna.

**WHY INCOMPATIBLE:** N/A.

**IMPACT ON OTHER D19 DECISIONS:** Ninguna (cierre de boundaries).

**IMPACT ON D12:** A: REQUIERE CONDICIÓN (*). B,C: COHERENTE.

**IMPACT ON HD-VC-1..7:** A,B,C: COHERENTE (preservan HD-4/HD-VC-7).

**IMPLEMENTATION CONSEQUENCES:** Boundary físico, sin crear/renombrar obligatorio ahora. NO implementación.

### HUMAN DECISION

**SELECTED ALTERNATIVE:** **C — Hybrid Core Registry + Lane-Specific Extensions**

La arquitectura conceptual decidida es:

**Core Registry:** `InstrumentIdentityRegistryPort` permanece como contrato core para las responsabilidades que forman naturalmente parte de la resolución de identidad:
- ListingIdentity
- SymbolBinding
- `get_identity` / resolución equivalente ya existente

El objetivo es preservar la cohesión del caso de uso de resolución de identidad.

**Lane-Specific Extensions:** Los conceptos que ya fueron declarados independientes en D19-03, D19-05 y D19-06 mantienen contratos separados conceptualmente:
- DefinitionVersion → `DefinitionVersionRegistryPort`
- VenueConstraints → `VenueConstraintsRegistryPort`
- Evidence + Provenance → `EvidenceProvenanceRegistryPort`
- IdentityRelation → `IdentityRelationRegistryPort`

La separación debe entenderse como **separación de responsabilidad**, no como obligación de utilizar stores físicos separados.

### RESPONSIBILITY BOUNDARIES

**Domain:** Es dueño de significado semántico, invariantes, identidad, DefinitionVersion, VenueConstraints, Evidence, Provenance, IdentityRelation, fail-closed behavior.

**Application:** Es responsable de casos de uso, orquestación, coordinación entre componentes, utilización de ports. Application NO se convierte en dueño de la semántica del dominio.

**Ports:** Son contratos arquitectónicos. No deben: decidir identidad, inferir relaciones, decidir DefinitionVersion, decidir VenueConstraints, convertirse en autoridad semántica.

**Registry:** Debe respetar `PORT/REGISTRY = INFORM`. El Registry NO decide semántica.

**Adapters:** Son responsables de: proporcionar capacidades externas, traducir representaciones, aportar evidencia cuando corresponda, ocultar detalles tecnológicos.

### PRESERVING PREVIOUS DECISIONS

**HD-4:** `PORT/REGISTRY = INFORM` — El Registry no es autoridad semántica.

**HD-VC-7:** VenueConstraints = domain-owned; Registry/Port = INFORM; Adapter = TRANSLATE/PROVIDE EVIDENCE.

**D19-08:** PostgreSQL = canonical operational authority / SSOT; Kafka = transport / propagation; Iceberg = derived historical / analytical representation; DuckDB = local analysis/testing. Multiple Ports ≠ Multiple Authorities. Todos los contracts conceptualmente acceden/coordinan alrededor del mismo canonical state.

**D19-09:** Domain Events = domain concepts; Domain Events ≠ Kafka. La separación por semantic lanes en los ports debe ser coherente con la separación semántica ya decidida para Domain Events. Pero NO afirmar que cada port implica automáticamente un Kafka topic, consumer group, partición, schema físico, transporte obligatorio. Eso sigue diferido.

### DEFERRED DECISIONS

D19-10 NO decide:
- Transactional Outbox / CDC / Debezium / Event Sourcing
- Kafka topics / partitions / consumer groups
- Event schemas (Avro / Protobuf / JSON) / Schema Registry
- Event envelope / event ID format / UUID version
- Delivery guarantees / retries / DLQ / idempotency implementation / reconciliation
- PostgreSQL schema / Alembic migrations / Iceberg schema
- Deployment topology / Kubernetes / Terraform / concrete adapter implementation

Todos permanecen: `DEFERRED / FUTURE DECISION`.

**DECISION:** DECIDED — C — Hybrid Core Registry + Lane-Specific Extensions (human decision)

**Condición (*) — A:** Mantener estrictamente `PORT/REGISTRY = INFORM` (sin tomar decisiones semánticas). **No fusionar responsabilidades de carriles** (Identity/Definition vs VenueConstraints). No convertir port en agregado de carriles. Preservar boundaries (DOMAIN DECIDE, APPLICATION ORCHESTRATE, ADAPTER TRANSLATE/PROVIDE EVIDENCE). Si falla → deja de ser coherente.

---
 
## D20 — SPLIT GATES

**STATUS:** DECIDED — SPLIT GATES  
**QUESTION:** ¿Cómo debe descomponerse arquitectónicamente el siguiente gate después de D19-10 para avanzar hacia la implementación del Canonical Instrument Identity Registry sin mezclar decisiones arquitectónicas con autorización de implementación?

**WHY THIS DECISION EXISTS:** D19-01..D19-10 establecieron la arquitectura conceptual completa del Canonical Instrument Identity Registry, pero las decisiones físicas de propagación, persistencia, eventos y esquemas permanecen abiertas. Un único mega-gate crearía una decisión demasiado grande; posponer indefinidamente crearía deuda arquitectónica.

**PREVIOUS DECISIONS THAT CONSTRAIN IT:** D19-01..D19-10, HD-4, HD-VC-7, D19-08, D19-09, D19-10.

**NON-NEGOTIABLE INVARIANTS:** Architecture Decision ≠ Implementation Authorization. PostgreSQL = canonical operational authority. Kafka = optional transport. Iceberg = derived projection. Domain Events ≠ Kafka. Domain Events por semantic lanes (Definition, VenueConstraints, IdentityRelation).

### HUMAN DECISION

**SELECTED ALTERNATIVE:** **SPLIT GATES — D20a + D20b**

D20 no será un único mega-gate. Se divide en dos gates arquitectónicos secuenciales:

### D20a — Propagation Architecture + Domain Event Semantics

**Pregunta:** ¿Cómo deben propagarse los cambios canónicos del Instrument Identity Registry y cuál es la semántica arquitectónica de los Domain Events utilizados para dicha propagación?

**Alcance normativo:**

**Propagation Architecture**
- Mecanismo de propagación (Transactional Outbox vs CDC)
- Relación entre persistencia canónica (PostgreSQL) y eventos
- Propagación hacia representaciones derivadas (Iceberg, consumers)
- Autoridad y ownership
- Ordering conceptual
- Delivery semantics
- Idempotency conceptual
- Replay y reconciliation conceptual

**Domain Event Semantics**
- Relación Domain Event ↔ canonical change
- Semantic lanes (Definition, VenueConstraints, IdentityRelation)
- Event identity
- Event envelope
- Versioning
- Schema evolution conceptual

**Estado:** D20a = DECIDED — Transactional Outbox + Domain Event Propagation Semantics

### D20a-01 — Propagation Mechanism

**Decisión:** Transactional Outbox

La arquitectura debe establecer que el cambio canónico y la intención de publicación del evento deben quedar vinculados de forma transaccional a la autoridad canónica.

Conceptualmente:

Canonical Domain State Change
            +
Domain Event Publication Intent
            ↓
      Same PostgreSQL
       transaction
            ↓
          COMMIT

Esto evita la inconsistencia arquitectónica fundamental:

canonical state committed
+
event lost

No materializa todavía: nombre de tabla, columnas, índices, retention, polling, relay, CDC, Kafka topics, schema físico. Todo eso queda para D20b/Implementation.

### D20a-02 — Semantic Lanes

Materializa como lanes conceptuales:

DefinitionEvents
VenueConstraintEvents
IdentityRelationEvents

Estas lanes corresponden a las semánticas establecidas por D19-09.

Debe quedar explícito:

- las lanes son semánticas;
- no implican necesariamente Kafka topics;
- no implican partitions;
- no implican consumer groups;
- no implican una implementación física concreta.

La separación:

Definition
VenueConstraints
IdentityRelation

debe permanecer conceptualmente independiente.

### D20a-03 — Ordering

Materializa como requisito arquitectónico:

Ordering lógico por ListingIdentity dentro de cada semantic lane.

Es decir:

ListingIdentity + Semantic Lane

es el ámbito lógico de ordering.

NO convierta todavía esto en: Kafka partition key, database index, physical sequence implementation, consumer partition, infrastructure configuration. Esas decisiones quedan para D20b/Implementation.

### D20a-04 — Event Identity

Materializa la necesidad de una identidad de evento estable y única.

La recomendación de análisis fue:

UUIDv7 + sequence_number

Pero NO congela prematuramente detalles físicos.

Documenta:

Event Identity

Debe existir una identidad única de evento.

Logical Ordering

Debe existir una capacidad de establecer ordering dentro del ámbito:

ListingIdentity + Semantic Lane

Sequence

Un sequence_number puede utilizarse como mecanismo de ordering lógico, pero su representación física, generación y persistencia quedan para D20b/Implementation.

UUIDv7

UUIDv7 puede ser utilizado como candidato/recomendación para identidad física del evento, pero NO convierta UUIDv7 en una obligación de schema físico de D20a.

La regla debe ser:

D20a decide la semántica de identidad y ordering; D20b/Implementation decide su representación física.

### D20a-05 — Delivery Semantics

Materializa:

At-least-once delivery + consumer idempotency

como requisito arquitectónico.

Debe quedar claro:

Duplicate delivery
       ↓
Expected / tolerated
       ↓
Consumer must be idempotent

No diseñe todavía: deduplication table, unique constraints, Kafka producer configuration, consumer implementation, retry library, DLQ, offsets. Esos elementos quedan para fases posteriores.

### D20a-06 — Replay

Materializa:

Replay capability = REQUIRED

Debe soportarse conceptualmente:

- full replay;
- selective replay;
- recovery/reprocessing.

No decide todavía: mecanismo físico, almacenamiento concreto, retention period, Kafka retention, Iceberg reconstruction procedure, tooling.

Distingue explícitamente:

replay de Domain Events

de:

reconstrucción desde una representación física.

La primera es una propiedad arquitectónica de D20a.

La segunda queda para D20b/Implementation.

### D20a-07 — Reconciliation

Materializa:

Reconciliation = REQUIRED

Debe existir una capacidad arquitectónica de detectar divergencias entre:

Canonical PostgreSQL
        ↓
Propagation
        ↓
Derived Consumers / Projections

Debe contemplar conceptualmente:

- detección de lag;
- detección de eventos faltantes;
- detección de divergencias;
- recuperación/reparación.

IMPORTANTE:

NO congele como decisión normativa de D20a:

"daily full sync"

ni ninguna frecuencia concreta.

La frecuencia: daily, hourly, continuous, on-demand es una decisión operacional/implementativa que puede depender del SLA y del diseño físico.

Por tanto documenta:

Continuous/operational lag detection and periodic/full reconciliation are required capabilities, while their concrete cadence and implementation are deferred.

### D20a — Failure Semantics

Materializa los principios arquitectónicos:

Caso 1

PostgreSQL commit
+
Propagation temporarily unavailable

Resultado:

El cambio canónico NO se pierde; debe permanecer propagable.

Caso 2

Event delivered
+
Consumer fails

Resultado:

Retry/reprocessing debe ser posible.

Caso 3

Duplicate event

Resultado:

Consumer debe tolerar/reprocesar idempotentemente.

Caso 4

Out-of-order event

Resultado:

El sistema debe detectar/gestionar ordering según el ámbito definido para ListingIdentity + lane.

Caso 5

Derived projection unavailable

Resultado:

PostgreSQL continúa siendo autoridad; la proyección se recupera posteriormente.

No convierta estos principios en implementación física.

### D20a — Domain Event vs Database Change

Materializa explícitamente:

Un cambio físico de base de datos NO es automáticamente un Domain Event.

Debe mantenerse la distinción:

Database Change
    ≠ necesariamente ≠
Domain Event

Un Domain Event representa una semántica relevante del dominio.

Ejemplos:

DefinitionEvent
VenueConstraintEvent
IdentityRelationEvent

CDC puede detectar cambios físicos, pero D20a no debe redefinir Domain Events como simples row changes.

Esto es una razón arquitectónica importante para la elección de Transactional Outbox.

### D20a — Dual Write

Materializa explícitamente que:

Dual Write NO es una arquitectura autorizada para resolver la autoridad canónica.

La razón es la decisión previa D19-08:

PostgreSQL = SSOT
Kafka       = transport
Iceberg     = projection

No deben existir dos escrituras independientes que creen dos posibles autoridades.

### D20a — D20a vs D20b

Actualiza claramente la frontera.

D20a DECIDE

- propagation architecture;
- Transactional Outbox;
- Domain Event semantics;
- semantic lanes;
- ordering semantics;
- delivery semantics;
- idempotency requirement;
- replay requirement;
- reconciliation requirement;
- event identity semantics;
- event evolution principles;
- failure semantics.

D20b DECIDE

- PostgreSQL physical schema;
- Outbox table structure;
- indexes;
- retention;
- physical event schema;
- serialization format;
- Avro/Protobuf/JSON;
- Schema Registry;
- Iceberg schemas;
- partitioning;
- physical persistence representation.

IMPLEMENTATION DECIDES

- relay implementation;
- adapters;
- ports;
- CompositionRoot wiring;
- Kafka configuration;
- consumer implementation;
- deployment;
- monitoring implementation;
- retries;
- DLQ;
- concrete reconciliation jobs.

NO adelante ninguna de estas decisiones.

**Estado:** D20a = DECIDED — Transactional Outbox + Domain Event Propagation Semantics

**NO decide en D20a:**

- Outbox vs CDC (es la decisión de D20a, ya resuelta: Transactional Outbox)
- Formato físico de evento (Avro/Protobuf/JSON)
- Schema Registry concreto
- Delivery guarantees concretas
- Kafka topics/partitions/consumer groups
- Implementación física

### D20b — Physical Persistence / Representation Architecture + Implementation Boundary

**Pregunta:** ¿Cómo deben representarse físicamente el estado canónico, las proyecciones históricas y los eventos, y cuál es la frontera entre la decisión arquitectónica y la futura implementación?

**Alcance normativo:**

**PostgreSQL**
- Physical persistence model
- Relational representation
- Temporal representation
- Constraints, indexes
- Migration strategy (Alembic vs raw SQL)
- Canonical persistence boundaries

**Iceberg**
- Projection strategy
- Historical representation
- Schema strategy
- Snapshot relationship
- Domain Time vs Storage Time

**Event Representation**
- Physical event schema
- Serialization format (Avro/Protobuf/JSON)
- Schema governance
- Compatibility policy
- Schema Registry (si corresponde)

**Ports / Adapters**
- Port implementation boundary (arquitectura de implementación vs contratos)
- Adapter architecture (no implementación concreta)

**Implementation Boundary**
- Architecture Decision ≠ Implementation Authorization
- D20b define arquitectura necesaria para implementar
- IMPLEMENTATION = NOT AUTHORIZED hasta gate específico

**Estado:** DECIDED — Physical Persistence / Representation Architecture

### HUMAN DECISION

**SELECTED ALTERNATIVE:** **DECIDED — Physical Persistence / Representation Architecture**

**D20b-01 — PostgreSQL Canonical Representation**

**DECIDED**

Relational canonical model with explicit temporal semantics.

Debe quedar establecido que PostgreSQL representa la autoridad canónica operacional para el dominio de Instrument Identity y sus conceptos relacionados.

La representación debe soportar conceptualmente:

* ListingIdentity;
* DefinitionVersion;
* VenueConstraints;
* Evidence;
* Provenance;
* IdentityRelation;
* SymbolBinding;
* historical durability;
* Point-in-Time;
* temporal independence where required by D19 decisions.

NO decidir ni materializar:

* tablas físicas;
* DDL;
* triggers;
* índices;
* ORM;
* Alembic;
* nombres físicos de relaciones;
* SQL.

Estos permanecen:

IMPLEMENTATION DEFERRED.

**D20b-02 — Transactional Outbox Architecture**

**DECIDED**

Single logical Transactional Outbox.

Debe quedar explícitamente preservado D20a:

Canonical State
      +
Event Intention
      ↓
Same PostgreSQL Transaction

La decisión significa:

* existe una única abstracción lógica de Transactional Outbox;
* los eventos mantienen sus semantic lanes;
* la intención de propagación queda asociada atómicamente al cambio canónico;
* Outbox no es Kafka;
* Outbox no es Domain Event;
* Outbox no convierte Kafka en autoridad;
* PostgreSQL continúa siendo SSOT.

Preservar:

DefinitionEvents
VenueConstraintEvents
IdentityRelationEvents

como semantic lanes conceptuales.

NO decidir ni materializar:

* physical outbox table;
* partitioning;
* indexes;
* retention;
* relay;
* polling;
* workers;
* CDC;
* Debezium;
* deployment.

Todo ello:

IMPLEMENTATION / OPERATIONS DEFERRED.

**D20b-03 — Event Schema Governance**

**DECIDED**

Domain-event schemas are versioned and evolve under controlled compatibility rules.

Esta decisión NO debe seleccionar una política concreta como TRANSITIVE.

MUY IMPORTANTE:

NO materializar:

TRANSITIVE
BACKWARD
FULL

como decisión normativa de D20b.

La selección concreta del compatibility mode queda:

DEFERRED TO FUTURE SCHEMA GOVERNANCE / PHYSICAL DESIGN DECISION.

Tampoco decidir:

* Avro;
* Protobuf;
* JSON Schema;
* Apicurio;
* Confluent;
* Schema Registry;
* subject naming;
* code generation.

La arquitectura sí debe dejar establecido que:

1. los Domain Events son versionados;
2. los schemas evolucionan bajo control;
3. los cambios deben preservar compatibilidad según una política que será definida posteriormente;
4. la política física concreta queda deferred.

**D20b-04 — Semantic Transport Topology**

**DECIDED**

Three semantic-lane transport channels corresponding to the three D20a lanes.

Los tres canales/lane concepts son:

DefinitionEvents
VenueConstraintEvents
IdentityRelationEvents

La decisión arquitectónica establece separación semántica por lane.

Debe quedar claro:

Semantic Lane ≠ necessarily physical implementation detail

La arquitectura recomienda una correspondencia de tres canales/topics físicos cuando se materialice el transporte, pero los parámetros físicos permanecen diferidos.

NO decidir:

* topic names;
* partition count;
* replication factor;
* retention;
* consumer groups;
* DLQ;
* retry topology;
* deployment.

**D20b-05 — Ordering / Partition Identity**

**DECIDED**

Logical ordering identity is ListingIdentity + Semantic Lane.

Esto debe quedar alineado exactamente con D20a.

La regla normativa es:

Ordering Scope =
ListingIdentity + Semantic Lane

NO introducir:

aggregate_id

como concepto de dominio.

NO reemplazar ListingIdentity por aggregate_id.

La implementación futura deberá preservar el ordering lógico definido por:

ListingIdentity + Semantic Lane

pero los mecanismos físicos quedan deferred:

* Kafka message key;
* hashing;
* partition count;
* partition assignment;
* consumer groups;
* rebalance behavior.

La decisión es sobre la identidad lógica de ordering, no sobre el mecanismo físico de Kafka.

**D20b-06 — Iceberg Projection Strategy**

**DECIDED**

Hybrid event-history + state/analytical projections.

Debe quedar explícito:

PostgreSQL
    ↓
Canonical Authority
Transactional Outbox
    ↓
Domain Event Propagation
Kafka / Transport
    ↓
Optional Propagation Mechanism
Iceberg
    ↓
Derived Historical / Analytical Representation

Iceberg debe soportar conceptualmente:

Event History

Para:

* audit;
* replay;
* reconstruction;
* lineage;
* historical analysis.

State / Analytical Projections

Para:

* analytical querying;
* efficient reads;
* Point-in-Time analysis;
* materialized analytical views.

Debe quedar explícito:

Iceberg is derived and is NOT the canonical authority.

NO convertir Iceberg en SSOT.

NO implementar:

* Iceberg DDL;
* table schemas;
* partition transforms;
* sort orders;
* catalog;
* snapshot policies;
* compaction;
* retention.

Todo queda deferred.

**Estado:** D20b = DECIDED — Physical Persistence / Representation Architecture

**D20b DEFERRED ITEMS (Implementation / Operations / Future Gates):**

- Transactional Outbox / CDC / Debezium / Event Sourcing (implementation details)
- Kafka topics / partitions / consumer groups (physical configuration)
- Event schemas físicos (Avro / Protobuf / JSON) / Schema Registry (physical implementation)
- Event envelope / event ID format / UUID version (physical format)
- Delivery guarantees / retries / DLQ / idempotency implementation / reconciliation (operational details)
- PostgreSQL schema / Alembic migrations / Iceberg schema (physical implementation)
- Deployment topology / Kubernetes / Terraform / concrete adapter implementation

Todos permanecen: `DEFERRED / FUTURE DECISION` / `IMPLEMENTATION DEFERRED` según su dependencia.

**DECISION:** DECIDED — Physical Persistence / Representation Architecture

---

### PRESERVING PREVIOUS DECISIONS

**D19-08:** PostgreSQL = canonical operational authority / SSOT; Kafka = transport / propagation; Iceberg = derived historical / analytical representation; DuckDB = local analysis/testing. Multiple Ports ≠ Multiple Authorities. Todos los contracts conceptualmente acceden/coordinan alrededor del mismo canonical state.

**D19-09:** Domain Events = domain concepts; Domain Events ≠ Kafka. Separación por semantic lanes (Definition, VenueConstraints, IdentityRelation) preservada.

**D19-10:** Core Registry + Lane-Specific Extensions. Core = InstrumentIdentityRegistryPort (ListingIdentity, SymbolBinding). Lanes: DefinitionVersion, VenueConstraints, EvidenceProvenance, IdentityRelation.

**HD-4:** `PORT/REGISTRY = INFORM` — Registry no es autoridad semántica.

**HD-VC-7:** VenueConstraints = domain-owned; Registry/Port = INFORM; Adapter = TRANSLATE/PROVIDE EVIDENCE.

**NO SOBRE-ESPECIFICAR**

D20 NO decide (todos permanecen TBD / HUMAN DECISION REQUIRED / DEFERRED):

- Transactional Outbox / CDC / Debezium / Event Sourcing
- Kafka topics / partitions / consumer groups
- Event schemas físicos (Avro / Protobuf / JSON) / Schema Registry
- Event envelope / event ID format / UUID version
- Delivery guarantees / retries / DLQ / idempotency implementation / reconciliation
- PostgreSQL schema / Alembic migrations / Iceberg schema
- Deployment topology / Kubernetes / Terraform / concrete adapter implementation

Todos permanecen: `DEFERRED / FUTURE DECISION` / `HUMAN DECISION REQUIRED` según su dependencia.

**DECISION:** DECIDED — SPLIT GATES (human decision)

---

## D19 EXECUTIVE STATUS

| D19-01 | D19-02 | D19-03 | D19-04 | D19-05 | D19-06 | D19-07 | D19-08 | D19-09 | D19-10 | D20 | D20a | D20b |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **DECIDED: C+** | **DECIDED: A** | **DECIDED: A** | **DECIDED: B** | **DECIDED: A** | **DECIDED: A** | **DECIDED: A** | **DECIDED: D** | **DECIDED: B** | **DECIDED: C** | **DECIDED — SPLIT GATES** | **DECIDED — TRANSACTIONAL OUTBOX + DOMAIN EVENT PROPAGATION SEMANTICS** | **DECIDED — PHYSICAL PERSISTENCE / REPRESENTATION ARCHITECTURE** |

- **D19:** PENDING / PARTIALLY UNBLOCKED ACCORDING TO DEPENDENCIES
- **GATE C:** BLOCKED
- **IMPLEMENTATION:** NOT AUTHORIZED

**Nota:** Este documento materializa el D19 HUMAN DECISION PACK de forma persistente y auditable. **Registra la decisión humana D19-01 = C+ y D19-02 = A y D19-03 = A y D19-04 = B y D19-05 = A y D19-06 = A y D19-07 = A y D19-08 = D y D19-09 = B y D19-10 = C y D20 = SPLIT GATES y D20a = TRANSACTIONAL OUTBOX + DOMAIN EVENT PROPAGATION SEMANTICS y D20b = DECIDED — PHYSICAL PERSISTENCE / REPRESENTATION ARCHITECTURE.** No autoriza implementación y no cambia otros estados. Solo presenta alternativas, restricciones, invariantes, consecuencias arquitectónicas y dependencias para deliberación humana.

## D19 HUMAN DECISION PACK — MATERIALIZATION REPORT

**Artifact created/updated:** `docs/audits/D19_HUMAN_DECISION_PACK_2026-09-30.md`  
**D19-01..D19-10:** PRESENT  
**D19 status:** PENDING / PARTIALLY UNBLOCKED ACCORDING TO DEPENDENCIES  
**GATE C:** BLOCKED  
**IMPLEMENTATION:** NOT AUTHORIZED  
**Files modified:** 2 (this document updated; ADR-0049 updated)  
**Code files modified:** 0  
**Tests modified:** 0  
**ADR modified:** 1 (registration only)  
**tracking.yaml modified:** 0  
**GOVERNANCE.md modified:** 0  
**CI modified:** 0  
**Git mutations:** NONE  
**Commit:** NOT PERFORMED  
**Push:** NOT PERFORMED  
**Decision registration:** D19-01 = C+, D19-02 = A, D19-03 = A, D19-04 = B, D19-05 = A, D19-06 = A, D19-07 = A, D19-08 = D, D19-09 = B, D19-10 = C, D20 = SPLIT GATES, D20a = DECIDED, D20b = DECIDED  
**Human decisions:** D19-01 = C+ — HYBRID CONTROLLED IDENTITY, D19-02 = A — PURE DOMAIN ENTITY, D19-03 = A — PURE DOMAIN ENTITY + EFFECTIVE INTERVALS, D19-04 = B — CAMPOS EMBEBIDOS, D19-05 = A — INDEPENDENT VENUECONSTRAINTS DOMAIN ENTITY, D19-06 = A — INDEPENDENT CONCEPTS (H-01..H-05 = YES), D19-07 = A — DURABLE CANONICAL STATE (H-07-01..H-07-04 = YES), D19-08 = D — HYBRID RELATIONAL + ICEBERG, D19-09 = B — DOMAIN EVENTS BY SEMANTIC LANE WITH OPTIONAL TRANSPORT, D19-10 = C — HYBRID CORE REGISTRY + LANE-SPECIFIC EXTENSIONS, D20a = DECIDED — TRANSACTIONAL OUTBOX + DOMAIN EVENT PROPAGATION SEMANTICS, D20b = DECIDED — PHYSICAL PERSISTENCE / REPRESENTATION ARCHITECTURE, D20b-01 = DECIDED — RELATIONAL CANONICAL MODEL WITH EXPLICIT TEMPORAL SEMANTICS, D20b-02 = DECIDED — SINGLE LOGICAL TRANSACTIONAL OUTBOX, D20b-03 = DECIDED — VERSIONED DOMAIN-EVENT SCHEMAS WITH CONTROLLED COMPATIBILITY, D20b-04 = DECIDED — THREE SEMANTIC-LANE TRANSPORT CHANNELS, D20b-05 = DECIDED — LOGICAL ORDERING BY LISTINGIDENTITY + SEMANTIC LANE, D20b-06 = DECIDED — HYBRID EVENT-HISTORY + STATE/ANALYTICAL PROJECTION  
**Implementation:** NOT AUTHORIZED
