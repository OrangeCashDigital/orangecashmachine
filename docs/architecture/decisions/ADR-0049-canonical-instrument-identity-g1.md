# ADR-0049: Canonical Instrument Identity — serie G1 (decisiones humanas D01–D15)

**Estado:** ACEPTADO
**Estado (detalle):** Registro normativo de la serie G1 — decisiones humanas D01–D15 adoptadas — G1-D01..D13 ACEPTADO CON ACLARACIONES (G1-ACCEPTANCE = B, 2026-09-28); G1-D14 DECIDIDO + MATERIALIZADO + ACEPTADO (G1-D14-ACCEPTANCE = A, 2026-09-28); G1-D15 DECIDIDO + MATERIALIZADO + ACEPTADO (G1-D15-ACCEPTANCE = A, 2026-09-29)
**Decisión humana asociada:** G1 Human Decision Gate D01–D12 (adoptadas 2026-09-28) + G1-D13 = C — HYBRID / TWO-LEVEL RELATION MODEL (adoptada 2026-09-28) + G1-D14 = OPCIÓN B — HÍBRIDO A+ — SEMÁNTICA DE RELACIONES ENTRE LISTING IDENTITIES (adoptada 2026-09-28) + G1-D15 = (a) NO / (b) NOT APPLICABLE / (c) LATER / (d) NOT APPLICABLE / (e) NOT APPLICABLE / NOT AUTHORIZED / (f) SEPARATE FUTURE DECISION (adoptada 2026-09-29)
**Materialización:** D01–D12 REALIZADA VÍA GATE AUTORIZADO 2026-09-28; G1-D13 REALIZADA VÍA G1-MAT-D13 = A (actualización formal de ADR-0049) 2026-09-28 — CONJUNTO PENDIENTE DE HUMAN REVIEW (RESUELTO: ACEPTADO CON ACLARACIONES, G1-ACCEPTANCE = B — 2026-09-28); G1-D14 REALIZADA VÍA AUTORIZACIÓN HUMANA EXPLÍCITA (actualización formal de ADR-0049) 2026-09-28 — PENDIENTE DE HUMAN REVIEW (RESUELTO: ACEPTADO, G1-D14-ACCEPTANCE = A — 2026-09-28); G1-D15 REALIZADA VÍA AUTORIZACIÓN HUMANA EXPLÍCITA (actualización formal de ADR-0049) 2026-09-29
**Fecha:** 2026-09-28 (D01–D14); 2026-09-29 (actualización formal G1-D15)
**Bounded context(s) afectado(s):** market_data | trading | portfolio | shared | ocm

## Contexto

La serie G1 (Canonical Instrument Identity) se ejecutó como una secuencia de Human
Decision Gates. Cada decisión D01–D12 fue adoptada explícitamente por el humano en
deliberaciones de gobernanza; ninguna fue decidida por el agente. Este ADR consolida
las formulaciones normativas ya adoptadas para preservar trazabilidad normativa.

Documentar D01–D11 no constituye nuevas decisiones: son decisiones humanas previas
que se consolidan aquí. D12 motivó la materialización inicial (2026-09-28); G1-D13,
adoptada posteriormente como decisión humana, motivó esta actualización formal del
artefacto (G1-MAT-D13 = A, autorizada por el humano). G1-D14, adoptada posteriormente
como decisión humana (OPCIÓN B — HÍBRIDO A+), motivó una actualización formal adicional
bajo autorización humana explícita (2026-09-28).

Fuentes normativas previas:

- ADR-0048 §8 difiere "Canonical Instrument Identity" (instrument_id, venue, símbolos,
  market_type SPOT/PERPETUAL/FUTURE/OPTION…) a una decisión posterior (Phase E),
  registrada ahora en este ADR.
- ADR-0046 (H3, H3-H2): la materialización puede ser un ADR; no todo requiere un ADR
  independiente, pero el registro debe ser persistente y verificable.
- Principios vinculantes de la cadena de gobernanza: DECIDED ≠ MATERIALIZED ≠ ACCEPTED;
  ningún agente crea autoridad por inferencia.

## Regla de no-inferencia aplicada a este ADR

- NO se reinterpretan D01–D13.
- NO se mejoran, completan ni cambian formulaciones normativas ya decididas.
- NO se resuelven cuestiones que quedaron explícitamente abiertas.
- NO se convierten open questions en decisiones.
- G1-D13 se incorpora exclusivamente porque fue adoptada explícitamente por el humano
  (G1-D13 = C) y su materialización fue autorizada (G1-MAT-D13 = A). No es una decisión
  creada ni inferida por el agente en este documento.
- G1-D14 se incorpora exclusivamente porque fue adoptada explícitamente por el humano
  (G1-D14 = OPCIÓN B — HÍBRIDO A+) y su materialización fue autorizada (autorización
  humana explícita de materialización, 2026-09-28). No es una decisión creada ni
  inferida por el agente en este documento.
- G1-D15 se incorpora exclusivamente porque fue adoptada explícitamente por el humano
  (G1-D15 = (a) NO / (b) NOT APPLICABLE / (c) LATER / (d) NOT APPLICABLE / (e) NOT
  APPLICABLE / NOT AUTHORIZED / (f) SEPARATE FUTURE DECISION, 2026-09-29) y su
  materialización fue autorizada explícitamente (G1-D15 DECISION MATERIALIZATION GATE,
  2026-09-29). No es una decisión creada ni inferida por el agente en este documento.
- NO se introducen D16 ni decisiones posteriores.
- D19-01 = DECIDED (C+ — Hybrid Controlled Identity). D19-02 = DECIDED (A — Pure Domain Entity). D19-03..D19-10 permanecen pendientes.
- NO se anticipa D20–D24.
- Ningún vacío se rellena por inferencia.

## Decisiones adoptadas (serie G1, D01–D12)

### G1-D01 — Identity ≠ Symbol (B)

**Formulación normativa adoptada:**

La identidad representa una entidad de mercado estable durante su ciclo de vida; sus
representaciones y atributos pueden cambiar. Cuando cambia una propiedad que define
otra entidad negociable, se crea otra identidad y se establece explícitamente la
relación entre ambas.

Por tanto:

- Symbol ≠ Instrument Identity.
- Symbol es una representación/nombre.
- Instrument Identity representa la entidad negociable.
- Un SymbolBinding podrá relacionar una identidad con uno o más símbolos nativos a lo
  largo del tiempo.
- Esta decisión NO determina todavía la codificación de la identidad.
- Esta decisión NO determina todavía venue, instrument type, settlement, expiry,
  strike, multiplier, etc.

### G1-D02 — Listing Identity: unidad conceptual (C)

**Formulación normativa adoptada:**

La unidad conceptual de identidad será una Listing Identity: una identidad estable de
una entidad negociable dentro de su contexto de mercado. Esta identidad es distinta de
su símbolo o representación nativa y deberá poder mantener continuidad histórica
aunque cambien sus representaciones o determinados atributos no identitarios.

NO interpretar como: decisión sobre UUID, opaque ID, composite key, {symbol}.{venue},
venue dentro del ID, settlement, linear/inverse, expiry, strike/right, multiplier,
EconomicGroup, cross-venue equivalence, ni autorización de cambios.

### G1-D03 — Venue-scoped Listing Identity (A)

**Formulación normativa adoptada:**

Una Listing Identity está asociada a un venue concreto. Dos listings del mismo
producto económico en venues diferentes representan identidades distintas. La
equivalencia económica o relación cross-venue, si se requiere, deberá modelarse
explícitamente mediante una relación posterior y no mediante identidad compartida.

NO decide: cómo se codifica técnicamente el venue en el ID; UUID vs opaque vs
composite/structured ID; D19; instrument type; settlement; linear/inverse; margin
mode; expiry; strike/right; multiplier; EconomicGroup; equivalencia cross-venue de D15.

### G1-D04 — Instrument Type grueso identitario (C)

**Formulación normativa adoptada:**

El instrument type forma parte del contexto identitario de la Listing Identity a nivel
de categoría fundamental de instrumento. SPOT, PERPETUAL, FUTURE y OPTION representan
categorías distintas de entidades negociables y no deben compartir una misma identidad
por tratarse simplemente de valores distintos de un atributo.

D04 NO resuelve todavía los términos contractuales específicos de cada categoría:

- D04 = categoría fundamental (SPOT, PERPETUAL, FUTURE, OPTION).
- D05 = términos identity-defining específicos (settlement; linear/inverse;
  coin-margined; underlying/quote; margin/settlement semantics; y cualquier propiedad
  que determine si estamos ante otra entidad negociable).
- D06 = expiry.
- D07 = strike / option right.
- D08 = contract multiplier.

### G1-D05 — Settlement y margen condicional identitarios (C)

**Formulación normativa adoptada:**

Para las categorías no-spot, underlying, quote y settlement forman parte de la
definición identitaria de la Listing Identity. La convención de margen —linear,
inverse, coin-margined u otra semántica equivalente— forma parte de la identidad
cuando su diferencia define mercados negociables distintos que pueden coexistir bajo
el mismo venue. No debe asumirse automáticamente que la convención de margen es
solamente un atributo.

- IDENTITY-DEFINING en D05: underlying; quote; settlement; margin convention cuando
  sea diferenciadora de mercados coexistentes.
- NO resuelto en D05: expiry → D06; strike → D07; option right → D07; contract
  multiplier → D08; representación/codificación del ID → D19; cross-venue equivalence
  → D15; formato exacto de market_type o nombres de campos de código.
- Se mantiene explícita la separación semántica entre: settlement contractual del
  instrumento; settlement/P&L utilizado por trading; margin convention; instrument type.

### G1-D06 — Expiry identity-defining cuando aplique (A)

**Formulación normativa adoptada:**

Para instrumentos con vencimiento, especialmente FUTURE y OPTION, el expiry forma parte
de la definición identitaria de la Listing Identity. Dos contratos iguales en venue,
tipo, underlying, quote, settlement y demás términos identity-defining, pero con
vencimientos diferentes, representan Listing Identities distintas.

Dimensiones separadas:

- IDENTIDAD: expiry identifica el contrato cuando el instrumento tiene vencimiento.
- LIFECYCLE: announced; active; suspended; expired; delisted; successor/replacement.
  El paso de ACTIVE → EXPIRED NO crea una nueva identidad; la identidad existente
  cambia de estado dentro de su lifecycle.

NO implica: que SPOT tenga expiry; que PERPETUAL tenga expiry; que cada representación
de tenor/serie sea una identidad adicional; que expiry determine por sí solo toda la
identidad; que expiry sea parte del formato físico del ID; ninguna decisión sobre
UUID/opaque/composite/structured ID; ninguna decisión sobre D19. La serie/tenor puede
utilizarse posteriormente como concepto analítico o relación, pero no se introduce una
identidad adicional para ella salvo que una decisión posterior lo determine.

### G1-D07 — Strike + Option Right identity-defining para OPTION (A)

**Formulación normativa adoptada:**

Para la categoría OPTION, strike y option right forman parte de la definición
identitaria junto con los demás términos identity-defining aplicables, incluyendo
underlying y expiry. Una combinación diferente de strike o option right representa una
Listing Identity distinta porque produce una entidad negociable/payoff distinto.

Conceptualmente, OPTION Identity:

- Venue; Instrument Type; Underlying; Quote/Settlement según corresponda; Expiry;
  Strike; Option Right.

Ejemplos: `BTC / Venue X / OPTION / 2027-01-27 / 100000 / CALL` ≠
`BTC / Venue X / OPTION / 2027-01-27 / 100000 / PUT`, y ≠
`BTC / Venue X / OPTION / 2027-01-27 / 110000 / CALL`.

Aplica únicamente a OPTION. SPOT y PERPETUAL permanecen sin expiry/strike/right. FUTURE
conserva expiry según D06. Chain NO es identidad. Series/tenors NO crean
automáticamente otra identidad; chain/series son relaciones analíticas posteriores. No
se decide todavía cómo representar strike/right físicamente, ni escala decimal, tick
size, precisión ni encoding, ni UUID/opaque/composite/structured (D19). No se autoriza
implementación de opciones.

### G1-D08 — Contract Multiplier identity-defining cuando diferencia (A)

**Formulación normativa adoptada:**

Para instrumentos derivados, el contract multiplier forma parte de la definición
identitaria de la Listing Identity cuando su valor diferencia una entidad contractual
negociable de otra que puede coexistir bajo el mismo venue y demás términos
identity-defining. Dos contratos que sean iguales en los demás términos identity-defining
pero tengan un multiplier diferente representan Listing Identities distintas cuando
ambos pueden existir como mercados/contratos negociables diferenciados.

- Aplica a FUTURE y OPTION.
- NO significa que todo cambio administrativo o histórico del multiplier cree
  automáticamente una nueva identidad. La condición relevante es que el multiplier
  diferencie una entidad contractual negociable distinta. Si un cambio corresponde a
  una modificación/revisión contractual del mismo listing y no a una entidad negociable
  coexistente distinta, deberá tratarse posteriormente mediante Definition/Lifecycle/
  relaciones, no asumir automáticamente una nueva identidad.
- SPOT y PERPETUAL no quedan afectados salvo decisión específica futura.
- No se decide cómo representar físicamente el multiplier; ni Decimal/precision/scale/
  tick size; ni cómo serializarlo; ni si proviene del native symbol, API field, metadata
  u otra fuente; ni UUID/opaque/composite/structured (continúa bloqueado en D19).

### G1-D09 — Identidad estable + Lifecycle State + relaciones explícitas (A)

**Formulación normativa adoptada:**

La Listing Identity permanece estable durante su ciclo de vida. La evolución temporal
de la entidad se representa mediante estados de lifecycle y, cuando corresponda,
mediante relaciones explícitas con otras entidades o bindings. Un cambio de estado no
crea por sí mismo una nueva Listing Identity.

Estructura conceptual:

```
Listing Identity
    ├── Lifecycle State
    │      ├── ANNOUNCED
    │      ├── ACTIVE
    │      ├── SUSPENDED
    │      ├── EXPIRED
    │      └── DELISTED
    └── Explicit Relations
           ├── SUCCESSOR_OF
           ├── SYMBOL_CHANGED_TO
           ├── REPLACED_BY
           └── otras relaciones que se decidan posteriormente
```

- ACTIVE → SUSPENDED, SUSPENDED → ACTIVE, ACTIVE → EXPIRED, EXPIRED → DELISTED: no
  crean una nueva identidad.
- Un cambio de símbolo tampoco crea automáticamente una nueva identidad; debe poder
  representarse mediante una relación/Binding temporal explícito.
- Un successor/replacement puede ser otra Listing Identity relacionada explícitamente
  con la anterior. SUCCESSOR_OF no implica que ambas identidades sean la misma entidad.
- Una nueva Listing Identity se crea cuando existe una entidad negociable distinta según
  los términos identity-defining ya decididos en G1.
- No se adopta el modelo de identidad mutable/versionada como identidad. El versionado
  de definiciones contractuales queda separado de la identidad. El histórico debe poder
  reconstruirse mediante lifecycle + relaciones + bindings temporales.
- Governance: NO se decide el modelo concreto de governor; NO se añade governor como
  campo de la identidad; la autoridad/fuente del lifecycle se definirá posteriormente
  dentro del modelo de governance/SSOT correspondiente.

### G1-D10 — SymbolBinding contextual-temporal determinista, resolución única (C/RESTRICTED)

**Formulación normativa adoptada:**

Una Listing Identity puede estar asociada a múltiples SymbolBindings. Cada SymbolBinding
representa una asociación contextual y temporal entre una Listing Identity y una
representación nativa utilizada por un venue, adapter o contexto de integración
determinado. Los bindings pueden coexistir cuando pertenecen a contextos distintos, y
pueden sucederse temporalmente cuando representan cambios de símbolo. Para cualquier
resolución concreta (contexto + tiempo + native_symbol), el resultado debe ser
determinista: exactamente una Listing Identity válida, o FAIL-CLOSED ante ausencia o
ambigüedad.

```
resolve(native_symbol, context, time)
    ├── 0 matches → FAIL-CLOSED
    ├── 1 match   → RESOLVE
    └── >1 match  → FAIL-CLOSED
```

- Se permite one-to-many a través del tiempo y one-to-many entre contextos/adapters
  cuando sea legítimo. No se permite ambigüedad para una resolución concreta.
- Un cambio de símbolo no modifica la Listing Identity. Un nuevo SymbolBinding permite
  reconstruir el histórico.
- La identidad continúa siendo venue-scoped según D03. SymbolBinding NO crea
  equivalencia cross-venue. Native symbol continúa siendo representación, no identidad.
- No se permite inferir silenciosamente una identidad cuando existen múltiples
  candidatos; la resolución ambigua debe fallar cerradamente.
- NO define: schema físico de SymbolBinding; UUID; formato/encoding de la identidad;
  representación física de native_symbol; campos exactos de context; estrategia concreta
  de reverse mapping; Kafka; Iceberg; registry implementation; adapters; governance; D19;
  implementación de Futures/Options.

### G1-D11 — Governance híbrido con autoridad por origen y FAIL-CLOSED ante conflicto (C)

**Formulación normativa adoptada:**

La autoridad sobre el estado y los hechos observables propios del mercado corresponde
primariamente al venue que los origina. Las decisiones y estados administrativos propios
de la operación de OCM corresponden a OCM y deben identificarse explícitamente como
administrativos. Las dos categorías no deben mezclarse semánticamente. Cuando exista un
conflicto no resoluble entre fuentes sobre un hecho crítico para identidad, lifecycle,
binding o ejecución, OCM debe actuar FAIL-CLOSED y conservar la evidencia del conflicto
en lugar de inferir una resolución.

- **VENUE como autoridad primaria de hechos observables** (autoridad de ORIGEN, no
  autoridad absoluta sobre todo el estado interno de OCM): existencia/listing del
  instrumento; estado de mercado publicado; suspensión declarada; expiración contractual;
  delisting; cambio de símbolo anunciado; sucesión/reemplazo declarado cuando exista
  información explícita.
- **OCM como autoridad de estados administrativos propios**, etiquetados explícitamente
  como administrativos/operacionales y NO reinterpretables automáticamente como hechos
  contractuales del venue: feed habilitado/deshabilitado; adapter habilitado/
  deshabilitado; cuarentena operacional; bloqueo interno; disponibilidad operacional;
  políticas internas de ingestión; decisiones administrativas de OCM.
  Ejemplo: `VENUE: Instrument = ACTIVE` + `OCM: Feed = QUARANTINED` NO significa
  `Instrument = SUSPENDED`.
- **FAIL-CLOSED** ante conflicto sin regla aprobada: preservar evidencia → HUMAN
  DECISION / resolución formal. Prohibido: elegir silenciosamente una fuente; elegir la
  información más reciente sin regla aprobada; asumir prioridad de OCM por poseer el
  dato; asumir prioridad del venue sobre estados administrativos de OCM; inferir una
  transición no declarada; convertir una señal de calidad en un cambio de lifecycle.
- **SOURCE / PROVENANCE**: toda transición relevante deberá poder distinguir
  conceptualmente su origen: OBSERVED_FROM_VENUE | ADMINISTRATIVE_OCM | DERIVED |
  UNKNOWN/CONFLICT. La forma física de almacenar source, provenance, timestamps,
  versionado y demás metadatos queda fuera de D11 (decisión posterior).
- **SymbolBinding (D10)**: un anuncio explícito del venue es evidencia primaria del
  cambio de símbolo; OCM no debe inventar un nuevo binding únicamente porque el feed
  dejó de producir datos; evidencia contradictoria o múltiples mappings posibles →
  FAIL-CLOSED; el cambio de símbolo debe quedar temporalmente trazable; la
  implementación concreta del mapping continúa fuera de G1.
- **NO confundir lifecycle con operación**: un fallo de feed/adapter/infraestructura NO
  implica automáticamente ACTIVE → SUSPENDED. Puede producir Feed Quality = FAILED,
  Adapter State = DEGRADED, OCM Operational State = QUARANTINED sin modificar el
  lifecycle contractual observado del instrumento.
- Fuera de D11: catálogo exhaustivo de lifecycle transitions; schema físico de
  provenance/source; timestamps/versionado físico; Kafka; Iceberg; registry; governance
  implementation; mecanismo de reconciliación concreto; D19; D20–D24; implementación de
  Futures/Options; cambios de código.

### G1-D12 — Separación Identity/Definition: versionado de Definition vs nueva identidad + relación (C/RESTRICTED)

**Formulación normativa adoptada:**

Los cambios en Definition que continúan describiendo la misma entidad negociable se
representan mediante versiones de la Definition asociadas a la misma Listing Identity.
Cuando un cambio define una entidad negociable distinta que puede coexistir en el mismo
contexto de mercado, se crea una nueva Listing Identity y se establece explícitamente la
relación correspondiente con la identidad anterior. La clasificación no podrá depender
de heurísticas ni de la mera existencia de una nueva versión; deberá aplicar el criterio
normativo de entidad negociable establecido en G1.

Frontera conceptual:

```
CAMBIO
  ├── ¿Sigue describiendo la misma entidad negociable?
  ├── SÍ   → misma Listing Identity → DefinitionVersion
  └── NO   → nueva Listing Identity → relación explícita con la anterior
```

Criterio normativo establecido en G1
(`¿El cambio distingue una entidad negociable distinta que puede coexistir en el mismo
contexto de mercado?`): NO → Definition/versión dentro de la misma identidad;
SÍ → nueva Listing Identity + relación explícita.

NO decidir todavía: catálogo exhaustivo de términos identity-defining vs Definition;
ventanas temporales de DefinitionVersion; [valid_from, valid_to], solapes o cierre de
intervalos; autoridad para validar ventanas; provenance; SUCCESSOR_OF vs REPLACED_BY;
schemas físicos; Kafka; Iceberg; registry; implementación; adapters; código; tests; CI;
D19; D20–D24.

### G1-D13 — Modelo de relaciones de dos niveles (HYBRID) (C)

**Formulación normativa adoptada:**

"Los cambios de representación o símbolo se modelan mediante SymbolBinding y sus
relaciones temporales, conforme a G1-D10. Los cambios que representan una entidad
negociable distinta se modelan mediante una nueva Listing Identity y una relación
explícita entre identidades. Las relaciones entre identidades deberán ser tipadas,
direccionales y conservar su origen/provenance conceptual, y cualquier ambigüedad
relevante para identidad, lifecycle, binding o ejecución deberá resolverse
FAIL-CLOSED."

Frontera conceptual:

```
REPRESENTACIÓN
    │
    └── SymbolBinding
          └── SYMBOL_CHANGED_TO
                │
                └── misma Listing Identity
ENTIDAD NEGOCIABLE
    │
    └── nueva Listing Identity
          │
          ├── SUCCESSOR_OF
          └── REPLACED_BY
```

Reglas:

1. Un cambio de símbolo no implica automáticamente una nueva Listing Identity.
2. Una entidad negociable distinta requiere una nueva Listing Identity.
3. La relación entre identidades debe ser explícita y no inferirse silenciosamente.
4. Las relaciones entre identidades son tipadas y direccionales.
5. Debe conservarse provenance conceptual (origen).
6. La resolución de las relaciones debe ser determinista para cualquier resolución
   concreta.
7. La ambigüedad relevante para identidad, lifecycle, binding o ejecución debe
   resolverse FAIL-CLOSED.
8. G1-D11 continúa gobernando la autoridad/provenance conceptual: hechos observables
   del venue → venue; estados administrativos propios → OCM; conflicto crítico →
   FAIL-CLOSED.
9. G1-D12 continúa gobernando el criterio de cuándo un cambio de Definition mantiene
   la misma identidad o crea una nueva identidad.

La decisión NO establece que SYMBOL_CHANGED_TO, SUCCESSOR_OF y REPLACED_BY tengan
todavía un modelo físico ni que sus semánticas detalladas estén completamente
definidas.

Esta decisión NO decide todavía: semántica definitiva de SUCCESSOR_OF; semántica
definitiva de REPLACED_BY; cuándo exactamente usar una u otra; catálogo exhaustivo de
relaciones; ventanas temporales; valid_from / valid_to; provenance física;
representación física; schemas; Kafka; Iceberg; adapters; CCXT; Cryptofeed; D15
cross-venue; D19; D20–D24; implementación. Esas cuestiones permanecen para gates
posteriores.

### G1-D14 — Semántica de relaciones entre Listing Identities: SUCCESSOR_OF / REPLACED_BY (B / HÍBRIDO A+)

**Formulación normativa adoptada:**

SUCCESSOR_OF y REPLACED_BY permanecen como relaciones tipadas y direccionales entre
Listing Identities. Ambas operan exclusivamente entre Listing Identities y son
semánticamente distintas; no son etiquetas equivalentes ni se distinguen solo por su
nomenclatura.

- **SUCCESSOR_OF** expresa una relación de sucesión/continuidad de linaje entre una
  Listing Identity anterior y otra Listing Identity posterior. La interpretación de
  "continuidad" es continuidad de linaje/sucesión entre identidades distintas; NO
  implica que ambas identidades sean la misma entidad (guarda de G1-D09). La
  continuidad de la misma Listing Identity mediante evolución de su definición continúa
  gobernada por G1-D12 (DefinitionVersion), no por SUCCESSOR_OF.
- **REPLACED_BY** expresa una relación de sustitución entre una Listing Identity y otra
  Listing Identity que la sustituye.
- La clasificación de una relación como SUCCESSOR_OF o REPLACED_BY no puede inferirse
  silenciosamente por coincidencia de símbolo, fecha, venue, lifecycle state u otro
  atributo. Ante ambigüedad sobre dicha clasificación debe aplicarse la autoridad y las
  reglas de gobernanza definidas por G1-D11 (FAIL-CLOSED), sin recurrir a heurística
  silenciosa.
- No se introducen relaciones nuevas no documentadas. SYMBOL_CHANGED_TO permanece en el
  nivel de SymbolBinding conforme a G1-D10, G1-D13 y su Aclaración; NO es una relación
  identity-to-identity y NO se ve alterada por G1-D14.

NO decide: temporalidad ni valid_from / valid_to (permanecen pendientes); lifecycle
transitions (permanecen pendientes); cross-venue equivalence (corresponde a D15);
encoding / UUID / composite key / representación física (corresponde al trabajo
posterior / D19); schemas; Kafka; Iceberg; adapters; CCXT; Cryptofeed; contratos de
implementación.

**Ambigüedades residuales (permanecen pendientes):**

- R1: "continuidad" en SUCCESSOR_OF = continuidad de linaje entre identidades
  distintas, NO misma entidad (guarda de D09).
- R2: la clasificación operativa de casos reales como SUCCESSOR_OF o REPLACED_BY
  requiere evidencia y autoridad (G1-D11); no heurística.
- R3: la coordinación con el lifecycle (p. ej. relación entre delisting/suspensión y
  sucesión) permanece pendiente.
- R4: temporalidad / valid_from / valid_to permanece pendiente.
- R5: el catálogo exhaustivo de otras relaciones permanece pendiente; las únicas
  relaciones identity-to-identity documentadas siguen siendo SUCCESSOR_OF y REPLACED_BY.
- R6: D15 (cross-venue), D19 (representación/encoding) y D20–D24 permanecen BLOCKED.

### G1-D15 — Relación económica entre Listing Identities distintas (2026-09-29)

**Formulación normativa adoptada:**

OCM no requiere actualmente una relación económica canónica explícita entre Listing
Identities distintas. Las Listing Identities permanecen diferenciadas conforme a
G1-D01–D14 y, en particular, por el contexto de venue establecido en G1-D03 (Binance
BTC/USDT ≠ Bybit BTC/USDT).

EconomicGroup queda fuera del alcance actual y se reserva para una decisión futura
explícita. La selección LATER no autoriza la creación de una entidad, relación,
schema, registry, API, campo de almacenamiento, agregación de portfolio o agregación
de riesgo asociadas a agrupación económica.

Execution Interchangeability queda fuera del modelo económico actual y requiere una
decisión futura separada. No deberá inferirse a partir de underlying común, símbolo
común, relación económica, agrupación, correlación de precios u otra heurística.

La decisión D15 no establece que nunca pueda existir una relación económica
cross-listing entre Listing Identities. Establece únicamente que dicha relación no
forma parte del modelo canónico autorizado en el estado actual de OCM.

**Decisiones registradas (G1 D15 Human Decision, 2026-09-29):**

- (a) NO — no se requiere actualmente una relación económica explícita entre Listing
  Identities distintas.
- (b) NOT APPLICABLE — no se selecciona TYPED ASSOCIATION, ECONOMIC EQUIVALENCE,
  ECONOMIC GROUP, COMBINATION ni OTHER.
- (c) LATER — EconomicGroup se reserva para una decisión futura explícita; no crea
  esquema, entidad, relación, agregación ni autoridad alguna en el estado actual.
- (d) NOT APPLICABLE — no se selecciona ECONOMIC EQUIVALENCE.
- (e) NOT APPLICABLE / NOT AUTHORIZED — no se autoriza ninguna modificación de D13.
- (f) SEPARATE FUTURE DECISION — Execution Interchangeability queda fuera del modelo
  económico actual y requiere decisión futura separada.

**Delimitación semántica (no-inferencia):**

- Relación económica D15 NO implica: misma Listing Identity, mismo venue, mismo precio,
  misma liquidez, mismo riesgo, misma posición de portfolio ni execution interchangeability.
- EconomicGroup (futuro) NO implica: misma Listing Identity, equivalencia económica ni
  execution interchangeability.
- Sacar de D15 los defectos de scoping ya reportados (p. ej. `portfolio_service.py:126`,
  merge por `symbol+side` sin `exchange`): son asuntos de cumplimiento de G1-D03 /
  scoping de identidad, no evidencian una relación económica y no se corrigen en este
  gate (REPORT ONLY). `market_data/main.py:562` (`market_type="spot"`) es asunto D04,
  separado de D15.

**No decide:** aquello ya cubierto por D01–D14; la semántica de relaciones económicas
futuras; el mecanismo de EconomicGroup; execution interchangeability; D19; D20–D24;
schemas; registries; APIs; topics Kafka; campos Iceberg; agregaciones de portfolio ni
de riesgo; implementación.

## Aclaración de clasificación D09 / D13 (G1-ACCEPTANCE = B)

D09 establece que la evolución de una Listing Identity puede representarse mediante
estados de lifecycle y relaciones/bindings explícitos. G1-D13 refina la clasificación
de esas relaciones: SYMBOL_CHANGED_TO pertenece al nivel de SymbolBinding y representa
un cambio de representación asociado a la misma Listing Identity; SUCCESSOR_OF y
REPLACED_BY pertenecen al nivel de relaciones entre Listing Identities y se utilizan
cuando existe una entidad negociable distinta. Esta aclaración no modifica D09, sino
que precisa su clasificación conforme a la decisión posterior G1-D13. No introduce
decisiones nuevas: no decide semántica exhaustiva de SUCCESSOR_OF ni REPLACED_BY, ni
catálogo definitivo de relaciones, ni ventanas temporales, ni valid_from/valid_to, ni
provenance física, ni representación física, ni schemas, ni D15, ni D19, ni D20–D24,
ni implementación.

## Estado posterior de la serie G1

```
G1-D01..D13 = DECIDED + MATERIALIZED + ACCEPTED WITH CLARIFICATIONS
G1-D14      = DECIDED + MATERIALIZED + ACCEPTED (G1-D14-ACCEPTANCE = A)
G1-D15      = DECIDED + MATERIALIZED + ACCEPTED (G1-D15-ACCEPTANCE = A, 2026-09-29)
D16+        = BLOCKED / NOT DECIDED
D19-01      = DECIDED (C+ — Hybrid Controlled Identity)
D19-02      = DECIDED (A — Pure Domain Entity / Entidad Propia)
D19-03      = DECIDED (A — Pure Domain Entity + Effective Intervals / Entidad Propia + Intervalos Efectivos)
D19-04      = DECIDED (B — Embedded Fields per Concept / Campos Embebidos Específicos por Concepto)
D19-05      = DECIDED (A — Independent VenueConstraints Domain Entity / Entidad Independiente de VenueConstraints)
D19-06      = DECIDED (A — Independent Concepts / Conceptos Independientes; H-01..H-05 = YES)
D19-07      = DECIDED (A — Durable Canonical State; H-07-01..H-07-04 = YES)
D19-08      = DECIDED (D — Hybrid Relational + Iceberg)
D19-09      = DECIDED (B — Domain Events by Semantic Lane with Optional Transport)
D19-10      = DECIDED (C — Hybrid Core Registry + Lane-Specific Extensions)
D19-03..D19-10 = AMBIGUO — pendientes de verificación y reconciliación normativa (medida provisional de control documental)
D20         = DECIDED — SPLIT GATES
D20a        = DECIDED — Transactional Outbox + Domain Event Propagation Semantics
D20b        = DECIDED — Physical Persistence / Representation Architecture
D20b-01     = DECIDED — Relational canonical model with explicit temporal semantics
D20b-02     = DECIDED — Single logical Transactional Outbox
D20b-03     = DECIDED — Versioned domain-event schemas with controlled compatibility
D20b-04     = DECIDED — Three semantic-lane transport channels
D20b-05     = DECIDED — Logical ordering by ListingIdentity + Semantic Lane
D20b-06     = DECIDED — Hybrid event-history + state/analytical projection
D21–D24     = BLOCKED
IMPLEMENTATION = NOT AUTHORIZED
```

### Contradicciones no resueltas (C1–C4)

- **C1 (D19-03..D19-10 / D20):** Contradicción interna entre ADR-0049:L53-L54 (D19-03..D19-10 permanecen pendientes; NO se anticipa D20–D24) y ADR-0049:L545-L551 (D19-03..D19-10 = DECIDED con sus opciones respectivas; D20/D20a/D20b-01..06 = DECIDED). No se declara qué pasaje prevalece. Se conservan ambos registros históricos íntegramente. La clasificación como "AMBIGUO" es una medida provisional de control documental, **no** una declaración sobre la validez o invalidez de los registros DECIDED existentes. La determinación del estado normativo efectivo deberá realizarse mediante las reglas de precedencia aplicables y evidencia documental verificable.
- **C2 (PENDING/HUMAN DECISION REQUIRED vs DECIDED):** Persisten etiquetas históricas (`HUMAN DECISION REQUIRED`/`PENDING`) en documentos subordinados (p. ej. D19-01_INDUSTRY_BOW, KNOWLEDGE_BASE_CONTRACT) frente a las decisiones registradas. La precedencia normativa establecida por Q1 (ADR-0055) **no implica** la supresión automática de dichas etiquetas ni una reconciliación material. Se mantiene la contradicción abierta. No se infiere precedencia por mera posición de pasajes.
- **C3 (histórico Industry/KN_BASE vs ADR-0049):** Se documenta la discrepancia entre registros históricos y los estados reflejados en ADR-0049. No se infiere precedencia sin una regla expresa y aplicable. La contradicción permanece sin resolver.
- **C4 (D12 / relación con D20b):** Se documenta la discrepancia relacionada con el tratamiento C/RESTRICTED de D12 y su relación con D20b. **D12 queda fuera del alcance de esta enmienda.** No se adopta ninguna decisión arquitectónica nueva sobre D12 en esta ronda. La relación entre estos elementos permanece sin resolver en el presente ámbito.

*Nota:* Se conserva la distinción entre **existencia de registros históricos DECIDED** y **estado normativo efectivo pendiente de reconciliación**. Esta medida provisional **no** autoriza implementación, **no** levanta `IMPLEMENTATION = NOT AUTHORIZED`, **no** abre GATE C y **no** autoriza Q2. D20b-01..06 no se ven afectados por esta clasificación ambigua.

G1-D13 está DECIDED + MATERIALIZED + ACCEPTED WITH CLARIFICATIONS. La aceptación
(G1-ACCEPTANCE = B) cierra la serie G1-D01..D13 como decisión arquitectónica, pero NO
crea autoridad universal ni autoriza implementación: DECIDED ≠ MATERIALIZED ≠ ACCEPTED
≠ AUTHORITY. La aclaración autorizada (D09/D13) fue incorporada; la autoridad continúa
dependiendo de las reglas de gobernanza ya aprobadas.

G1-D14 está DECIDED + MATERIALIZED + ACCEPTED (G1-D14-ACCEPTANCE = A, 2026-09-28). La
aceptación registra el estado de aceptación de la decisión tal y como fue materializada;
NO crea autoridad universal ni autoriza implementación: DECIDED ≠ MATERIALIZED ≠
ACCEPTED ≠ AUTHORITY. La aceptación NO resuelve los residuales R1–R6, que permanecen
pendientes. R6, en lo relativo a "D15 (cross-venue)", quedó RESUELTO por G1-D15.

G1-D15 está DECIDED + MATERIALIZED + ACCEPTED (G1-D15-ACCEPTANCE = A, 2026-09-29). La
aceptación registra el estado de la decisión tal y como fue materializada; NO crea
autoridad universal ni autoriza implementación: DECIDED ≠ MATERIALIZED ≠ ACCEPTED ≠
AUTHORITY. G1-D15 NO autoriza: relación económica canónica; EconomicGroup (LATER);
execution interchangeability (SEPARATE FUTURE DECISION); modificación de D13 (NOT
AUTHORIZED); schemas; registries; APIs; topics Kafka; campos Iceberg; agregaciones de
portfolio o riesgo; implementación.

Ninguna decisión D16+ se declara decidida, aceptada ni materializada. La materialización
de D13 no autorizó: catálogo exhaustivo de relaciones; semántica exhaustiva de
SUCCESSOR_OF ni REPLACED_BY; ventanas temporales; valid_from/valid_to; provenance
física; representación física; schemas; Kafka; Iceberg; adapters; CCXT; Cryptofeed;
D15; D19; D20–D24; contratos de implementación; código; migraciones; tests de
implementación; cambios de registry; cambios de CI. G1-D14 resolvió posteriormente la
semántica de SUCCESSOR_OF y REPLACED_BY (ver su sección); G1-D15 resolvió
posteriormente la pregunta de relación económica cross-venue (ver su sección). El resto
permanece BLOCKED / NOT DECIDED o NOT AUTHORIZED según corresponda. Este ADR tampoco
autoriza implementación de InstrumentIdentity, Futuros, Opciones, SymbolBinding,
registry, Kafka/Iceberg, reverse mapping ni migraciones.

## Justificación técnica

- La serie G1 fue ejecutada como Human Decision Gates: cada formulación fue adoptada por
  el humano y verificada contra el repositorio (evidencia en deliberaciones D01–D13).
- ADR-0048 §8 reservaba Canonical Instrument Identity como decisión posterior (Phase E);
  este ADR materializa esa decisión posterior con las decisiones D01–D12 adoptadas.
- G1-D13 fue adoptada por el humano (2026-09-28) y materializada por actualización
  formal de este ADR (G1-MAT-D13 = A). El artefacto fue aceptado con aclaraciones
  (G1-ACCEPTANCE = B, 2026-09-28): la materialización no crea autoridad; la autoridad
  continúa dependiendo de las reglas de gobernanza previamente decididas.
- G1-D14 fue adoptada por el humano (2026-09-28, OPCIÓN B — HÍBRIDO A+) y materializada
  por actualización formal de este ADR bajo autorización humana explícita (2026-09-28).
  La materialización no crea autoridad; la autoridad continúa dependiendo de las reglas
  de gobernanza previamente decididas. La aceptación (G1-D14-ACCEPTANCE = A, 2026-09-28)
  registra el estado de aceptación sin alterar el contenido normativo ni resolver los
  residuales R1–R6.
- La metodología transversal de G1 (criterio identity-defining vs Definition; reglas de
  resolución 0/1/>1 → FAIL-CLOSED; separación de IDENTITY / DEFINITION / LIFECYCLE /
  RELATION / REPRESENTATION / SYMBOL BINDING) es la base normativa sobre la que se
  continuará la serie (gates posteriores).
- G1-D15 fue adoptada por el humano (2026-09-29, (a) NO / (b) NOT APPLICABLE / (c) LATER
  / (d) NOT APPLICABLE / (e) NOT APPLICABLE / NOT AUTHORIZED / (f) SEPARATE FUTURE
  DECISION), materializada por actualización formal de este ADR bajo autorización humana
  explícita (G1-D15 DECISION MATERIALIZATION GATE, 2026-09-29) y aceptada
  (G1-D15-ACCEPTANCE = A, 2026-09-29). La materialización no crea autoridad; no resuelve
  D16+; no autoriza EconomicGroup ni execution interchangeability ni D13 refinement.

## Consecuencias

- Qué queda más fácil: trazabilidad normativa de la serie G1 (D01–D15 en un único
  artefacto); frontera clara identity-defining vs Definition y frontera
  Representation/SymbolBinding vs Relationship; registro explícito de que no existe
  relación económica canónica cross-listing en el estado actual.
- Qué deuda se acepta conscientemente: D01–D11 no tenían antes artefacto persistente
  (se consolidan aquí); D16+, D19 y D20–D24 permanecen sin decidir; EconomicGroup queda
  como decisión futura (LATER); execution interchangeability exige decisión futura
  separada; SUCCESSOR_OF y REPLACED_BY tienen semántica normativa en G1-D14, pero su
  clasificación operativa caso a caso, la coordinación con lifecycle, la temporalidad /
  valid_from / valid_to y el catálogo restante de relaciones permanecen pendientes
  (R1–R5).
- Qué contratos BC-NN hacen cumplir esta decisión: ninguno en esta fase (sin cambios de
  código, schemas, Kafka, Iceberg, registry, CI, adapters, tests ni configuración).

## Referencias

- Decisor humano: G1 Human Decision Gate D01–D12 (2026-09-28).
- Decisor humano: G1-D13 = C — HYBRID / TWO-LEVEL RELATION MODEL (2026-09-28).
- Aceptación humana: G1-ACCEPTANCE = B — ACCEPT WITH CLARIFICATIONS (2026-09-28).
- Decisor humano: G1-D14 = OPCIÓN B — HÍBRIDO A+ — SEMÁNTICA DE RELACIONES ENTRE
  LISTING IDENTITIES (2026-09-28).
- Autorización humana de materialización: G1-D14 MATERIALIZATION — actualización formal
  de ADR-0049 (2026-09-28).
- Aceptación humana: G1-D14-ACCEPTANCE = A — ACCEPTED (2026-09-28).
- Decisor humano: G1-D15 HUMAN DECISION (2026-09-29) — decisión capturada y validada:
  (a) NO / (b) NOT APPLICABLE / (c) LATER / (d) NOT APPLICABLE / (e) NOT APPLICABLE /
  NOT AUTHORIZED / (f) SEPARATE FUTURE DECISION.
- Autorización humana de materialización: G1-D15 DECISION MATERIALIZATION GATE —
  actualización formal de ADR-0049 (2026-09-29; restringida a ADR-0049; tracking.yaml
  NO REQUIRED para G1).
- Aceptación humana: G1-D15-ACCEPTANCE = A — ACCEPTED (2026-09-29).
- Decisión humana de materialización: G1-MAT-D13 = A (actualización formal de ADR-0049;
  sin ADR-0050; tracking.yaml NO fuente normativa alternativa para D13).
- ADR-0048 §8: Canonical Instrument Identity — decisión posterior (Phase E).
- ADR-0046 (H3): Decision Records & Materialization.
- Modelo conceptual de trabajo: Listing Identity → SymbolBinding → native_symbol →
  adapter → exchange API; distinción de Economic Reference/Underlying, Listing Identity,
  Instrument Definition, Symbol, EconomicGroup.