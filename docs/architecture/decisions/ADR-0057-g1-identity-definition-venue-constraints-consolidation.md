# ADR-0057: Consolidación de decisiones humanas G1: identidad canónica, DefinitionVersion y VenueConstraints

**Estado:** ACEPTADO — aceptación documental autorizada 2026-10-11 (decisión humana: ADR-0057 aceptado como vehículo de consolidación documental de §17.3 con alcance limitado; no declara vigencia normativa ni autoriza implementación; C1–C4 sin resolver; D12 fuera de alcance decisorio)
**Fecha:** 2026-10-11
**Bounded context(s) afectado(s):** market_data | trading

## Contexto

La serie G1 de decisiones humanas sobre identidad canónica de instrumento,
DefinitionVersion y VenueConstraints se materializó en dos fuentes con estatus
documental desiguales:

- ADR-0049 (trackeado, `docs/architecture/decisions/ADR-0049-canonical-instrument-identity-g1.md`)
  registra las decisiones D01–D15 de la serie y el índice de D19/D20b, pero sus
  líneas 53–54 declaran un estado («D19-03..D19-10 permanecen pendientes»,
  «NO se anticipa D20–D24») que quedó desactualizado frente al registro posterior
  de esas mismas decisiones (contradicción C1).
- El decision-support `docs/audits/AUDIT_OCM_decision-support-hd-venue-constraints-identity_2026-09-29.md`
  contiene los DECISION RECORDS íntegros de HD-1..HD-8 y HD-VC-1..HD-VC-7, pero
  no está trackeado en `main`; su única copia verificable es el commit backup
  `4ad03ebae7b2d25c6a31f8954f0d8e67d8a677e0` (rama `backup/untracked-2026-10-11`
  y su ref remota).

§17.3 del propio decision-support exige, como precondición de GATE C, un
«ADR nuevo (B-MD-005 o G1 addendum) formalizado por el humano; ADR-0049 sin
modificar». Este ADR es ese artefacto de consolidación: un registro trazable de
decisiones humanas ya tomadas, sin reabrirlas, sin armonizar sus fuentes
históricas y sin constituir autorización de implementación.

Nota editorial (no es texto original): el número inicialmente previsto, ADR-0056,
resultó ocupado por `ADR-0056-risk-accept-pyarrow.md` (renumeración concurrente
externa, commit `e06759e5`); la reasignación a ADR-0057 fue decisión humana
posterior. No se renumeró, modificó ni reemplazó ningún ADR existente.

## Alternativas evaluadas

1. **ADR independiente (adoptada)** — crear un ADR con plantilla, numeración y
   estado tipificados por GOVERNANCE y `ADR-template.md`. Ventaja: autoridad,
   trazabilidad y compatibilidad con M4/M15 sin mecanismos nuevos. Coste-riesgo:
   consolidación que debe distinguirse claramente de la fuente primaria.
2. **Documento complementario de la serie G1 (descartada)** — sin tipo
   documental definido en GOVERNANCE ni en la plantilla; carecería de estado
   tipificado y añadiría identidad documental no reconocida, con riesgo de
   duplicación o contradicción de fuentes normativas.

## Decisión

Materializar §17.3 como ADR independiente (esta decisión) que:

1. consolida documentalmente, con procedencia explícita, los registros de las
   decisiones humanas HD-1..HD-8 y HD-VC-1..HD-VC-7;
2. inventaría las contradicciones históricas C1–C4 sin resolverlas;
3. mantiene ADR-0049 y ADR-0055 intactos como fuentes existentes;
4. deja D12 fuera de su alcance decisorio;
5. no modifica `docs/plans/tracking.yaml` ni ejecuta §17.4 ni GATE C;
6. no concede autorización de implementación.

Bounded contexts del presente ADR: `market_data | trading` — con evidencia
directa verificable (market_data como owner semántico del modelo de identidad
por HD-VC-7; trading como consumer contractado por port, con la separación
`min_notional ≠ RiskPolicy.min_order_usd` de HD-VC-3). Los contextos
`portfolio`, `shared` y `ocm` quedan **pendientes de justificación humana** y
no se incluyen ni se excluyen como decisión de este documento (la mera
enumeración en el frontmatter de ADR-0049 no se considera prueba de afectación).

## Procedencia y autoridad de las decisiones humanas

- Fuente literal de los 15 registros transcritos en la sección siguiente:
  commit backup `4ad03ebae7b2d25c6a31f8954f0d8e67d8a677e0`, ruta
  `docs/audits/AUDIT_OCM_decision-support-hd-venue-constraints-identity_2026-09-29.md`,
  sección «DECISION RECORDS» (líneas 1117–1132 del blob), leída mediante
  operaciones de Git de solo lectura. El backup no se restauró, extrajo ni
  persistió.
- Cada registro se transcribe **verbatim** (incluida su apertura
  «DECISION RECORD — HD-x: …» y su cierre «· justificación REGISTRADA (§xx)»).
  La línea «HUMAN ACTION REQUIRED» interpuesta en la fuente entre HD-VC-6 y
  HD-VC-7 se conserva también como parte del bloque fuente.
- Las justificaciones desarrolladas de cada decisión permanecen en las
  secciones §14.a–§14.h y §15.a–§15.g de ese mismo documento fuente; este ADR
  no las sustituye ni las reescribe.
- La fuente solo existe en el commit backup indicado (no trackeado en `main`);
  esta dependencia se declara como limitación de disponibilidad. Su persistencia
  en `main` es una decisión humana independiente, no autorizada por este ADR.
- Autoridad: las decisiones consolidadas fueron tomadas por el humano entre
  2026-09-29 y 2026-09-30 (fechas declaradas en la fuente). Este ADR solo las
  registra; no las amplía, modifica ni reinterpreta.
- Explicaciones y marcadores añadidos por redacción de este ADR se distinguen
  explícitamente («Nota editorial») del texto original.

## Registros de decisiones humanas (transcripción literal desde la fuente)

Los quince registros siguientes son texto original del decision-support
(identificador `4ad03ebae7b2d25c6a31f8954f0d8e67d8a677e0`, líneas 1117–1132),
sin omisiones ni reescritura:

DECISION RECORD — HD-1: A (separación formal, contrato de domain; alineada con G1 materializado) · justificación REGISTRADA (§14.a)
DECISION RECORD — HD-2: A (separación estricta; ListingIdentity independiente de native_symbol, native_symbol exclusivo de SymbolBinding) · justificación REGISTRADA (§14.b)
DECISION RECORD — HD-3: C refinada (regla semántica normativa + catálogo de casos conocidos como materialización operacional + FAIL-CLOSED) · justificación REGISTRADA (§14.c)
DECISION RECORD — HD-4: D (reparto contractizado: DOMAIN = DECIDE · APPLICATION = ORCHESTRATE · PORT/REGISTRY = INFORM; regla semántica de HD-3 en dominio; normalización fuera → HD-8; constraints fuera → HD-VC) · justificación REGISTRADA (§14.d)
DECISION RECORD — HD-5: A + D (ValidityWindow como VO propio de dominio: semántica D10 + invariantes Fail-Fast; ventana fuera de ListingIdentity; contrato normativo explícito; alcance limitado a SymbolBinding/D10/temporalidad de resolución/invariantes del VO; sin comprometer D12/HD-VC) · justificación REGISTRADA (§14.e)
DECISION RECORD — HD-6: A/I (P1=A fail-closed ante evidencia conflictiva · P2=B separación conceptual EvidenceSource/EvidenceStatus · P3=C guard central en DOMAIN (identity-resolution policy) · P4=A IdentityConflictError como semántica de error · P5=A observed_at conceptual · P6=A renuncias explícitas; sin jerarquía de autoridad; sin cierre de D12/HD-VC; retención conceptual del conflicto) · justificación REGISTRADA (§14.f)
DECISION RECORD — HD-7: A (APROBAR ACOTADO — instrumentos/límites/precisión como datos observados del venue, con condiciones vinculantes: ExchangeQuirks solo transporte; restricciones de venue no absorbidas en ExchangeQuirks; propiedad/modelado físico en HD-VC-2/3/4/5; datos observados con trazabilidad/provenance (HD-VC-5); sin cierre de HD-VC; sin modificación de D12/D13/D14; sin implementación) · justificación REGISTRADA (§14.g)
DECISION RECORD — HD-8: A — CATÁLOGO FORMAL ACOTADO (COMPUESTA — catálogo/identificador gobernado de venues; Symbol = representación canónica base/quote, fuera de ListingIdentity y ContractTerms; native_symbol = representación específica del venue (D10); ExchangeQuirks solo transporte; contrato de materialización no decidido; D12 abierto; HD-VC-1..7 pendientes; D19 pendiente; D15 LATER/SEPARATE; sin implementación) · justificación REGISTRADA (§14.h)
DECISION RECORD — HD-VC-2: A — VenueConstraints como modelo conceptual separado de ExchangeQuirks (ExchangeQuirks solo transporte/API/connector; restricciones operativas observables gobernadas por el venue; evita doble SSOT de tick_size; riesgo/execution consumen sin dependencia semántica del connector; separación Identity/Definition/Representation/VenueExecutionConstraints/ConnectorQuirks/RiskPolicy; min_notional≠min_order_usd; multiplier NO constraint automático — sujeto a D08/D12; PIT no decidido → HD-VC-4; sin especificación física; sin implementación) · justificación REGISTRADA (§15.a)
DECISION RECORD — HD-VC-3: A — frontera semántica PRICE / SIZE / NOTIONAL (decisión SEMÁNTICA, no física; VenueConstraints con ejes precision/increment/limits según apliquen; A = frontera, B = materialización futura para HD-VC-7, no incompatible; precision ≠ increment ≠ limits, sin derivación 10^-precision ni tick/limits derivados; PRICE ∩ {price_precision, price_increment/tick_size, min/max_price}; SIZE ∩ {size_precision, amount_precision, size_increment/step_size, lot_size, min/max_quantity, min/max_amount}; NOTIONAL ∩ {min/max_notional, min/max_cost}; min_notional NO ≡ min_cost declarado, y ninguno ≡ RiskPolicy.min_order_usd; currency/unit/cost/fees/inverse/conversiones abiertos a HD-VC-5/7 y D12/D19; multiplier = PENDING — D08/D12/D19; post_only/reduce_only/order-type/execution flags NO en VenueConstraints — OrderPolicy/ExecutionPolicy NO decidida, sin catch-all; ExchangeQuirks solo transporte (HD-7); CCXT/Nautilus = evidencia externa, no SSOT semántico; versionado/inmutabilidad de constraints NO decidido (§11 PROPUESTA); HD-VC-3 no decide HD-VC-4/5/6/7 ni D12 ni D19 ni Currency/Unit; sin especificación física; sin implementación) · justificación REGISTRADA (§15.b)
DECISION RECORD — HD-VC-4: D — temporalidad propia: B + invariante de continuidad/no solapamiento + bitemporalidad diferida (VenueConstraints con temporalidad propia e independiente de DefinitionVersion; pertenece conceptualmente a (ListingIdentity, venue); intervalo efectivo effective_at/effective_to; un cambio puramente operativo [tick_size, price_increment, size_increment, lot_size, min_notional] NO implica nueva ListingIdentity ni DefinitionVersion — nueva versión temporal de VenueConstraints; cambio de término contractual/identity-defining ⇒ derivar a D08/D12/D13; multiplier NO registrado como VenueConstraint — PENDING D08/D12/D19; PIT = requisito semántico get_venue_constraints(listing_identity, venue, as_of), prohibido usar «versión actual» para consulta histórica; intervalos efectivos no solapados; continuidad = invariante cuando exista cobertura esperada; política gap/overlap DIFERIDA a HD-VC-6; bitemporalidad/provenance [observed_at, recorded_at, source precedence, authority, correction, supersession, retraction] DIFERIDA a HD-VC-5; HD-5 intacto (ValidityWindow gobierna SymbolBinding; reutilización futura = materialización/D19); SymbolBinding y VenueConstraints cambian en ejes independientes (native_symbol no owner de constraints); sin API/port/schema/storage; sin especificación física; sin implementación) · justificación REGISTRADA (§15.c)
DECISION RECORD — HD-VC-5: B — Evidence explícita (VenueConstraints requieren provenance obligatoria [source · observed_at · recorded_at] + evidencia explícita identificable/referenciable [evidence_id · evidence_ref · evidence_hash · evidence_status] para auditar y reconstruir el fundamento de la regla conocida por OCM; Provenance ≠ Evidence; effective_at/effective_to [HD-VC-4, no reabierto] ≠ observed_at ≠ recorded_at; published_at = timestamp declarado por la fuente, no sustituye a effective/observed/recorded; versiones de HD-VC-4 conceptualmente inmutables — cambio de regla ⇒ nueva versión, evidencia adicional sobre la misma regla NO implica automáticamente nueva versión; conflicto [prioridad, jerarquía de autoridad, latest-wins, precedence, evidencias incompatibles, UNKNOWN_CONFLICT, ausencia, gap, overlap] y correction/supersession/retraction como política DIFERIDOS a HD-VC-6; HD-6 preservado [evidencia conflictiva conservada y no descartada]; bitemporalidad completa/as-known-by permanece abierta; source ≠ authority; sin tipos físicos/schema/tabla/storage/puerto/registry/API — HD-VC-7/D19; D12 = OPEN · D19 = PENDING; sin implementación) · justificación REGISTRADA (§15.d)
DECISION RECORD — HD-VC-6: D — Hybrid (resolución híbrida de VenueConstraints: autoridad explícita/contextual/granular/determinista —sin jerarquía física, materialización en fases posteriores— + aplicabilidad temporal efectiva [HD-VC-4, no reabierto] + resolución determinista reproducible; cuando NO existe resolución gobernada, inequívoca y reproducible → el resultado permanece explícitamente no resuelto bajo FAIL-CLOSED; SOURCE ≠ AUTHORITY; LATEST OBSERVED ≠ LATEST TRUTH; OBSERVED_AT ≠ EFFECTIVE_AT ≠ RECORDED_AT; secuencia conceptual: Paso 1 aplicabilidad temporal → Paso 2 evaluación de autoridad → Paso 3 resolución determinista [RESOLVED, reproducible] → Paso 4 no-resolución; estados conceptuales UNRESOLVED_CONFLICT [evidencia incompatible NO destruida; HD-6 preservado] · UNRESOLVED_GAP [sin relleno automático del valor anterior, sin interpolación, sin continuidad no demostrada] · UNRESOLVED_OVERLAP [sin selección por recencia/primera/última insertada/API/fuente determinada sin regla explícita futura] · UNRESOLVED_EVIDENCE [sin reconstrucción histórica por inferencia; current state ≠ historical proof; APIs current-only NO demuestran histórico] · UNRESOLVED_AUTHORITY [gobernanza insuficiente, distinto de fuente incorrecta] — taxonomía CONCEPTUAL, no enums/schemas/código; corrección [modifica estado interpretado] / supersession [modifica aplicabilidad/interpretación] / retraction [modifica estado de evidencia] sin borrado destructivo, historial auditable [HD-VC-5 preservado]; auditabilidad (RESOLVED y UNRESOLVED) y determinismo como requisitos; sin física ni consumidores → HD-VC-7/D19; no reapertura de HD-1..8, HD-VC-2..5, D08 [multiplier], D12, D19; sin especificación física; sin implementación) · justificación REGISTRADA (§15.e)
HUMAN ACTION REQUIRED — GATE C (§17.3 ADR nuevo formalizado por el humano · §17.4 registro en `tracking.yaml` + contrato/linter) — ninguna casilla de decisión pendiente: HD-1..HD-8 y HD-VC-1..HD-VC-7 completadas
DECISION RECORD — HD-VC-7: A — Domain-owned VenueConstraints (responsabilidad SEMÁNTICA del dominio de instrument/market metadata de market_data, separada de Canonical Instrument Identity, DefinitionVersion, SymbolBinding, ExchangeQuirks y RiskPolicy; resolución de VenueConstraints PERTENECE AL DOMAIN aplicando HD-VC-4 [temporalidad efectiva propia + PIT], HD-VC-5 [evidence/provenance: effective≠observed≠recorded, Provenance≠Evidence] y HD-VC-6 [D — Hybrid: aplicabilidad efectiva → autoridad explícita/contextual/granular/determinista → resolución determinista reproducible → RESOLVED | UNRESOLVED_* conceptual → FAIL-CLOSED; sin latest-wins/source-wins/observed-wins, sin interpolación ni reconstrucción histórica]; frontera no física Registry/Port outbound conceptual: INFORM al Application layer — no decide semántica, no sustituye al Domain, no es segundo policy engine, no es SSOT físico por sí mismo; reparto HD-4: APPLICATION=ORCHESTRATE · DOMAIN=DECIDE · PORT/REGISTRY=INFORM · ADAPTER=TRANSLATE/PROVIDE EVIDENCE · CONSUMERS=CONSUME; adapter puede observar exchange metadata / API response / venue definition / security definition / market info pero eso es EVIDENCE, no canonical truth — no declara "este valor es la verdad canónica", no resuelve, no publica VenueConstraints canónica; trading/execution pueden usar VenueConstraints para validar precio, cantidad, incrementos, notional y preparar órdenes sin ser owners semánticos; risk = consumer (RiskPolicy.min_order_usd ≠ VenueConstraints.min_notional; ninguna sustituye a la otra); research/analytics = consumers read-only, no redefinen; relación ListingIdentity + venue + effective time → VenueConstraints, con VenueConstraints ≠ DefinitionVersion ≠ SymbolBinding ≠ Identity, cambio de native_symbol NO altera constraints y cambio de constraints NO crea nueva ListingIdentity/DefinitionVersion; ExchangeQuirks = solo transporte/API/connector quirks (HD-7) — no SSOT semántico de constraints, y que un dato aparezca en adapter/connector no cambia ownership; multiplier/contract_size NO se incorpora automáticamente a VenueConstraints y permanece bajo D08/D12 (no reabierto); IDENTITY/DEFINITION/REPRESENTATION ≠ VenueConstraints ≠ ExchangeQuirks ≠ RiskPolicy (HD-8 preservado); materialización física NO cerrada — package layout, clases concretas, schema, tablas, Iceberg, Kafka topics, nombres de tables/ports, identificadores físicos, API pública, consumer contract, audit ledger y almacenamiento de estados UNRESOLVED_* quedan sujetos a D19 + decisión posterior de materialización; deuda aceptada y controlada: nuevo concepto de dominio, Registry/Port conceptual, mapeo por venue, normalización de alias, materialización futura, curación de metadata por venue; no reabre HD-1..8, HD-VC-2..6 ni D08; no decide D12, D19, HD-VC-1, multiplier, contrato de consumidor, schema, persistencia ni normalización completa de símbolos; sin especificación física; sin implementación) · justificación REGISTRADA (§15.f)
DECISION RECORD — HD-VC-1: A+ — VenueConstraints como hechos de admisibilidad operativa del venue + cláusula de derivación/exclusión semántica (HD-VC-1 decidido por el humano como A+, 2026-09-30; A+ = A + cláusula; A declara la NATURALEZA: VenueConstraints representa hechos de admisibilidad operativa GOBERNADOS POR EL VENUE —reglas de admisión de órdenes—, NO hechos contractuales del instrumento; scope conceptual (ListingIdentity, venue) [A.2]; versión temporal propia por (ListingIdentity, venue) con intervalo efectivo effective_at/effective_to y PIT, y un cambio de regla ⇒ NUEVA VERSIÓN TEMPORAL de VenueConstraints, NO ⇒ ListingIdentity, NO ⇒ DefinitionVersion, NO ⇒ relación D13/D14 [A.3, HD-VC-4 intacto]; evidence obligatoria + provenance [A.4, HD-VC-5 intacto]; ownership semántico en el DOMAIN de instrument/market metadata de market_data [A.5, HD-VC-7 intacto]; VenueConstraints NO ES Identity [A.6], NO ES DefinitionVersion [A.7], NO ES Representation [A.8], NO ES ExchangeQuirks [A.9], NO ES RiskPolicy [A.10]; ausencia NO invalida identidad pero SÍ puede bloquear ejecución ⇒ UNRESOLVED_GAP → FAIL-CLOSED [A.11]; alcance = seis ejes PRICE/SIZE/NOTIONAL × precision/increment/limits [A.12, HD-VC-3 intacto]; LA CLAUSULA A+: la «derivación» es DERIVACIÓN DE LA CLASIFICACIÓN SEMÁNTICA de un campo observado —un veredicto de clasificación/enrutamiento— y NUNCA derivación o inferencia de un valor; NO existe bajo A+ un «VenueConstraint derivado de una fuente semántica gobernada» como mecanismo de inferencia, y A+ NO deriva numéricamente VenueConstraints desde ContractTerms, DefinitionVersion u otra fuente semántica [desambiguación vinculante]; procedencia ≠ admisibilidad — que el valor lo publique el venue, un adapter o cualquier fuente NO le confiere la categoría; la admisibilidad se decide por la semántica de lo que el valor describe (regla semántica HD-3 capa A) y NO por el nombre del campo, su tipo, su posición en el payload ni su procedencia [A+.1]; los datos observados deben clasificarse semánticamente antes de cualquier admisión [A+.2]; si el dato pertenece inequívocamente al dominio de VenueConstraints y existe evidencia admisible ⇒ puede ser admitido como VenueConstraint [A+.3] — admisión directa por evidencia con ejemplos conceptuales tickSize → PRICE increment, stepSize/size increment → SIZE increment, minNotional → NOTIONAL limit, conservando source/provenance, evidence, observed_at, recorded_at, aplicabilidad efectiva HD-VC-4 y trazabilidad suficiente para auditoría; si el dato pertenece a otra frontera semántica ⇒ debe ser ENRUTADO al dominio/carril correspondiente, y el enrutamiento NO crea valor, versión, entidad, relación ni artefacto, sino un veredicto de clasificación auditable ligado a la evidencia concreta desde la que se emite [A+.4]; NO se permite inferencia numérica para fabricar VenueConstraints [A+.5] — prohibido derivar tick_size desde price_precision, derivar increment desde precision (incl. 10^-precision), derivar min_notional desde min_order_usd, derivar contractSize desde multiplier, derivar VenueConstraints mediante fórmulas no gobernadas, o asumir que dos nombres de campos equivalentes en un exchange significan el mismo concepto OCM; se preserva precision ≠ increment ≠ limits; NO se permite crear un SEGUNDO SSOT [A+.6] — un valor no puede ser simultáneamente término de ContractTerms/DefinitionVersion y VenueConstraint; si ya pertenece al contrato, su observación en el payload del venue es EVIDENCE y no una restricción adicional; un campo admitido como VenueConstraints no se replica en ContractTerms; y el carril de restricciones NO puede usarse para suplir una Definition incompleta; la admisión es auditable y conserva provenance/evidence, reutilizando —sin duplicar— la evidencia del campo observado [A+.7]; clasificación no determinista [A+.8] ⇒ NO se produce VenueConstraint canónico, NO se inventa valor, NO se utiliza «unknown constraint» como sustituto, NO se habilita vía alternativa por inferencia, se conserva la evidencia y se remite al carril semántico correspondiente (HD-3 Guard 1/Guard 2), sin redefinir HD-VC-6 y sin crear un UNRESOLVED_* nuevo (refinamiento físico = D19); REGLA DE CONFLICTO con dos naturalezas: (A) conflicto INTERNO del carril VenueConstraints (evidencia 1 tickSize = 0.01 vs evidencia 2 tickSize = 0.001 en el mismo (ListingIdentity, venue, T), ambas admisiones directas) ⇒ UNRESOLVED_CONFLICT → evidencia preservada → resolución conforme a HD-VC-6 → FAIL-CLOSED si no existe resolución gobernada, explícita y determinista, SIN source-wins / API-wins / latest-wins / observed-wins; (B) discrepancia que PERTENECE A OTRO CARRIL (venue publica contractSize = 100 vs ContractTerms.contract_multiplier = 10) ⇒ NO se convierte automáticamente en conflicto de VenueConstraints, permanece en el carril D08/D12/D13, la evidencia se conserva conforme a HD-VC-5/6 (HD-6 preservado: el conflicto no se destruye) y NO se resuelve mediante VenueConstraints; REGLA DE EXCLUSIÓN/ENRUTAMIENTO: contractSize/multiplier NO se incorpora automáticamente a VenueConstraints y permanece bajo D08/D12/D13; post_only y reduce_only fuera de VenueConstraints → OrderPolicy/ExecutionPolicy (NO decidida); native_symbol → Representation/SymbolBinding (HD-2); min_order_usd → RiskPolicy (HD-VC-2/3); A+ NO se utiliza para resolver decisiones de D08, D12, D13, D19 ni de otros carriles; PRESERVA HD-1..HD-8 y HD-VC-2..HD-VC-7 sin reapertura, en particular IDENTITY ≠ DEFINITION ≠ REPRESENTATION, VenueConstraints ≠ ExchangeQuirks, VenueConstraints ≠ RiskPolicy, Trading/Execution/Risk/Research como CONSUMIDORES y no owners, DOMAIN = DECIDE · APPLICATION = ORCHESTRATE · PORT/REGISTRY = INFORM · ADAPTER = TRANSLATE/PROVIDE EVIDENCE, evidence que no se destruye y resolución no determinista → FAIL-CLOSED cuando corresponda; preservados literalmente precision ≠ increment ≠ limits, min_notional ≠ min_order_usd, ExchangeQuirks = transporte/API únicamente, multiplier NO incorporado automáticamente, VenueConstraints domain-owned, PIT/effective_at (HD-VC-4), provenance/evidence (HD-VC-5), resolución determinista + FAIL-CLOSED (HD-VC-6), effective_at ≠ observed_at ≠ recorded_at, SOURCE ≠ AUTHORITY, LATEST OBSERVED ≠ LATEST TRUTH; NO decide ni reabre D08, D12, D13, D19 (D12 = OPEN · D19 = PENDING), ni si multiplier es Identity/Definition/VenueConstraint, ni catálogo o ventanas de DefinitionVersion, ni autoridad física concreta, ni jerarquía física de fuentes, ni corrección/supersession/retraction como mecanismo físico, ni persistence, schemas, tablas, Iceberg, Kafka, cache, API, package layout, class names, port names, registry names, consumer contracts, ni normalización completa Symbol/native_symbol/underlying/quote, ni OrderPolicy/ExecutionPolicy; cierra la faceta de COLOCACIÓN de §14.a punto 5 (fuera de DefinitionVersion, concepto con estructura y versionado propios) sin reabrir las facetas ya decididas; ALCANCE: establece la naturaleza semántica de VenueConstraints y su frontera de admisión/exclusión y enrutamiento de clasificación semántica; es un CONTRATO SEMÁNTICO y NO CONSTITUYE AUTORIZACIÓN DE IMPLEMENTACIÓN; sin especificación física; sin implementación) · justificación REGISTRADA (§15.g)

## Inventario de contradicciones históricas (C1–C4)

Las siguientes discrepancias se inventarían como históricas. Este ADR **no las
resuelve**, no armoniza fuentes y no autoriza reescribir ningún documento:

**C1 — ADR-0049: estado de D19-03..D19-10 y anticipación de D20–D24.**
Afirmación A: `ADR-0049-canonical-instrument-identity-g1.md` líneas 53–54 —
«D19-01 = DECIDED (C+ — Hybrid Controlled Identity). D19-02 = DECIDED (A — Pure
Domain Entity). D19-03..D19-10 permanecen pendientes.» y «NO se anticipa D20–D24.»
Afirmación B: mismo archivo, líneas 544–560 — registro D19-03..D19-10 = DECIDED
y D20b-01..06 = DECIDED. Ambas verificadas directamente por lectura. ADR-0049
no se modifica; §17.3 lo prohíbe explícitamente. Decisión sobre eventual
edición editorial de ADR-0049: pendiente de decisión humana aparte.

**C2 — Pack D19: estado de D19-06..D19-10.**
Afirmación A: `4ad03ebae7b2d25c6a31f8954f0d8e67d8a677e0:docs/audits/AUDIT_OCM_d19-human-decision-pack_2026-09-30.md`
línea 841 — «D19-06..D19-10 = HUMAN DECISION REQUIRED / BLOCKED».
Afirmación B: mismo blob, líneas 2262–2291 — tabla ejecutiva con D19-01..10 y
D20b = DECIDED, más línea de registration. Ambas verificadas por lectura del
blob. Sin resolución.

**C3 — INDUSTRY / KN_BASE frente a ADR-0049 (faceta F-OCM-01).**
Afirmación A: campos separados en dos fuentes (cada cita entre comillas es un
fragmento literal de su ubicación; no constituyen una cita continua única):
`4ad03eba:docs/audits/AUDIT_OCM_d19-01-industry-bow-review_2026-10-03.md`
líneas 34–35 («IMPLEMENTATION = NOT AUTHORIZED», «DECISION REGISTERED = NO») y
líneas 167–169 («D19-01: HUMAN DECISION REQUIRED», «DECISION: NOT YET DECIDED»,
«D19: PENDING / BLOCKED»); y
`4ad03eba:docs/knowledge/KNOWLEDGE_BASE_CONTRACT.md` línea 166
(«D19-01 = HUMAN DECISION REQUIRED · D19 = PENDING / BLOCKED · GATE C = BLOCKED»).
Afirmación B: ADR-0049 líneas 544+ — D19-01 = DECIDED. La jerarquía normativa
entre estas fuentes fue resuelta por Q1 (registrada en ADR-0055); la
reconciliación material de los textos contradictorios (fuentes B1/B2 no
localizadas en revisiones previas) permanece pendiente. Este ADR no la ejecuta.

**C4 — Estado de D12 en tres fuentes.**
Afirmación A: decision-support blob, línea 24 (al cierre: «D12 = OPEN · D19 =
PENDING · GATE C = BLOCKED · IMPLEMENTATION = NOT AUTHORIZED»), y las líneas 813
(«Se mantiene **D12 = OPEN · D19 = PENDING**») y 1046 («D12 = OPEN»).
Afirmación B: pack blob línea 13/L34 — «D12: DECIDED — Alternative 5 +
Alternative 3».
Afirmación C: ADR-0049 líneas 335/361 — G1-D12 = «C/RESTRICTED» con «NO decidir
todavía: catálogo exhaustivo…». **D12 queda fuera del alcance decisorio de
este ADR**: no se elige interpretación, no se cambia estado, no se declara
reconciliado.

## Justificación técnica

- ADR independiente: única alternativa con tipo documental, ruta, numeración y
  enum de estados ya definidos y verificables en GOVERNANCE y la plantilla.
- `**Estado:** Propuesto`: grafía de la plantilla; compatible con M4 por
  normalización (`propuest* → PROPUESTO` en `scripts/audit_validator.py`)
  y permitida por M15, que solo prohíbe estados inventados cerrados.
  Validadores **NOT VERIFIED** en esta entrega (no ejecutados por mandato).
- BC `market_data | trading`: únicos contextos con impacto directamente
  demostrable (owner por HD-VC-7; consumer por port con la separación
  `min_notional ≠ min_order_usd` de HD-VC-3). No se infiere afectación de
  `portfolio`, `shared` u `ocm` a partir de su enumeración en ADR-0049.
- Consolidación literal desde el commit backup, no desde resúmenes, para
  preservar procedencia verificable de cada decisión.

## Consecuencias

- §17.3 queda materializada documentalmente como ADR-0057, con aceptación
  documental de alcance limitado registrada en el campo Estado mediante decisión
  humana separada (2026-10-11); la casilla «ADR nuevo formalizado por el humano»
  de la línea HUMAN ACTION del source se satisface como artefacto. La aceptación
  es únicamente documental: no declara vigencia normativa, no resuelve C1–C4,
  mantiene D12 fuera del alcance decisorio y no autoriza implementación.
- §17.4 (registro en `tracking.yaml` + contrato/linter), GATE C y vigencia
  normativa de este ADR quedan **fuera de alcance** y requieren
  autorización humana independiente. La aceptación documental se registró en
  el campo Estado por decisión humana separada.
- La disponibilidad de la fuente de consolidación depende del commit backup
  `4ad03eba` (no mergeado a `main`); persistir el decision-support en `main`
  es decisión humana independiente, no cubierta por este ADR.
- C1–C4 permanecen abiertas y documentadas; D12 permanece sin resolver.
- `portfolio`, `shared` y `ocm` quedan como contextos pendientes de justificación.
- Sin contratos BC-NN nuevos; este ADR no crea, modifica ni deroga contratos de
  import-linter ni reglas de `architecture_linter`.
- Este ADR no autoriza ni modifica `docs/plans/tracking.yaml`, ni código, tests,
  adaptadores, linters ni CI.
- Sin declaración de vigencia normativa; la aceptación documental queda limitada al campo Estado.

**IMPLEMENTATION = NOT AUTHORIZED** — heredada de las fuentes originales
(ADR-0049 L562 y líneas de prohibición del decision-support) y reafirmada como
límite de este documento. Este ADR es un artefacto documental en estado
Propuesto; no otorga autorización de implementación, de §17.4, de GATE C, de
commit/push ni de modificación de tracking.

## Referencias

- ADRs relacionados: ADR-0049 (serie G1, fuente normativa existente, intacta);
  ADR-0055 (autoridad normativa D19/D20b, aceptado documentalmente con la cláusula
  de no vigencia declarada en su propio estado).
- Fuente literal de los registros: commit `4ad03ebae7b2d25c6a31f8954f0d8e67d8a677e0`
  (ramas `backup/untracked-2026-10-11` y su ref remota), ruta
  `docs/audits/AUDIT_OCM_decision-support-hd-venue-constraints-identity_2026-09-29.md`,
  líneas 1117–1132 (DECISION RECORDS); §14.a–§14.h y §15.a–§15.g (justificaciones);
  §16 (marco G1); §17 (precondiciones GATE C). Solo lectura; no restaurado.
- Fuentes de contradicciones: `4ad03eba:docs/audits/AUDIT_OCM_d19-human-decision-pack_2026-09-30.md`;
  `4ad03eba:docs/audits/AUDIT_OCM_d19-01-industry-bow-review_2026-10-03.md`;
  `4ad03eba:docs/knowledge/KNOWLEDGE_BASE_CONTRACT.md`; ADR-0049 (líneas citadas).
- Plantilla y gobernanza: `docs/architecture/decisions/ADR-template.md`;
  `docs/architecture/GOVERNANCE.md` (§2–§3: triggers, numeración secuencial,
  plantilla obligatoria).
- Código (referencia, sin modificación): `packages/market_data/domain/` (VOs de
  identidad, trackeados en `bb90c85f`); `packages/trading/risk/models.py`
  (`min_order_usd`, separado de `min_notional` por decisión HD-VC-3).
