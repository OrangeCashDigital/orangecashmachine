# Decision Support Pack — HD-1..HD-8 y HD-VC-1..HD-VC-7 (venue constraints) + evidencia de industria

**Fecha:** 2026-09-29
**Propósito:** soporte formal de decisión humana para GATE C de la serie G1 (Canonical Instrument Identity, ADR-0049) — separación Identity/Definition, catálogo, guard, temporalidad, provenance, B-MD-005 y modelo de venue constraints (`tick_size`, `lot_size`, `min_notional`).
**Destino de escritura:** `docs/audits/` (único destino permitido por el protocolo de auditoría).
**Restricciones:** READ-ONLY absoluto. No modificar código, tests, ADR, `tracking.yaml`, CI, linters, contratos, schemas, registry, configs; no crear archivos fuera de esta entrega; no `git add/commit/push`; no cambios de rama.

> **MARCO HUMANO:** este documento consolida en disco los entregables emitidos en la sesión (Decision Pack HD-1..HD-8; Decision Pack HD-VC-1..HD-VC-7; investigación arquitectónica profesional con evidencia de industria) y el registro de decisiones humanas. Estado de decisiones: **HD-1 decidido y cerrado (A — Separación formal; justificación en §14.a)**; **HD-2 decidido y cerrado (A — Separación estricta respecto de native_symbol; justificación en §14.b)**; **HD-3 decidido y cerrado (C refinada — Regla semántica normativa + catálogo operacional + FAIL-CLOSED; justificación en §14.c)**; **HD-4 decidido y cerrado (D — reparto contractizado: DOMAIN decide · APPLICATION orquesta · PORT/REGISTRY informa; justificación en §14.d)**; **HD-5 decidido y cerrado (A + D — ValidityWindow como VO propio de dominio + contrato normativo; justificación en §14.e)**; **HD-6 decidido y cerrado (A/I — P1=A fail-closed ante evidencia conflictiva · P2=B separación conceptual EvidenceSource/EvidenceStatus · P3=C guard central en DOMAIN · P4=A IdentityConflictError · P5=A observed_at conceptual · P6=A renuncias explícitas; justificación en §14.f)**; **HD-7 decidido y cerrado (A — APROBAR ACOTADO: instrumentos/límites/precisión como datos observados del venue, con condiciones vinculantes de alcance; justificación en §14.g)**; **HD-8 decidido y cerrado (A — CATÁLOGO FORMAL ACOTADO: decisión compuesta — catálogo/identificador gobernado de venues; Symbol como representación canónica fuera de identidad y ContractTerms; ExchangeQuirks solo transporte; D12 abierto; HD-VC-1..7 pendientes; contrato de materialización no decidido; justificación en §14.h)**; **HD-VC-2 decidido y cerrado (A — VenueConstraints como modelo conceptual separado de ExchangeQuirks; restricciones operativas observables gobernadas por el venue; min_notional≠min_order_usd; multiplier sujeto a D08/D12; sin implementación; justificación en §15.a)**; **HD-VC-3 decidido y cerrado (A — frontera semántica PRICE / SIZE / NOTIONAL con ejes precision / increment / limits; decisión SEMÁNTICA, no física; B = refinamiento de materialización futuro, no frontera distinta; precision ≠ increment ≠ limits; min_notional/min_cost ≠ RiskPolicy.min_order_usd; multiplier = PENDING — D08/D12/D19; OrderPolicy/ExecutionPolicy NO decidida; sin implementación; justificación en §15.b)**; **HD-VC-4 decidido y cerrado (D — temporalidad propia: VenueConstraints con intervalo efectivo effective_at/effective_to por (ListingIdentity, venue), independiente de DefinitionVersion; un cambio puramente operativo de constraint NO implica nueva ListingIdentity ni DefinitionVersion — nueva versión temporal; gatillo contractual ⇒ D08/D12/D13; PIT = requisito semántico get_venue_constraints; intervalos no solapados y continuidad cuando exista cobertura esperada; política gap/overlap diferida a HD-VC-6; bitemporalidad/provenance diferida a HD-VC-5; HD-5 intacto; sin implementación; justificación en §15.c)**; **HD-VC-5 decidido y cerrado (B — Evidence explícita: VenueConstraints requieren provenance obligatoria [source · observed_at · recorded_at] + evidencia explícita identificable/referenciable [evidence_id/evidence_ref/evidence_hash/evidence_status]; Provenance ≠ Evidence; effective ≠ observed ≠ recorded; published_at = timestamp declarado por la fuente, no sustituye a effective/observed/recorded; versiones inmutables — cambio de regla ⇒ nueva versión, evidencia adicional NO implica automáticamente nueva versión; conflicto/autoridad/precedencia/correction/supersession/retraction como política diferidos a HD-VC-6; bitemporalidad completa/as-known-by abierta; HD-6 preservado; sin implementación; justificación en §15.d)**; **HD-VC-6 decidido y cerrado (D — Hybrid: resolución híbrida basada en autoridad explícita/contextual/granular/determinista [sin jerarquía física], aplicabilidad temporal efectiva y resolución determinista; cuando no hay resolución gobernada, inequívoca y reproducible → resultado explícitamente no resuelto bajo FAIL-CLOSED; SOURCE≠AUTHORITY; LATEST OBSERVED≠LATEST TRUTH; OBSERVED_AT≠EFFECTIVE_AT≠RECORDED_AT; taxonomía conceptual UNRESOLVED_CONFLICT/GAP/OVERLAP/EVIDENCE/AUTHORITY [no física]; sin física ni consumidores → HD-VC-7; HD-1..8 y HD-VC-2..5 no reabiertos; justificación en §15.e)**; **HD-VC-7 decidido y cerrado (A — Domain-owned VenueConstraints: ownership semántico en el dominio de instrument/market metadata de market_data, separado de Identity/DefinitionVersion/SymbolBinding/ExchangeQuirks/RiskPolicy; resolución de VenueConstraints en DOMAIN aplicando HD-VC-4/5/6; Registry/Port outbound INFORM; adapter TRANSLATE/PROVIDE EVIDENCE; consumers (trading/execution/risk/research) consumen por port sin ser owners; materialización física NO cerrada — D19; sin implementación; justificación en §15.f)**; **HD-VC-1 decidido y cerrado (A+ — VenueConstraints como hechos de admisibilidad operativa del venue + cláusula de derivación/exclusión semántica: naturaleza declarada de dato de venue con scope (ListingIdentity, venue), versión temporal propia, evidence obligatoria y ownership en el dominio; explícitamente NO Identity / NO DefinitionVersion / NO Representation / NO ExchangeQuirks / NO RiskPolicy; cláusula de derivación entendida como regla de exclusión y enrutamiento de la clasificación semántica — NUNCA como inferencia o derivación de valores; —; admisión directa por evidencia, enrutamiento al carril semántico propio cuando la semántica no pertenece al contrato de VenueConstraints, prohibición de inferencia numérica y de segundo SSOT, y FAIL-CLOSED de admisión si la clasificación no es determinista; D08/D12/D13/D19 NO se deciden ni se reabren; materialización NO autorizada; justificación en §15.g)**. Las casillas de decisión del pack quedan **completas**; las precondiciones §17.3 (ADR nuevo formalizado por el humano) y §17.4 (registro en `tracking.yaml` + contrato/linter) siguen pendientes, por lo que **GATE C permanece BLOCKED**.

---

## 1. Executive Summary

La serie G1 (ADR-0049) fijó identidad canónica de instrumento (D01–D15), pero **no clasifica los límites/precisión de venue** (`tick_size`, `lot_size`, `min_notional`, y vecinos). Su estatus normativo es **REQUIRES HUMAN DECISION**: D12 dijo "NO decidir todavía" (catálogo/ventanas/autoridad/provenance) y B-MD-005 (instrumentos/límites/precisión como datos) está **PROPOSED**, ausente de `tracking.yaml`.

La evidencia de la industria (NautilusTrader, Hummingbot, Freqtrade, CCXT, Binance, CME, OpenFIGI, FIX, ISO 20022) demuestra cinco ejes semánticos — **IDENTITY / DEFINITION / REPRESENTATION / EXECUTION CONSTRAINTS / EXCHANGE CONNECTOR QUIRKS** — con consecuencias operativas:

1. Los constraints (`tick`/`lot`/`min`) son **datos por símbolo×venue**, mutables, provistos por el venue; **nunca identidad**.
2. `precision ≠ limits ≠ increment` (CCXT lo documenta literalmente; Nautilus lo impone como invariante de construcción).
3. `min_notional` (venue) **no es** `min_order_usd` (política de riesgo).
4. La industria **no resuelve point-in-time de constraints** (Hummingbot snapshot, Nautilus upsert destructivo, Freqtrade limitación documentada). El único patrón profesional es FIX: **event stream bitemporal** (`SecurityUpdateAction` + `TransactTime` + `EffectiveBusinessDate`).
5. La identidad se separa de la representación (ticker ≠ identidad; FIGI/MIC jerárquicos; FIX separa Instrument / InstrumentExtension / MarketSegmentGrp).

La propuesta objetivo **NO vinculante** (Best of Worlds) — **cuyas partes nuevas son PROPUESTAS, no contrato vigente**: `ListingIdentity → DefinitionVersion [ContractTerms (inmutable, D05–D08) + VenueConstraints (temporal, [from,to))]` — donde **VenueConstraints**, su ventana `[from,to)`, su `provenance` y la regla «mutación de constraint ⇒ nueva DefinitionVersion» son **PROPUESTAS** y no deciden lo que D12 dejó abierto (catálogo, ventanas, autoridad de validación, provenance y metadatos). `SymbolBinding → native_symbol` permanece en la capa de representación (D01/D10); `ExchangeQuirks` = solo transporte. Antes del GATE C quedan dos bloqueadores: **HD-7 / HD-VC-2** (status y dueño de B-MD-005 para evitar doble SSOT de `tick_size`) y la decisión del catálogo. **HD-8 ya está decidido** (A — CATÁLOGO FORMAL ACOTADO, §14.h): catálogo/identificador gobernado de venues y `Symbol` como representación canónica (fuera de identidad y de ContractTerms), sin cerrar D12. **HD-VC-2 ya está decidido** (A — VenueConstraints como modelo separado de ExchangeQuirks, §15.a): resuelve el dueño del modelo, evita el doble SSOT de `tick_size` y mantiene `ExchangeQuirks` como solo transporte. **HD-VC-3 ya está decidido** (A — frontera semántica PRICE / SIZE / NOTIONAL con ejes precision/increment/limits, §15.b): resuelve qué conceptos pertenecen a VenueConstraints como decisión semántica; B queda disponible como refinamiento de materialización para HD-VC-7 (NO implementación); multiplier permanece PENDING — D08/D12/D19; OrderPolicy/ExecutionPolicy NO decidida. **HD-VC-4 ya está decidido** (D — temporalidad propia: B + invariante de continuidad/no solapamiento + bitemporalidad diferida, §15.c): VenueConstraints tiene temporalidad propia e independiente de DefinitionVersion (intervalo efectivo `effective_at`/`effective_to` por `(ListingIdentity, venue)`); un cambio puramente operativo de constraint NO dispara nueva ListingIdentity ni DefinitionVersion — nueva versión temporal; PIT es requisito semántico; política gap/overlap → HD-VC-6; bitemporalidad/provenance → HD-VC-5. **HD-VC-5 ya está decidido** (B — Evidence explícita, §15.d): provenance obligatoria (source · observed_at · recorded_at) + evidencia explícita identificable/referenciable (evidence_id/evidence_ref/evidence_hash/evidence_status), con Provenance ≠ Evidence y effective ≠ observed ≠ recorded; conflicto/autoridad/precedencia y correction/supersession/retraction → HD-VC-6. **HD-VC-6 ya está decidido** (D — Hybrid, §15.e): autoridad explícita/contextual/granular y determinista sin jerarquía física; secuencia aplicabilidad temporal → autoridad → resolución determinista → no-resolución explícita; FAIL-CLOSED residual con taxonomía conceptual UNRESOLVED_CONFLICT/GAP/OVERLAP/EVIDENCE/AUTHORITY; source ≠ authority; latest-observed ≠ latest-truth; evidencia nunca eliminada destructivamente; sin física ni consumidores → HD-VC-7. **HD-VC-7 ya está decidido** (A — Domain-owned VenueConstraints, §15.f): ownership semántico en domain de market_data (instrument/market metadata), separación estricta entre Identity/Definition/Representation/VenueConstraints/ExchangeQuirks/RiskPolicy; APPLICATION = ORCHESTRATE · DOMAIN = DECIDE · PORT/REGISTRY = INFORM · ADAPTER = TRANSLATE/PROVIDE EVIDENCE; resolución pertenece al dominio con PIT y FAIL-CLOSED; VenueConstraints × Venue con tiempo efectivo, independiente de SymbolBinding; materialización física, schemas, tablas, package layout, nombres e identificadores NO cerrados (D19 y decisiones posteriores); **HD-VC-1 ya está decidido** (A+ — VenueConstraints como hechos de admisibilidad operativa del venue + cláusula de derivación/exclusión semántica, §15.g): la naturaleza queda declarada —dato de venue con scope (ListingIdentity, venue), versión temporal propia (HD-VC-4), evidence obligatoria (HD-VC-5), resolución determinista + FAIL-CLOSED (HD-VC-6), ownership en el dominio (HD-VC-7), y explícitamente NO Identity / NO DefinitionVersion / NO Representation / NO ExchangeQuirks / NO RiskPolicy— y se añade la cláusula de derivación entendida como **regla de exclusión y enrutamiento de la clasificación semántica**, que **no** es derivación numérica ni inferencia de valores: los datos observados se clasifican semánticamente, se admiten por evidencia directa si su semántica cae dentro del contrato de VenueConstraints, y se enrutan a su carril semántico en caso contrario — sin fabricar valores, sin crear un segundo SSOT y cerrando la admisión si la clasificación no es determinista. **D12 = OPEN · D19 = PENDING · GATE C = BLOCKED · IMPLEMENTATION = NOT AUTHORIZED.**

---

## 2. Alcance y método

- **Encargo:** preparar la decisión humana formal; entregables emitidos en el chat y consolidados aquí.
- **Método:** discovery conforme a AUDIT_PROTOCOL §C; baseline §D; evidencia read-only §K; sin findings NUEVO (nada aquí es un incumplimiento: es soporte de decisión).
- **Fuentes externas:** LEVEL 0 (conocimiento). Ninguna se adopta como norma por este documento; requiere cadena `Conocimiento → Decisión Humana → ADR → Control`.

---

## 3. Governance baseline (jerarquía de autoridad)

- LEVEL 4: Plan Maestro, `docs/architecture/GOVERNANCE.md`, AGENTS.md, AUDIT_PROTOCOL v2.1.
- LEVEL 3: ADR-0049 (D01–D15 adoptados por el humano el 2026-09-28/29: G1-ACCEPTANCE = B; G1-D14/D15 aceptados). **D01–D13 no se reinterpretan.**
- LEVEL 2: `docs/plans/tracking.yaml` (B-MD-005 **ausente**), código G1 (`listing_identity`, VOs, policies, port, resolver), contratos import-linter.
- LEVEL 1/0: notas internas, KB y literatura externa (solo referencia).

**Regla:** libro/paper/estándar externo NO es contrato OCM. Toda obligación requiere la cadena `Conocimiento → Decisión Humana → ADR/Governance → Control → Implementación`.

---

## 4. SAFEOPS baseline (antes de esta entrega)

- Working tree root: `/home/orangemusic/trading/orangecashmachine`
- Branch: `feat/m22b-governance`
- HEAD: `05317f149c57afa05fa5f667b53272e6ace7ba49`
- `git status --short`: **44 marcas preexistentes** (17 `M` + 27 `??`), idénticas en las 5 verificaciones de la sesión.
- Estado mecánico `scripts/audit_validator.py` (previo, sin cambios): FAIL preexistentes **M21** (`docs/audits/ARCHITECTURE-DISCOVERY-2026-09-27.md` nombre no canónico) y **M4** (ADR-0049 `status` no está en el enum `ACEPTADO/OBSOLETO/PROPUESTO/REEMPLAZADO`); WARN preexistentes **M25** (ADR-0020, ADR-0044 en `related_adrs` sin regla) y **M20** (3 controles FAIL sin finding NUEVO). Nada de esto lo introduce esta entrega.

---

## 5. Estado actual de OCM

- G1 materializado: `ListingIdentity` (venue-scoped, D03; instrument_type, D04; terms, D05–D08; lifecycle, D09; definition_version; `transition()`; `new_definition_version(terms, provenance)`), VOs (`contract_terms`, `instrument_type` [PERPETUAL/FUTURE/OPTION/SPOT], `symbol_binding`, `lifecycle`, `relations`, `provenance`, `exchange_quirks`), policy de resolución FAIL-CLOSED, port read-only, use case.
- `ExchangeQuirks` = **solo transporte** (backward_pagination, requires_end_at, reject_zero_since, origin_fallback_date). Sin tick/lot/min.
- **Tercera acepción de "símbolo":** existe un VO `Symbol` (par `base/quote`, "BTC/USDT") junto a `native_symbol` y a `underlying/quote` de ContractTerms → ownership fijado por **HD-8 (A — CATÁLOGO FORMAL ACOTADO, §14.h)**: `Symbol` = representación canónica base/quote, fuera de identidad y de ContractTerms; `native_symbol` = representación específica del venue (D10); contrato exacto `Symbol ↔ native_symbol ↔ underlying/quote` para la fase de materialización.
- `trading/risk/models.py`: `min/max_order_usd`, `max_open_positions` (política de riesgo — nivel distinto de constraints de venue).
- B-MD-005 (del deep audit 2026-08-14): **PROPOSED, P1** — extender `exchange_quirks.py` con `price_precision`, `amount_precision`, `min_amount`, `min_cost`, `tick_size`. No adoptado, sin ADR, sin tracking. Auditoría F-MD-007.

---

## 6. Evidencia de industria (5 unidades de investigación, LEVEL 0)

### 6.1 NautilusTrader
- InstrumentClass real: `CryptoPerpetual` / `PerpetualContract` = **SWAP**. **NO existen** las clases `Perpetual`/`OptionsContract`/`FinancialInstrument` (naming: cuidado al citarlo como referencia).
- `Instrument` trait exige `price_increment()` y `size_increment()`; `lot_size()` es opcional; `min/max_quantity` y `min/max_notional` opcionales.
- `price_precision`/`size_precision` existen con invariante de construcción `increment.precision == precision` (precision es **derivada** del incremento).
- `InstrumentProvider` = **upsert destructivo** (sin historial/versionado). `InstrumentStatus` != instrumento.
- RiskEngine valida precisión y bounds; **no** valida divisibilidad por incremento y **nunca redondea** la cantidad.

### 6.2 Hummingbot + Freqtrade
- Hummingbot: `TradingRule` (15 campos) por (exchange × symbol), construido por el connector en loop de polling (~30 min). Cuantización: `floor(amount / quantum) * quantum`. Rechazo duro si la cantidad post-cuantización no es válida. **Sin timestamps ni versionado.**
- Freqtrade: constraints derivadas de `markets` de ccxt; `validate_stake_amount` con tope de desvío **+30%** y `max()` de candidatos amount/cost. Limitación histórica **oficial**: *"Freqtrade has however no information about historic limits"* / *"historic precision limits are not available"*.

### 6.3 CCXT
- Precisión: `precision.price/amount` dependen de `exchange.precisionMode` (**TICK_SIZE** float / **SIGNIFICANT_DIGITS** / **DECIMAL_PLACES** deprecado).
- Límites ortogonales a precisión — *"Do not confuse limits with precision!"*: `limits.{amount,price,cost,leverage}.{min,max}`.
- `cost = price * amount` (notional). Redondeos: amount→TRUNCATE, price→ROUND, cost→TRUNCATE.
- Derivados: mismo `market` por símbolo en el string `BASE/QUOTE:SETTLE[-YYMMDD[-STRIKE-C|P]]`; campos `contract`, `settle`, `contractSize`, `linear`, `inverse`. `info` duplica el payload crudo del venue.

### 6.4 Binance + CME
- Spot: filtros por símbolo en `symbols[].filters[]`: **PRICE_FILTER** (`price % tickSize == 0`), **LOT_SIZE** (`quantity % stepSize == 0`), **MIN_NOTIONAL**, **NOTIONAL**, `MAX_NUM_ORDERS`, `PERCENT_PRICE`, etc.
- USDT-M futures (fapi): `pricePrecision`/`quantityPrecision` enteros + filtros PRICE_FILTER/LOT_SIZE/MARKET_LOT_SIZE/`MAX_NUM_ORDERS{limit}`/`MIN_NOTIONAL{notional}`. Identidad contractual (`contractType`, `deliveryDate`, `onboardDate`, `baseAsset`, `quoteAsset`, `marginAsset`) **separada** de `filters[]`.
- **Contradicción observada** entre precision y tickSize (BLZUSDT: precision 5 vs tickSize 4 decimales) → la derivación precision↔tick es convencional de implementación, **no normativa**.
- COIN-M (dapi): `contractSize` es campo propio del símbolo (fuera de filters). MIN_NOTIONAL→NOTIONAL es migración documentada.
- CME: specs con Contract Unit, Price Quotation, Minimum Price Fluctuation (**Regular vs Reduced tick**), Value per Tick. El tick es **venue-defined y mutable** ("shall be determined by the Exchange"), varía por banda de precio y calendario de delivery.

### 6.5 OpenFIGI / FIX / ISO
- FIGI: 12 chars; jerarquía **leaf FIGI** (venue) → **Composite** (country/market) → **Share Class** (global; solo equities/funds). Leaf con **0-1 exchange code**; **sin equivalencia cross-venue** (venue distinto ⇒ leaf FIGI distinto).
- **Ticker NO es identidad** ("may be linked to multiple instruments"); `exchCode` está sobrecargado; el key de venue es **MIC** (ISO 10383).
- FIX `SecurityDefinition` (35=d) separa: **Instrument** (composición), **InstrumentExtension** (`CFICode`), **MarketSegmentGrp** (capa de listing/trading rules). **`MinPriceIncrement` (tag 969) vive en la capa market-segment**, no en el instrumento. `SettlCurrency` no está en SecurityDefinition. `ContractMultiplier` (231), `SecurityExchange` (207).
- **Patrón bitemporal profesional:** `SecurityUpdateAction` (A/M/D) + `TransactTime` (60) + `EffectiveBusinessDate` (2400) = event stream **versionado punto-en-el-tiempo**. Único patrón PIT real de la industria.
- ISO 20022: MIC es eje separado (venue); SRD es área de negocio aparte; `GenericIdentification20`. OFR/ACTUS = instrumento como obligaciones de cash-flow contractuales.

---

## 7. Ejes semánticos (5) — consecuencia central

| Eje | Qué modela | Evidencia | En OCM |
|---|---|---|---|
| **IDENTITY** | qué es el instrumento, venue-scoped | FIX Instrument; FIGI leaf; CCXT market/contractType | `ListingIdentity` (D03–D08) |
| **DEFINITION** | términos contractuales estables | FIX Instrument; Nautilus InstrumentClass | `ContractTerms` + DefinitionVersion (D12) |
| **REPRESENTATION** | formas de nombrar/referenciar | FIGI ticker vs MIC; CCXT symbol string | `SymbolBinding`/`native_symbol` (D10); `Symbol` |
| **EXECUTION CONSTRAINTS** | tick/lot/min/precision por símbolo×venue, mutables | Binance filters; CME Minimum Price Fluctuation; Nautilus increments | **NO modelado → HD-VC** |
| **EXCHANGE QUIRKS** | comportamiento de transporte del connector | — | `ExchangeQuirks` (solo transporte) |

**Consecuencia (alineada con G1):** los constraints **nunca** forman parte de la identidad (D01/D09/D12) y su ausencia no invalida la identidad (sí puede bloquear ejecución). **HD-VC-3 resolvió la frontera semántica** (A — PRICE/SIZE/NOTIONAL, §15.b) y **HD-VC-4 la temporalidad** (D — §15.c): un cambio de constraint puramente operativo se representa como **nueva versión temporal de VenueConstraints** (intervalo efectivo propio), NO como **nueva DefinitionVersion**. El versionado/ventanas de DefinitionVersion como entidad contractual permanece en **D12 (PROPUESTA no decidida)**.

---

## 8. Precision vs Limits (frontera normativa)

1. `tick_size` (precision) = granularidad del precio; `stepSize` = granularidad de la cantidad.
2. `min/max` (limits) = cota de cantidad y **notional** (`cost = price*amount`), no solo quantity.
3. La **divisibilidad** (`% tickSize == 0`) es el control de validez del venue (Binance la impone; Nautilus **no** la valida al enviar).
4. **Precision ≠ increment ≠ limits.** En Nautilus la precisión es invariante derivada del incremento; en Binance son campos inconsistentes (BLZUSDT). → se recomienda (PROPUESTA de diseño, no norma) no asumir derivación determinista entre precision y tick y **almacenar ambos como datos observados** con su fuente — **decidido en HD-VC-5 (B — provenance/evidence explícitas, §15.d)**.

---

## 9. Temporalidad / point-in-time

- Hummingbot: snapshot opaco sin timestamps → no auditable.
- Nautilus: upsert destructivo → no hay historial.
- Freqtrade: **documenta explícitamente** la falta de límites históricos.
- **FIX: evento bitemporal (`TransactTime` + `EffectiveBusinessDate`)** es el único patrón profesional con punto-en-el-tiempo.

**Consecuencia:** **D10 gobierna la validez temporal de SymbolBinding, no la de VenueConstraints.** **HD-VC-4 ya resuelto (D — §15.c):** VenueConstraints adopta un **intervalo efectivo propio** (`effective_at`/`effective_to`) por `(ListingIdentity, venue)`, con PIT como requisito semántico, sin introducir un nuevo paradigma; la reutilización de `ValidityWindow` como abstracción temporal común queda como cuestión futura de materialización/**D19**; FIX completo (A/M/D events) no se adopta para G1.

---

## 10. Identidad y representación

- Ticker/native_symbol es **representación**, no identidad (FIGI, FIX, CCXT lo confirman) → refuerza D01/D10.
- MIC = eje de venue; FIGI leaf venue-scoped → refuerza D03.
- FIX separa marketplace (listing) del instrumento → refuerza D12 (DefinitionVersion) y, para HD-VC-2, la **colocación de las trading rules en una capa de listing**, no en el instrumento.
- Cross-venue económico sigue regido por D15 (NOT AUTHORIZED / SEPARATE FUTURE DECISION) — los constraints de venue distintos por venue **no implican** relación económica.

---

## 11. Propuesta de modelo objetivo — NO VINCULANTE (Best of Worlds)

> **Todo el contenido de esta sección es PROPUESTA de diseño, sin valor normativo.**
> D12 permanece explícitamente abierta en lo no decidido: catálogo exhaustivo de términos
> identity-defining vs Definition, ventanas de validez de Definition, autoridad de validación,
> provenance, metadatos y otros detalles de DefinitionVersion. Ningún elemento nuevo se
> convierte en contrato vigente sin decisión humana.

```
ListingIdentity (D03–D08, términos inmutables)
 └─ DefinitionVersion { version:int ; [from,to) ; terms: ContractTerms (inmutable) ;
                        constraints: VenueConstraints? (tick_size, lot_size, min/max_qty,
                        min/max_notional, price_precision, amount_precision …
                        con valid_from/valid_to [from,to) y provenance) }  ── PROPUESTA
SymbolBinding (D10) → native_symbol (representación; se despeja el tercer VO Symbol, HD-8)
ExchangeQuirks → SOLO transporte (backward_pagination, requires_end_at, reject_zero_since, origin_fallback_date)
```

- **HD-VC-3 resolvió la frontera semántica** de `VenueConstraints` (A — PRICE/SIZE/NOTIONAL, §15.b) y **HD-VC-4 su temporalidad** (D — §15.c): la colocación de `constraints` dentro de `DefinitionVersion` que muestra el bloque anterior queda **descartada por la decisión** — VenueConstraints posee intervalo efectivo propio e independiente; el lenguaje `[from,to)`/`valid_from/valid_to` de esta sección era PROPUESTA y con HD-VC-4 se concreta como `effective_at`/`effective_to` por `(ListingIdentity, venue)`. Su **provenance/evidence** fue **decidida por HD-VC-5 (B — Evidence explícita, §15.d)**: provenance obligatoria (source · observed_at · recorded_at) + evidencia identificable/referenciable; los **detalles físicos** (tipos, schema, storage, registry, API) siguen siendo **PROPUESTA sujeta a HD-VC-7/D19**.
- **HD-VC-3 ya resolvió la frontera semántica de constraints** (A — PRICE / SIZE / NOTIONAL con ejes precision/increment/limits, §15.b). Tratar un cambio de constraints como `nueva DefinitionVersion` (head) sigue siendo **PROPUESTA NO decidida** (el versionado/inmutabilidad NO lo resuelve HD-VC-3; permanece en D12/HD-VC).
- Cambio de término D05–D08: cuando define una entidad negociable distinta coexistente ⇒ **nueva ListingIdentity + relación** (D12; HD-3). Un cambio administrativo/histórico no crea automáticamente identidad (D08).
- `VenueConstraints` es **dato de entrada** del venue (con forma de FIX 969: capa market-segment), **no** campo de ContractTerms ni de ExchangeQuirks — PROPUESTA.
- No se copia ciegamente: no se adopta la semántica de Hummingbot/Nautilus/Freqtrade al pie de la letra; se toma la **semántica**, se valida contra D01–D15 y se formaliza por ADR.

---

## 12. Alternativas evaluadas (soporte, no decisión)

| Alt. | Modelo de constraints | Ventaja | Riesgo |
|---|---|---|---|
| **A** | Extender `ExchangeQuirks` (B-MD-005 letra) | mínimo movimiento; cabida en tracking | **doble SSOT de `tick_size`** si además se modela Definition; USD God-object del connector |
| **B** | `VenueConstraints` como VO dentro de `DefinitionVersion` (propuesta §11) | separa contrato de dato de venue; PIT `[from,to)`; alineado con FIX layers | requiere ADR nuevo (B-MD-005 o G1 addendum) |
| **C** | Campos sueltos en `ContractTerms` | trivial | contamina el contrato contractual con datos operativos; rompe la semántica D05–D08 |
| **D** | Upsert snapshot estilo Hummingbot/Nautilus | simple | sin PIT → no auditable, choca con la dirección D11 (FAIL-CLOSED) |
| **E** | Event stream FIX completo (A/M/D + bitemporal) | el patrón más completo | sobredimensionado para G1; coste alto |

La alternativa B ofrece una separación semántica explícita entre términos contractuales y constraints de venue dentro del conjunto evaluado. Su adopción queda sometida a **HD-VC-2** y **HD-VC-3**; la alternativa A se conserva únicamente como lectura/transporte del payload crudo (`info`) del venue. Ninguna alternativa queda seleccionada por este documento.

---

## 13. Riesgos y bloqueadores (antes de GATE C)

1. **Doble SSOT de `tick_size`** si se adopta B-MD-005 (alt A) y además se agrega `VenueConstraints` → **HD-7 resuelto (A — APROBAR ACOTADO, §14.g)**: el status de B-MD-005 queda aprobado con condición de alcance que mantiene `ExchangeQuirks` como solo transporte; **HD-VC-2 resuelto (A — VenueConstraints como modelo separado, §15.a)**, lo que evita el doble SSOT del dueño del modelo; **HD-VC-3 resuelto (A — frontera semántica PRICE/SIZE/NOTIONAL, §15.b)**; **HD-VC-4 resuelto (D — temporalidad propia: intervalo efectivo + continuidad/no solapamiento + bitemporalidad diferida, §15.c)**; **HD-VC-5 resuelto (B — evidence explícita: provenance obligatoria + evidencia identificable/referenciable, §15.d)**; **HD-VC-6 resuelto (D — Hybrid: autoridad gobernada + resolución determinista + FAIL-CLOSED residual, §15.e)**; **HD-VC-7 resuelto (A — Domain-owned VenueConstraints: owner semántico en market_data.domain + Registry/Port INFORM + adapter como evidence, §15.f)**; **HD-VC-1 resuelto (A+ — VenueConstraints como hechos de admisibilidad operativa del venue + cláusula de derivación/exclusión semántica: la cláusula fija la frontera de admisión, de modo que un valor ya contractual (`contract_multiplier`) no puede admitirse también como `VenueConstraint`, y un campo cuya semántica describe la entidad negociable no puede entrar en el carril de restricciones; cierra el riesgo de doble SSOT por contaminación desde el payload del venue y la vía de suplir una Definition incompleta por el Registry/Port de constraints; sin inferencia numérica; FAIL-CLOSED de admisión ante clasificación no determinista; §15.g)**.
2. **Contaminación de `ContractTerms`** (alt. C) — descartable por D05–D08.
3. **PIT inexistente** (alt. D) → no auditable; contradice la dirección D11 (provenance) aunque los constraints estén fuera del alcance de D11.
4. **Derivación determinista precision↔tick** — contradicha por Binance (BLZUSDT); OCM debe persistir ambas como datos observados.
5. **Tercer VO `Symbol`** — ownership resuelto por **HD-8 (A — CATÁLOGO FORMAL ACOTADO, §14.h)**: `Symbol` = representación canónica base/quote, fuera de identidad y de ContractTerms; el contrato exacto con `native_symbol` / `underlying/quote` queda para la materialización.
6. `min_notional` del venue vs `min_order_usd` de risk: no unificarlos en el mismo campo.
7. **ADR-0049 no se modifica**; nuevas decisiones van a ADR nuevo (B-MD-005 o G1 addendum); GATE C solo tras autorización humana.

---

## 14. Matriz de decisión — HD-1..HD-8 (G1 pendientes)

> **Decisión humana** vacía por diseño (salvo HD-1, HD-2, HD-3, HD-4, HD-5, HD-6, HD-7 y HD-8, ya marcadas). Las opciones son referencia del análisis; el conocimiento externo NO es norma.

| ID | Pregunta | Opciones analizadas | Evidencia clave | **Decisión humana** |
|---|---|---|---|---|
| **HD-1** | ¿Se separa formalmente Identity de Definition (D12) como contrato de domain? | A. Asumida en G1 (ya materializada) · B. Formalizar más | D12 C/RESTRICTED; FIX layers | **A — separación formal confirmada** (2026-09-29) · justificación registrada (§14.a) |
| **HD-2** | ¿Debe mantenerse la identidad canónica **independiente de native_symbol**, dejando native_symbol exclusivamente en SymbolBinding (capa de representación)? | A. Sí (independencia; native_symbol solo en SymbolBinding) · B. alternativa con mayor acoplamiento | D01/D02/D03/D10/D19; CCXT market; FIGI leaf | **A — separación estricta confirmada** (2026-09-29) · justificación registrada (§14.b) |
| **HD-3** | ¿Guard anti-versionado: término identity-defining (D05–D08) inmutable; cambio ⇒ nueva identity + relación D14? | A. Guard duro (rechazo) · B. transición autorizada | D09 guarda; D12/D14 | **C refinada — regla semántica normativa + catálogo de casos conocidos como materialización operacional + FAIL-CLOSED** (2026-09-29) · justificación registrada (§14.c) |
| **HD-4** | ¿Política de resolución integral: dónde vive la normalización de símbolos (domain policy vs port infrastructure)? | A. domain policy (actual) · B. port | D01/D11 FAIL-CLOSED; MIC | **D — reparto contractizado: DOMAIN decide · APPLICATION orquesta · PORT/REGISTRY informa** (2026-09-29) · justificación registrada (§14.d) |
| **HD-5** | ¿Ventana `[from,to)` de SymbolBinding (D10) como VO propio vs dentro de identity? | A. VO propio · B. integrada | D10 C/RESTRICTED | **A + D — ValidityWindow como VO propio de dominio (semántica D10 + invariantes) con contrato normativo explícito** (2026-09-29) · justificación registrada (§14.e) |
| **HD-6** | ¿D11 provenance + UNKNOWN_CONFLICT FAIL-CLOSED: dónde debe vivir el enforcement? Hoy las invariantes las aplican la entidad y los VOs de dominio (`ListingIdentity.new_definition_version` y `IdentityRelation.__post_init__` rechazan UNKNOWN_CONFLICT); `domain/policies/` contiene la política pura de resolución (0/1/>1). | A. mantener enforcement en el modelo de dominio (estado actual) · B. mover a port/capa de datos | D11 | **DECIDIDO (A/I): P1=A fail-closed ante evidencia conflictiva · P2=B separación conceptual EvidenceSource/EvidenceStatus · P3=C guard central en DOMAIN (identity-resolution policy) · P4=A IdentityConflictError como semántica de error · P5=A observed_at conceptual · P6=A renuncias explícitas** (2026-09-29) · justificación registrada (§14.f) |
| **HD-7** | **B-MD-005** (instrumentos/límites/precisión como datos): ¿APROBAR (con alcance), RECHAZAR o DIFERIR? | A. aprobar acotado · B. rechazar · C. diferir | F-MD-007; §12 alt A/B; doble SSOT | **A — APROBAR ACOTADO** (2026-09-29) · justificación registrada (§14.g) |
| **HD-8** | ¿Composición/ownership: catálogo de venues, autoridad de emisión de Definition, y el tercer VO `Symbol` (base/quote)? | A. catálogo formal · B. mantener `Symbol` separado · C. unificar | D12/D11; FIGI/MIC | **A — CATÁLOGO FORMAL ACOTADO** (2026-09-29, COMPUESTA) · justificación registrada (§14.h) |

### 14.a. Cierre documental — HD-1 (Identity vs Definition)

**Decisión:** HD-1 = **A — separación formal** entre `ListingIdentity` y `DefinitionVersion`.

**Justificación registrada (base normativa):** La separación formal entre `ListingIdentity` y `DefinitionVersion` evita que la identidad estable de una entidad negociable quede acoplada a atributos de su definición que pueden evolucionar, corregirse o adquirir nuevas versiones sin que necesariamente exista una nueva entidad negociable. Esta frontera es coherente con D01, D09 y D12: la identidad debe permanecer estable mientras la definición asociada puede evolucionar de forma explícita y trazable. La separación también permite analizar posteriormente, sin prejuzgar la decisión, qué elementos pertenecen al catálogo de `DefinitionVersion`, cuáles constituyen restricciones específicas del venue, cuáles son únicamente representaciones del instrumento y cuáles son políticas internas de OCM. Por tanto, HD-1 establece la frontera conceptual Identity/Definition, pero deliberadamente **no cierra todavía** el contenido completo de `DefinitionVersion`.

**Lectura formal de la decisión:**

1. **Identidad.** `ListingIdentity` representa una entidad negociable estable dentro del dominio definido por OCM.
2. **Definición.** `DefinitionVersion` describe una versión de la definición asociada a esa identidad.
3. **Evolución.** Una modificación de la definición no implica automáticamente una nueva `ListingIdentity`.
4. **Catálogo no cerrado.** HD-1 NO decide qué campos exactos pertenecen a `DefinitionVersion`. D12 sigue siendo la autoridad y mantiene abiertos: catálogo exhaustivo de Definition, reglas completas de versionado, ventanas temporales, autoridad de validación, provenance, metadatos adicionales y la relación exacta entre Definition y VenueConstraints.
5. **VenueConstraints.** HD-1 NO decide si `VenueConstraints` pertenece dentro de `DefinitionVersion` o si debe existir como estructura/versionado separado — corresponde a HD-VC-1 / HD-VC-2 / HD-VC-3.
6. **SymbolBinding.** HD-1 tampoco convierte `native_symbol` en parte de la identidad; su estatus se decide formalmente en HD-2.

### 14.b. Cierre documental — HD-2 (ListingIdentity vs native_symbol)

**Decisión:** HD-2 = **A — separación estricta**.

**Regla adoptada:** `ListingIdentity` es **independiente de `native_symbol`**. El `native_symbol` pertenece **exclusivamente a `SymbolBinding`** como representación contextual y temporal de una `ListingIdentity`. Un cambio de `native_symbol`, por sí mismo, **NO determina una nueva `ListingIdentity`**.

**Justificación registrada:** OCM debe mantener separadas la identidad de una entidad negociable y su representación mediante símbolos del venue. `native_symbol` es una representación contextual y temporal que puede cambiar, ser corregida, ser reutilizada o presentar aliases sin que esos cambios impliquen necesariamente una modificación de la entidad negociable. Esta separación es coherente con D09, que establece identidad estable y lifecycle; con D10, que materializa `SymbolBinding` como resolución contextual-temporal fail-closed; y con D13/D14, que reservan las relaciones de identidad como `SUCCESSOR_OF` y `REPLACED_BY` para casos en los que existen entidades de identidad distintas y evidencia suficiente. Por tanto, un cambio de símbolo no constituye por sí mismo un cambio de identidad. La existencia de un nuevo símbolo, un rename, un alias o una corrección debe resolverse mediante `SymbolBinding` y su contexto temporal. Cuando exista realmente una nueva entidad negociable, la creación de una nueva `ListingIdentity` y su eventual relación tipada se determinarán conforme a las reglas de identidad ya materializadas.

**Consecuencias registradas:**

1. **Identity.** `ListingIdentity ≠ native_symbol` — la identidad no depende del string del símbolo.
2. **SymbolBinding.** Es el propietario conceptual del `native_symbol` y mantiene su contexto y temporalidad.
3. **Rename.** Un rename administrativo no crea automáticamente una nueva identidad.
4. **Alias.** Dos representaciones distintas pueden corresponder a una misma identidad cuando la evidencia y el binding así lo establezcan.
5. **Reutilización.** El mismo `native_symbol` puede eventualmente aparecer en ventanas temporales diferentes y corresponder a identidades diferentes. Por tanto: *same symbol ≠ same identity* y *different symbol ≠ different identity*.
6. **Relaciones.** Un cambio de símbolo no genera automáticamente `SUCCESSOR_OF` / `REPLACED_BY`. Las relaciones D13/D14 requieren entidades de identidad distintas y la evidencia/provenance correspondiente.
7. **Resolution.** La resolución continúa siendo: 0 candidates → FAIL-CLOSED; 1 candidate → resolve; >1 candidates → FAIL-CLOSED.
8. **Historical replay.** La separación permite reconstruir qué símbolo representaba una identidad en un determinado intervalo temporal.

**Coherencia con decisiones ya materializadas:** HD-2 se presenta como **continuación coherente** de D09 (identidad estable + lifecycle), D10 (`SymbolBinding` contextual-temporal + FAIL-CLOSED), D13 (relaciones tipadas/direccionales) y D14 (`SUCCESSOR_OF` / `REPLACED_BY`), **no como modificación** de ellas; D09/D10/D13/D14 quedan intactas.

**Qué NO decide HD-2** (permanece pendiente de sus respectivos gates): canonical `Symbol` / base / quote; autoridad del canonical symbol; coexistencia exacta de múltiples bindings; restricciones de registro de bindings; política de reutilización de símbolos; política de correcciones históricas; Point-in-Time completo; provenance detallada de administrative renames; `DefinitionVersion`; `VenueConstraints`; precision; limits; `tick_size`; `lot_size`; `min_notional`; B-MD-005; D15; EconomicGroup; Execution Interchangeability.

### 14.c. Cierre documental — HD-3 (Identity-defining terms / identity change guard)

**Decisión:** HD-3 = **C refinada — Regla semántica normativa + catálogo de casos conocidos como materialización operacional + FAIL-CLOSED**. (No es "C" sin la calificación "refinada".)

**Regla principal (autoridad normativa):** una modificación mantiene la misma `ListingIdentity` cuando continúa describiendo la **misma entidad negociable**; cuando la modificación define una entidad negociable **distinta**, debe existir una nueva `ListingIdentity`. La **regla semántica es la autoridad normativa principal**. El catálogo de términos/casos conocidos es una **materialización operacional** del conocimiento actualmente establecido, pero: NO es exhaustivo; NO es inmutable; NO sustituye la regla semántica; NO cierra el catálogo que D12 dejó abierto; NO puede convertir automáticamente todo cambio de término en una nueva identidad.

**Estructura adoptada (tres capas):**

- **Capa A — Regla semántica.** Pregunta normativa principal: ¿el cambio define una entidad negociable distinta? NO → misma `ListingIdentity` → nueva `DefinitionVersion` cuando corresponda. SÍ → nueva `ListingIdentity` → relación D13/D14 **solamente cuando exista** la evidencia/provenance requerida.
- **Capa B — Catálogo conocido.** D05–D08 y las decisiones ya materializadas proporcionan términos/casos conocidos que ayudan a resolver automáticamente situaciones directas. Conserva la semántica ya establecida para: `underlying`; `quote`; `settlement`; `margin_convention` cuando diferencia mercados; `expiry`; `strike`; `option_right`; `contract_multiplier` cuando su valor diferencia la entidad contractual. Estos términos NO constituyen un catálogo exhaustivo de todos los posibles identity-defining terms; **D12 mantiene abierto el catálogo completo**.
- **Capa C — Unknown / Conflict.** Término o modificación no clasificable de manera determinista → **UNKNOWN → FAIL-CLOSED**. Evidencia contradictoria → **CONFLICT → UNKNOWN_CONFLICT → FAIL-CLOSED**. Nunca `UNKNOWN → assume same identity`, ni `UNKNOWN → assume new identity`.

**Justificación normativa (coherencia):**

- **D05–D08:** esos apartados ya identifican términos contractuales relevantes, pero algunos utilizan condiciones ("cuando diferencia"; "cuando su valor diferencia"). HD-3 NO transforma esos términos condicionales en una regla mecánica `any change → new identity`.
- **D09:** la identidad permanece estable mientras siga siendo la misma entidad negociable; el lifecycle no debe confundirse con Definition.
- **D10:** `native_symbol` continúa siendo responsabilidad de `SymbolBinding`; HD-3 NO introduce símbolos como criterio de identidad.
- **D11:** los conflictos deben resolverse mediante gobernanza/provenance y FAIL-CLOSED.
- **D12:** frontera semántica fundamental — *same negotiable entity → versions of Definition; distinct negotiable entity → new ListingIdentity + explicit relation when required* — y D12 deja deliberadamente abierto el catálogo exhaustivo.
- **D13/D14:** una nueva identidad **no implica automáticamente una relación**; `SUCCESSOR_OF` / `REPLACED_BY` requieren la evidencia y provenance correspondientes; un cambio administrativo o de representación no debe generar automáticamente una relación de identidad.

**Consecuencias registradas:**

1. La regla semántica es la autoridad conceptual para determinar cambio de identidad.
2. El catálogo explícito de términos/casos conocidos es una ayuda operacional, no una lista exhaustiva.
3. Los términos conocidos pueden utilizarse para clasificación determinista cuando su semántica ya está establecida.
4. Un término nuevo o ambiguo no puede clasificarse por heurística.
5. Ante incertidumbre de clasificación se aplica FAIL-CLOSED.
6. Ante conflicto de evidencia se aplica `UNKNOWN_CONFLICT` y FAIL-CLOSED.
7. Un cambio de término no implica automáticamente una nueva `ListingIdentity`; debe determinarse si cambia la entidad negociable.
8. Una nueva `DefinitionVersion` no implica automáticamente una nueva `ListingIdentity`.
9. Una nueva `ListingIdentity` tampoco implica automáticamente `SUCCESSOR_OF` o `REPLACED_BY`.
10. `native_symbol` permanece fuera del criterio identity-defining adoptado por HD-2.
11. La resolución de símbolos continúa perteneciendo a `SymbolBinding`/resolution y no debe absorber la clasificación de cambios de identidad.
12. Las Venue Constraints permanecen fuera de la decisión HD-3 y se resolverán mediante HD-VC.
13. La clasificación debe ser auditable mediante evidencia/provenance.
14. Las correcciones históricas y sus efectos temporales permanecen abiertas donde D12/HD-VC todavía no las hayan decidido.

**Matriz conceptual de ejemplos** (registro documental; NO es catálogo exhaustivo; conserva la naturaleza condicional de "puede definir"/"cuando diferencia"):

| Cambio | Tratamiento |
|---|---|
| `native_symbol` cambia | `SymbolBinding`; no determina identidad por sí solo |
| `underlying` cambia | puede definir nueva entidad → nueva `ListingIdentity` |
| `quote` cambia | puede definir nueva entidad → nueva `ListingIdentity` |
| `settlement` cambia | identity-defining cuando diferencia la entidad |
| `margin_convention` cambia | identity-defining cuando diferencia el mercado |
| `expiry` cambia | nueva `ListingIdentity` |
| `strike` cambia | nueva `ListingIdentity` |
| `option_right` cambia | nueva `ListingIdentity` |
| `multiplier` cambia | nueva `ListingIdentity` cuando diferencia la entidad contractual |
| ACTIVE → EXPIRED | lifecycle; no crea identidad nueva |
| `tick_size` cambia | Venue Constraint; fuera de HD-3 |
| `lot_size` cambia | Venue Constraint; fuera de HD-3 |
| `min_notional` cambia | Venue Constraint; fuera de HD-3 |
| price/amount precision cambia | Venue Constraint; fuera de HD-3 |
| cambio de API | ExchangeQuirks; fuera de identity |
| fuente A vs fuente B en conflicto | `UNKNOWN_CONFLICT` → FAIL-CLOSED |

**Guardas registradas:**

- **Guard 1 — Unknown term:** un término no clasificado no puede inventarse como identity-defining ni definition-only → `UNKNOWN → FAIL-CLOSED`.
- **Guard 2 — Conflict:** fuentes o autoridades contradictorias → `UNKNOWN_CONFLICT → FAIL-CLOSED`.
- **Guard 3 — Evidence:** una clasificación identity-defining debe poder rastrearse a evidencia/provenance.
- **Guard 4 — Authority:** no se permite que una heurística de símbolo, string matching o fecha determine por sí sola la identidad.
- **Guard 5 — Symbol independence:** HD-3 no puede reintroducir `native_symbol` como criterio de identidad.
- **Guard 6 — Venue constraint boundary:** `tick_size`, `lot_size`, `min_notional`, precision y límites de ejecución no se incorporan al catálogo identity-defining de HD-3.
- **Guard 7 — Relations:** no inferir automáticamente `SUCCESSOR_OF` / `REPLACED_BY` solo porque se creó una nueva identity.

**B-MD-005 / HD-VC:** HD-3 NO adopta ni rechaza B-MD-005. Su estado continúa siendo **PROPOSED / P1** y su decisión permanece en el flujo HD-VC correspondiente. No se modifica tracking; no se adelanta HD-VC-1..HD-VC-7; no se modifica `exchange_quirks.py`; no se implementan VenueConstraints.

**Fuera de alcance de HD-3 (NO decidido):** catálogo exhaustivo definitivo de `DefinitionVersion`; autoridad definitiva del catálogo; ventanas temporales definitivas de `DefinitionVersion`; registry físico; encoding de `ListingId`; D19; VenueConstraints; B-MD-005; PIT de VenueConstraints; provenance física definitiva de VenueConstraints; Kafka; Iceberg; adapters; portfolio; trading; execution surfaces; economic equivalence; execution interchangeability; D15; implementación concreta de guards; cambios en `symbol.py`; cambios en `symbol_binding.py`; cambios en `identity_resolution.py` salvo que una futura implementación sea expresamente autorizada; cualquier modificación funcional.

**Relación con las decisiones anteriores:**

```
HD-1 = A — Identity ≠ Definition
HD-2 = A — Identity ≠ native_symbol
HD-3 = C refinada — Semantic Rule + Known-Case Catalog + FAIL-CLOSED
```

```
                ListingIdentity
                       │
              "qué entidad es"
                       │
             ┌─────────▼─────────┐
             │ DefinitionVersion │
             │                   │
             │ "cómo se define"  │
             └─────────┬─────────┘
                       │
             classification
                       │
             ┌─────────┴─────────┐
             │                   │
        SAME ENTITY          NEW ENTITY
             │                   │
             ▼                   ▼
       new Definition       new Identity
       Version              + relation if
                            evidence requires
```

Además, por separado: `ListingIdentity → SymbolBinding → native_symbol` (D01/D10/HD-2) y `Definition/Instrument → VenueConstraints → HD-VC`.

**Evidencia externa de apoyo (sin autoridad normativa):** OpenFIGI (identidad estable independiente del ticker); CME (separación de términos contractuales y características de negociación); NautilusTrader (términos del instrumento separados de increments/precision/lotes/límites); CCXT (separación explícita entre precision y limits); Hummingbot (reglas operativas del venue para validación/ejecución); Freqtrade (dificultad de reconstruir históricamente límites y relevancia temporal). Informan la arquitectura; la norma de OCM continúa siendo D01–D15 + decisiones humanas registradas en ADR-0049/Decision Support Pack.

**NO sobreinterpretar C:** no se registra "C es la mejor opción", "C gana" ni ranking A/B/C. La decisión adoptada es una **política híbrida refinada**: la regla semántica constituye la autoridad normativa; el catálogo de casos conocidos constituye una materialización operacional no exhaustiva; y los casos desconocidos o conflictivos se resuelven FAIL-CLOSED. **Esa formulación es la decisión normativa.**

**Estado:** DECIDED + REGISTERED (2026-09-29) · **IMPLEMENTATION = NOT AUTHORIZED**.

---

### 14.d. Cierre documental — HD-4 (Política de resolución integral: reparto contractizado de responsabilidades)

**Decisión humana:** HD-4 = **D — reparto contractizado de responsabilidades** (autorizada 2026-09-29). Decisión **normativa**; NO implica implementación.

**A. DOMAIN — DECIDE.** El dominio posee la autoridad semántica para aplicar la política de resolución de identidad. Comprende: criterio 0/1/>1; la temporalidad necesaria para la resolución; FAIL-CLOSED; UNKNOWN_CONFLICT; y la aplicación de las reglas semánticas de HD-3 (regla semántica normativa + catálogo operacional no exhaustivo + FAIL-CLOSED ante desconocido/conflicto). La policy de dominio debe permanecer **pura y sin I/O**.

**B. PORT / REGISTRY — INFORM.** El Port/Registry proporciona el material necesario para resolver: bindings; candidatos; identidad; evidencia/provenance cuando corresponda. NO decide identidad; NO infiere `SUCCESSOR_OF`; NO infiere `REPLACED_BY`; NO convierte `native_symbol` en identidad; NO sustituye la política de dominio. El hecho de que el Registry pueda realizar una consulta o filtrado temporal **no lo convierte en dueño de la política semántica**.

**C. APPLICATION — ORCHESTRATE.** Application/use case coordina el flujo `material → policy → resultado`. NO redefine las reglas de identidad; NO duplica la política temporal; NO crea heurísticas de identidad; NO convierte símbolos en identidades; NO infiere relaciones D13/D14.

**D. ADAPTER / REPRESENTATION.** La normalización de strings y las particularidades de representación quedan **fuera de HD-4**; su ownership definitivo permanece pendiente de **HD-8**. Esta decisión NO resuelve HD-8.

**E. VENUE CONSTRAINTS.** `tick_size`, `lot_size`, `min_notional`, precision, limits y demás constraints permanecen **fuera de HD-4**; su tratamiento queda sujeto a **HD-VC**. Esta decisión NO resuelve HD-VC.

**Aclaración [from,to) (D10):** la regla semántica de validez temporal pertenece a la **resolución de dominio**; el Registry/Storage puede ejecutar consultas o filtros necesarios para proporcionar candidatos válidos; **consultar/filtrar por tiempo no equivale a decidir identidad**. Esto evita doble fuente de verdad y mantiene las responsabilidades separadas.

**Aclaración A vs D:** HD-4 = D **NO introduce una arquitectura estructural nueva** respecto de la alternativa A. D formaliza normativamente el reparto que A ya materializa: DOMAIN = DECIDE; APPLICATION = ORCHESTRATE; PORT/REGISTRY = INFORM. HD-4 = D es principalmente una **decisión de formalización contractual/gobernanza arquitectónica**.

**Relación con decisiones previas:** coherente con HD-1 = A (`ListingIdentity ≠ DefinitionVersion`); HD-2 = A (`native_symbol ≠ ListingIdentity`); HD-3 = C refinada (regla semántica normativa + catálogo operativo no exhaustivo + FAIL-CLOSED); D09 (identidad estable + lifecycle); D10 (`SymbolBinding` contextual-temporal con `[from,to)`); D11 (provenance + UNKNOWN_CONFLICT); D13/D14 (`SUCCESSOR_OF` / `REPLACED_BY` requieren evidencia y relación explícita; no se infieren por símbolo, fecha, venue o similitud). No se modifica ni se interpreta como adopción de HD-5, HD-6, HD-7, HD-8 o HD-VC.

**NO decidido (permanece pendiente):** HD-5 (ventana `[from,to)` como VO propio); HD-6 (locus del enforcement de provenance); HD-7 (B-MD-005); HD-8 (ownership del tercer VO `Symbol`, catálogo de venues, autoridad de Definition); HD-VC-1..HD-VC-7; B-MD-005; D19; D15; EconomicGroup; Execution Interchangeability; normalización de strings en adapters; ni cualquier modificación funcional.

**Estado:** DECIDED + REGISTERED (2026-09-29) · **IMPLEMENTATION = NOT AUTHORIZED**.

---

### 14.e. Cierre documental — HD-5 (ValidityWindow: ventana temporal `[from,to)` de SymbolBinding como VO propio de dominio + contrato normativo)

**Decisión humana:** HD-5 = **A + D** (autorizada 2026-09-29). Decisión **normativa**; NO implica implementación — no se crea el VO `ValidityWindow`, no se modifica `SymbolBinding`, ni policies, ni ports, ni tests.

**A. ValidityWindow como VO propio de dominio.** La ventana temporal `[from,to)` de `SymbolBinding` (D10) se modelará como un **Value Object propio del dominio** denominado `ValidityWindow`, ubicado en `domain/value_objects/`. `ValidityWindow` encapsula la semántica temporal de D10 (intervalo `[from,to)` semiabierto) y es **responsable de sus invariantes propios** (validación en construcción, Fail-Fast). `SymbolBinding` pasará a **contener** `window: ValidityWindow` (conservándose `provenance`); la policy de resolución delegará la semántica temporal en el VO. La migración de constructores/tests es consecuencia de una materialización futura y **solo cuando una autorización humana adicional lo permita**.

**B. Semántica normada de la ventana.**

| Intervalo | Semántica |
|---|---|
| `from < to` (to finito) | **VÁLIDO** — ventana normal `[from, to)` |
| `from == to` | **INVÁLIDO / FAIL-FAST** (`[t,t)` vacío; ningún contrato de OCM lo exige; comparte observabilidad con corrupción de datos) |
| `from > to` | **INVÁLIDO / FAIL-FAST** (corrupción) |
| `to = None` | **VÁLIDO** — ventana abierta hasta +∞ (D10) |
| `from = None` | **NO SOPORTADO** (D10 exige `valid_from`; no se adoptan semánticas con inicio abierto) |
| `from = None, to = None` | **NO SOPORTADO** (idem) |

La semántica semiabierta `from ≤ t < to` y la open-ended (`to=None` = +∞) permanecen intactas (D10); la resolución continúa FAIL-CLOSED (0/1/>1, nunca selección silenciosa).

**C. Separación conceptual.**

```
ListingIdentity
      ≠
SymbolBinding
      ≠
ValidityWindow
```

La ventana **no forma parte de `ListingIdentity`** (la identidad permanece estable, D09; la temporalidad es un aspecto de la representación del binding, HD-1/HD-2). `ValidityWindow` pertenece al dominio.

**D. Alineación con HD-4 (contrato de responsabilidades).**

- **DOMAIN** → define la semántica y valida invariantes (`ValidityWindow` en `domain/value_objects/`).
- **APPLICATION** → orquesta (transporta el instante de resolución; no interpreta la ventana).
- **PORT / REGISTRY** → informa: proporciona bindings/evidencia; puede consultar/filtrar por tiempo; **consultar/filtrar no equivale a decidir identidad** (HD-4).
- **ADAPTER** → no interpreta identidad ni redefine la temporalidad.

**E. Eje normativo D adoptado.** Las responsabilidades y límites de `ValidityWindow` deben quedar **formalizados explícitamente en el contrato arquitectónico correspondiente**: quién define la semántica; dónde se valida; quién puede consultar; quién no puede reinterpretarla; relación con HD-4; relación con `SymbolBinding`; relación con `ListingIdentity`; qué queda fuera de HD-5. El presente registro constituye la base formal de ese contrato.

**F. Representación temporal.** La representación temporal de `ValidityWindow` utilizará la **convención temporal canónica adoptada por el dominio para esta resolución**: `datetime` **aware UTC** con precisión efectiva de milisegundo, alineada con la convención de ms del ecosistema de market data. No se inventa mediante HD-5 una política temporal global para todo OCM; lo decidido queda **limitado a lo necesario para implementar correctamente la semántica de `ValidityWindow`**.

**G. Alcance normativo de HD-5 (LIMITADO).** `SymbolBinding` · D10 · temporalidad de resolución · invariantes de `ValidityWindow`.

**H. HD-5 NO decide:** temporalidad de `DefinitionVersion`; temporalidad de `VenueConstraints`; HD-VC-4; catálogo de `DefinitionVersion`; provenance futura de `DefinitionVersion`; persistencia; schemas físicos; wire format; D19; B-MD-005; normalización de símbolos; ni cualquier decisión posterior de G1. Reserva explícita: `ValidityWindow` **podría reutilizarse** en el futuro (p. ej. PIT de constraints), pero **no se decide** que ninguna entidad (`DefinitionVersion`, `VenueConstraints`) deba utilizarlo — eso permanece en D12/HD-VC.

**Relación con decisiones previas:** coherente con HD-1 = A (Identity ≠ Definition); HD-2 = A (`native_symbol ≠ ListingIdentity`, representación en `SymbolBinding`); HD-3 = C refinada (regla semántica + catálogo + FAIL-CLOSED); HD-4 = D (reparto contractizado DOMAIN/APPLICATION/PORT·REGISTRY); D09 (identidad estable); D10 (`SymbolBinding` contextual-temporal `[from,to)`); D11 (provenance + UNKNOWN_CONFLICT); D13/D14 (relaciones explícitas, no inferidas). No modifica ninguna decisión anterior y no se interpreta como adopción de HD-6, HD-7, HD-8 ni HD-VC.

**NO decidido (permanece pendiente):** HD-6 (locus del enforcement de provenance); HD-7 (B-MD-005); HD-8 (ownership del tercer VO `Symbol`, catálogo de venues, autoridad de Definition); HD-VC-1..HD-VC-7; D12 (detalles de `DefinitionVersion`); D19; B-MD-005; y cualquier modificación funcional.

**Justificación técnica soporte (no vinculante, de la revisión previa):** la semántica temporal es constitutiva de `SymbolBinding` (D10, C/RESTRICTED), no incidental; el invariante `from ≤ to` merece vivienda formal y Fail-Fast (una ventana invertida/vacía produciría 0 candidatos activos y enmascararía corrupción como ausencia legítima); el resto de VOs de dominio ya valida en construcción (`__post_init__`); la ventana de refactor es barata hoy (GATE B, sin persistencia) y más cara después; el eje D formaliza el reparto sin cambiar la arquitectura (patrón HD-4). La alternativa B (integrada) habría preservado la superficie actual con el invariante en `SymbolBinding.__post_init__`, pero sin tipo de dominio reutilizable — no fue la decisión adoptada.

**Estado:** DECIDED + REGISTERED (2026-09-29) · **IMPLEMENTATION = NOT AUTHORIZED**.

---

### 14.f. Cierre documental — HD-6 (D11 provenance + UNKNOWN_CONFLICT FAIL-CLOSED: naturaleza del conflicto, guard central de dominio y semántica de evidencia)

**Decisión humana:** HD-6 = **A/I** (autorizada 2026-09-29). Decisión **normativa**; NO implica implementación — no se modifica `SymbolBinding`, `identity_resolution.py`, `provenance.py`, excepciones, policies, ports, tests ni schemas.

**A. P1 = A — Fail-closed ante evidencia conflictiva.** Un `SymbolBinding` con evidencia conflictiva **no puede producir una identidad resuelta**, aunque sea el único candidato activo. Se registra explícitamente: *1 candidato + evidencia conflictiva ≠ 1 candidato + evidencia válida*; el primero queda **FAIL-CLOSED**. La resolución no debe elegir una fuente ni inferir un mapping en un hecho disputado (D11:294-297; D11:320-323). Decisión normativa; la implementación del guard no forma parte de esta tarea.

**B. P2 = B — Separación conceptual EvidenceSource / EvidenceStatus.** HD-6 adopta conceptualmente la separación entre el **origen** de la evidencia (`EvidenceSource`: OBSERVED_FROM_VENUE | ADMINISTRATIVE_OCM | DERIVED) y su **estado** (`EvidenceStatus`: p. ej. CONFLICT), de modo que conceptualmente pueda representarse `source = OBSERVED_FROM_VENUE; status = CONFLICT`, y **no** `UNKNOWN_CONFLICT` como si CONFLICT fuera un origen. Es un refinamiento conceptual de la taxonomy de D11:316-318. **No** se rediseñan las clases físicas, **no** se modifica `provenance.py`, **no** se crean enums nuevos.

**C. P3 = C — DOMAIN como guard central.** La política de identidad/evidencia que determina si un candidato puede atravesar la resolución pertenece al **DOMAIN / identity-resolution policy**. Se conserva HD-4: **DOMAIN = DECIDE · APPLICATION = ORCHESTRATE · PORT/REGISTRY = INFORM · ADAPTER = NO interpretación de identidad**. El Port/Registry no decide identidad; la Application no redefine la política semántica; el Adapter no interpreta conflictos de identidad. NO implementar todavía el guard.

**D. P4 = A — Semántica de error.** `IdentityConflictError` representa **explícitamente** la condición de conflicto de identidad/evidencia. HD-6 busca evitar que una condición semántica de dominio quede representada simplemente como `ValueError` genérico. NO se modifican excepciones todavía; NO se reemplazan los `ValueError` actuales; la implementación queda para una fase posterior autorizada.

**E. P5 = A — observed_at conceptual.** `observed_at` = momento en que la evidencia fue observada; `recorded_at` = momento en que OCM registró la evidencia. Se mantienen separados de `ValidityWindow` / `valid_from` / `valid_to`: **ValidityWindow** → cuándo el binding es válido; **observed_at** → cuándo se observó la evidencia; **recorded_at** → cuándo OCM registró la evidencia. Esta decisión es conceptual: sin schema físico, sin modificar `provenance.py`, sin imponer todavía una política global de timestamps para OCM, y **sin cerrar `effective_at` como decisión global**.

**F. P6 = A — Alcance y renuncias explícitas.** HD-6 **NO decide**: jerarquía de autoridad entre fuentes; prioridad temporal; "último dato gana"; correction; retraction; supersession; versionado físico; ledger; persistencia física de provenance; schema físico de provenance; provenance de DefinitionVersion; ventanas temporales de DefinitionVersion; VenueConstraints; B-MD-005; normalización de símbolos; D19. Estos temas permanecen en **D12**, **HD-7 / HD-8**, **HD-VC** y gates posteriores según corresponda. HD-6 no establece `VENUE > OCM`, `OCM > VENUE`, `DERIVED > VENUE` ni `VENUE > DERIVED`, ni "newest evidence wins" / "latest timestamp wins" — D11 no autoriza esas reglas.

**G. Retención de conflicto (obligación conceptual D11).** **Decidido ahora:** retención *conceptual* de la evidencia conflictiva: `CONFLICT → evidencia conservada → FAIL-CLOSED` (D11:295-297). **NO decidido ahora:** cómo se persiste físicamente; cómo se versiona; cómo se corrige; cómo se retracta; cómo se implementa un ledger; cómo se almacena el "otro lado" de la evidencia. Esos aspectos quedan fuera de HD-6.

**H. Relación con HD-4 y HD-5.** HD-6 **no contradice** HD-4: Domain = decisión semántica · Application = orquestación · Port = información · Adapter = sin interpretación de identidad. HD-5 **sigue gobernando** `ValidityWindow`/`[from,to)`; HD-6 no modifica la semántica temporal de la ventana; únicamente añade conceptualmente otra dimensión (`observed_at`) sin alterar la validez.

**I. Relación con D12 / D13 / D14.** HD-6 **NO cierra** las preguntas abiertas de D12 relativas a `DefinitionVersion` (catálogo, ventanas temporales, provenance de DefinitionVersion, materialización física). Se mantienen D13/D14: relaciones tipadas; direccionalidad; provenance obligatoria; **no** inferencia por símbolo, fecha, venue ni lifecycle; **conflicto → FAIL-CLOSED**. No se introducen nuevas relaciones; no se cambia `SUCCESSOR_OF` ni `REPLACED_BY`.

**Antecedente (NO autoridad de la decisión):** la recomendación técnica previa del agente (P1=a, P2=b, P3=c, P4=a, P5=a, P6=a) fue insumo del análisis; la decisión registrada es **decisión humana** (2026-09-29) y se registra como tal, no como recomendación del agente.

**Estado:** DECIDED + REGISTERED (2026-09-29) · **IMPLEMENTATION = NOT AUTHORIZED**.

---

### 14.g. Cierre documental — HD-7 (B-MD-005: instrumentos/límites/precisión como datos — status de la propuesta)

**Decisión humana:** HD-7 = **A — APROBAR ACOTADO** (autorizada 2026-09-29). Decisión **normativa**; NO implica implementación — no se modifica `exchange_quirks.py`, `ccxt_adapter.py`, `provenance.py`, policies, ports, tests, schemas ni `tracking.yaml`; no se crea `VenueConstraints` ni ningún VO de constraints.

**Decisión adoptada:** se aprueba, con alcance acotado, la necesidad expresada por B-MD-005 (F-MD-007): exponer la precisión/límites/tick de venue como **datos observados del venue**. No constituye adopción del modelo físico de constraints ni determina su dueño.

**Condiciones vinculantes (parte integrante de la decisión):**

1. `ExchangeQuirks` permanece **conceptualmente limitado a quirks de transporte/API/conector** (backward_pagination, requires_end_at, reject_zero_since, origin_fallback_date).
2. Las **restricciones propias de la negociación en venue** (`tick_size`, `lot_size`, `min_notional`, precision, etc.) **no deben quedar absorbidas dentro de `ExchangeQuirks`**.
3. La **decisión de propiedad/modelado físico** de esas restricciones queda para **HD-VC-2 / HD-VC-3 / HD-VC-4 / HD-VC-5**, según corresponda.
4. La semántica de **datos observados** debe conservar su **trazabilidad/provenance** (la provenance de constraints permanece en **HD-VC-5**).
5. **HD-7 NO cierra** las decisiones pendientes de **HD-VC**.
6. **HD-7 NO modifica** **D12, D13 ni D14**.
7. **HD-7 NO autoriza ninguna implementación**.

**Alcance de lo aprobado:** el reconocimiento formal del gap F-MD-007 (P1) y de la exposición de precisión/límites/tick como datos observados del venue, bajo las condiciones anteriores. Lo aprobado **NO decide**: modelo físico ni schemas; dueño del modelo (HD-VC-2); versionado (HD-VC-3); ventana temporal de constraints (HD-VC-4); provenance (HD-VC-5); consumidores/enforcement (HD-VC-7 / B-MD-004); ni el contenido de `DefinitionVersion` (D12).

**HD-VC permanece PENDING:** HD-VC-1 · HD-VC-2 · HD-VC-3 · HD-VC-4 · HD-VC-5 · HD-VC-6 · HD-VC-7. Ninguna decisión HD-VC queda marcada como decidida.

**Relación con decisiones previas:** coherente con HD-1 = A (Identity ≠ Definition); HD-2 = A (`native_symbol ≠ ListingIdentity`); HD-3 = C refinada (guard 6: `tick_size`, `lot_size`, `min_notional`, precision y límites de ejecución fuera del catálogo identity-defining y de `ContractTerms`); HD-4 = D (reparto contractizado: ADAPTER obtiene el dato · DOMAIN decide su semántica · PORT/REGISTRY informa); HD-5 = A+D (`ValidityWindow` no comprometida; la temporalidad de constraints no queda decidida); HD-6 = A/I (renuncias P6: sin jerarquía de autoridad; sin cierre de D12/HD-VC). No modifica ninguna decisión anterior y no se interpreta como adopción de HD-8 ni HD-VC.

**Antecedente (NO autoridad de la decisión):** la recomendación técnica previa del agente (Alternativa A — APROBAR acotado) fue únicamente el antecedente técnico que permitió evaluar las alternativas; la decisión registrada es **decisión humana** (2026-09-29) y se registra como tal, no como recomendación del agente.

**NO decidido (permanece pendiente):** HD-VC-1, HD-VC-3..HD-VC-7; D12 (catálogo exhaustivo, ventanas temporales, provenance y materialización de `DefinitionVersion`); D19; B-MD-005 fuera del status ahora resuelto (modelo físico/implementación); `tracking.yaml`; ADR nuevo (B-MD-005 o G1 addendum); y cualquier modificación funcional.

**Estado:** DECIDED + REGISTERED (2026-09-29) · **IMPLEMENTATION = NOT AUTHORIZED**.

### 14.h. Cierre documental — HD-8 (Composición/ownership: catálogo de venues, autoridad de emisión de Definition y el tercer VO `Symbol`)

**Decisión humana:** HD-8 = **A — CATÁLOGO FORMAL ACOTADO** (autorizada 2026-09-29). Decisión **compuesta y normativa**; NO autoriza implementación.

**Decisión adoptada (aceptada explícitamente):**

1. Se acepta un **catálogo/identificador gobernado de venues** (SSOT conceptual de `venue`/`context`); el catálogo físico no se implementa en este gate.
2. `Symbol` = **representación canónica base/quote**.
3. `Symbol` **fuera de `ListingIdentity`**.
4. `Symbol` **fuera de `ContractTerms`**.
5. `native_symbol` = **representación específica del venue** (`SymbolBinding`, D10).
6. `ExchangeQuirks` **limitado a transporte/API/conector** (HD-7 condiciones 1-2).
7. **D12 permanece abierto.**
8. **HD-VC-1..7 permanecen abiertos.**
9. El **contrato exacto `Symbol ↔ native_symbol ↔ underlying/quote`** queda para la fase de **materialización** correspondiente — NO decidido por HD-8.
10. **NO se autoriza implementación funcional** derivada de HD-8.

**Invariantes preservadas (verificación READ-ONLY del registro):**

- **Identity:** `Symbol ≠ ListingIdentity` · `native_symbol ≠ ListingIdentity` · `Venue ≠ ListingIdentity` (coherente con HD-1 = A, HD-2 = A). Ninguna alternativa aprobada introduce `Symbol` dentro de `ListingIdentity`.
- **ContractTerms:** `Symbol` NO está dentro de `ContractTerms`; `native_symbol` NO está dentro de `ContractTerms`; la representación NO se trata como término identity-defining (coherente con HD-3 guard 6 y D05–D08).
- **Representación:** `Symbol` = representación canónica base/quote; `native_symbol` = representación específica del venue (D10).
- **Venue:** existe conceptualmente un **identificador/catálogo gobernado de venue**; no se implementa el catálogo físico todavía.
- **ExchangeQuirks:** sigue limitado a transporte (backward_pagination, requires_end_at, reject_zero_since, origin_fallback_date); NO es owner de identidad, `Symbol`, `ContractTerms` ni VenueConstraints.
- **Temporalidad:** HD-5 (`ValidityWindow`) intacto.
- **Provenance:** HD-6 (P1..P6) intacto; sin jerarquía de fuentes.
- **Relations:** HD-8 no introduce relaciones D13/D14 nuevas (SUCCESSOR_OF/REPLACED_BY sin cambios; SYMBOL_CHANGED_TO en SymbolBinding).
- **D12 abierto (NO se cierra):** catálogo exhaustivo identity-defining; temporalidad de DefinitionVersion; validación de ventanas; autoridad; provenance; schemas; materialización; registry; D19; D20-D24.
- **Venue Constraints:** HD-8 = A **NO** equivale a decidir VenueConstraints; **HD-VC-2 decidido posteriormente** (A — VenueConstraints como modelo separado, §15.a); HD-VC-1, HD-VC-3..7 permanecen **PENDING**.

**Límites semánticos del alcance aprobado:** la decisión fija composición/ownership — venue gobernado (conceptual) y rol de `Symbol` como representación canónica, fuera de identidad y de contrato. La «autoridad de emisión de Definition» se interpreta bajo la lectura mínima (clasificación por taxonomía D11 sin ranking), sin cerrar la autoridad de validación de ventanas de D12.

**HD-8 NO decide:** el contrato físico `Symbol ↔ native_symbol ↔ underlying/quote` (fase de materialización posterior); catálogo físico de venues; VenueConstraints (`tick_size`, `lot_size`, `min_notional`, `price_precision`, `amount_precision`, límites de cantidad/precio/notional); temporalidad y ownership físico de VenueConstraints; D19; **D15 = LATER / SEPARATE FUTURE DECISION**; nuevas relaciones D13/D14; `tracking.yaml`; ADR nuevo; implementación funcional.

**Relación con decisiones previas:** coherente con HD-1 = A (Identity ≠ Definition); HD-2 = A (`native_symbol` en `SymbolBinding`); HD-3 = C refinada (guard 6: representación fuera del catálogo identity-defining y de `ContractTerms`); HD-4 = D (reparto contractizado; normalización fuera, como ownership conceptual de representación); HD-5 = A+D (`ValidityWindow` intacto); HD-6 = A/I (sin jerarquía de fuentes); HD-7 = A (APROBAR ACOTADO — `ExchangeQuirks` solo transporte). D13/D14 sin nuevas relaciones. No modifica ninguna decisión anterior.

**Antecedente (NO autoridad de la decisión):** la recomendación técnica previa del agente (Alternativa A — Catálogo formal acotado) fue únicamente el antecedente técnico que permitió evaluar las alternativas; la decisión registrada es **decisión humana** (2026-09-29), compuesta, y se registra como tal, no como recomendación del agente.

**NO decidido (permanece pendiente):** HD-VC-1, HD-VC-3..HD-VC-7; D12 (catálogo exhaustivo, ventanas temporales, autoridad de validación, provenance, materialización, registry); D19; D15; contrato de materialización de representación; `tracking.yaml`; ADR nuevo (B-MD-005 o G1 addendum); y cualquier modificación funcional.

**Estado:** DECIDED + REGISTERED (2026-09-29) · **IMPLEMENTATION = NOT AUTHORIZED**.

---

## 15. Matriz de decisión — HD-VC-1..HD-VC-7 (venue constraints)

> Misma regla: formulario sin seleccionar; solo el humano decide (salvo **HD-VC-2, HD-VC-3, HD-VC-4, HD-VC-5, HD-VC-6 y HD-VC-7**, ya marcadas, justificación en §15.a / §15.b / §15.c / §15.d / §15.e / §15.f).

| ID | Pregunta | Opciones analizadas | Evidencia clave | **Decisión humana** |
|---|---|---|---|---|
| **HD-VC-1** | ¿Naturaleza: constraints = datos por símbolo×venue, **nunca identidad**? | A. Sí (dato de venue) · B. tratarlos como contrato | Binance filters; CME; Nautilus | **A+ — VenueConstraints como hechos de admisibilidad operativa del venue + cláusula de derivación/exclusión semántica** (2026-09-30) · justificación registrada (§15.g) · **A+ = A + cláusula**: A declara la naturaleza (hechos de admisibilidad operativa gobernados por el venue; scope `(ListingIdentity, venue)`; versión temporal propia HD-VC-4; evidence obligatoria HD-VC-5; resolución determinista + FAIL-CLOSED HD-VC-6; ownership domain-owned HD-VC-7; NO Identity / NO DefinitionVersion / NO Representation / NO ExchangeQuirks / NO RiskPolicy) y la cláusula de derivación se añade como **regla de exclusión y enrutamiento de la clasificación semántica — NO como derivación numérica ni inferencia de valores** (no existe un «VenueConstraint derivado de una fuente semántica gobernada» como mecanismo de inferencia); admisión directa por evidencia (tickSize → PRICE increment; stepSize/size increment → SIZE increment; minNotional → NOTIONAL limit) conservando source/provenance, evidence, `observed_at`, `recorded_at`, aplicabilidad efectiva HD-VC-4 y trazabilidad de auditoría; enrutamiento explícito cuando la semántica pertenece a otra frontera (contractSize/multiplier → **NO** se incorpora automáticamente, permanece bajo D08/D12/D13; post_only/reduce_only → OrderPolicy/ExecutionPolicy; native_symbol → Representation/SymbolBinding; min_order_usd → RiskPolicy); prohibición de inferencia numérica (no `tick_size = f(precision)`, no `increment = 10^-precision`, no `min_notional = min_order_usd`, no `contractSize` desde multiplier, ni fórmulas no gobernadas) preservando `precision ≠ increment ≠ limits`; prohibición de segundo SSOT (un valor no puede ser término de ContractTerms y VenueConstraint a la vez; el carril de restricciones no puede suplir una Definition incompleta); conflicto interno del carril (p. ej. tickSize 0.01 vs 0.001 en el mismo `(ListingIdentity, venue, T)`) → `UNRESOLVED_CONFLICT`, evidencia preservada, resolución conforme a HD-VC-6, FAIL-CLOSED si no hay resolución gobernada/explícita/determinista, sin source-wins/API-wins/latest-wins/observed-wins; discrepancia con otro carril (venue `contractSize = 100` vs `ContractTerms.contract_multiplier = 10`) → NO se convierte en conflicto de VenueConstraints, se conserva evidencia (HD-VC-5/6) y se remite a D08/D12/D13; clasificación no determinista → NO se produce VenueConstraint canónico, NO se inventa valor, NO se usa «unknown constraint», NO se habilita vía alternativa por inferencia, evidencia conservada y remisión al carril correspondiente, sin redefinir HD-VC-6 ni crear un `UNRESOLVED_*` nuevo; preserva HD-1..HD-8 y HD-VC-2..HD-VC-7; NO decide D08/D12/D13/D19 ni materialización física; sin especificación física; sin implementación) |
| **HD-VC-2** | ¿Owner del modelo: `VenueConstraints` (novación al §11/alt B) vs extender `ExchangeQuirks` (B-MD-005, alt A)? | A. VenueConstraints · B. ExchangeQuirks · C. híbrido capa listing | FIX MarketSegmentGrp/969; doble SSOT | **A — VenueConstraints como modelo separado** (2026-09-29) · justificación registrada (§15.a) |
| **HD-VC-3** | ¿Frontera semántica de `VenueConstraints`: qué conceptos pertenecen (**PRICE / SIZE / NOTIONAL** con ejes **precision / increment / limits**)? — el versionado/inmutabilidad (cambio de constraint ⇒ nueva DefinitionVersion) **NO se resuelve en esta casilla** (§11 sigue PROPUESTA, en D12/HD-VC) | A. frontera semántica por familias funcionales · B. submodelos Price/Size/NotionalConstraints (materialización futura) · C. extensible catch-all · D. separación adicional | NautilusTrader (precision+increment separados y requeridos); CCXT (precision≠limits, precisionMode); FIX (SecurityDefinition / trade rules) | **A — frontera semántica PRICE / SIZE / NOTIONAL** (2026-09-29) · justificación registrada (§15.b) · B = refinamiento de materialización para HD-VC-7, NO implementación |
| **HD-VC-4** | ¿Semántica temporal de `VenueConstraints`: cómo representar cambios de reglas del venue (`tick_size`, `price_increment`, `size_increment`, `lot_size`, `min_notional`) para PIT correcto sin confundirlos con identity/Definition? | A. dentro de DefinitionVersion (cambio ⇒ nueva DefinitionVersion) · B. temporalidad/versionado propios · C. bitemporalidad explícita ahora · D. B + invariante de continuidad/no solapamiento + bitemporalidad diferida | Binance anuncios con fecha efectiva (PIT real); CME/FIX MarketSegment trading rules; NautilusTrader upsert destructivo (anti-patrón PIT) | **D — temporalidad propia: B + invariante de continuidad/no solapamiento + bitemporalidad diferida** (2026-09-30) · justificación registrada (§15.c) · constraint operativa ⇒ nueva versión temporal, NO DefinitionVersion |
| **HD-VC-5** | ¿Qué provenance/evidence debe acompañar una VenueConstraint para auditar y reconstruir su fundamento, sin mezclarla con identity/Definition/validez efectiva/política de conflicto? | A. provenance mínima (source · observed_at · recorded_at) · B. evidence explícita (A + evidence_id/ref/hash/status) · C. + lifecycle (correction/supersession/retraction como acciones) · D. bitemporalidad completa (FIX event stream) | FIX SecurityDefinition (SecurityReportID + SecurityUpdateAction); Binance exchangeInfo snapshot current-only + anuncios con fecha efectiva; Nautilus `info` (sin provenance); CCXT sin historial | **B — Evidence explícita** (2026-09-30) · justificación registrada (§15.d) · decisión humana (coincide con la recomendación técnica, pero la decisión es humana; ver §15.d) · Provenance ≠ Evidence · conflictos → HD-VC-6 |
| **HD-VC-6** | ¿Cómo se resuelve —o se niega a resolver— OCM una VenueConstraint cuando la evidencia no permite establecer una única verdad operativa (conflicto, gaps, overlaps, ausencia, autoridad ambigua, evidencia insuficiente, corrección/supersession/retraction, FAIL-CLOSED)? | A. jerarquía de autoridad explícita · B. resolución determinista de evidencia · C. FAIL-CLOSED ante todo conflicto no resuelto · D. Hybrid (autoridad + resolución determinista + FAIL-CLOSED residual) · E. otros | FIX MDUpdateAction/SecurityUpdateAction; bitemporalidad valid/transaction time; MDM survivorship rules gobernadas; Hummingbot #6230 (ausencia→crash); CCXT/Binance current-only; BLZUSDT | **D — Hybrid** (2026-09-30) · justificación registrada (§15.e) · autoridad explícita/contextual/granular/determinista sin jerarquía física (materialización posterior); secuencia aplicabilidad temporal → autoridad → resolución determinista → no-resolución FAIL-CLOSED; UNRESOLVED_* conceptual · sin física ni consumidores (HD-VC-7) |
| **HD-VC-7** | ¿Quién es el **owner semántico** de VenueConstraints y su frontera owner↔consumers (responsabilidad de resolución, dirección de dependencias, responsabilidad del adapter, existencia conceptual de un Registry/Port, y quién consume: trading/execution/risk/research)? | A. Domain-owned (market_data: domain DECIDE, application ORCHESTRATE, port/registry INFORM, adapter TRANSLATE/PROVIDE EVIDENCE; consumers por port sin ser owners) · B. Domain-owned pero con catálogo materializado como owner de la verdad · C. Trading/Execution-owned (trading rules como reglas de trading) · D. BC dedicado de instrument/market metadata · E. otro | NautilusTrader (Instrument = especificación completa de trading; sin PIT) · CCXT (precision≠limits; current-only, sin historial) · FIX SecurityDefinition/SecurityTradingRules (reference data de trading rules versionada, SecurityUpdateAction, override segment→instrument) · Hummingbot (reglas por connector; ausencia de regla → fallo no gobernado) · MDM/golden record | **A — Domain-owned VenueConstraints** (2026-09-30) · justificación registrada (§15.f) · ownership semántico en market_data.domain; resolución en DOMAIN aplicando HD-VC-4/5/6; Registry/Port outbound conceptual INFORM (no decide, no es segundo policy engine, no SSOT físico); adapter = TRANSLATE/PROVIDE EVIDENCE (nunca verdad canónica); trading/execution/risk/research = CONSUME por port; materialización física, schema, tablas, package layout e identificadores NO cerrados (D19 + decisiones posteriores); multiplier bajo D08/D12 (no se incorpora automáticamente); sin implementación |

### 15.a. Cierre documental — HD-VC-2 (Ownership del modelo: VenueConstraints vs extensión de ExchangeQuirks)

**Decisión humana:** HD-VC-2 = **A — VenueConstraints como modelo conceptual separado de ExchangeQuirks** (autorizada 2026-09-29). Decisión **conceptual/arquitectónica**; NO autoriza implementación.

**Decisión adoptada (aceptada explícitamente):**

- `VenueConstraints` se tratará conceptualmente como un **modelo separado de `ExchangeQuirks`**.
- `ExchangeQuirks` permanece **limitado a comportamiento de transporte/API/connector** (backward_pagination, requires_end_at, reject_zero_since, origin_fallback_date; HD-7 condiciones 1-2).
- `VenueConstraints` representa **restricciones operativas observables gobernadas por el venue** (PRICE: precision/increment/min-max; SIZE: precision/increment/lot/min-max quantity; NOTIONAL: min/max), como datos observados — no como contrato.
- El modelo separado **evita doble SSOT** de campos como `tick_size` (riesgo del §13.1).
- Permite que **risk/execution consuman el concepto sin depender semánticamente de infraestructura/connector** (HD-4: DOMAIN decide semántica; los adapters no reinterpretan identidad ni son SSOT de semántica).
- Mantiene la **separación semántica** entre: **Identity · Definition · Representation · Venue Execution Constraints · Exchange Connector Quirks · Risk Policy**.

**Fronteras preservadas (verificación READ-ONLY del registro):**

- **`min_notional` (venue) ≠ `min_order_usd` (risk).** Evidencia: `trading/risk/models.py` (`OrderLimits`) es política de riesgo sobre capital aplicada en aperturas por `RiskManager`; `min_notional` es restricción del venue sobre el tamaño nominal de la orden. VenueConstraints ≠ Risk Policy.
- **`multiplier` NO se convierte automáticamente en VenueConstraint.** Permanece sujeto a **D08/D12**: identity-defining cuando diferencia entidades negociables coexistentes (D08); término contractual del mismo listing → Definition (D12). D08 no se re-decide; tampoco se decide aquí cómo representarlo físicamente.
- **HD-7 intacto:** `ExchangeQuirks` = solo transporte/API.
- **HD-8 intacto:** `Symbol` = representación canónica, fuera de identidad y de ContractTerms; catálogo de venues gobernado (conceptual).
- **HD-4 intacto:** DOMAIN decide semántica; los adapters observan/cargan metadatos sin reinterpretar identidad.
- **ContractTerms (D05–D08) intacto:** ninguna restricción de ejecución entra en ContractTerms como término identity-defining.

**B-MD-005 (antecedente, NO adoptado):** resolvía F-MD-007 (ejecución sin `precision`/`min_amount`/`tick` como contrato de dominio; `load_markets()` sin exponer) proponiendo extender `ExchangeQuirks`. Tras HD-7 (condición 1) y HD-VC-2, su **problema sigue válido** pero su **vehículo (extender `ExchangeQuirks`) queda descartado**. Se referencia como antecedente; **NO se adopta**, ni se implementan sus campos, ni se extiende `ExchangeQuirks`, ni se crea `VenueConstraints` en esta tarea.

**NO decidido (permanece pendiente):** HD-VC-1 · HD-VC-3 · **HD-VC-4 (temporalidad/PIT — este gate NO registra «Sí» ni «No» sobre PIT; la temporalidad permanece PENDING para HD-VC-4)** · HD-VC-5 (provenance materializada) · HD-VC-6 (FAIL-CLOSED/resolución) · HD-VC-7 (consumidores); D12 (catálogo exhaustivo identity-defining, ventanas temporales de DefinitionVersion, autoridad de validación, provenance, materialización, registry); D19; autoridad de validación; registry físico; schema físico; persistencia; wire format; contrato `Symbol ↔ native_symbol ↔ underlying/quote`; implementación de B-MD-005; y cualquier modificación funcional. **NO se agrega especificación física** de clases, campos, schema, registry ni persistencia.

**Antecedente (NO autoridad de la decisión):** la revisión técnica previa del agente (Alternativa A — VenueConstraints como modelo separado, con matriz y evidencia) fue únicamente antecedente técnico; la decisión registrada es **decisión humana** (2026-09-29) y se registra como tal, no como recomendación del agente.

**Estado:** DECIDED + REGISTERED (2026-09-29) · **IMPLEMENTATION = NOT AUTHORIZED**.

### 15.b. Cierre documental — HD-VC-3 (Frontera semántica de VenueConstraints: PRICE / SIZE / NOTIONAL)

**Decisión humana:** HD-VC-3 = **A — frontera semántica PRICE / SIZE / NOTIONAL** (autorizada 2026-09-29). Decisión **SEMÁNTICA** (qué conceptos pertenecen a `VenueConstraints`), NO una decisión de implementación física; NO autoriza implementación.

**Decisión adoptada (aceptada explícitamente):**

`VenueConstraints` tiene como frontera semántica las **familias funcionales PRICE / SIZE / NOTIONAL**, distinguiendo explícitamente los ejes **precision, increment y limits** cuando sean aplicables.

**Definición normativa (frontera conceptual):**

```
VenueConstraints
├── PRICE
│   ├── precision
│   ├── increment
│   └── limits
├── SIZE
│   ├── precision
│   ├── increment
│   └── limits
└── NOTIONAL
    └── limits
```

Esta estructura representa la **frontera conceptual**; no obliga a que la implementación futura utilice exactamente esta estructura de clases.

**Relación A — B (registrada explícitamente):**

- **A = frontera semántica.** Determina «¿qué conceptos pertenecen a VenueConstraints?» → **PRICE / SIZE / NOTIONAL** con separación entre **precision / increment / limits**.
- **B = materialización/organización futura.** `PriceConstraints` / `SizeConstraints` / `NotionalConstraints` puede utilizarse posteriormente como **organización interna** de `VenueConstraints`.
- **B NO constituye una frontera semántica diferente.** Por tanto, **A y B NO se registran como alternativas conceptualmente incompatibles**: A es la decisión de frontera; B queda disponible como refinamiento de materialización para **HD-VC-7**.
- **NO implementar B ahora.**

**Precision / Increment / Limits (registro normativo):**

- `precision ≠ increment` · `precision ≠ limits` · `increment ≠ limits`.
- **No** se asume `increment = 10^-precision`, ni que un tick size pueda derivarse necesariamente de una precisión decimal, ni que límites mínimos/máximos puedan derivarse de precision o increment (evidencia: Binance BLZUSDT precision vs tickSize; CCXT `precisionMode`; Nautilus trata precision e increment como invariantes separados).
- La futura materialización deberá conservar estos conceptos como **datos semánticamente diferenciados**.

**PRICE (conceptual, no exhaustivo):** `price_precision` · `price_increment` / `tick_size` · `min_price` · `max_price`. No asumir que todos están siempre presentes; no inventar valores. **NO decidir aquí:** price bands dinámicos (HD-VC-6); temporalidad/PIT (HD-VC-4); provenance (HD-VC-5); resolución de conflictos y autoridad entre fuentes (HD-VC-6).

**SIZE (conceptual, no exhaustivo):** `size_precision` · `amount_precision` · `size_increment` / `step_size` · `lot_size` · `min_quantity` / `min_amount` · `max_quantity` / `max_amount`. `precision` **NO equivale a** `step_size` / `increment`, y ninguno equivale necesariamente a mínimo/máximo. Los aliases específicos de cada exchange **NO se resuelven ahora**; la curaduría/mapping de aliases queda para la materialización.

**NOTIONAL / COST:** `VenueConstraints` puede representar límites nocionales/cost: `min_notional` / `max_notional`. Registro explícito: **`min_notional` NO se declara automáticamente idéntico a `min_cost`**, y ninguno debe confundirse con `min_order_usd`.

```
min_notional / min_cost
        ≠
OCM RiskPolicy.min_order_usd
```

La semántica exacta de **currency / unit / cost / fees incluidos-excluidos / inverse contracts / conversiones** queda **abierta** para decisiones posteriores relacionadas con **HD-VC-5/7 y D12/D19**. **NO se cierra en HD-VC-3.**

**MULTIPLIER = PENDING — D08/D12/D19 (registro explícito):**

- No se registra `multiplier ∈ VenueConstraints` ni `multiplier ∉ VenueConstraints` como decisión definitiva.
- D08 establece que el multiplier puede ser **identity-defining** cuando diferencia una entidad contractual → permanece bajo reconciliación **D08/D12/D19** y **no forma parte automáticamente** de `VenueConstraints`.
- **D08, D12 y D19 NO se modifican.**

**ORDER / EXECUTION POLICY (NO decidida):** HD-VC-3 **NO incorpora** como parte de `VenueConstraints` un catch-all para `post_only` · `reduce_only` · `market_order_allowed` · `limit_order_allowed` · `stop_order_allowed` · order-type restrictions · execution flags. Estas capacidades podrán requerir posteriormente una **frontera específica OrderPolicy / ExecutionPolicy**, pero esa frontera **NO ESTÁ DECIDIDA EN HD-VC-3** y no debe implementarse.

**EXCHANGEQUIRKS (HD-7 preservado):** `ExchangeQuirks` = transport/API/connector behavior. **No** mover `tick_size`, precision, limits ni `min_notional` a `ExchangeQuirks` simplemente porque provengan de una API de exchange: la **fuente externa** y la **propiedad semántica del dato** son conceptos distintos.

**CCXT / NAUTILUS (evidencia externa, no SSOT):** NautilusTrader y CCXT se utilizaron como evidencia externa para identificar patrones maduros de modelado, pero **ninguno constituye el SSOT semántico de OCM**:

```
CCXT ≠ OCM semantic SSOT
external mapping source ≠ domain authority
```

No se introducen dependencias de CCXT. No se modifican adapters.

**HD-VC-3 NO decide:** HD-VC-4 (PIT/temporalidad de VenueConstraints) · HD-VC-5 (provenance/evidence) · HD-VC-6 (FAIL-CLOSED, ausencia y conflicto entre fuentes) · HD-VC-7 (consumidores y materialización física) · D12 (Definición/versionado/identidad contractual pendiente) · D19 (cuestiones de gobernanza/modelado pendientes) · OrderPolicy/ExecutionPolicy (frontera futura) · Currency/Unit (semántica futura). Tampoco resuelve el **versionado/inmutabilidad** que la matriz histórica asociaba a HD-VC-3 (§11: tratar un cambio de constraints como `nueva DefinitionVersion` sigue siendo **PROPUESTA NO decidida**, en D12/HD-VC).

**Fronteras preservadas (verificación READ-ONLY del registro):**

- **Identity:** `VenueConstraints ≠ ListingIdentity` · constraints nunca identidad (HD-1; **HD-VC-1 = DECIDED A+**, §15.g — la cláusula de derivación/exclusión confirma que un campo cuya semántica describe la entidad negociable se enruta a D08/D12/D13 y no se admite como `VenueConstraint`).
- **Definition / ContractTerms:** `VenueConstraints ≠ Definition`; ninguna restricción de ejecución entra en `ContractTerms` (HD-3 guard 6).
- **Representation:** `VenueConstraints ≠ SymbolBinding/native_symbol`.
- **VenueConstraints:** PRICE / SIZE / NOTIONAL — frontera semántica decidida (§15.b).
- **ExchangeQuirks:** solo transporte (HD-7 condición 1).
- **RiskPolicy:** `VenueConstraints ≠ RiskPolicy`; `min/max_order_usd` y `max_open_positions` son política de riesgo (`config/risk/risk.yaml`; `trading/risk/models.py`), no constraints de venue.
- **precision ≠ increment ≠ limits:** registradas como semánticamente diferenciadas (no derivables entre sí).
- **multiplier:** ≠ automáticamente VenueConstraint — **PENDING D08/D12/D19**.
- **CCXT:** fuente externa/evidencia, no SSOT semántico.

**Antecedente (NO autoridad de la decisión):** las revisiones técnicas previas del agente (frontera PRICE/SIZE/NOTIONAL y revisión best-of-world con Nautilus/CCXT/FIX) fueron únicamente antecedente técnico; la decisión registrada es **decisión humana** (2026-09-29) y se registra como tal, no como recomendación del agente.

**NO decidido (permanece pendiente):** HD-VC-1 · HD-VC-4 · HD-VC-5 · HD-VC-6 · HD-VC-7 · D12 · D19 · OrderPolicy/ExecutionPolicy · Currency/Unit semantics · versionado/inmutabilidad de constraints (§11 PROPUESTA) · materialización física. **NO se agrega especificación física** de clases, campos, schemas, registry ni persistencia.

**Estado:** DECIDED + REGISTERED (2026-09-29) · **IMPLEMENTATION = NOT AUTHORIZED**.

---

### 15.c. Cierre documental — HD-VC-4 (Temporalidad/PIT de VenueConstraints: D — B + continuidad/no solapamiento + bitemporalidad diferida)

**Decisión humana:** HD-VC-4 = **D** (autorizada 2026-09-30). Opción: **B + invariante de continuidad/no solapamiento + bitemporalidad diferida**. Decisión **semántica/arquitectónica**; **NO autoriza implementación**.

**Decisión adoptada (explícitamente aceptada):**

> `VenueConstraints` tiene temporalidad **propia e independiente de `DefinitionVersion`**.

```
VenueConstraints
    └── pertenece conceptualmente a (ListingIdentity, venue)
            │
            └── posee intervalo efectivo propio
                    ├── effective_at
                    └── effective_to
```

**Semántica decidida:**

- Una modificación **puramente operativa** de una VenueConstraint (`tick_size`, `price_increment`, `size_increment`, `lot_size`, `min_notional`, etc.) **NO implica automáticamente** nueva `ListingIdentity` ni nueva `DefinitionVersion`.
- La modificación se representa conceptualmente como una **nueva versión temporal de VenueConstraints**.
- El vector «cambio de constraint ⇒ nueva DefinitionVersion» **NO se adopta** como regla general.

**Separación con D08 / D12 (sin redecidir):**

- HD-VC-4 **no modifica ni redecide D08 ni D12**.
- Si un cambio aparentemente relacionado con constraints modifica realmente un **término contractual o identity-defining**, el caso se deriva a **D08 / D12 / D13** según corresponda.
- Ejemplo conceptual: `tick_size` cambia → **VenueConstraints temporal** · `multiplier` contractual cambia → **evaluar D08/D12**.
- **No se registra multiplier como VenueConstraint**; su clasificación continúa pendiente según **D08/D12/D19**.

**PIT (requisito semántico):**

- La temporalidad de VenueConstraints debe permitir **Point-in-Time lookup** conceptual: `get_venue_constraints(listing_identity, venue, as_of)`.
- La consulta resuelve la versión de constraints cuyo **intervalo efectivo contiene `as_of`**.
- **No se permite** utilizar implícitamente «la versión actual» para responder una consulta histórica.
- Establece el **requisito semántico de PIT**, pero **NO implementa** API, port, schema ni storage.

**Continuidad / no solapamiento (con precisión):**

- Las versiones temporales de VenueConstraints para un mismo `(ListingIdentity, venue)` deben mantener **intervalos efectivos no solapados**.
- **La continuidad temporal es una invariante de la secuencia cuando existe cobertura esperada**; HD-VC-4 **NO decide** la política operativa ante gaps o ausencia de evidencia.
- **NO se registra en HD-VC-4** política definitiva de `gap → FAIL-CLOSED` ni `overlap → FAIL-CLOSED`: pertenece a **HD-VC-6**.
- HD-VC-4 establece la **estructura temporal**; HD-VC-6 decidirá el comportamiento ante **ausencia/conflicto**.

**Bitemporalidad diferida (a HD-VC-5):**

- HD-VC-4 decide el **eje de validez efectiva** (`effective_at`/`effective_to`) y **difiere** la decisión sobre bitemporalidad/provenance completa a **HD-VC-5**.
- NO decidir todavía: `observed_at` · `recorded_at` · source precedence · authority · correction · supersession · retraction → permanecen pendientes en **HD-VC-5**.

**Relación con HD-5 (intacto):**

- La temporalidad de VenueConstraints es **conceptualmente independiente** de `ValidityWindow` de `SymbolBinding`.
- HD-5 continúa gobernando: `SymbolBinding → ValidityWindow`. HD-VC-4 gobierna conceptualmente: `VenueConstraints → effective interval`.
- **No se extiende HD-5** ni se modifica su decisión.
- La posible **reutilización de una abstracción temporal común** queda como cuestión futura de materialización y **D19**.

**Relación con SymbolBinding (ejes independientes):**

- `SymbolBinding →` temporalidad de **representación/nombre** · `VenueConstraints →` temporalidad de **reglas operativas del venue**. Ambos ejes pueden cambiar **independientemente**.
- Un cambio `BTCUSDT → BTC-USDT-PERP` **no cambia por sí mismo** `ListingIdentity`, `Definition` ni `VenueConstraints` (D10).
- El PIT debe poder **combinar ambos ejes temporalmente** sin convertir `native_symbol` en identidad ni en owner de las constraints.

**Fronteras preservadas (verificación READ-ONLY del registro):**

- **Identity:** constraints ≠ identidad (HD-1); ningún cambio de constraint operativa toca `ListingIdentity`.
- **Definition:** constraint operativa ⇒ **versión temporal propia** de VenueConstraints, no `DefinitionVersion`; gatillo contractual (identity-defining) ⇒ **D08/D12/D13**.
- **Temporalidad:** dos ejes independientes — SymbolBinding = `ValidityWindow`/D10 (HD-5) · VenueConstraints = `effective interval` (HD-VC-4).
- **Política de ausencia/conflicto NO decidida aquí:** gap/overlap → HD-VC-6.
- **Bitemporalidad/provenance NO decidida aquí:** observed_at/recorded_at/autoridad → HD-VC-5.
- **Instancias previas intactas:** HD-VC-3 (frontera semántica §15.b) · HD-VC-2 (modelo separado §15.a) · HD-7 (`ExchangeQuirks` solo transporte) · HD-8 (catálogo) · HD-5.

**NO decidido por HD-VC-4 (permanece pendiente):** HD-VC-1 · HD-VC-5 · HD-VC-6 · HD-VC-7 · D12 · D19 · multiplier · provenance/evidence model · source authority · conflict resolution · absence/gap policy · overlap policy · physical schema · persistence model · consumers · materialization · OrderPolicy/ExecutionPolicy · Currency/Unit · min_cost semantics. **NO se agrega especificación física** de clases, campos, schemas, registry ni persistencia.

**Antecedente (NO autoridad de la decisión):** la revisión best-of-world del agente para HD-VC-4 (NautilusTrader, CME/FIX SecurityDefinition trading rules, anuncios oficiales de Binance con fecha efectiva; modelos temporales A/B/C/D) fue únicamente antecedente técnico y formulario de decisión; la decisión registrada es **decisión humana** (2026-09-30) y se registra como tal, no como recomendación del agente.

**Estado:** DECIDED + REGISTERED (2026-09-30) · **IMPLEMENTATION = NOT AUTHORIZED**.

---

### 15.d. Cierre documental — HD-VC-5 (Provenance / Evidence de VenueConstraints: B — Evidence explícita)

**Decisión humana:** HD-VC-5 = **B** (autorizada 2026-09-30). Opción: **B — Evidence explícita**. Decisión **semántica/conceptual**; **NO autoriza implementación**.

La decisión fue una **decisión humana** que coincidió con la recomendación técnica del agente; la coincidencia no convierte la recomendación en decisión: la fuente de la decisión es la selección humana (ver Antecedente al final).

**Decisión adoptada (explícitamente aceptada):**

> VenueConstraints requieren **provenance obligatoria** y, adicionalmente, **evidencia explícita identificable/referenciable** que permita auditar y reconstruir el fundamento de la regla conocida por OCM.
>
> **Provenance ≠ Evidence**

**5.1 Provenance obligatoria.** Toda VenueConstraint admitida debe estar acompañada de provenance suficiente para saber: de qué fuente procede · cuándo fue observada · cuándo fue registrada. Mínimo conceptual: `source` · `observed_at` · `recorded_at`. **NO** se convierten todavía en clases, schemas o campos físicos.

**5.2 Evidence explícita.** La evidencia concreta que sustenta una VenueConstraint debe poder **identificarse · referenciarse · verificarse · conservarse** para auditoría/reconstrucción. Conceptual puede incluir: `evidence_id` · `evidence_ref` · `evidence_hash` · `evidence_status`. **NO se deciden aún:** tipos físicos · nombres definitivos de columnas · schema · tabla · storage · puerto · registry · API → pertenecen a **HD-VC-7/D19**.

**Separación de timestamps:**

> `effective_at` / `effective_to` (HD-VC-4) ≠ `observed_at` ≠ `recorded_at`

- `effective_at`/`effective_to` → cuándo la regla **aplica** (validez normativa/operativa, HD-VC-4; no reabierto).
- `observed_at` → cuándo OCM **observó** la evidencia.
- `recorded_at` → cuándo OCM **registró** la evidencia.
- Dimensiones distintas; no mezclar.

**published_at (principio conceptual):** `published_at`, cuando exista, representa el timestamp **declarado por la fuente/publicación** y NO sustituye a `effective_at`, `observed_at` ni `recorded_at`. Sin política física de almacenamiento todavía.

**Versiones inmutables:** las versiones de VenueConstraints decididas en HD-VC-4 son conceptualmente **inmutables**. Una modificación real de la regla produce una **nueva versión** conforme a HD-VC-4. **No modificar retrospectivamente una versión ya registrada.** Ejemplo conceptual:

```
V1: { tick_size = 0.01 ; effective_at = T1 ; effective_to = T2 }
V2: { tick_size = 0.10 ; effective_at = T2 ; effective_to = ... }
```

**Evidencia adicional ≠ automáticamente nueva versión:**

- **CAMBIO DE REGLA** → nueva versión.
- **EVIDENCIA ADICIONAL SOBRE LA MISMA REGLA** → NO implica automáticamente nueva versión.
- El modelo físico para múltiples evidencias **no se decide**.

**Conflictos — NO resuelto (delegado a HD-VC-6).** HD-VC-5 **SÍ decide**: preservar evidencia; hacerla identificable/referenciable; mantener provenance; permitir reconstrucción/auditoría. HD-VC-5 **NO decide**: qué fuente tiene prioridad · jerarquía de autoridad · latest-wins · source precedence · cómo resolver dos evidencias incompatibles · cuándo existe UNKNOWN_CONFLICT · qué hacer ante ausencia · gap · overlap · correction · supersession · retraction como política → todo permanece pendiente de **HD-VC-6**. Se preserva **HD-6**: la evidencia conflictiva se **conserva y no se descarta** por el mero hecho del conflicto.

**Bitemporalidad:** HD-VC-5 **NO** es una decisión completa de bitemporalidad. HD-VC-4 ya dejó diferida la bitemporalidad completa/as-known-by. Se registra solo que `effective time` y `observation/recording time` son **conceptualmente diferentes**. La política completa de reconstrucción "as-known-by OCM" permanece **abierta**.

**Source / Authority:** separación conceptual: `source` (qué fuente produjo la evidencia) es describible por HD-VC-5; `authority`/`precedence` (qué fuente es más autoritativa) **NO** es decidido por HD-VC-5 → permanece para **HD-VC-6**.

**Correction / Supersession / Retraction:** **NO** son política decidida. Puede mencionarse que una evidencia puede posteriormente ser corregida, superseded o retractada; las **reglas operativas** para correction/supersession/retraction permanecen pendientes de **HD-VC-6**. No se registran acciones, estados ni workflows físicos.

**Fronteras preservadas (sin reapertura):** HD-VC-2 (modelo conceptual separado de ExchangeQuirks) · HD-VC-3 (PRICE/SIZE/NOTIONAL con precision/increment/limits) · HD-VC-4 (temporalidad propia, effective_at/effective_to, PIT, continuidad/no-overlap) · HD-6 (evidencia conflictiva retenida) · HD-7 (`ExchangeQuirks` limitado a transporte/API/connector quirks). No se reabre HD-1..HD-4, HD-6, HD-7 ni HD-8.

**NO reabrir D12/D19:** HD-VC-5 **NO decide** si toda modificación de VenueConstraint implica DefinitionVersion · ownership de multiplier (gobernado por **D08/D12/D19**; NO se incorpora automáticamente a VenueConstraints) · ContractTerms · ListingIdentity · catalog · physical persistence · schemas · registry. Se mantiene **D12 = OPEN · D19 = PENDING**.

**Open questions que permanecen abiertas (no decisiones):** taxonomía definitiva de `source type` · granularidad de `observed_at` para evidencia compartida (anuncios multi-símbolo) · `retrieval mechanism` · hash del payload íntegro vs representación canónica · ownership/admission de evidencia. Ninguna queda convertida en decisión por B (B fijó el nivel evidence explícita; no la taxonomía ni la física).

**Antecedente (NO autoridad de la decisión):** la revisión best-of-world READ-ONLY del agente para HD-VC-5 (FIX SecurityDefinition/SecurityReportID/SecurityUpdateAction; Binance exchangeInfo snapshot current-only + anuncios con fecha efectiva; Nautilus `info`/ts_event/ts_init sin provenance; CCXT snapshot sin historial; Hummingbot snapshot opaco) y su **recomendación técnica B** fueron únicamente antecedente técnico y formulario de decisión; la decisión registrada es **decisión humana** (2026-09-30) y se registra como tal, no como recomendación del agente.

**Estado:** DECIDED + REGISTERED (2026-09-30) · **IMPLEMENTATION = NOT AUTHORIZED**.

### 15.e. Cierre documental — HD-VC-6 (Resolución de conflictos, gaps, overlaps, ausencia y FAIL-CLOSED: D — Hybrid)

**Decisión humana (2026-09-30):** HD-VC-6 = **D — Hybrid**. Decisión **semántica/conceptual**; **NO autoriza implementación** (IMPLEMENTATION = NOT AUTHORIZED).

**Decisión adoptada (explícitamente aceptada):**

> Resolución híbrida basada en autoridad explícita/contextual/granular, aplicabilidad temporal efectiva y resolución determinista; cuando la evidencia disponible no permite una resolución gobernada, inequívoca y reproducible, el resultado debe permanecer explícitamente no resuelto y operar bajo FAIL-CLOSED.

**Alternativas consideradas en el análisis previo (NO elegidas):** A — jerarquía de autoridad explícita; B — resolución determinista de evidencia; C — FAIL-CLOSED ante todo conflicto no resuelto; D — híbrido (elegida); E — scoped source-of-record + taxonomía explícita de no-resuelto. La decisión es **D — Hybrid**; **NO se adopta ninguna jerarquía física concreta de fuentes** (no se decide nada como «official API > official documentation > connector > third party»); la autoridad queda definida conceptualmente como **explícita, contextual y granular**; la materialización física de esa autoridad queda para las fases posteriores correspondientes.

**Razón de la decisión (registro):** la evidencia de industria muestra que (a) la mayoría de los conflictos aparentes son desalineación temporal (efectiva) o de admisibilidad; (b) la precedencia entre fuentes exige regla gobernada, nunca heurística (latest-wins/source-wins/API-wins/observed-wins NO se asumen); (c) la ausencia de evidencia no se rellena por inferencia ni interpolación; (d) la falsa certeza es un riesgo operativo mayor que el no-resuelto explícito (Hummingbot #6230: ausencia de `min_notional` → crash). D combina autoridad gobernada + determinismo reproducible + FAIL-CLOSED residual.

**Principios registrados (SOURCE ≠ AUTHORITY; LATEST OBSERVED ≠ LATEST TRUTH):**
- **SOURCE ≠ AUTHORITY:** `source` (qué fuente produjo la observación) queda descrito por HD-VC-5 (provenance); `authority` (qué fuente es autoritativa para un contexto) es una propiedad **gobernada** y **NO se deduce** del source ni de la ausencia de fuente.
- **LATEST OBSERVED ≠ LATEST TRUTH:** que una fuente haya observado un dato más recientemente NO convierte automáticamente ese dato en autoritativo.
- **OBSERVED_AT ≠ EFFECTIVE_AT ≠ RECORDED_AT:** se preservan las distinciones ya establecidas en HD-VC-4 (`effective_at`/`effective_to` — HD-VC-4, no reabierto) y HD-VC-5 (`observed_at`/`recorded_at`/`published_at`). No mezclar ejes.

**Secuencia conceptual de resolución (4 pasos):**

- **Paso 1 — Aplicabilidad temporal:** primero se determina qué evidencia/versiones son aplicables al instante efectivo consultado, respetando la temporalidad decidida en HD-VC-4 (`effective_at`/`effective_to`, consultas PIT conceptuales). «Versiones no solapadas» se mantiene como **condición normativa pendiente de las reglas de resolución correspondientes**, no como mecanismo físico. NO mezclar `effective_at` con `observed_at` o `recorded_at`.
- **Paso 2 — Evaluación de autoridad:** entre la evidencia temporalmente aplicable se evalúa la autoridad gobernada: **explícita · contextual · granular · determinista**. NO asumir latest-wins · source-wins · API-wins · observed-wins; NO inventar una jerarquía concreta.
- **Paso 3 — Resolución determinista:** si existe una resolución gobernada e inequívoca → **RESOLVED**. El resultado debe ser **reproducible**: para los mismos inputs relevantes de evidencia, autoridad y tiempo efectivo, la resolución produce el mismo resultado.
- **Paso 4 — No resolución:** si NO existe una resolución gobernada, inequívoca y reproducible → **NO se elige arbitrariamente un valor**; el resultado permanece **explícitamente no resuelto**. Estados conceptuales posibles: `UNRESOLVED_CONFLICT` · `UNRESOLVED_GAP` · `UNRESOLVED_OVERLAP` · `UNRESOLVED_EVIDENCE` · `UNRESOLVED_AUTHORITY`. Esta taxonomía es **conceptual** y puede usarse en este documento, pero **NO se convierte en enums, clases, schemas ni código**; la taxonomía física puede refinarse durante HD-VC-7/materialización si fuese necesario.

**Reglas FAIL-CLOSED (semántica de resolución; NO define comportamiento por consumidor):** cuando una situación no puede resolverse mediante las reglas gobernadas y deterministas disponibles → **FAIL-CLOSED**: no inventar un valor · no seleccionar arbitrariamente una fuente · no asumir latest-wins · no interpolar valores históricos · no convertir ausencia de evidencia en evidencia. HD-VC-6 establece la **semántica de resolución/no-resolución**; HD-VC-7 decidirá posteriormente la materialización y los consumidores.

**Gaps → UNRESOLVED_GAP → FAIL-CLOSED:** una ausencia de versión/evidencia aplicable no se rellena artificialmente. Ejemplo conceptual `effective timeline A ──────────┐ GAP └────────── B`: si no existe evidencia/versionado gobernado que cubra el instante consultado → **UNRESOLVED_GAP**. NO se permite: copiar automáticamente el valor anterior · usar el valor actual · interpolar · asumir continuidad no demostrada.

**Overlaps → UNRESOLVED_OVERLAP → FAIL-CLOSED:** si existen múltiples versiones/evidencias aplicables al mismo instante y las reglas de autoridad no permiten resolverlas de manera determinista → **UNRESOLVED_OVERLAP**. No se selecciona arbitrariamente la más reciente, la primera, la última insertada, la del API ni la de una fuente determinada, salvo que una **futura regla explícita de autoridad** lo gobierne (esa regla NO se inventa aquí).

**Conflicts → UNRESOLVED_CONFLICT → FAIL-CLOSED:** si dos o más evidencias aplicables presentan valores incompatibles y la autoridad gobernada no permite resolver el conflicto → **UNRESOLVED_CONFLICT**. El conflicto **NO se destruye eliminando evidencia**; se preserva HD-6: el conflicto es una **condición explícita de dominio** y no puede resolverse porque exista un único candidato materializado.

**Evidencia insuficiente / ausencia → UNRESOLVED_EVIDENCE → FAIL-CLOSED:** si existe evidencia pero no es suficiente para establecer una resolución gobernada → **UNRESOLVED_EVIDENCE**. Si no existe evidencia suficiente para establecer el estado histórico, **NO se reconstruye por inferencia**. Especialmente: una API actual de un exchange que no proporciona historial NO demuestra cuál era el estado histórico de VenueConstraints en un instante pasado; no inventar reconstrucciones históricas.

**Autoridad ambigua → UNRESOLVED_AUTHORITY → FAIL-CLOSED:** si existen múltiples fuentes/evidencias pero no existe una regla de autoridad suficiente para decidir cuál debe prevalecer → **UNRESOLVED_AUTHORITY**. Es **distinto** de afirmar que una fuente es incorrecta: la situación significa que existe evidencia, pero la gobernanza disponible no permite determinar cuál debe considerarse autoritativa para ese contexto.

**Corrección / Supersession / Retraction (HD-VC-5 preservado):** la evidencia **no se elimina destructivamente**; una **corrección** modifica el estado interpretado; una **supersession** modifica la aplicabilidad/interpretación; una **retraction** modifica el estado de la evidencia; el **historial de evidencia permanece auditable**. NO se implementa ningún mecanismo físico; no se deciden ahora schemas, tablas, IDs físicos ni puertos (HD-VC-7/D19).

**Auditabilidad (requisito conceptual):** la resolución futura debe permitir explicar — si **RESOLVED**: qué evidencia fue considerada · cuál era temporalmente aplicable · qué regla de autoridad permitió resolver · por qué el valor seleccionado fue válido; si **UNRESOLVED**: qué evidencia existía · qué condición impidió resolver · qué tipo de no-resolución ocurrió · por qué se aplicó FAIL-CLOSED.

**Determinismo (registro):** la resolución no puede depender del orden accidental de inserción, timing de ejecución, último valor observado, comportamiento incidental del adapter ni de selección arbitraria de una fuente. Con el mismo conjunto de inputs relevantes → **misma resolución**.

**Current-only exchange APIs:** se preserva la conclusión del análisis Best-of-World: los exchanges pueden proporcionar endpoints actuales `exchangeInfo`/`markets`/metadata que NO constituyen por sí mismos un historial completo de VenueConstraints. Por tanto **current state ≠ historical proof**: no se permite reconstruir retrospectivamente `tick_size` · `price_increment` · `size_increment` · `min_notional` · `limits` si la evidencia histórica no existe.

**Fronteras preservadas (NO se reabren):** HD-1 · HD-2 · HD-3 · HD-4 · HD-5 · HD-6 · HD-7 · HD-8 · HD-VC-2 · HD-VC-3 · HD-VC-4 · HD-VC-5 · D08 · D12 · D19. En particular: **D08/multiplier** — NO se decide nuevamente si multiplier pertenece o no a VenueConstraints; **D12** — NO se deciden reglas generales de DefinitionVersion/ListingIdentity; **D19** — NO se decide materialización física; **HD-VC-7** — NO se deciden consumidores, ports, schemas, persistence ni wiring.

**Alcance exacto de HD-VC-6:** cómo se resuelve **semánticamente** la información temporalmente aplicable de VenueConstraints cuando existe evidencia potencialmente conflictiva, incompleta, ausente, solapada o ambiguamente autoritativa. HD-VC-6 **NO es** diseño de implementación.

**Antecedente (NO autoridad de la decisión):** la revisión best-of-world READ-ONLY del agente para HD-VC-6 (FIX MDUpdateAction 0/1/2/3/4 · SecurityUpdateAction A/M/D · TransactTime(60)/EffectiveBusinessDate(2400); bitemporalidad valid/transaction time con corrección insert-only; MDM golden record/survivorship rules gobernadas + escalada a steward; Hummingbot #6230 ausencia→crash; CCXT precision≠limits y current-only; Binance current-only; BLZUSDT) y su **recomendación técnica D — Hybrid** fueron únicamente antecedente técnico y formulario de decisión; la decisión registrada es **decisión humana** (2026-09-30) y se registra como tal, no como recomendación del agente.

**Estado global post-decisión:**

```
HD-1..HD-8       = DECIDED
HD-VC-2          = DECIDED — A
HD-VC-3          = DECIDED — A
HD-VC-4          = DECIDED — D
HD-VC-5          = DECIDED — B
HD-VC-6          = DECIDED — D
HD-VC-1          = PENDING
HD-VC-7          = PENDING
D12              = OPEN
D19              = PENDING
GATE C           = BLOCKED
IMPLEMENTATION   = NOT AUTHORIZED
```

**Estado:** DECIDED + REGISTERED (2026-09-30) · **IMPLEMENTATION = NOT AUTHORIZED**.

### 15.f. Cierre documental — HD-VC-7 (Materialización, ownership, consulta y consumidores: A — Domain-owned VenueConstraints)

**Decisión humana (2026-09-30):** HD-VC-7 = **A — Domain-owned VenueConstraints**. Decisión de **ownership semántico**; **NO autoriza implementación** (IMPLEMENTATION = NOT AUTHORIZED).

**Decisión adoptada (explícitamente aceptada):**

> VenueConstraints es **responsabilidad semántica del dominio de instrument/market metadata de `market_data`**, separada de Canonical Instrument Identity, DefinitionVersion, SymbolBinding, ExchangeQuirks y RiskPolicy. La **resolución de VenueConstraints pertenece al DOMAIN** y aplica las reglas decididas en HD-VC-4, HD-VC-5 y HD-VC-6. La interacción externa se conceptualiza mediante un **outbound Registry/Port que informa al Application layer; no decide semántica, no sustituye al Domain y no se convierte por sí mismo en SSOT físico**.

**Precisión fundamental de alcance (vinculante):** esta decisión es **SEMÁNTICA**. **NO fija todavía** materialización física definitiva, persistencia, schema, package layout, nombres de clases, nombres de tablas, Iceberg, Kafka topics, nombres definitivos de ports, identificadores físicos, API pública, consumer contract definitivo, audit ledger ni almacenamiento de estados no resueltos. Todo ello queda **sujeto a D19 y a decisiones posteriores de materialización**.

**Reparto de responsabilidades (HD-4, preservado):** `APPLICATION = ORCHESTRATE` · `DOMAIN = DECIDE` · `PORT/REGISTRY = INFORM` · `ADAPTER = TRANSLATE / PROVIDE EVIDENCE` · `CONSUMERS = CONSUME`.

**Reglas de resolución:** las de **HD-VC-6** (D — Hybrid) siguen gobernando sin alteración: aplicabilidad temporal efectiva → autoridad explícita/contextual/granular/determinista → resolución determinista reproducible → `RESOLVED` **o** estado no resuelto explícito (`UNRESOLVED_CONFLICT` / `UNRESOLVED_GAP` / `UNRESOLVED_OVERLAP` / `UNRESOLVED_EVIDENCE` / `UNRESOLVED_AUTHORITY`, taxonomía **conceptual**) → **FAIL-CLOSED**. La resolución **NO** puede residir dentro de adapter, trading, risk, query de base de datos ni cliente HTTP (invariante HD-4: DOMAIN = DECIDE).

**Frontera owner ↔ consumers:**
- **Domain (owner semántico):** `VenueConstraints`, modelo semántico PRICE/SIZE/NOTIONAL con ejes `precision` / `increment` / `limits` (HD-VC-3, `precision ≠ increment ≠ limits`), aplicabilidad temporal efectiva (`effective_at` / `effective_to`, PIT, HD-VC-4), interpretación de `provenance` y `evidence` (HD-VC-5: `Provenance ≠ Evidence`; `effective_at ≠ observed_at ≠ recorded_at`), autoridad y resolución determinista, y **FAIL-CLOSED** (HD-VC-6). El dominio **NO** inventa valores históricos, **NO** interpola, **NO** aplica `latest-wins` / `source-wins` / `observed-wins`, y **NO** convierte ausencia de evidencia en evidencia.
- **Application (orquestador):** coordina `input → lookup → resolution → output` usando el Registry/Port y la política de dominio; **NO redefine** la política semántica, **NO decide** autoridad, **NO resuelve** conflictos por sí mismo.
- **Registry / Port (outbound, conceptual):** expone el acceso al conocimiento de VenueConstraints para un contexto `ListingIdentity + venue + effective/as_of`; **INFORMA**; **NO** decide identidad, **NO** resuelve conflictos por sí mismo, **NO** aplica `latest-wins`, **NO** es un segundo policy engine, **NO** es SSOT físico por sí mismo. **Existe conceptualmente** (no se fija aquí el nombre definitivo del port, ni métodos, ni tipos de retorno — D19 + materialización posterior).
- **Adapter:** traduce representación exchange-specific (`exchangeInfo` / `markets` / metadata / security definition) hacia **evidence**/material de observación; **NO** declara "este valor es la verdad canónica", **NO** resuelve, **NO** publica `VenueConstraints` canónica. Lo observado por un adapter es **evidence**, no canonical truth (HD-VC-5/HD-VC-6).
- **Trading / Execution (consumers):** pueden **usar** VenueConstraints para validar precio, cantidad, incrementos, notional y preparar órdenes; **NO** son owner semántico ni los redefinen.
- **Risk (consumer):** `RiskPolicy.min_order_usd` es política propia de OCM y **NO** es owner ni sustituto de `VenueConstraints.min_notional` (HD-VC-3).
- **Research / Analytics (consumers read-only conceptualmente):** **NO** redefinen VenueConstraints.

**Frontera conceptual de la resolución (no física):**

```
Exchange
   ↓
Adapter  (TRANSLATE / PROVIDE EVIDENCE)
   ↓
Evidence  (observed · provenance · recorded)
   ↓
Domain Resolution  (aplicabilidad efectiva → autoridad → determinismo)
   ↓
Canonical VenueConstraints  (o UNRESOLVED_* + FAIL-CLOSED)
   ↓
Registry / Port  (INFORM)
   ↓
Application  (ORCHESTRATE)
   ↓
Consumers  (trading · execution · risk · research)
```

**Relación con ListingIdentity (preservada):** `ListingIdentity` es la raíz conceptual de `DefinitionVersion`, `SymbolBinding` y `VenueConstraints`; son **conceptos distintos** y ninguno se equate: `VenueConstraints ≠ DefinitionVersion`, `≠ SymbolBinding`, `≠ Identity`. La relación es `ListingIdentity + venue + effective time → VenueConstraints`. HD-VC-4 y HD-5 intactos: el cambio de `native_symbol` **NO** altera VenueConstraints y un cambio de VenueConstraints **NO** crea nueva `ListingIdentity`/`DefinitionVersion`.

**Separación de conceptos (invariante):** `IDENTITY ≠ DEFINITION ≠ REPRESENTATION ≠ VenueConstraints ≠ ExchangeQuirks ≠ RiskPolicy` — HD-8 preservado. `ExchangeQuirks` = **solo** transporte/API/connector quirks (HD-7); **NO** se usa `ExchangeQuirks.tick_size` / `.min_notional` como SSOT semántico; que un dato aparezca en un adapter/connector **NO cambia** su ownership semántico.

**Multiplier / contract size (no reabrir D08):** permanece bajo el ámbito de **D08/D12**; **NO** se incorpora automáticamente a VenueConstraints por el hecho de que frameworks externos lo modelen junto a operational instrument metadata. Su frontera definitiva sigue PENDING.

**Physical materialization — NO se cierra (permanece OPEN/FUTURE):** package layout definitivo · clases concretas · schema · tablas · Iceberg · Kafka topics · nombres de tablas · identificadores físicos · versión de persistencia · repositorio · caché · API pública · contrato de consumidor · audit ledger · almacenamiento de estados `UNRESOLVED_*`. Todo ello **sujeto a D19 + decisión posterior de materialización**; esta decisión **no los anticipa**.

**Deuda técnica (registrada):** evita múltiples SSOT (`ExchangeAdapter.tick_size` / `Trading.tick_size` / `Risk.tick_size` / `MarketData.tick_size`), adapter-owned truth, `latest-wins` implícito, pérdida de PIT/provenance/evidence, mezcla de venue constraints con RiskPolicy o ExchangeQuirks, y resolver duplicado por consumidor. Acepta, como deuda **explícita y controlada** (no razón para mover ownership): nuevo concepto de dominio · Registry/Port conceptual · mapeo por venue · normalización de alias · materialización futura · curación de metadata por venue.

**Alcance de la decisión:** `HD-VC-7` establece **ownership semántico, responsabilidad de resolución, frontera owner↔consumers, dirección de dependencias, responsabilidad del adapter, existencia conceptual de un Registry/Port, y separación respecto de los demás conceptos**. **No** es diseño de implementación ni cierra D12/D19.

**No reabrir:** HD-1 · HD-2 · HD-3 · HD-4 · HD-5 · HD-6 · HD-7 · HD-8 · HD-VC-2 · HD-VC-3 · HD-VC-4 · HD-VC-5 · HD-VC-6 · D08. **No decidir todavía:** D12 · D19 · HD-VC-1 · materialización · multiplier · contrato de consumidor · schema físico · persistencia · normalización completa de símbolos.

**Best-of-World (evidencia, NO autoridad de la decisión):** la revisión best-of-world READ-ONLY del agente (ownership semántico único; consumers no son owners; adapter produce evidence no canonical truth; resolución en dominio; PIT first-class; materialización posterior; y el riesgo de que el resolver quede oculto en trading/adapters/queries) fue antecedente técnico y formulario de decisión. La decisión registrada es **decisión humana** (2026-09-30), no la recomendación del agente.

**Estado global post-decisión:**

```
HD-1..HD-8       = DECIDED
HD-VC-2          = DECIDED — A
HD-VC-3          = DECIDED — A
HD-VC-4          = DECIDED — D
HD-VC-5          = DECIDED — B
HD-VC-6          = DECIDED — D
HD-VC-7          = DECIDED — A
HD-VC-1          = PENDING
D12              = OPEN
D19              = PENDING
GATE C           = BLOCKED
IMPLEMENTATION   = NOT AUTHORIZED
```

**Estado:** DECIDED + REGISTERED (2026-09-30) · **IMPLEMENTATION = NOT AUTHORIZED**.

### 15.g. Cierre documental — HD-VC-1 (Naturaleza de `VenueConstraints`: A+ — hechos de admisibilidad operativa del venue + cláusula de derivación/exclusión semántica)

**Decisión humana (2026-09-30):** HD-VC-1 = **A+**. Título conceptual: **"VenueConstraints como hechos de admisibilidad operativa del venue + cláusula de derivación/exclusión semántica"**. Decisión **semántica/arquitectónica**; **NO autoriza implementación** (IMPLEMENTATION = NOT AUTHORIZED). Registrada **después** de HD-VC-2..HD-VC-7 y **sin reabrir** ninguna de ellas.

> **Nota de trazabilidad (obligatoria para leer este documento sin error):** las menciones «NO decidido (permanece pendiente): HD-VC-1 …» dentro de los cierres históricos §14.a–§14.h y §15.a–§15.f, así como los bloques «Estado global post-decisión» de §15.e y §15.f que muestran `HD-VC-1 = PENDING`, son **fotografías del estado en la fecha de cada gate** y **se conservan sin alteración** (no se reescribe razonamiento previo ni se sobrescriben decisiones anteriores). El estado **vigente** de HD-VC-1 es el de esta sección §15.g: **DECIDED — A+**.

**Desambiguación previa, vinculante (qué significa y qué NO significa la cláusula):**

> **A+ NO significa que OCM derive numéricamente `VenueConstraints` desde `ContractTerms`, `DefinitionVersion` u otra fuente semántica.** No existe bajo A+ un «`VenueConstraint` derivado de una fuente semántica gobernada» como mecanismo de inferencia de valores. La palabra «derivación» designa **exclusivamente la derivación de la clasificación semántica** de un campo observado —un veredicto de clasificación/enrutamiento— y **nunca la derivación de un valor**.

**A — contrato semántico declarado (naturaleza):**

> `VenueConstraints` representa **hechos de admisibilidad operativa gobernados por el venue** —reglas de admisión de órdenes—, no hechos contractuales del instrumento.

| Cláusula | Contenido |
|---|---|
| **A.1** | `VenueConstraints` son hechos de admisibilidad operativa del venue; **no** son términos contractuales del instrumento. |
| **A.2** | **Scope conceptual** = `(ListingIdentity, venue)`. |
| **A.3** | **Versión temporal propia** por `(ListingIdentity, venue)` con intervalo efectivo `effective_at`/`effective_to` y PIT (HD-VC-4, no reabierto). Un cambio de regla ⇒ **nueva versión temporal de `VenueConstraints`**; **no** ⇒ `ListingIdentity`, **no** ⇒ `DefinitionVersion`, **no** ⇒ relación D13/D14. |
| **A.4** | **Evidence obligatoria** + `provenance` (HD-VC-5, no reabierto). |
| **A.5** | **Ownership semántico en el dominio** de instrument/market metadata de `market_data`; `DOMAIN = DECIDE` (HD-VC-7, no reabierto). |
| **A.6** | **`VenueConstraints` NO es Identity.** Ningún constraint es identity-defining ni participa en construcción, versionado, lifecycle o relaciones de `ListingIdentity`. |
| **A.7** | **`VenueConstraints` NO es DefinitionVersion.** Ningún constraint pertenece a `ContractTerms` ni a `DefinitionVersion`. |
| **A.8** | **`VenueConstraints` NO es Representation.** No es `native_symbol` ni `Symbol`; el eje temporal de `SymbolBinding` es independiente (HD-2, HD-VC-4). |
| **A.9** | **`VenueConstraints` NO es ExchangeQuirks.** `ExchangeQuirks` permanece limitado a transporte/API/connector (HD-7). **La procedencia de un valor no confiere la categoría.** |
| **A.10** | **`VenueConstraints` NO es RiskPolicy.** `VenueConstraints.min_notional ≠ RiskPolicy.min_order_usd`; ninguna sustituye a la otra. |
| **A.11** | **Ausencia:** la ausencia de constraints **no invalida** una identidad; **sí puede bloquear la ejecución** → `UNRESOLVED_GAP` → FAIL-CLOSED (HD-VC-6). |
| **A.12** | **Alcance del concepto:** los seis ejes decididos en HD-VC-3 — PRICE / SIZE / NOTIONAL × precision / increment / limits. |

**A+ — cláusula añadida: derivación como regla de exclusión y enrutamiento de clasificación semántica.**

| Cláusula | Contenido |
|---|---|
| **A+.1** | **Procedencia ≠ admisibilidad.** Que un valor sea publicado por el venue, cargado por un adapter o provisto por cualquier fuente **no le confiere** la categoría de `VenueConstraint`. La admisibilidad se decide por **la semántica de lo que el valor describe** (regla semántica de HD-3, capa A) y **no** por el nombre del campo, su tipo, su posición en el payload ni su procedencia. |
| **A+.2** | **Los datos observados deben clasificarse semánticamente** antes de cualquier decisión de admisión. |
| **A+.3** | **Si el dato pertenece inequívocamente al dominio de `VenueConstraints` y existe evidencia admisible → puede ser admitido como `VenueConstraint`.** |
| **A+.4** | **Si el dato pertenece a otra frontera semántica → debe ser enrutado** al dominio/carril correspondiente. El enrutamiento **no crea** valor, versión, entidad, relación ni artefacto: produce **un veredicto de clasificación auditable**, ligado a la evidencia concreta desde la que se emite. |
| **A+.5** | **No se permite inferencia numérica para fabricar `VenueConstraints`** (ver regla de no-inferencia). |
| **A+.6** | **No se permite crear un segundo SSOT:** un valor no puede ser simultáneamente término de `ContractTerms`/`DefinitionVersion` y `VenueConstraint`; si ya pertenece al contrato, su observación en el payload del venue es **evidence**, no una restricción adicional; simétricamente, un campo admitido como `VenueConstraints` no se replica en `ContractTerms`; y el carril de restricciones **no** puede usarse para suplir una `Definition` incompleta. |
| **A+.7** | **La admisión es auditable y conserva provenance/evidence:** el veredicto de clasificación reutiliza —no duplica— la evidencia del campo observado (HD-VC-5) y explica qué se observó y por qué se clasificó así. La forma física de almacenarlo es **D19**. |
| **A+.8** | **Clasificación no determinista → admisión FAIL-CLOSED y remisión** al carril semántico correspondiente. |

**Regla de admisión directa (ejemplos conceptuales, no normativos de nomenclatura):**

Cuando una fuente proporciona **directamente** un dato cuya semántica está dentro del contrato de `VenueConstraints`, el dato **puede ser admitido por evidencia**. Ejemplos conceptuales: `tickSize` → PRICE increment · `stepSize` / size increment → SIZE increment · `minNotional` → NOTIONAL limit. La admisión debe conservar: `source`/`provenance` · `evidence` · `observed_at` · `recorded_at` · aplicabilidad efectiva según HD-VC-4 · trazabilidad suficiente para auditoría. **No se asume que el nombre de un campo determina automáticamente su semántica OCM** (A+.1).

**Regla de exclusión / enrutamiento (frontera explícita):**

| Dato observado | Carril de destino | Estado |
|---|---|---|
| `contractSize` / multiplier | **NO** se incorpora automáticamente a `VenueConstraints`; permanece bajo **D08/D12/D13** | No decidido aquí |
| `post_only` | Fuera de `VenueConstraints` → **OrderPolicy/ExecutionPolicy** | No decidida |
| `reduce_only` | Fuera de `VenueConstraints` → **OrderPolicy/ExecutionPolicy** | No decidida |
| `native_symbol` | **Representation / SymbolBinding**, no `VenueConstraints` | Decidido (HD-2) |
| `min_order_usd` | **RiskPolicy**, no `VenueConstraints` | Decidido (HD-VC-2/3) |

**A+ no se utiliza para resolver decisiones pertenecientes a D08, D12, D13, D19 ni a otros carriles.**

**Regla de conflicto (distinción obligatoria entre dos naturaleza):**

**(A) Conflicto interno del carril `VenueConstraints`.** Ej.: evidencia 1 `tickSize = 0.01` · evidencia 2 `tickSize = 0.001` · mismo `(ListingIdentity, venue, T)`. Ambas son admisiones directas. Resultado: **`UNRESOLVED_CONFLICT` → evidencia preservada → resolución conforme a HD-VC-6 → FAIL-CLOSED si no existe resolución gobernada, explícita y determinista.** NO `source-wins` · NO `API-wins` · NO `latest-wins` · NO `observed-wins`.

**(B) Discrepancia que pertenece a otro carril semántico.** Ej.: el venue publica `contractSize = 100` y `ContractTerms.contract_multiplier = 10`. Esto **NO** se convierte automáticamente en un conflicto de `VenueConstraints`; permanece en el carril de **D08/D12/D13**. La evidencia se conserva conforme a HD-VC-5/6 (HD-6 preservado: el conflicto no se destruye). **No se resuelve mediante `VenueConstraints`.**

**Regla de no inferencia (prohibiciones expresas):**

A+ **prohíbe** inferencias numéricas o semánticas destinadas a completar `VenueConstraints`: derivar `tick_size` desde `price_precision` · derivar `increment` desde `precision` (incl. `10^-precision`) · derivar `min_notional` desde `min_order_usd` · derivar `contractSize` desde `multiplier` · derivar `VenueConstraints` mediante fórmulas no gobernadas · asumir que dos nombres de campos equivalentes en un exchange significan necesariamente el mismo concepto OCM. **A+ preserva la distinción `precision ≠ increment ≠ limits`** (HD-VC-3).

**Cláusula de FAIL-CLOSED para clasificación (no determinista):**

Si un campo observado **no puede clasificarse de forma determinista** dentro de la semántica OCM (término no clasificado, semántica ambigua, evidencia contradictoria — HD-3 Guard 1/Guard 2): **NO** se produce un `VenueConstraint` canónico · **NO** se inventa un valor · **NO** se utiliza «unknown constraint» como sustituto · **NO** se habilita una vía alternativa para obtener el valor por inferencia · **se conserva la evidencia** · **se remite** la cuestión al carril semántico correspondiente. La clasificación no determinista **no redefine HD-VC-6 ni crea automáticamente un nuevo `UNRESOLVED_*`** (el refinamiento físico de la taxonomía es D19).

**Invariantes preservados (no se reabre ninguno):**

HD-1 · HD-2 · HD-3 · HD-4 · HD-5 · HD-6 · HD-7 · HD-8 · HD-VC-2 · HD-VC-3 · HD-VC-4 · HD-VC-5 · HD-VC-6 · HD-VC-7. En particular: `IDENTITY ≠ DEFINITION ≠ REPRESENTATION` · `VenueConstraints ≠ ExchangeQuirks` · `VenueConstraints ≠ RiskPolicy` · `Trading/Execution/Risk/Research` son **consumidores, no owners** · `DOMAIN = DECIDE` · `APPLICATION = ORCHESTRATE` · `PORT/REGISTRY = INFORM` · `ADAPTER = TRANSLATE / PROVIDE EVIDENCE` · la evidence **no se destruye** · la resolución no determinista → **FAIL-CLOSED** cuando corresponda (HD-VC-6). Preservados literalmente: `precision ≠ increment ≠ limits` (HD-VC-3) · `min_notional ≠ min_order_usd` (HD-VC-2/3) · `ExchangeQuirks` = transporte/API únicamente (HD-7) · `multiplier` **NO** se incorpora automáticamente (D08/D12/D19) · `VenueConstraints` es domain-owned (HD-VC-7) · PIT/`effective_at` decididos en HD-VC-4 · provenance/evidence decididos en HD-VC-5 · resolución determinista + FAIL-CLOSED decididos en HD-VC-6 · `effective_at ≠ observed_at ≠ recorded_at` · `SOURCE ≠ AUTHORITY` · `LATEST OBSERVED ≠ LATEST TRUTH`.

**NO decidido / NO reabierto por HD-VC-1 = A+:**

**D08** · **D12** · **D13** · **D19** (permanecen exactamente como estaban: D12 = OPEN, D19 = PENDING). Y específicamente NO se decide: si `multiplier` es Identity / Definition / VenueConstraint · catálogo de `DefinitionVersion` · ventanas de `DefinitionVersion` · autoridad física concreta · jerarquía física de fuentes · correction/supersession/retraction como mecanismo físico · persistence · schemas · tablas · Iceberg · Kafka · cache · API · package layout · class names · port names · registry names · consumer contracts · normalización completa `Symbol`/`native_symbol`/`underlying`/`quote` · `OrderPolicy`/`ExecutionPolicy`.

**Relación con §14.a punto 5:** HD-VC-1 cierra la faceta de **colocación** (fuera de `DefinitionVersion`, concepto con estructura y versionado propios). Las facetas de **qué campos pertenecen** (HD-VC-2/HD-VC-3), **temporalidad** (HD-VC-4), **provenance/evidence** (HD-VC-5), **resolución** (HD-VC-6) y **ownership/consumidores** (HD-VC-7) ya estaban decididas y **no se reabren**.

**Alcance de la decisión:** `HD-VC-1 = A+` establece la **naturaleza semántica** de `VenueConstraints` y su **frontera de admisión/exclusión y enrutamiento de clasificación semántica**. Es un **contrato semántico**; **no constituye autorización de implementación** y no anticipa ninguna forma física.

**Antecedente (NO autoridad de la decisión):** la revisión best-of-world READ-ONLY del agente para HD-VC-1 (NautilusTrader · CCXT · FIX `MarketSegmentGrp`/969 · MDM survivorship rules · OpenFIGI/MIC · Binance `filters[]` vs identidad contractual · CME *Minimum Price Fluctuation* venue-defined y mutable por banda/calendario; alternativas A, B1, B2, C y A+; recomendación técnica **A+**) fue antecedente técnico y formulario de decisión. La decisión registrada es **decisión humana** (2026-09-30), no la recomendación del agente.

**Estado global post-decisión (VIGENTE — sustituye a los bloques históricos de §15.e y §15.f):**

```
HD-1..HD-8       = DECIDED
HD-VC-1          = DECIDED — A+
HD-VC-2          = DECIDED — A
HD-VC-3          = DECIDED — A
HD-VC-4          = DECIDED — D
HD-VC-5          = DECIDED — B
HD-VC-6          = DECIDED — D
HD-VC-7          = DECIDED — A
D12              = OPEN
D19              = PENDING
GATE C           = BLOCKED
IMPLEMENTATION   = NOT AUTHORIZED
```

**Estado:** DECIDED + REGISTERED (2026-09-30) · **IMPLEMENTATION = NOT AUTHORIZED** · **GATE C = BLOCKED**.

---

## 16. Guardas de nomenclatura para el modelo futuro

- **No asumir** clases Nautilus inexistentes: las clases reales son `CryptoPerpetual`/`PerpetualContract` (= SWAP); el enum OCM ya usa `PERPETUAL/FUTURE/OPTION/SPOT` — sin conflicto.
- `min_order_usd` (risk) ≠ `min_notional` (venue): nombres distintos en módulos distintos.
- `precision` (entero) ≠ `tick_size` (decimal) ≠ `stepSize`: si se guardan como pares, etiquetar con el `precisionMode` del venue (CCXT) para no derivar ciegamente.
- Sin estados inventados en ADR/tracking: los estados nuevos deben usar los enums cerrados del protocolo (M15).

---

## 17. Precondiciones de GATE C (bloqueadas hasta decisión humana)

1. Decisión HD-7 / HD-VC-2 (status de B-MD-005 + owner del modelo) para evitar doble SSOT. — **HD-7 ✓** (A — APROBAR ACOTADO, §14.g: `ExchangeQuirks` se mantiene solo transporte); **HD-8 ✓** (A — CATÁLOGO FORMAL ACOTADO, §14.h: catálogo gobernado de venues conceptual; `Symbol` como representación); **HD-VC-2 ✓** (A — VenueConstraints como modelo separado, §15.a: owner del modelo resuelto); **HD-VC-3 ✓** (A — frontera semántica PRICE/SIZE/NOTIONAL, §15.b: frontera conceptual resuelta; B como materialización futura); **HD-VC-4 ✓** (D — temporalidad propia de VenueConstraints: intervalo efectivo `effective_at`/`effective_to` + continuidad/no solapamiento + bitemporalidad diferida, §15.c); **HD-VC-5 ✓** (B — evidence explícita: provenance obligatoria + evidencia identificable/referenciable, §15.d); **HD-VC-6 ✓** (D — Hybrid: autoridad explícita/contextual/granular/determinista sin jerarquía física + aplicabilidad temporal + resolución determinista reproducible + no-resolución explícita bajo FAIL-CLOSED; taxonomía conceptual UNRESOLVED_*; sin física ni consumidores → HD-VC-7; §15.e); **HD-VC-7 ✓** (A — Domain-owned VenueConstraints: ownership semántico en el dominio de instrument/market metadata de market_data, separado de Identity/DefinitionVersion/SymbolBinding/ExchangeQuirks/RiskPolicy; resolución en DOMAIN aplicando HD-VC-4/5/6; Registry/Port outbound conceptual INFORM; adapter TRANSLATE/PROVIDE EVIDENCE; consumers trading/execution/risk/research consumen por port sin ser owners; materialización física, schema, tablas, package layout e identificadores NO cerrados → D19 y decisión posterior de materialización; §15.f); **HD-VC-1 ✓** (A+ — VenueConstraints como hechos de admisibilidad operativa del venue + cláusula de derivación/exclusión semántica: naturaleza de dato de venue con scope (ListingIdentity, venue), versión temporal propia, evidence obligatoria, domain-owned, y NO Identity / NO DefinitionVersion / NO Representation / NO ExchangeQuirks / NO RiskPolicy; cláusula de derivación = regla de exclusión y enrutamiento de clasificación semántica, nunca inferencia ni derivación de valores; admisión directa por evidencia; enrutamiento al carril semántico propio cuando la semántica no pertenece al contrato de VenueConstraints; prohibición de inferencia numérica y de segundo SSOT; conflicto interno del carril → `UNRESOLVED_CONFLICT` + FAIL-CLOSED (HD-VC-6), discrepancia con otro carril → se conserva evidencia y se remite; clasificación no determinista → no se admite, evidencia conservada, remisión, sin inventar constraint y sin crear `UNRESOLVED_*` nuevo; D08/D12/D13/D19 no decididos ni reabiertos; §15.g).
2. Formularios HD-1..HD-8 y HD-VC-1..HD-VC-7 marcados por el humano (**HD-1 ✓ · HD-2 ✓ · HD-3 ✓ · HD-4 ✓ · HD-5 ✓ · HD-6 ✓ · HD-7 ✓ · HD-8 ✓ · HD-VC-1 ✓ (A+) · HD-VC-2 ✓ · HD-VC-3 ✓ · HD-VC-4 ✓ · HD-VC-5 ✓ · HD-VC-6 ✓ · HD-VC-7 ✓ — formularios completos; ninguna casilla de decisión pendiente en el pack**). Las precondiciones 3 (ADR nuevo formalizado por el humano) y 4 (registro en `tracking.yaml` + contrato/linter) siguen pendientes: **GATE C = BLOCKED**.
3. ADR nuevo (B-MD-005 o G1 addendum) **formalizado por el humano**; ADR-0049 sin modificar.
4. Registro en `tracking.yaml` (B-MD-005) y, si el modelo lo requiere, contrato/linter — solo después de la autorización.

---

## 18. Reproducibilidad

```
REPRODUCIBILIDAD
- commit: 05317f149c57afa05fa5f667b53272e6ace7ba49
- branch: feat/m22b-governance
- fecha: 2026-09-29
- protocolo: AUDIT_PROTOCOL v2.1
- agente/modelo: opencode (big-pickle)
- herramientas: ruff 0.15.10; python 3.13.5 (mypy/pytest/pip-audit no capturados: sync de entorno lenta en la sesión)
- comandos: uv run python scripts/audit_validator.py (baseline: FAIL M21/M4 + WARN M25/M20, preexistentes)
- golden: PASS
- resultado: PASS/FAIL → sin FAIL nuevo tras esta entrega (ver §19)
```

---

## 19. Validación y cierre

- Validación mecánica tras esta entrega (sin modificaciones de código/ADR/tracking): `uv run python scripts/audit_validator.py` — se espera **idéntico** al baseline (§4): FAIL preexistentes M21/M4 + WARN M25/M20; **sin** FAIL nuevo por el artefacto (nombre canónico `AUDIT_OCM_<slug>_<YYYY-MM-DD>.md` cumple M21).
- Un solo archivo creado: este documento, único artefacto documental autorizado de la entrega (destino permitido por el protocolo). Git baseline preservado fuera del único artefacto documental autorizado de esta entrega: sin modificaciones de código/ADR/tracking/etc. y sin Git mutation; el propio documento es el único objeto `??` nuevo.

---

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
IMPLEMENTATION NOT AUTHORIZED
ADR MODIFICATION NOT AUTHORIZED
TRACKING MODIFICATION NOT AUTHORIZED
GIT MUTATION NOT AUTHORIZED