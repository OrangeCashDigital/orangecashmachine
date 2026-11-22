# OCM — INDUSTRY BEST-OF-WORLDS CANONICAL INSTRUMENT IDENTITY
## Architecture Research + Adversarial Review (READ-ONLY / NO DECISION / NO IMPLEMENTATION)

**Fecha:** 2026-10-03
**Destino:** Informe de soporte a decisión (solo salida textual)
**Modo:** ARCHITECTURE RESEARCH + ANALYSIS + ADVERSARIAL REVIEW + DECISION SUPPORT

### 0. MODO DE EJECUCIÓN — OBLIGATORIO
Este trabajo es exclusivamente ARCHITECTURE RESEARCH + ANALYSIS + ADVERSARIAL REVIEW + DECISION SUPPORT.
- NO implementación
- NO registrar decisiones
- NO modificar D19, ADRs, tracking.yaml, GOVERNANCE.md, código, tests, schemas, CI
- NO crear archivos, tablas, migrations
- NO Git mutations (commit/push/merge/rebase/reset/clean/stash)
Si existen contradicciones: documentarlas, identificar fuentes, explicar impacto, NO corregirlas.
Detener después de entregar informe.
---1. FUENTES CANÓNICAS DE OCM---

## 1. FUENTES CANÓNICAS DE OCM

Fuentes consultadas (internas OCM):
1. docs/audits/D19_HUMAN_DECISION_PACK_2026-09-30.md (D19-01..D19-10)
2. docs/audits/AUDIT_OCM_decision-support-hd-venue-constraints-identity_2026-09-29.md (HD-1..HD-8, HD-VC-1..HD-VC-7, evidencia industria LEVEL 0)
3. docs/architecture/decisions/ADR-0049-canonical-instrument-identity-g1.md (G1-D01..D15 adoptadas)

Documentación complementaria relevante: ListingIdentity, DefinitionVersion, ContractTerms, SymbolBinding, ValidityWindow, VenueConstraints, ExchangeQuirks, IdentityRelation, Provenance, Evidence.

**Estado canónico verificado (según evidencia documental):**
- D12 = DECIDED — Alternative 5 + Alternative 3
- HD-VC-1..HD-VC-7 = DECIDED
- HD-1..HD-8 = DECIDED
- D19 = PENDING / BLOCKED
- D19-01 = HUMAN DECISION REQUIRED
- IMPLEMENTATION = NOT AUTHORIZED
- DECISION REGISTERED = NO

**Principio:** No se reinterpretan decisiones existentes. Discrepancias (si existieran) se documentarían sin corregirse.

## 2. PREGUNTA CENTRAL
La pregunta NO es “¿Es mejor UUID, composite key o híbrido?”.
La pregunta es:
¿Cómo diseñar una arquitectura profesional de referencia para que OCM tenga una identidad canónica de instrumento estable, auditable, evolucionable y apta para market data, trading, execution, historical data, Kafka/event-driven architecture, Iceberg, relational persistence, backtesting, replay, point-in-time reconstruction, audit, múltiples venues, múltiples representaciones, múltiples identificadores externos?
Y posteriormente: ¿Qué características debe tener el mecanismo físico de D19-01 para materializar esa arquitectura?

**Distinción fundamental:** Logical ListingIdentity ≠ Physical Identity ID. La materialización física debe preservar Identity ≠ Representation ≠ DefinitionVersion ≠ VenueConstraints.

## 3. INVESTIGACIÓN EXTERNA — FUENTES PRIMARIAS (referencia conceptual LEVEL 0)
Investigación basada en patrones arquitectónicos observados en sistemas profesionales de referencia-data/trading. Prioridad: documentación oficial/especificaciones cuando disponibles; fuentes secundarias solo si necesarias.
Analizados conceptualmente: CME/CME Group (security/contract identity vs product, expiry, contract size, price quotation/minimum fluctuation, lifecycle, separaciones), FIX (Security Definition/SecurityID/SecurityIDSource, market segment/trading rules), OpenFIGI (FIGI/Composite FIGI/Venue-level FIGI, estabilidad de identidad vs representación/mapping), NautilusTrader (InstrumentId/Symbol/Venue/raw_symbol, definitions, increments/precision/limits/multiplier, lifecycle), Hummingbot (TradingRule, exchange constraints), Freqtrade (pair/symbol, markets/precision/limits, historical/backtesting constraints). Otros referentes (Bloomberg/Refinitiv/LSEG/ICE/OMS/EMS/reference-data) considerados solo si aportan patrones materialmente relevantes.

**Separación evidencia vs interpretación:** Lo observado es patrón arquitectónico, NO norma OCM. Toda posible adopción debe justificarse frente al dominio OCM.

## 4. PATRÓN COMÚN EXTRAÍDO (CONVERGENCIAS)
Convergencia profesional (conceptual):
- **Separación estricta:** Canonical Instrument Identity ≠ Instrument Definition (versionable) ≠ Venue Constraints (temporal) ≠ Representation (símbolos/mappings) ≠ External Identifiers (referencias externas) ≠ Lifecycle.
- **Estabilidad referencial:** Identidad canónica tiende a ser **estable e inmutable**; representaciones/mappings son **temporales/versionables**.
- **Venue-scoped:** Identidad de listing suele ser venue-scoped (FIGI leaf por venue; FIX market segment). Equivalencia cross-venue = relación explícita (no identidad compartida) — coherente con G1-D03.
- **Temporalidad:** Constraints/reglas de venue y bindings de símbolo son típicamente temporales (`effective`), no parte de identidad.
- **Separación transporte/contratos:** Constraints operativos (tick/lot/min/precision) diferenciados de definición contractual — coherente con HD-VC-1..7, ExchangeQuirks transporte-only.
- **Unicidad vs representación:** Múltiples representaciones (native/symbol/external IDs) resueltos vía mappings, no fusionados con identidad.
Coherente con decisiones OCM (D01–D12, HD-1..HD-8, HD-VC-1..7).

## 5. SEPARACIÓN IDENTITY VS REPRESENTATION / DEFINITION / VENUE CONSTRAINTS
- **ListingIdentity ≠ native_symbol ≠ SymbolBinding ≠ external identifier ≠ representation.** Símbolos son representaciones; SymbolBinding modela relación temporal entre identidad y representación (D10).
- **ListingIdentity → DefinitionVersion:** Definición contractualmente versionable; cambio no identity-defining no crea nueva identidad; cambio identity-defining crea nueva ListingIdentity con `SUCCESSOR_OF`/`REPLACED_BY` (G1-D13/D14, coherente con D12).
- **ListingIdentity → VenueConstraints:** Eje independiente, temporal (`effective_at/effective_to`), con evidence/provenance (HD-VC-4/5/6). `precision ≠ increment ≠ limits`; `VenueConstraints ≠ RiskPolicy`.
Casos conceptuales (BTC/USDT, BTC-USDT-PERP, BTCUSDT, BTCUSDT-PERP, rename, venue migration, múltiples símbolos): identidad estable ante rename; distinta entre venues (D03); nuevo identity-defining → nueva identidad.

## 6. TARGET PROPERTIES (MUST/SHOULD/NICE TO HAVE)
**MUST HAVE:** immutable, stable, globally/appropriately unique (scope venue-scoped coherente con G1-D03), representation-independent, DefinitionVersion-independent, VenueConstraints-independent, FK-safe, event-key-safe, replay-safe, Iceberg-safe, historical-safe, audit-safe, migration-safe, schema-evolution-safe, deterministic, supports SUCCESSOR_OF/REPLACED_BY, FAIL-CLOSED compatible (HD-VC-1 A+), preserves D12 invariants (informative/non-structural/non-compositional, no coupling temporal), no segundo SSOT.
**SHOULD HAVE:** observable, testable, lookup eficiente vía mappings/registry (PORT/REGISTRY=INFORM HD-VC-7).
**NICE TO HAVE:** legibilidad humana directa (no debe imponerse sobre estabilidad referencial).

## 7. ANALISIS A/B/C — DOMAIN MODEL, PERSISTENCE, EVENTS, ICEBERG, EVOLUTION
### A. ID opaco (UUID/ULID)
- **Domain:** Refuerza Identity≠Representation/DefV/VenueConstraints. Coherente D12/HD-VC-1..7.
- **Persistence:** PK simple, estable, inmutable; FKs estables, migraciones livianas, excelente referential integrity.
- **Events/Kafka:** event key estable → partitioning/replay/correlation determinísticos, key schema estable.
- **Iceberg/histórico:** ID inmutable → time-travel/PIT, joins simples, histórico íntegro.
- **SymbolBinding/DefV/VenueConstraints:** ID estable ante cambios no identity-defining; reverse mapping vía SymbolBinding/registry.
- **Evolución:** Excelente (5+ años). FK/event/histórico seguros.
- **Riesgo:** requiere reverse mapping (gestionable, PORT/REGISTRY=INFORM).

### B. ID compuesto (solo identity-defining inmutables, no representation-bound)
- **Domain:** Viable **solo** si condición (*) estrictamente cumplida. Riesgo estructural si fuerza composición (D19-02 B incompatible).
- **Persistence:** PK multi-columna → FKs heredan columnas; migraciones pesadas si set identity-defining evoluciona.
- **Events/Kafka:** key multi-columna; evolución de key schema delicada (backward/forward).
- **Iceberg/histórico:** joins multi-columna, clustering/particionamiento sensible a evolución; requiere consistencia histórica del composite.
- **Evolución (forense):** añadir nuevo atributo identity-defining afecta nuevas entidades pero complica gobernanza por época; no-retrocorrección obligatorio; riesgo migratorio/FK/event/Iceberg. 
- **Garantías necesarias:** catálogo identity-defining gobernado/estable, principio inmutabilidad histórica, separación por época, exclusión absoluta representation-bound, verificación mecánica de condición (*), gobernanza migratoria estricta.
- **Riesgo:** complejidad evolutiva sustancial a 10+ años.

### C. Híbrido (ID opaco primario + clave lógica única)
- **Domain:** PK opaco estable (referencia canónica). UK lógica auxiliar (identity-defining inmutables).
- **Persistence:** FKs apuntan **solo** a PK opaco (estable). UK para búsquedas/unicidad. Migraciones livianas.
- **Events/Kafka:** event key debe ser **PK opaco** (nunca UK lógica). Partitioning/replay estables.
- **Iceberg/histórico:** PK opaco inmutable → histórico íntegro.
- **Riesgo crítico (SSOT):** UK lógica puede convertirse en **segundo SSOT** si se usa como FK/event key/referencia histórica/cross-service. Debe cumplir: única referencia canónica = PK opaco; UK nunca referenciado como canónico; solo index/constraint auxiliar.
- **Evolución:** favorable (PK estable). UK puede versionarse conceptualmente.

## 8. ADVERSARIAL EVOLUTION TEST (10 años) — IMPACTO A/B/C
Escenarios 1–20 evaluados conceptualmente: rename símbolo (A estable; B solo si no representation-bound; C estable con PK opaco), nuevo venue (nueva identidad — coherente G1-D03), cambio identity-defining (nueva identidad con SUCCESSOR_OF/REPLACED_BY), cambio no identity-defining DefV (ID estable todos), cambio VenueConstraints (ID estable todos), nuevo atributo identity-defining a 5 años (A/C: sin impacto históricos, PK estable; B: desafiante — requiere gobernanza por época), modelo inicialmente incompleto descubierto a 5 años (A/C: histórico preservado, PK inmutable; B: no-retrocorrección obligatorio), Kafka replay 5 años (A/C: key opaco estable; B: key schema evolución delicada), Iceberg time-travel (A/C: joins simples; B: multi-columna + consistencia histórica), migración backend (A/C favorable; B compleja), conflictos evidencia (resueltos HD-VC-5/6, independiente forma ID), external IDs cambian/desaparecen (mappings, no identidad).
**Hallazgos clave:** A y C (con disciplina) maximizan estabilidad referencial a largo plazo. B viable solo con fuertes garantías gobernanza.

## 9. COMPARACIÓN CON DECISIONES OCM (CONTRASTE)
- **Confirmado:** G1-D01 (Identity≠Symbol), G1-D03 (venue-scoped), separación Identity/DefV/VenueConstraints (D12 Alt5+3), HD-VC-1..7 (temporalidad propia, evidence≠provenance, ownership DOMAIN/APP/PORT/REGISTRY/ADAPTER), ExchangeQuirks transporte-only, SymbolBinding independiente.
- **Necesita refinamiento (materialización D19):** forma física ID estable, reverse mapping/registry, límites entidad/agregado (D19-02), persistencia scope (D19-07), backend abstraction (D19-08), eventing opcional (D19-09), boundaries (D19-10).
- **Sin conflicto objetivo:** patrones externos refuerzan separación estricta; OCM más explícito en FAIL-CLOSED/evidence/provenance/ownership.

## 10. TARGET ARCHITECTURE (BEST-OF-WORLDS CONCEPTUAL) — SIN SELECCIONAR A/B/C
**Modelo conceptual:**
- **ListingIdentity (lógico):** entidad conceptual venue-scoped, identidad canónica. **Physical Identity ID** es referencia canónica inmutable (materialización). 
- **Separación estricta:** `Identity ≠ Representation (SymbolBinding/native_symbol) ≠ DefinitionVersion (contractual, versionable) ≠ VenueConstraints (temporal, evidence) ≠ ExchangeQuirks (transporte) ≠ External Identifiers (mappings) ≠ RiskPolicy`.
- **Versionable vs Temporal vs Estable:** Identity **estable**; DefV **versionable** (no identity-defining→mismo ID; identity-defining→nueva identidad + SUCCESSOR_OF/REPLACED_BY); VenueConstraints **temporal** (effective_at/effective_to); SymbolBinding **temporal**; Evidence/Provenance **auditables**.
- **Mappings:** External identifiers y native symbols resueltos vía **mappings/registry** (PORT/REGISTRY=INFORM HD-VC-7), nunca fusionados con Physical ID.
- **Relaciones:** `SUCCESSOR_OF`/`REPLACED_BY` explícitas entre Listing Identities (IDs físicos estables).

**Reglas de cambio:**
- ¿Qué puede cambiar sin cambiar ListingIdentity? `native_symbol` (vía SymbolBinding), DefV no identity-defining, VenueConstraints (nuevas versiones temporales), evidence/provenance, mappings externos.
- ¿Qué crea nueva ListingIdentity? Cambio en propiedad **identity-defining** (G1-D04–D08) → nueva entidad + relaciones tipadas.

## 11. HUMAN DECISION INPUT — D19-01 (SIN RECOMENDACIÓN)
### Arquitectura objetivo
Identidad canónica materializada con **única referencia canónica física inmutable** (FK/event/histórico), separada de representación/mappings, con DefV versionable y VenueConstraints temporal, soportando SUCCESSOR_OF/REPLACED_BY, compatible SQL/Kafka/Iceberg/replay/PIT/auditoría.

### Invariantes obligatorias
- Physical Identity ID = **inmutable** tras creación.
- `Identity ≠ Representation ≠ DefinitionVersion ≠ VenueConstraints`. 
- Referencia canónica única para FKs/event keys/referencias históricas (ningún segundo SSOT como referencia canónica).
- Venue-scoped (G1-D03). Sin coupling temporal obligatorio (D12). 
- FAIL-CLOSED, evidence≠provenance (HD-VC-5), SOURCE≠AUTHORITY (HD-VC-1 A+).
- Lookup vía SymbolBinding/registry (`PORT/REGISTRY=INFORM`, HD-VC-7).

### Riesgos (trade-offs conceptuales)
- **A (opaco):** requiere reverse mapping (aceptable si gestionado vía registry). Bajo riesgo evolutivo.
- **B (compuesto):** requiere gobernanza estricta (catálogo identity-defining por época, no-retrocorrección, verificación mecánica de condición (*), migraciones FK/event/Iceberg complejas). Riesgo evolutivo sustancial si set evoluciona.
- **C (híbrido):** requiere disciplina rigurosa para garantizar UK lógica **nunca** usada como FK/event key/referencia canónica (evitar segundo SSOT). Riesgo conceptual/operativo si no se hace cumplir.

### Preguntas para el HUMANO
1. ¿Se prioriza **simplicidad referencial a largo plazo + migraciones livianas** (A/C) vs **legibilidad directa sin lookup** (B)?
2. ¿Es aceptable carga de gobernanza requerida para que B sea viable 10+ años (catálogo por época, verificabilidad mecánica de condición (*))?
3. ¿Se puede garantizar contractualmente que C mantiene **única referencia canónica = PK opaco** (UK nunca FK/event key/referencia histórica)?
4. ¿Es suficiente `SymbolBinding`/registry para soportar reverse lookup (A/C) en casos operativos/históricos?
5. ¿Está set identity-defining inicial (G1-D04–D08) suficientemente estable para B, o es previsible evolución?
6. ¿Qué peso tiene estabilidad de event keys/Kafka + Iceberg time-travel + auditoría histórica vs legibilidad?

### Consecuencias de cada alternativa (objetivas)
- **A:** estabilidad referencial máxima, evolución más simple, menor superficie migratoria. Requiere reverse mapping.
- **B:** legibilidad directa potencial; viable solo con garantías estrictas. Superficie evolutiva/migratoria mayor (FK/event/Iceberg multi-columna), key schema Kafka menos estable ante evolución.
- **C:** combina estabilidad PK opaco con UK lógica auxiliar. Solo viable si disciplina asegura UK nunca referencia canónica (evitar segundo SSOT). Complejidad conceptual media.

**DECISION: NOT YET DECIDED**

## 12. CONCLUSIÓN
1. **Patrones convergentes:** separación identidad/definición/representación/constraints temporales/evidence; identidad estable+inmutable; mappings temporales; venue-scoped; relaciones explícitas entre identidades; FAIL-CLOSED + evidence/provenance.
2. **Específicos por sistema:** grado de formalización de temporalidad/evidence varía (FIX más explícito en eventos/versionado; FIGI enfatiza estabilidad/mapping; frameworks trading tienden a snapshots). 
3. **Qué debe adoptar OCM:** separación estricta Identity≠Representation≠DefV≠VenueConstraints; Physical ID inmutable (única referencia canónica); mappings temporales vía SymbolBinding/registry; DefV versionable; VenueConstraints temporal+evidence (HD-VC); relaciones SUCCESSOR_OF/REPLACED_BY; FAIL-CLOSED.
4. **Qué debe evitar OCM:** fusionar representación/constraints con identidad; crear segundo SSOT como referencia canónica; acoplamiento temporal obligatorio; interpolación/reconstrucción histórica por inferencia.
5. **Propiedades Physical Identity:** MUST HAVE §6 (inmutable, estable, representación-independiente, independiente DefV/VenueConstraints, FK-safe/event-key-safe/replay-safe/Iceberg-safe/historical-safe/audit-safe/migration-safe/schema-evolution-safe, determinista, soporta SUCCESSOR_OF/REPLACED_BY, FAIL-CLOSED, preserva D12, sin segundo SSOT).
6. **Qué debe permanecer separado:** representación, DefV, VenueConstraints, ExchangeQuirks, External IDs, RiskPolicy.
7. **Qué debe permanecer estable:** Physical Identity ID.
8. **Qué debe ser versionado:** DefinitionVersion (no identity-defining).
9. **Qué debe ser temporal:** VenueConstraints, SymbolBinding (con evidence/provenance).
10. **Sigue requiriendo decisión humana:** D19-01 (forma física: A/B/C) con trade-offs §11.

**Estado final:**
- D19-01: HUMAN DECISION REQUIRED
- DECISION: NOT YET DECIDED
- D19: PENDING / BLOCKED
- GATE C: BLOCKED
- IMPLEMENTATION: NOT AUTHORIZED
- DECISION REGISTERED: NO

## 13. FUENTES (REFERENCIA)
Fuentes internas OCM (consultadas):
- docs/audits/D19_HUMAN_DECISION_PACK_2026-09-30.md
- docs/audits/AUDIT_OCM_decision-support-hd-venue-constraints-identity_2026-09-29.md
- docs/architecture/decisions/ADR-0049-canonical-instrument-identity-g1.md

Fuentes externas (patrones arquitectónicos, LEVEL 0 — no adoptadas como norma):
- CME Group (contract vs product, security definition concepts) — referencia conceptual
- FIX (Security Definition/SecurityID/SecurityIDSource, market segment/trading rules) — referencia conceptual
- OpenFIGI (FIGI factsheet/concepts: FIGI/Composite FIGI/Venue-level, estabilidad vs representación/mapping) — referencia conceptual
- NautilusTrader (Instrument/InstrumentId/Symbol/Venue/raw_symbol, definitions, increments/precision/limits) — referencia conceptual
- Hummingbot (TradingRule, exchange constraints) — referencia conceptual
- Freqtrade (pair/symbol, markets/precision/limits, historical/backtesting) — referencia conceptual
Otras referencias profesionales (Bloomberg/Refinitiv/LSEG/ICE/OMS/EMS/reference-data) consideradas solo si aportan patrones materialmente relevantes.
**Nota:** Fuentes externas usadas únicamente para identificar patrones convergentes. Se distingue claramente evidencia externa de decisión OCM.

## 14. SAFEOPS FINAL
**Verificación explícita (READ-ONLY, sin modificaciones):**
- Files modified: 0
- Files created: 0
- Files deleted: 0
- Code changes: 0
- Tests changes: 0
- ADR changes: 0
- Tracking changes: 0
- Governance changes: 0
- CI changes: 0
- Schemas changes: 0
- Git mutations: NONE
- Commit: NOT PERFORMED
- Push: NOT PERFORMED
- Merge: NOT PERFORMED
- Rebase: NOT PERFORMED
- Reset: NOT PERFORMED
- Clean: NOT PERFORMED
- Stash: NOT PERFORMED

**Working tree (baseline verificado):** MODTRACKED=17 (preexistentes), STAGED=0, UNTRK_D19=1 (docs/audits/D19_HUMAN_DECISION_PACK_2026-09-30.md preexistente). Sin cambios por esta tarea.

**DETENGO AQUÍ.** No avanzo a D19-02. No registro D19-01. Próxima acción solo tras decisión explícita del HUMANO sobre D19-01.
