# OCM — READ-ONLY KNOWLEDGE BASE AUDIT (PDFs relevantes)

**Fecha:** 2026-10-03
**Modo:** EXCLUSIVAMENTE READ-ONLY (sin modificaciones, sin creación, sin Git mutations)
**Alcance:** docs/knowledge + PDFs relevantes en repo

## 1. SAFEOPS — REGLAS CUMPLIDAS
NO modificar/crear/eliminar archivos, NO editar ADRs/tracking/GOVERNANCE/código/tests/CI, NO Git mutations. Solo lectura, extracción, clasificación, comparación con arquitectura OCM.

## 2. DESCUBRIMIENTO COMPLETO
**docs/knowledge structure:**
- manifest.yaml (484 líneas)
- mappings/: ocm-domains.yaml, provenance-log.md
- notes/: bybit-perpetuals-reference.md
- README.md

**PDFs bajo docs/knowledge:** 0 archivos PDF encontrados.
**Manifest entries (manifest.yaml):** 30 entradas listadas (paths bajo docs/knowledge: books/*, papers/*). Ninguno de esos archivos PDF existe físicamente en el árbol (paths no presentes). 

**Otros PDFs en repo:** 1
- ./docs/Clean Architecture A Craftsman Guide to Software Structure and Design.pdf (Robert C. Martin)

**Duplicados/versiones:** No detectados (único PDF fuera de knowledge tree). 
**Tipos según manifest (conceptual):** books (financial-data-quality, data-engineering, market-microstructure, historical), papers (advances in financial ML SSRN preprint), notes/historical. Estado: mayormente needs_verification/needs_attribution_review/needs_legal_review, authority TIER_1–TIER_3/TIER_4 (según manifest).

## 3. LECTURA Y EXTRACCIÓN (PDFs existentes)
**PDF único encontrado:** docs/Clean Architecture A Craftsman Guide to Software Structure and Design.pdf
- Título: Clean Architecture: A Craftsman's Guide to Software Structure and Design (Robert C. Martin Series)
- Autor: Robert C. Martin
- Fecha/creación: 2017-09-20 09:03:42 -05 (Calibre)
- Páginas: 444
- Tipo: libro técnico (arquitectura de software)
- Alcance: Clean Architecture, principios SOLID, boundaries, layers, dependencias (Dependency Rule), use cases, entities, gateways, presenters, frameworks/drivers, tests, arquitectura limpia, componentes, coupling/cohesión.
- Conceptos relevantes (para OCM): Domain/Use Cases/Interfaces Adapters/Frameworks&Drivers, Dependency Inversion/Rule, ports/adapters (Hexagonal), boundaries, entities vs use cases, independencia de frameworks (DB/UI/externos), testabilidad, arquitectura por capas con reglas de dependencia. No trata identidad de instrumentos financieros, market data standards, exchange/venue constraints, Kafka/Iceberg específicos, ni referencia data standards (FIX/FIGI/CME) directamente.
- Páginas relevantes (indicativas): principios generales arquitectura (capítulos sobre boundaries, components, details). 
- Clasificación: técnico (arquitectura software). No normativo financiero.

## 4. CLASIFICACIÓN DOCUMENTAL (PDFs existentes)
**Clean Architecture (RC Martin):**
- A. IDENTIDAD INSTRUMENTOS: No
- B. DEFINICIÓN INSTRUMENTO: No
- C. REPRESENTACIÓN: No
- D. VENUE CONSTRAINTS: No
- E. EXTERNAL IDENTIFIERS: No
- F. LIFECYCLE/TEMPORALIDAD: No (conceptos genéricos)
- G. PROVENANCE/EVIDENCE: No
- H. MARKET DATA: No
- I. PERSISTENCE/HISTORICAL DATA: Parcial (arquitectura general persistence, independencia de DB)
- J. TRADING/EXECUTION: No
- K. EVENT/MESSAGING (Kafka): No

**Conclusión clasificación:** Arquitectura de software general (Clean/Hexagonal, boundaries, Dependency Rule). Útil como referencia arquitectónica transversal (no dominio financiero específico).

## 5. RELACIÓN CON ESTADO ACTUAL DE OCM
Comparación con decisiones existentes:
- **ADR-0049/G1 (D01–D15):** Clean Architecture refuerza separación domain/application/adapters/infrastructure, Dependency Rule, boundaries. Coherente con Hexagonal usado en market_data (domain→ports→application→adapters→infrastructure). No contradice.
- **D12 (Alt5+3):** separación ejes (DefinitionVersion vs VenueConstraints) alineado con boundaries/independencia. Coherente.
- **HD-1..HD-8, HD-VC-1..HD-VC-7:** ownership (DOMAIN decide, APPLICATION orquesta, PORT/REGISTRY informa, ADAPTER traduce/provee evidencia) alineado con Clean Architecture (inversión dependencias, ports/adapters). Coherente.
- **D19/D19-01:** materialización física de identidad requiere aplicar mismos principios (estabilidad referencial, boundaries, independencia frameworks) — Clean Architecture apoya enfoque, pero no resuelve A/B/C.

**Mapping DECIDIDO/MATERIALIZADO/IMPLEMENTADO/PROPUESTO/EXTERNO/PENDIENTE:**
- Clean Architecture (EXTERNO, arquitectura software general) — apoya principios ya adoptados (MATERIALIZADO/IMPLEMENTADO parcialmente). No decide D19-01.

**Conclusión:** Coherente. Sin contradicciones. Refuerza fundamentos arquitectónicos (not dominio financiero).

## 6. MAPEO PDFs CONTRA D19-01
**Único PDF relevante arquitectónicamente:** Clean Architecture (RC Martin).
- **Opaque IDs vs composite vs híbrido:** libro no aborda identidad de instrumentos. No proporciona evidencia directa para A/B/C.
- **Estabilidad referencial/FKs/event keys:** refuerza principios (entidades estables, boundaries, evitar acoplamiento) — **SUPPORTING EVIDENCE** conceptual (no específico).
- **Immutable identifiers, independence frameworks:** alineado con A/C (opaco estable). 
- **Conclusión:** **CONTEXT ONLY** para D19-01. No hay evidencia documental externa específica (FIX/FIGI/CME/OpenFIGI/Nautilus/Hummingbot/Freqtrade) en docs/knowledge PDFs. 

**Hallazgo clave:** manifest.yaml lista 30 PDFs (books/papers) bajo docs/knowledge pero **ninguno existe físicamente** en árbol. El único PDF existente es Clean Architecture en docs/ raíz.

## 7. MAPEO PDFs CONTRA D19 COMPLETO
| D19 | Tema | PDFs relevantes | Evidencia encontrada | Impacto potencial | Estado |
|---|---|---|---|---|---|
| D19-01 | Physical Identity ID | Clean Architecture (docs/Clean Architecture...) | Genérica (estabilidad, boundaries, independencia) | Contextual | CONTEXT ONLY |
| D19-02 | ListingIdentity | Clean Architecture | Boundaries/aggregates (DDD) — genérico | Contextual | CONTEXT ONLY |
| D19-03 | DefinitionVersion | — | Ninguna | Ninguno | CONTEXT ONLY |
| D19-04 | ValidityWindow | — | Ninguna | Ninguno | CONTEXT ONLY |
| D19-05 | VenueConstraints | — | Ninguna | Ninguno | CONTEXT ONLY |
| D19-06 | Evidence/Provenance | — | Ninguna | Ninguno | CONTEXT ONLY |
| D19-07 | Persistence | Clean Architecture | Independencia frameworks/DB — genérico | Contextual | CONTEXT ONLY |
| D19-08 | Backend strategy | Clean Architecture | Ports/adapters, abstracción — genérico | Contextual | CONTEXT ONLY |
| D19-09 | Event/Kafka | — | Ninguna específica | Ninguno | CONTEXT ONLY |
| D19-10 | Domain/App/Port | Clean Architecture | Ports/adapters, Dependency Rule — sólido apoyo | Contextual | SUPPORTING EVIDENCE (genérico) |

## 8. PUNTOS SENSIBLES (8.1–8.7)
- **8.1 Identity ≠ Symbol:** Sin evidencia documental financiera específica en PDFs existentes.
- **8.2 Identity ≠ Definition:** Sin evidencia específica.
- **8.3 Identity ≠ Venue Constraints:** Sin evidencia específica (tick/lot/min notional).
- **8.4 Identity ≠ External Identifier (FIGI/ISIN/MIC):** Sin evidencia específica.
- **8.5 Temporalidad (effective/observed/recorded):** Sin evidencia específica.
- **8.6 Historical stability (FKs/event streams/audit/replay):** Apoyo genérico Clean Architecture (estabilidad referencial) — **CONTEXT ONLY**.
- **8.7 Cross-venue:** Sin evidencia específica.

**Conclusión:** PDFs existentes no aportan evidencia financiera específica para estos puntos. Evidencia relevante para dominio financiero (FIX/FIGI/CME/OpenFIGI/Nautilus/Hummingbot/Freqtrade) **no está presente como PDFs físicos** en docs/knowledge (manifestado pero ausente). 

## 9. RELACIÓN CON INFORME INDUSTRY BEST-OF-WORLDS (D19-01)
Informe anterior comparó CME/FIX/OpenFIGI/NautilusTrader/Hummingbot/Freqtrade. 
**Respecto a PDFs locales:**
- **Evidencia adicional:** Ninguna (PDFs financieros ausentes).
- **Evidencia más específica:** Ninguna.
- **Evidencia contradictoria:** Ninguna detectada (no hay PDFs financieros).
- **Cambia premisa:** No.
- **Confirma lo ya encontrado:** Solo principios arquitectónicos transversales (Clean Architecture).
- **Conceptos nuevos:** Ninguno.

**Conclusión:** docs/knowledge PDFs no aportan evidencia financiera nueva al informe industry best-of-worlds. La evidencia para ese informe proviene de conocimiento de patrones profesionales (documentado en informe previo), no de PDFs locales.

## 10. DETECTAR CONTRADICCIONES
**Entre PDFs entre sí:** No aplicable (solo 1 PDF). 
**Entre PDFs y ADR-0049/D12/HD-1..HD-8/HD-VC-1..7/D19:** Ninguna contradicción. Clean Architecture es compatible con decisiones adoptadas (Hexagonal, boundaries, Dependency Rule, ownership definido).
**Conclusión:** Sin contradicciones detectadas.

## 11. JERARQUÍA DE EVIDENCIA
| Fuente | Autoridad (descriptiva) | Notas |
|---|---|---|
| Clean Architecture (RC Martin) | 6–7 (documentación técnica secundaria en dominio financiero; estándar arquitectónico en general) | Arquitectura software general. Útil transversalmente; NO norma financiera. |
| Manifestados (ausentes): books/papers sobre market microstructure/data engineering | TIER_1–TIER_3 (según manifest) | Potencialmente relevantes para dominio financiero, pero **no presentes físicamente**. No se pueden evaluar. |

**Principio:** distinción evidencia externa vs decisión OCM. Autoridad arquitectónica financiera ausente en PDFs físicos.

## 12. MATRIZ FINAL DE RELEVANCIA
| PDF | Tipo | Autoridad | Temas OCM | D19 afectado | Evidencia clave | Contradicción | Acción |
|---|---|---|---|---|---|---|---|
| docs/Clean Architecture A Craftsman Guide to Software Structure and Design.pdf | Arquitectura software | Arquitectónico general (secundario p/finanzas) | Persistence general, boundaries, ports/adapters, Dependency Rule | D19-02,07,08,10 (contextual); D19-01 débil | Boundaries estables, independencia frameworks, inversión dependencias | Ninguna | CONTEXT ONLY / SUPPORTING EVIDENCE (genérico) |

**Manifest entries ausentes:** 30 (books/papers) — **REQUIRES SOURCE VALIDATION** (archivos no presentes). No evaluables.

## 13. MATRIZ ESPECÍFICA PARA D19-01
| Evidencia documental | Fuente | Qué demuestra | A (opaco) | B (compuesto) | C (opaco+logical key) | Limitación |
|---|---|---|---|---|---|---|
| Boundaries estables, entidades independientes de frameworks | Clean Architecture (RC Martin) | Principios generales (no específicos identidad) | Apoya (estabilidad referencial) | Débil apoyo (requiere cuidado con evolución FK) | Apoya PK opaco estable | Genérico, no aborda identity-defining vs representation-bound |
| Inversión dependencias, ports/adapters | Clean Architecture | Abstracción (registry/ports) | Apoya reverse mapping vía ports | Neutro | Apoya separación PK/UK vía abstracción | No demuestra necesidad C vs A |
| Estabilidad referencial, independencia DB/eventing | Clean Architecture | Diseño para evolución/migraciones | Apoya FK-safe/event-key-safe | Más frágil evolución | Apoya PK estable | No evidencia financiera específica |

**Conclusión:** Solo **evidencia contextual**. No selecciona alternativa. 

## 14. PRUEBA DE 20 CASOS REALES DE OCM (relación con PDFs)
| Caso | Qué identidad necesita | Physical ID | Logical Key | Symbol | native_symbol | External ID | Evidencia documental (PDFs) | Capa responsable |
|---|---|---|---|---|---|---|---|---|
| 1. REST ingestion | ListingIdentity estable (venue-scoped) | Necesario | Opcional | Representación | Representación | Mapping | Ninguna específica (Clean Arch: boundaries/contexto solo) | Adapters→Application→Domain |
| 2. WebSocket | Igual | Necesario | Opcional | Representación | Representación | Mapping | Ninguna específica | Adapters→Application |
| 3. Orderbook | Referencia estable | Necesario | Opcional | Rep. | Rep. | Mapping | Ninguna específica | Market Data (adapters/app/domain) |
| 4. OHLCV | Referencia estable | Necesario | Opcional | Rep. | Rep. | Mapping | Ninguna específica | Market Data |
| 5. Bronze | Referencia histórica estable | Necesario | Opcional | Rep. | Rep. | Mapping | Clean Arch apoyo genérico (persistencia) | Infra/Adapters |
| 6. Silver | ID estable histórico | Necesario | Opcional | Rep. | Rep. | Mapping | Contexto solo | Application/Infra |
| 7. Gold | ID estable | Necesario | Opcional | Rep. | Rep. | Mapping | Contexto solo | Application/Infra |
| 8. Iceberg | ID estable (historical-safe) | Necesario | No canónico | Rep. | Rep. | Mapping | Contexto solo | Infra (lakehouse) |
| 9. Kafka | event key estable | Necesario | No como key | Rep. | Rep. | Mapping | Contexto solo | Infra/events |
| 10. Trading | Referencia estable | Necesario | Opcional | Rep. | Rep. | Mapping | Contexto solo | Trading |
| 11. Execution | Referencia estable | Necesario | Opcional | Rep. | Rep. | Mapping | Contexto solo | Trading/Adapters |
| 12. Risk | Referencia estable | Necesario | Opcional | Rep. | Rep. | Mapping | Contexto solo | Trading/Risk |
| 13. Backtesting | Histórica estable | Necesario | Opcional | Rep. | Rep. | Mapping | Contexto solo | Application/Research |
| 14. Replay | Histórica estable (replay-safe) | Necesario | No canónico | Rep. | Rep. | Mapping | Contexto solo | Infra/Application |
| 15. Portfolio | Referencia estable | Necesario | Opcional | Rep. | Rep. | Mapping | Contexto solo | Portfolio |
| 16. External IDs | Mappings (no canónico) | No | No | Rep. | Rep. | Mapping (externo) | Contexto solo | Adapters/Registry |
| 17. Cross-venue | Relación explícita (no compartido) | Necesario (distintos) | Opcional | Rep. | Rep. | Mapping | Contexto solo | Domain/Relations |
| 18. Historical queries | ID estable (PIT) | Necesario | No canónico | Rep. | Rep. | Mapping | Contexto solo | Infra/Queries |
| 19. API | Referencia estable | Necesario | Opcional | Rep. | Rep. | Mapping | Contexto solo | API/Gateway |
| 20. Operator UI | Display/search | Necesario | Opcional (search) | Rep. | Rep. | Mapping | Contexto solo | API/UI |

**Conclusión:** Casos refuerzan necesidad Physical ID estable (FK/event/historical-safe). PDFs no aportan evidencia específica para elegir A/B/C.

## 15. RESULTADO FINAL

### A. PDFs descubiertos
- **Bajo docs/knowledge:** 0 PDFs físicos (manifest lista 30: books/papers con paths ausentes).
- **Fuera docs/knowledge:** 1 PDF — docs/Clean Architecture A Craftsman Guide to Software Structure and Design.pdf (RC Martin, 2017, 444p).

### B. PDFs realmente relevantes
Solo **Clean Architecture** — relevante transversalmente (arquitectura software). No específico dominio financiero.

### C. Evidencia documental nueva (vs informe industry best-of-worlds anterior)
**Ninguna.** Informe previo se basó en patrones profesionales (CME/FIX/OpenFIGI/Nautilus/Hummingbot/Freqtrade). PDFs locales no aportan evidencia financiera nueva. Único aporte: confirmación principios arquitectónicos transversales.

### D. Evidencia que confirma decisiones existentes
- Identity/Definition separation (soportado por boundaries)
- SymbolBinding/representación independiente (alineado ports/adapters)
- VenueConstraints independiente (alineado boundaries)
- Temporalidad/provenance/fail-closed (principios arquitectura robusta, apoyo genérico)
- Historical stability/auditoría (estabilidad referencial, independencia frameworks)

### E. Posibles contradicciones
Ninguna detectada.

### F. Impacto sobre D19 (D19-01..D19-10)
Contextual/transversal. Principios Clean Architecture refuerzan D19-02,07,08,10 (boundaries/abstracción). Para D19-01 impacto débil (no específico). Resto (D19-03–06,09) sin evidencia específica en PDFs.

### G. Impacto específico sobre D19-01
- **A (opaco):** apoyado por estabilidad referencial + independencia frameworks.
- **B (compuesto):** evolución FK/event/Iceberg más compleja según principios generales.
- **C (híbrido):** PK opaco estable coherente con Clean Architecture; UK lógica requiere disciplina (evitar segundo SSOT).
**Sin evidencia para seleccionar alternativa. Solo trade-offs conceptuales.**

### H. OCM Real-Use-Case Validation
Casos (§14) refuerzan necesidad **Physical ID estable e inmutable** (FK/event/historical-safe). Clean Architecture apoya esto genéricamente. No evidencia específica para elegir A/B/C.

### I. Human Decision Input
Preguntas (desde evidencia existente en PDFs + análisis):
1. ¿Priorizar estabilidad referencial/migraciones (A/C) vs legibilidad directa (B)?
2. ¿Aceptable gobernanza para B (evolución set identity-defining)?
3. ¿Garantizable que C mantiene única referencia canónica = PK opaco (UK nunca FK/event key)?
4. ¿Suficiente reverse mapping vía registry/ports (A/C)?
5. ¿Set identity-defining inicial suficientemente estable para B?

**La existencia de evidencia favorable a alternativa NO constituye selección.**

## 16. ESTADO QUE DEBE PRESERVARSE
- D19-01 = HUMAN DECISION REQUIRED
- D19 = PENDING / BLOCKED
- GATE C = BLOCKED
- IMPLEMENTATION = NOT AUTHORIZED
- DECISION REGISTERED = NO

**Informe NO constituye decisión.** Existencia de evidencia favorable a alternativa NO constituye selección.

## 17. SAFEOPS FINAL (VERIFICACIÓN READ-ONLY)
- ningún archivo modificado: 0
- ningún archivo creado: 0 (informe temporal, luego copiado a repo? Esperado)
- ningún archivo eliminado: 0
- ningún cambio Git: 0 (working tree tracked sin cambios por esta tarea)
- commit/push/merge/rebase/reset/clean/stash: NONE

**Nota:** Se crea informe temporal y se guarda como archivo untracked nuevo (READ-ONLY). 
Working tree: MODTRACKED=17 (preexistentes), STAGED=0, untracked nuevos: 2 (D19_HUMAN_DECISION_PACK_2026-09-30.md preexistente + D19-01_INDUSTRY_BOW_ARCHITECTURE_REVIEW_2026-10-03.md creado + KB report). 
