# ADR-0041 — Canonical Market Data Contract

**Estado:** Aceptado
**Fecha:** 2026-09-24
**Bounded context(s) afectado(s):** market_data

> Propuesta original 2026-09-21; **aceptada en Fase 0 (Human Decision Gate) 2026-09-24**,
> opción A1 — ADR independiente que extiende ADR-0013, scope B (datasets con wire),
> sobre común + identidad por dataset, purpose como metadata. Ver §5 y §5.1.

## 1. Contexto

OrangeCashMachine tiene múltiples datasets (OHLCV, Trades, Ticker, OrderBook Snapshot/Delta, Funding, Open Interest, Liquidaciones) que pueden llegarse por mecanismos diferentes (Exchange REST, Exchange WebSocket, ambos). La auditoría maestre ha identificado que actualmente:

- `OHLCV ≠ REST` y `OrderBook ≠ WebSocket` — DATASET y TRANSPORT son conceptos distintos
- No existe un contrato canónico que una dataset, exchange, transport y propósito
- El maestro de Muse confirmó: "WS live OHLCV candles = YES pero OCM no tiene adapter OHLCV WS"
- El Big Pickle audit identificó drift de schema en Bronze (11 vs 12 columnas, run_id provenance)
- OrderBook issues son independientes de Bronze/schema drift

El problema arquitectónico: sin un contrato canónico, la integración entre datasets, transports y exchanges es ad hoc y propensa a inconsistencias.

## 2. Problema

Determinar si OCM necesita un contrato canónico común que defina:

- Dataset × Exchange × Transport × Propósito × Identidad de evento × Deduplicación

Para: OHLCV, OrderBook, Trades, Funding, OI, Liquidations (scope B — los que tienen
wire schema vigente; ver §5). Ticker/On-chain quedan fuera hasta que exista wire.

## 3. Drivers de decisión

Las siguientes propiedades importan:

- **fiabilidad**: un contrato unívoco evita interpretación errónea de datos
- **reproducibilidad**: los mismos inputs deben producir los mismos outputs
- **determinismo**: el flujo de datos debe ser predecible
- **seguridad operativa**: prevenir datos corruptos o duplicados en el lakehouse
- **observabilidad**: rastreo de lineage desde exchange hasta Gold
- **idempotencia**: procesamiento seguro de reenvíos y gaps
- **maintainability**: cambios en un transport no rompen al otro
- **deployment independence**: el contrato no depende de la forma de despliegue
- **recuperación correcta**: DETECTION ≠ RECOVERY, y el contrato debe apoyar ambos

## 4. Consideradas alternativas

**A. Sin contrato canónico** (estado actual)
- *Ventaja*: Sin sobrecarga de definir e imponer un contrato nuevo
- *Desventaja*: Interpretación inconsistente de datos; gaps de deduplicación; impossibilidad de validar lineage; integración ad hoc por exchange; impossible validar DETECTION ≠ RECOVERY a nivel de contrato

**B. Contrato canónico SOLO para OHLCV**
- *Ventaja*: Aborda el problema inmediato de OHLCV live/historical/recovery
- *Desventaja*: Deja OrderBook y otros datasets sin contrato unificado; inconsistency persists across datasets; no resuelve el problema dataset×transport general

**C. Contrato canónico para TODOS los datasets de OCM**
- *Ventaja*: Unified contract para todos los datasets; línea de lineage consistente; deduplicación consistente; validación DETECTION ≠ RECOVERY a nivel de contrato
- *Desventaja*: Requiere definición inicial más compleja; necesita adaptación de todos los adapters y producers; requiere decisión sobre identidad de evento y deduplicación

## 5. Decisión

**Adoptar un contrato canónico de Market Data para OCM**, aceptado en Fase 0
(2026-09-24, opción A1), que define:

- **Relación con ADR-0013 (extensión, no sustitución):** ADR-0013 sigue siendo
  normativo para feed / fuente / mecanismo, "Kafka como SSOT operacional" y
  Bronze/Silver/Gold como proyecciones materializadas. ADR-0041 extiende al nivel
  de **evento**: envelope, identidad y propósito. No reabre ni redefine los
  mecanismos ni el protocolo que gobierna ADR-0013.
- **Dataset (scope B — datasets con wire schema vigente):** OHLCV, OrderBook,
  Trades, Funding, Open Interest, Liquidations. **Ticker y On-chain quedan FUERA
  de este contrato** hasta que exista wire schema y productor real (hoy ABSENT).
- **Transport:** REST, WebSocket, Ambos (concepto ortogonal al dataset, ADR-0013).
- **Purpose:** metadata del envelope (source = live/backfill/replay hoy; extensible
  a recovery/reconciliation cuando existan). El propósito NO forma parte de la
  identidad de evento: la deduplicación/idempotencia no dependen del propósito.
- **Identidad de evento:** sobre común (envelope) + **identidad de negocio
  específica por dataset** (ver §5.1). NO se adopta una identidad universal
  sintética (opción A4 rechazada).
- **Deduplicación:** política ya decidida por B-19 (at-least-once + dedup L1/L2,
  mark-after-write). Este ADR la referencia, no la redefine.
- **Kafka:** estructura de topic/key/partition gobernada por `shared/kafka/topics.py`
  (SSOT) y BC-35 — este ADR no la redefine.
- **Bronze/Silver/Gold:** proyecciones materializadas (ADR-0013); este contrato no
  redefine sus esquemas.

## 5.1. Elementos del contrato y su clasificación (Fase 0, 2026-09-24)

| Elemento | Clase | Resolución |
|---|---|---|
| envelope común | EXTENSION | `_base.py` ya define campos + reglas de evolución; este ADR formaliza la norma |
| event identity (event_id del sobre) | NEW DECISION | sobre común; `_base.py` ya lo porta; se formaliza |
| business identity (clave de dominio) | NEW DECISION | específica por dataset (tabla §5.1.1) |
| dedup | ALREADY DECIDED | B-19 (at-least-once + L1/L2) |
| schema evolution | ALREADY DECIDED | `_base.py` (aditivo / breaking) + BC-35 |
| purpose/source | EXTENSION | source=live/backfill/replay ya en el wire; recovery/reconciliation NO están en el enum (abierto) |
| routing invariants | ALREADY DECIDED | `topics.py` (topic/group/retention/routing key) |
| ordering/sequence | UNRESOLVED | solo garantizado por dataset donde existe (`u` en OrderBook); orden por-stream no universal = deuda explícita |
| B/S/G projection invariants | ALREADY DECIDED | ADR-0013 (proyecciones); NO se crea "canonical Bronze" |
| lineage de ADR-0014 | OUT OF SCOPE | preservado; este contrato no lo redefine |

### 5.1.1. Identidad de negocio por dataset

| Dataset | Business identity | Ordenación | Notas |
|---|---|---|---|
| OHLCV (batch vs candle) | candle = (exchange, symbol, timeframe, ts) | cronológica | batch porta event_id de sobre; candle sin ID nativo robusto (gap conocido) |
| OrderBook | `u` (update_id / cross_seq) | `u` monotónico | u-jump sin reconnect y snapshot REST siguen en ADR-0028/0043 |
| Trades | `trade_id` | exchange-ts | ID nativo |
| Funding | snapshot-time | period | ID derivado del instante |
| OI | snapshot-time | period | ID derivado del instante |
| Liquidations | **sin native ID** | ts | requiere ID sintético dentro del sobre |

## 6. Consecuencias

### Consecuencias positivas

- Línea de lineage consistente desde exchange hasta Gold
- Deduplicación consistente across transports
- Validación DETECTION ≠ RECOVERY a nivel de contrato
- Integración más fácil de nuevos exchanges o transports
- Mejor observability y lineage tracking
- Base para contratos BC-NN ycontratos de bounded context

### Consecuencias negativas

- Sobrecarga inicial de definir y documentar el contrato
- Requiere adaptación de los producers/adapters de los 6 datasets con wire (scope B)
- Ticker/On-chain quedan fuera hasta tener wire (deuda explícita, documentada)
- El orden por-stream no universal (ordering/sequence) queda como deuda UNRESOLVED
  hasta que un consumidor lo exija (p. ej. estrategias)

### Consecuencias operacionales

- Los adapters REST y WS deben cumplir el contratocanónico
- Los producers Kafka deben publicar según el contrato
- Los consumers Bronze/Silver/Gold deben esperar el contrato
- Los gaps de recuperación deben validarse contra el contrato
- Los eventos tardíos y duplicates deben manejarse según el contrato

## 7. Alternativas Rechazadas

- **Sin contrato canónico**: rechazado porque perpetúa inconsistencias identificadas en la auditoría maestre
- **Contrato SOLO OHLCV**: rechazado porque deja OrderBook y otros datasets sin unificación; la auditoría confirma que dataset×transport es un problema general, no solo OHLCV
- **Enmienda de ADR-0013 (A3)**: rechazado porque ADR-0013 es un ADR aceptado y
  de principios; meter contrato de evento dentro de él degrada su alcance.
- **Identidad universal sintética (A4)**: rechazado — no soportado por la
  evidencia (no hay clave común entre los 6 datasets) y añade riesgo de dedup.
- **Defer completo (A5)**: rechazado — dejaría validated/gold wiring (Fase 3)
  sin marco formal.
- **Absorción en ADR-0013 (C)**: rechazado — ADR-0013 es "Principia"; absorberlo
  degrada un ADR ya aceptado.

## 8. Alcance

- **Dentro**: definición del contrato canónico (envelope + identidad por dataset);
  formalización de purpose/source como metadata; clasificación de elementos ya
  decididos vs extensiones vs nuevas decisiones (ver §5.1); políticas de
  deduplicación existentes referenciadas (B-19).
- **Fuera**: implementación de adapters REST/WS; migración de datos existente;
  corrección de schema drift en Bronze (tarea separada); implementación de
  recuperación de gaps (Fase 3); BookBuilder completo (ADR-0028); resync OrderBook
  (ADR-0043); recovery financiero (ADR-0027); lineage de ADR-0014; mecanismo y
  protocolo (ADR-0013).

## 9. Dependencias

- ADR-0013 (modelo unificado de ingestión) — **extendido por este ADR, no
  sustituido**; relación resuelta en Fase 0 (2026-09-24).
- Wire schemas en `shared/kafka/schemas/` (BC-35) — SSOT de los fields.
- `topics.py` — SSOT de routing/particiones.
- `provenance.py` — clasificación real de cada schema (CONFIRMED/DOCUMENTED_ONLY/ASSUMED).
- ADR-0014 (Proposed) — lineage; OUT OF SCOPE para este contrato.

## 10. Frontera de implementación

Esta decisión define el contrato; la implementación (adaptadores, producers, consumers) es una fase posterior. La frontera indica:

- **Definition**: contrato canónico documentado
- **Pending**: adaptación de los adapters/producers de los 6 datasets del scope B
- **Future**: validación runtime del contrato; evolución de schema controlada; incorporación de Ticker/On-chain cuando tengan wire

## Referencias

- AUDIT_OCM_master-market-data-architecture-reconciliation_2026-09-21.md
- AUDIT_OCM_dataset-continuity-recovery-reconciliation_2026-09-21.md
- AUDIT_OCM_market-data-e2e-incompleteness_2026-09-21.md
- AUDIT_OCM_iceberg-bronze-ohlcv-runid-provenance_2026-09-21.md
- AUDIT_OCM_comparative-architecture-refs_2026-09-21.md (Hummingbot/Nautilus/Freqtrade)
- ADR-0013 — modelo unificado-ingestion-datos
- ADR-0027 — recovery-source-of-truth
