# Auditoría — Market Data Deep Audit & Tracking Proposal

**Fecha:** 2026-08-14
**Auditor:** OpenAI Codex (asistido por agente de exploración)
**Rol:** Arquitecto Principal de Software · Market Data Engineer · Trading Systems Reviewer
**Alcance:** `packages/market_data` + integraciones de ejecución/riesgo/OMS/portfolio + contrato canónico Kafka (`shared/kafka`)
**Método:** evidencia de código real OCM + patrones de referencia Hummingbot (`docs/data.py`) y Freqtrade (`docs/Untitled_2.py`), con pipeline de decisión *External Pattern → Problema → ¿OCM lo tiene? → ¿Ya resuelto? → ¿Compatible? → Adaptar/Rechazar/Investigar*.
**Regla de honestidad:** todo hallazgo se marca **VERIFIED** (confirmado en código/evidencia directa vista en esta sesión), **UNVERIFIED** (pendiente de comprobar) o **INFERENCE** (deducción razonable, no demostrada). Nada se marca HECHO/correcto sin evidencia directa. `Unknown`/`Needs Investigation` cuando no es verificable.
**Restricciones respetadas:** sin cambios de producción; `tracking.yaml` NO modificado (solo propuestas §14); sin commits/pushes; sin ADRs automáticos.

---

## 1. Resumen ejecutivo

1. OCM tiene una base de market data excepcionalmente disciplinada: dominio framework-agnóstico, eventos inmutables con `event_id`/`occurred_at`/`source` (Kappa), contratos import-linter (BC-NN), fail-fast y fail-closed, dedup multi-nivel, calidad de OHLCV con invariantes y control-plane (gap events), y reconciliación de fills fail-closed.
2. El contrato canónico del wire es **limpio pero mínimo**: `BasePayload` tiene `event_id`, `schema_version`, `occurred_at` (wire) y `timestamp_ms` (dato). **No existe distinción `event_time` vs `received_at` vs `processed_at`** → no se puede medir latencia de ingestión ni detectar datos stale a nivel de evento (F-MD-007).
3. **El order book está producido pero nunca consumido**: hay VOs robustos (`OrderBookSnapshot`/`OrderBookDelta`, bids DESC/asks ASC, qty=0=delete) y productores (Cryptofeed → `orderbook.raw`), pero **cero consumidores** de `TOPIC_ORDERBOOK_RAW` (grep exhaustivo). No hay BookBuilder/MarketState/MicropriceEngine. ADR-0023 + tracking B-25 lo difieren formalmente (F-MD-003).
4. **El trading consume market data SOLO desde Gold (pull, pandas)**: `_GoldFeatureSource` (composition_root.py:136-194) vía port `FeatureSource`; `current_price = last close` del frame gold. **No hay feed de market data en vivo en la ruta de ejecución** — ni tick, ni order book, ni spread, ni última vista de mercado (F-MD-005).
5. **Riesgo de ejecución sin market data**: `RiskManager` valida montos contra capital×size_pct y posiciones abiertas; **no** valida precio stale, bid/ask inexistente, spread, liquidez, slippage, precisión ni min order size. Freqtrade y Hummingbot sí lo hacen (PricingError, min_stake, precision) (F-MD-006).
6. **Gap de trazabilidad temporal**: el wire no transporta `sequence` del order book (cryptofeed lo expone en `book.sequence_number` pero no se propaga), lo que impide gap-detection por secuencia en deltas (F-MD-004).
7. **Pipeline `trades_stream` huérfano**: el builder `_build_trades_stream` existe (pipeline_factory.py) y construye `TradesSourceManager`s, pero **nada los invoca ni los consume** (no conforman `PipelineTriggerPort` — no tienen `.run()`). Código muerto/orfano (F-MD-008).
8. **Strengths a proteger**: dedup 3 niveles (in-memory LRU + Redis durable + `trade_id` en Iceberg), `ExchangeQuirks` en dominio, provenance registry (`shared/kafka/provenance.py`), calidad OHLCV con gap scanner + control-plane, reconciliación fail-closed (ADR-0016).
9. **Freqtrade/Hummingbot** aportan 4 patrones validables: (a) reconciliación centralizada post-fill (`update_trade_state`) — **ya superado** por `fill_sync` fail-closed; (b) validación de precio/mercado antes de envío (PricingError, market validity); (c) instrumentos/limits/precisión como datos de mercado; (d) separación Evento vs Estado con reducer dedicado (OCM ya la tiene en VO pero sin consumer).
10. **Recomendación**: cerrar primero los gaps de **seguridad de ejecución** (freshness contract F-MD-001, pre-submit market validity F-MD-006) y el **BookBuilder** (consumer de orderbook.raw, F-MD-002/003); todo por proceso formal (B-MD-XXX §14 → tracking.yaml → ADR si cambia contrato §13).

---

## 2. Registro de hallazgos

### [F-MD-001] Sin contrato de frescura (freshness/staleness) de market data en vivo
- **Severidad:** P1 (safety — la ejecución no puede detectar un mercado congelado)
- **Estado:** VERIFIED
- **Evidencia:** `shared/kafka/schemas/_base.py` — `BasePayload` solo tiene `event_id`, `schema_version`, `occurred_at`; no hay `last_seen` por símbolo, heartbeat, ni umbral de staleness. Ningún consumidor live calcula edad del último dato. La calidad se mide solo sobre datos persistidos (`application/quality/data_quality.py`, `quality_consumer`).
- **Relación:** sin esto, F-MD-006 no puede rechazar precio stale.

### [F-MD-002] Sin `event_time` / `received_at` / `processed_at` → latencia no medible
- **Severidad:** P3 (observabilidad)
- **Estado:** VERIFIED
- **Evidencia:** `packages/market_data/domain/events/_base.py` `DomainEvent` (`event_id`, `occurred_at`); `shared/kafka/schemas/_base.py` `BasePayload` (`occurred_at`). Ambos usan un único timestamp de creación. No existe la tripleta tiempo-de-evento / tiempo-de-recepción / tiempo-de-proceso que permitiría medir latencia end-to-end de ingestión.
- **Nota:** no rompe `occurred_at` — es un bump de `SCHEMA_VERSION` aditivo (F-MD-009).

### [F-MD-003] Order book producido, jamás consumido (BookBuilder/MarketState inexistente)
- **Severidad:** P1 (correctness — datos producidos sin valor hasta que exista consumer)
- **Estado:** VERIFIED
- **Evidencia:** grep de `TOPIC_ORDERBOOK_RAW` en `packages/` + `apps/` = solo productor (`adapters/outbound/kafka/orderbook_producer.py`). `GROUP_BOOK_BUILDER`/`TOPIC_BOOK_DELTA`/`TOPIC_BOOK_SNAPSHOT` = 0 usos productivos. VOs `OrderBookSnapshot`/`OrderBookDelta` en `domain/value_objects/order_book.py` existen y son correctos (bids DESC / asks ASC, qty=0=delete).
- **Contexto:** ADR-0023 + tracking B-25 (2026-08-09) difieren formalmente gap/DLQ product-side hasta que exista consumidor real. Este hallazgo no contradice el deferral; lo confirma y lo vincula con F-MD-004.

### [F-MD-004] `sequence` del order book capturado pero no propagado al wire
- **Severidad:** P2
- **Estado:** VERIFIED
- **Evidencia:** `cryptofeed_orderbook_stream.py:158-161` captura `book.sequence_number` (cryptofeed lo expone), pero el payload de wire (`shared/kafka/schemas/orderbook.py`) no lo transporta → imposible gap-detection por secuencia en deltas. Sin esto, un BookBuilder (F-MD-002 propuesto) no puede validar continuidad.
- **Relación:** es exactamente el paso previo documentado en B-25 ("sequence aditivo en schema v2").

### [F-MD-005] Trading consume market data solo desde Gold (pull), sin feed en vivo
- **Severidad:** P1 (latencia/representatividad — decisión arquitectónica consciente, riesgo acotado)
- **Estado:** VERIFIED
- **Evidencia:** `packages/trading/engine.py` `run_once` → `_load_data()` → port `FeatureSource.load_features` (`shared/contracts/boundaries.py:30-52`); `_GoldFeatureSource` (`packages/trading/bootstrap/composition_root.py:136-194`) lee Iceberg Gold → pandas; `current_price = last close` del frame. `SyntheticDataSource` para paper. Ningún import/uso de Kafka en `packages/trading`.
- **Nota:** es coherente con el diseño actual (estrategia sobre features). El gap real está en la **ejecución** (F-MD-006), no en el motor de señales.

### [F-MD-006] Ejecución sin validación de mercado pre-envío (stale, bid/ask, spread, liquidez, precisión, min order)
- **Severidad:** P1 (safety — capital real, ver `IS_STUB=False` en `live_executor.py`)
- **Estado:** VERIFIED
- **Evidencia:** `RiskManager` (`packages/trading/risk/manager.py`) solo valida `min/max_order_usd` sobre capital×size_pct y `max_open_positions`; no consulta market data. `LiveExecutor` → `_BybitTransport` (CCXT market order) sin chequeo de spread/stale/liquidez. Freqtrade sí lo hace: `raise PricingError("Could not determine entry price.")` (`docs/Untitled_2.py:1157`), `get_min_pair_stake_amount` (:1183), `price_to_precision`/`amount_to_contract_precision`.
- **Impacto:** un mercado congelado o con bid/ask ausente produciría una orden de mercado a precio desplazado.

### [F-MD-007] Sin instrumentos/límites/precisión como datos de mercado para ejecución
- **Severidad:** P2
- **Estado:** VERIFIED
- **Evidencia:** `ExchangeQuirks` (`packages/market_data/domain/value_objects/exchange_quirks.py`) cubre paginación REST (backward_pagination, requires_end_at, origin_fallback_date) — **no** precision/min_amount/tick/min_cost. `ccxt_adapter.py` carga `load_markets` (con cache), pero esos metadatos no se exponen como contrato de dominio. Freqtrade: `get_precision_amount/price`, `precisionMode` (`docs/Untitled_2.py:390-398,1052-1055`).
- **Relación:** prerrequisito de F-MD-006 (sin precisión/límites no se puede validar la orden).

### [F-MD-008] Pipeline `trades_stream` (WS trades live) huérfano
- **Severidad:** P2 (dead code / capacidad WS trades sin consumir)
- **Estado:** VERIFIED
- **Evidencia:** `_build_trades_stream` (`infrastructure/bootstrap/pipeline_factory.py:86`) construye `TradesSourceManager`s (cursec + dedup LRU cross-source, `application/source_manager.py`), pero: (a) no conforman `PipelineTriggerPort` (no tienen `.run()`, ver `application/use_cases/pipeline_orchestrator.py` que exige `pipeline.run()`); (b) grep de `trades_stream` en `apps/` y `config/` = ∅ → nunca invocado. `TradesPipeline` REST (backfill → Silver) sí está vivo.
- **Nota:** el manager en sí es sólido (cursor monotónico, dedup LRU, fallback REST) — es el cableado lo que falta.

### [F-MD-009] Matriz de decisión de patrones externos (VERIFIED)
- **Severidad:** N/A (documentación de decisión)
- **Estado:** VERIFIED
- **Evidencia:** ver §10. Cada patrón evaluado contra evidencia de `docs/Untitled_2.py` (freqtrade) y `docs/data.py` (hummingbot).

### [F-MD-010] `docs/data.py` y `docs/Untitled_2.py` = material de referencia externa
- **Severidad:** N/A (documentación)
- **Estado:** VERIFIED
- **Evidencia:** `docs/data.py` (81 líneas) = dataclasses Hummingbot (`OrdersProposal`, `PricingProposal`, `SizingProposal`, `PriceSize`, `Proposal`, `HangingOrder`); `docs/Untitled_2.py` (2671 líneas) = fuente `freqtrade` (`FreqtradeBot`). Ambos untracked. No se copia código — solo se extraen conceptos/invariantes (§10). Freqtrade es GPL-3.0; Hummingbot Apache-2.0.
- **Nota:** `docs/data.py` no contiene `MarketData`/`mid_price` — las dataclasses son de la *capa de propuestas* (strategy layer), no de la capa market data de Hummingbot.

---

## 3. Arquitectura actual de Market Data (reconstruida desde código)

### 3.1 Topología de tópicos y flujo (`shared/kafka/topics.py`)

```
Exchange (Bybit/KuCoin)
  ├─ WS: CryptofeedOrderBookStream ──► OrderBookKafkaProducer ──► orderbook.raw   (canary streaming_hydra; SIN consumer)
  ├─ WS: BybitFeedAdapter / KucoinFeedAdapter ──► KafkaTradePublisher ──► trades.raw (SIN consumer confirmado)
  ├─ WS: WSTradesSource (trades_stream — huérfano, F-MD-008)
  └─ REST: OHLCVFetcher ──► ohlcv.raw
ohlcv.raw ─► BronzeWriter (Iceberg Bronze) ─► QualityGate ─► ohlcv.validated
  ─► FeatureEngine ─► ohlcv.features ─► StrategyConsumer ─► signals.raw
  ─► RiskGate ─► signals.approved / signals.rejected ─► ExecutionConsumer ─► OMS
```

Consumidores confirmados: `KafkaBronzeWriter` (ohlcv.raw → Bronze), `QualityConsumer` (lote persistido, fail-soft), `FeatureEngine`, `StrategyConsumer`, `RiskGate`. **Ninguno** consume `TOPIC_TRADES_RAW` ni `TOPIC_ORDERBOOK_RAW`.

### 3.2 Pipeline OHLCV (sólido)

`OHLCVFetcher` (REST, con `ExchangeQuirks`, limiter, throttle, resilience) → `OHLCVBatchReceived` → `ohlcv.raw` → `KafkaBronzeWriter` (Iceberg Bronze, idempotente) → `QualityConsumer` → `ohlcv.validated` → `FeatureEngine` → `ohlcv.features`. Control-plane: `gap_scanner` (OHLCV temporal) → `GapDetected/Healed/Failed` (`gap_events.py`).

### 3.3 Pipeline Trades (REST backfill real, WS live huérfano)

- `TradesBackfillFetcher` + `TradesSourceManager` (cursor monotónico + dedup LRU cross-source) → `TradesPipeline` → Silver Iceberg (`trades_storage.py`, dedup por `trade_id`). **Vivo.**
- `_build_trades_stream` (WS): **huérfano** (F-MD-008).
- `trades.raw` producido por `KafkaTradePublisher` desde adaptadores WS **sin consumidor confirmado** → `Needs Investigation` si apps/research lo consume.

### 3.4 Pipeline Order Book (productor sin consumidor)

Canary `streaming_hydra.py` (ORDERBOOK): `CryptofeedOrderBookStream` → `OrderBookKafkaProducer` (snapshot+delta → `orderbook.raw`). VOs en `domain/value_objects/order_book.py`. Sin BookBuilder (F-MD-003). ADR-0023 difiere gap/DLQ.

### 3.5 Ruta de ejecución (market data = solo Gold pull)

`TradingEngine.run_once()` → `_load_data()` → `FeatureSource.load_features()` (port `shared/contracts/boundaries.py:30-52`) → `_GoldFeatureSource` (Iceberg Gold → pandas) | `SyntheticDataSource` (paper) → `current_price = last close` → strategy → señal → `RiskManager` → `OMSService` → `LiveExecutor` → `_BybitTransport` (CCXT real, `IS_STUB=False`). **No hay consumo de Kafka en trading.**

---

## 4. Evidencia de `docs/` (material de referencia)

| Archivo | Qué es | Estado |
|---|---|---|
| `docs/Untitled_2.py` (2671 líneas) | Fuente de `freqtrade` (clase `FreqtradeBot` + deps) | Untracked, **solo referencia/research** |
| `docs/data.py` (81 líneas) | Dataclasses de Hummingbot (`OrdersProposal`, `PricingProposal`, `SizingProposal`, `PriceSize`, `Proposal`, `HangingOrder`) | Untracked, **solo referencia/research** |
| `docs/knowledge/` (~35 archivos) | KB interna (incl. `notes/bybit-perpetuals-reference.md`, citado por ADR-0025/26/27) | Untracked (reproducibilidad pendiente, ver auditoría 2026-08-13) |

---

## 5. Hallazgos de Hummingbot (`docs/data.py`)

1. **`Proposal` = paquete de órdenes** (entry/exit/order/order_type, `creation_timestamp`): OCM modela señal → orden en `signals.*` + OMS, enfoque por-evento equivalente pero no acoplado a market data.
2. **Separación `PricingProposal` vs `SizingProposal`**: el precio y el tamaño se deciden por separado. OCM: tamaño = f(capital, size_pct) en Risk; precio = señal (gold close). **Gap:** OCM no tiene capa de *pricing de ejecución* (bid/ask mid, spread, TIF).
3. **`HangingOrder`** (órdenes en espera de condición): OCM no tiene equivalente explícito en market data; OMS con órdenes abiertas (contingencias) es parcial.
4. Hummingbot en su núcleo mantiene `MarketData` con **best bid/ask, mid price, order book depth y last trade** como estado en memoria accesible por estrategias. **OCM: no existe en runtime** (F-MD-003, F-MD-005).
5. **Nota de evidencia:** `docs/data.py` NO contiene `MarketData`/`mid_price` en el fragmento visible — la afirmación sobre el núcleo de Hummingbot es `INFERENCE` (conocimiento de la herramienta, no del archivo) y debe verificarse contra la doc oficial de Hummingbot antes de usarse como premisa de diseño.

---

## 6. Hallazgos de Freqtrade (`docs/Untitled_2.py`)

1. **`update_trade_state` (:2339)** — reconciliación post-fill centralizada: `fetch_order_or_stoploss_order` → `trade.update_order` → `check_order_canceled_empty` → `handle_order_fee` → `_update_trade_after_fill` → hook `order_filled` → ajuste stoploss/liquidación → `Trade.commit()`. OCM tiene equivalente fail-closed vía `fill_sync.py` + OMS (`_reconcile_fills`), evento-driven (ADR-0016). **ALREADY EXISTS (diseño superior: fail-closed).**
2. **`PricingError` (:1157)** + `get_min_pair_stake_amount` (:1183) + `price_to_precision`/`amount_to_contract_precision`: Freqtrade **valida el precio contra el mercado y los límites del exchange antes de enviar**. **OCM: gap** (F-MD-006, F-MD-007).
3. **Order book en estrategias**: `fetch_l2_order_book(pair, 1000)` → sums de b_size/a_size → sizing por profundidad (:869-886). **OCM: no hay profundidad disponible para el motor.**
4. **Precisión/instrumentos como datos**: `startup_backpopulate_precision` + `get_precision_amount/price` + `precisionMode` (:390-398, :1052-1055). **OCM: `ExchangeQuirks` cubre paginación, no precisión/límites de ejecución** (F-MD-007).
5. **Patrón que NO replicar**: `FreqtradeBot` es un god-object con cientos de métodos. OCM ya lo supera con clean architecture + BCs.

---

## 7. Gaps de OCM (solo los demostrados)

| ID | Gap | Evidencia | Fase propuesta |
|---|---|---|---|
| G-01 | Sin contracto de frescura (freshness/staleness) en vivo | `_base.py` wire sin last_seen/heartbeat | F-MD-001 |
| G-02 | Sin `event_time`/`received_at`/`processed_at` | `DomainEvent` y `BasePayload` solo `occurred_at` | F-MD-002/007 |
| G-03 | Order book producido, jamás consumido | grep consumidores `orderbook.raw` = ∅; ADR-0023, B-25 | F-MD-003 |
| G-04 | Sin `sequence` en wire del order book | `cryptofeed_orderbook_stream.py:158-161` captura, no propaga | F-MD-004 |
| G-05 | Ejecución usa solo `last close` de Gold | `_GoldFeatureSource`; `run_once` current_price | F-MD-005 |
| G-06 | RiskManager sin controles de market data | `risk/manager.py` solo min/max_order_usd + max_open_positions | F-MD-006 |
| G-07 | Pipeline `trades_stream` huérfano | `pipeline_factory.py:86`; grep `trades_stream` apps/config = ∅ | F-MD-008 |
| G-08 | Sin instrumentos/límites/precisión como datos de ejecución | `exchange_quirks.py` solo paginación | F-MD-007 |
| G-09 | Sin métricas de observabilidad live (latencia, edad, throughput) | Calidad solo sobre datos persistidos | F-MD-002 |

**No-demos (declarados Unknown/Needs Investigation):** pauta de profundidad del book en KuCoin; parity real WS vs REST en producción (feed_orchestrator mode=dual lo advierte); si `trades.raw` es consumido por research (verificar apps/research).

---

## 8. Strengths de OCM

1. **Dominio framework-agnóstico**: zero pandas/polars en `domain/`; migración pandas→polars activa con boundary único en `ohlcv_transformer.py`.
2. **Eventos inmutables con trazabilidad**: `DomainEvent` (`event_id`, `occurred_at`) + `KappaSourceMixin` (`source: live|backfill|replay`).
3. **Dedup multi-nivel determinista**: LRU in-memory (`source_manager._is_duplicate`), Redis durable (L2), `trade_id` en Iceberg.
4. **Calidad de OHLCV con invariantes + control-plane**: vacío, timestamps futuros, gaps ≥2×tf, inconsistencias OHLC, outliers MAD/zscore, flatlines; `gap_scanner` + `GapDetected/Healed/Failed`.
5. **Fail-closed en reconciliación** (ADR-0016, `fill_sync.py`) — superior al `update_trade_state` de Freqtrade.
6. **`ExchangeQuirks` como Value Object de dominio** para paginación/backfill (kucoin backward_pagination, etc.).
7. **Provenance registry** en `shared/kafka/provenance.py` (OrderBook* PROTOCOL/wired; TradeSeries/Liquidation ASSUMED/orphan → señalado como pendiente).
8. **Contratos estrictos**: import-linter BC-NN, schemas versionados, `SCHEMA_VERSION`/`SchemaVersionError`.
9. **No-sobrediseño**: ADR-0023 difiere el BookBuilder en vez de construir al vacío; el canary streaming es explícitamente experimental.

---

## 9. Arquitectura propuesta (conceptual, sin implementación)

```
                    ┌────────────────────────────────────────────┐
                    │           MarketData BC (existente)        │
                    │  ohlcv.raw ─► Bronze ─► Quality ─► features │
                    │  trades.raw / orderbook.raw (producer-only) │
                    └───────────────┬────────────────────────────┘
                                    │ (futuro) MarketState consumer
                                    ▼
                    ┌────────────────────────────────────────────┐
                    │   MarketState / BookBuilder (propuesto)    │
                    │  ─ reducer de deltas + snapshot (seq check)│
                    │  ─ expone: best bid/ask, mid, spread, depth│
                    │  ─ freshness (last_seen, staleness umbral) │
                    └──────────────────┬─────────────────────────┘
                                       │ port (MarketDataView)
                                       ▼
   ┌───────────┐   ┌───────────┐   ┌──────────────────────────┐
   │ Strategy  │   │ RiskGate  │   │ Execution (pre-submit)   │
   │ (signals) │   │ (existing)│   │ market validity checks:  │
   └───────────┘   └───────────┘   │ stale price, spread,     │
                                   │ precision, min order,    │
                                   │ liquidity                │
                                   └──────────────────────────┘
```

Principios de la propuesta (respetando la arquitectura vigente):
- **MarketDataView port** nuevo en `shared/contracts/boundaries.py` (o port en market_data) — el trading **no** se acopla a Kafka; consume vista tipada.
- Reducer de book **dentro del BC market_data** (BookBuilder consumer de `orderbook.raw`), con validación de secuencia y resync por snapshot cuando falte.
- Ejecución añade **checks pre-submit** (stale price vs umbral configurado, spread vs máx, bid/ask presentes, precision/min order del instrumento) — fail-closed.
- Freshness como **métrica de control-plane** (last_seen por símbolo, alerta de staleness), no solo datos persistidos.
- Tiempo: añadir `received_at`/`processed_at` (envelope) **sin romper** `occurred_at` (backward-compatible, schema_version bump).

**No implementar en esta fase.** Es dirección para ADR (A-MD-XXX, §13).

---

## 10. Matriz de capacidades

| Capacidad | Hummingbot | Freqtrade | OCM | Nota |
|---|---|---|---|---|
| Ingesta OHLCV histórico (REST, paginación quirks) | Confirmed | Confirmed | **Confirmed** | `ExchangeQuirks`, backfill |
| Ingesta OHLCV live (WS) | Confirmed | Partial | **Partial** | canary streaming; ohlcv.raw live solo canary |
| Ingesta Trades live (WS) | Confirmed | Confirmed | **Partial** | `trades_stream` huérfano (F-MD-008); `trades.raw` productor sin consumer |
| Order book depth en vivo | Confirmed | Confirmed | **Partial** | producido, sin consumer (F-MD-003) |
| Best bid/ask / mid / spread disponible a estrategias | Confirmed | Confirmed | **Not Found** | F-MD-005 |
| BookBuilder / reconstructor (seq check, resync) | Confirmed | Partial | **Not Found** | ADR-0023 deferido (F-MD-003) |
| Freshness / staleness contract | Confirmed | Confirmed | **Not Found** | F-MD-001 |
| event_time/received_at/processed_at | Partial | Partial | **Not Found** | F-MD-002 |
| Data quality invariantes OHLCV | Confirmed | Partial | **Confirmed** | superior (MAD/zscore, control-plane) |
| Gap detection (OHLCV temporal) | Confirmed | Partial | **Confirmed** | `gap_scanner` |
| Gap detection por secuencia (order book) | Confirmed | Partial | **Not Found** | F-MD-004 |
| Precio de ejecución validado (PricingError) | Confirmed | Confirmed | **Not Found** | F-MD-006 |
| Min order size / precisión como datos | Confirmed | Confirmed | **Not Found** | F-MD-007 |
| Reconciliación de fills fail-closed | Partial | Partial | **Confirmed** | ADR-0016 (superior) |
| Dedup multi-nivel | Partial | Partial | **Confirmed** | LRU+Redis+Iceberg |
| Provenance (live/backfill/replay) | Not Applicable | Not Applicable | **Confirmed** | KappaSourceMixin |
| Límites de profundidad para sizing | Confirmed | Confirmed | **Not Found** | F-MD-005 |

---

## 11. Adopt / Adapt / Reject / Investigate

| Fuente | Patrón | Decisión | Justificación |
|---|---|---|---|
| Freqtrade | `update_trade_state` reconciliación central | **ALREADY EXISTS** | OCM `fill_sync` + OMS fail-closed (ADR-0016) es superior |
| Freqtrade | Validación de precio/mercado antes de envío (PricingError, min_stake, precision) | **ADOPT (adaptar)** | OCM necesita pre-submit market validity; implementar vía port MarketDataView + config, no copiar lógica |
| Freqtrade | Instrumentos/límites/precisión del exchange como datos | **ADAPT** | Extender el concepto `ExchangeQuirks` (dominio) a precision/min_amount/tick para ejecución |
| Hummingbot | `PricingProposal`/`SizingProposal` separados | **INVESTIGATE** | OCM separa tamaño (Risk) de precio (señal) por eventos; falta pricing de ejecución → ver si justifica capa propia |
| Hummingbot/Freqtrade | `fetch_l2_order_book` en estrategia | **REJECT (forma)** | OCM no debe llamar al exchange desde estrategias; si se necesita, exponer vía MarketDataView port |
| Hummingbot | Order book depth para sizing de órdenes | **ADAPT** | Solo tras BookBuilder; sizing por profundidad entra por Risk vía port |
| Freqtrade | God-object `FreqtradeBot` | **REJECT** | OCM clean architecture + BCs ya lo resuelve |
| Ambas | `event_time/received_at/processed_at` | **ADAPT** | Extender envelope con compatibilidad de schema_version |
| Ambas | Heartbeat/last_seen de feeds | **ADOPT** | Control-plane de frescura en market_data |

---

## 12. Clasificación A–E (de hallazgos)

- **A (Resuelto/correcto):** VOs order book, dedup multi-nivel, calidad OHLCV, reconciliación fail-closed, provenance, ExchangeQuirks, contrato event_id/occurred_at/source.
- **B (Mejora / deuda técnica):** F-MD-008 trades_stream huérfano (eliminar o cablear); schema wire sin sequence; provenance TradeSeries/Liquidation ASSUMED.
- **C (Inconveniente / riesgo):** F-MD-001 frescura, F-MD-005/F-MD-006 ejecución sin market data, F-MD-002 latencia no medible.
- **D (Adición futura):** F-MD-003 BookBuilder, F-MD-007 instrumentos de ejecución, F-MD-004 seq-check.
- **E (No aplica / rechazado):** god-object FreqtradeBot; fetch_l2 en estrategia; copiar PricingProposal literal.

---

## 13. Items que requieren ADR

- **A-MD-001** — Evolución del contrato canónico de market data: añadir `sequence` (order book) y `received_at`/`processed_at` (envelope) con compatibilidad `SCHEMA_VERSION`. *Vía ADR porque modifica contratos de wire.*
- **A-MD-002** — BookBuilder/MarketState en BC market_data como primer consumidor de `orderbook.raw` + nuevo port `MarketDataView` expuesto a trading/portfolio. *Vía ADR: nuevo componente + nuevo port inter-BC.*
- **A-MD-003** — Política de fallo de ejecución ante market data ausente/stale (fail-closed por defecto; modo degrade explícito y auditable). *Vía ADR: política de riesgo.*

---

## 14. Propuestas de tracking (formato B-MD-XXX) — NO aplicadas a tracking.yaml

> Formato alineado con `docs/plans/tracking.yaml` (id / hallazgo_informe / fase / prioridad / evidencia). No se insertaron en el YAML: pendiente de aprobación humana.

| ID | hallazgo | Fase | Prioridad | Problema (evidencia) | Resultado esperado | Aceptación |
|---|---|---|---|---|---|---|
| **B-MD-001** | F-MD-001 | F2 | ALTA (safety) | Sin last_seen/heartbeat/umbral staleness; ejecución no detecta mercado congelado | Métricas last_seen por símbolo + umbral configurable + alerta; pre-submit rechaza precio stale | Pre-submit falla-closed si dato > umbral; métrica en Prometheus |
| **B-MD-002** | F-MD-003 | F2 | ALTA (correctness) | `orderbook.raw` sin consumidor; sin estado de mercado | Consumer BookBuilder que reduce deltas→snapshot validando secuencia; expone best bid/ask/mid/spread/depth | Snapshot+delta reconstruyen el mismo book; salto de seq → resync; cubierto por tests |
| **B-MD-003** | F-MD-004 | F2 | ALTA | cryptofeed expone `book.sequence_number`, no propagado | `sequence` en payload order book + `SCHEMA_VERSION` bump | Consumidores legacy siguen validando; sequence presente |
| **B-MD-004** | F-MD-006 | F2 | ALTA (safety) | Risk no valida mercado; solo capital×size_pct | Checks fail-closed pre-envío (stale, bid/ask, spread máx, min order, precisión) con umbrales de config | Enviar orden requiere mercado válido + spread ≤ máx; tests de cada check |
| **B-MD-005** | F-MD-007 | F2 | MEDIA | ExchangeQuirks solo cubre paginación REST | VO de instrumento (precision, min_amount, tick, min_cost) por exchange/symbol | Risk/Execution usan límites reales del exchange |
| **B-MD-006** | F-MD-008 | F2 | MEDIA | Builder `trades_stream` sin invocación ni consumer | O bien consumer de trades.raw, o eliminación del builder | Sin código muerto; cobertura de tests actualizada |
| **B-MD-007** | F-MD-002 | F3 | BAJA | Sin latencia medible end-to-end | `received_at`/`processed_at` en envelope (schema_version bump) | Latencia de ingestión visible en Prometheus |

Dependencias: B-MD-002 ← B-MD-003 (sequence primero o en el mismo cambio). B-MD-004 ← B-MD-001 + B-MD-005. B-MD-001 independiente. B-MD-006 independiente. B-MD-007 independiente.

Riesgos: B-MD-003/B-MD-007 tocan wire → requieren ADR de contrato (A-MD-001). B-MD-002 nuevo consumer en BC market_data sin cambio de contrato externo. B-MD-004 toca trading/execution (zona sensible, pruebas obligatorias).

---

## 15. Items explícitamente rechazados

- Copiar el god-object `FreqtradeBot` ni `update_trade_state` (OCM ya es fail-closed event-driven).
- Llamadas `fetch_l2_order_book`/`fetch_ticker` directas desde estrategias o Risk (viola DIP/BC).
- Copiar `PricingProposal`/`SizingProposal` de Hummingbot como dataclasses de proposición (OCM usa señal→orden por eventos).
- Convertir `docs/data.py`/`docs/Untitled_2.py` en arquitectura aprobada — son research únicamente.

---

## 16. Archivos inspeccionados

**Market data (dominio):** `packages/market_data/domain/events/_base.py`, `events/ingestion.py`, `events/orderbook_events.py`, `events/trade_events.py`, `events/gap_events.py`, `events/_lineage.py`, `value_objects/order_book.py`, `value_objects/candle.py`, `value_objects/raw_trade.py`, `value_objects/exchange_quirks.py`, `quality/invariants.py`, `exceptions/__init__.py`.
**Market data (aplicación):** `application/use_cases/pipeline_orchestrator.py`, `application/source_manager.py`, `application/feed_orchestrator.py`, `application/quality/data_quality.py`, `application/quality/pipeline.py`, `application/processing/gap_scanner.py`.
**Market data (adapters/infra):** `adapters/inbound/websocket/ws_trades_source.py`, `adapters/inbound/websocket/cryptofeed_orderbook_stream.py`, `adapters/outbound/exchange/ccxt_adapter.py`, `adapters/outbound/exchange/exchange_quirks.py`, `adapters/outbound/kafka/orderbook_producer.py`, `adapters/outbound/storage/silver/trades_storage.py`, `adapters/outbound/storage/gold/gold_reader.py`, `infrastructure/bootstrap/composition_root.py`, `infrastructure/bootstrap/pipeline_factory.py`.
**Trading/ejecución:** `packages/trading/engine.py`, `packages/trading/bootstrap/composition_root.py` (`_GoldFeatureSource`, `_BybitTransport`, `assemble_live`), `packages/trading/execution/live_executor.py`, `packages/trading/execution/oms.py`, `packages/trading/execution/fill_sync.py`, `packages/trading/risk/manager.py`, `apps/app/use_cases/execute_live.py`.
**Shared/Kafka:** `shared/kafka/topics.py`, `shared/kafka/schemas/_base.py`, `shared/kafka/schemas/orderbook.py`, `shared/kafka/schemas/trades.py`, `shared/kafka/provenance.py`, `shared/contracts/boundaries.py`.
**Docs/referencia:** `docs/Untitled_2.py` (freqtrade), `docs/data.py` (hummingbot), `docs/plans/tracking.yaml` (solo lectura: B-25, F-009, B-46), `docs/architecture/decisions/ADR-0023` y ADR-0025/26/27.
**Auditorías previas (convenciones de formato):** `docs/audits/2026-08-08-streaming-canary-audit.md`, `docs/audits/2026-08-apps-audit.md`, `docs/audits/2026-05-market-data-audit.md`.

---

## 17. Archivos modificados y estado de Git

**Archivos modificados en este informe:** ninguno de producción, `tracking.yaml` ni tests. **Solo este archivo de auditoría** (`docs/audits/2026-08-14-market-data-deep-audit.md`) se crea como registro de resultados.

**Estado Git (sin cambios de este trabajo):**
- HEAD = `aa547b3` (`refactor(research): F-1 — composition root para research (DIP)`), en `main`.
- **Modificados (working tree, previos):** `AGENTS.md`, `README.md`, `apps/app/use_cases/execute_live.py`, `docs/audits/2026-08-apps-audit.md`, `tests/architecture/test_import_contracts.py` (incluyen las correcciones de la auditoría de documentación 2026-08-13/14: README y docstring de execute_live).
- **Untracked:** `docs/Untitled_2.py`, `docs/data.py`, `docs/architecture/decisions/ADR-0024-*.md`, `docs/audits/2026-08-13-*.md` (2), `docs/knowledge/`.
- **Sin commits ni pushes** en esta sesión.

**Pendiente de decisión humana (no bloqueante):** estatus de `ADR-0024`, publicación/tracking de `docs/knowledge/`, y aprobación de las propuestas B-MD-XXX (§14) para incorporarlas a `docs/plans/tracking.yaml` y abrir los ADR A-MD-001/2/3 (§13).
