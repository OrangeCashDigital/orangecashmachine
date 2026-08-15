# Auditoría — Market Data · Position · Execution · Risk + Diseño BookBuilder/MarketState

**Fecha:** 2026-08-15
**Auditor:** OpenAI Codex (asistido por agente de exploración)
**Rol:** Arquitecto Principal de Software · Market Data Engineer · Trading Systems Reviewer
**Alcance:** `packages/market_data` (order book, gap control-plane, composición root, canary ORDERBOOK) + `packages/trading` (OMS, execution, risk, fill-sync, settlement) + `packages/portfolio` (position) + contrato canónico Kafka (`shared/kafka`) + puertos (`ports/inbound`, `ports/outbound`) + contratos import-linter (`architecture/importlinter.toml`) + ADR-0023 (deferral gap detection).
**Método:** evidencia de código real OCM (verificada en esta sesión, con `archivo:línea`) + patrones de referencia Hummingbot (`docs/hummingbot-master.zip`, extraído a `/tmp/opencode/hummingbot/hummingbot-master/`) + pipeline de decisión *External Pattern → Problema → ¿OCM lo tiene? → ¿Ya resuelto? → ¿Compatible? → Adaptar/Rechazar/Investigar*. Hummingbot es GPL — solo se extraen patrones en prosa + firmas de interfaz propuestas, nunca código copiado.
**Regla de honestidad:** todo hallazgo se marca **VERIFIED** (confirmado en código/evidencia directa vista en esta sesión), **UNVERIFIED** (pendiente de comprobar) o **INFERENCE** (deducción razonable, no demostrada). Nada se marca HECHO/correcto sin evidencia directa. Los marcadores **DOCUMENTATION DRIFT** y **IMPLEMENTATION DRIFT** señalan diferencias entre lo que la documentación afirma y lo que el código realmente hace.
**Restricciones respetadas:** sin cambios de producción; `tracking.yaml` NO modificado (solo propuestas §17); sin commits/pushes; sin ADRs automáticos (el diseño BookBuilder va como **propuesta de ADR** en `docs/architecture/decisions/ADR-0028-draft-bookbuilder-marketstate.md`, Estado=Propuesto, sin aprobar). La fase 1 del roadmap es **solo market_data**: el diseño BookBuilder no toca `trading/` ni `portfolio/`.

---

## 1. Resumen ejecutivo

1. **El BookBuilder es la pieza central que desbloquea 3 gaps abiertos (P1/P2).** `orderbook.raw` se produce (canary `streaming_hydra.py`, `OrderBookKafkaProducer`) pero **cero consumidores** (`grep` exhaustivo, VERIFIED). ADR-0023 + tracking B-25 lo difieren formalmente *hasta que exista consumidor real*. Proponer el BookBuilder activa: B-25 (gap detection), F-MD-004 (sequence al wire), F-MD-003 (consumer de orderbook.raw) y el puerto de lectura que F-MD-006/B-MD-004 necesitan.
2. **El orden correcto es: primero `sequence` en el wire (B-MD-003/ADR A-MD-001), luego el BookBuilder (B-MD-002), luego la validación de mercado pre-envío (B-MD-004).** El BookBuilder **no puede** detectar gaps ni recuperar continuidad sin `sequence`/`first_update_id` en el wire — el patrón Hummingbot `order_book_message.py` (update_id / first_update_id) lo exige.
3. **El control-plane de gaps YA existe y debe reutilizarse, no reinventarse.** `GapDetectedEvent`/`GapHealedEvent`/`GapFailedEvent` (`domain/events/gap_events.py`) + port `GapEventPublisherPort` + `KafkaGapPublisher` → topic `market.gaps`. Semántica actual = OHLCV temporal (symbol/timeframe). El BookBuilder mapea su gap de *secuencia* sobre este mecanismo (misma triada Detected/Healed/Failed, campos de secuencia).
4. **Existen temas reservados y grupos de consumidor ya declarados que el diseño aprovecha sin tocar.** `TOPIC_BOOK_SNAPSHOT="book.snapshot"`, `TOPIC_BOOK_DELTA="book.delta"`, `GROUP_BOOK_BUILDER="ocm-book-builder"` (`shared/kafka/topics.py:117-120,179`) — 0 usos productivos hoy (VERIFIED). El diseño los hace productivos.
5. **El puerto de lectura del estado reconstruido es un port OUTBOUND nuevo en market_data (propuesta `MarketDataViewPort`), no un contrato en `shared/` ni una importación desde trading.** Sigue la dirección de dependencia `trading → market_data` (ya existente vía `FeatureSource` en `shared/contracts/boundaries.py`); para Fase 1 no se conecta ningún consumidor en `trading/`.
6. **DOCUMENTATION DRIFT verificado:** no existe el stub `orderbook_stream.py` con `NotImplementedError` en infrastructure. Los únicos `NotImplementedError` están en `adapters/inbound/rest/derivatives_fetcher.py:209,212`, `application/pipeline/runtime.py:468`, `ports/outbound/exchange.py:142,165`. El flujo real del book es `cryptofeed_orderbook_stream.py` (ACL) → `OrderBookKafkaProducer` → `orderbook.raw`.
7. **El wire NO transporta sequence hoy** (VERIFIED): `OrderBookSnapshotPayload`/`OrderBookDeltaPayload` (`shared/kafka/schemas/orderbook.py`) no tienen campo de secuencia; `cryptofeed` sí lo expone (`book.sequence_number`, documentado en `cryptofeed_orderbook_stream.py:158-161`).
8. **El patrón de consumidor Kafka del proyecto ya está establecido** (`_bronze_writer_loop` en `packages/market_data/main.py:222-307`, `KafkaConsumerAdapter.for_bronze_writer()`, `KafkaBronzeWriter` con DLQ + dedup durable L2). El BookBuilder reutiliza exactamente este patrón, no inventa uno nuevo.
9. **Estrategia de estado en memoria recomendada: Kafka-replay del snapshot, no Hummingbot in-process PubSub ni Redis.** Hummingbot usa buffering en proceso (`_past_diffs_windows` deque en `order_book_tracker.py`) porque su productor y consumidor viven en el mismo proceso. OCM separa productor/consumidor por Kafka → el buffer de diffs pendientes debe vivir en el consumidor (BookBuilder) con la tripleta *snapshot de arranque + replay desde offset + ventana de diffs* para cubrir la reconexión.
10. **Live-readiness sigue bloqueada (P0):** sin B-MD-001 (freshness), B-MD-002 (BookBuilder) y B-MD-004 (market validity) el `LiveExecutor` envía órdenes de mercado CCXT sin validar bid/ask/spread/staleness → **¿Pondrías dinero real mañana? → NO**.

---

## 2. Registro de hallazgos

> Los hallazgos F-MD-001..010 del `2026-08-14-market-data-deep-audit.md` se asumen vigentes (mismos VERIFIED). Esta sesión añade los siguientes, verificados contra el código actual.

### [F-MD-011] DOCUMENTATION DRIFT — no existe el stub `orderbook_stream.py` con NotImplementedError
- **Severidad:** P3 (documentación engañosa)
- **Estado:** VERIFIED
- **Evidencia:** grep global de `NotImplementedError` en `packages/market_data`: solo `adapters/inbound/rest/derivatives_fetcher.py:209,212` (paginación no soportada), `application/pipeline/runtime.py:468` (pipeline.run no soportado), `ports/outbound/exchange.py:142,165` (métodos abstractos del port). **No hay `orderbook_stream.py`** en infrastructure ni adapters. El flujo real del book es `adapters/inbound/websocket/cryptofeed_orderbook_stream.py` (ACL cryptofeed L2_BOOK) → `OrderBookKafkaProducer` (`adapters/inbound/websocket/orderbook_producer.py`).
- **Contexto:** la suposición de un "NotImplementedError stub en mi propio historial" no corresponde a la realidad actual del código.

### [F-MD-012] El wire de order book no transporta `sequence` ni `first_update_id`
- **Severidad:** P2 (bloquea gap-detection del BookBuilder)
- **Estado:** VERIFIED
- **Evidencia:** `shared/kafka/schemas/orderbook.py` — `OrderBookSnapshotPayload` (exchange, symbol, timestamp_ms, bids, asks, depth, checksum) y `OrderBookDeltaPayload` (exchange, symbol, timestamp_ms, side, price, size). Sin campo de secuencia. `cryptofeed_orderbook_stream.py:158-161` documenta que `book.sequence_number` se asigna al callback de cryptofeed pero NO se propaga al payload (`orderbook_producer.py::on_snapshot/on_delta` no reciben ni emiten sequence).
- **Relación:** es el paso previo exacto de B-25 ("sequence aditivo en schema v2") y el prerrequisito del BookBuilder (F-MD-002/003). Coincide con F-MD-004 del audit anterior.

### [F-MD-013] Control-plane de gaps (GapDetected/Healed/Failed) ya existe y es OHLCV-temporal
- **Severidad:** N/A (base de diseño)
- **Estado:** VERIFIED
- **Evidencia:** `domain/events/gap_events.py` — `GapDetectedEvent(symbol, timeframe, exchange_id, start_ms, end_ms, expected, detected_at_ms)`, `GapHealedEvent(rows_written, fill_ratio)`, `GapFailedEvent(reason, is_transient)`. Port `ports/outbound/gap_event_publisher.py` (GapEventPublisherPort). Emisor: `RepairStrategy`; consumidor: `KafkaGapPublisher` → topic `market.gaps`. Semántica = huecos temporales OHLCV en Silver.
- **Implicación de diseño:** el BookBuilder debe mapear su gap de secuencia sobre esta misma triada (eventos de control plane con `detected_at_ms`), no crear un mecanismo paralelo.

### [F-MD-014] `streaming_hydra.py` (canary ORDERBOOK) es solo productor, con `DATASOURCE_REPLAY`
- **Severidad:** N/A (contexto)
- **Estado:** VERIFIED
- **Evidencia:** `apps/app/cli/streaming_hydra.py:186-209` — instancia `CryptofeedOrderBookStream` vía `CompositionRoot.build_ws_producers()`, `source=DATASOURCE_REPLAY` (canary, no genera señales, F-008). Produce a `orderbook.raw` sin consumidor. El `WSProducerBundle.orderbook` es `OrderBookKafkaProducer` (`composition_root.py:355-360`, client_id `ocm-ws-orderbook`).
- **Nota:** el canary valida producción; el BookBuilder añade el lado consumo en el mismo proceso o en uno dedicado (ver ADR-0022 lifecycle proceso realtime).

### [F-MD-015] `market.gaps` está producido por KafkaGapPublisher pero sin consumidor visible de control-plane
- **Severidad:** P3 (observabilidad del gap control-plane)
- **Estado:** UNVERIFIED
- **Evidencia:** `KafkaGapPublisher` existe y publica a `market.gaps`; no se encontró consumidor de `TOPIC_GAPS` en esta sesión (grep parcial). Requiere grep exhaustivo de `TOPIC_GAPS`/`market.gaps` para confirmar si algo consume el control-plane (dashboard/alerting). No bloquea el diseño.

### [F-MD-016] Puertos de lectura de market data: `FeatureReaderPort` existe (Gold, pull); no existe port de lectura de estado de book en vivo
- **Severidad:** N/A (base de diseño)
- **Estado:** VERIFIED
- **Evidencia:** `ports/outbound/feature_reader.py` (`FeatureReaderPort`: load_features/list_versions/list_datasets/get_manifest, Gold pull → polars). `ports/outbound/state.py` (`CursorStorePort`/`AsyncCursorStorePort`: cursores de ingesta). No existe ningún port de lectura de order book en vivo (mid/best/spread/depth). El BookBuilder introduce el primero.

### [F-MD-017] El patrón de consumidor Kafka del proyecto es `KafkaConsumerAdapter` + loop dedicado en main.py
- **Severidad:** N/A (base de diseño)
- **Estado:** VERIFIED
- **Evidencia:** `main.py::_bronze_writer_loop` (`main.py:222-307`): imports lazy, `KafkaConsumerAdapter.for_bronze_writer()` (topic ohlcv.raw, group ocm-bronze-writer), DLQ opcional (`TOPIC_DLQ`, reasons dlq_unavailable/dlq_sent/dlq_send_error), dedup durable L2 vía `RedisCursorStore` (fail-soft si Redis cae), `KafkaBronzeWriter.start()/run()/stop()` con CancelledError limpio. `consumer.py` tiene factory methods por grupo (`for_bronze_writer`, `for_feature_consumer`, `for_strategy_consumer`...).

### [F-MD-018] Hummingbot referencia: snapshot+diff con UID contiguo y restauración (patrón SSOT para el diseño)
- **Severidad:** N/A (referencia externa, GPL — patrón solo en prosa)
- **Estado:** VERIFIED
- **Evidencia:** `/tmp/opencode/hummingbot/hummingbot-master/` — `order_book.pyx` (in-memory bid_book/ask_book, `apply_diffs`/`apply_snapshot`, `best_bid`/`best_ask` por Side, `snapshot_uid`/`last_diff_uid`, `restore_from_snapshot_and_diffs(diffs, snapshot_uid)`), `order_book_message.py` (`update_id`, `first_update_id`; gap = `message.first_update_id != last_diff_uid + 1`), `order_book_tracker.py` (`_past_diffs_windows` deque con `DEFAULT_DIFF_BUFFER_SIZE` para bufferear diffs mientras no hay snapshot; `_saved_message_queues`), `data_feed/market_data_provider.py` (`get_price_by_type(PriceType.MidPrice)`, `get_price_for_volume`, `get_vwap_for_volume` como read API del book en memoria).

---

## 3. Estado actual del pipeline de order book (reconstruido desde código, VERIFIED)

```
Bybit WS (L2_BOOK, cryptofeed 2.4.1)
  → CryptofeedOrderBookStream (ACL)        [adapters/inbound/websocket/cryptofeed_orderbook_stream.py]
      · snapshot: book.book.to_dict() → bids/asks (SortedDict, sin .items())
      · delta:    book.delta → side/price/size (qty 0 = delete)
      · book.sequence_number disponible pero NO propagado (F-MD-012)
  → OrderBookKafkaProducer                  [adapters/inbound/websocket/orderbook_producer.py]
      · on_snapshot / on_delta → OrderBookSnapshotPayload / OrderBookDeltaPayload
      · routing key make_symbol_key(exchange, symbol) → FIFO por símbolo
      · source=REPLAY (canary) | LIVE (producción)
  → TOPIC_ORDERBOOK_RAW (orderbook.raw, retención 1h, Kappa)   [shared/kafka/topics.py:135]
      · consumidores productivos: NINGUNO (F-MD-003)
  → [reservado] book.snapshot / book.delta / microprice.rt      [topics.py:117-120, topics.py §aspiracional]
      · GROUP_BOOK_BUILDER = "ocm-book-builder" [topics.py:179] — 0 usos hoy
```

Componentes relacionados (VERIFIED):
- `CompositionRoot.build_ws_producers()` → `WSProducerBundle(orderbook, funding, oi, liquidations)` (`infrastructure/bootstrap/composition_root.py:323-360`). `build_feed_orchestrator`/`build_external_ingestion_orchestrator` existen para OHLCV/REST (no WS book).
- `packages/market_data/main.py` corre `_ingestion_loop` + `_bronze_writer_loop` en paralelo (`main.py:354-375`); no hay loop de book.
- Ports: `ports/inbound/` = `event_consumer.py` (EventConsumerPort), `external/{errors,polling,replay}`, `market_data_source.py`, `pipeline_factory.py`, `pipeline_trigger.py`, `trades_source.py`. `ports/outbound/` = `feature_reader.py`, `gap_event_publisher.py`, `kafka_consumer.py`, `kafka_producer.py`, `metrics.py`, `observability.py`, `state.py`, `storage/...`, `quality/...`.

---

## 4. Contratos import-linter relevantes (VERIFIED)

- `architecture/importlinter.toml` usa `BC-NN` secuencial. Estado actual (grep `BC-\d+` → 01..55; ELIMINADOS: BC-02, BC-28, BC-31; BC-49 REACTIVADO 2026-08-05). **BC-47 OCUPADO** (no asumir libre). El último número usado es **BC-55** (`research` composition root). → el nuevo contrato del BookBuilder propone **BC-56**.
- BC-37a/37b: `ports/inbound` no importa `ports/outbound` y viceversa. El BookBuilder (use case/application) consume `ports/outbound/` (kafka_consumer, gap_event_publisher) — respeta el flujo inbound→outbound solo vía application.
- BC-08 / BC-09: domain no importa adapters/infrastructure; el BookBuilder es **application/infrastructure**, no domain.
- BC-30 (medallion unidireccional), BC-32 (shared.kafka), BC-38 (PipelineFactory). BC-35: wire schemas solo en `shared/kafka/schemas/`.
- Contratos estructurales adicionales: `tests/architecture/` (import contracts, kafka contracts) — los nuevos schemas/topics deben actualizar `tests/architecture/test_kafka_contracts.py` (documentado, no implementado).

---

## 5. Análisis de diseño — BookBuilder / MarketState (objetivo, no implementación)

### 5.1 Referencia Hummingbot (patrones, no código)

| Patrón Hummingbot | Archivo (zip) | Problema que resuelve | Adaptación a OCM |
|---|---|---|---|
| `apply_snapshot` + `apply_diffs` sobre book in-memory | `order_book.pyx` | Reconstrucción L2 incremental | Idéntico concepto; implementación OCM con VOs `OrderBookSnapshot`/`OrderBookDelta` (`domain/value_objects/order_book.py`) |
| `snapshot_uid`/`last_diff_uid` contiguos | `order_book.pyx` | Continuidad snapshot→diffs | Requiere `sequence` en el wire (B-MD-003) |
| `message.first_update_id != last_diff_uid + 1` → gap | `order_book_message.py` | Detección de delta perdido | Gap de secuencia → evento `GapDetectedEvent` de control-plane (reutilizando `gap_events.py`) |
| `_past_diffs_windows` deque (buffer diffs hasta snapshot) | `order_book_tracker.py` | Reconexión/arranque sin perder diffs | Buffer de diffs pendientes en el BookBuilder consumidor (Kafka: replay desde offset) |
| `LatencyStats` rolling window con sample rate | `order_book_tracker.py` | Métricas de latencia de actualización del book | Gating en campos de tiempo (B-MD-007: received_at/processed_at) |
| `get_price_by_type(MidPrice)`/`get_price_for_volume`/`get_vwap_for_volume` | `market_data_provider.py` | Read API del estado en memoria | Puerto `MarketDataViewPort` (mid_price, best_bid, best_ask, spread, depth) |

**Decisión de estrategia (INFERENCE fundamentada en arquitectura OCM):** Hummingbot usa PubSub in-process porque productor y consumidor del book viven en el mismo proceso. OCM separa por Kafka (Kappa) → el buffer de diffs pendientes vive en el consumidor (BookBuilder), con arranque por snapshot + replay desde offset. **No se recomienda Redis como estado primario del book** (latencia, complejidad de consistencia, TTL) — el estado vive en memoria del proceso consumidor; Redis queda reservado para cursores/dedup (ya existente `CursorStorePort`).

### 5.2 Localización y estructura propuesta (Fase 1 — market_data únicamente)

- **Consumer:** `market_data.application.use_cases.book_builder.py` (o `application/pipelines/orderbook_book_builder.py`) — use case que recibe `OrderBookSnapshotPayload`/`OrderBookDeltaPayload` vía port `ports/outbound/kafka_consumer.py` (KafkaConsumerAdapter con `GROUP_BOOK_BUILDER`, topic `orderbook.raw`), reconstruye el estado en memoria por (exchange, symbol).
- **Estado en memoria:** `market_data.domain/application` value object `BookState` (bids DESC / asks ASC, `last_snapshot_uid`, `last_diff_uid`) — reutiliza VOs de `domain/value_objects/order_book.py`.
- **Read port (NUEVO, outbound):** `packages/market_data/ports/outbound/market_data_view.py` — `MarketDataViewPort` con `mid_price`, `best_bid`, `best_ask`, `spread`, `depth`, `is_ready`, `stale_threshold_ms`. **Sin consumidor en Fase 1** (B-MD-004 lo consumirá en Fase 2 desde trading/risk).
- **Control-plane:** gap de secuencia → `GapDetectedEvent`/`GapHealedEvent`/`GapFailedEvent` vía `GapEventPublisherPort` → `market.gaps` (reutiliza `KafkaGapPublisher`).
- **Output topics reservados:** `book.snapshot`/`book.delta` (opcionales en Fase 1 — el diseño primario es el port de lectura en memoria; los topics quedan para consumidores externos futuros).
- **Composition root:** `build_ws_producers` extiende a `build_book_builder` (o un nuevo `CompositionRoot.build_book_consumer()`) inyectando `KafkaConsumerAdapter` + `GapEventPublisherPort`. Wiring en `main.py` como tercer loop (`_book_builder_loop`) siguiendo `_bronze_writer_loop`.
- **Contrato import-linter nuevo:** **BC-56** — "BookBuilder (application/infrastructure market_data) depende solo de ports/outbound y domain; no importa trading/ni portfolio/ ni adapters inbound".

### 5.3 Borde y SafeOps (objetivo)

- `orderbook.raw` retención 1h → el BookBuilder debe arrancar con snapshot fresco dentro de esa ventana; si el snapshot del stream llega después de diffs, bufferizar diffs en un deque acotado (patrón `_past_diffs_windows`) y aplicar al recibir snapshot.
- Gap de secuencia → `GapDetectedEvent` + no aplicar el delta; esperar resync (nuevo snapshot) o marcar `not_ready`.
- `is_ready` = snapshot aplicado + último delta contiguo + edad < umbral. Consumidores consultan `is_ready` antes de usar mid/spread (B-MD-004).
- Métricas: `ocm_book_reconstruction_gap_total`, `ocm_book_latency_ms` (si B-MD-007 añade received_at/processed_at), `ocm_book_ready_total` — gated en campos de tiempo wire.

### 5.4 Fuera de alcance (explícito)

- No conecta `trading/` ni `portfolio/` (Fase 2 con B-MD-004).
- No implementa `trades_stream` huérfano (F-MD-008, B-MD-006).
- No añade RiskManager controls nuevos (B-MD-004 los desbloquea, no los implementa).
- No toca el canary `streaming_hydra.py` más allá de lo necesario (fuente `orderbook.raw` sigue siendo el mismo producer).

---

## 6. Estado de ejecución/riesgo/position (VERIFIED — amplía el comparativo 2026-08-14)

- **OMS `cancel()` es local-only** (`packages/trading/execution/oms.py:300`): transición de estado interno, **no** envía cancelación al exchange (el transporte no tiene cancel). Impacto: un `cancel` no puede revertir una orden en vuelo.
- **`reject_reason` es string libre** (`execution/order.py:121`), no tipo enumerado → sin taxonomía para alerting/filtrado.
- **`LiveExecutor` solo envía market orders**: `_BybitTransport` con `order_type="market"` (`bootstrap/composition_root.py:238`), retry+backoff, `timeout_s=10` fail-closed, reconciliación vía `_reconcile` (fetch_state; no confirmado → reject) (`execution/live_executor.py:161-261`).
- **No existe `fetch_balance`/reconciliación de saldo** en `trading/` (grep = ∅). El RiskManager valida capital×size_pct contra el capital configurado, no contra el saldo real del exchange.
- **Fees:** `Settlement` (`execution/settlement.py`) — `FeeStatus.KNOWN/UNKNOWN`, `fee_currency=None`, sin fallback de fee (GAP F7 del audit 2026-08-14). Position no tiene `cum_fees` ni breakeven ajustado por fees.
- **Position:** `PositionSnapshot` (`packages/portfolio/models/position.py`) — quantity, avg_entry (WAC, ADR-0025), cost_basis; `current_price=None` por defecto → unrealized PnL no disponible salvo que el caller lo inyecte. No hay cum_fees/breakeven.
- **ExecutionGuard global** (`ocm/runtime/guard.py`): kill switch global (max_errors, max_runtime_s, trigger manual); **no** hay circuit breaker por símbolo.
- **`live_hydra.py`**: `--mode` default=paper, `--capital` requerido; `PaperExecutor` rellena a `signal.price`.
- **Freqtrade/Hummingbot (referencia, VERIFIED):** Freqtrade valida mercado pre-envío (PricingError, get_min_pair_stake_amount, price_to_precision/amount_to_contract_precision) — OCM **no** (F-MD-006). Hummingbot `MarketDataProvider.get_price_by_type(MidPrice)` como read API del book — OCM no tiene equivalente (F-MD-016).

---

## 7. Matriz de decisión de patrones externos (VERIFIED)

| Patrón externo | Fuente | ¿OCM lo tiene? | ¿Compatible? | Veredicto |
|---|---|---|---|---|
| Snapshot+diff con UID contiguo | Hummingbot `order_book.pyx` | Parcial (VOs OK, sin consumer) | Sí | Adaptar (BookBuilder) |
| Gap detection por `first_update_id` | Hummingbot `order_book_message.py` | No (sin sequence en wire) | Sí, requiere B-MD-003 | Adaptar |
| Buffering diffs hasta snapshot | Hummingbot `order_book_tracker.py` | No | Sí (deque acotado en consumidor) | Adaptar |
| Read API del book (mid/spread/depth) | Hummingbot `market_data_provider.py` | No (solo Gold pull) | Sí (MarketDataViewPort) | Adaptar |
| Validación de mercado pre-envío | Freqtrade PricingError/precision | No | Sí (B-MD-004) | Adaptar (Fase 2) |
| Cancel real en exchange | — | **No** (OMS.cancel local-only) | Pendiente decisión | Investigar (fuera de Fase 1) |

---

## 8. Riesgos y decisiones de seguridad de ejecución (referencia 2026-08-14)

- **P0:** LiveExecutor market-order sin market validity (F-MD-006) — NO poner dinero real mañana.
- **P1:** Sin freshness (F-MD-001); OrderBook sin consumer (F-MD-003); OMS.cancel local-only; sin fetch_balance; RiskManager ciego a precio (F-MD-006).
- **P2:** Sin sequence en wire (F-MD-004/F-MD-012); sin instrumentos/precisión como datos (F-MD-007); trades_stream huérfano (F-MD-008).
- **P3:** Sin event_time/received_at/processed_at (F-MD-002/F-MD-007); DOCUMENTATION DRIFT (F-MD-011).

---

## 9. Live-Readiness Assessment

| Capa | Estado | Bloqueador |
|---|---|---|
| Ingesta OHLCV | Viable (Gold push, calidad + control-plane) | — |
| Orden Book | **NO viable** — producido sin consumir | B-MD-002 (BookBuilder) |
| Ejecución | **NO viable** — market orders sin validación de mercado | B-MD-001 + B-MD-004 |
| Riesgo | **NO viable** — sin market data ni fetch_balance | B-MD-004 + investigación saldo |
| Positions/Fees | Parcial — unrealized y fees incompletos | Fase 2/3 |
| Live executor | Fail-closed (timeout_s=10, reconcile) — sólido | requiere market validity |

**Veredicto: NO.** ¿Pondrías dinero real mañana? **NO.**

---

## 10. Documentación/implementación drift detectados

- **F-MD-011:** no existe stub `orderbook_stream.py` (ver §2).
- **ADR-0023, nota 2026-08-10:** el productor de `ohlcv.raw` usa `NullPublisher()` (F-031/B-46) → el precedente DLQ de OHLCV no tiene flujo real que lo ejercite hoy; no invalida el patrón para el BookBuilder.
- **`cryptofeed_orderbook_stream.py` docstring vs wire:** documenta `book.sequence_number` asignado pero el payload no lo emite (F-MD-012).

---

## 11. Fortalezas a proteger (VERIFIED)

- Dedup 3 niveles, `ExchangeQuirks`, provenance registry, calidad OHLCV + control-plane, reconciliación fail-closed (ADR-0016).
- Contractos import-linter robustos (49+ activos, BC-01..BC-55) y tests de arquitectura que los complementan.
- Control-plane de gaps ya diseñado (`gap_events.py`) — reutilizable tal cual para secuencia.
- Temas/grupos reservados (`book.snapshot`, `book.delta`, `GROUP_BOOK_BUILDER`) — la infraestructura ya anticipó el diseño.
- Patrón de consumidor Kafka establecido (`KafkaConsumerAdapter` + loops en `main.py`).

---

## 12. Diagrama objetivo (Fase 1 — market_data)

```
Bybit WS (cryptofeed)
  → CryptofeedOrderBookStream (ACL)
  → OrderBookKafkaProducer ──► orderbook.raw   [v2: +sequence/first_update_id — B-MD-003]
  → BookBuilder (consumer GROUP_BOOK_BUILDER)   [NUEVO — B-MD-002]
      · buffer diffs pendientes (deque acotado)
      · apply snapshot / apply diff → BookState en memoria
      · gap de secuencia → GapDetectedEvent ──► market.gaps (control-plane existente)
  → MarketDataViewPort (mid/spread/depth/is_ready)   [NUEVO port outbound]
      · consumidor: NINGUNO en Fase 1 (B-MD-004 en Fase 2)
  → [opcional] book.snapshot / book.delta (topics reservados)
```

---

## 13. Roadmap y prioridad

| ID | Descripción | Prioridad | Fase | ADR | Esfuerzo |
|---|---|---|---|---|---|
| B-MD-003 | `sequence`/`first_update_id` aditivo al wire (v2) | P1 | 1 | A-MD-001 | S |
| B-MD-001 | Freshness/staleness contract | P1 | 1 | — | S |
| B-MD-005 | Instrumentos/precisión/limits como datos | P1 | 1 | — | M |
| B-MD-002 | BookBuilder + MarketDataViewPort | P0 | 2 (depende de 003) | A-MD-002 | L |
| B-MD-004 | Pre-submit market validity | P0 | 2 (depende 001+002+005) | A-MD-003 | M |
| B-MD-006 | trades_stream huérfano | P2 | 2/3 | — | S |
| B-MD-007 | received_at/processed_at wire | P2 | 2/3 | A-MD-001 | S |

**Orden obligatorio:** B-MD-003 → B-MD-002 → B-MD-004 (001/005 prerrequisitos de 004). Fase 1 (market_data): B-MD-003 + B-MD-001 + B-MD-005. El diseño de esta auditoría cubre **B-MD-002** (BookBuilder/MarketState) y su prerrequisito **B-MD-003** (sequence wire).

---

## 14. Referencias

- Código: `packages/market_data/adapters/inbound/websocket/cryptofeed_orderbook_stream.py`, `.../orderbook_producer.py`, `packages/market_data/domain/events/gap_events.py`, `packages/market_data/domain/value_objects/order_book.py`, `packages/market_data/ports/outbound/{feature_reader,gap_event_publisher,state}.py`, `packages/market_data/infrastructure/bootstrap/composition_root.py`, `packages/market_data/main.py`, `packages/market_data/infrastructure/kafka/consumer.py`, `shared/kafka/schemas/orderbook.py`, `shared/kafka/topics.py`, `architecture/importlinter.toml`
- Docs: `docs/architecture/decisions/ADR-0023-...`, `docs/audits/2026-08-14-*`, `docs/plans/tracking.yaml` (B-25), `docs/hummingbot-master.zip`
- ADRs relacionados: ADR-0014 (diseño interno), ADR-0022 (realtime feeds), ADR-0023 (deferral B-25), ADR-0025/0026/0027 (position/fees/recovery), **ADR-0028-draft** (BookBuilder/MarketState, propuesto).

---

## 15. Entregables de esta sesión

- `docs/audits/2026-08-15-ocm-market-data-position-execution-risk-deep-audit.md` (este archivo).
- `docs/audits/2026-08-15-ocm-proposals-detail.md` — detalle de propuestas B-MD-001..007 + nuevas (B-MD-008 OMS.cancel, B-MD-009 fetch_balance) en formato requerido (14 campos).
- `docs/architecture/decisions/ADR-0028-draft-bookbuilder-marketstate.md` — diseño BookBuilder/MarketState como propuesta de ADR (Estado=Propuesto).

## 16. Próximos pasos

1. Aprobar/desechar B-MD-003 y B-MD-002 en `tracking.yaml` (decisión humana — no se modifica en esta sesión).
2. Al aprobar: implementar B-MD-003 (schema v2 aditivo + propagación sequence en producer) con el ADR A-MD-001.
3. Implementar B-MD-002 (BookBuilder + MarketDataViewPort + BC-56) con ADR-0028.
4. Verificar `TOPIC_GAPS` consumers (F-MD-015) antes de cablear control-plane.

## 17. Nota sobre `tracking.yaml`

No se ha modificado `docs/plans/tracking.yaml` en esta sesión. B-25 permanece PENDIENTE (ADR-0023). Las propuestas B-MD-XXX de esta auditoría y de la del 2026-08-14 quedan como candidatas a validación humana antes de cualquier cambio de contrato.
