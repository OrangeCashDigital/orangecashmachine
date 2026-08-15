# ADR-0028 (PROPUESTA): BookBuilder/MarketState — primer consumidor de `orderbook.raw` (desbloquea B-25 / B-MD-002)

> **ESTADO: PROPUESTA** — borrador de diseño para decisión humana. NO aprobado. NO implementado. Ningún contrato cambia hasta su aceptación formal (proceso ADR + tracking.yaml).
> **Relación con B-25/ADR-0023:** ADR-0023 difiere el gap detection/DLQ product-side "hasta que exista un consumidor real de orderbook.raw". Este ADR-propuesta **es** ese consumidor — al aceptarse, B-25 se reabre.

**Estado:** Propuesto
**Fecha:** 2026-08-15
**Bounded context(s) afectado(s):** market_data | shared (contrato wire)

## Contexto

F-003/F-009/F-012/F-MD-003: `orderbook.raw` se produce (canary `streaming_hydra.py` → `CryptofeedOrderBookStream` → `OrderBookKafkaProducer`) pero **no tiene consumidores** (grep exhaustivo VERIFIED). No existe bid/ask/mid/spread/depth en runtime; el único acceso a market data del motor es Gold pull (`FeatureReaderPort`, `_GoldFeatureSource`). ADR-0023 + tracking B-25 (2026-08-09) difieren formalmente el gap detection product-side hasta que exista consumidor real.

Esta propuesta diseña (no implementa) el **BookBuilder**: un consumidor de `orderbook.raw` que reconstruye el L2 en memoria por (exchange, symbol) y expone un port de lectura — habilitando B-MD-004 (market validity pre-envío) en Fase 2 y reabriendo B-25 (gap detection) con la infraestructura que ADR-0023 anticipó (sequence en schema v2, `book.sequence_number`, `ocm_kafka_gap_total`, `TOPIC_DLQ`).

Hechos verificados en esta sesión:
- Wire v1 sin `sequence`: `shared/kafka/schemas/orderbook.py` (F-MD-012).
- `cryptofeed` expone `book.sequence_number` (documentado en `cryptofeed_orderbook_stream.py:158-161`), no propagado.
- `TOPIC_BOOK_SNAPSHOT="book.snapshot"`, `TOPIC_BOOK_DELTA="book.delta"`, `GROUP_BOOK_BUILDER="ocm-book-builder"` ya reservados (`shared/kafka/topics.py:117-120,179`), 0 usos productivos.
- Control-plane de gaps OHLCV existente: `domain/events/gap_events.py` (GapDetected/Healed/Failed) + `ports/outbound/gap_event_publisher.py` + `KafkaGapPublisher` → `market.gaps` (F-MD-013).
- Patrón de consumidor Kafka establecido: `KafkaConsumerAdapter` + `_bronze_writer_loop` (`main.py:222-307`) con DLQ + dedup durable L2 (F-MD-017).
- Hummingbot (referencia GPL, solo patrón en prosa): `order_book.pyx` (apply_snapshot/apply_diff, snapshot_uid/last_diff_uid, restore_from_snapshot_and_diffs), `order_book_message.py` (update_id/first_update_id → gap = first_update_id != last_diff_uid+1), `order_book_tracker.py` (`_past_diffs_windows` deque buffer diffs hasta snapshot), `market_data_provider.py` (get_price_by_type(MidPrice), get_price_for_volume, get_vwap_for_volume) (F-MD-018).

## Alternativas evaluadas

1. **Estado en Redis como SSOT del book** — ventaja: durable/compartible. Costo/riesgo: latencia, consistencia (partial writes), TTL, complejidad operativa innecesaria para un consumidor single-process; duplica el patrón `CursorStorePort` (cursores) en algo que no es cursor. Rechazada.
2. **BookBuilder en el mismo proceso del producer (in-process PubSub como Hummingbot)** — ventaja: sin Kafka para el estado. Costo/riesgo: rompe el patrón Kappa (productor/consumidor separados ya establecido), acopla canary a consumo, no aprovecha `orderbook.raw` ni `GROUP_BOOK_BUILDER`. Rechazada: OCM ya separa por Kafka.
3. **Solo port de lectura sin BookBuilder (leer book desde cryptofeed directo)** — ventaja: más simple. Costo/riesgo: duplicaría conexiones WS, perdería el Kappa/registro, no reabriría B-25, y acoplaría trading a cryptofeed. Rechazada.
4. **BookBuilder consumidor Kafka (ELEGIDA)** — consume `orderbook.raw` con `GROUP_BOOK_BUILDER`, reconstruye BookState en memoria, buffer de diffs pendientes acotado, control-plane de gaps de secuencia sobre `gap_events.py`, read port `MarketDataViewPort`. Compatible con Kappa, reutiliza infraestructura existente, reabre B-25 de forma natural.

## Decisión (propuesta)

En Fase 1 (market_data únicamente, sin tocar trading/ni portfolio/):

1. **B-MD-003 (prerrequisito):** schema v2 aditivo con `sequence` (snapshot) y `sequence`+`first_update_id` (delta) en `shared/kafka/schemas/orderbook.py`; propagar `book.sequence_number` en `cryptofeed_orderbook_stream.py` y `orderbook_producer.py`. Bump `SCHEMA_VERSION` (aditivo-backward-compatible). ADR A-MD-001.
2. **BookBuilder (B-MD-002):**
   - `market_data/application/use_cases/book_builder.py` — consume `orderbook.raw` (port `kafka_consumer.py`, `KafkaConsumerAdapter.for_book_builder()` con `GROUP_BOOK_BUILDER`), reconstruye BookState en memoria por (exchange, symbol).
   - `BookState` (domain VO reutilizando `OrderBookSnapshot`/`OrderBookDelta`): bids DESC/asks ASC, `last_snapshot_uid`, `last_diff_uid`, `is_ready`.
   - Buffer de diffs pendientes acotado (deque, patrón `_past_diffs_windows`) mientras no hay snapshot; aplicar al recibir snapshot.
   - Gap de secuencia → `GapDetectedEvent` (mapeando `symbol/timeframe` a `symbol` + secuencia esperada/recibida) → `GapEventPublisherPort` → `market.gaps`; no aplicar el delta; esperar resync (nuevo snapshot) o `is_ready=False`.
   - Métricas: `ocm_book_reconstruction_gap_total`, `ocm_book_ready_total`, `ocm_book_latency_ms` (este último solo si B-MD-007 añade received_at/processed_at).
3. **`MarketDataViewPort`** — `packages/market_data/ports/outbound/market_data_view.py`: `mid_price`, `best_bid`, `best_ask`, `spread`, `depth`, `is_ready`, `stale_threshold_ms`. **Sin consumidor en Fase 1** (B-MD-004 lo consumirá en Fase 2).
4. **Wiring:** `CompositionRoot.build_book_consumer()`; tercer loop `_book_builder_loop` en `main.py` (patrón `_bronze_writer_loop`).
5. **Contrato import-linter nuevo:** **BC-56** — BookBuilder (application/infrastructure market_data) depende solo de domain + ports/outbound; no importa trading/ni portfolio/ ni adapters inbound.
6. Topics `book.snapshot`/`book.delta` quedan **opcionales** (consumidores externos futuros); el read path primario es el port en memoria.

## Justificación técnica

- **Kappa y separación productor/consumidor ya son la arquitectura de OCM**: consumir `orderbook.raw` vía Kafka es el camino natural, no una alternativa nueva.
- **Reutiliza infraestructura probada**: `KafkaConsumerAdapter`, loop de `main.py`, DLQ (`TOPIC_DLQ`), dedup durable L2 (Redis fail-soft), `GapEventPublisherPort`/`KafkaGapPublisher`, VOs de order book.
- **Desbloquea B-25 sin rediseñar**: ADR-0023 ya anticipó exactamente esta secuencia (campo sequence aditivo, `book.sequence_number` propagado, `ocm_kafka_gap_total`, `TOPIC_DLQ`).
- **Hummingbot valida el patrón snapshot+diff+UID** en producción (estrategias reales sobre L2); solo se adopta el concepto, no código (GPL).
- **En memoria, no Redis**: el estado es efímero y single-consumer; Redis añade latencia/consistencia sin beneficio para el read port en proceso. Redis sigue siendo el store de cursores (`CursorStorePort`).
- **`is_ready` como compuerta** evita que consumidores (B-MD-004) usen un book no reconstruido o stale — extensión natural del fail-closed de OCM.

## Consecuencias

- **Queda más fácil:** B-MD-004 (market validity) tendrá de dónde leer spread/liquidez; B-25 se reabre con infraestructura ya diseñada; research/operaciones ganan bid/ask/mid/spread en runtime.
- **Deuda aceptada:** `orderbook.raw` retención 1h limita replay a ventana corta; el BookBuilder debe arrancar con snapshot fresco (canal WS) — fuera de alcance el backfill histórico de book.
- **Contratos BC que lo hacen cumplir:** BC-56 (nuevo), BC-35 (wire schemas en shared/kafka/schemas), BC-37a/37b (ports inbound/outbound), BC-30 (medallion), BC-09 (domain sin infra).
- **No implementado:** nada cambia hasta aprobación humana (proceso ADR + tracking.yaml). El ADR queda como propuesta.

## Referencias

- Código: `shared/kafka/schemas/orderbook.py`, `shared/kafka/topics.py`, `packages/market_data/adapters/inbound/websocket/cryptofeed_orderbook_stream.py`, `.../orderbook_producer.py`, `packages/market_data/domain/events/gap_events.py`, `packages/market_data/ports/outbound/gap_event_publisher.py`, `packages/market_data/main.py`, `packages/market_data/infrastructure/kafka/consumer.py`, `architecture/importlinter.toml`
- Docs: `docs/architecture/decisions/ADR-0023-...`, `docs/audits/2026-08-15-ocm-market-data-position-execution-risk-deep-audit.md` (F-MD-011..018, §5), `docs/audits/2026-08-15-ocm-proposals-detail.md`, `docs/plans/tracking.yaml` (B-25)
- ADRs relacionados: ADR-0014 (diseño interno), ADR-0022 (realtime feeds), ADR-0023 (deferral B-25)
