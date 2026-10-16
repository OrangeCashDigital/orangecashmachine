# ADR-0028: BookBuilder/MarketState — primer consumidor de `orderbook.raw` (desbloquea B-25 / B-MD-002)

> **ESTADO: ACEPTADO CON REVISIÓN** — Decisión humana Fase 0, 2026-09-24 (D1 = ACCEPT AFTER REVISION). Decisiones cerradas: D1=B, D2=u, D3=market.gaps, D4=MarketDataViewPort, D5=discard, D6=fix-dedup-u, ND1-ND5, C1.
> **Fecha de decisión:** 2026-09-24.
> **Relación con B-25/ADR-0023:** ADR-0023 difirió formalmente el gap detection/DLQ product-side "hasta que exista un consumidor real de orderbook.raw" (decisión histórica, 2026-08-09). Este ADR **es** ese consumidor; su aceptación reabre B-25 (estado: EN_PROGRESO en tracking.yaml).

**Estado:** Aceptado con revisión
**Fecha:** 2026-08-15 (revisión aprobada 2026-09-24)
**Bounded context(s) afectado(s):** market_data | shared (contrato wire)

## Contexto

F-003/F-009/F-012/F-MD-003: `orderbook.raw` se produce (canary `streaming_hydra.py` → `CryptofeedOrderBookStream` → `OrderBookKafkaProducer`) pero históricamente **no tenía consumidores** (grep exhaustivo VERIFIED en auditorías previas). No existe bid/ask/mid/spread/depth en runtime; el único acceso a market data del motor es Gold pull (`FeatureReaderPort`, `_GoldFeatureSource`). ADR-0023 + tracking B-25 (2026-08-09) difieren formalmente el gap detection product-side hasta que exista consumidor real.

El **BookBuilder** es el consumidor de `orderbook.raw` que reconstruye el L2 en memoria por (exchange, symbol) y expone un port de lectura — habilitando B-MD-004 (market validity pre-envío) en Fase 2 y reabriendo B-25 (gap detection).

Hechos verificados:
- Wire schema v2 actual: `update_id` (u), `cross_seq`, `cts_ms` — `shared/kafka/schemas/orderbook.py`.
- `cryptofeed` expone `book.sequence_number` (documentado en `cryptofeed_orderbook_stream.py:158-161`), no propagado (referencia histórica, no vigente).
- `TOPIC_BOOK_SNAPSHOT="book.snapshot"`, `TOPIC_BOOK_DELTA="book.delta"`, `GROUP_BOOK_BUILDER="ocm-book-builder"` ya reservados (`shared/kafka/topics.py:117-120,179`).
- Control-plane de gaps OHLCV existente: `domain/events/gap_events.py` (GapDetected/Healed/Failed) + `ports/outbound/gap_event_publisher.py` + `KafkaGapPublisher` → `market.gaps`.
- Patrón de consumidor Kafka establecido: `KafkaConsumerAdapter` + `_bronze_writer_loop` (`main.py`) con DLQ + dedup durable L2.
- BookBuilderConsumer **implementado y cableado** en commit `b561339f` (2026-09-19): `packages/market_data/application/processing/book_builder.py`, `packages/market_data/infrastructure/kafka/book_builder_consumer.py`, wiring en `streaming_hydra.py`.

## Alternativas evaluadas

1. **Estado en Redis como SSOT del book** — durable/compartible pero añade latencia, consistencia (partial writes), TTL y complejidad operativa para un consumidor single-process. Rechazada.
2. **BookBuilder en el mismo proceso del producer (in-process PubSub como Hummingbot)** — rompe el patrón Kappa (productor/consumidor separados ya establecido) y acopla canary a consumo. Rechazada.
3. **Solo port de lectura sin BookBuilder (leer book desde cryptofeed directo)** — duplicaría conexiones WS, perdería el registro Kappa, no reabriría B-25. Rechazada.
4. **BookBuilder consumidor Kafka (ELEGIDA)** — consume `orderbook.raw` con `GROUP_BOOK_BUILDER`, reconstruye BookState en memoria, gap detection sobre la continuidad de `update_id`, read port `MarketDataViewPort`. Compatible con Kappa, reutiliza infraestructura existente.

## Decisión (aceptada con revisión)

Decisión humana Fase 0 (2026-09-24). Se incorporan las decisiones D1-D6, ND1-ND5 y C1. La propuesta original queda revisada conforme a la implementación real y a la semántica decidida.

### 3.1 Schema v2 — NO CHANGE (D2)

- Schema v2 actual permanece estable: `update_id` (u), `cross_seq`, `cts_ms`.
- Para **Bybit**: `update_id`/`u` es simultáneamente **business identity del OrderBook** y **continuity token del BookBuilder**.
- **Canonical universal multi-exchange = UNKNOWN** (no se alegoriza el alcance de Bybit).
- **NO** `sequence`. **NO** `first_update_id`. **NO** schema v3 bump.
- El `cross_seq` (seq) es metadata de correlación cruzada, **no** base de gap detection.

### 3.2 Readiness (ND1 + C1)

**`is_ready = has_snapshot AND not_stale`** (simplificación C1 aceptada).

Semántica fail-closed explícita:

```
GAP_DETECTED
→ BookState invalidated/reset
→ has_snapshot = false
→ is_ready = false
```

- `has_snapshot`: `true` tras `on_snapshot` exitoso; `false` tras `reset()` (gap detectado, delta inválido, snapshot rechazado).
- `not_stale`: `now_ms - last_update_at_ms <= stale_threshold_ms`.
- **Un gap invalida el estado (`reset()` → `has_snapshot=false`) → `is_ready=false` automáticamente.** No se crea un segundo source of truth `unresolved_gap` (solo si la implementación lo requiere para otra semántica; no es necesario para esta).
- `stale_threshold` **configurable vía `AppConfig`**, default aproximado 2000 ms (ND2). NO hardcodearlo exclusivamente en BookBuilder.

### 3.3 Boundary ADR-0028 / ADR-0043 (D3, ND1)

**ADR-0028 (Reconstruction + Gap Detection + Readiness):**
- Consume `orderbook.raw` (schema v2, `GROUP_BOOK_BUILDER`).
- Reconstruye BookState L2 en memoria por (exchange, symbol).
- Detecta gaps por discontinuidad de `update_id` (`u`).
- Publica gap real → `OrderBookGapDetectedEvent` → `GapEventPublisherPort` → `market.gaps`.
- Invalida BookState (`reset()`) ante gap → `has_snapshot=false` → `is_ready=false`.
- **NO implementa:** buffer/replay, REST snapshot fetch, resync orchestration, recovery.

**ADR-0043 (Recovery/Resync) — responsabilidad exclusiva:**
- Decide cómo obtener el snapshot de recuperación (REST snapshot activo durante WS life, o reconnect + WS snapshot).
- Owner de `OB-SNAPSHOT-01` (adapter `orderbook_snapshot_fetcher`) y `OB-RESYNC-01` (connected u-jump, race, buffering/replay).
- Cuando la recuperación termina operacionalmente, un snapshot válido llega por `orderbook.raw` → BookBuilder.apply_snapshot → `has_snapshot=true` → `is_ready` reevalúa.

**Interfaz de coordinación:** puramente event-driven vía Kafka topic `market.gaps`. **NO existe callback, flag ni estado interno compartido entre ADR-0028 y ADR-0043.** ADR-0028 no consulta a ADR-0043 para determinar readiness.

### 3.4 Pre-snapshot policy — DISCARD (D5)

- Deltas recibidos antes de un snapshot válido se **DESCARTAN** (`DELTA_BEFORE_SNAPSHOT`).
- **NO bounded pre-snapshot buffer** (elimina el patrón propuesto `_past_diffs_windows`).
- Semántica **fail-closed**: sin snapshot válido, `is_ready=false`.
- El re-sincronizado/captura de deltas perdidos pertenece a ADR-0043.

### 3.5 Idempotency / gap policy — fix-dedup-u (D6)

Continuidad por `update_id` (`u`):

| Condición | Clasificación | Acción |
|---|---|---|
| `u < last_u` | STALE / REPLAY | **IGNORE** (no gap) |
| `u == last_u` | DUPLICATE | **IGNORE** (no gap) |
| `u == last_u + 1` | CONTINUOUS | **APPLY** |
| `u > last_u + 1` | GAP | **GAP_DETECTED** → invalidar, configurar `OrderBookGapDetectedEvent` |

- **Duplicate/replay NUNCA producen GAP_DETECTED.**
- La implementación actual (`book_builder.py`) debe ajustarse a esta tabla (corrección D6 pendiente de implementación, ver fases posteriores).

### 3.6 MarketDataViewPort (D4)

- **Frontera pública de lectura:** `packages/market_data/ports/outbound/market_data_view.py`.
- Superficie: `mid_price`, `best_bid`, `best_ask`, `spread`, `depth`, `is_ready`, `stale_threshold_ms`.
- **BookState permanece interno**; no se expone como API pública.
- Sin consumidor en Fase 1 (B-MD-004 lo consumirá en Fase 2).
- **Coexistencia con ARCH-006:** esta frontera sin consumidor en Fase 1 está gobernada por ADR-0044 (Staged Ports Gobernados); el enforcement técnico queda pendiente de una fase posterior. D4 permanece sin cambios.

### 3.7 Wiring (reconciliado con la implementación real)

- `CompositionRoot.build_book_builder_consumer()` crea el consumidor.
- La tarea de background se ejecuta en **`streaming_hydra.py`** (entrypoint ORDERBOOK canary). **NO** en `main.py` ni como tercer loop `_book_builder_loop` (propuesta original corregida a la implementación real, commit `b561339f`).

### 3.8 OrderBookGapDetectedEvent (ND4, ND5) — diseño documental, NO implementado en Phase A

- Evento de dominio específico de OrderBook L2 (NUEVO en fase posterior):
  - `event_id`, `exchange_id`, `symbol`, `last_u`, `observed_u`, `expected_u`, `missing_count`, `detected_at_ms`.
- **Semántica L2 explícita:** NO reutiliza campos OHLCV (`timeframe`).
- Publicación prevista: topic único **`market.gaps`** con header **`x-ocm-dataset=orderbook`**.
- **NO crear `market.gaps.orderbook`**.
- Reutilizar infraestructura OHLCV existente (`GapEventPublisherPort`, `KafkaGapPublisher`); **no duplicar publishers**.
- Este ADR documenta el diseño/gobernanza; **no implementa el evento en Phase A**.

### 3.9 BC-56 (ND3) — PRECONDITION de Phase B

- Contrato import-linter para aislar BookBuilder (application + infrastructure de market_data) de `trading/`, `portfolio/` y `adapters.inbound`.
- **Precondición obligatoria de Phase B:** auditar los contratos import-linter existentes antes de crear BC-56. NO crear BC-56 ciegamente.
- Si la auditoría revela violaciones preexistentes relevantes que impiden añadir BC-56 limpio → **STOP** + **REQUIRED HUMAN DECISION**.
- NO tocar `importlinter.toml` en Phase A.

### 3.10 B-25 — Estado de gobernanza

- **Precondición técnica (SATISFIED):** BookBuilderConsumer existe y está cableado en CompositionRoot/streaming_hydra.py (commit `b561339f`). **Runtime NO verificado en Phase A.**
- **Cierre formal de gobernanza (PENDING):** NO marcar B-25 como HECHO.
- El cierre formal de B-25 requiere (ver tracking.yaml):
  1. D3 implementado (publicación `OrderBookGapDetectedEvent` en `market.gaps`);
  2. D6 implementado (corrección idempotencia fix-dedup-u);
  3. Decisión humana explícita de cierre.
- Los pendientes arquitectónicos F6 (`ownership_storage`, `orquestacion_recovery`, `disparo_repair_strategy`, `maquina_estado_recovery`) permanecen en la lista intacta de tracking.yaml; **ADR-0028 no resuelve ninguno de ellos**.

## Justificación técnica

- **Kappa y separación productor/consumidor ya son la arquitectura de OCM**: consumir `orderbook.raw` vía Kafka es el camino natural, no una alternativa nueva.
- **Reutiliza infraestructura probada**: `KafkaConsumerAdapter`, DLQ (`TOPIC_DLQ`), dedup durable L2, `GapEventPublisherPort`/`KafkaGapPublisher`, VOs de order book.
- **Desbloquea B-25 con la infraestructura ya diseñada**: la especificación histórica de ADR-0023 (sequence aditivo, `book.sequence_number`, `ocm_kafka_gap_total`) queda **reemplazada** por la solución vigente de este ADR (identity/continuity por `update_id` en schema v2 estable, sin sequence/first_update_id, sin schema v3) conforme a D2.
- **En memoria, no Redis**: el estado es efímero y single-consumer; Redis añade latencia/consistencia sin beneficio para el read port en proceso. Redis sigue siendo el store de cursores (`CursorStorePort`).
- **Readiness fail-closed** (`is_ready = has_snapshot AND not_stale`) evita que consumidores (B-MD-004) usen un book no reconstruido o stale — extensión natural del fail-closed de OCM.
- **No se introduce un segundo source of truth**: un gap invalida `has_snapshot`; `unresolved_gap` no requiere estado independiente.

## Consecuencias

- **Queda más fácil:** B-MD-004 (market validity) tendrá de dónde leer spread/liquidez; B-25 queda EN_PROGRESO con infraestructura ya diseñada; research/operaciones ganan bid/ask/mid/spread en runtime.
- **Deuda aceptada:** `orderbook.raw` retención 1h limita replay a ventana corta; el BookBuilder debe arrancar con snapshot fresco (canal WS) — backfill histórico de book fuera de alcance (ADR-0043).
- **Contratos BC que lo hacen cumplir:** BC-56 (nuevo, Phase B), BC-35 (wire schemas en shared/kafka/schemas), BC-37a/37b (ports inbound/outbound), BC-30 (medallion), BC-09 (domain sin infra).
- **Fases posteriores (NO autorizadas por esta decisión documental):** implementación de D3/D6, creación de `OrderBookGapDetectedEvent`, `MarketDataViewPort`, BC-56, ajuste de AppConfig. Requieren autorización explícita.

## Referencias

- Código: `shared/kafka/schemas/orderbook.py` (schema v2), `shared/kafka/topics.py`, `packages/market_data/application/processing/book_builder.py`, `packages/market_data/infrastructure/kafka/book_builder_consumer.py` (commit `b561339f`), `packages/market_data/ports/outbound/gap_event_publisher.py`, `packages/market_data/adapters/outbound/kafka_gap_publisher.py`, `apps/app/cli/streaming_hydra.py`, `packages/market_data/infrastructure/bootstrap/composition_root.py`.
- Docs: `docs/architecture/decisions/ADR-0023-...` (deferral histórico), `docs/architecture/decisions/ADR-0041-...` (wire schemas / topic identity), `docs/architecture/decisions/ADR-0043-orderbook-snapshot-resync.md` (recovery/resync, KEEP SEPARATE), `docs/audits/2026-08-15-ocm-market-data-position-execution-risk-deep-audit.md` (F-MD-011..018, §5), `docs/audits/2026-08-15-ocm-proposals-detail.md`, `docs/plans/tracking.yaml` (B-25, fase0_gobernanza).
- ADRs relacionados: ADR-0014 (diseño interno), ADR-0022 (realtime feeds), ADR-0023 (deferral histórico B-25), ADR-0041, ADR-0043, ADR-0044 (Staged Ports Gobernados — gobernanza de la frontera pública en Fase 1).