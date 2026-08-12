# ADR-0023: Deferral del gap detection / DLQ de productores WS→Kafka (F-009 / B-25)

**Estado:** Aceptado (registra deferral explicitado el 2026-08-09)
**Fecha:** 2026-08-09
**Bounded context(s) afectado(s):** market_data | shared

## Contexto

F-009 / B-25 (docs/audits/2026-08-08-streaming-canary-audit.md, docs/plans/
tracking.yaml) detectó que el pipeline WS→Kafka de order book no tiene gap
detection ni retry product-side: un delta perdido deja el book local
desincronizado sin recuperación automática. La remediación propuesta exigía:

1. Añadir un campo de secuencia (sequence number) a `OrderBookDeltaPayload`,
   un contrato público wire en `shared/kafka/schemas/orderbook.py`.
2. Propagar `book.sequence_number` (que `cryptofeed` ya expone, verificado en
   `cryptofeed_orderbook_stream.py:158-161`) desde el adapter al payload.
3. Diseñar la estrategia de DLQ/gap del lado productor (retry topic, replay).

Estado verificado del código (2026-08-09), evidencia no especulativa:

- **No existe consumidor real de `orderbook.raw` en el repo.**
  `grep` de `TOPIC_ORDERBOOK_RAW` en `packages/` y `apps/`: único uso es el
  productor (`orderbook_producer.py:48,74,105`). `GROUP_BOOK_BUILDER`,
  `TOPIC_BOOK_DELTA`, `TOPIC_BOOK_SNAPSHOT` (declarados en
  `shared/kafka/topics.py`) tienen **0 usos en código productivo**.
  El BookBuilder que reconstruiría el book y aplicaría los deltas es
  aspiracional — el hallazgo documenta el riesgo "en teoría", sin beneficiario.
- **El DLQ infrastructure ya existe**: `TOPIC_DLQ = "ocm.dlq"` (topics.py:102)
  y el lado consumidor de OHLCV ya lo usa (`bronze_writer.py::_send_to_dlq`,
  reasons `dlq_unavailable`/`dlq_sent`/`dlq_send_error`).
- `send_async()` del producer ya clasifica las causas de fallo (F-019 / B-35,
  resuelto): `broker_timeout` | `connection_error` | `broker_response` |
  `unknown_error`; `produce()` eleva `KafkaProducerError` visible (F-013).

## Alternativas evaluadas

1. **Implementar gap detection ahora** — añadir `sequence` al wire schema
   (cambio aditivo-opcional) y propagar `book.sequence_number` desde el
   producer. Costo: toca un contrato público sin consumidor que lo use y sin
   donde verificar gaps; viola la regla "no cambios especulativos" si el gap
   se mide pero nadie lo consume.
2. **No hacer nada / posponer sin registro** — deja la decisión arquitectónica
   implícita; la próxima persona que lea `OrderBookDeltaPayload` no sabe por
   qué no hay campo de secuencia y puede volver a plantear lo mismo.
3. **Deferral explícito + ADR (elegida)** — documentar que el gap detection
   se bloquea hasta que exista un consumidor real del topic (`BookBuilder`),
   momento en el que se propone un wire schema v2 con sequence number y la
   política de DLQ/replay product-side.

## Decisión

Diferir formalmente la implementación de gap detection y DLQ product-side de
`orderbook.raw` (F-009 / B-25). B-25 queda en `PENDIENTE` con esta ADR como
referencia de decisión: **no se modifica el wire schema ni el producer hasta
que exista un consumidor real de `orderbook.raw`** en el repositorio.

## Justificación técnica

- Implementar un mecanismo de detección de gaps sin consumidor que lo
  procese es código muerto inverificable — viola la política del proyecto de
  no introducir cambios especulativos: sin consumidor, ni un test ni la
  observabilidad pueden certificar un comportamiento real.
- La infraestructura DLQ ya está SSOT en `shared/` y el lado consumidor de
  OHLC la ejercita; el patrón a seguir ya existe y será reutilizable tal cual
  cuando haya consumidor de order book.
- La señal de fallo product-side ya es visible hoy (F-013/F-019 resueltos):
  `KafkaProducerError` en `produce()` + clasificación de causas en
  `send_async()`. La "pérdida silenciosa" del hallazgo original quedó
  mitigada — lo pendiente es recuperación, dependiente del consumidor.

## Consecuencias

- B-25 queda en `PENDIENTE` (no HECHO) con ADR-0023 como `adr_relacionado`.
- Cuando exista un consumidor real: implementar paso a paso lo que sugiere la
  decisión (campo sequence aditivo en schema v2, `book.sequence_number`
  propagado por `orderbook_producer`, verificación product- o consumer-side
  con métrica `ocm_kafka_gap_total`, reutilizando `TOPIC_DLQ`).
- No hay contrato público roto hoy: `OrderBookDeltaPayload` queda intacto.
- Trackeador: B-25 mantiene su `riesgo_residual` (pérdida posible de deltas
  sin gap detection) hasta la reapertura.

## Referencias

- Código: `shared/kafka/schemas/orderbook.py`,
  `shared/kafka/topics.py`, `packages/market_data/adapters/inbound/websocket/
  orderbook_producer.py`, `cryptofeed_orderbook_stream.py`,
  `packages/market_data/infrastructure/kafka/bronze_writer.py`
- Docs: `docs/audits/2026-08-08-streaming-canary-audit.md` F-009,
  `docs/plans/tracking.yaml` B-25, `docs/plans/backlog-priorizado-2026-08-08.md`.
- ADRs relacionados: ADR-0014 (diseño interno), ADR-0022 (realtime_feeds).

## Nota de discrepancia (2026-08-10) — F-031 / B-46

Este ADR cita como referencia que "el lado consumidor de OHLCV ya usa"
`bronze_writer.py::_send_to_dlq` sobre `ohlcv.raw`. Cierto que el consumidor
existe, pero el **productor** de `ohlcv.raw` no está conectado en producción:
`OHLCVPipeline` publica a `NullPublisher()` y `_chunk_converter` no se
inyecta, por lo que hoy no llega ningún evento a ese topic y `bronze_writer`
no tiene qué consumir (F-031/B-46). La afirmación de este ADR era un
precedente de patrón, válido cuando se reabra B-25; con el wiring Kappa
pendiente, el patrón DLQ de OHLCV queda por el momento sin flujo real que lo
ejercite.