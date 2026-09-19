# -*- coding: utf-8 -*-
"""
market_data/infrastructure/kafka/book_builder_consumer.py
===========================================================

BookBuilderConsumer — stream processor Kappa (orderbook.raw -> book.*).

Flujo
-----
  Kafka: orderbook.raw (schema v2)
      ↓ poll()
  deserialize → OrderBookSnapshotPayload | OrderBookDeltaPayload (por payload_type)
      ↓
  BookBuilder (use case puro, Decimal) — mantiene el libro por (exchange, symbol)
      ↓ outcome
  SNAPSHOT_APPLIED → publish book.snapshot
  DELTA_APPLIED    → publish book.delta
  GAP/STRUCTURAL_INVALID/STALE → métrica + log (el estado ya quedó invalidado;
                                  el snapshot fresco llega del stream al reconectar)
  DELTA_BEFORE_SNAPSHOT → skip (debug)

Semántica at-least-once
-----------------------
  poll() → procesar → commit() solo si no hubo errores de escritura (igual
  que KafkaBronzeWriter). Los mensajes no deserializables van al DLQ.

Idempotencia
------------
  A diferencia de OHLCV (B-19), un delta se aplica al libro del BookBuilder
  con tope de continuidad por update_id. Un reproceso de un delta ya aplicado
  (mismo update_id < last) se detecta y descarta (idempotente por 'u').

Principios: SRP · DIP · SafeOps · Kappa · at-least-once · SSOT
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, Optional

from loguru import logger

from market_data.infrastructure.kafka.metrics import KafkaMetrics
from market_data.ports.outbound.book_builder import (
    BookBuilderOutcome,
    BookBuilderPort,
    OutcomeKind,
)
from market_data.ports.outbound.kafka_consumer import KafkaConsumerPort
from market_data.ports.outbound.kafka_producer import KafkaProducerPort
from shared.kafka.schemas.orderbook import (
    OrderBookDeltaPayload,
    OrderBookSnapshotPayload,
)
from shared.kafka.serializer import deserialize, make_symbol_key, serialize
from shared.kafka.topics import (
    TOPIC_BOOK_DELTA,
    TOPIC_BOOK_SNAPSHOT,
    TOPIC_DLQ,
)

# Unión del wire orderbook v2. Ambas clases comparten el subset de campos que
# el BookBuilder consume (exchange, symbol, timestamp_ms, bids, asks, update_id)
# y ambas son subtipos de BasePayload (serializable). La unión expresa el
# contrato real de orderbook.raw: snapshot O delta, sin perder el tipado de
# los miembros (a diferencia de Any).
OrderBookPayload = OrderBookSnapshotPayload | OrderBookDeltaPayload


def _load_payload_type(value: bytes) -> Optional[str]:
    """Lee el discriminador payload_type sin deserializar el resto. Fail-soft."""
    try:
        raw: Dict[str, Any] = json.loads(value.decode("utf-8"))
        return raw.get("payload_type")
    except Exception:
        return None


class BookBuilderConsumer:
    """
    Stream processor: orderbook.raw → book.snapshot / book.delta.

    DIP — el use case de reconstrucción (``builder``) se inyecta como
    BookBuilderPort, NUNCA se instancia la implementación concreta aquí
    (BC-07: infrastructure no importa application). La implementación
    concreta (BookBuilder de application) la ensambla el composition root.

    Parámetros
    ----------
    consumer     : KafkaConsumerPort — fuente (orderbook.raw, grupo ocm-book-builder)
    producer     : KafkaProducerPort — salida (book.snapshot / book.delta / DLQ)
    builder      : BookBuilderPort — use case de reconstrucción L2 (inyectado)
    poll_timeout_ms / max_poll_records : tuning del loop
    """

    def __init__(
        self,
        consumer: KafkaConsumerPort,
        producer: KafkaProducerPort,
        builder: BookBuilderPort,
        poll_timeout_ms: int = 1_000,
        max_poll_records: int = 500,
        metrics: Optional[KafkaMetrics] = None,
        raw_metrics: Optional[KafkaMetrics] = None,
    ) -> None:
        if producer is None:
            raise TypeError("BookBuilderConsumer: 'producer' es obligatorio (DIP)")
        if builder is None:
            raise TypeError("BookBuilderConsumer: 'builder' es obligatorio (DIP, BC-07)")
        self._consumer = consumer
        self._producer = producer
        self._builder = builder
        self._poll_timeout_ms = poll_timeout_ms
        self._max_poll_records = max_poll_records
        self._metrics = metrics or KafkaMetrics(topic=TOPIC_BOOK_SNAPSHOT)
        self._raw_metrics = raw_metrics or KafkaMetrics(topic="orderbook.raw")
        self._running = False
        self._log = logger.bind(component="BookBuilderConsumer")

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        await self._consumer.start()
        await self._producer.start()
        self._running = True
        self._log.info("book_builder_consumer_started")

    async def stop(self) -> None:
        self._running = False
        await self._consumer.close()
        await self._producer.stop()
        self._log.info("book_builder_consumer_stopped")

    # ------------------------------------------------------------------ #
    # Main loop                                                            #
    # ------------------------------------------------------------------ #

    async def run(self) -> None:
        self._log.info("book_builder_loop_started")
        while self._running:
            try:
                await self._run_once()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._log.error("book_builder_loop_error", error=str(exc))
                await asyncio.sleep(1.0)

    async def run_once(self) -> tuple[int, int]:
        """Una iteración del loop. Útil para tests. Retorna (procesados, fallidos)."""
        return await self._run_once()

    # ------------------------------------------------------------------ #
    # Internal                                                             #
    # ------------------------------------------------------------------ #

    async def _run_once(self) -> tuple[int, int]:
        messages = await self._consumer.poll(
            timeout_ms=self._poll_timeout_ms,
            max_records=self._max_poll_records,
        )
        if not messages:
            await self._check_stale()
            return 0, 0

        processed = 0
        handled = 0
        write_errors = 0

        for msg in messages:
            outcome = await self._process_message(msg)
            if outcome == "written":
                processed += 1
            elif outcome == "handled":
                handled += 1
            else:  # "write_error"
                write_errors += 1

        if write_errors == 0 and (processed + handled) > 0:
            await self._consumer.commit()
        elif write_errors > 0:
            self._log.bind(write_errors=write_errors).warning(
                "book_builder_commit_skipped — write errors en batch, reintento al próximo poll"
            )

        await self._check_stale()
        return processed, write_errors

    async def _process_message(self, msg) -> str:
        """Procesa un mensaje. Retorna "written" | "handled" | "write_error"."""
        # ── Deserializar por payload_type (v2) ─────────────────────────
        try:
            ptype = _load_payload_type(msg.value)
            payload: OrderBookPayload
            outcome: BookBuilderOutcome
            if ptype == "snapshot":
                payload = deserialize(msg.value, OrderBookSnapshotPayload)
                outcome = self._builder.on_snapshot(
                    exchange=payload.exchange,
                    symbol=payload.symbol,
                    timestamp_ms=payload.timestamp_ms,
                    bids=list(payload.bids),
                    asks=list(payload.asks),
                    update_id=payload.update_id,
                )
            elif ptype == "delta":
                payload = deserialize(msg.value, OrderBookDeltaPayload)
                outcome = self._builder.on_delta(
                    exchange=payload.exchange,
                    symbol=payload.symbol,
                    timestamp_ms=payload.timestamp_ms,
                    bids=list(payload.bids),
                    asks=list(payload.asks),
                    update_id=payload.update_id,
                )
            else:
                # payload_type desconocido o ausente → DLQ (no aplica a v2).
                await self._send_to_dlq(msg, reason=f"unknown_payload_type:{ptype}")
                return "handled"
        except Exception as exc:
            self._log.warning(
                "book_builder_deserialize_error",
                offset=getattr(msg, "offset", "unknown"),
                ptype=ptype,
                error=str(exc),
            )
            await self._send_to_dlq(msg, reason=f"deserialize_error:{type(exc).__name__}")
            return "handled"

        return await self._apply_outcome(outcome, msg)

    async def _apply_outcome(self, outcome: BookBuilderOutcome, msg) -> str:
        """Traduce un outcome del BookBuilder a publicación / métrica / skip."""
        if outcome.publishes:
            # Publicar book.snapshot / book.delta
            payload: OrderBookPayload
            if outcome.kind is OutcomeKind.SNAPSHOT_APPLIED:
                out_topic = TOPIC_BOOK_SNAPSHOT
                payload = OrderBookSnapshotPayload(
                    exchange=outcome.exchange,
                    symbol=outcome.symbol,
                    timestamp_ms=outcome.timestamp_ms,
                    update_id=outcome.update_id,
                    bids=list(outcome.bids or []),
                    asks=list(outcome.asks or []),
                    depth=len(outcome.bids or []),
                )
            else:  # DELTA_APPLIED
                out_topic = TOPIC_BOOK_DELTA
                payload = OrderBookDeltaPayload(
                    exchange=outcome.exchange,
                    symbol=outcome.symbol,
                    timestamp_ms=outcome.timestamp_ms,
                    update_id=outcome.update_id,
                    bids=list(outcome.bids or []),
                    asks=list(outcome.asks or []),
                )

            key = make_symbol_key(outcome.exchange, outcome.symbol)
            try:
                await self._producer.produce(
                    topic=out_topic,
                    value=serialize(payload),
                    key=key,
                    headers={},
                )
                self._metrics.event_published(exchange=outcome.exchange)
                self._log.bind(
                    exchange=outcome.exchange,
                    symbol=outcome.symbol,
                    update_id=outcome.update_id,
                    topic=out_topic,
                ).debug("book_builder_published")
                return "written"
            except Exception as exc:
                self._metrics.event_failed(exchange=outcome.exchange, reason="write_error")
                self._log.bind(error=str(exc)).warning("book_builder_publish_error")
                return "write_error"

        # ── Outcomes no-publicables → métrica + log (ya contabilizados) ──
        if outcome.kind in (OutcomeKind.GAP_DETECTED, OutcomeKind.STRUCTURAL_INVALID):
            self._metrics.event_failed(exchange=outcome.exchange, reason=outcome.kind.value)
            self._log.bind(
                exchange=outcome.exchange,
                symbol=outcome.symbol,
                update_id=outcome.update_id,
            ).warning("book_builder_invalidated | {}", outcome.detail)
        elif outcome.kind is OutcomeKind.DELTA_BEFORE_SNAPSHOT:
            self._log.bind(
                exchange=outcome.exchange,
                symbol=outcome.symbol,
                update_id=outcome.update_id,
            ).debug("book_builder_delta_before_snapshot")
        return "handled"

    async def _check_stale(self) -> None:
        """Invoca BookBuilder.check_stale y alerta/metrica si hay STALE."""
        from time import time

        now_ms = int(time() * 1000)
        stale = self._builder.check_stale(now_ms)
        for outcome in stale:
            self._metrics.event_failed(exchange=outcome.exchange, reason="stale")
            self._log.bind(
                exchange=outcome.exchange,
                symbol=outcome.symbol,
                update_id=outcome.update_id,
            ).warning("book_builder_stale | {}", outcome.detail)

    async def _send_to_dlq(self, msg, reason: str) -> None:
        """Envía mensaje no procesable al DLQ. SafeOps — nunca lanza."""
        _offset = getattr(msg, "offset", "unknown")
        _topic = getattr(msg, "topic", "unknown")
        try:
            await self._producer.produce(
                topic=TOPIC_DLQ,
                value=msg.value,
                headers={"reason": reason, "original_topic": _topic},
            )
            self._raw_metrics.event_failed(reason="dlq_sent")
        except Exception as exc:
            self._log.error(
                "book_builder_dlq_send_error — mensaje perdido sin recuperación",
                reason=reason,
                offset=_offset,
                error=str(exc),
            )
            self._raw_metrics.event_failed(reason="dlq_send_error")


__all__ = ["BookBuilderConsumer"]
