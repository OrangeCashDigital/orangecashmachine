from __future__ import annotations

"""
market_data/streaming/source.py
================================

StreamSource — orquesta el loop de consumo Redis → EventRouter.

Idempotencia (dos capas)
------------------------
  L1 SeenFilter      — memoria, O(1), muere con el proceso
  L2 PersistentSeenFilter — Redis SET/TTL, sobrevive reinicios

Si se inyecta dedup_store (RedisCursorStore), se usa CompositeSeenFilter.
Si no, se usa solo SeenFilter (modo memoria — backward compat).

Principios: SRP · DI · SafeOps · at-least-once · self-healing
           · idempotencia persistente · observabilidad
"""

import time
from typing import Dict, List, Optional, Protocol, Tuple, runtime_checkable

from loguru import logger

from market_data.streaming.dedup    import SeenFilter, CompositeSeenFilter
from market_data.streaming.metrics  import StreamMetrics, timer
from market_data.streaming.payloads import EventPayload, SchemaVersionError
from market_data.streaming.router   import EventRouter

_MAX_RETRIES_BEFORE_DLQ: int = 3
_DEFAULT_POLL_INTERVAL_S: float = 0.1  # sleep entre polls vacíos — evita CPU spin


# ---------------------------------------------------------------------------
# RedisConsumerProtocol — contrato del consumer inyectado (DIP)
# ---------------------------------------------------------------------------

@runtime_checkable
class RedisConsumerProtocol(Protocol):
    """Contrato mínimo que cualquier consumer Redis debe cumplir.

    Permite sustituir el consumer en tests sin dependencia de Redis real.
    Structural subtyping — no requiere herencia.
    """

    def consume(self) -> List[Tuple[str, Dict]]:
        """Lee el siguiente batch de mensajes. Retorna lista de (entry_id, fields)."""
        ...

    def ack(self, entry_id: str) -> None:
        """Confirma procesamiento de un mensaje."""
        ...


class StreamSource:
    """
    Orquesta el loop: RedisStreamConsumer → EventPayload → EventRouter → ACK.

    Parámetros
    ----------
    consumer       : RedisStreamConsumer — fuente de mensajes (inyectado).
    router         : EventRouter         — destino de eventos (inyectado).
    max_errors     : int                 — errores consecutivos antes de detener.
    stream_name    : str                 — nombre lógico para métricas.
    claim_idle_ms  : int                 — ms idle antes de reclamar pendientes.
    dedup_max_size : int                 — tamaño máximo L1 (memoria).
    dedup_store    : RedisCursorStore | None — si se provee, activa L2 persistente.
    dedup_ttl_days : int                 — TTL de event_ids en Redis (L2).
    """

    def __init__(
        self,
        consumer:          RedisConsumerProtocol,
        router:            EventRouter,
        max_errors:        int   = 10,
        stream_name:       str   = "ohlcv",
        claim_idle_ms:     int   = 60_000,
        dedup_max_size:    int   = 10_000,
        dedup_store                = None,
        dedup_ttl_days:    int   = 7,
        poll_interval_s:   float = _DEFAULT_POLL_INTERVAL_S,
    ) -> None:
        self._consumer      = consumer
        self._router        = router
        self._max_errors    = max_errors
        self._claim_idle_ms = claim_idle_ms
        self._metrics       = StreamMetrics(stream_name=stream_name)
        self._log           = logger.bind(component="StreamSource")
        self._poll_interval_s = poll_interval_s
        self._retry_counts: Dict[str, int] = {}

        # Deduplicación: L1 solo o L1+L2 según dedup_store
        if dedup_store is not None:
            self._seen = CompositeSeenFilter(
                store    = dedup_store,
                max_size = dedup_max_size,
                ttl_days = dedup_ttl_days,
            )
        else:
            self._seen = SeenFilter(max_size=dedup_max_size)

    # --------------------------------------------------
    # Public API
    # --------------------------------------------------

    def run_once(self) -> tuple[int, int]:
        """Procesa un batch de mensajes nuevos. Retorna (processed, failed)."""
        return self._process_batch(self._consumer.consume())

    def run_once_pending(self) -> tuple[int, int]:
        """
        Recupera y procesa mensajes pendientes (claim_pending).
        SafeOps: retorna (0, 0) si el consumer no tiene claim_pending.
        """
        claim_fn = getattr(self._consumer, "claim_pending", None)
        if not callable(claim_fn):
            return 0, 0
        return self._process_batch(claim_fn(min_idle_ms=self._claim_idle_ms))

    def run(self, max_iterations: Optional[int] = None) -> None:
        """
        Loop continuo de consumo. Para workers en producción.

        Por iteración:
          1. claim_pending() — self-healing, prioridad alta
          2. consume()       — mensajes nuevos
        """
        self._log.info("stream_source_starting")
        consecutive_errors = 0
        iteration          = 0

        while True:
            if max_iterations is not None and iteration >= max_iterations:
                self._log.bind(iterations=iteration).info(
                    "stream_source_max_iterations_reached"
                )
                break

            try:
                p_pending, f_pending = self.run_once_pending()
                p_new,     f_new     = self.run_once()

                processed = p_pending + p_new
                failed    = f_pending + f_new

                if processed > 0 or failed > 0:
                    self._log.bind(
                        processed         = processed,
                        failed            = failed,
                        recovered_pending = p_pending,
                        iteration         = iteration,
                    ).info("batch_processed")

                consecutive_errors = 0 if failed == 0 else consecutive_errors + 1

            except Exception as exc:
                consecutive_errors += 1
                self._log.bind(
                    error              = str(exc),
                    consecutive_errors = consecutive_errors,
                    max_errors         = self._max_errors,
                ).error("run_once_unhandled_error")

            if consecutive_errors >= self._max_errors:
                self._log.bind(
                    consecutive_errors = consecutive_errors,
                ).critical("stream_source_stopping — max_errors reached")
                break

            iteration += 1

        self._log.info("stream_source_stopped")

    # --------------------------------------------------
    # Internal
    # --------------------------------------------------

    def _process_batch(
        self,
        messages: List[Tuple[str, Dict]],
    ) -> tuple[int, int]:
        if not messages:
            return 0, 0

        processed = 0
        failed    = 0

        for entry_id, fields in messages:

            # -- Deserialización --
            try:
                event = EventPayload.from_dict(fields)
            except SchemaVersionError as exc:
                self._log.bind(
                    entry_id = entry_id,
                    error    = str(exc),
                ).error("schema_version_mismatch — acking to unblock stream")
                self._consumer.ack(entry_id)
                self._metrics.event_failed(reason="schema_mismatch")
                self._retry_counts.pop(entry_id, None)
                failed += 1
                continue
            except Exception as exc:
                self._log.bind(
                    entry_id = entry_id,
                    error    = str(exc),
                ).warning("payload_deserialize_failed — acking to unblock stream")
                self._consumer.ack(entry_id)
                self._metrics.event_failed(reason="deserialize_error")
                self._retry_counts.pop(entry_id, None)
                failed += 1
                continue

            # -- Deduplicación (L1 + L2) --
            if self._seen.is_duplicate(event.event_id):
                self._consumer.ack(entry_id)
                self._log.bind(
                    entry_id = entry_id,
                    event_id = event.event_id,
                ).debug("duplicate_event_skipped")
                continue

            # -- Routing con latencia medida --
            with timer() as t:
                ok = self._router.route(event)

            if ok:
                self._consumer.ack(entry_id)
                self._seen.mark_seen(event.event_id)
                self._metrics.event_processed(
                    exchange   = event.exchange,
                    latency_ms = t.elapsed_ms,
                )
                self._retry_counts.pop(entry_id, None)
                processed += 1
                self._log.bind(
                    entry_id   = entry_id,
                    event_id   = event.event_id,
                    exchange   = event.exchange,
                    latency_ms = round(t.elapsed_ms, 2),
                ).debug("event_processed_and_acked")
            else:
                retries = self._retry_counts.get(entry_id, 0) + 1
                self._retry_counts[entry_id] = retries
                self._metrics.event_failed(
                    exchange = event.exchange,
                    reason   = "router_rejected",
                )
                failed += 1

                if retries >= _MAX_RETRIES_BEFORE_DLQ:
                    self._send_to_dlq(entry_id, fields, event, reason="router_rejected")
                    self._consumer.ack(entry_id)
                    self._retry_counts.pop(entry_id, None)
                else:
                    self._log.bind(
                        entry_id = entry_id,
                        event_id = event.event_id,
                        retries  = retries,
                    ).warning("router_rejected — left in PEL for retry")

        return processed, failed

    def _send_to_dlq(
        self,
        entry_id: str,
        fields:   Dict,
        event:    EventPayload,
        reason:   str,
    ) -> None:
        """Envía mensaje al Dead Letter Stream. SafeOps."""
        dlq_fn = getattr(self._consumer, "send_to_dlq", None)
        if callable(dlq_fn):
            try:
                dlq_fn(fields=fields, reason=reason, original_entry_id=entry_id)
                self._log.bind(
                    entry_id = entry_id,
                    event_id = event.event_id,
                    reason   = reason,
                ).error("message_sent_to_dlq")
            except Exception as exc:
                self._log.bind(
                    entry_id = entry_id,
                    error    = str(exc),
                ).warning("dlq_send_failed — message will be acked anyway")
        else:
            self._log.bind(
                entry_id = entry_id,
                event_id = event.event_id,
                reason   = reason,
            ).error("message_dead — no DLQ configured, acking to unblock")
