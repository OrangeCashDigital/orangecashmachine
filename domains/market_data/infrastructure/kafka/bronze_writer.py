# -*- coding: utf-8 -*-
"""
market_data/infrastructure/kafka/bronze_writer.py
==================================================

KafkaBronzeWriter — stream processor Kappa.

Responsabilidad
---------------
Consumir mensajes del tópico ohlcv.raw y escribirlos a Iceberg Bronze.
Es el punto central de la arquitectura Kappa: Kafka → Bronze.

Flujo
-----
  Kafka: ohlcv.raw
      ↓  poll()
  deserialize() → EventPayload
      ↓
  dedup via event_id (SeenFilter L1 + Redis L2)
      ↓
  BronzeStorage.write()
      ↓
  commit() offset — at-least-once garantizado

Idempotencia
------------
Dos rutas de dedup:
  1. SeenFilter (L1, en memoria) — dedup dentro de la misma sesión
  2. BronzeStorage — Iceberg append-only deduplica por event_id
     al hacer merge en la capa Silver (Dagster job)

SafeOps
-------
- Mensajes no deserializables → DLQ (ohlcv.dlq) + commit + continuar
- Errores de escritura Bronze → log + NO commit (se reprocesa)
- El loop nunca se detiene por un mensaje individual

Principios: SRP · DIP · SafeOps · Kappa · at-least-once · SSOT
"""
from __future__ import annotations

import asyncio
from typing import Optional

from loguru import logger

from market_data.infrastructure.kafka.serializer import deserialize
from market_data.infrastructure.streaming.dedup  import SeenFilter
from market_data.ports.outbound.kafka_consumer   import KafkaConsumerPort
from market_data.ports.outbound.kafka_producer   import KafkaProducerPort, TOPIC_DLQ


class KafkaBronzeWriter:
    """
    Stream processor: ohlcv.raw → Iceberg Bronze.

    Parámetros
    ----------
    consumer       : KafkaConsumerPort — fuente de mensajes
    bronze_storage : protocolo de escritura Bronze (DIP — no IcebergStorage directo)
    dlq_producer   : KafkaProducerPort opcional — para mensajes no procesables
    poll_timeout_ms: tiempo de espera por poll en ms
    max_poll_records: máximo de mensajes por ciclo
    """

    def __init__(
        self,
        consumer:         KafkaConsumerPort,
        bronze_storage:   object,
        dlq_producer:     Optional[KafkaProducerPort] = None,
        poll_timeout_ms:  int = 1_000,
        max_poll_records: int = 500,
    ) -> None:
        self._consumer         = consumer
        self._bronze           = bronze_storage
        self._dlq              = dlq_producer
        self._poll_timeout_ms  = poll_timeout_ms
        self._max_poll_records = max_poll_records
        self._dedup            = SeenFilter(max_size=10_000)
        self._running          = False
        self._log              = logger.bind(component="KafkaBronzeWriter")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Arranca el consumer. Llamar antes de run()."""
        await self._consumer.start()  # type: ignore[union-attr]
        self._running = True
        self._log.info("bronze_writer_started")

    async def stop(self) -> None:
        """Detiene el loop y cierra el consumer. SafeOps."""
        self._running = False
        await self._consumer.close()  # type: ignore[union-attr]
        self._log.info("bronze_writer_stopped")

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """
        Loop principal. Corre hasta que stop() sea llamado.

        Cada iteración:
          1. poll() → mensajes
          2. procesar cada mensaje (deserializar + dedup + escribir)
          3. commit() si todos procesados exitosamente
        """
        self._log.info("bronze_writer_loop_started")
        while self._running:
            try:
                await self._run_once()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                # Fail-soft: loguear y continuar — no detener el loop
                self._log.error("bronze_writer_loop_error", error=str(exc))
                await asyncio.sleep(1.0)

    async def run_once(self) -> tuple[int, int]:
        """
        Una iteración del loop. Útil para tests.

        Retorna (procesados, fallidos).
        """
        return await self._run_once()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _run_once(self) -> tuple[int, int]:
        messages = await self._consumer.poll(  # type: ignore[union-attr]
            timeout_ms  = self._poll_timeout_ms,
            max_records = self._max_poll_records,
        )
        if not messages:
            return 0, 0

        processed = 0
        failed    = 0
        wrote_any = False

        for msg in messages:
            ok = await self._process_message(msg)
            if ok:
                processed += 1
                wrote_any  = True
            else:
                failed += 1

        # Commit solo si hubo escrituras exitosas — at-least-once
        if wrote_any:
            await self._consumer.commit()  # type: ignore[union-attr]

        self._log.bind(
            processed = processed,
            failed    = failed,
            total     = len(messages),
        ).debug("bronze_writer_cycle")

        return processed, failed

    async def _process_message(self, msg) -> bool:
        """
        Procesa un mensaje individual.

        Retorna True si fue escrito a Bronze, False si falló o fue duplicado.
        Mensajes no deserializables van a DLQ — SafeOps.
        """
        # ── Deserializar ──────────────────────────────────────────────
        try:
            event = deserialize(msg.value)
        except Exception as exc:
            self._log.warning(
                "bronze_writer_deserialize_error",
                offset = msg.offset,
                error  = str(exc),
            )
            await self._send_to_dlq(msg, reason=f"deserialize_error:{exc}")
            return False

        # ── Dedup L1 (en memoria) ─────────────────────────────────────
        if self._dedup.is_duplicate(event.event_id):
            self._log.bind(event_id=event.event_id).debug("bronze_writer_dedup_skip")
            return False
        self._dedup.mark_seen(event.event_id)

        # ── Escribir a Bronze ─────────────────────────────────────────
        try:
            await asyncio.to_thread(
                self._bronze.write,
                exchange  = event.exchange,
                symbol    = event.symbol,
                timeframe = event.timeframe,
                bars      = [b.to_dict() for b in event.bars],
            )
            self._log.bind(
                event_id  = event.event_id,
                exchange  = event.exchange,
                symbol    = event.symbol,
                bars      = len(event.bars),
            ).info("bronze_written")
            return True
        except Exception as exc:
            self._log.error(
                "bronze_write_error",
                event_id = event.event_id,
                error    = str(exc),
            )
            return False

    async def _send_to_dlq(self, msg, reason: str) -> None:
        """Envía mensaje no procesable al DLQ. SafeOps."""
        if self._dlq is None:
            return
        try:
            await self._dlq.send_async(  # type: ignore[union-attr]
                topic   = TOPIC_DLQ,
                value   = msg.value,
                headers = {"reason": reason, "original_topic": msg.topic},
            )
        except Exception as exc:
            self._log.warning("dlq_send_error", error=str(exc))


__all__ = ["KafkaBronzeWriter"]
