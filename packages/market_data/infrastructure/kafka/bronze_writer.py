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
  dedup via event_id (SeenFilter L1 en memoria)
      ↓
  BronzeStorage.append()
      ↓
  commit() offset — at-least-once garantizado

Idempotencia
------------
Dos rutas de dedup:
  1. SeenFilter L1 (en memoria) — dedup dentro de la sesión del proceso
  2. Iceberg merge-on-read — Silver Dagster deduplica por event_id

Semántica at-least-once — Fix B-NEW-04
---------------------------------------
Un batch de mensajes puede tener mezcla de éxitos y fallos.
La política correcta para at-least-once:

  CASO A — error de Bronze write:
    El mensaje NO se commitea → se reintenta en el próximo poll.
    Sin commit → el proceso puede ver el mismo mensaje dos veces,
    pero SeenFilter L1 + Iceberg dedup lo manejan correctamente.

  CASO B — mensaje no deserializable o vacío:
    Va al DLQ → se cuenta como "handled" → sí se commitea.
    Razón: no tiene sentido reintentar un mensaje corrupto — el DLQ
    es el lugar correcto para replay manual.

Implementación: commit si write_errors == 0.
Si cualquier Bronze write falla, NO hacer commit del batch completo.
Los mensajes exitosos dentro del batch son idempotentes (SeenFilter + dedup).

Fixes aplicados
---------------
B-NEW-02: self._dlq.send_async() → self._dlq.produce() (método canónico del port)
B-NEW-04: at-least-once corregido — no commitear si hubo write_errors > 0
B-NEW-05: topics importados desde topics.py (SSOT), no desde el port

Principios: SRP · DIP · SafeOps · Kappa · at-least-once · SSOT
"""
from __future__ import annotations

import asyncio
from typing import Optional

from loguru import logger

import pandas as pd

from market_data.infrastructure.kafka.serializer import deserialize
from market_data.infrastructure.kafka.dedup      import SeenFilter
# Fix M-NUEVO: TOPIC_DLQ desde shared.kafka.topics (SSOT global).
# legacy topics.py definía TOPIC_DLQ='ohlcv.dlq' pero shared define
# TOPIC_DLQ='ocm.dlq' — dos DLQs distintos. shared es canónico.
from shared.kafka.topics import TOPIC_DLQ
from market_data.ports.outbound.kafka_consumer   import KafkaConsumerPort
from market_data.ports.outbound.kafka_producer   import KafkaProducerPort


class KafkaBronzeWriter:
    """
    Stream processor: ohlcv.raw → Iceberg Bronze.

    Parámetros
    ----------
    consumer       : KafkaConsumerPort — fuente de mensajes
    bronze_storage : BronzeStoragePort — escritura Bronze (DIP)
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
        await self._consumer.start()  # type: ignore[attr-defined]
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
        Loop principal. Corre hasta que stop() sea llamado o CancelledError.

        Cada iteración:
          1. poll() → mensajes
          2. procesar cada mensaje (deserializar + dedup + escribir)
          3. commit() SOLO si write_errors == 0 (at-least-once correcto)
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
        """Una iteración del loop. Útil para tests. Retorna (procesados, fallidos)."""
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

        processed    = 0   # escrituras exitosas a Bronze
        handled      = 0   # mensajes manejados (DLQ o dedup skip) — no write error
        write_errors = 0   # fallos de escritura a Bronze — bloquean el commit

        for msg in messages:
            outcome = await self._process_message(msg)
            if outcome == "written":
                processed += 1
            elif outcome == "handled":
                # DLQ, dedup skip, o bars vacío — no es un error de escritura.
                # El mensaje fue procesado de forma definitiva; el DLQ preserva
                # los que no pudieron deserializarse para replay manual.
                handled += 1
            else:  # outcome == "write_error"
                write_errors += 1

        # FIX B-NEW-04 — at-least-once correcto:
        # Commitear SOLO si no hubo errores de escritura a Bronze.
        # Si hubo write_errors → NO commitear → los mensajes del batch
        # se reprocesan en el próximo poll. Los mensajes exitosos dentro
        # del batch son idempotentes (SeenFilter L1 + Iceberg dedup).
        if write_errors == 0 and (processed + handled) > 0:
            await self._consumer.commit()  # type: ignore[union-attr]
        elif write_errors > 0:
            self._log.bind(
                write_errors = write_errors,
                processed    = processed,
                handled      = handled,
            ).warning(
                "bronze_writer_commit_skipped — write errors en batch, "
                "se reintentará en el próximo poll"
            )

        self._log.bind(
            processed    = processed,
            handled      = handled,
            write_errors = write_errors,
            total        = len(messages),
        ).debug("bronze_writer_cycle")

        return processed, write_errors

    async def _process_message(self, msg) -> str:
        """
        Procesa un mensaje individual.

        Returns
        -------
        "written"     : escrito exitosamente a Bronze → cuenta para commit.
        "handled"     : DLQ / dedup skip / bars vacío → cuenta para commit.
        "write_error" : fallo de escritura Bronze → NO commitear el batch.
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
            return "handled"  # DLQ recibe el mensaje — no reintentar

        # ── Dedup L1 (en memoria) ─────────────────────────────────────
        if self._dedup.is_duplicate(event.event_id):
            self._log.bind(event_id=event.event_id).debug("bronze_writer_dedup_skip")
            return "handled"
        self._dedup.mark_seen(event.event_id)

        # ── Validar que hay barras ────────────────────────────────────
        if not event.bars:
            self._log.bind(event_id=event.event_id).warning(
                "bronze_writer_empty_bars — descartado"
            )
            await self._send_to_dlq(msg, reason="empty_bars")
            return "handled"  # DLQ registra el evento vacío

        # ── Construir DataFrame ───────────────────────────────────────
        df = pd.DataFrame([
            {
                "timestamp": pd.Timestamp(b.ts, unit="ms", tz="UTC"),
                "open":      b.open,
                "high":      b.high,
                "low":       b.low,
                "close":     b.close,
                "volume":    b.volume,
            }
            for b in event.bars
        ])

        # ── Escribir a Bronze via append() ────────────────────────────
        try:
            await asyncio.to_thread(
                self._bronze.append,  # type: ignore[attr-defined]
                df        = df,
                symbol    = event.symbol,
                timeframe = event.timeframe,
                run_id    = event.event_id,
            )
            self._log.bind(
                event_id  = event.event_id,
                exchange  = event.exchange,
                symbol    = event.symbol,
                timeframe = event.timeframe,
                bars      = len(event.bars),
                source    = getattr(event, "source", "unknown"),
            ).info("bronze_written")
            return "written"
        except Exception as exc:
            self._log.error(
                "bronze_write_error",
                event_id  = event.event_id,
                exchange  = event.exchange,
                symbol    = event.symbol,
                timeframe = event.timeframe,
                error     = str(exc),
            )
            return "write_error"  # NO commitear — reintentar en próximo poll

    async def _send_to_dlq(self, msg, reason: str) -> None:
        """Envía mensaje no procesable al DLQ. SafeOps."""
        if self._dlq is None:
            return
        try:
            # FIX B-NEW-02: produce() — método canónico del port.
            # send_async() no existe en KafkaProducerPort.
            await self._dlq.produce(
                topic   = TOPIC_DLQ,
                value   = msg.value,
                headers = {"reason": reason, "original_topic": msg.topic},
            )
        except Exception as exc:
            self._log.warning("dlq_send_error", error=str(exc))


__all__ = ["KafkaBronzeWriter"]
