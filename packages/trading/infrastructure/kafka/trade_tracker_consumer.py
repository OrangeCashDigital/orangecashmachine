import asyncio
# -*- coding: utf-8 -*-
import json
import asyncio
from datetime import datetime
from typing import Any

from loguru import logger
from market_data.ports.outbound.state import CursorStorePort
from trading.analytics.trade_tracker import TradeTracker
from trading.domain.models.settlement import FeeStatus, Settlement

from shared.kafka.schemas.orders import OrderFilledPayload


class TradeTrackerConsumer:
    """
    Consumidor de eventos de ejecución (fills) para actualizar TradeTracker.

    Responsabilidades:
    - Escuchar topic orders.filled.
    - Garantizar idempotencia vía CursorStore.
    - Mapear eventos a Settlement canónico.
    - Notificar a TradeTracker cierres de posición.
    """

    def __init__(self, tracker: TradeTracker, consumer: Any, cursor_store: CursorStorePort):
        self._tracker = tracker
        self._consumer = consumer  # KafkaConsumerAdapter o AIOKafkaConsumer
        self._cursor_store = cursor_store  # Deduplicación persistente
        self._running = False
        self._log = logger.bind(component="TradeTrackerConsumer")

    async def start(self) -> None:
        """Inicia el loop de consumo. Propietario del ciclo de vida si no viene iniciado."""
        self._running = True

        # Detectar si el consumer ya está iniciado (caso AIOKafkaConsumer en tests)
        # o si es un Adaptador que gestiona su propio inicio.
        is_started = getattr(self._consumer, "_started", False)
        
        if not is_started:
            try:
                await self._consumer.start()
            except Exception as exc:
                # Si falla por estar ya iniciado en una implementación no detectable vía _started
                if "start called twice" in str(exc).lower():
                    self._log.debug("consumer_already_started")
                else:
                    self._log.error("consumer_start_failed | error={}", exc)
                    raise

        self._log.info("consumer_loop_started")
        while self._running:
            try:
                # Compatibilidad: Adapter usa getmany(), AIOKafkaConsumer usa poll() o getmany()
                if hasattr(self._consumer, "getmany"):
                    messages_dict = await self._consumer.getmany(timeout_ms=1000)
                    # getmany retorna Dict[TopicPartition, List[ConsumerRecord]]
                    messages = [msg for records in messages_dict.values() for msg in records]
                else:
                    # Fallback a poll si no hay getmany
                    messages = await self._consumer.poll(timeout_ms=1000)

                for msg in messages:
                    val = msg.value
                    if isinstance(val, (bytes, bytearray)):
                        val = json.loads(val)
                    await self._on_event(val)
                
                if messages:
                    await self._consumer.commit()
            except Exception as exc:
                self._log.error("poll_loop_error | error={}", exc)
                if not self._running:
                    break
                await asyncio.sleep(0.5)

    async def stop(self) -> None:
        """Detiene el consumo."""
        self._running = False
        # No cerramos el consumer aquí si queremos que el inyector sea el dueño del stop(),
        # pero para el pipeline E2E actual, necesitamos asegurar el stop.
        try:
            if hasattr(self._consumer, "stop"):
                await self._consumer.stop()
        except Exception as exc:
            self._log.warning("stop_error | error={}", exc)

    async def _on_event(self, event: dict) -> None:
        """Procesa un evento individual de orders.filled."""
        try:
            payload = OrderFilledPayload.from_dict(event)
        except Exception as exc:
            self._log.error("payload_parse_error | error={}", exc)
            return

        fill_id = payload.event_id
        # Clave de deduplicación persistente en Redis
        dedup_key = f"dedup:fill:{fill_id}"

        # 1. Idempotencia: Verificar si ya fue procesado usando la API de claves planas
        if self._cursor_store.get_raw(dedup_key):
            self._log.debug("event_already_processed | fill_id={}", fill_id)
            return

        try:
            # 2. Mapeo financiero: Distinguir entre apertura (BUY) y cierre (SELL)
            if payload.side == "sell" and payload.closed_qty > 0:
                # Reconstrucción de Settlement canónico
                settlement = Settlement.compute(
                    order_id=payload.order_id,
                    symbol=payload.symbol,
                    closed_qty=payload.closed_qty,
                    avg_entry_price=payload.avg_entry_price or 0.0,
                    exit_price=payload.fill_price,
                    fee_amount_usd=payload.fee_amount_usd,
                    fee_currency=payload.fee_currency,
                    fee_status=FeeStatus(payload.fee_status)
                    if payload.fee_status in [s.value for s in FeeStatus]
                    else FeeStatus.UNKNOWN,
                )

                exit_at = datetime.fromisoformat(payload.filled_at) if payload.filled_at else datetime.now()

                self._tracker.record_settlement(
                    settlement,
                    exchange=payload.exchange,
                    timeframe=payload.timeframe or "1m",
                    size_pct=payload.size_pct,
                    entry_at=exit_at,  # Fallback
                    exit_at=exit_at,
                    trade_id=payload.order_id,
                )
                self._log.info("position_closed_recorded | fill_id={} symbol={}", fill_id, payload.symbol)
            else:
                self._log.debug("fill_received_not_closing | fill_id={} side={}", fill_id, payload.side)

            # 3. Confirmar procesamiento en store persistente
            self._cursor_store.set_raw(dedup_key, "processed", ttl_s=86400 * 7)

        except Exception as exc:
            self._log.error("process_fill_error | fill_id={} error={}", fill_id, exc)


