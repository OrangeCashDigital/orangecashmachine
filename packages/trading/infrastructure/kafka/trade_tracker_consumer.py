# -*- coding: utf-8 -*-
"""
trading/infrastructure/kafka/trade_tracker_consumer.py
=======================================================

Consumidor de `orders.filled` → TradeTracker.

Ruta canónica única (F3)
------------------------
El producer (KafkaEventBusAdapter) publica la economía ya calculada por el OMS
en `Order.settlement` (filled_qty, fees, closed_qty, avg_entry_price,
remaining_qty). Este consumer NO reconstruye una `Order` NI recalcula P&L:
recompone el `Settlement` con `Settlement.compute(...)` desde el wire payload
y delega en `TradeTracker.record_settlement(...)` (TradeRecord.from_settlement
es el único constructor canónico; ADR-0025/0026/0027).

BUY vs SELL
-----------
- BUY: el OMS local ya abrió la posición (fill_sync). Solo se deduplica; volver
  a abrir duplicaría la posición.
- SELL: la posición abierta se lee de `TradeTracker.open_positions` (portfolio
  es la SSOT) y se cierra vía settlement.

Idempotencia y offsets (garantía efectiva, NO exactly-once)
-----------------------------------------------------------
Dedupe persistente por `event_id` con `get_raw`/`set_raw` y TTL de 30 días
(igual retention de `orders.filled`). Un payload inválido o un SELL sin
posición NO se marca como procesado (at-least-once → se reintenta). Un evento
con versión de schema incompatible sí se descarta para no bloquear la partición.

El offset SOLO se commitea si TODOS los mensajes del lote se procesaron. Si un
SELL no pudo cerrarse, se omite el commit del lote: al reiniciar se releerán
mensajes ya dedupados (seguros) y el pendiente (at-least-once). Queda una
ventana: si el proceso muere tras `record_settlement` pero antes de escribir
el dedupe, el replay puede duplicar el trade — por eso se documenta como
at-least-once con dedupe, no exactly-once.

BC-50
-----
Este módulo NO importa market_data: el store se tipa con `DedupeStorePort`
(trading/ports/outbound), que `ocm.runtime.state` satisface estructuralmente.

Principios: DIP · SafeOps · SSOT · BC-35 · BC-50
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger

from shared.kafka.schemas.orders import OrderFilledPayload, OrderSchemaVersionError
from trading.analytics.trade_tracker import TradeTracker
from trading.domain.models.settlement import FeeStatus, Settlement
from trading.ports.outbound.dedupe_store import DedupeStorePort

_DEDUPE_KEY_PREFIX = "order:seen:"
_DEDUPE_TTL_SECONDS = 30 * 24 * 60 * 60  # 30 días == retention de orders.filled


def _parse_ts(value: str) -> Optional[datetime]:
    """ISO-8601 (con o sin 'Z') → datetime tz-aware; None si no es parseable."""
    if not value:
        return None
    try:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)


def _fee_status(value: str) -> FeeStatus:
    """Fee status del wire (str) → FeeStatus; desconocido nunca se inventa como 0."""
    try:
        return FeeStatus(value)
    except ValueError:
        return FeeStatus.UNKNOWN


class TradeTrackerConsumer:
    """Consume `orders.filled` y registra cierres en el TradeTracker."""

    def __init__(
        self,
        tracker: TradeTracker,
        consumer: Any,
        dedupe_store: DedupeStorePort,
        *,
        consumer_started: bool = False,
    ) -> None:
        self._tracker = tracker
        self._consumer = consumer
        self._dedupe_store = dedupe_store
        self._consumer_started = consumer_started
        self._running = False
        self._log = logger.bind(component="TradeTrackerConsumer")

    async def start(self) -> None:
        """Bucle de consumo hasta stop(); commit solo si el lote se procesó."""
        self._running = True
        if not self._consumer_started:
            await self._consumer.start()
            self._consumer_started = True
        while self._running:
            messages = await self._poll(timeout_ms=1000)
            if not messages:
                continue  # no confirmar offsets sin mensajes procesados
            if await self._process_batch(messages):
                await self._consumer.commit()
            else:
                self._log.warning("commit omitido — mensajes pendientes de reintento | lote={}", len(messages))

    async def _poll(self, timeout_ms: int) -> list[Any]:
        """Acepta AIOKafkaConsumer (getmany) y KafkaConsumerPort (poll)."""
        getmany = getattr(self._consumer, "getmany", None)
        if callable(getmany):
            batches = await getmany(timeout_ms=timeout_ms, max_records=500)
            return [record for records in batches.values() for record in records]
        messages: list[Any] = await self._consumer.poll(timeout_ms=timeout_ms)
        return messages

    async def _process_batch(self, messages: list[Any]) -> bool:
        """Procesa un lote. False si algún mensaje requiere reintento."""
        all_processed = True
        for msg in messages:
            try:
                event = json.loads(msg.value)
            except (TypeError, ValueError) as exc:
                # Poison pill: reintentar no cambia el resultado. Se descarta.
                self._log.error("mensaje no-JSON descartado | error={}", exc)
                continue
            if not await self._on_event(event):
                all_processed = False
        return all_processed

    async def stop(self) -> None:
        self._running = False
        stop = getattr(self._consumer, "stop", None)
        if callable(stop):
            await stop()
        else:
            await self._consumer.close()

    async def _on_event(self, event: dict[str, Any]) -> bool:
        """True = procesado o descartado definitivamente; False = reintentar."""
        try:
            payload = OrderFilledPayload.from_dict(event)
        except (OrderSchemaVersionError, KeyError, TypeError, ValueError) as exc:
            # Poison pill: reintentar no cambia el resultado. Se descarta.
            self._log.error("payload inválido — descartado | error={}", exc)
            return True

        key = f"{_DEDUPE_KEY_PREFIX}{payload.event_id}"
        try:
            already_seen = self._dedupe_store.get_raw(key) is not None
        except Exception as exc:
            self._log.error("no se pudo consultar dedupe | key={} error={}", key, exc)
            return False
        if already_seen:
            self._log.debug("duplicado ignorado | event_id={}", payload.event_id)
            return True

        if payload.side == "buy":
            # El OMS ya abrió la posición con fill_sync; no reabrir.
            return self._mark_processed(key)

        if payload.side != "sell":
            self._log.error("side no soportado | side={} event_id={}", payload.side, payload.event_id)
            return True

        if not self._book_close(payload):
            return False  # sin posición / datos inválidos: no marcar → se reintenta
        return self._mark_processed(key)

    def _book_close(self, payload: OrderFilledPayload) -> bool:
        """Reconstruye el Settlement del wire y registra el cierre. True si se registró."""
        position = self._tracker.open_positions.get(payload.symbol)
        if position is None:
            self._log.warning("SELL sin posición abierta | symbol={} order={}", payload.symbol, payload.order_id)
            return False
        if not position.order_id:
            # La clave contractual del trade es la pierna de apertura; nunca
            # el order_id del SELL (evita trade_id inválido / falso cierre).
            self._log.error(
                "SELL sin order_id de apertura | symbol={} order={}",
                payload.symbol,
                payload.order_id,
            )
            return False

        # INV-08 (F1): el cierre nunca supera la cantidad mantenida,
        # aunque el exchange reporte un fill mayor.
        closed_qty = min(payload.closed_qty, position.quantity) if payload.closed_qty > 0.0 else position.quantity
        avg_entry = payload.avg_entry_price if payload.avg_entry_price is not None else position.avg_entry
        if closed_qty <= 0.0 or not avg_entry or avg_entry <= 0.0 or payload.fill_price <= 0.0:
            self._log.error(
                "SELL con económicos inválidos | symbol={} qty={} avg={} exit={}",
                payload.symbol,
                closed_qty,
                avg_entry,
                payload.fill_price,
            )
            return False

        settlement = Settlement.compute(
            order_id=payload.order_id,
            symbol=payload.symbol,
            closed_qty=closed_qty,
            avg_entry_price=avg_entry,
            exit_price=payload.fill_price,
            fee_amount_usd=payload.fee_amount_usd,
            fee_currency=payload.fee_currency or None,
            fee_status=_fee_status(payload.fee_status),
        )
        trade = self._tracker.record_settlement(
            settlement,
            exchange=payload.exchange or position.exchange,
            timeframe=payload.timeframe,
            size_pct=position.size_pct,
            entry_at=position.entry_at,
            exit_at=_parse_ts(payload.filled_at),
            trade_id=position.order_id,
        )
        return trade is not None

    def _mark_processed(self, key: str) -> bool:
        try:
            self._dedupe_store.set_raw(key, "processed", _DEDUPE_TTL_SECONDS)
            return True
        except Exception as exc:
            # Sin marca no se confirma el offset: se reintenta y se documenta
            # la ventana de duplicado si el settlement ya se registró.
            self._log.error("no se pudo marcar evento como procesado | key={} error={}", key, exc)
            return False
