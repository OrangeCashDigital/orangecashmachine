# -*- coding: utf-8 -*-
import json
from typing import Any

from loguru import logger

from shared.contracts.boundaries import OrderEventPublisherPort
from shared.kafka.schemas.orders import OrderFilledPayload, OrderRejectedPayload
from trading.domain.models.order import Order


class KafkaEventBusAdapter(OrderEventPublisherPort):
    def __init__(self, producer: Any) -> None:
        self._producer = producer
        self._log = logger.bind(component="KafkaEventBusAdapter")

    def publish(self, topic: str, event: object) -> None:
        if not isinstance(event, Order):
            raise TypeError(f"KafkaEventBusAdapter espera Order, recibió {type(event).__name__}")
        try:
            payload: OrderFilledPayload | OrderRejectedPayload
            if topic == "orders.filled":
                payload = self._map_filled(event)
            elif topic == "orders.rejected":
                payload = self._map_rejected(event)
            else:
                self._log.error("topic no soportado | {}", topic)
                return
            value = json.dumps(payload.to_dict()).encode()
            self._producer.produce(
                topic=topic,
                value=value,
                key=event.order_id.encode(),
                headers={"x-ocm-event-id": payload.event_id},
            )
            self._log.debug("Event published | topic={} event={}", topic, event.order_id)
        except Exception as exc:
            self._log.error("Kafka publish failed | topic={} event={} error={}", topic, event.order_id, exc)
            raise

    def _map_filled(self, event: Order) -> OrderFilledPayload:
        return OrderFilledPayload(
            event_id=event.order_id,  # Usar order_id como base para event_id o generar UUID nuevo
            occurred_at=event.fill_timestamp.isoformat() if event.fill_timestamp else "",
            order_id=event.order_id,
            exchange=event.signal.metadata.get("exchange", "unknown"),
            symbol=event.symbol,
            side=event.side.value,
            fill_price=event.fill_price if event.fill_price is not None else self._raise_missing_price(event.order_id),
            size_pct=event.size_pct,
            **self._economics(event),
            filled_at=event.fill_timestamp.isoformat() if event.fill_timestamp else "",
            signal_event_id=event.signal.metadata.get("signal_event_id", ""),
            run_id=event.signal.metadata.get("run_id", ""),
        )

    def _map_rejected(self, event: Order) -> OrderRejectedPayload:
        return OrderRejectedPayload(
            event_id=event.order_id,
            occurred_at=event.created_at.isoformat(),
            order_id=event.order_id,
            exchange=event.signal.metadata.get("exchange", "unknown"),
            symbol=event.symbol,
            side=event.side.value,
            reason=event.reject_reason or "unknown",
            signal_event_id=event.signal.metadata.get("signal_event_id", ""),
            run_id=event.signal.metadata.get("run_id", ""),
        )

    def _economics(self, event: Order) -> dict[str, Any]:
        out: dict[str, Any] = {
            "filled_qty": event.filled_qty if event.filled_qty is not None else 0.0,
            "timeframe": event.signal.timeframe,
        }
...
    def _raise_missing_price(self, order_id: str) -> float:
        raise ValueError(f"Order {order_id} filled but missing fill_price")
        s = getattr(event, "settlement", None)
        if s is None:
            return out
        out.update(
            closed_qty=s.closed_qty,
            avg_entry_price=s.avg_entry_price,
            remaining_qty=s.remaining_qty,
            fee_amount_usd=s.fee_amount_usd,
            fee_currency=s.fee_currency or "",
            fee_status=s.fee_status.value,
        )
        return out
