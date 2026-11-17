# -*- coding: utf-8 -*-
import json
from typing import Any, Optional
from loguru import logger
from trading.domain.models.order import Order
from shared.contracts.event_bus import EventBusPort
from shared.kafka.schemas.orders import OrderFilledPayload, OrderRejectedPayload
from shared.kafka.schemas._base import OrderSide

class KafkaEventBusAdapter(EventBusPort):
    def __init__(self, producer: Any) -> None:
        self._producer = producer
        self._log = logger.bind(component="KafkaEventBusAdapter")

    def publish(self, topic: str, order: Order) -> None:
        try:
            if topic == "orders.filled":
                payload = self._map_filled(order)
                value = json.dumps(payload.to_dict()).encode()
            elif topic == "orders.rejected":
                payload = self._map_rejected(order)
                value = json.dumps(payload.to_dict()).encode()
            else:
                self._log.error("topic no soportado | {}", topic)
                return

            # Utiliza el productor Kafka inyectado (asume método produce(topic, value, key, headers))
            # Ajustado para usar la interfaz del productor existente.
            self._producer.produce(
                topic=topic,
                value=value,
                key=order.order_id.encode(),
                headers={"x-ocm-event-id": payload.event_id}
            )
            self._log.debug("Event published | topic={} order={}", topic, order.order_id)
        except Exception as exc:
            self._log.error("Kafka publish failed | topic={} order={} error={}", topic, order.order_id, exc)
            raise

    def _map_filled(self, order: Order) -> OrderFilledPayload:
        return OrderFilledPayload(
            event_id=order.order_id, # Usar order_id como base para event_id o generar UUID nuevo
            occurred_at=order.fill_timestamp.isoformat() if order.fill_timestamp else "",
            order_id=order.order_id,
            exchange=order.signal.metadata.get("exchange", "unknown"),
            symbol=order.symbol,
            side=OrderSide(order.side.value),
            fill_price=order.fill_price or 0.0,
            size_pct=order.size_pct,
            filled_at=order.fill_timestamp.isoformat() if order.fill_timestamp else "",
            signal_event_id=order.signal.metadata.get("signal_event_id", ""),
            run_id=order.signal.metadata.get("run_id", ""),
        )

    def _map_rejected(self, order: Order) -> OrderRejectedPayload:
        return OrderRejectedPayload(
            event_id=order.order_id,
            occurred_at=order.created_at.isoformat(),
            order_id=order.order_id,
            exchange=order.signal.metadata.get("exchange", "unknown"),
            symbol=order.symbol,
            side=OrderSide(order.side.value),
            reason=order.reject_reason or "unknown",
            signal_event_id=order.signal.metadata.get("signal_event_id", ""),
            run_id=order.signal.metadata.get("run_id", ""),
        )
