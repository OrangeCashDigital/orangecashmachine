# -*- coding: utf-8 -*-
import json
from typing import Any
from loguru import logger
from trading.analytics.trade_tracker import TradeTracker
from trading.domain.models.order import Order, OrderSide
from ocm.runtime.state.cursor_store import CursorStorePort
from shared.kafka.topics import TOPIC_ORDERS_FILLED, GROUP_PORTFOLIO
from shared.kafka.schemas.orders import OrderFilledPayload

class TradeTrackerConsumer:
    def __init__(self, tracker: TradeTracker, consumer: Any, cursor_store: CursorStorePort):
        self._tracker = tracker
        self._consumer = consumer  # KafkaConsumerAdapter
        self._cursor_store = cursor_store # Deduplicación persistente
        self._running = False

    async def start(self):
        self._running = True
        await self._consumer.start()
        while self._running:
            messages = await self._consumer.poll(timeout_ms=1000)
            for msg in messages:
                await self._on_event(json.loads(msg.value))
            await self._consumer.commit()

    async def stop(self):
        self._running = False
        await self._consumer.close()

    async def _on_event(self, event: dict):
        payload = OrderFilledPayload.from_dict(event)
        fill_id = payload.event_id
        
        # Deduplicación persistente
        if self._cursor_store.get(fill_id):
            return
        
        try:
            from trading.domain.models.order import OrderSide, OrderStatus
            from datetime import datetime

            order = Order(
                symbol=payload.symbol,
                side=OrderSide(payload.side.value),
                size_pct=payload.size_pct,
                quantity=payload.fill_price,
            )
            order.exchange_order_id = payload.order_id
            
            order.transition(
                OrderStatus.FILLED, 
                fill_price=payload.fill_price, 
                filled_qty=payload.size_pct,
                fill_timestamp=datetime.fromisoformat(payload.filled_at) if payload.filled_at else None
            )
            
            self._tracker.on_fill(order)
            self._cursor_store.set(fill_id, b"processed")
        except Exception as exc:
            logger.error("Error procesando fill | fill_id={} error={}", fill_id, exc)
