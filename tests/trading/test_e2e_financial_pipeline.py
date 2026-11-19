# -*- coding: utf-8 -*-
import asyncio
import json
import os
import uuid
from typing import Any, Callable, Coroutine

import pytest
from trading.analytics.trade_tracker import TradeTracker
from trading.infrastructure.kafka.trade_tracker_consumer import TradeTrackerConsumer

from ocm.runtime.state.cursor_store import RedisCursorStore
from shared.kafka.topics import TOPIC_ORDERS_FILLED
from tests.kafka.conftest import _raw_consumer, _raw_producer, _unique_group


async def _wait_until(predicate: Callable[[], Coroutine[Any, Any, bool]], timeout: float = 5.0) -> bool:
    start_time = asyncio.get_event_loop().time()
    while asyncio.get_event_loop().time() - start_time < timeout:
        if await predicate():
            return True
        await asyncio.sleep(0.1)
    return False


@pytest.mark.asyncio
@pytest.mark.integration
async def test_e2e_order_filled_pipeline(redis_store):
    fill_id = f"e2e-{uuid.uuid4().hex}"
    event = {
        "event_id": fill_id,
        "occurred_at": "2026-11-16T20:00:00Z",
        "order_id": "ord-123",
        "exchange": "bybit",
        "symbol": "BTC/USDT",
        "side": "sell",
        "fill_price": 60000.0,
        "size_pct": 0.01,
        "filled_qty": 1.0,
        "closed_qty": 1.0,
        "filled_at": "2026-11-16T20:00:00Z",
        "event_version": 1,
    }


    tracker = TradeTracker(exchange="bybit")
    # Redis para el test debe estar limpio
    redis = redis_store
    redis.flush_test_namespace()
    redis.flush_pattern("trade_tracker:processed:*")

    consumer = await _raw_consumer(_unique_group("e2e"), TOPIC_ORDERS_FILLED)

    # TradeTrackerConsumer usa redis directamente ahora
    trade_tracker_consumer = TradeTrackerConsumer(tracker, consumer, redis)

    producer = await _raw_producer()
    await producer.send_and_wait(TOPIC_ORDERS_FILLED, value=json.dumps(event).encode())
    await producer.stop()

    consumer_task = asyncio.create_task(trade_tracker_consumer.start())

    async def predicate():
        return await redis.exists(f"trade_tracker:processed:{fill_id}")

    try:
        processed = await _wait_until(predicate)
        assert processed, "El evento no fue procesado dentro del tiempo límite"
        assert tracker.trade_count == 1
    finally:
        await trade_tracker_consumer.stop()
        consumer_task.cancel()
        import contextlib
        with contextlib.suppress(asyncio.CancelledError):
            await consumer_task
        await redis.close()
