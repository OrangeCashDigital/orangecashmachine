# -*- coding: utf-8 -*-
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from trading.adapters.outbound.kafka_event_bus import KafkaEventBusAdapter
from trading.domain.models.order import Order, OrderSide, OrderStatus
from trading.domain.strategies.base import Signal

from shared.types.values import Price


@pytest.fixture
def mock_producer():
    producer = MagicMock()
    producer.produce = MagicMock()
    return producer


@pytest.fixture
def order():
    signal = Signal(
        symbol="BTC/USDT",
        timeframe="1m",
        direction="buy",
        price=Price(Decimal("60000")),
        timestamp=datetime.now(timezone.utc),
        metadata={"exchange": "bybit", "signal_event_id": "sig-1", "run_id": "run-1"},
    )
    return Order(symbol="BTC/USDT", side=OrderSide.BUY, size_pct=0.1, signal=signal)


def test_publish_filled_missing_price_raises(mock_producer, order):
    adapter = KafkaEventBusAdapter(mock_producer)
    order.transition(OrderStatus.SUBMITTED)
    # Simular fill sin precio (esto debería ser impedido por el OMS, pero el adaptador debe ser robusto)
    order.transition(OrderStatus.FILLED, fill_price=None, filled_qty=0.01)

    with pytest.raises(ValueError, match="missing fill_price"):
        adapter.publish("orders.filled", order)

    assert not mock_producer.produce.called
