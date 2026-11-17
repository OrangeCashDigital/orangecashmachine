# -*- coding: utf-8 -*-
from unittest.mock import MagicMock
import pytest
from trading.domain.models.order import Order, OrderSide, OrderStatus
from trading.domain.strategies.base import Signal
from datetime import datetime, timezone
from trading.adapters.outbound.kafka_event_bus import KafkaEventBusAdapter

@pytest.fixture
def mock_producer():
    return MagicMock()

@pytest.fixture
def order():
    signal = Signal(
        symbol="BTC/USDT",
        timeframe="1m",
        direction="buy",
        price=50000.0,
        timestamp=datetime.now(timezone.utc),
        metadata={"exchange": "bybit", "signal_event_id": "sig-1", "run_id": "run-1"}
    )
    return Order(symbol="BTC/USDT", side=OrderSide.BUY, size_pct=0.1, signal=signal)

def test_publish_filled(mock_producer, order):
    adapter = KafkaEventBusAdapter(mock_producer)
    order.transition(OrderStatus.SUBMITTED)
    order.transition(OrderStatus.FILLED, fill_price=51000.0, filled_qty=0.01)
    
    adapter.publish("orders.filled", order)
    
    assert mock_producer.produce.called
    args, kwargs = mock_producer.produce.call_args
    assert kwargs["topic"] == "orders.filled"
    assert kwargs["key"] == order.order_id.encode()

def test_publish_rejected(mock_producer, order):
    adapter = KafkaEventBusAdapter(mock_producer)
    order.transition(OrderStatus.SUBMITTED)
    order.transition(OrderStatus.REJECTED, reject_reason="risk_limit")
    
    adapter.publish("orders.rejected", order)
    
    assert mock_producer.produce.called
    args, kwargs = mock_producer.produce.call_args
    assert kwargs["topic"] == "orders.rejected"

def test_unsupported_topic(mock_producer, order):
    adapter = KafkaEventBusAdapter(mock_producer)
    adapter.publish("unknown.topic", order)
    assert not mock_producer.produce.called
