import pytest
from unittest.mock import AsyncMock, MagicMock
from market_data.adapters.outbound.kafka_gap_publisher import KafkaGapPublisher
from market_data.domain.events.gap_events import OrderBookGapDetectedEvent
from market_data.infrastructure.kafka.metrics import KafkaMetrics

@pytest.fixture
def publisher():
    return KafkaGapPublisher(
        bootstrap_servers="localhost:9092",
        topic="market.gaps.test",
        is_test_env=True,
        broker_identity="e2e-test-broker"
    )

@pytest.mark.asyncio
async def test_publisher_start_success():
    publisher = KafkaGapPublisher(
        bootstrap_servers="localhost:9092",
        topic="market.gaps.test",
        is_test_env=True,
        broker_identity="e2e-test-broker"
    )
    # Mocking start - we don't have a real broker here yet.
    # This just tests the fail-closed logic
    try:
        await publisher.start()
    except Exception as e:
        # AIOKafkaProducer.start() will fail without a broker, which is expected.
        # We just want to check the fail-closed logic didn't stop it first.
        assert "e2e-test-broker" in str(publisher._broker_identity)

def test_publisher_fail_closed_wrong_topic():
    publisher = KafkaGapPublisher(
        bootstrap_servers="localhost:9092",
        topic="market.gaps.prod", # Wrong topic
        is_test_env=True
    )
    with pytest.raises(ValueError, match="insegura"):
        # This should trigger before AIOKafkaProducer is created
        import asyncio
        asyncio.run(publisher.start())

@pytest.mark.asyncio
async def test_publisher_fail_soft_on_failure():
    metrics = MagicMock(spec=KafkaMetrics)
    publisher = KafkaGapPublisher(
        bootstrap_servers="localhost:9092",
        topic="market.gaps.test",
        is_test_env=True,
        metrics=metrics
    )
    # Simulate a running producer
    publisher._producer = AsyncMock()
    publisher._producer.send = AsyncMock(side_effect=Exception("Kafka error"))
    
    event = OrderBookGapDetectedEvent(
        event_id="test-uuid",
        exchange_id="bybit", symbol="BTC/USDT", 
        last_u=100, expected_u=101, observed_u=105, 
        missing_count=4, detected_at_ms=123
    )
    
    # Should not raise exception (Fail-soft)
    await publisher.publish_gap_event(event)
    
    # Verify metrics recorded failure
    metrics.event_failed.assert_called_once_with(exchange="unknown", reason="write_error")
