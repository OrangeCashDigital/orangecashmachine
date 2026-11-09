import asyncio
import json
import uuid

import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from market_data.adapters.outbound.kafka_gap_publisher import KafkaGapPublisher
from market_data.domain.events.gap_events import OrderBookGapDetectedEvent

pytestmark = pytest.mark.integration

# Broker real dedicado
BOOTSTRAP_SERVERS = "localhost:9093"
EXPECTED_CLUSTER_ID = "BzDDULoJTiyazxHGjtSIhg"
TOPIC_NAME = "market.gaps.test"

@pytest.fixture(scope="module", autouse=True)
async def ensure_topic_exists():
    """Asegura la existencia del tópico antes de las pruebas."""
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    await admin.start()
    try:
        topics = await admin.list_topics()
        if TOPIC_NAME not in topics:
            await admin.create_topics([
                NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1)
            ])
    finally:
        await admin.close()

@pytest.mark.asyncio
async def test_publisher_e2e_real_kafka():
    """
    Test de integración real contra broker local.
    Verifica: Conexión -> Verificación ID -> Publicación -> Consumo.
    """
    event_id = str(uuid.uuid4())
    
    # 1. Setup Publisher
    publisher = KafkaGapPublisher(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic=TOPIC_NAME,
        headers={"x-ocm-dataset": "orderbook"},
        is_test_env=True,
        broker_identity=EXPECTED_CLUSTER_ID
    )
    await publisher.start()
    
    # 2. Setup Consumer
    consumer = AIOKafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=f"test-group-{uuid.uuid4()}",
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    await consumer.start()
    
    try:
        # 3. Publish Event
        event = OrderBookGapDetectedEvent(
            event_id=event_id,
            exchange_id="bybit", 
            symbol="BTC/USDT",
            last_u=100, 
            expected_u=101, 
            observed_u=105,
            missing_count=4, 
            detected_at_ms=123456789
        )
        await publisher.publish_gap_event(event)
        
        # 4. Verify Consumption
        # Consumir hasta encontrar el evento publicado para evitar residuos de pruebas previas
        found = False
        payload = None
        msg = None
        for _ in range(10): # Timeout seguro
            try:
                msg = await asyncio.wait_for(consumer.getone(), timeout=2.0)
                payload = msg.value
                if payload["event_id"] == event_id:
                    found = True
                    break
            except asyncio.TimeoutError:
                break
        
        assert found, f"Event {event_id} not found in topic {TOPIC_NAME}"
        assert payload["event_type"] == "orderbook.gap.detected"
        
        # Validate header
        headers = {k: v.decode() if isinstance(v, bytes) else v for k, v in msg.headers}
        assert headers.get("x-ocm-dataset") == "orderbook"
        
    finally:
        await publisher.stop()
        await consumer.stop()

@pytest.mark.asyncio
async def test_publisher_identity_lock_fails():
    """
    Verifica que el publisher lanza ValueError si el cluster_id no coincide (Broker Lock).
    """
    publisher = KafkaGapPublisher(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic=TOPIC_NAME,
        is_test_env=True,
        broker_identity="WRONG_ID_123"
    )
    
    with pytest.raises(ValueError, match="Broker identity mismatch"):
        await publisher.start()
    
    await publisher.stop()
