# -*- coding: utf-8 -*-
"""
tests/market_data/adapters/inbound/websocket/test_orderbook_producer_metrics.py
===============================================================================

F2.6c (instrumentación PREPARADA, no cerrada): snapshot y delta del
OrderBookKafkaProducer deben incrementar las métricas REALES de KafkaMetrics
(ocm_kafka_events_*) registradas en Prometheus REGISTRY.

Se verifica contra el registro Prometheus real (no fakes): un snapshot y un
delta exitosos incrementan published+processed (con latencia), un fallo de
produce incrementa failed con reason="write_error".

Para evitar acarreo de estado entre tests (REGISTRY es global y no se
resetea), se leen los valores ANTES y DESPUÉS de cada operación y se
compara el delta — no el valor absoluto.

Principios: F2.6c contract · instrumented real metrics · no-prometheus-safe
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from market_data.adapters.inbound.websocket.orderbook_producer import (
    OrderBookKafkaProducer,
)
from market_data.infrastructure.kafka.metrics import KafkaMetrics
from prometheus_client import REGISTRY

from shared.kafka.topics import TOPIC_ORDERBOOK_RAW


class _FakeProducer:
    """KafkaProducerPort simulada — produce exitosa controlable."""

    def __init__(self, fail: bool = False) -> None:
        self._fail = fail
        self.start = AsyncMock()
        self.stop = AsyncMock()
        self.produce = AsyncMock(side_effect=self._maybe_fail)

    async def _maybe_fail(self, **kwargs) -> None:
        if self._fail:
            raise RuntimeError("kafka broker unreachable")


def _counter_value(name: str, labels: dict[str, str]) -> float:
    """Valor actual de un contador Prometheus dado, 0.0 si nunca registrado."""
    try:
        return float(REGISTRY.get_sample_value(name, labels) or 0.0)
    except Exception:
        return 0.0


def _counter_delta(
    name: str,
    labels: dict[str, str],
    before: float,
) -> float:
    return _counter_value(name, labels) - before


@pytest.fixture
def metrics() -> KafkaMetrics:
    # Mismo topic que produce el producer (SSOT shared.kafka.topics).
    return KafkaMetrics(topic=TOPIC_ORDERBOOK_RAW)


@pytest.fixture
def producer(metrics: KafkaMetrics) -> OrderBookKafkaProducer:
    fake = _FakeProducer()
    return OrderBookKafkaProducer(producer=fake, metrics=metrics)


# ---------------------------------------------------------------------------
# Labels compartidos — topic canonico del producer + exchange del evento
# ---------------------------------------------------------------------------


def _published_labels(exchange: str = "bybit") -> dict[str, str]:
    return {"topic": TOPIC_ORDERBOOK_RAW, "exchange": exchange}


def _failed_labels(exchange: str = "bybit") -> dict[str, str]:
    return {"topic": TOPIC_ORDERBOOK_RAW, "exchange": exchange, "reason": "write_error"}


# ---------------------------------------------------------------------------
# Snapshot → published + processed incrementan
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestSnapshotMetrics:
    async def test_snapshot_increments_published(self, producer, metrics):
        before = _counter_value("ocm_kafka_events_published_total", _published_labels())
        await producer.on_snapshot(
            exchange="bybit",
            symbol="BTC-USDT-PERP",
            timestamp_ms=1785_000_000_000,
            bids=[("64800.0", "1.5")],
            asks=[("64801.0", "2.0")],
            depth=50,
            checksum=None,
        )
        after = _counter_value("ocm_kafka_events_published_total", _published_labels())
        assert after - before == 1.0

    async def test_snapshot_increments_processed_and_latency(self, producer, metrics):
        before_pub = _counter_value("ocm_kafka_events_published_total", _published_labels())
        before_proc = _counter_value("ocm_kafka_events_processed_total", _published_labels())
        await producer.on_snapshot(
            exchange="bybit",
            symbol="BTC-USDT-PERP",
            timestamp_ms=1786450000000,
            bids=[("64800.0", "1.5")],
            asks=[("64801.0", "2.0")],
        )
        # processed se incrementa junto a published (latencia registrada)
        assert _counter_delta("ocm_kafka_events_published_total", _published_labels(), before_pub) == 1.0
        assert _counter_delta("ocm_kafka_events_processed_total", _published_labels(), before_proc) == 1.0

    async def test_snapshot_failure_increments_failed_write_error(self):
        fake = _FakeProducer(fail=True)
        m = KafkaMetrics(topic=TOPIC_ORDERBOOK_RAW)
        producer = OrderBookKafkaProducer(producer=fake, metrics=m)
        before = _counter_value("ocm_kafka_events_failed_total", _failed_labels())
        await producer.on_snapshot(
            exchange="bybit",
            symbol="BTC-USDT-PERP",
            timestamp_ms=1786450000000,
            bids=[],
            asks=[],
        )
        after = _counter_value("ocm_kafka_events_failed_total", _failed_labels())
        assert after - before == 1.0


# ---------------------------------------------------------------------------
# Delta → published_total incrementa
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestDeltaMetrics:
    async def test_delta_increments_published(self, producer, metrics):
        before_pub = _counter_value("ocm_kafka_events_published_total", _published_labels())
        await producer.on_delta(
            exchange="bybit",
            symbol="BTC-USDT-PERP",
            timestamp_ms=1786450000001,
            bids=[("64800.0", "0")],
            asks=[("64801.0", "1.5")],
            update_id=500,
        )
        assert _counter_delta("ocm_kafka_events_published_total", _published_labels(), before_pub) == 1.0

    async def test_delta_failure_increments_error_write_error(self):
        fake = _FakeProducer(fail=True)
        m = KafkaMetrics(topic=TOPIC_ORDERBOOK_RAW)
        producer = OrderBookKafkaProducer(producer=fake, metrics=m)
        before = _counter_value("ocm_kafka_events_failed_total", _failed_labels())
        await producer.on_delta(
            exchange="bybit",
            symbol="BTC-USDT-PERP",
            timestamp_ms=1786450000001,
            bids=[("64801.0", "0")],
            asks=[],
            update_id=501,
        )
        after = _counter_value("ocm_kafka_events_failed_total", _failed_labels())
        assert after - before == 1.0


# ---------------------------------------------------------------------------
# Inyección opcional de KafkaMetrics (patrón bronze_writer)
# ---------------------------------------------------------------------------


class TestMetricsInjection:
    def test_default_metrics_created_from_topic(self):
        producer = OrderBookKafkaProducer(producer=_FakeProducer())
        assert producer._metrics._topic == TOPIC_ORDERBOOK_RAW

    def test_metrics_injected_is_used(self):
        fake = _FakeProducer()
        m = KafkaMetrics(topic=TOPIC_ORDERBOOK_RAW)
        producer = OrderBookKafkaProducer(producer=fake, metrics=m)
        assert producer._metrics is m
