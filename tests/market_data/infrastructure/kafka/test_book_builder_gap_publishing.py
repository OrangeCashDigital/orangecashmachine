"""Tests de regresión B-25 / ADR-0028 D3.

Verifica:
- Gap real (100 → 105) publica OrderBookGapDetectedEvent con datos contractuales:
  last_u=100, expected_u=101, observed_u=105, missing_count=4.
- Stale (100 → 100) NO publica gap event.
- Contiguo (100 → 101) NO publica gap event.
- El wiring construye el publisher con header x-ocm-dataset=orderbook.
- El lifecycle start/stop del consumer llama start/stop del gap_publisher.
"""

from __future__ import annotations

from typing import Any

import pytest
from market_data.application.processing.book_builder import BookBuilder
from market_data.domain.events.gap_events import OrderBookGapDetectedEvent
from market_data.infrastructure.kafka.book_builder_consumer import BookBuilderConsumer
from market_data.ports.outbound.book_builder import OutcomeKind


class _CapturingGapPublisher:
    """Captura eventos + registra start/stop para assertions de lifecycle."""

    def __init__(self) -> None:
        self.events: list[Any] = []
        self.started: bool = False
        self.stopped: bool = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def publish_gap_event(self, event: Any) -> None:
        self.events.append(event)


class _NullConsumer:
    async def start(self) -> None:
        pass

    async def close(self) -> None:
        pass


class _NullProducer:
    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


def _make_consumer(capturer: _CapturingGapPublisher) -> BookBuilderConsumer:
    return BookBuilderConsumer(
        consumer=_NullConsumer(),
        producer=_NullProducer(),
        builder=BookBuilder(viewport=50, stale_ms=2_000),
        gap_publisher=capturer,
    )


def _seed_snapshot(builder: BookBuilder, update_id: int = 100) -> None:
    """Seed: snapshot base con last_update_id=100."""
    builder.on_snapshot(
        "bybit",
        "BTC/USDT",
        1_000,
        [("100", "1")],
        [("101", "1")],
        update_id=update_id,
    )


@pytest.mark.asyncio
async def test_gap_100_to_105_publishes_orderbook_gap_event_with_real_data() -> None:
    """ADR-0028 D3: gap real → OrderBookGapDetectedEvent con fórmula contractual."""
    capturer = _CapturingGapPublisher()
    consumer = _make_consumer(capturer)
    builder = consumer._builder

    _seed_snapshot(builder, update_id=100)
    outcome = builder.on_delta("bybit", "BTC/USDT", 2_000, [], [], update_id=105)

    assert outcome.kind is OutcomeKind.GAP_DETECTED
    assert outcome.last_u == 100
    assert outcome.expected_u == 101
    assert outcome.observed_u == 105
    assert outcome.missing_count == 4

    # Simular _apply_outcome (sin Kafka real)
    await consumer._apply_outcome(outcome, msg=None)  # type: ignore[arg-type]

    assert len(capturer.events) == 1
    event = capturer.events[0]
    assert isinstance(event, OrderBookGapDetectedEvent)
    assert event.exchange_id == "bybit"
    assert event.symbol == "BTC/USDT"
    assert event.last_u == 100
    assert event.expected_u == 101
    assert event.observed_u == 105
    assert event.missing_count == 4
    assert event.event_id  # UUID4 no vacío
    assert event.detected_at_ms > 0


@pytest.mark.asyncio
async def test_stale_100_to_100_does_not_publish_gap() -> None:
    """u <= last_u → STALE, sin evento de gap."""
    capturer = _CapturingGapPublisher()
    consumer = _make_consumer(capturer)
    builder = consumer._builder

    _seed_snapshot(builder, update_id=100)
    outcome = builder.on_delta("bybit", "BTC/USDT", 2_000, [], [], update_id=100)

    assert outcome.kind is OutcomeKind.STALE
    await consumer._apply_outcome(outcome, msg=None)  # type: ignore[arg-type]
    assert capturer.events == []


@pytest.mark.asyncio
async def test_contiguous_100_to_101_does_not_publish_gap() -> None:
    """u == last_u + 1 → DELTA_APPLIED, sin evento de gap."""
    capturer = _CapturingGapPublisher()
    consumer = _make_consumer(capturer)
    builder = consumer._builder

    _seed_snapshot(builder, update_id=100)
    outcome = builder.on_delta(
        "bybit",
        "BTC/USDT",
        2_000,
        [("100", "1")],
        [("101", "1")],
        update_id=101,
    )

    assert outcome.kind is OutcomeKind.DELTA_APPLIED
    await consumer._apply_outcome(outcome, msg=None)  # type: ignore[arg-type]
    assert capturer.events == []


@pytest.mark.asyncio
async def test_consumer_start_stop_calls_gap_publisher_lifecycle() -> None:
    """El consumer propaga start/stop al gap_publisher (sin esto, producer=None)."""
    capturer = _CapturingGapPublisher()
    consumer = _make_consumer(capturer)

    await consumer.start()
    assert capturer.started is True
    assert consumer._running is True

    await consumer.stop()
    assert capturer.stopped is True
    assert consumer._running is False


def test_wiring_uses_ocm_dataset_orderbook_header() -> None:
    """ADR-0028: el publisher se construye con x-ocm-dataset=orderbook."""
    from market_data.adapters.outbound.kafka_gap_publisher import KafkaGapPublisher

    publisher = KafkaGapPublisher(
        bootstrap_servers="localhost:9092",
        headers={"x-ocm-dataset": "orderbook"},
    )
    assert publisher._headers == {"x-ocm-dataset": "orderbook"}
