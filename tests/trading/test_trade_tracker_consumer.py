# -*- coding: utf-8 -*-
"""
Tests unitarios de TradeTrackerConsumer — sin Kafka, sin Redis.

Cubre la ruta canónica: Settlement.compute → TradeTracker.record_settlement,
la deduplicación persistente por event_id y el no-doble-conteo de BUYs.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from portfolio.models.position import PositionSnapshot
from trading.analytics.trade_tracker import TradeTracker
from trading.infrastructure.kafka.trade_tracker_consumer import TradeTrackerConsumer

from shared.kafka.schemas.orders import OrderFilledPayload

_ENTRY_AT = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)


class _MemoryDedupe:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.ttls: dict[str, int] = {}

    def get_raw(self, key: str) -> str | None:
        return self.values.get(key)

    def set_raw(self, key: str, value: str, ttl_seconds: int) -> None:
        self.values[key] = value
        self.ttls[key] = ttl_seconds


class _FakePortfolio:
    def __init__(self, *positions: PositionSnapshot) -> None:
        self.positions = list(positions)

    def snapshot(self) -> SimpleNamespace:
        return SimpleNamespace(positions=self.positions)


def _position() -> PositionSnapshot:
    return PositionSnapshot(
        symbol="BTC/USDT",
        exchange="bybit",
        side="long",
        quantity=0.5,
        avg_entry=60_000.0,
        size_pct=0.01,
        entry_at=_ENTRY_AT,
        order_id="buy-1",
    )


def _filled(
    *,
    side: str = "sell",
    event_id: str = "fill-1",
    closed_qty: float = 0.5,
    fee_status: str = "known",
) -> dict:
    return OrderFilledPayload(
        event_id=event_id,
        occurred_at="2026-01-01T00:10:00+00:00",
        order_id="sell-1",
        exchange="bybit",
        symbol="BTC/USDT",
        side=side,  # type: ignore[arg-type]
        fill_price=61_000.0,
        size_pct=0.01,
        filled_qty=0.5,
        fee_amount_usd=1.0,
        fee_currency="USDT",
        fee_status=fee_status,
        timeframe="1h",
        closed_qty=closed_qty,
        avg_entry_price=60_000.0,
        remaining_qty=0.0,
        filled_at="2026-01-01T00:10:00+00:00",
        signal_event_id="sig-1",
        run_id="run-1",
    ).to_dict()


def _consumer(with_position: bool = True) -> tuple[TradeTrackerConsumer, TradeTracker, _MemoryDedupe]:
    tracker = TradeTracker(exchange="bybit", portfolio=_FakePortfolio(*([_position()] if with_position else [])))
    dedupe = _MemoryDedupe()
    return TradeTrackerConsumer(tracker, SimpleNamespace(), dedupe), tracker, dedupe


@pytest.mark.asyncio
async def test_buy_is_deduplicated_and_does_not_open_position() -> None:
    consumer, tracker, dedupe = _consumer()
    await consumer._on_event(_filled(side="buy"))
    assert tracker.trade_count == 0
    assert dedupe.values == {"order:seen:fill-1": "processed"}


@pytest.mark.asyncio
async def test_sell_books_settlement_and_marks_event() -> None:
    consumer, tracker, dedupe = _consumer()
    await consumer._on_event(_filled())

    assert tracker.trade_count == 1
    trade = tracker.last_trade()
    assert trade is not None
    assert trade.trade_id == "buy-1"
    assert trade.closed_qty == 0.5
    assert trade.gross_realized_usd == pytest.approx(500.0)
    assert trade.net_realized_usd == pytest.approx(499.0)
    assert dedupe.values == {"order:seen:fill-1": "processed"}
    assert dedupe.ttls["order:seen:fill-1"] == 30 * 24 * 60 * 60


@pytest.mark.asyncio
async def test_duplicate_sell_is_not_booked_twice() -> None:
    consumer, tracker, dedupe = _consumer()
    event = _filled()
    await consumer._on_event(event)
    await consumer._on_event(event)
    assert tracker.trade_count == 1
    assert len(dedupe.values) == 1


@pytest.mark.asyncio
async def test_sell_without_open_position_is_retryable() -> None:
    consumer, tracker, dedupe = _consumer(with_position=False)
    await consumer._on_event(_filled())
    assert tracker.trade_count == 0
    assert dedupe.values == {}


@pytest.mark.asyncio
async def test_malformed_payload_is_discarded_without_raising() -> None:
    consumer, tracker, dedupe = _consumer()
    await consumer._on_event({"event_id": "bad"})
    assert tracker.trade_count == 0
    assert dedupe.values == {}


@pytest.mark.asyncio
async def test_unknown_fee_does_not_become_zero() -> None:
    consumer, tracker, dedupe = _consumer()
    await consumer._on_event(_filled(fee_status="unknown"))
    trade = tracker.last_trade()
    assert trade is not None
    assert trade.gross_realized_usd == pytest.approx(500.0)
    assert trade.net_realized_usd is None
    assert dedupe.values == {"order:seen:fill-1": "processed"}


@pytest.mark.asyncio
async def test_legacy_v1_without_closed_qty_falls_back_to_position_quantity() -> None:
    consumer, tracker, dedupe = _consumer()
    await consumer._on_event(_filled(closed_qty=0.0))
    trade = tracker.last_trade()
    assert trade is not None
    assert trade.closed_qty == 0.5


@pytest.mark.asyncio
async def test_closed_qty_is_capped_to_open_position() -> None:
    """INV-08: un fill que报告 más cantidad no puede sobrecerrar la posición."""
    consumer, tracker, dedupe = _consumer()
    await consumer._on_event(_filled(closed_qty=1.0))
    trade = tracker.last_trade()
    assert trade is not None
    assert trade.closed_qty == 0.5
    assert trade.gross_realized_usd == pytest.approx(500.0)


@pytest.mark.asyncio
async def test_process_batch_reports_retryable_failure_for_missing_position() -> None:
    consumer, tracker, dedupe = _consumer(with_position=False)
    msg = SimpleNamespace(value=json.dumps(_filled()).encode())
    assert await consumer._process_batch([msg]) is False
    assert tracker.trade_count == 0
    assert dedupe.values == {}


@pytest.mark.asyncio
async def test_process_batch_accepts_buy_and_rejects_invalid_json_without_retry() -> None:
    consumer, tracker, dedupe = _consumer()
    messages = [
        SimpleNamespace(value=json.dumps(_filled(side="buy")).encode()),
        SimpleNamespace(value=b"not-json"),
    ]
    assert await consumer._process_batch(messages) is True
    assert tracker.trade_count == 0
    assert dedupe.values == {"order:seen:fill-1": "processed"}


@pytest.mark.asyncio
async def test_commit_is_skipped_on_retryable_batch() -> None:
    consumer, tracker, dedupe = _consumer(with_position=False)
    commits: list[int] = []

    class _Consumer:
        async def start(self) -> None:
            return None

        async def poll(self, timeout_ms: int) -> list[SimpleNamespace]:
            consumer._running = False
            return [SimpleNamespace(value=json.dumps(_filled()).encode())]

        async def commit(self) -> None:
            commits.append(1)

        async def close(self) -> None:
            return None

    consumer._consumer = _Consumer()
    await consumer.start()
    assert commits == []


@pytest.mark.asyncio
async def test_commit_runs_after_successful_batch() -> None:
    consumer, tracker, dedupe = _consumer()
    commits: list[int] = []

    class _Consumer:
        async def start(self) -> None:
            return None

        async def poll(self, timeout_ms: int) -> list[SimpleNamespace]:
            if commits:
                consumer._running = False
                return []
            return [SimpleNamespace(value=json.dumps(_filled()).encode())]

        async def commit(self) -> None:
            commits.append(1)

        async def close(self) -> None:
            return None

    consumer._consumer = _Consumer()
    await consumer.start()
    assert commits == [1]
    assert tracker.trade_count == 1
