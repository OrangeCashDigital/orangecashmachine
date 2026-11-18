# -*- coding: utf-8 -*-
"""
E2E real (Kafka): orders.filled → TradeTracker → Settlement.

Reglas de este módulo
---------------------
- Kafka es REAL: producer/consumer vía harness tests/kafka/conftest.py.
- InMemoryCursorStore se usa SOLO como dedupe store, nunca como broker.
- Sin mocks, XFAIL, SKIP ni aserciones debilitadas.

Cubrimiento financiero
----------------------
- BUY no abre posición (OMS/fill_sync ya lo hizo) y se deduplica.
- SELL con fees known → Settlement y TradeRecord con net correcto.
- SELL con fees UNKNOWN → net None (UNKNOWN ≠ 0).
- closed_qty nunca supera la posición abierta (INV-08).
- Replay por event_id no duplica el trade.
- SELL sin posición no se marca y NO confirma offset.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Awaitable, Callable, Literal, Optional

import pytest
from portfolio.models.position import PositionSnapshot
from trading.analytics.trade_tracker import TradeTracker
from trading.infrastructure.kafka.trade_tracker_consumer import TradeTrackerConsumer

from ocm.runtime.state.cursor_store import InMemoryCursorStore
from shared.kafka.schemas.orders import OrderFilledPayload
from shared.kafka.topics import TOPIC_ORDERS_FILLED
from tests.kafka.conftest import (
    _raw_consumer,
    _raw_producer,
    _unique_group,
    warm_up_consumer,
)

_TIMEOUT_S = 20.0


class _FakePortfolio:
    def __init__(self, *positions: PositionSnapshot) -> None:
        self.positions = list(positions)

    def snapshot(self) -> SimpleNamespace:
        return SimpleNamespace(positions=self.positions)


def _filled(
    *,
    event_id: str,
    symbol: str,
    side: Literal["buy", "sell"],
    order_id: str,
    fill_price: float,
    closed_qty: float,
    avg_entry_price: Optional[float],
    fee_amount_usd: Optional[float] = 1.0,
    fee_status: str = "known",
) -> dict:
    return OrderFilledPayload(
        event_id=event_id,
        occurred_at="2026-11-17T04:00:00+00:00",
        order_id=order_id,
        exchange="bybit",
        symbol=symbol,
        side=side,
        fill_price=fill_price,
        size_pct=0.01,
        filled_qty=closed_qty,
        fee_amount_usd=fee_amount_usd,
        fee_currency="USDT",
        fee_status=fee_status,
        timeframe="1h",
        closed_qty=closed_qty,
        avg_entry_price=avg_entry_price,
        remaining_qty=0.0,
        filled_at="2026-11-17T04:10:00+00:00",
        signal_event_id="sig-e2e",
        run_id="run-e2e",
    ).to_dict()


def _position(symbol: str, order_id: str, quantity: float, avg_entry: float) -> PositionSnapshot:
    return PositionSnapshot(
        symbol=symbol,
        exchange="bybit",
        side="long",
        quantity=quantity,
        avg_entry=avg_entry,
        size_pct=0.01,
        entry_at=datetime(2026, 11, 17, 4, 0, tzinfo=timezone.utc),
        order_id=order_id,
    )


async def _wait_until(check: Callable[[], Awaitable[bool]], message: str, *, timeout_s: float = _TIMEOUT_S) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        if await check():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"timeout: {message}")


def _assigned_partition(consumer: object, topic: str, partition: int) -> object:
    # AIOKafkaConsumer.assignment() es síncrono; helpers tipan el consumer como object.
    for tp in consumer.assignment():  # type: ignore[attr-defined]
        if tp.topic == topic and tp.partition == partition:
            return tp
    raise AssertionError(f"sin partición asignada para {topic}:{partition}")


async def _committed(consumer: object, tp: object) -> Optional[int]:
    # AIOKafkaConsumer.committed() es async y devuelve Optional[int] (API 0.14).
    return await consumer.committed(tp)  # type: ignore[attr-defined,no-any-return]


async def _ge_committed(consumer: object, tp: object, minimum: int) -> bool:
    value = await _committed(consumer, tp)
    return value is not None and value >= minimum


async def _position_offset(consumer: object, tp: object) -> int:
    # AIOKafkaConsumer.position() es async; devuelve el siguiente offset local.
    return int(await consumer.position(tp))  # type: ignore[attr-defined]


async def _ge_position(consumer: object, tp: object, minimum: int) -> bool:
    return await _position_offset(consumer, tp) >= minimum


async def _result(value: bool) -> bool:
    return value


async def _broker_ids(consumer: object, expected: set[str], *, timeout_s: float = _TIMEOUT_S) -> set[str]:
    found: set[str] = set()
    deadline = asyncio.get_running_loop().time() + timeout_s
    while found < expected and asyncio.get_running_loop().time() < deadline:
        # AIOKafkaConsumer.getmany() es async; el harness expone el consumer crudo.
        batches = await consumer.getmany(timeout_ms=500, max_records=200)  # type: ignore[attr-defined]
        for records in batches.values():
            for record in records:
                try:
                    payload = json.loads(record.value)
                except (TypeError, ValueError):
                    continue
                event_id = payload.get("event_id")
                if event_id in expected:
                    found.add(event_id)
    return found


@pytest.mark.asyncio
@pytest.mark.integration
async def test_e2e_order_filled_pipeline() -> None:
    suffix = uuid.uuid4().hex
    btc = f"BTC-{suffix}/USDT"
    eth = f"ETH-{suffix}/USDT"
    missing = f"MISSING-{suffix}/USDT"
    key = f"trade-tracker-e2e-{suffix}".encode()

    buy_btc_id = f"e2e-buy-btc-{suffix}"
    sell_btc_id = f"e2e-sell-btc-{suffix}"
    buy_eth_id = f"e2e-buy-eth-{suffix}"
    sell_eth_id = f"e2e-sell-eth-{suffix}"
    overclose_id = f"e2e-overclose-{suffix}"
    missing_id = f"e2e-missing-{suffix}"

    tracker = TradeTracker(
        exchange="bybit",
        portfolio=_FakePortfolio(
            _position(btc, "ord-buy-btc", 0.5, 60_000.0),
            _position(eth, "ord-buy-eth", 2.0, 2_000.0),
        ),
    )
    dedupe = InMemoryCursorStore()

    # Consumidor real de la app: latest + warm-up para no leer历史事件 del topic.
    app_consumer = await _raw_consumer(_unique_group("e2e-app"), TOPIC_ORDERS_FILLED, offset_reset="latest")
    verifier = await _raw_consumer(_unique_group("e2e-verify"), TOPIC_ORDERS_FILLED, offset_reset="latest")
    producer = await _raw_producer()
    task: Optional[asyncio.Task[None]] = None
    try:
        await warm_up_consumer(app_consumer)
        await warm_up_consumer(verifier)

        events = [
            _filled(
                event_id=buy_btc_id,
                symbol=btc,
                side="buy",
                order_id="ord-buy-btc",
                fill_price=60_000.0,
                closed_qty=0.5,
                avg_entry_price=60_000.0,
            ),
            _filled(
                event_id=sell_btc_id,
                symbol=btc,
                side="sell",
                order_id="ord-sell-btc",
                fill_price=61_000.0,
                closed_qty=0.5,
                avg_entry_price=60_000.0,
            ),
            _filled(
                event_id=buy_eth_id,
                symbol=eth,
                side="buy",
                order_id="ord-buy-eth",
                fill_price=2_000.0,
                closed_qty=2.0,
                avg_entry_price=2_000.0,
            ),
            _filled(
                event_id=sell_eth_id,
                symbol=eth,
                side="sell",
                order_id="ord-sell-eth",
                fill_price=2_100.0,
                closed_qty=2.0,
                avg_entry_price=2_000.0,
                fee_amount_usd=None,
                fee_status="unknown",
            ),
        ]
        metas = []
        for event in events:
            meta = await producer.send_and_wait(
                TOPIC_ORDERS_FILLED,
                key=key,
                value=json.dumps(event).encode(),
            )
            assert meta.topic == TOPIC_ORDERS_FILLED
            assert meta.offset >= 0
            metas.append(meta)

        tp = _assigned_partition(app_consumer, TOPIC_ORDERS_FILLED, metas[0].partition)

        # El broker confirma recepción y un consumidor independiente lee los payloads.
        seen = await _broker_ids(verifier, {e["event_id"] for e in events})
        assert seen == {e["event_id"] for e in events}

        app = TradeTrackerConsumer(
            tracker,
            app_consumer,
            dedupe,
            consumer_started=True,  # harness ya hizo start()
        )
        task = asyncio.create_task(app.start())

        await _wait_until(
            lambda: _result(tracker.trade_count == 2),
            "dos cierres financieros registrados desde Kafka",
        )
        committed_after_success = max(int(m.offset) for m in metas) + 1
        await _wait_until(
            lambda: _ge_committed(app_consumer, tp, committed_after_success),
            "offset confirmado tras el procesamiento correcto",
        )

        trades = {trade.trade_id: trade for trade in tracker.closed_trades}
        assert set(trades) == {"ord-buy-btc", "ord-buy-eth"}

        btc_trade = trades["ord-buy-btc"]
        assert btc_trade.symbol == btc
        assert btc_trade.closed_qty == pytest.approx(0.5)
        assert btc_trade.gross_realized_usd == pytest.approx(500.0)
        assert btc_trade.net_realized_usd == pytest.approx(499.0)

        eth_trade = trades["ord-buy-eth"]
        assert eth_trade.symbol == eth
        assert eth_trade.closed_qty == pytest.approx(2.0)
        assert eth_trade.gross_realized_usd == pytest.approx(200.0)
        assert eth_trade.net_realized_usd is None, "UNKNOWN no debe convertirse en 0"

        # Replay del mismo event_id: el broker lo recibe, se consume, no se duplica.
        replay_meta = await producer.send_and_wait(
            TOPIC_ORDERS_FILLED,
            key=key,
            value=json.dumps(events[1]).encode(),
        )
        await _wait_until(
            lambda: _ge_position(app_consumer, tp, replay_meta.offset + 1),
            "replay consumido",
        )
        await _wait_until(
            lambda: _ge_committed(app_consumer, tp, replay_meta.offset + 1),
            "offset confirmado tras replay deduplicado",
        )
        assert tracker.trade_count == 2, "replay no debe duplicar el Settlement"
        assert dedupe.get_raw(f"order:seen:{sell_btc_id}") == "processed"

        # INV-08: closed_qty mayor que la posición se capa; los fees del wire no se inventan.
        overclose_meta = await producer.send_and_wait(
            TOPIC_ORDERS_FILLED,
            key=key,
            value=json.dumps(
                _filled(
                    event_id=overclose_id,
                    symbol=btc,
                    side="sell",
                    order_id="ord-sell-over",
                    fill_price=61_000.0,
                    closed_qty=1.0,
                    avg_entry_price=60_000.0,
                )
            ).encode(),
        )
        await _wait_until(
            lambda: _ge_position(app_consumer, tp, overclose_meta.offset + 1),
            "overclose consumido",
        )
        await _wait_until(
            lambda: _ge_committed(app_consumer, tp, overclose_meta.offset + 1),
            "offset confirmado tras overclose capado",
        )
        assert tracker.trade_count == 3
        over_trade = tracker.last_trade()
        assert over_trade is not None
        assert over_trade.closed_qty == pytest.approx(0.5)
        assert over_trade.gross_realized_usd == pytest.approx(500.0)

        # SELL sin posición: no se marca, no se confirma offset.
        committed_before_failure = await _committed(app_consumer, tp)
        assert committed_before_failure is not None
        failure_meta = await producer.send_and_wait(
            TOPIC_ORDERS_FILLED,
            key=key,
            value=json.dumps(
                _filled(
                    event_id=missing_id,
                    symbol=missing,
                    side="sell",
                    order_id="ord-sell-missing",
                    fill_price=100.0,
                    closed_qty=1.0,
                    avg_entry_price=90.0,
                )
            ).encode(),
        )
        await _wait_until(
            lambda: _ge_position(app_consumer, tp, failure_meta.offset + 1),
            "SELL sin posición consumido",
        )
        await asyncio.sleep(1.5)  # dar tiempo al ciclo de poll/procesamiento
        assert tracker.trade_count == 3
        assert dedupe.get_raw(f"order:seen:{missing_id}") is None
        assert await _committed(app_consumer, tp) == committed_before_failure

        await app.stop()
        await asyncio.wait_for(task, timeout=5)
        task = None
    finally:
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await producer.stop()
        await verifier.stop()
        await app_consumer.stop()
