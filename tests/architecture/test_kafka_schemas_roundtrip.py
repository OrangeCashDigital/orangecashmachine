"""
tests/architecture/test_kafka_schemas_roundtrip (C5/F2.3) — policy tests de wire schemas Kafka.

Cubre los 8 módulos de `shared/kafka/schemas/` que estaban a 0 % de cobertura:
  liquidations, ohlcv, oi, orderbook, orders, positions, signals, trades.

Verifica (F2.3 del Plan Maestro §4):
  1. round-trip serialize() -> deserialize() preserva cada payload.
  2. envelope común (event_id | event_version | occurred_at) presente.
  3. event_version coincide con SCHEMA_VERSION y sobrevive el viaje.
  4. SchemaVersionError se eleva ante un event_version incompatible (fail-fast).
  5. el tópico canónico de cada payload existe en shared.kafka.topics (BC-29).

No es un test de lógica de negocio: es un test de *contrato de transporte Kafka*.
"""

from __future__ import annotations

import json

import pytest

from shared.kafka.schemas._base import BasePayload, SchemaVersionError
from shared.kafka.schemas.liquidations import LiquidationPayload
from shared.kafka.schemas.ohlcv import EventPayload, KafkaOHLCVBar
from shared.kafka.schemas.oi import OpenInterestPayload
from shared.kafka.schemas.orderbook import OrderBookSnapshotPayload
from shared.kafka.schemas.orders import OrderFilledPayload
from shared.kafka.schemas.positions import PositionOpenedPayload
from shared.kafka.schemas.signals import SignalPayload
from shared.kafka.schemas.trades import TradePayload
from shared.kafka.serializer import deserialize, serialize


def _bar(ts: int = 1) -> KafkaOHLCVBar:
    return KafkaOHLCVBar(ts=ts, open=100.0, high=101.0, low=99.0, close=100.5, volume=10.0)


def _make_payloads() -> dict[str, BasePayload]:
    return {
        "liquidations": LiquidationPayload(
            exchange="bybit",
            symbol="BTC/USDT",
            timestamp_ms=1,
            price="60000.5",
            quantity="1.0",
            quantity_usd="60000",
            side="buy",
            order_type="market",
        ),
        "ohlcv": EventPayload(
            exchange="bybit",
            symbol="BTC/USDT",
            timeframe="1m",
            bars=[_bar(1), _bar(2)],
            run_id="run-1",
        ),
        "oi": OpenInterestPayload(
            exchange="bybit",
            symbol="BTC/USDT",
            timestamp_ms=1,
            open_interest_contracts="1000",
            open_interest_value="60000000",
        ),
        "orderbook": OrderBookSnapshotPayload(
            exchange="bybit",
            symbol="BTC/USDT",
            timestamp_ms=1,
            bids=[("60000.0", "1.0")],
            asks=[("60001.0", "2.0")],
            depth=1,
        ),
        "orders": OrderFilledPayload(
            order_id="o-1",
            exchange="bybit",
            symbol="BTC/USDT",
            side="buy",
            fill_price=60000.0,
            size_pct=0.5,
        ),
        "positions": PositionOpenedPayload(
            order_id="o-1",
            exchange="bybit",
            symbol="BTC/USDT",
            side="long",
            entry_price=60000.0,
            size_pct=0.5,
        ),
        "signals": SignalPayload(
            exchange="bybit",
            symbol="BTC/USDT",
            timeframe="1h",
            direction="buy",
            price=60000.0,
            confidence=0.9,
            strategy="sma",
            run_id="run-1",
        ),
        "trades": TradePayload(
            exchange="bybit",
            market_type="spot",
            symbol="BTC/USDT",
            trade_id="t-1",
            timestamp_ms=1,
            price="60000.0",
            amount="1.0",
            side="buy",
            source="live",
        ),
    }


PAYLOADS = _make_payloads()


@pytest.mark.parametrize("name", sorted(PAYLOADS))
def test_roundtrip_preserves_payload(name: str) -> None:
    """serialize -> deserialize reconstruye el mismo payload (idempotencia wire)."""
    original = PAYLOADS[name]
    raw = serialize(original)
    assert isinstance(raw, bytes) and raw
    restored = deserialize(raw, type(original))
    assert restored == original, f"{name}: round-trip no preservó fields"


@pytest.mark.parametrize("name", sorted(PAYLOADS))
def test_envelope_fields_present(name: str) -> None:
    """Todo wire payload lleva envelope común (event_id | event_version | occurred_at)."""
    d = PAYLOADS[name].to_dict()
    for field in ("event_id", "event_version", "occurred_at"):
        assert field in d, f"{name} no emite campo envelope {field!r}"
    assert isinstance(d["event_id"], str) and d["event_id"], f"{name} event_id vacío"


@pytest.mark.parametrize("name", sorted(PAYLOADS))
def test_event_version_roundtrips(name: str) -> None:
    """La versión del schema sobrevive el viaje wire y coincide con SCHEMA_VERSION."""
    obj = PAYLOADS[name]
    assert obj.to_dict()["event_version"] == obj.SCHEMA_VERSION, f"{name}"
    raw = serialize(obj)
    restored = deserialize(raw, type(obj))
    assert restored.to_dict()["event_version"] == obj.SCHEMA_VERSION


@pytest.mark.parametrize("name", sorted(PAYLOADS))
def test_from_dict_rejects_wrong_version(name: str) -> None:
    """Deserializar un event_version incompatible eleva SchemaVersionError (fail-fast)."""
    cls = type(PAYLOADS[name])
    d = dict(PAYLOADS[name].to_dict())
    d["event_version"] = PAYLOADS[name].SCHEMA_VERSION + 999
    raw = bytes(json.dumps(d), "utf-8")
    with pytest.raises(SchemaVersionError):
        deserialize(raw, cls)


# ── Tópico canónico por payload (BC-29: wire schemas en shared) ───────────────

_PAYLOAD_TOPIC = {
    "liquidations": "liquidations.raw",
    "ohlcv": "ohlcv.raw",
    "oi": "oi.raw",
    "orderbook": "orderbook.raw",
    "orders": "orders.filled",
    "positions": "positions.opened",
    "signals": "signals.raw",
    "trades": "trades.raw",
}


@pytest.mark.parametrize("name", sorted(_PAYLOAD_TOPIC))
def test_topic_exists(name: str) -> None:
    """El tópico canónico del payload está definido en shared.kafka.topics."""
    from shared.kafka import topics as T

    topic = _PAYLOAD_TOPIC[name]
    values = {v for v in vars(T).values() if isinstance(v, str) and "." in v and "ocm" not in v}
    assert topic in values, f"{name}: tópico {topic!r} no definido en shared.kafka.topics"
