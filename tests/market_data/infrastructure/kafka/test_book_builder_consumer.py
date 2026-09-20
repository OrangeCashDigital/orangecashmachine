# -*- coding: utf-8 -*-
"""
tests/market_data/infrastructure/kafka/test_book_builder_consumer.py
=====================================================================
Tests del BookBuilderConsumer (orderbook.raw → book.snapshot/book.delta).

Verifica el ciclo poll→procesar→publicar→commit con ports fake (DIP, sin
Kafka real): snapshot→book.snapshot, delta→book.delta, DLQ para payload_type
desconocido, y no-commit ante error de escritura.
"""

from __future__ import annotations

import json

import pytest
from market_data.application.processing.book_builder import BookBuilder
from market_data.infrastructure.kafka.book_builder_consumer import (
    BookBuilderConsumer,
)
from market_data.ports.outbound.kafka_consumer import KafkaMessage

from shared.kafka.schemas.orderbook import OrderBookDeltaPayload, OrderBookSnapshotPayload
from shared.kafka.topics import TOPIC_BOOK_DELTA, TOPIC_BOOK_SNAPSHOT, TOPIC_DLQ, TOPIC_ORDERBOOK_RAW


class FakeConsumer:
    def __init__(self, messages=()):
        self._messages = list(messages)
        self.commits = 0
        self.started = False

    async def start(self):
        self.started = True

    async def close(self):
        self.started = False

    async def poll(self, timeout_ms=1000, max_records=500):
        return self._messages

    async def commit(self):
        self.commits += 1

    async def seek_to_beginning(self):
        pass


class FakeProducer:
    def __init__(self):
        self.published = []
        self.started = False

    async def start(self):
        self.started = True

    async def stop(self):
        self.started = False

    async def produce(self, topic, value, key=None, headers=None):
        self.published.append((topic, value, key, headers))

    async def flush(self, timeout=10.0):
        pass


def _bytes(payload) -> bytes:
    return json.dumps(payload.to_dict()).encode("utf-8")


def _snapshot_payload(**over):
    base = dict(
        exchange="bybit",
        symbol="BTC/USDT",
        timestamp_ms=1_700_000_000_000,
        update_id=1,
        bids=[("30250.0", "0.5"), ("30200.0", "1.2")],
        asks=[("30300.0", "0.7")],
    )
    base.update(over)
    return OrderBookSnapshotPayload(**base)


def _delta_payload(**over):
    base = dict(
        exchange="bybit",
        symbol="BTC/USDT",
        timestamp_ms=1_700_000_000_100,
        update_id=2,
        bids=[("30200.0", "1.5")],
        asks=[],
    )
    base.update(over)
    return OrderBookDeltaPayload(**base)


def _seed_snapshot(builder) -> None:
    """Siembra el builder con un snapshot base (u=1) para poder aplicar deltas."""
    builder.on_snapshot(
        "bybit",
        "BTC/USDT",
        1_700_000_000_000,
        [("30250.0", "0.5"), ("30200.0", "1.2")],
        [("30300.0", "0.7")],
        update_id=1,
    )


class TestSnapshotDeltaFlow:
    @pytest.mark.asyncio
    async def test_snapshot_publishes_to_book_snapshot(self):
        consumer = FakeConsumer([KafkaMessage(TOPIC_ORDERBOOK_RAW, 0, 0, b"k", _bytes(_snapshot_payload()), 0, ())])
        producer = FakeProducer()
        bc = BookBuilderConsumer(consumer, producer, BookBuilder(stale_ms=2_000))
        processed, failed = await bc._run_once()
        assert processed == 1 and failed == 0
        topics = [t for t, _, _, _ in producer.published]
        assert topics == [TOPIC_BOOK_SNAPSHOT]
        body = json.loads(producer.published[0][1])
        assert body["payload_type"] == "snapshot"
        assert body["update_id"] == 1

    @pytest.mark.asyncio
    async def test_delta_publishes_to_book_delta(self):
        consumer = FakeConsumer([KafkaMessage(TOPIC_ORDERBOOK_RAW, 0, 0, b"k", _bytes(_delta_payload()), 0, ())])
        producer = FakeProducer()
        bc = BookBuilderConsumer(consumer, producer, BookBuilder(stale_ms=2_000))
        # El delta necesita un snapshot previo como base.
        _seed_snapshot(bc._builder)
        processed, failed = await bc._run_once()
        assert processed == 1 and failed == 0
        topics = [t for t, _, _, _ in producer.published]
        assert topics == [TOPIC_BOOK_DELTA]

    @pytest.mark.asyncio
    async def test_commit_happens_on_success(self):
        consumer = FakeConsumer([KafkaMessage(TOPIC_ORDERBOOK_RAW, 0, 0, b"k", _bytes(_snapshot_payload()), 0, ())])
        producer = FakeProducer()
        bc = BookBuilderConsumer(consumer, producer, BookBuilder(stale_ms=2_000))
        await bc._run_once()
        assert consumer.commits == 1

    @pytest.mark.asyncio
    async def test_producer_error_skips_commit(self):
        consumer = FakeConsumer([KafkaMessage(TOPIC_ORDERBOOK_RAW, 0, 0, b"k", _bytes(_snapshot_payload()), 0, ())])

        class FailProducer(FakeProducer):
            async def produce(self, topic, value, key=None, headers=None):
                raise RuntimeError("broker down")

        producer = FailProducer()
        bc = BookBuilderConsumer(consumer, producer, BookBuilder(stale_ms=2_000))
        processed, failed = await bc._run_once()
        assert failed == 1
        assert consumer.commits == 0


class TestDLQ:
    @pytest.mark.asyncio
    async def test_unknown_payload_type_goes_to_dlq(self):
        raw = json.dumps({"payload_type": "bogus"}).encode("utf-8")
        consumer = FakeConsumer([KafkaMessage(TOPIC_ORDERBOOK_RAW, 0, 0, b"k", raw, 0, ())])
        producer = FakeProducer()
        bc = BookBuilderConsumer(consumer, producer, BookBuilder(stale_ms=2_000))
        processed, failed = await bc._run_once()
        assert processed == 0 and failed == 0
        topics = [t for t, _, _, _ in producer.published]
        assert TOPIC_DLQ in topics


class TestGap:
    @pytest.mark.asyncio
    async def test_delta_gap_invalidates_and_publishes_nothing(self):
        consumer = FakeConsumer(
            [KafkaMessage(TOPIC_ORDERBOOK_RAW, 0, 0, b"k", _bytes(_delta_payload(update_id=99)), 0, ())]
        )
        producer = FakeProducer()
        bc = BookBuilderConsumer(consumer, producer, BookBuilder(stale_ms=2_000))
        _seed_snapshot(bc._builder)
        processed, failed = await bc._run_once()
        assert processed == 0 and failed == 0
        assert producer.published == []


class SpyMetrics:
    """Spy inyectable para observabilidad (metricas del consumer)."""

    def __init__(self):
        self.failed: list[tuple[str, str]] = []  # (exchange, reason)
        self.published: int = 0

    def event_failed(self, exchange: str = "unknown", reason: str = "unknown") -> None:
        self.failed.append((exchange, reason))

    def event_published(self, exchange: str = "unknown") -> None:
        self.published += 1


class TestStale:
    """Stale detection end-to-end a través del consumer (port, BC-07).

    Demuestra que la capacidad de detectar libros stale NO depende de la
    llegada de mensajes: `_check_stale` se invoca en cada iteración del loop
    (incluso en polls vacíos) y traduce los STALE del BookBuilder en
    métrica/aviso sin publicar nada.
    """

    def _make(self, builder: BookBuilder):
        spy = SpyMetrics()
        producer = FakeProducer()
        bc = BookBuilderConsumer(FakeConsumer(), producer, builder, metrics=spy)
        return bc, spy, producer

    @pytest.mark.asyncio
    async def test_idle_poll_detects_stale_without_messages(self):
        """Sin mensajes entrantes, el loop aun invoca check_stale y alerta STALE."""
        builder = BookBuilder(stale_ms=2_000)
        builder.on_snapshot(
            "bybit",
            "BTC/USDT",
            timestamp_ms=1,  # epoch+1ms: ya stale frente a cualquier now_ms real
            bids=[("30250.0", "0.5")],
            asks=[("30300.0", "0.7")],
            update_id=1,
        )
        bc, spy, producer = self._make(builder)

        processed, failed = await bc._run_once()  # poll vacio -> _check_stale
        assert processed == 0 and failed == 0
        assert producer.published == []  # STALE nunca publica
        assert spy.failed, "esperaba al menos una metrica STALE (reason='stale')"
        assert all(reason == "stale" for _, reason in spy.failed)

    @pytest.mark.asyncio
    async def test_recent_book_is_not_stale(self):
        """Un libro recien actualizado (now_ms real) no dispara STALE."""
        from time import time

        now_ms = int(time() * 1000)
        builder = BookBuilder(stale_ms=60_000)
        builder.on_snapshot(
            "bybit",
            "BTC/USDT",
            timestamp_ms=now_ms,
            bids=[("30250.0", "0.5")],
            asks=[("30300.0", "0.7")],
            update_id=1,
        )
        bc, spy, _ = self._make(builder)

        processed, failed = await bc._run_once()
        assert processed == 0 and failed == 0
        assert spy.failed == []  # dentro de la ventana -> sin STALE


__all__ = []
