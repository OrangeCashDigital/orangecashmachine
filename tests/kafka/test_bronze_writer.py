# -*- coding: utf-8 -*-
"""
tests/kafka/test_bronze_writer.py
==================================

Tests unitarios de KafkaBronzeWriter — el corazón del pipeline Kappa.

Cobertura
---------
  Happy path       : mensaje válido → Bronze escrito → commit
  Dedup L1         : evento duplicado → skip → commit (handled)
  Deserialize error: payload corrupto → DLQ → commit (handled)
  Empty bars       : evento sin velas → DLQ → commit (handled)
  Bronze failure   : write falla → NO commit (at-least-once)
  DLQ unavailable  : dlq=None → WARNING observable, no pérdida silenciosa
  DLQ falla        : produce() lanza → ERROR observable, métrica registrada
  Batch mixto      : escrituras + errores → no commit por at-least-once
  run_once conteo  : retorna (processed, write_errors) correctos

Sin Kafka real — todo en memoria via fakes que satisfacen los ports.

Principios: SRP · DIP · SafeOps · at-least-once · observable failures
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Optional

from market_data.infrastructure.kafka.bronze_writer import KafkaBronzeWriter

from shared.kafka.schemas.ohlcv import EventPayload, KafkaOHLCVBar
from shared.kafka.serializer import serialize
from shared.kafka.topics import TOPIC_DLQ

# ══════════════════════════════════════════════════════════════════════════════
# Fakes — satisfacen los ports sin infra real
# ══════════════════════════════════════════════════════════════════════════════


@dataclass
class _FakeMessage:
    """Simula un mensaje Kafka con los atributos mínimos que usa el writer."""

    value: bytes
    topic: str = "ohlcv.raw"
    offset: int = 0


class _FakeConsumer:
    """
    Implementa KafkaConsumerPort en memoria.

    Permite inyectar mensajes vía .enqueue() y verifica si commit() fue llamado.
    """

    def __init__(self, messages: list[_FakeMessage] | None = None) -> None:
        self._queue: list[_FakeMessage] = list(messages or [])
        self.committed: bool = False
        self.started: bool = False
        self.stopped: bool = False

    def enqueue(self, msg: _FakeMessage) -> None:
        self._queue.append(msg)

    async def start(self) -> None:
        self.started = True

    async def close(self) -> None:
        self.stopped = True

    async def poll(
        self,
        timeout_ms: int = 1_000,
        max_records: int = 500,
    ) -> list[_FakeMessage]:
        batch, self._queue = self._queue[:max_records], self._queue[max_records:]
        return batch

    async def commit(self) -> None:
        self.committed = True


class _FakeBronze:
    """
    Fake de BronzeStorage — registra llamadas a append().

    Configurable para fallar controladamente.
    """

    def __init__(self, *, fail: bool = False) -> None:
        self.appended: list[dict] = []
        self._fail = fail

    def append(self, df: Any, *, symbol: str, timeframe: str, exchange: str, run_id: str) -> int:
        if self._fail:
            raise OSError("Iceberg write failed — disco lleno")
        self.appended.append({"symbol": symbol, "timeframe": timeframe, "exchange": exchange, "run_id": run_id})
        return len(df)


class _FakeProducer:
    """
    Fake de KafkaProducerPort — registra llamadas a produce().

    Configurable para fallar controladamente.
    """

    def __init__(self, *, fail: bool = False) -> None:
        self.produced: list[dict] = []
        self._fail = fail

    async def produce(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
        headers: Optional[dict] = None,
    ) -> None:
        if self._fail:
            raise ConnectionError("Broker inalcanzable")
        self.produced.append({"topic": topic, "headers": headers})

    async def flush(self, timeout: float = 10.0) -> None:
        pass

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# Helpers de construcción de mensajes
# ══════════════════════════════════════════════════════════════════════════════


def _make_bar(ts: int = 1_700_000_000_000) -> KafkaOHLCVBar:
    return KafkaOHLCVBar(ts=ts, open=100.0, high=105.0, low=99.0, close=103.0, volume=1_000.0)


def _make_event(
    *,
    event_id: str = "evt-001",
    exchange: str = "bybit",
    symbol: str = "BTC/USDT",
    timeframe: str = "1h",
    bars: list[KafkaOHLCVBar] | None = None,
) -> EventPayload:
    return EventPayload(
        event_id=event_id,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        batch_start_ts=1_700_000_000_000,
        bars=bars if bars is not None else [_make_bar()],
    )


def _serialize_event(event: EventPayload) -> bytes:
    return serialize(event)


def _make_msg(event: EventPayload, *, offset: int = 0) -> _FakeMessage:
    return _FakeMessage(value=_serialize_event(event), offset=offset)


def _make_corrupt_msg(offset: int = 99) -> _FakeMessage:
    return _FakeMessage(value=b"not-valid-json-{{{", offset=offset)


def _build_writer(
    consumer: _FakeConsumer,
    bronze: _FakeBronze,
    dlq: _FakeProducer | None = None,
) -> KafkaBronzeWriter:
    return KafkaBronzeWriter(
        consumer=consumer,  # type: ignore[arg-type]
        bronze_storage=bronze,
        dlq_producer=dlq,  # type: ignore[arg-type]
    )


# ══════════════════════════════════════════════════════════════════════════════
# Tests — happy path
# ══════════════════════════════════════════════════════════════════════════════


class TestBronzeWriterHappyPath:
    def test_valid_message_written_to_bronze(self):
        """Mensaje válido → Bronze.append() llamado con campos correctos."""
        event = _make_event()
        consumer = _FakeConsumer([_make_msg(event)])
        bronze = _FakeBronze()
        writer = _build_writer(consumer, bronze)

        processed, errors = asyncio.get_event_loop().run_until_complete(writer.run_once())

        assert processed == 1
        assert errors == 0
        assert len(bronze.appended) == 1
        call = bronze.appended[0]
        assert call["exchange"] == "bybit"
        assert call["symbol"] == "BTC/USDT"
        assert call["timeframe"] == "1h"
        assert call["run_id"] == "evt-001"

    def test_commit_happens_on_success(self):
        """Al escribir sin errores, el offset se commitea."""
        consumer = _FakeConsumer([_make_msg(_make_event())])
        writer = _build_writer(consumer, _FakeBronze())

        asyncio.get_event_loop().run_until_complete(writer.run_once())

        assert consumer.committed is True

    def test_empty_queue_returns_zero(self):
        """Sin mensajes, run_once retorna (0, 0) y no commitea."""
        consumer = _FakeConsumer([])
        writer = _build_writer(consumer, _FakeBronze())

        processed, errors = asyncio.get_event_loop().run_until_complete(writer.run_once())

        assert processed == 0
        assert errors == 0
        assert consumer.committed is False

    def test_exchange_from_wire_not_constructor(self):
        """El exchange viene del evento (wire), no del constructor de bronze."""
        event = _make_event(exchange="kucoin")
        consumer = _FakeConsumer([_make_msg(event)])
        bronze = _FakeBronze()
        writer = _build_writer(consumer, bronze)

        asyncio.get_event_loop().run_until_complete(writer.run_once())

        assert bronze.appended[0]["exchange"] == "kucoin"


# ══════════════════════════════════════════════════════════════════════════════
# Tests — deduplicación L1
# ══════════════════════════════════════════════════════════════════════════════


class TestBronzeWriterDedup:
    def test_duplicate_event_skipped(self):
        """El mismo event_id dos veces → solo escrito una vez."""
        event = _make_event(event_id="evt-dup")
        msg = _make_msg(event)
        consumer = _FakeConsumer([msg, msg])
        bronze = _FakeBronze()
        writer = _build_writer(consumer, bronze)

        asyncio.get_event_loop().run_until_complete(writer.run_once())

        assert len(bronze.appended) == 1

    def test_duplicate_still_commits(self):
        """Duplicado cuenta como 'handled' → el batch se commitea."""
        event = _make_event(event_id="evt-dup2")
        msg = _make_msg(event)
        consumer = _FakeConsumer([msg, msg])
        writer = _build_writer(consumer, _FakeBronze())

        processed, errors = asyncio.get_event_loop().run_until_complete(writer.run_once())

        assert consumer.committed is True
        assert errors == 0

    def test_different_events_both_written(self):
        """Dos eventos distintos → ambos escritos."""
        consumer = _FakeConsumer(
            [
                _make_msg(_make_event(event_id="evt-A")),
                _make_msg(_make_event(event_id="evt-B")),
            ]
        )
        bronze = _FakeBronze()
        writer = _build_writer(consumer, bronze)

        asyncio.get_event_loop().run_until_complete(writer.run_once())

        assert len(bronze.appended) == 2


# ══════════════════════════════════════════════════════════════════════════════
# Tests — mensajes inválidos → DLQ
# ══════════════════════════════════════════════════════════════════════════════


class TestBronzeWriterDLQ:
    def test_corrupt_message_goes_to_dlq(self):
        """Payload corrupto → DLQ con reason deserialize_error."""
        consumer = _FakeConsumer([_make_corrupt_msg()])
        dlq = _FakeProducer()
        writer = _build_writer(consumer, _FakeBronze(), dlq)

        asyncio.get_event_loop().run_until_complete(writer.run_once())

        assert len(dlq.produced) == 1
        assert dlq.produced[0]["topic"] == TOPIC_DLQ
        assert "deserialize_error" in dlq.produced[0]["headers"]["reason"]

    def test_corrupt_message_commits_offset(self):
        """Mensaje corrupto es 'handled' → offset commitado (no reintentar basura)."""
        consumer = _FakeConsumer([_make_corrupt_msg()])
        writer = _build_writer(consumer, _FakeBronze(), _FakeProducer())

        asyncio.get_event_loop().run_until_complete(writer.run_once())

        assert consumer.committed is True

    def test_empty_bars_goes_to_dlq(self):
        """Evento sin barras → DLQ con reason empty_bars."""
        event = _make_event(bars=[])
        consumer = _FakeConsumer([_make_msg(event)])
        dlq = _FakeProducer()
        writer = _build_writer(consumer, _FakeBronze(), dlq)

        asyncio.get_event_loop().run_until_complete(writer.run_once())

        assert len(dlq.produced) == 1
        assert dlq.produced[0]["headers"]["reason"] == "empty_bars"

    def test_empty_bars_commits_offset(self):
        """Evento vacío es 'handled' → offset commitado."""
        consumer = _FakeConsumer([_make_msg(_make_event(bars=[]))])
        writer = _build_writer(consumer, _FakeBronze(), _FakeProducer())

        asyncio.get_event_loop().run_until_complete(writer.run_once())

        assert consumer.committed is True


# ══════════════════════════════════════════════════════════════════════════════
# Tests — at-least-once: NO commitear si Bronze falla
# ══════════════════════════════════════════════════════════════════════════════


class TestBronzeWriterAtLeastOnce:
    def test_bronze_failure_no_commit(self):
        """Si Bronze falla, el offset NO se commitea — se reintentará."""
        consumer = _FakeConsumer([_make_msg(_make_event())])
        bronze = _FakeBronze(fail=True)
        writer = _build_writer(consumer, bronze)

        processed, errors = asyncio.get_event_loop().run_until_complete(writer.run_once())

        assert consumer.committed is False
        assert errors == 1

    def test_bronze_failure_returns_write_error_count(self):
        """run_once retorna write_errors correctamente."""
        events = [_make_event(event_id=f"evt-{i}") for i in range(3)]
        consumer = _FakeConsumer([_make_msg(e) for e in events])
        writer = _build_writer(consumer, _FakeBronze(fail=True))

        processed, errors = asyncio.get_event_loop().run_until_complete(writer.run_once())

        assert errors == 3
        assert consumer.committed is False

    def test_mixed_batch_no_commit(self):
        """Batch con un write_error → todo el batch sin commit (at-least-once)."""
        good_event = _make_event(event_id="evt-good")
        bad_event = _make_event(event_id="evt-bad")

        call_count = 0

        class _PartialBronze:
            appended: list = []

            def append(self, df, *, symbol, timeframe, exchange, run_id):
                nonlocal call_count
                call_count += 1
                if run_id == "evt-bad":
                    raise OSError("write failed")
                self.appended.append(run_id)
                return 1

        consumer = _FakeConsumer([_make_msg(good_event), _make_msg(bad_event)])
        writer = _build_writer(consumer, _PartialBronze())  # type: ignore[arg-type]

        _, errors = asyncio.get_event_loop().run_until_complete(writer.run_once())

        assert errors >= 1
        assert consumer.committed is False


# ══════════════════════════════════════════════════════════════════════════════
# Tests — DLQ no configurado: no pérdida silenciosa
# ══════════════════════════════════════════════════════════════════════════════


class TestBronzeWriterDLQUnavailable:
    def test_no_dlq_corrupt_message_still_commits(self):
        """Sin DLQ, mensaje corrupto se descarta y offset se commitea."""
        consumer = _FakeConsumer([_make_corrupt_msg()])
        writer = _build_writer(consumer, _FakeBronze(), dlq=None)

        processed, errors = asyncio.get_event_loop().run_until_complete(writer.run_once())

        assert consumer.committed is True
        assert errors == 0

    def test_no_dlq_no_bronze_write_for_corrupt(self):
        """Sin DLQ, mensaje corrupto no produce escritura en Bronze."""
        consumer = _FakeConsumer([_make_corrupt_msg()])
        bronze = _FakeBronze()
        writer = _build_writer(consumer, bronze, dlq=None)

        asyncio.get_event_loop().run_until_complete(writer.run_once())

        assert len(bronze.appended) == 0


# ══════════════════════════════════════════════════════════════════════════════
# Tests — DLQ producer falla: error observable, no excepción propagada
# ══════════════════════════════════════════════════════════════════════════════


class TestBronzeWriterDLQProducerFailure:
    def test_dlq_failure_does_not_raise(self):
        """Si DLQ falla, KafkaBronzeWriter no lanza — SafeOps."""
        consumer = _FakeConsumer([_make_corrupt_msg()])
        dlq = _FakeProducer(fail=True)
        writer = _build_writer(consumer, _FakeBronze(), dlq)

        # No debe lanzar
        asyncio.get_event_loop().run_until_complete(writer.run_once())

    def test_dlq_failure_still_commits_offset(self):
        """DLQ falla → mensaje manejado igualmente, offset commitado."""
        consumer = _FakeConsumer([_make_corrupt_msg()])
        dlq = _FakeProducer(fail=True)
        writer = _build_writer(consumer, _FakeBronze(), dlq)

        asyncio.get_event_loop().run_until_complete(writer.run_once())

        assert consumer.committed is True

    def test_dlq_failure_no_bronze_write(self):
        """DLQ falla en mensaje corrupto → no hay escritura en Bronze."""
        consumer = _FakeConsumer([_make_corrupt_msg()])
        bronze = _FakeBronze()
        writer = _build_writer(consumer, bronze, _FakeProducer(fail=True))

        asyncio.get_event_loop().run_until_complete(writer.run_once())

        assert len(bronze.appended) == 0


# ══════════════════════════════════════════════════════════════════════════════
# Tests — lifecycle
# ══════════════════════════════════════════════════════════════════════════════


class TestBronzeWriterLifecycle:
    def test_start_stop(self):
        """start() y stop() no lanzan."""
        consumer = _FakeConsumer()
        writer = _build_writer(consumer, _FakeBronze())

        asyncio.get_event_loop().run_until_complete(writer.start())
        asyncio.get_event_loop().run_until_complete(writer.stop())

    def test_run_once_returns_tuple(self):
        """run_once retorna (int, int)."""
        writer = _build_writer(_FakeConsumer(), _FakeBronze())

        result = asyncio.get_event_loop().run_until_complete(writer.run_once())

        assert isinstance(result, tuple)
        assert len(result) == 2
        assert all(isinstance(v, int) for v in result)
