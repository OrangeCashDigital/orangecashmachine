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

  Dedup durable (B-19)
    A. Evento nuevo  → escrito + marcado durable (L2 sobrevive reinicios)
    B. event_id repetido → no reprocesado tras el mark durable
    C. Write falla  → event_id NO marcado → el retry puede reintentar
    D. L2 no disponible → fail-soft: L1 sigue activa, sin garantías inventadas
    E. Compartiendo store → un solo procesamiento efectivo tras el mark
    F. Ventana crash (write OK → mark no ejecutado) → duplicado recuperable,
       nunca pérdida silenciosa

Sin Kafka real — todo en memoria via fakes que satisfacen los ports.

Principios: SRP · DIP · SafeOps · at-least-once · observable failures
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Optional

from market_data.infrastructure.kafka.bronze_writer import KafkaBronzeWriter
from market_data.infrastructure.kafka.dedup import PersistentSeenFilter

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


class _DurableStore:
    """
    Fake de DeduplicationStoreProtocol (get_raw/set_raw) — dedup L2 durable.

    Simula RedisCursorStore: sobrevive a la "muerte" de un writer porque el
    estado vive en el store, no en la instancia del filtro.

    Modos de fallo independientes (para probar la ventana de crash):
      fail_get : get_raw lanza → fail-open en is_duplicate (L2 caído)
      fail_set : set_raw lanza → el mark no persiste (crash tras write OK)
    """

    def __init__(self, *, fail_get: bool = False, fail_set: bool = False) -> None:
        self._data: dict[str, str] = {}
        self._fail_get = fail_get
        self._fail_set = fail_set

    def get_raw(self, key: str) -> Optional[str]:
        if self._fail_get:
            raise ConnectionError("Redis down")
        return self._data.get(key)

    def set_raw(self, key: str, value: str, ttl_seconds: int) -> None:
        if self._fail_set:
            raise ConnectionError("Redis down")
        self._data[key] = value


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
    dedup_store: _DurableStore | None = None,
) -> KafkaBronzeWriter:
    return KafkaBronzeWriter(
        consumer=consumer,  # type: ignore[arg-type]
        bronze_storage=bronze,
        dlq_producer=dlq,  # type: ignore[arg-type]
        dedup_store=dedup_store,  # type: ignore[arg-type]
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

        processed, errors = asyncio.run(writer.run_once())

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

        asyncio.run(writer.run_once())

        assert consumer.committed is True

    def test_empty_queue_returns_zero(self):
        """Sin mensajes, run_once retorna (0, 0) y no commitea."""
        consumer = _FakeConsumer([])
        writer = _build_writer(consumer, _FakeBronze())

        processed, errors = asyncio.run(writer.run_once())

        assert processed == 0
        assert errors == 0
        assert consumer.committed is False

    def test_exchange_from_wire_not_constructor(self):
        """El exchange viene del evento (wire), no del constructor de bronze."""
        event = _make_event(exchange="kucoin")
        consumer = _FakeConsumer([_make_msg(event)])
        bronze = _FakeBronze()
        writer = _build_writer(consumer, bronze)

        asyncio.run(writer.run_once())

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

        asyncio.run(writer.run_once())

        assert len(bronze.appended) == 1

    def test_duplicate_still_commits(self):
        """Duplicado cuenta como 'handled' → el batch se commitea."""
        event = _make_event(event_id="evt-dup2")
        msg = _make_msg(event)
        consumer = _FakeConsumer([msg, msg])
        writer = _build_writer(consumer, _FakeBronze())

        processed, errors = asyncio.run(writer.run_once())

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

        asyncio.run(writer.run_once())

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

        asyncio.run(writer.run_once())

        assert len(dlq.produced) == 1
        assert dlq.produced[0]["topic"] == TOPIC_DLQ
        assert "deserialize_error" in dlq.produced[0]["headers"]["reason"]

    def test_corrupt_message_commits_offset(self):
        """Mensaje corrupto es 'handled' → offset commitado (no reintentar basura)."""
        consumer = _FakeConsumer([_make_corrupt_msg()])
        writer = _build_writer(consumer, _FakeBronze(), _FakeProducer())

        asyncio.run(writer.run_once())

        assert consumer.committed is True

    def test_empty_bars_goes_to_dlq(self):
        """Evento sin barras → DLQ con reason empty_bars."""
        event = _make_event(bars=[])
        consumer = _FakeConsumer([_make_msg(event)])
        dlq = _FakeProducer()
        writer = _build_writer(consumer, _FakeBronze(), dlq)

        asyncio.run(writer.run_once())

        assert len(dlq.produced) == 1
        assert dlq.produced[0]["headers"]["reason"] == "empty_bars"

    def test_empty_bars_commits_offset(self):
        """Evento vacío es 'handled' → offset commitado."""
        consumer = _FakeConsumer([_make_msg(_make_event(bars=[]))])
        writer = _build_writer(consumer, _FakeBronze(), _FakeProducer())

        asyncio.run(writer.run_once())

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

        processed, errors = asyncio.run(writer.run_once())

        assert consumer.committed is False
        assert errors == 1

    def test_bronze_failure_returns_write_error_count(self):
        """run_once retorna write_errors correctamente."""
        events = [_make_event(event_id=f"evt-{i}") for i in range(3)]
        consumer = _FakeConsumer([_make_msg(e) for e in events])
        writer = _build_writer(consumer, _FakeBronze(fail=True))

        processed, errors = asyncio.run(writer.run_once())

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

        _, errors = asyncio.run(writer.run_once())

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

        processed, errors = asyncio.run(writer.run_once())

        assert consumer.committed is True
        assert errors == 0

    def test_no_dlq_no_bronze_write_for_corrupt(self):
        """Sin DLQ, mensaje corrupto no produce escritura en Bronze."""
        consumer = _FakeConsumer([_make_corrupt_msg()])
        bronze = _FakeBronze()
        writer = _build_writer(consumer, bronze, dlq=None)

        asyncio.run(writer.run_once())

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
        asyncio.run(writer.run_once())

    def test_dlq_failure_still_commits_offset(self):
        """DLQ falla → mensaje manejado igualmente, offset commitado."""
        consumer = _FakeConsumer([_make_corrupt_msg()])
        dlq = _FakeProducer(fail=True)
        writer = _build_writer(consumer, _FakeBronze(), dlq)

        asyncio.run(writer.run_once())

        assert consumer.committed is True

    def test_dlq_failure_no_bronze_write(self):
        """DLQ falla en mensaje corrupto → no hay escritura en Bronze."""
        consumer = _FakeConsumer([_make_corrupt_msg()])
        bronze = _FakeBronze()
        writer = _build_writer(consumer, bronze, _FakeProducer(fail=True))

        asyncio.run(writer.run_once())

        assert len(bronze.appended) == 0


# ══════════════════════════════════════════════════════════════════════════════
# Tests — lifecycle
# ══════════════════════════════════════════════════════════════════════════════


class TestBronzeWriterLifecycle:
    def test_start_stop(self):
        """start() y stop() no lanzan."""
        consumer = _FakeConsumer()
        writer = _build_writer(consumer, _FakeBronze())

        asyncio.run(writer.start())
        asyncio.run(writer.stop())

    def test_run_once_returns_tuple(self):
        """run_once retorna (int, int)."""
        writer = _build_writer(_FakeConsumer(), _FakeBronze())

        result = asyncio.run(writer.run_once())

        assert isinstance(result, tuple)
        assert len(result) == 2
        assert all(isinstance(v, int) for v in result)


# ══════════════════════════════════════════════════════════════════════════════
# Tests — dedup durable L2 (B-19)
# ══════════════════════════════════════════════════════════════════════════════


class TestBronzeWriterDurableDedup:
    """B-19: dedup durable — el mark_seen ocurre SOLO tras el write exitoso."""

    def test_new_event_written_and_durably_marked(self):
        """A: evento nuevo → escrito + marcado durable (L2 sobrevive al writer)."""
        store = _DurableStore()
        event = _make_event(event_id="evt-A")
        writer = _build_writer(_FakeConsumer([_make_msg(event)]), _FakeBronze(), dedup_store=store)

        processed, errors = asyncio.run(writer.run_once())

        assert processed == 1
        assert errors == 0
        # El mark vive en el store (L2), no solo en memoria (L1):
        # un segundo writer con store fresco lo ve como duplicado.
        writer2 = _build_writer(_FakeConsumer([_make_msg(event)]), _FakeBronze(), dedup_store=store)
        processed2, _ = asyncio.run(writer2.run_once())

        assert processed2 == 0  # skip — duplicado detectado via L2

    def test_duplicate_across_writer_instances_skipped(self):
        """B: event_id repetido → no reprocesado tras el mark durable."""
        store = _DurableStore()
        event = _make_event(event_id="evt-B")
        msg = _make_msg(event)

        writer1 = _build_writer(_FakeConsumer([msg]), _FakeBronze(), dedup_store=store)
        asyncio.run(writer1.run_once())

        # "Reinicio": nuevo writer, nuevo L1, mismo L2 durable.
        bronze2 = _FakeBronze()
        writer2 = _build_writer(_FakeConsumer([msg]), bronze2, dedup_store=store)
        processed, errors = asyncio.run(writer2.run_once())

        assert processed == 0
        assert errors == 0
        assert len(bronze2.appended) == 0

    def test_write_failure_does_not_mark_retry_reprocesses(self):
        """C: write falla → event_id NO marcado → el retry reintenta."""
        store = _DurableStore()
        event = _make_event(event_id="evt-C")
        msg = _make_msg(event)

        # Primer intento: Bronze falla.
        writer_fail = _build_writer(_FakeConsumer([msg]), _FakeBronze(fail=True), dedup_store=store)
        _, errors = asyncio.run(writer_fail.run_once())
        assert errors == 1
        # NO marcado durable — un filtro nuevo con el mismo store no lo ve.
        assert not PersistentSeenFilter(store=store).is_duplicate(event.event_id)

        # Retry con Bronze OK: el evento se procesa (no se pierde silenciosamente).
        bronze_ok = _FakeBronze()
        writer_retry = _build_writer(_FakeConsumer([msg]), bronze_ok, dedup_store=store)
        processed, errors = asyncio.run(writer_retry.run_once())

        assert errors == 0
        assert processed == 1
        assert len(bronze_ok.appended) == 1

    def test_store_unavailable_fail_soft(self):
        """D: L2 caído → fail-open: L1 sigue activa, pipeline no se bloquea."""
        store = _DurableStore(fail_get=True)  # Redis down
        event = _make_event(event_id="evt-D")
        bronze = _FakeBronze()
        writer = _build_writer(_FakeConsumer([_make_msg(event)]), bronze, dedup_store=store)

        processed, errors = asyncio.run(writer.run_once())

        assert errors == 0
        assert processed == 1
        assert len(bronze.appended) == 1  # escrito — fail-open no bloquea

    def test_shared_store_single_effective_processing(self):
        """E: dos escritores comparten L2 → solo un procesamiento efectivo."""
        store = _DurableStore()
        event = _make_event(event_id="evt-E")
        msg = _make_msg(event)

        writer_a = _build_writer(_FakeConsumer([msg]), _FakeBronze(), dedup_store=store)
        asyncio.run(writer_a.run_once())

        bronze_b = _FakeBronze()
        writer_b = _build_writer(_FakeConsumer([msg]), bronze_b, dedup_store=store)
        asyncio.run(writer_b.run_once())

        assert len(bronze_b.appended) == 0  # B lo ve duplicado via L2 → no escribe

    def test_crash_window_recoverable_duplicate_not_loss(self):
        """F: write OK pero mark_seen no llega a ejecutarse (crash) → duplicado recuperable."""
        # L2 funciona para leer, pero el mark no persiste → simula el crash
        # "write exitoso → proceso muere antes del mark_seen".
        store = _DurableStore(fail_set=True)
        event = _make_event(event_id="evt-F")
        msg = _make_msg(event)

        # Write exitoso, pero mark_seen falla silenciosamente (fail-open).
        bronze1 = _FakeBronze()
        writer1 = _build_writer(_FakeConsumer([msg]), bronze1, dedup_store=store)
        processed1, _ = asyncio.run(writer1.run_once())
        assert processed1 == 1

        # El evento fue escrito pero NO marcado durable → el retry reintenta
        # y escribe de nuevo. Duplicado recuperable — nunca pérdida silenciosa.
        bronze2 = _FakeBronze()
        writer2 = _build_writer(_FakeConsumer([msg]), bronze2, dedup_store=store)
        processed2, _ = asyncio.run(writer2.run_once())

        assert processed2 == 1  # reprocesado (el mark no persistió)
        assert len(bronze2.appended) == 1  # duplicado recuperable
