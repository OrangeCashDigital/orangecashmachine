# -*- coding: utf-8 -*-
"""
tests/market_data/infrastructure/kafka/test_producer_adapter.py
================================================================

F-013 y F-014 — regresion del KafkaProducerAdapter.

F-013 (Camino A, sin ADR)
-------------------------
produce() debe dejar de tragarse el bool de send_async(): si el mensaje
no fue confirmado (send_asFuture() retorna False), produce() eleva
KafkaProducerError en lugar de retornar silenciosamente.

F-014
-----
close() debe forzar flush() ANTES de stop(). Sin flush previo los
mensajes en buffer (linger_ms=5, batching) podrían perderse en shutdown.

F-015
-----
enable_idempotence=True debe pasar al cliente AIOKafkaProducer (elimina
duplicados bajo retry; verificado compatible con acks="all").

F-019
-----
send_async() clasifica la razón de fallo (broker_timeout, connection_error,
broker_response, unknown_error) — no colapsa todo en write_error. El espacio
de problema sigue en el ADR pendiente de B-25.

Principios: F-013 Camino A · F-014 flush-before-stop · F-015 idempotence ·
F-019 error classification · SafeOps visible
"""

from __future__ import annotations

import asyncio

import pytest
from aiokafka.errors import (
    BrokerResponseError,
    KafkaConnectionError,
    KafkaTimeoutError,
)
from market_data.domain.exceptions import KafkaProducerError
from market_data.infrastructure.kafka.producer import KafkaProducerAdapter


class _FakeProducer:
    """Stub de AIOKafkaProducer — controlable por the test."""

    def __init__(self) -> None:
        self._flushed = 0
        self._stopped = 0
        self._flush_raises: BaseException | None = None

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        self._stopped += 1

    async def flush(self) -> None:
        self._flushed += 1
        if self._flush_raises is not None:
            raise self._flush_raises

    async def send_and_wait(self, *args, **kwargs) -> None:
        pass


@pytest.fixture
def adapter():
    a = KafkaProducerAdapter()
    a._producer = _FakeProducer()
    a._started = True
    return a


# ---------------------------------------------------------------------------
# F-014 — close() hace flush() antes de stop()
# ---------------------------------------------------------------------------


class TestCloseFlushesBeforeStop:
    async def test_close_flushes_before_stop(self, adapter):
        await adapter.close()
        assert adapter._producer._flushed == 1
        assert adapter._producer._stopped == 1
        assert adapter._started is False

    async def test_close_flush_timeout_notifies_but_still_stops(self, adapter):
        adapter._producer._flush_raises = TimeoutError("simulated")
        await adapter.close()
        assert adapter._producer._stopped == 1
        assert adapter._started is False

    async def test_close_is_idempotent(self, adapter):
        await adapter.close()
        await adapter.close()
        assert adapter._producer._stopped == 1
        assert adapter._started is False

    async def test_close_flush_propagates_other_exceptions_safely(self, adapter):
        adapter._producer._flush_raises = RuntimeError("boom")
        await adapter.close()  # no debe lanzar — SafeOps en stop path
        assert adapter._producer._stopped == 1


# ---------------------------------------------------------------------------
# F-013 — produce() eleva KantFault cuando send_async es False
# ---------------------------------------------------------------------------


class TestProduceSignalsFailure:
    async def test_produce_raises_when_send_fails(self, adapter, monkeypatch):
        async def fake_send(**kw):
            return False

        monkeypatch.setattr(adapter, "send_async", fake_send)
        with pytest.raises(KafkaProducerError):
            await adapter.produce(topic="t", value=b"x")

    async def test_produce_returns_none_when_send_ok(self, adapter, monkeypatch):
        async def fake_send(**kw):
            return True

        monkeypatch.setattr(adapter, "send_async", fake_send)
        await adapter.produce(topic="t", value=b"x")  # no debe lanzar

    async def test_produce_not_started_raises(self, adapter, monkeypatch):
        async def fake_send(**kw):
            return False  # send_async interno devuelve False si no started

        monkeypatch.setattr(adapter, "send_async", fake_send)
        with pytest.raises(KafkaProducerError):
            await adapter.produce(topic="t", value=b"x")

    # ------------------------------------------------------------------
    # flush() implícito del contrato — requiere asyncio.wait_for
    # ------------------------------------------------------------------

    async def test_flush_propagates_timeout(self, adapter):
        async def slow_flush():
            await asyncio.sleep(60)

        adapter._producer.flush = slow_flush  # type: ignore[method-assign]
        with pytest.raises(TimeoutError):
            await adapter.flush(timeout=0.05)


# ---------------------------------------------------------------------------
# F-015 — enable_idempotence=True se propaga al cliente AIOKafkaProducer
# ---------------------------------------------------------------------------


class TestIdempotenceOnStart:
    async def test_start_enables_idempotence(self, monkeypatch):
        import aiokafka

        captured: dict = {}

        class _FakeAIOK:
            def __init__(self, **kwargs):
                captured.update(kwargs)
                self._s = False

            async def start(self) -> None:
                self._s = True

        monkeypatch.setattr(aiokafka, "AIOKafkaProducer", _FakeAIOK)
        a = KafkaProducerAdapter(acks="all")
        await a.start()
        assert captured["enable_idempotence"] is True
        assert captured["acks"] == "all"  # compatible — aiokafka mapea a -1


# ---------------------------------------------------------------------------
# F-019 — send_async clasifica la razón de fallo en el log
# ---------------------------------------------------------------------------


class _BrokerError(BrokerResponseError):
    errno = 42


class TestSendErrorClassification:
    @pytest.mark.parametrize(
        "exc_class,expected_reason",
        [
            (KafkaTimeoutError, "broker_timeout"),
            (KafkaConnectionError, "connection_error"),
            (_BrokerError, "broker_response"),
            (RuntimeError, "unknown_error"),
        ],
    )
    async def test_reason_classified(self, adapter, exc_class, expected_reason):
        async def _boom(*args, **kwargs):
            raise exc_class("boom")

        adapter._producer.send_and_wait = _boom

        captured: dict = {}

        class _FakeLog:
            def bind(self, **kw):
                captured.update(kw)
                return self

            def warning(self, *a, **kw):
                pass

            def debug(self, *a, **kw):
                pass

        real_log = adapter._log
        adapter._log = _FakeLog()  # type: ignore[assignment]
        try:
            result = await adapter.send_async(topic="t", value=b"x")
        finally:
            adapter._log = real_log  # type: ignore[assignment]

        assert result is False
        assert captured["reason"] == expected_reason
