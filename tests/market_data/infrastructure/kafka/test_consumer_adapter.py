# -*- coding: utf-8 -*-
"""
tests/market_data/infrastructure/kafka/test_consumer_adapter.py
================================================================

Cobertura directa de KafkaConsumerAdapter — hasta ahora solo cubierto
indirectamente por tests/market_data/test_quality_consumer_wiring.py
(wiring), sin tests de la lógica propia del adapter.

Cubre:
- Factory methods (for_bronze_writer, for_feature_consumer, etc.):
  topic/group_id/auto_offset_reset correctos por rol.
- poll() SafeOps: retorna [] en excepción, no propaga.
- commit() SafeOps: no propaga en excepción.
- seek_to_beginning() SafeOps + no-op si no está started.
- close() idempotente.

Principios: mismo patrón que test_producer_adapter.py — fakes ligeros,
sin red real, sin mocks pesados.
"""

from __future__ import annotations

import pytest
from market_data.infrastructure.kafka.consumer import KafkaConsumerAdapter

from shared.kafka.topics import (
    GROUP_BRONZE_WRITER,
    GROUP_EXECUTION,
    GROUP_FEATURES,
    GROUP_PORTFOLIO,
    GROUP_RISK_GATE,
    GROUP_STRATEGY,
    TOPIC_OHLCV_FEATURES,
    TOPIC_OHLCV_RAW,
    TOPIC_OHLCV_VALIDATED,
    TOPIC_ORDERS_FILLED,
    TOPIC_SIGNALS_APPROVED,
    TOPIC_SIGNALS_RAW,
)


class _FakeConsumer:
    """Stub de AIOKafkaConsumer — controlable por el test."""

    def __init__(self) -> None:
        self._stopped = 0
        self._committed = 0
        self._sought = 0
        self._getmany_raises: BaseException | None = None
        self._commit_raises: BaseException | None = None
        self._seek_raises: BaseException | None = None
        self._getmany_result: dict = {}

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        self._stopped += 1

    async def getmany(self, timeout_ms: int = 1_000, max_records: int = 500):
        if self._getmany_raises is not None:
            raise self._getmany_raises
        return self._getmany_result

    async def commit(self) -> None:
        self._committed += 1
        if self._commit_raises is not None:
            raise self._commit_raises

    async def seek_to_beginning(self) -> None:
        self._sought += 1
        if self._seek_raises is not None:
            raise self._seek_raises


@pytest.fixture
def adapter():
    a = KafkaConsumerAdapter(topics=["t"], group_id="g")
    a._consumer = _FakeConsumer()
    a._started = True
    return a


# ---------------------------------------------------------------------------
# Factory methods — topic/group_id/offset_reset correctos por rol
# ---------------------------------------------------------------------------


class TestFactoryMethods:
    def test_for_bronze_writer(self):
        c = KafkaConsumerAdapter.for_bronze_writer()
        assert c._topics == [TOPIC_OHLCV_RAW]
        assert c._group_id == GROUP_BRONZE_WRITER

    def test_for_feature_consumer(self):
        c = KafkaConsumerAdapter.for_feature_consumer()
        assert c._topics == [TOPIC_OHLCV_VALIDATED]
        assert c._group_id == GROUP_FEATURES

    def test_for_strategy_consumer(self):
        c = KafkaConsumerAdapter.for_strategy_consumer()
        assert c._topics == [TOPIC_OHLCV_FEATURES]
        assert c._group_id == GROUP_STRATEGY

    def test_for_risk_gate_uses_latest_offset(self):
        c = KafkaConsumerAdapter.for_risk_gate()
        assert c._topics == [TOPIC_SIGNALS_RAW]
        assert c._group_id == GROUP_RISK_GATE
        assert c._auto_offset_reset == "latest"

    def test_for_execution_uses_latest_offset(self):
        c = KafkaConsumerAdapter.for_execution()
        assert c._topics == [TOPIC_SIGNALS_APPROVED]
        assert c._group_id == GROUP_EXECUTION
        assert c._auto_offset_reset == "latest"

    def test_for_portfolio_uses_earliest_offset(self):
        c = KafkaConsumerAdapter.for_portfolio()
        assert c._topics == [TOPIC_ORDERS_FILLED]
        assert c._group_id == GROUP_PORTFOLIO
        assert c._auto_offset_reset == "earliest"

    def test_all_factories_disable_auto_commit(self):
        # Semántica at-least-once: commit manual, nunca auto-commit.
        for factory in (
            KafkaConsumerAdapter.for_bronze_writer,
            KafkaConsumerAdapter.for_feature_consumer,
            KafkaConsumerAdapter.for_strategy_consumer,
            KafkaConsumerAdapter.for_risk_gate,
            KafkaConsumerAdapter.for_execution,
            KafkaConsumerAdapter.for_portfolio,
        ):
            c = factory()
            assert c._enable_auto_commit is False


# ---------------------------------------------------------------------------
# poll() — SafeOps: nunca propaga, retorna [] en fallo o si no está started
# ---------------------------------------------------------------------------


class TestPollSafeOps:
    async def test_poll_returns_empty_when_not_started(self):
        c = KafkaConsumerAdapter(topics=["t"], group_id="g")
        result = await c.poll()
        assert result == []

    async def test_poll_returns_empty_on_exception(self, adapter):
        adapter._consumer._getmany_raises = RuntimeError("boom")
        result = await adapter.poll()
        assert result == []

    async def test_poll_returns_empty_when_no_messages(self, adapter):
        adapter._consumer._getmany_result = {}
        result = await adapter.poll()
        assert result == []


# ---------------------------------------------------------------------------
# commit() — SafeOps: nunca propaga
# ---------------------------------------------------------------------------


class TestCommitSafeOps:
    async def test_commit_noop_when_not_started(self):
        c = KafkaConsumerAdapter(topics=["t"], group_id="g")
        await c.commit()  # no debe lanzar

    async def test_commit_calls_underlying_consumer(self, adapter):
        await adapter.commit()
        assert adapter._consumer._committed == 1

    async def test_commit_does_not_propagate_exception(self, adapter):
        adapter._consumer._commit_raises = RuntimeError("boom")
        await adapter.commit()  # no debe lanzar — SafeOps


# ---------------------------------------------------------------------------
# seek_to_beginning() — Kappa replay, SafeOps
# ---------------------------------------------------------------------------


class TestSeekToBeginning:
    async def test_seek_noop_when_not_started(self):
        c = KafkaConsumerAdapter(topics=["t"], group_id="g")
        await c.seek_to_beginning()  # no debe lanzar

    async def test_seek_calls_underlying_consumer(self, adapter):
        await adapter.seek_to_beginning()
        assert adapter._consumer._sought == 1

    async def test_seek_does_not_propagate_exception(self, adapter):
        adapter._consumer._seek_raises = RuntimeError("boom")
        await adapter.seek_to_beginning()  # no debe lanzar — SafeOps


# ---------------------------------------------------------------------------
# close() — idempotente
# ---------------------------------------------------------------------------


class TestCloseIdempotent:
    async def test_close_stops_consumer(self, adapter):
        await adapter.close()
        assert adapter._consumer._stopped == 1
        assert adapter._started is False

    async def test_close_is_idempotent(self, adapter):
        await adapter.close()
        await adapter.close()
        assert adapter._consumer._stopped == 1

    async def test_close_noop_when_not_started(self):
        c = KafkaConsumerAdapter(topics=["t"], group_id="g")
        await c.close()  # no debe lanzar
