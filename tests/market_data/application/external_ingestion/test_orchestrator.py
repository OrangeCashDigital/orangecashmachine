"""Tests del ExternalIngestionOrchestrator (ADR-0014)."""

from __future__ import annotations

import asyncio

from market_data.application.external_ingestion.orchestrator import (
    ExternalIngestionOrchestrator,
    ExternalSourceRuntime,
)
from market_data.domain.events.external_events import ExternalMetricEvent
from market_data.ports.inbound.external import (
    ExternalSourceUnavailable,
    PollingRequest,
    PollingResult,
)
from market_data.ports.outbound.external_publisher import ExternalEventPublisherPort

_FUNDING_ROW = {"symbol": "BTC-USDT-PERP", "fundingRate": 0.0001, "updateTime": 1700000000000}


class _FakeSource:
    source_id = "coinglass"

    def __init__(self, rows=None, failures_before_success: int = 0) -> None:
        self.rows = rows if rows is not None else []
        self.failures = failures_before_success
        self.calls = 0
        self.closed = False

    async def fetch(self, request: PollingRequest) -> PollingResult:
        self.calls += 1
        if self.calls <= self.failures:
            raise ExternalSourceUnavailable("provider down")
        return PollingResult(source_id=self.source_id, metric=request.metric, payload=self.rows)

    async def close(self) -> None:
        self.closed = True


class _FakePublisher(ExternalEventPublisherPort):
    def __init__(self) -> None:
        self.published: list[tuple[str, ExternalMetricEvent]] = []

    async def publish(self, topic: str, event: ExternalMetricEvent) -> None:
        self.published.append((topic, event))


def _cfg(**overrides) -> ExternalSourceRuntime:
    base = dict(
        source_id="coinglass",
        metric="funding_rate",
        topic="external.raw",
        enabled=True,
    )
    base.update(overrides)
    return ExternalSourceRuntime(**base)


async def _wait_for(predicate, timeout: float = 5.0) -> None:
    for _ in range(int(timeout / 0.02)):
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("timeout esperando condición")


class TestRuntimeConfig:
    def test_min_poll_interval_from_rate_limit(self):
        cfg = _cfg(rate_limit_per_minute=60)
        assert cfg.min_poll_interval_s == 1.0

    def test_min_poll_interval_zero_when_no_limit(self):
        cfg = _cfg(rate_limit_per_minute=0)
        assert cfg.min_poll_interval_s == 0.0

    def test_backoff_increments(self):
        cfg = _cfg(backoff_factor=2.0, backoff_cap_s=100.0)
        assert 1.0 <= cfg.next_backoff_s(1) <= 1.25
        assert 2.0 <= cfg.next_backoff_s(2) <= 2.5

    def test_backoff_capped(self):
        cfg = _cfg(backoff_factor=10.0, backoff_cap_s=5.0)
        assert cfg.next_backoff_s(5) <= 5.0


class TestOrchestrator:
    async def test_full_cycle_fetch_normalize_publish(self):
        src = _FakeSource(rows=[_FUNDING_ROW])
        pub = _FakePublisher()
        orch = ExternalIngestionOrchestrator(
            sources=[_cfg()],
            get_source=lambda sid: src,
            publisher=pub,
        )
        task = asyncio.create_task(orch.run())
        await _wait_for(lambda: bool(pub.published))
        orch._stop_event.set()
        await asyncio.wait_for(task, timeout=5.0)

        assert len(pub.published) == 1
        topic, event = pub.published[0]
        assert topic == "external.raw"
        assert event.source_id == "coinglass"
        assert event.metric == "funding_rate"
        assert event.symbol == "BTC/USDT"
        assert event.value == "0.0001"
        assert orch.last_processed["coinglass"] == 1700000000000

    def test_disabled_sources_are_filtered(self):
        pub = _FakePublisher()
        orch = ExternalIngestionOrchestrator(
            sources=[_cfg(enabled=False)],
            get_source=lambda sid: _FakeSource(),
            publisher=pub,
        )
        assert orch._sources == []
        assert orch.last_processed == {}

    async def test_retry_recovers_after_transient_failure(self):
        src = _FakeSource(rows=[_FUNDING_ROW], failures_before_success=1)
        pub = _FakePublisher()
        orch = ExternalIngestionOrchestrator(
            sources=[_cfg(max_attempts=2, backoff_factor=1.0, backoff_cap_s=0.01)],
            get_source=lambda sid: src,
            publisher=pub,
        )
        task = asyncio.create_task(orch.run())
        await _wait_for(lambda: bool(pub.published))
        orch._stop_event.set()
        await asyncio.wait_for(task, timeout=5.0)

        assert src.calls == 2  # fallo + reintento
        assert len(pub.published) == 1

    async def test_exhausts_retries_and_continues_loop(self):
        src = _FakeSource(rows=[_FUNDING_ROW], failures_before_success=99)
        pub = _FakePublisher()
        orch = ExternalIngestionOrchestrator(
            sources=[_cfg(max_attempts=2, backoff_factor=1.0, backoff_cap_s=0.005)],
            get_source=lambda sid: src,
            publisher=pub,
        )
        task = asyncio.create_task(orch.run())
        # espera un par de ciclos: fetch falla → retry (2 intentos) → no publica
        await _wait_for(lambda: src.calls >= 3)
        orch._stop_event.set()
        await asyncio.wait_for(task, timeout=5.0)

        assert not pub.published
        assert src.calls >= 3

    async def test_source_created_once_and_closed_on_shutdown(self):
        created: dict[str, int] = {"n": 0}

        def factory(sid: str) -> _FakeSource:
            created["n"] += 1
            return _FakeSource(rows=[_FUNDING_ROW])

        pub = _FakePublisher()
        orch = ExternalIngestionOrchestrator(
            sources=[_cfg()],
            get_source=factory,
            publisher=pub,
        )
        task = asyncio.create_task(orch.run())
        await _wait_for(lambda: bool(pub.published))
        orch._stop_event.set()
        await asyncio.wait_for(task, timeout=5.0)

        assert len(orch._source_instances) == 1  # una instancia por source_id
        assert created["n"] == 1  # get_source se llama una sola vez
        (src,) = orch._source_instances.values()
        assert src.closed is True  # close() explícito en shutdown

    async def test_retry_reuses_same_source_instance(self):
        def factory(sid: str) -> _FakeSource:
            return _FakeSource(rows=[_FUNDING_ROW], failures_before_success=1)

        pub = _FakePublisher()
        orch = ExternalIngestionOrchestrator(
            sources=[_cfg(max_attempts=2, backoff_factor=1.0, backoff_cap_s=0.01)],
            get_source=factory,
            publisher=pub,
        )
        task = asyncio.create_task(orch.run())
        await _wait_for(lambda: bool(pub.published))
        orch._stop_event.set()
        await asyncio.wait_for(task, timeout=5.0)

        assert len(orch._source_instances) == 1  # fetch + reintento comparten instancia
