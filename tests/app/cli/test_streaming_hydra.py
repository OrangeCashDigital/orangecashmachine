# -*- coding: utf-8 -*-
"""
tests/app/cli/test_streaming_hydra.py
=======================================

Tests de lifecycle de apps/app/cli/streaming_hydra.py (canary ORDERBOOK, F2.6b).

Verifica:
  1. _heartbeat_loop empuja labels {exchange: "orderbook", gateway} y para al
     setear el stop event (SIGINT/SIGTERM path).
  2. _run_streaming hace shutdown ordenado ADR-0022 (stop event → stream.stop()
     → bundle.close_all()) y retorna 0.
  3. main() falla rápido si el exchange no está habilitado en feeds.

F2.6c (métricas en Pushgateway) se cubre en
tests/market_data/.../test_orderbook_producer_metrics.py (KafkaMetrics real).

Principios: lifecycle · Composition Root · ADR-0022 · fail-fast
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from app.cli import streaming_hydra


class _FakePusher:
    def __init__(self) -> None:
        self.pushed: list[dict[str, Any]] = []

    def push(self, labels: dict[str, Any] | None = None) -> None:
        self.pushed.append(labels or {})


class _FakeContext:
    pushgateway = "metrics.internal:9091"


class _FakeProducer:
    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def on_snapshot(self, **kwargs) -> None:
        return None

    async def on_delta(self, **kwargs) -> None:
        return None


class _FakeBundle:
    def __init__(self) -> None:
        self.orderbook = _FakeProducer()
        self.started = False
        self.closed = False

    async def start_all(self) -> None:
        self.started = True

    async def close_all(self) -> None:
        self.closed = True


class _FakeStream:
    def __init__(self, bundle: _FakeBundle) -> None:
        self._bundle = bundle
        self.stopped = False

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        self.stopped = True


class _FakeMetrics:
    enabled = True


class _FakeObservability:
    metrics = _FakeMetrics()


class _FakeKafka:
    bootstrap_servers = "localhost:9092"


class _FakeIntegrations:
    kafka = _FakeKafka()


class _FakeFeedsEntry:
    enabled = True
    symbols = ["BTC-USDT-PERP"]


class _FakeFeeds:
    feeds = {"bybit": _FakeFeedsEntry()}


class _FakeConfig:
    observability = _FakeObservability()
    integrations = _FakeIntegrations()
    feeds = _FakeFeeds()


@pytest.fixture
def bundle() -> _FakeBundle:
    return _FakeBundle()


@pytest.fixture
def fake_stream(bundle: _FakeBundle) -> _FakeStream:
    return _FakeStream(bundle)


@pytest.fixture
def patch_streaming(monkeypatch, bundle: _FakeBundle, fake_stream: _FakeStream):
    """Parchea CompositionRoot.build_ws_producers, CryptofeedOrderBookStream y
    RuntimeContext por fakes — no toca red ni Kafka."""
    import market_data.adapters.inbound.websocket.cryptofeed_orderbook_stream as stream_mod
    import market_data.infrastructure.bootstrap.composition_root as cr_mod

    import ocm.runtime.context as ctx_mod

    monkeypatch.setattr(cr_mod.CompositionRoot, "build_ws_producers", lambda bootstrap: bundle)
    monkeypatch.setattr(stream_mod, "CryptofeedOrderBookStream", lambda **kwargs: fake_stream)
    monkeypatch.setattr(ctx_mod, "RuntimeContext", lambda **kwargs: _FakeContext())


# ---------------------------------------------------------------------------
# _heartbeat_loop — labels y stop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestHeartbeatLoop:
    async def test_pushes_labels_orderbook_and_gateway(self):
        pusher = _FakePusher()
        stop = asyncio.Event()

        async def _run() -> None:
            task = asyncio.create_task(streaming_hydra._heartbeat_loop(pusher, "metrics.internal:9091", stop, 0.01))
            await asyncio.sleep(0.05)
            stop.set()
            await task

        await _run()

        assert pusher.pushed, "el loop debe empujar al menos un heartbeat"
        assert pusher.pushed[0]["exchange"] == "orderbook"
        assert pusher.pushed[0]["gateway"] == "metrics.internal:9091"

    async def test_stops_when_event_set(self):
        pusher = _FakePusher()
        stop = asyncio.Event()
        stop.set()  # ya seteado → el loop no debe empujar nada

        await streaming_hydra._heartbeat_loop(pusher, "gw:9091", stop, 0.01)
        assert pusher.pushed == []


# ---------------------------------------------------------------------------
# _run_streaming — shutdown ordenado ADR-0022
# ---------------------------------------------------------------------------


async def _short_heartbeat(pusher, gateway, stop, push_interval):
    """Simula el heartbeat interrumpido por stop event (como una señal real).

    En tests no llegan SIGINT/SIGTERM — el event interno de _run_streaming
    nunca se setea, así que el loop real no terminaría. Este double enroca
    el camino "el heartbeat retorna → finally hace shutdown ordenado".
    """
    await asyncio.sleep(0.01)


@pytest.mark.asyncio
class TestRunStreaming:
    async def test_shutdown_order_and_return_zero(
        self,
        patch_streaming,
        monkeypatch,
        bundle: _FakeBundle,
        fake_stream: _FakeStream,
    ):
        monkeypatch.setattr(streaming_hydra, "_heartbeat_loop", _short_heartbeat)
        run_cfg = type("RunCfg", (), {"env": "test"})()

        result = await streaming_hydra._run_streaming(
            _FakeConfig(),
            run_cfg,
            exchange="bybit",
            symbols=["BTC-USDT-PERP"],
            push_interval=0.01,
        )

        assert result == 0
        assert bundle.started is True
        assert fake_stream.stopped is True
        assert bundle.closed is True


# ---------------------------------------------------------------------------
# main — fail-fast si el exchange no está habilitado
# ---------------------------------------------------------------------------


class TestMainFailFast:
    def test_exchange_not_enabled_returns_1(self, monkeypatch):
        class _Entry:
            enabled = False
            symbols = ["BTC-USDT-PERP"]

        class _Feeds:
            feeds = {"bybit": _Entry()}

        class _Cfg:
            feeds = _Feeds()

        monkeypatch.setattr(streaming_hydra, "_load_config", lambda env, *, run_id=None: _Cfg())
        assert streaming_hydra.main(["--exchange", "bybit"]) == 1

    def test_missing_exchange_returns_1(self, monkeypatch):
        class _Feeds:
            feeds = {}

        class _Cfg:
            feeds = _Feeds()

        monkeypatch.setattr(streaming_hydra, "_load_config", lambda env, *, run_id=None: _Cfg())
        assert streaming_hydra.main(["--exchange", "bybit"]) == 1

    def test_config_load_failure_returns_1(self, monkeypatch):
        monkeypatch.setattr(streaming_hydra, "_load_config", lambda env, *, run_id=None: None)
        assert streaming_hydra.main(["--exchange", "bybit"]) == 1
