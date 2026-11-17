# -*- coding: utf-8 -*-
"""
tests/trading/test_freshness_enforcement.py
===========================================

D-1 (catálogo mínimo de identidad) + execution gate de freshness en OMS (D-3).

Cubre:
  · StreamIdentity: render/parse con round-trip, símbolos con ``:``, rechazo
    de gramática inválida (fail-closed).
  · StreamIdentityCatalog: autoridad única del mapping, rechazo de duplicados,
    resolución desconocida ⇒ error (nunca None).
  · FreshnessEnforcement: bloqueo tipado en stale / identidad desconocida /
    proveedor caído / snapshot de otro stream.
  · OMS.submit(): ejecución bloqueada sin llamar al executor; camino fresco
    intacto; PAPER (sin enforcement) conserva su comportamiento.

Hermético: sin red, sin Kafka, sin Iceberg, sin sleeps.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from portfolio.infra.memory_store import InMemoryPositionStore
from portfolio.services.portfolio_service import PortfolioService
from trading.application.freshness_enforcement import (
    FreshnessEnforcement,
    FreshnessIdentityMismatchError,
    FreshnessStaleError,
    FreshnessUnavailableError,
    FreshnessUnknownError,
)
from trading.application.oms import OMS
from trading.domain.models.order import Order
from trading.domain.risk.models import RiskConfig
from trading.domain.strategies.base import Signal
from trading.ports.outbound.transport import ExchangeOrderStatus as ExchangeStatus
from trading.ports.outbound.transport import OrderResult, OrderState
from trading.risk.manager import RiskManager

from shared.contracts.boundaries import FreshnessSnapshot
from shared.contracts.stream_identity import (
    AmbiguousStreamBindingError,
    InvalidStreamIdError,
    StreamIdentity,
    StreamIdentityCatalog,
    UnknownStreamBindingError,
)

_NOW = datetime(2024, 1, 1, tzinfo=timezone.utc)
_STREAM_ID = "ws:bybit:BTC/USDT"


# ── Fakes ────────────────────────────────────────────────────────────────────


def _snapshot(
    stream_id: str = _STREAM_ID,
    *,
    is_stale: bool = False,
    lag_ms: int = 100,
    threshold_ms: int = 30_000,
) -> FreshnessSnapshot:
    return FreshnessSnapshot(
        stream_id=stream_id,
        stream_type="ws_trades",
        last_seen_ms=1_000,
        lag_ms=lag_ms,
        is_stale=is_stale,
        staleness_reason="lag_sobre_umbral" if is_stale else None,
        threshold_ms=threshold_ms,
        read_at_ms=1_000 + lag_ms,
        source="test",
    )


class _StubFreshnessPort:
    def __init__(self, snapshot: FreshnessSnapshot | None = None, error: Exception | None = None) -> None:
        self._snapshot = snapshot if snapshot is not None else _snapshot()
        self._error = error
        self.calls: list[str] = []

    def get_freshness(self, stream_id: str) -> FreshnessSnapshot:
        self.calls.append(stream_id)
        if self._error is not None:
            raise self._error
        return self._snapshot


class _RecordingExecutor:
    def __init__(self) -> None:
        self.calls = 0

    def execute(self, order: Order) -> OrderResult:
        self.calls += 1
        return OrderResult(
            accepted=True,
            state=OrderState(
                order_id=f"exc-{order.order_id}",
                status=ExchangeStatus.FILLED,
                fill_price=100.0,
                filled_qty=1.0,
                fees=0.0,
            ),
        )


def _portfolio() -> PortfolioService:
    return PortfolioService(capital_usd=10_000.0, store=InMemoryPositionStore(), exchange="bybit")


def _signal(symbol: str = "BTC/USDT") -> Signal:
    return Signal(
        symbol=symbol,
        timeframe="1h",
        direction="buy",
        price=100.0,
        timestamp=_NOW,
        confidence=1.0,
    )


def _catalog(*pairs: tuple[str, str]) -> StreamIdentityCatalog:
    return StreamIdentityCatalog(pairs or (("bybit", "BTC/USDT"),))


def _enforcement(port: _StubFreshnessPort, catalog: StreamIdentityCatalog | None = None) -> FreshnessEnforcement:
    return FreshnessEnforcement(consumer=port, catalog=catalog or _catalog(), exchange="bybit")


def _oms(enforcement: FreshnessEnforcement | None, executor: _RecordingExecutor | None = None) -> OMS:
    return OMS(
        risk_manager=RiskManager(config=RiskConfig()),
        executor=executor if executor is not None else _RecordingExecutor(),
        portfolio=_portfolio(),
        freshness_enforcement=enforcement,
    )


# ── StreamIdentity ───────────────────────────────────────────────────────────


def test_stream_identity_renders_canonical_grammar() -> None:
    assert StreamIdentity(exchange="bybit", symbol="BTC/USDT").stream_id == "ws:bybit:BTC/USDT"


def test_stream_identity_parse_round_trips() -> None:
    identity = StreamIdentity(exchange="Bybit", symbol="BTC/USDT")
    assert StreamIdentity.parse(identity.stream_id) == identity


def test_stream_identity_supports_symbols_with_colon() -> None:
    identity = StreamIdentity(exchange="bybit", symbol="ETH/USDT:USDT")
    assert StreamIdentity.parse(identity.stream_id) == identity


def test_stream_identity_normalizes_exchange() -> None:
    assert StreamIdentity(exchange="  BYBIT ", symbol="BTC/USDT").exchange == "bybit"


def test_stream_identity_rejects_exchange_with_separator() -> None:
    with pytest.raises(ValueError, match="must not contain"):
        StreamIdentity(exchange="by:bit", symbol="BTC/USDT")


@pytest.mark.parametrize(
    "stream_id",
    ["", "bybit:BTC/USDT", "ws:bybit", "rest:bybit:BTC/USDT"],
)
def test_stream_identity_parse_rejects_invalid_grammar(stream_id: str) -> None:
    with pytest.raises(InvalidStreamIdError):
        StreamIdentity.parse(stream_id)


# ── StreamIdentityCatalog ────────────────────────────────────────────────────


def test_catalog_resolves_declared_binding() -> None:
    identity = _catalog().resolve("bybit", "BTC/USDT")
    assert identity.stream_id == _STREAM_ID


def test_catalog_resolution_is_case_insensitive_on_exchange() -> None:
    assert _catalog().resolve("BYBIT", "BTC/USDT").stream_id == _STREAM_ID


def test_catalog_rejects_unknown_binding() -> None:
    with pytest.raises(UnknownStreamBindingError):
        _catalog().resolve("bybit", "ETH/USDT")


def test_catalog_rejects_duplicate_binding() -> None:
    with pytest.raises(AmbiguousStreamBindingError):
        StreamIdentityCatalog([("bybit", "BTC/USDT"), ("BYBIT", "BTC/USDT")])


def test_catalog_contains_stream_id_rejects_undeclared_and_invalid() -> None:
    catalog = _catalog()
    assert catalog.contains_stream_id(_STREAM_ID) is True
    assert catalog.contains_stream_id("ws:bybit:ETH/USDT") is False
    assert catalog.contains_stream_id("garbage") is False


def test_catalog_exposes_declared_stream_ids() -> None:
    catalog = _catalog(("bybit", "BTC/USDT"), ("kucoin", "ETH/USDT"))
    assert catalog.stream_ids() == ("ws:bybit:BTC/USDT", "ws:kucoin:ETH/USDT")
    assert len(catalog) == 2


# ── FreshnessEnforcement ─────────────────────────────────────────────────────


def test_enforcement_returns_snapshot_when_fresh() -> None:
    port = _StubFreshnessPort()
    snapshot = _enforcement(port).check("BTC/USDT")
    assert snapshot.stream_id == _STREAM_ID
    assert port.calls == [_STREAM_ID]


def test_enforcement_blocks_on_stale() -> None:
    port = _StubFreshnessPort(_snapshot(is_stale=True, lag_ms=60_000))
    with pytest.raises(FreshnessStaleError) as exc:
        _enforcement(port).check("BTC/USDT")
    assert exc.value.reason == "stale"
    assert exc.value.stream_id == _STREAM_ID


def test_enforcement_blocks_on_unknown_symbol() -> None:
    with pytest.raises(FreshnessUnknownError) as exc:
        _enforcement(_StubFreshnessPort()).check("DOGE/USDT")
    assert exc.value.reason == "unknown_stream_identity"
    assert exc.value.stream_id is None


def test_enforcement_blocks_on_provider_error() -> None:
    port = _StubFreshnessPort(error=ConnectionError("feed down"))
    with pytest.raises(FreshnessUnavailableError) as exc:
        _enforcement(port).check("BTC/USDT")
    assert exc.value.reason == "provider_unavailable"


def test_enforcement_blocks_on_identity_mismatch() -> None:
    port = _StubFreshnessPort(_snapshot(stream_id="ws:bybit:ETH/USDT"))
    with pytest.raises(FreshnessIdentityMismatchError) as exc:
        _enforcement(port).check("BTC/USDT")
    assert exc.value.reason == "identity_mismatch"


def test_enforcement_requires_consumer_and_catalog() -> None:
    with pytest.raises(ValueError, match="consumer"):
        FreshnessEnforcement(consumer=None, catalog=_catalog(), exchange="bybit")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="catalog"):
        FreshnessEnforcement(consumer=_StubFreshnessPort(), catalog=None, exchange="bybit")  # type: ignore[arg-type]


# ── Execution gate en OMS.submit() ───────────────────────────────────────────


def test_submit_proceeds_and_calls_executor_when_fresh() -> None:
    executor = _RecordingExecutor()
    order = _oms(_enforcement(_StubFreshnessPort()), executor).submit(_signal())
    assert order is not None
    assert executor.calls == 1


def test_submit_blocks_stale_without_calling_executor() -> None:
    executor = _RecordingExecutor()
    enforcement = _enforcement(_StubFreshnessPort(_snapshot(is_stale=True)))
    assert _oms(enforcement, executor).submit(_signal()) is None
    assert executor.calls == 0


def test_submit_blocks_unknown_identity_without_calling_executor() -> None:
    executor = _RecordingExecutor()
    enforcement = _enforcement(_StubFreshnessPort(), _catalog(("kucoin", "BTC/USDT")))
    assert _oms(enforcement, executor).submit(_signal()) is None
    assert executor.calls == 0


def test_submit_blocks_provider_error_without_calling_executor() -> None:
    executor = _RecordingExecutor()
    enforcement = _enforcement(_StubFreshnessPort(error=TimeoutError("ws timeout")))
    assert _oms(enforcement, executor).submit(_signal()) is None
    assert executor.calls == 0


def test_submit_blocks_identity_mismatch_without_calling_executor() -> None:
    executor = _RecordingExecutor()
    enforcement = _enforcement(_StubFreshnessPort(_snapshot(stream_id="ws:bybit:ETH/USDT")))
    assert _oms(enforcement, executor).submit(_signal()) is None
    assert executor.calls == 0


def test_submit_without_enforcement_preserves_paper_behaviour() -> None:
    executor = _RecordingExecutor()
    order = _oms(None, executor).submit(_signal())
    assert order is not None
    assert executor.calls == 1
