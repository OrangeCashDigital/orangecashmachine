# -*- coding: utf-8 -*-
"""
tests/trading/test_fill_sync_close_divergence.py
=================================================

B-15 (H-09): observabilidad de cierre de posición no confirmado.

El SELL path de `build_fill_sync` debe capturar el retorno de
`portfolio.close_position(buy_order_id)` y, cuando la porción cerrada sea
None (cierre no confirmado por SafeOps), emitir una alerta CRITICAL
identificando symbol, sell_order_id y buy_order_id — sin lanzar y sin
cambiar el contrato SafeOps.

Cobertura
---------
A. Caso normal    : close_position() confirma (porción no-None) → sin alerta.
B. Caso de fallo  : close_position() devuelve porción None → alerta.
C. SafeOps         : un fill cuyo close_position() devuelve porción None NO
                     propaga excepción desde fill_sync.
D. Captura de logs : se usa un fake de loguru (patrón del proyecto, ver
                     tests/market_data/test_quality_consumer_wiring.py),
                     no una búsqueda frágil de cadena de texto.

El contrato (ADR-0025/F4b) devuelve (closed, remaining): `closed` None sigue
siendo indistinguible entre "posición no existía" y "fallo de persistencia";
en el SELL path ambos implican divergencia (ver documentación en fill_sync).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from loguru import logger
from trading.analytics.trade_tracker import TradeTracker
from trading.execution.fill_sync import build_fill_sync
from trading.execution.order import Order, OrderSide, OrderStatus
from trading.strategies.base import Signal


def _signal(side: str) -> Signal:
    return Signal(
        symbol="BTC/USDT",
        timeframe="1m",
        direction=side,
        price=50_000.0,
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        confidence=1.0,
    )


def _order(side: OrderSide, symbol: str = "BTC/USDT", order_id: str | None = None) -> Order:
    order = Order(
        symbol=symbol,
        side=side,
        size_pct=0.5,
        signal=_signal(side.value),
        order_id=order_id or f"{side.value}-{symbol}",
    )
    order.transition(OrderStatus.SUBMITTED)
    order.transition(
        OrderStatus.FILLED,
        fill_price=50_000.0,
        fill_timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    return order


class _FakeLogger:
    """Captura las llamadas critical/warning hechas a loguru (patrón del repo)."""

    def __init__(self) -> None:
        self.critical_calls: list[str] = []
        self.warning_calls: list[str] = []

    def critical(self, msg: str, *args, **kwargs) -> None:
        self.critical_calls.append(msg.format(*args) if args else msg)

    def warning(self, msg: str, *args, **kwargs) -> None:
        self.warning_calls.append(msg.format(*args) if args else msg)


class _TrackerStub:
    """TradeTracker falso — capture on_fill sin acoplar analytics."""

    def on_fill(self, order) -> None:
        pass


class _ClosedPosition:
    """Retornado por close_position cuando el cierre se confirma."""

    def __init__(self, order_id: str) -> None:
        self.order_id = order_id


# ── A. Caso normal: cierre confirmado → sin alerta crítica ────────────────────


def test_normal_close_confirmed_no_critical_alert(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_logger = _FakeLogger()
    monkeypatch.setattr(logger, "critical", fake_logger.critical)
    monkeypatch.setattr(logger, "warning", fake_logger.warning)

    class _Portfolio:
        def open_position(self, **kwargs) -> None:
            pass

        def close_position(self, order_id, quantity=None):
            # Cierre confirmado → devuelve (porción cerrada, remaining=0)
            return _ClosedPosition(order_id), 0.0

    tracker, portfolio = _TrackerStub(), _Portfolio()
    on_fill = build_fill_sync(tracker, portfolio)

    on_fill(_order(OrderSide.BUY, order_id="buy-1"))
    on_fill(_order(OrderSide.SELL, order_id="sell-1"))

    assert fake_logger.critical_calls == [], "Cierre confirmado no debe emitir alerta crítica"


# ── B. Caso de fallo: cierre None → alerta de divergencia ─────────────────────


def test_unconfirmed_close_emits_critical_alert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_logger = _FakeLogger()
    monkeypatch.setattr(logger, "critical", fake_logger.critical)

    class _Portfolio:
        def open_position(self, **kwargs) -> None:
            pass

        def close_position(self, order_id, quantity=None):
            # SafeOps: retorna (None, 0.0) (no confirmado) sin lanzar
            return None, 0.0

    on_fill = build_fill_sync(_TrackerStub(), _Portfolio())
    on_fill(_order(OrderSide.BUY, order_id="buy-1"))
    on_fill(_order(OrderSide.SELL, order_id="sell-1"))

    assert len(fake_logger.critical_calls) == 1, "Debe emitir exactamente una alerta crítica"
    msg = fake_logger.critical_calls[0]
    assert "POSITION_CLOSE_UNCONFIRMED" in msg
    assert "sell-1" in msg, "Debe identificar el sell_order_id"
    assert "buy-1" in msg, "Debe identificar el buy_order_id"
    assert "BTC/USDT" in msg, "Debe identificar el symbol"
    assert "divergencia" in msg, "Debe señalar la posible divergencia de estado"


# ── C. SafeOps: el fallo de close NO propaga excepción desde fill_sync ────────


def test_unconfirmed_close_does_not_raise(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_logger = _FakeLogger()
    monkeypatch.setattr(logger, "critical", fake_logger.critical)

    class _Portfolio:
        def open_position(self, **kwargs) -> None:
            pass

        def close_position(self, order_id, quantity=None):
            return None, 0.0

    on_fill = build_fill_sync(_TrackerStub(), _Portfolio())
    on_fill(_order(OrderSide.BUY, order_id="buy-1"))
    on_fill(_order(OrderSide.SELL, order_id="sell-1"))  # no debe lanzar

    assert len(fake_logger.critical_calls) == 1


# ── SELL sin BUY previo: conserva la advertencia existente, no alerta crítica ─


def test_lone_sell_preserves_warning_not_critical(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_logger = _FakeLogger()
    monkeypatch.setattr(logger, "critical", fake_logger.critical)
    monkeypatch.setattr(logger, "warning", fake_logger.warning)

    class _Portfolio:
        def open_position(self, **kwargs) -> None:
            pass

        def close_position(self, order_id, quantity=None):
            return None, 0.0

    on_fill = build_fill_sync(_TrackerStub(), _Portfolio())
    on_fill(_order(OrderSide.SELL, order_id="sell-1"))  # sin BUY previo

    assert fake_logger.critical_calls == []
    assert len(fake_logger.warning_calls) == 1, "Debe conservar el warning de SELL sin BUY previo"
    assert "SELL sin BUY previo" in fake_logger.warning_calls[0]


# ── Utiliza traker real de TradeTracker para confirmar que el flujo no regresa ─


def test_tracker_real_still_syncs_on_fill(monkeypatch: pytest.MonkeyPatch) -> None:
    """Un tracker real (TradeTracker) recibe el fill sin drama + alerta en fallo."""
    fake_logger = _FakeLogger()
    monkeypatch.setattr(logger, "critical", fake_logger.critical)

    class _Portfolio:
        def open_position(self, **kwargs) -> None:
            pass

        def close_position(self, order_id, quantity=None):
            return None, 0.0

    tracker = TradeTracker(exchange="bybit")
    on_fill = build_fill_sync(tracker, _Portfolio())

    on_fill(_order(OrderSide.BUY, order_id="buy-1"))
    on_fill(_order(OrderSide.SELL, order_id="sell-1"))

    assert fake_logger.critical_calls != []
