# -*- coding: utf-8 -*-
"""
tests/trading/test_f1_execution_quantity.py
=============================================

F1 (ADR-0025/0026/0027) — Execution Quantity / SELL sizing.

Regla canónica: la cantidad de una orden de reducción/cierre (SELL) se deriva
de la cantidad económica real de la posición (Position.quantity), NUNCA de
signal.quantity/requested/size_pct cuando representen una cantidad diferente.
Nunca se pide más de lo disponible (INV-08). Un partial fill reduce la
cantidad económica disponible; requested ≠ executed no contamina el sizing
posterior.

Cubre los TEST 1-7 obligatorios de la tarea:

  TEST 1 — requested BUY = 1.0, executed = 0.37 → Position.quantity = 0.37;
           el SELL de cierre se pide como MÁXIMO 0.37.
  TEST 2 — Position = 0.37, signal.quantity = 1.0 → SELL generado = 0.37.
  TEST 3 — Position = 0.37, close requested = 0.10 → SELL = 0.10,
           remaining = 0.27.
  TEST 4 — close requested > position quantity → se impide vender más de lo
           disponible (clamp) y un SELL sin posición se rechaza (fail-closed).
  TEST 5 — signal.price != fill real → el estado económico usa el fill real
           (INV-10), nunca signal.price.
  TEST 6 — stop-loss con Position.quantity = 0.37 → SELL ≤ 0.37.
  TEST 7 — quantity UNKNOWN → no se inventa cantidad (política ADR-0025/0027).

Además se verifica el fallback de fill_price eliminado (fill_price UNKNOWN no
se sustituye por signal.price).

Principios: Aislamiento (sin Redis, sin Iceberg, sin red) · Fail-Fast ·
Nomenclatura test_<condición>.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import polars as pl
import pytest
from portfolio.infra.memory_store import InMemoryPositionStore
from portfolio.models.position import PortfolioState, PositionSnapshot
from portfolio.services.portfolio_service import PortfolioService
from trading.analytics.trade_tracker import TradeTracker
from trading.engine import TradingEngine
from trading.execution.fill_sync import build_fill_sync
from trading.execution.oms import OMS
from trading.execution.order import Order
from trading.execution.transport import OrderResult, OrderState
from trading.execution.transport import OrderStatus as TStatus
from trading.risk.manager import RiskManager
from trading.risk.models import RiskConfig, StopLossConfig
from trading.strategies.base import BaseStrategy, Signal

_NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

_Fill = tuple[Optional[float], Optional[float], Optional[float]]


# ---------------------------------------------------------------------------
# Fakes / helpers
# ---------------------------------------------------------------------------


class _ScriptedExecutor:
    """Executor que llena secuencialmente con (price, qty, fees) por lado."""

    def __init__(self, fills_by_side: dict[str, list[_Fill]]) -> None:
        self._iters = {side: iter(fills) for side, fills in fills_by_side.items()}

    def execute(self, order: Order) -> OrderResult:
        fill_price, filled_qty, fees = next(self._iters[order.side.value])
        return OrderResult(
            accepted=True,
            state=OrderState(
                order_id=f"exc-{order.order_id}",
                status=TStatus.FILLED,
                fill_price=fill_price,
                filled_qty=filled_qty,
                fees=fees,
            ),
        )


def _signal(side: str, price: float, quantity: Optional[float] = None) -> Signal:
    return Signal(
        symbol="BTC/USDT",
        timeframe="1h",
        direction=side,
        price=price,
        timestamp=_NOW,
        confidence=1.0,
        quantity=quantity,
    )


def _chain(executor) -> tuple[OMS, PortfolioService, RiskManager]:
    """OMS + fill_sync + PortfolioService + TradeTracker reales, en memoria."""
    store = InMemoryPositionStore()
    portfolio = PortfolioService(capital_usd=10_000.0, store=store, exchange="bybit")
    tracker = TradeTracker(exchange="bybit")
    risk = RiskManager(config=RiskConfig())
    oms = OMS(
        risk_manager=risk,
        executor=executor,
        on_fill=build_fill_sync(tracker, portfolio),
    )
    return oms, portfolio, risk


# ── TEST 1 — requested=1.0, executed=0.37 → SELL máximo 0.37 ─────────────────


def test_sell_request_never_exceeds_executed_position() -> None:
    """requested BUY = 1.0, executed = 0.37 → Position.quantity = 0.37.

    La orden SELL que llega después se pide como MÁXIMO 0.37 — nunca 1.0.
    """
    oms, portfolio, _ = _chain(_ScriptedExecutor({"buy": [(100.0, 0.37, 0.0)], "sell": [(110.0, 0.37, 0.0)]}))

    buy = oms.submit(_signal("buy", price=100.0, quantity=1.0))  # requested target 1.0
    assert buy is not None and buy.quantity == pytest.approx(1.0)  # REQUESTED
    assert buy.filled_qty == pytest.approx(0.37)  # EXECUTED

    pos = portfolio.snapshot().positions
    assert len(pos) == 1 and pos[0].quantity == pytest.approx(0.37)

    sell = oms.submit(_signal("sell", price=110.0))
    assert sell is not None
    assert sell.quantity == pytest.approx(0.37), "SELL ≤ Position.quantity (0.37), nunca 1.0"
    assert sell.quantity <= 0.37
    assert sell.filled_qty == pytest.approx(0.37)
    assert portfolio.snapshot().is_flat


# ── TEST 2 — Position=0.37, signal.quantity=1.0 → SELL=0.37 ──────────────────


def test_sell_clamped_to_position_when_signal_over_requests() -> None:
    """signal.quantity = 1.0 con Position = 0.37 → SELL generado = 0.37."""
    oms, portfolio, _ = _chain(_ScriptedExecutor({"buy": [(100.0, 0.37, 0.0)], "sell": [(110.0, 0.37, 0.0)]}))

    oms.submit(_signal("buy", price=100.0))
    sell = oms.submit(_signal("sell", price=110.0, quantity=1.0))

    assert sell is not None
    assert sell.quantity == pytest.approx(0.37), "el SELL se clampa a la posición (INV-08)"
    assert sell.quantity <= 0.37


# ── TEST 3 — Position=0.37, close requested=0.10 → SELL=0.10, remaining=0.27 ──


def test_partial_close_request_sized_and_remaining_updated() -> None:
    """Cierre parcial 0.10 sobre 0.37 → SELL=0.10, remaining=0.27."""
    oms, portfolio, _ = _chain(_ScriptedExecutor({"buy": [(100.0, 0.37, 0.0)], "sell": [(110.0, 0.10, 0.0)]}))

    oms.submit(_signal("buy", price=100.0))
    sell = oms.submit(_signal("sell", price=110.0, quantity=0.10))

    assert sell is not None
    assert sell.quantity == pytest.approx(0.10)
    assert sell.filled_qty == pytest.approx(0.10)

    positions = portfolio.snapshot().positions
    assert len(positions) == 1, "cierre parcial mantiene la posición abierta"
    assert positions[0].quantity == pytest.approx(0.27)
    assert positions[0].avg_entry == pytest.approx(100.0)

    # La cantidad disponible se redujo: el siguiente SELL se pide ≤ 0.27.
    assert oms._entry_positions["BTC/USDT"][0] == pytest.approx(0.27)


# ── TEST 4 — close requested > position quantity → impedido ──────────────────


def test_sell_over_request_never_exceeds_available() -> None:
    """No se puede pedir más de la posición: se clampa y, sin posición, se rechaza."""
    # (a) clamp sobre lo disponible
    oms, portfolio, _ = _chain(_ScriptedExecutor({"buy": [(100.0, 0.37, 0.0)], "sell": [(110.0, 0.37, 0.0)]}))
    oms.submit(_signal("buy", price=100.0))
    sell = oms.submit(_signal("sell", price=110.0, quantity=5.0))
    assert sell is not None
    assert sell.quantity == pytest.approx(0.37)
    assert sell.quantity <= 0.37, "nunca más de lo disponible"

    # (b) sin posición y sin TARGET de cantidad → rechazo fail-closed (None).
    oms2, portfolio2, risk2 = _chain(_ScriptedExecutor({}))
    rejected = oms2.submit(_signal("sell", price=110.0))
    assert rejected is None, "SELL sin posición y sin cantidad → rechazo fail-closed"
    assert risk2.open_positions == 0
    assert portfolio2.snapshot().open_count == 0


# ── TEST 5 — signal.price != fill real → estado económico usa el fill real ────


def test_economic_state_never_uses_signal_price_as_fill() -> None:
    """El estado económico usa el fill real (INV-10), nunca signal.price."""
    oms, portfolio, risk = _chain(_ScriptedExecutor({"buy": [(49_800.0, 0.37, 0.0)], "sell": [(49_900.0, 0.37, 0.0)]}))

    buy = oms.submit(_signal("buy", price=50_000.0))
    positions = portfolio.snapshot().positions
    assert len(positions) == 1, "BUY abre la posición antes del cierre"
    assert positions[0].avg_entry == pytest.approx(49_800.0)
    assert positions[0].avg_entry != 50_000.0, "el avg es el fill real, no signal.price"

    sell = oms.submit(_signal("sell", price=50_100.0))

    assert buy is not None and buy.fill_price == pytest.approx(49_800.0)
    assert buy.fill_price != buy.signal.price, "no debe usar signal.price como fill"
    assert sell is not None and sell.fill_price == pytest.approx(49_900.0)
    assert sell.fill_price != sell.signal.price, "no debe usar signal.price como fill"

    # P&L realizado contra fills reales (49_900 vs avg 49_800), no signal.price.
    # RiskManager.state() usa total_pnl_usd (ADR-0025/0026).
    assert risk.state()["total_pnl_usd"] == pytest.approx(37.0)


# ── TEST 6 — stop-loss con Position.quantity = 0.37 → SELL ≤ 0.37 ────────────


class _NoSignalStrategy(BaseStrategy):
    """Estrategia que nunca genera señales — aísla el stop-loss."""

    name = "noop"
    symbol = "BTC/USDT"
    timeframe = "1h"

    def generate_signals(self, df):
        return []


class _CloseDataSource:
    """FeatureSource con un único close fijo — sin Iceberg."""

    def __init__(self, close: float) -> None:
        self._close = close

    def load_features(self, exchange, symbol, timeframe, market_type="spot", **kwargs):
        return pl.DataFrame(
            {
                "timestamp": [_NOW],
                "open": [self._close],
                "high": [self._close * 1.001],
                "low": [self._close * 0.999],
                "close": [self._close],
                "volume": [100.0],
            }
        )


class _PositionPortfolio:
    """Fake de PortfolioService con posiciones abiertas fijas."""

    def __init__(self, positions: list[PositionSnapshot]) -> None:
        self._positions = positions

    def snapshot(self) -> PortfolioState:
        return PortfolioState(positions=tuple(self._positions), capital_usd=10_000.0)

    def open_position(self, **kwargs) -> None:
        pass

    def close_position(self, order_id):
        return None


def test_stop_loss_sell_request_respects_position_quantity() -> None:
    """Stop-loss con Position.quantity = 0.37 → SELL solicitado ≤ 0.37."""
    stop = StopLossConfig(enabled=True, default_pct=0.02)  # stop en 49_000
    portfolio = _PositionPortfolio(
        [
            PositionSnapshot(
                symbol="BTC/USDT",
                exchange="bybit",
                side="long",
                quantity=0.37,
                avg_entry=50_000.0,
                size_pct=0.05,
                entry_at=_NOW,
                order_id="pos-1",
            )
        ]
    )
    risk = RiskManager(config=RiskConfig(stop_loss=stop))
    oms = OMS(
        risk_manager=risk,
        executor=_ScriptedExecutor({"sell": [(48_000.0, 0.37, 0.0)]}),
    )
    engine = TradingEngine(
        strategy=_NoSignalStrategy(),
        oms=oms,
        data_source=_CloseDataSource(48_000.0),
        exchange="bybit",
        portfolio=portfolio,
        stop_loss=stop,
    )

    result = engine.run_once()

    assert result.stop_loss_closes == 1
    sell = result.orders[0]
    assert sell.side.value == "sell"
    assert sell.status.value == "filled"
    assert sell.quantity == pytest.approx(0.37), "stop-loss pide como máximo la posición"
    assert sell.quantity <= 0.37


# ── TEST 7 — quantity UNKNOWN → no se inventa ────────────────────────────────


def test_unknown_quantity_not_invented() -> None:
    """BUY fill sin filled_qty → sin WAC y sin posición; SELL posterior rechazado."""
    oms, portfolio, _ = _chain(_ScriptedExecutor({"buy": [(100.0, None, 0.0)]}))

    buy = oms.submit(_signal("buy", price=100.0))
    assert buy is not None
    assert buy.filled_qty is None, "cantidad UNKNOWN (exchange no reporta) → None"
    assert oms._entry_positions.get("BTC/USDT") is None, "sin cantidad → sin WAC acumulado"
    assert portfolio.snapshot().open_count == 0, "no se crea posición con cantidad inventada"

    # Sin cantidad económica disponible, el SELL se rechaza — no se inventa qty.
    sell = oms.submit(_signal("sell", price=100.0))
    assert sell is None, "SELL sin cantidad disponible → rechazo fail-closed"


# ── Fallback de fill_price eliminado (INV-10) ────────────────────────────────


def test_unknown_buy_fill_price_not_replaced_by_signal_price() -> None:
    """fill_price UNKNOWN no se sustituye por signal.price (fallback eliminado)."""
    oms, portfolio, _ = _chain(_ScriptedExecutor({"buy": [(None, 0.5, 0.0)]}))

    buy = oms.submit(_signal("buy", price=50_000.0))

    assert buy is not None
    assert buy.fill_price is None, "precio UNKNOWN → None, nunca signal.price (INV-10)"
    assert buy.filled_qty == pytest.approx(0.5)
    assert oms._entry_positions.get("BTC/USDT") is None, "sin precio → sin avg inventado"


def test_unknown_sell_fill_price_reduces_quantity_but_keeps_pnl_unknown() -> None:
    """SELL con precio UNKNOWN: la cantidad sí reduce la posición; el P&L no se inventa."""
    oms, portfolio, risk = _chain(_ScriptedExecutor({"buy": [(100.0, 1.0, 0.0)], "sell": [(None, 1.0, 0.0)]}))

    oms.submit(_signal("buy", price=100.0))
    sell = oms.submit(_signal("sell", price=100.0))

    assert sell is not None and sell.fill_price is None
    assert oms._entry_positions.get("BTC/USDT") is None, "cerrada a qty=0 (cantidad ejecutada)"
    assert portfolio.snapshot().is_flat, "la posición se reduce con la cantidad ejecutada"
    # P&L no inventado: risk no gana ni pierde por un precio desconocido (ADR-0026).
    assert risk.state()["total_pnl_usd"] == 0.0
