# -*- coding: utf-8 -*-
"""
tests/trading/test_f2_risk_exposure.py
========================================

F2 (ADR-0025/0026/0027) — Risk basado en EXPOSICIÓN ECONÓMICA REAL.

Risk debe representar la exposición de una posición existente con la
cantidad realmente ejecutada y el estado canónico de Position
(quantity × avg_entry), NUNCA con signal.quantity/requested/size_pct/
capital×size_pct como sustitutos. Position count ≠ quantity ≠ notional.
UNKNOWN ≠ ZERO.

Cubre los TEST 1-8 obligatorios de la tarea:

  TEST 1 — EXPOSICIÓN REAL: requested = 1.0, executed = 0.37 → Risk expone
           0.37 × avg, no 1.0.
  TEST 2 — CIERRE PARCIAL: 1.0 → cierre 0.30 → remaining 0.70; Risk
           reconoce la posición todavía abierta (exposición 0.70 × avg).
  TEST 3 — COUNT: un cierre parcial NO convierte count 1 → 0; solo el cierre
           completo decrementa.
  TEST 4 — REDUCE: SELL/reduce quantity ≤ Position.quantity (INV-F2-05).
  TEST 5 — SIGNAL SIZE: signal size_pct (capital × size_pct) ≠ exposición
           real; Risk no usa size_pct para representar la posición existente.
  TEST 6 — MIN/MAX: min/max_order_usd gobiernan ENTRADAS (BUY); una
           reducción no se rechaza por sizing de una nueva posición.
  TEST 7 — STOP-LOSS: una reducción de stop-loss no se trata como apertura
           (no bloqueada por max_open_positions).
  TEST 8 — UNKNOWN: cantidad/precio desconocido NO se convierte en 0 para
           calcular exposición financiera (INV-F2-06).

Invariantes demostradas: INV-F2-01..06 (en cada test, marcado).

Principios: Aislamiento (sin Redis, sin Iceberg, sin red) · Fail-Fast ·
Nomenclatura test_<condición>.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pandas as pd
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
from trading.risk.models import (
    OrderLimits,
    PositionConfig,
    RiskConfig,
    StopLossConfig,
)
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
        return pd.DataFrame(
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

    def close_position(self, order_id, quantity=None):
        return None, 0.0


# ── TEST 1 — exposición real (no signal quantity) ────────────────────────────


def test_exposure_uses_real_executed_quantity_not_signal_request() -> None:
    """TEST 1: requested = 1.0, executed = 0.37 → exposición = 0.37 × avg.

    INV-F2-01: risk.quantity = Position.quantity (0.37), nunca signal quantity.
    INV-F2-04: exposición != capital × size_pct.
    """
    oms, portfolio, risk = _chain(_ScriptedExecutor({"buy": [(49_800.0, 0.37, 0.0)]}))

    buy = oms.submit(_signal("buy", price=50_000.0, quantity=1.0))
    assert buy is not None
    assert buy.filled_qty == pytest.approx(0.37)  # EXECUTED real

    # INV-F2-01: la cantidad económica de Risk es la ejecutada, no la pedida.
    assert risk.quantity("BTC/USDT") == pytest.approx(0.37)
    assert risk.quantity("BTC/USDT") != 1.0

    # Exposición real = 0.37 × avg (cost basis, ADR-0025).
    expected = 0.37 * 49_800.0
    assert risk.exposure_usd == pytest.approx(expected)
    assert risk.state()["exposure_usd"] == pytest.approx(expected, abs=0.01)

    # INV-F2-04: la exposición no es capital × size_pct (5% de $10k = $500).
    assert risk.exposure_usd != pytest.approx(10_000.0 * 0.05)
    assert risk.exposure_usd > 10_000.0 * 0.05
    assert risk.state()["quantity_by_symbol"]["BTC/USDT"] == pytest.approx(0.37, abs=1e-8)


# ── TEST 2 — cierre parcial → remaining, posición abierta ────────────────────


def test_partial_close_keeps_position_open_with_remaining() -> None:
    """TEST 2: 1.0 → cierre 0.30 → remaining 0.70; Risk sigue la posición abierta.

    INV-F2-02: remaining_quantity > 0 → posición conservada (no cerrada).
    INV-F2-03: count (1) ≠ quantity (0.70) — conceptos separados.
    """
    oms, portfolio, risk = _chain(_ScriptedExecutor({"buy": [(100.0, 1.0, 0.0)], "sell": [(110.0, 0.30, 0.0)]}))

    oms.submit(_signal("buy", price=100.0))
    sell = oms.submit(_signal("sell", price=110.0, quantity=0.30))
    assert sell is not None
    assert sell.filled_qty == pytest.approx(0.30)

    # INV-F2-02: remaining = 0.70 > 0 → la posición sigue abierta.
    assert risk.quantity("BTC/USDT") == pytest.approx(0.70)
    assert risk.open_positions == 1  # INV-F2-02: no se cierra
    assert risk.exposure_usd == pytest.approx(0.70 * 100.0)
    assert portfolio.snapshot().open_count == 1
    assert portfolio.snapshot().positions[0].quantity == pytest.approx(0.70)

    # INV-F2-03: count (posiciones HELD) ≠ quantity (unidades base).
    assert risk.open_positions == 1
    assert risk.quantity("BTC/USDT") == pytest.approx(0.70)
    assert risk.state()["open_positions"] == 1
    assert risk.state()["quantity_by_symbol"]["BTC/USDT"] == pytest.approx(0.70, abs=1e-8)


# ── TEST 3 — count no se anula en cierre parcial ─────────────────────────────


def test_partial_close_does_not_decrement_position_count() -> None:
    """TEST 3: cierre parcial NO convierte count 1 → 0; solo el completo."""
    oms, portfolio, risk = _chain(
        _ScriptedExecutor(
            {
                "buy": [(100.0, 1.0, 0.0)],
                "sell": [(110.0, 0.30, 0.0), (110.0, 0.70, 0.0)],
            }
        )
    )

    oms.submit(_signal("buy", price=100.0))
    assert risk.open_positions == 1

    oms.submit(_signal("sell", price=110.0, quantity=0.30))
    assert risk.open_positions == 1, "cierre parcial NO anula el conteo (INV-F2-03)"

    # El cierre completo (remaining = 0) sí decrementa.
    oms.submit(_signal("sell", price=110.0, quantity=0.70))
    assert risk.open_positions == 0
    assert risk.quantity("BTC/USDT") is None
    assert portfolio.snapshot().is_flat


# ── TEST 4 — reduce quantity ≤ position quantity (INV-F2-05) ─────────────────


def test_reduce_quantity_never_exceeds_position() -> None:
    """TEST 4: un SELL/reduce nunca pide más de la posición (clamp INV-08)."""
    oms, portfolio, risk = _chain(_ScriptedExecutor({"buy": [(100.0, 1.0, 0.0)], "sell": [(110.0, 1.0, 0.0)]}))

    oms.submit(_signal("buy", price=100.0))
    sell = oms.submit(_signal("sell", price=110.0, quantity=5.0))

    assert sell is not None
    assert sell.quantity <= 1.0, "INV-F2-05: reduce ≤ Position.quantity"
    assert sell.quantity == pytest.approx(1.0), "clamp a lo disponible"


# ── TEST 5 — señal size_pct ≠ exposición real ────────────────────────────────


def test_exposure_is_not_signal_size_pct() -> None:
    """TEST 5: Risk no usa size_pct (capital × size_pct) para representar la
    exposición de una posición ya existente (INV-F2-04)."""
    oms, portfolio, risk = _chain(_ScriptedExecutor({"buy": [(49_800.0, 0.37, 0.0)]}))

    buy = oms.submit(_signal("buy", price=50_000.0, quantity=1.0))
    assert buy is not None
    assert buy.size_pct == pytest.approx(0.05)  # sizing de RiskDecision (entrada)
    assert buy.filled_qty == pytest.approx(0.37)  # ejecutado real

    # Exposición real ≠ notional por asignación (size_pct × capital).
    assert risk.exposure_usd == pytest.approx(0.37 * 49_800.0)
    assert risk.exposure_usd != pytest.approx(10_000.0 * buy.size_pct)


# ── TEST 6 — min/max_order_usd: entradas vs reducciones ──────────────────────


def test_min_order_usd_blocks_small_entries_but_not_reductions() -> None:
    """TEST 6: min_order_usd gobierna ENTRADAS; una reducción no se rechaza
    porque su notional no coincida con el sizing de una nueva posición."""
    cfg = RiskConfig(order=OrderLimits(min_order_usd=600.0, max_order_usd=1000.0))
    risk = RiskManager(config=cfg, capital_usd=10_000.0)
    oms = OMS(
        risk_manager=risk,
        executor=_ScriptedExecutor({"buy": [], "sell": [(110.0, 0.01, 0.0)]}),
    )

    # (a) ENTRADA: BUY con capital×size_pct = $500 < min $600 → rechazado.
    buy = oms.submit(_signal("buy", price=100.0))
    assert buy is None

    # (b) REDUCCIÓN: posición existente (espejo local, p. ej. sesión previa).
    #     Un SELL pequeño ($1.1 de notional) NO se rechaza por min_order_usd
    #     de entrada — su tamaño lo gobierna la posición, no capital×size_pct.
    oms._entry_positions["BTC/USDT"] = (0.37, 100.0)
    sell = oms.submit(_signal("sell", price=110.0, quantity=0.01))
    assert sell is not None, "una reducción no se bloquea por min_order_usd de ENTRADA"
    assert sell.quantity == pytest.approx(0.01)


def test_max_order_usd_clamps_entry_sizing() -> None:
    """TEST 6: comportamiento actual de max_order_usd sobre ENTRADAS — el
    size_pct de asignación se clampa a max_order_usd / capital."""
    cfg = RiskConfig(order=OrderLimits(min_order_usd=10.0, max_order_usd=400.0))
    risk = RiskManager(config=cfg, capital_usd=10_000.0)

    decision = risk.validate(_signal("buy", price=100.0))
    assert decision.approved
    assert decision.size_pct == pytest.approx(400.0 / 10_000.0), "clamp a max_order_usd"


def test_validate_skips_max_open_positions_for_reduction() -> None:
    """TEST 6/7: con el límite de posiciones alcanzado, un BUY se rechaza pero
    un SELL (reducción) pasa — no se bloquea un cierre por regla de entrada."""
    cfg = RiskConfig(position=PositionConfig(max_position_pct=0.05, max_open_positions=1))
    risk = RiskManager(config=cfg, capital_usd=10_000.0)
    risk.record_open()  # 1 posición HELD → límite alcanzado

    assert risk.validate(_signal("buy", price=100.0)).rejected, "apertura bloqueada"
    assert risk.validate(_signal("sell", price=100.0, quantity=0.37)).approved, (
        "reducción no bloqueada por max_open_positions"
    )


# ── TEST 7 — stop-loss: reducción no tratada como apertura ───────────────────


def test_stop_loss_reduction_not_treated_as_opening() -> None:
    """TEST 7: con max_open_positions=1 y 1 posición contada, la reducción de
    stop-loss NO se rechaza como si fuera una apertura."""
    stop = StopLossConfig(enabled=True, default_pct=0.02)  # stop en 49_000
    portfolio = _PositionPortfolio(
        [
            PositionSnapshot(
                symbol="BTC/USDT",
                exchange="bybit",
                side="long",
                quantity=1.0,
                avg_entry=50_000.0,
                size_pct=0.05,
                entry_at=_NOW,
                order_id="pos-1",
            )
        ]
    )
    risk = RiskManager(
        config=RiskConfig(
            position=PositionConfig(max_position_pct=0.05, max_open_positions=1),
            stop_loss=stop,
        )
    )
    risk.record_open()  # la posición HELD ya está contada → límite alcanzado
    oms = OMS(
        risk_manager=risk,
        executor=_ScriptedExecutor({"sell": [(48_000.0, 1.0, 0.0)]}),
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

    assert result.stop_loss_closes == 1, "el stop-loss NO se bloquea como apertura"
    assert len(result.orders) == 1
    assert result.orders[0].side.value == "sell"
    assert result.orders[0].status.value == "filled"


# ── TEST 8 — UNKNOWN ≠ ZERO ──────────────────────────────────────────────────


def test_unknown_price_position_not_exposure_zero() -> None:
    """TEST 8: precio UNKNOWN con cantidad real → la posición se registra con
    precio UNKNOWN (INV-F2-06), no como exposición 0 ni posición inexistente."""
    oms, portfolio, risk = _chain(_ScriptedExecutor({"buy": [(None, 0.5, 0.0)]}))

    buy = oms.submit(_signal("buy", price=50_000.0))
    assert buy is not None
    assert buy.fill_price is None
    assert buy.filled_qty == pytest.approx(0.5)

    # La cantidad real está presente; el precio es UNKNOWN (no 0).
    assert risk.quantity("BTC/USDT") == pytest.approx(0.5)
    assert risk.state()["positions_unknown_price"] == 1
    assert "BTC/USDT" in risk.state()["quantity_by_symbol"]
    # Sin precio inventado → sin exposición fabricada para esa cantidad.
    assert risk.exposure_usd == 0.0
    assert risk.open_positions == 1  # la posición está abierta (held)


def test_unknown_quantity_not_invented() -> None:
    """TEST 8: cantidad UNKNOWN → no se inventa cantidad ni exposición."""
    oms, portfolio, risk = _chain(_ScriptedExecutor({"buy": [(100.0, None, 0.0)]}))

    buy = oms.submit(_signal("buy", price=100.0))
    assert buy is not None and buy.filled_qty is None

    assert risk.quantity("BTC/USDT") is None, "no se inventa cantidad"
    assert risk.state()["quantity_by_symbol"] == {}
    assert risk.state()["positions_unknown_price"] == 0
    assert risk.exposure_usd == 0.0


# ── INV-F2-01 directo ────────────────────────────────────────────────────────


def test_record_position_reflects_real_executed_state() -> None:
    """INV-F2-01 directo: risk.quantity es exactamente la cantidad ejecutada."""
    risk = RiskManager(config=RiskConfig(), capital_usd=10_000.0)
    risk.record_open()
    risk.record_position("BTC/USDT", 0.37, 49_800.0)

    assert risk.quantity("BTC/USDT") == pytest.approx(0.37)
    assert risk.exposure_usd == pytest.approx(0.37 * 49_800.0)

    # Cierre total: la cantidad desaparece y el conteo decrementa.
    risk.record_position("BTC/USDT", None, None)
    risk.record_close(pnl_usd=0.0, close_position=True)
    assert risk.quantity("BTC/USDT") is None
    assert risk.open_positions == 0
