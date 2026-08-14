# -*- coding: utf-8 -*-
"""
tests/trading/test_oms_fill_lifecycle.py
==========================================

Guard R3 / B-03 (H-03): semántica HELD-POSITION de `risk._open_positions`.

- BUY opens a held position (mantiene abierta hasta que se vende).
- SELL closes it (record_close en el flujo de fill).

Sin el `record_close` en el SELL fill, el contador fuga al primer BUY→SELL y
`max_open_positions` se agota artificialmente.

El ejecutor fake acepta siempre → submit() llena la orden de forma síncrona.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from trading.execution.oms import OMS
from trading.execution.order import Order, OrderSide
from trading.execution.transport import OrderResult, OrderState, OrderStatus
from trading.risk.manager import RiskManager
from trading.risk.models import RiskConfig
from trading.strategies.base import Signal


class _AcceptingExecutor:
    """OrderExecutor que acepta (fill) toda orden — sin I/O.

    S1: execute() retorna OrderResult (contrato nuevo) — fill al precio de señal.
    F1: reporta filled_qty (1.0) para que el BUY acumule entrada WAC y el SELL
    pueda dimensionarse contra la posición económica.
    """

    def execute(self, order: Order) -> OrderResult:
        return OrderResult(
            accepted=True,
            state=OrderState(
                order_id=order.order_id,
                status=OrderStatus.FILLED,
                fill_price=order.signal.price,
                filled_qty=1.0,
                fees=0.0,
            ),
        )


def _risk_cfg() -> RiskConfig:
    return RiskConfig()


def _sg(side: OrderSide) -> Signal:
    return Signal(
        symbol="BTC/USDT",
        timeframe="1m",
        direction=side.value,
        price=50_000.0,
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        confidence=1.0,
    )


def _new_oms() -> tuple[RiskManager, OMS]:
    risk = RiskManager(config=_risk_cfg())
    oms = OMS(risk_manager=risk, executor=_AcceptingExecutor())
    return risk, oms


def _submit(oms: OMS, side: OrderSide) -> Optional[Order]:
    return oms.submit(_sg(side))


def test_buy_fill_keeps_position_open() -> None:
    """BUY abre una posición que se mantiene (held) hasta cerrarla."""
    risk, oms = _new_oms()
    _submit(oms, OrderSide.BUY)
    assert risk.open_positions == 1, "BUY fill debe dejar la posición abierta (held)"


def test_sell_fill_closes_opened_position() -> None:
    """B-03/R3: BUY→SELL round-trip devuelve `_open_positions` a 0 (sin fuga)."""
    risk, oms = _new_oms()
    _submit(oms, OrderSide.BUY)
    _submit(oms, OrderSide.SELL)
    assert risk.open_positions == 0, (
        f"SELL fill debe record_close → `_open_positions` vuelve a 0. actual={risk.open_positions}"
    )


def test_lone_sell_does_not_open_a_position() -> None:
    """SELL sin BUY previo no debe crear ninguna posición HELD.

    F1: un SELL sin posición económica disponible se RECHAZA en submit
    (fail-closed, return None) — nunca se vende de una posición desconocida.
    """
    risk, oms = _new_oms()
    order = _submit(oms, OrderSide.SELL)
    assert order is None, "SELL sin posición debe rechazarse en submit (F1)"
    assert risk.open_positions == 0, "SELL no abre posición (ni cierra una inexistente)"
