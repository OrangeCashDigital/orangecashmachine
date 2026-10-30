# -*- coding: utf-8 -*-
"""Servicios de sincronización de fills para el bounded context de trading."""

from __future__ import annotations

from typing import Any, Callable, Optional

from trading.ports.execution.fill_sync import SupportsOnFill, SupportsPositionSync
from trading.domain.models.order import Order
from trading.execution.order import OrderSide


def build_fill_sync(
    tracker: SupportsOnFill,
    portfolio: SupportsPositionSync,
) -> Callable[[Order], None]:
    """
    Construye el callback on_fill compartido entre live y paper.

    SSOT del tracking buy->sell: el dict _open_order_ids es la unica
    fuente de verdad del mapeo symbol -> buy_order_id en todo el sistema.
    portfolio.close_position requiere el order_id del BUY (key de
    apertura), no el del SELL -- sin este mapeo las posiciones nunca
    cierran (bug silencioso, ya documentado en el codigo original).

    Parameters
    ----------
    tracker   : recibe todos los fills, independiente del estado de portfolio.
    portfolio : sincroniza apertura/cierre de posiciones.

    Returns
    -------
    Callable que se pasa como on_fill= al OMS en
    TradingCompositionRoot.assemble_live()/assemble_paper().
    """
    open_order_ids: dict[str, str] = {}

    def on_fill_composite(order: Order) -> None:
        """Callback OMS -> TradeTracker + PortfolioService.

        ADR-0025 (F4a/F4b): la posición se asienta con la cantidad y el
        precio económicamente ejecutados del fill real (order.filled_qty y
        order.fill_price), nunca signal.price. La clave de la posición es la
        pierna de apertura: `open_order_ids[symbol]` se fija con la PRIMERA
        BUY (setdefault) y se conserva mientras la posición esté abierta;
        las BUY posteriores se fusionan en la misma posición (WAC). El
        mapeo se elimina solo cuando el cierre deja la posición a 0.

        1. TradeTracker -- siempre primero (analytics independiente de portfolio).
        2. Portfolio    -- sincroniza estado de posicion abierta/cerrada.
        """
        tracker.on_fill(order)

        if order.side == OrderSide.BUY:
            open_order_ids.setdefault(order.symbol, order.order_id)
            assert order.fill_price is not None, "open_position: BUY fill sin fill_price"
            portfolio.open_position(
                order_id=order.order_id,
                symbol=order.symbol,
                side="long",
                avg_entry=order.fill_price,
                size_pct=order.size_pct,
                entry_at=order.fill_timestamp,
                quantity=order.filled_qty,
            )
        elif order.side == OrderSide.SELL:
            buy_order_id = open_order_ids.get(order.symbol)
            if buy_order_id is not None:
                closed, remaining = portfolio.close_position(
                    buy_order_id,
                    quantity=order.filled_qty,
                )
                # B-15 (H-09): cierre no confirmado -> riesgo de posición
                # fantasma. PortfolioService devuelve None tanto si la
                # posición no existía como si falló la persistencia (SafeOps);
                # el contrato actual no permite distinguirlo. En el SELL path
                # ambos implican divergencia entre el tracking local y el
                # estado persistido -> alerta de operación, sin lanzar.
                if closed is None:
                    logger.critical(
                        "POSITION_CLOSE_UNCONFIRMED | SELL fill pero cierre de "
                        "PortfolioService no confirmado | symbol={} sell_order_id={} "
                        "buy_order_id={} — posible divergencia de estado "
                        "(posición fantasma): TradeTracker la considera cerrada "
                        "mientras PortfolioService/PositionStore puede retenerla",
                        order.symbol,
                        order.order_id,
                        buy_order_id,
                    )
                elif remaining <= 0.0:
                    open_order_ids.pop(order.symbol, None)
            else:
                logger.warning(
                    "on_fill_composite | SELL sin BUY previo registrado | symbol={} sell_order_id={}",
                    order.symbol,
                    order.order_id,
                )

    return on_fill_composite
