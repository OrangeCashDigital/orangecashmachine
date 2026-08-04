# -*- coding: utf-8 -*-
"""
trading/execution/fill_sync.py
================================

Sincronización OMS -> TradeTracker + PortfolioService.

Responsabilidad
---------------
Bridge de dominio entre ejecucion de ordenes (OMS) y las dos vistas
consumidoras de un fill: analytics (TradeTracker) y estado de portfolio
(PortfolioService). Vive en trading/ porque es logica de dominio del
bounded context de ejecucion, no boilerplate de bootstrap de un CLI.

SSOT: unica implementacion de on_fill_composite -- antes duplicada
linea por linea entre execute_live.py y execute_paper.py.
Ver docs/audits/2026-08-composition-root-audit.md, hallazgo 2.2.

Principios: SRP . DRY . SSOT . DIP
"""

from __future__ import annotations

from typing import Any, Callable, Protocol

from loguru import logger

from trading.execution.order import OrderSide


class SupportsOnFill(Protocol):
    def on_fill(self, order) -> None: ...


class SupportsPositionSync(Protocol):
    def open_position(
        self,
        *,
        order_id: str,
        symbol: str,
        side: str,
        entry_price: float,
        size_pct: float,
        entry_at,
    ) -> None: ...

    def close_position(self, order_id: str) -> Any:
        """build_fill_sync descarta el retorno; Any admite PortfolioService
        (devuelve Optional[PositionSnapshot]) sin acoplar trading->portfolio."""


def build_fill_sync(
    tracker: SupportsOnFill,
    portfolio: SupportsPositionSync,
) -> Callable[[object], None]:
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

    def on_fill_composite(order) -> None:
        """Callback OMS -> TradeTracker + PortfolioService.

        1. TradeTracker -- siempre primero (analytics independiente de portfolio).
        2. Portfolio    -- sincroniza estado de posicion abierta/cerrada.
        """
        tracker.on_fill(order)

        if order.side == OrderSide.BUY:
            open_order_ids[order.symbol] = order.order_id
            portfolio.open_position(
                order_id=order.order_id,
                symbol=order.symbol,
                side="long",
                entry_price=order.fill_price,
                size_pct=order.size_pct,
                entry_at=order.fill_timestamp,
            )
        elif order.side == OrderSide.SELL:
            buy_order_id = open_order_ids.pop(order.symbol, None)
            if buy_order_id is not None:
                portfolio.close_position(buy_order_id)
            else:
                logger.warning(
                    "on_fill_composite | SELL sin BUY previo registrado | symbol={} sell_order_id={}",
                    order.symbol,
                    order.order_id,
                )

    return on_fill_composite
