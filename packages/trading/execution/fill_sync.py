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

B-15 (H-09) — observabilidad del cierre de posición
---------------------------------------------------
El SELL path captura el retorno de `portfolio.close_position(buy_order_id)`.
Si devuelve una porción cerrada `None` (cierre no confirmado por SafeOps),
se emite `logger.critical` con symbol, sell_order_id y buy_order_id para que
una posición fantasma —TradeTracker considera cerrada mientras
PortfolioService/PositionStore puede retenerla— sea visible en operación.

Limitación conocida (contrato actual, no se cambia): `close_position`
devuelve porción `None` tanto si la posición no existía como si falló la
persistencia; el log no distingue ambos casos. La alerta se emite en ambos
porque en el SELL path cualquiera de los dos implica divergencia entre
tracking y estado persistido.

ADR-0025 (F4a/F4b): `close_position` devuelve (closed, remaining). `closed`
trae la porción cerrada con su cantidad y WAC al cierre (datos para el
realized P&L); `remaining > 0` indica un cierre parcial, en cuyo caso la
posición queda abierta y el mapeo symbol→buy_order_id se conserva.

La unificación de ownership del estado de posiciones (múltiples fuentes de
verdad: TradeTracker._open_positions vs PortfolioService) queda FUERA de
esta mitigación — es una decisión arquitectónica pendiente, no resuelta aquí.

Principios: SRP . DRY . SSOT . DIP
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Protocol

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
        avg_entry: float,
        size_pct: float,
        entry_at,
        quantity: Optional[float],
    ) -> None: ...

    def close_position(
        self,
        order_id: str,
        quantity: Optional[float] = None,
    ) -> tuple[Any, float]:
        """Devuelve (closed, remaining): la porción cerrada (con cantidad y
        WAC al cierre, ADR-0025) y la cantidad restante tras el cierre
        (0.0 en cierre completo). `closed` es None si el cierre no se
        confirmó — el caller lo captura para detectar divergencias
        (B-15/H-09). Any admite PortfolioService sin acoplar
        trading->portfolio."""


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
