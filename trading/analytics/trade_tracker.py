# -*- coding: utf-8 -*-
"""
trading/analytics/trade_tracker.py
=====================================

TradeTracker — empareja fills del OMS en TradeRecords completos.

Responsabilidad única (SRP)
---------------------------
Recibe fills (Order) del OMS via callback on_fill.
Empareja BUY + SELL del mismo símbolo → TradeRecord cerrado.
No calcula métricas — eso es PerformanceEngine.

Integración con OMS
-------------------
    tracker = TradeTracker(exchange="bybit")
    oms = OMS(
        risk_manager = ...,
        executor     = ...,
        on_fill      = tracker.on_fill,   # ← callback
    )

SafeOps
-------
- Thread-safe: _open_positions y _closed bajo _lock.
- on_fill nunca lanza — errores son logueados y descartados.
- Posiciones BUY sin SELL correspondiente quedan en _open_positions
  accesibles via open_positions property.

Principios: SOLID · KISS · DRY · SafeOps
"""
from __future__ import annotations

import threading
from typing import Optional

from loguru import logger

from trading.analytics.trade_record import TradeRecord
from trading.execution.order import Order, OrderSide


class TradeTracker:
    """
    Empareja fills BUY + SELL en TradeRecords cerrados.

    Parameters
    ----------
    exchange : str — exchange de origen (para enriquecer TradeRecord).
    """

    def __init__(self, exchange: str = "bybit") -> None:
        self._exchange = exchange
        self._lock     = threading.Lock()

        # symbol → Order de apertura (BUY fill pendiente de cierre)
        self._open_positions: dict[str, Order] = {}

        # Historial de trades cerrados — append-only
        self._closed: list[TradeRecord] = []

        self._log = logger.bind(component="TradeTracker")

    # ------------------------------------------------------------------
    # OMS callback
    # ------------------------------------------------------------------

    def on_fill(self, order: Order) -> None:
        """
        Callback para OMS.on_fill.

        SafeOps: nunca lanza — cualquier error se loguea y descarta.
        """
        try:
            self._process_fill(order)
        except Exception as exc:
            self._log.error(
                "on_fill error | order={} error={}",
                order.order_id, exc,
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def closed_trades(self) -> list[TradeRecord]:
        """Copia del historial de trades cerrados."""
        with self._lock:
            return list(self._closed)

    @property
    def open_positions(self) -> dict[str, Order]:
        """
        Posiciones BUY abiertas sin SELL correspondiente.

        Clave: symbol. Valor: Order de apertura.
        """
        with self._lock:
            return dict(self._open_positions)

    @property
    def trade_count(self) -> int:
        with self._lock:
            return len(self._closed)

    def last_trade(self) -> Optional[TradeRecord]:
        with self._lock:
            return self._closed[-1] if self._closed else None

    def summary(self) -> dict:
        """Estado observable. SafeOps: nunca lanza."""
        try:
            with self._lock:
                return {
                    "closed_trades":   len(self._closed),
                    "open_positions":  len(self._open_positions),
                    "open_symbols":    list(self._open_positions.keys()),
                }
        except Exception:
            return {"closed_trades": 0, "open_positions": 0, "open_symbols": []}

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def _process_fill(self, order: Order) -> None:
        if order.side == OrderSide.BUY:
            self._register_open(order)
        elif order.side == OrderSide.SELL:
            self._register_close(order)

    def _register_open(self, order: Order) -> None:
        """Registra apertura de posición (BUY fill)."""
        with self._lock:
            if order.symbol in self._open_positions:
                # Posición ya abierta — loguear y reemplazar
                # (estrategia pyramid no soportada aún)
                self._log.warning(
                    "Posición ya abierta para {} — reemplazando | old={} new={}",
                    order.symbol,
                    self._open_positions[order.symbol].order_id,
                    order.order_id,
                )
            self._open_positions[order.symbol] = order
            self._log.debug(
                "Posición abierta | {} {} @ {:.4f} size={:.1%}",
                order.symbol, order.order_id,
                order.fill_price, order.size_pct,
            )

    def _register_close(self, order: Order) -> None:
        """Empareja SELL con BUY abierto → TradeRecord."""
        with self._lock:
            entry_order = self._open_positions.pop(order.symbol, None)

        if entry_order is None:
            self._log.warning(
                "SELL sin BUY correspondiente | {} {} — ignorado",
                order.symbol, order.order_id,
            )
            return

        trade = TradeRecord.close(
            trade_id    = entry_order.order_id,
            symbol      = order.symbol,
            exchange    = self._exchange,
            timeframe   = entry_order.signal.timeframe,
            entry_price = entry_order.fill_price,
            exit_price  = order.fill_price,
            size_pct    = entry_order.size_pct,
            entry_at    = entry_order.fill_timestamp,
            exit_at     = order.fill_timestamp,
        )

        with self._lock:
            self._closed.append(trade)

        self._log.info(
            "Trade cerrado | {} pnl={:+.2%} entry={:.4f} exit={:.4f} dur={:.0f}s",
            trade.trade_id, trade.pnl_pct,
            trade.entry_price, trade.exit_price, trade.duration_s,
        )
