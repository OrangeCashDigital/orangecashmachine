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
    tracker = TradeTracker(exchange="bybit", portfolio=portfolio)
    oms = OMS(
        risk_manager = ...,
        executor     = ...,
        portfolio    = portfolio,
        on_fill      = tracker.on_fill,   # ← callback
    )

SSOT (ARCH-001/010)
-------------------
- TradeTracker NO mantiene su propio estado de posiciones.
- open_positions property → vista del PortfolioService (SSOT).
- Al cerrar (SELL), consulta position.avg_entry / entry_at del portfolio.

SafeOps
-------
- Thread-safe: _closed bajo _lock.
- on_fill nunca lanza — errores son logueados y descartados.

Principios: SOLID · KISS · DRY · SafeOps
"""

from __future__ import annotations

import threading
from typing import Any, Optional

from loguru import logger

from trading.analytics.trade_record import TradeRecord
from trading.execution.order import Order, OrderSide
from trading.execution.settlement import Settlement


class TradeTracker:
    """
    Empareja fills BUY + SELL en TradeRecords cerrados.

    Parameters
    ----------
    exchange : str — exchange de origen (para enriquecer TradeRecord).
    """

    def __init__(self, exchange: str = "bybit", portfolio: Any = None) -> None:
        self._exchange = exchange
        self._portfolio = portfolio
        self._lock = threading.Lock()

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
            # SafeOps: NO acceder a order.order_id aquí — el objeto
            # puede estar corrupto y causar un segundo error en el handler.
            self._log.error("on_fill error | error={}", exc)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def closed_trades(self) -> list[TradeRecord]:
        """Copia del historial de trades cerrados."""
        with self._lock:
            return list(self._closed)

    @property
    def open_positions(self) -> dict[str, Any]:
        """
        Posiciones abiertas — vista del PortfolioService (SSOT).

        Clave: symbol. Valor: PositionSnapshot.
        """
        if self._portfolio is None:
            return {}
        return {p.symbol: p for p in self._portfolio.snapshot().positions}

    @property
    def trade_count(self) -> int:
        with self._lock:
            return len(self._closed)

    def last_trade(self) -> Optional[TradeRecord]:
        with self._lock:
            return self._closed[-1] if self._closed else None

    def summary(self) -> dict[str, Any]:
        """Estado observable. SafeOps: nunca lanza."""
        try:
            with self._lock:
                closed_n = len(self._closed)
            open_pos = self.open_positions
            return {
                "closed_trades": closed_n,
                "open_positions": len(open_pos),
                "open_symbols": list(open_pos.keys()),
            }
        except Exception:
            return {"closed_trades": 0, "open_positions": 0, "open_symbols": []}

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def _process_fill(self, order: Order) -> None:
        if order.side == OrderSide.BUY:
            # Portfolio.open_position (via fill_sync) es el owner; aquí no se
            # mantiene espejo (ARCH-001/010).
            return
        elif order.side == OrderSide.SELL:
            self._register_close(order)

    def _register_close(self, order: Order) -> None:
        """Empareja SELL con BUY abierto (vía portfolio, SSOT) → TradeRecord."""
        position = None
        if self._portfolio is not None:
            positions = self._portfolio.snapshot().positions
            position = next((p for p in positions if p.symbol == order.symbol), None)

        if position is None:
            self._log.warning(
                "SELL sin BUY correspondiente | {} {} — ignorado",
                order.symbol,
                order.order_id,
            )
            return

        assert position.avg_entry is not None and position.avg_entry > 0,             "TradeRecord.close: position.avg_entry inválido"
        assert order.fill_price is not None, "TradeRecord.close: exit order sin fill_price"
        assert position.entry_at is not None, "TradeRecord.close: position.entry_at ausente"

        # Costes (S1): usar fees del settlement si están disponibles.
        # Si fee_status es UNKNOWN, NO asumir zero (ADR-0026).
        settlement: Optional[Settlement] = getattr(order, "settlement", None)

        if settlement is not None:
            # Camino canónico (F3): el TradeRecord consume el settlement.
            # Toda la economía (closed_qty, WAC, exit, gross, net, fee_status)
            # proviene del settlement; aquí no se recalcula P&L.
            trade = TradeRecord.from_settlement(
                settlement,
                trade_id=position.order_id,
                exchange=self._exchange,
                timeframe=order.signal.timeframe,  # contexto del cierre
                size_pct=position.size_pct,
                entry_at=position.entry_at,
                exit_at=order.fill_timestamp,
            )
        else:
            # Modo defensivo legacy (sin settlement — no debería pasar en un
            # flujo F3 correcto, pero protegemos contra casos límite).
            self._log.warning(
                "TradeTracker: settlement None — cayendo en cálculo legacy "
                "(F3: settlement debería estar siempre presente)"
            )
            closed_qty = order.filled_qty or position.quantity or 1.0
            avg_entry_price = position.avg_entry
            exit_price = order.fill_price
            fees = (order.fees or 0.0)  # solo fees del exit (entry fees desconocidas aquí)
            entry_notional = position.avg_entry * position.quantity if position.quantity else None

            trade = TradeRecord.close(
                trade_id=position.order_id,
                symbol=order.symbol,
                exchange=self._exchange,
                timeframe=order.signal.timeframe,
                entry_price=avg_entry_price,
                exit_price=exit_price,
                size_pct=position.size_pct,
                entry_at=position.entry_at,
                exit_at=order.fill_timestamp,
                fees=fees,
                entry_notional=entry_notional,
                closed_qty=closed_qty,
            )

        with self._lock:
            self._closed.append(trade)

        self._log.info(
            "Trade cerrado | {} pnl={:+.2%} net={:+.2%} entry={:.4f} exit={:.4f} dur={:.0f}s",
            trade.trade_id,
            trade.pnl_pct,
            trade.net_pnl_pct,
            trade.entry_price,
            trade.exit_price,
            trade.duration_s,
        )
