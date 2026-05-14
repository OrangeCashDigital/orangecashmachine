# -*- coding: utf-8 -*-
"""
trading/analytics/trade_record.py
===================================

TradeRecord — unidad atómica de historial de trading.

Un trade = par completo (entry BUY + exit SELL).
Inmutable una vez cerrado — frozen dataclass.

Principios: SOLID · KISS · SafeOps
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


@dataclass(frozen=True)
class TradeRecord:
    """
    Registro completo de un trade cerrado (entry + exit).

    Campos de identidad
    -------------------
    trade_id    : correlación con Order.order_id de la entrada
    symbol      : par (e.g. "BTC/USDT")
    exchange    : origen del dato
    timeframe   : timeframe de la señal

    Campos de ejecución
    -------------------
    entry_price : fill_price de la orden BUY
    exit_price  : fill_price de la orden SELL
    size_pct    : % del capital asignado
    entry_at    : timestamp de apertura
    exit_at     : timestamp de cierre

    Métricas derivadas (calculadas en __post_init__)
    -------------------------------------------------
    pnl_pct     : (exit - entry) / entry
    duration_s  : segundos entre entry y exit
    """
    trade_id:    str
    symbol:      str
    exchange:    str
    timeframe:   str
    entry_price: float
    exit_price:  float
    size_pct:    float
    entry_at:    datetime
    exit_at:     datetime

    # Derivadas — calculadas externamente y pasadas al construir
    pnl_pct:    float
    duration_s: float

    @classmethod
    def close(
        cls,
        trade_id:    str,
        symbol:      str,
        exchange:    str,
        timeframe:   str,
        entry_price: float,
        exit_price:  float,
        size_pct:    float,
        entry_at:    datetime,
        exit_at:     Optional[datetime] = None,
    ) -> "TradeRecord":
        """
        Factory — crea un TradeRecord calculando métricas automáticamente.

        Usar este método en lugar del constructor directo.
        """
        closed_at  = exit_at or datetime.now(timezone.utc)
        pnl_pct    = (exit_price - entry_price) / entry_price if entry_price > 0 else 0.0
        duration_s = (closed_at - entry_at).total_seconds()

        return cls(
            trade_id    = trade_id,
            symbol      = symbol,
            exchange    = exchange,
            timeframe   = timeframe,
            entry_price = entry_price,
            exit_price  = exit_price,
            size_pct    = size_pct,
            entry_at    = entry_at,
            exit_at     = closed_at,
            pnl_pct     = pnl_pct,
            duration_s  = duration_s,
        )

    @property
    def is_winner(self) -> bool:
        return self.pnl_pct > 0.0

    @property
    def pnl_usd(self) -> Optional[float]:
        """
        P&L en USD si se conoce el capital total.

        Retorna None — el TradeRecord no conoce el capital absoluto.
        Usar PerformanceEngine.pnl_usd(trade, capital_usd) para el cálculo.
        """
        return None

    def __str__(self) -> str:
        direction = "WIN" if self.is_winner else "LOSS"
        return (
            f"Trade({self.trade_id} {self.symbol} {direction}"
            f" pnl={self.pnl_pct:+.2%} entry={self.entry_price:.2f}"
            f" exit={self.exit_price:.2f} dur={self.duration_s:.0f}s)"
        )
