# -*- coding: utf-8 -*-
"""
trading/strategies/ema_crossover.py
=====================================

Estrategia EMA Crossover.

Lógica
------
- BUY  cuando EMA rápida cruza por ENCIMA de EMA lenta (golden cross)
- SELL cuando EMA rápida cruza por DEBAJO de EMA lenta (death cross)
- HOLD en cualquier otro caso

Parámetros por defecto: EMA(9) / EMA(21) — conservadores para crypto 1h.
"""

from __future__ import annotations

from datetime import timezone
from typing import Literal

import polars as pl

from trading.strategies.base import BaseStrategy, Signal


class EMACrossoverStrategy(BaseStrategy):
    """
    Estrategia basada en cruce de medias exponenciales.

    Parameters
    ----------
    fast_period : int
        Período de la EMA rápida (default: 9).
    slow_period : int
        Período de la EMA lenta (default: 21).
    symbol : str
        Par de trading (ej: "BTC/USDT").
    timeframe : str
        Timeframe del DataFrame (ej: "1h").
    """

    name = "ema_crossover"

    def __init__(
        self,
        fast_period: int = 9,
        slow_period: int = 21,
        symbol: str = "BTC/USDT",
        timeframe: str = "1h",
    ) -> None:
        if fast_period >= slow_period:
            raise ValueError(f"fast_period ({fast_period}) must be < slow_period ({slow_period})")
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.symbol = symbol
        self.timeframe = timeframe

    def generate_signals(self, df: pl.DataFrame) -> list[Signal]:
        self.validate_df(df)

        if len(df) < self.slow_period + 1:
            return []  # no hay suficientes datos para calcular EMA

        df = df.sort("timestamp").with_row_index("_idx")
        df = df.with_columns(
            pl.col("close").ewm_mean(span=self.fast_period, adjust=False).alias("ema_fast"),
            pl.col("close").ewm_mean(span=self.slow_period, adjust=False).alias("ema_slow"),
        )

        # Cruce: fast cruza sobre o bajo slow entre t-1 y t
        df = df.with_columns(
            (pl.col("ema_fast") > pl.col("ema_slow")).alias("above"),
        ).with_columns((pl.col("above") != pl.col("above").shift(1)).alias("cross"))

        last = df.filter(pl.col("_idx") == len(df) - 1).row(0, named=True)

        if not last["cross"]:
            return []

        signal_type: Literal["buy", "sell"] = "buy" if last["above"] else "sell"
        ts = last["timestamp"]
        if hasattr(ts, "to_pydatetime"):
            ts = ts.to_pydatetime()
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        return [
            Signal(
                symbol=self.symbol,
                timeframe=self.timeframe,
                direction=signal_type,
                price=float(last["close"]),
                timestamp=ts,
                confidence=1.0,
                metadata={
                    "ema_fast": round(float(last["ema_fast"]), 4),
                    "ema_slow": round(float(last["ema_slow"]), 4),
                    "fast_period": self.fast_period,
                    "slow_period": self.slow_period,
                },
            )
        ]
