# -*- coding: utf-8 -*-
"""
trading/analytics/performance.py
===================================

PerformanceEngine — métricas de rendimiento de trading.

Diseño
------
Funciones puras + PerformanceSummary inmutable.
Sin estado, sin I/O, sin dependencias externas más allá de math.

Métricas implementadas
-----------------------
  total_pnl_pct   : suma de pnl_pct de todos los trades
  pnl_usd         : total_pnl_pct * capital_usd
  win_rate        : trades ganadores / total trades
  avg_win_pct     : promedio de pnl_pct en trades ganadores
  avg_loss_pct    : promedio de pnl_pct en trades perdedores
  profit_factor   : gross_profit / gross_loss
  sharpe_ratio    : media(returns) / std(returns) * sqrt(periods/año)
  max_drawdown    : máxima caída desde pico en equity curve
  avg_duration_s  : duración media de trades en segundos

Principios: SOLID · KISS · DRY · SafeOps
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

from trading.analytics.trade_record import TradeRecord


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PerformanceSummary:
    """
    Resumen de rendimiento de una sesión o período.

    Todos los campos son Optional para soportar historial vacío
    (0 trades cerrados) sin lanzar excepciones.
    """
    total_trades:   int
    winning_trades: int
    losing_trades:  int

    total_pnl_pct:  float          # suma acumulada
    pnl_usd:        Optional[float]  # None si capital_usd no se provee

    win_rate:       Optional[float]  # None si total_trades == 0
    avg_win_pct:    Optional[float]
    avg_loss_pct:   Optional[float]
    profit_factor:  Optional[float]  # None si no hay pérdidas

    sharpe_ratio:   Optional[float]  # None si < 2 trades
    max_drawdown:   float            # siempre >= 0

    avg_duration_s: Optional[float]

    def __str__(self) -> str:
        wr  = f"{self.win_rate:.1%}"   if self.win_rate   is not None else "N/A"
        sr  = f"{self.sharpe_ratio:.2f}" if self.sharpe_ratio is not None else "N/A"
        mdd = f"{self.max_drawdown:.2%}"
        pnl = f"{self.total_pnl_pct:+.2%}"
        return (
            f"Performance(trades={self.total_trades}"
            f" pnl={pnl} win_rate={wr}"
            f" sharpe={sr} max_dd={mdd})"
        )


# ---------------------------------------------------------------------------
# PerformanceEngine
# ---------------------------------------------------------------------------

class PerformanceEngine:
    """
    Calcula métricas de rendimiento a partir de una lista de TradeRecords.

    Todas las funciones son estáticas — sin estado interno.
    Uso:
        summary = PerformanceEngine.summarize(trades, capital_usd=10_000)
        sr      = PerformanceEngine.sharpe_ratio(trades)
        mdd     = PerformanceEngine.max_drawdown(trades)
    """

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    @staticmethod
    def summarize(
        trades:      list[TradeRecord],
        capital_usd: Optional[float] = None,
        periods_per_year: int = 252,    # días de trading por año
    ) -> PerformanceSummary:
        """
        Calcula todas las métricas de una vez.

        SafeOps: nunca lanza — historial vacío retorna summary con ceros.
        """
        if not trades:
            return PerformanceSummary(
                total_trades   = 0,
                winning_trades = 0,
                losing_trades  = 0,
                total_pnl_pct  = 0.0,
                pnl_usd        = 0.0 if capital_usd is not None else None,
                win_rate       = None,
                avg_win_pct    = None,
                avg_loss_pct   = None,
                profit_factor  = None,
                sharpe_ratio   = None,
                max_drawdown   = 0.0,
                avg_duration_s = None,
            )

        winners = [t for t in trades if t.is_winner]
        losers  = [t for t in trades if not t.is_winner]
        returns = [t.pnl_pct for t in trades]

        total_pnl  = sum(returns)
        win_rate   = len(winners) / len(trades)
        avg_win    = sum(t.pnl_pct for t in winners) / len(winners) if winners else None
        avg_loss   = sum(t.pnl_pct for t in losers)  / len(losers)  if losers  else None

        gross_profit = sum(t.pnl_pct for t in winners) if winners else 0.0
        gross_loss   = abs(sum(t.pnl_pct for t in losers)) if losers else 0.0
        profit_factor = (
            gross_profit / gross_loss if gross_loss > 0 else None
        )

        return PerformanceSummary(
            total_trades   = len(trades),
            winning_trades = len(winners),
            losing_trades  = len(losers),
            total_pnl_pct  = total_pnl,
            pnl_usd        = total_pnl * capital_usd if capital_usd is not None else None,
            win_rate       = win_rate,
            avg_win_pct    = avg_win,
            avg_loss_pct   = avg_loss,
            profit_factor  = profit_factor,
            sharpe_ratio   = PerformanceEngine.sharpe_ratio(
                trades, periods_per_year=periods_per_year
            ),
            max_drawdown   = PerformanceEngine.max_drawdown(trades),
            avg_duration_s = (
                sum(t.duration_s for t in trades) / len(trades)
            ),
        )

    # ------------------------------------------------------------------
    # Individual metrics (pure functions)
    # ------------------------------------------------------------------

    @staticmethod
    def sharpe_ratio(
        trades:           list[TradeRecord],
        risk_free:        float = 0.0,
        periods_per_year: int   = 252,
    ) -> Optional[float]:
        """
        Sharpe Ratio anualizado.

        sharpe = (mean(returns) - risk_free) / std(returns) * sqrt(N)

        Returns None si hay menos de 2 trades (std indefinida).
        """
        if len(trades) < 2:
            return None

        returns = [t.pnl_pct for t in trades]
        n       = len(returns)
        mean_r  = sum(returns) / n
        excess  = mean_r - risk_free

        variance = sum((r - mean_r) ** 2 for r in returns) / (n - 1)
        std_r    = math.sqrt(variance)

        if std_r == 0.0:
            return None

        return (excess / std_r) * math.sqrt(periods_per_year)

    @staticmethod
    def max_drawdown(trades: list[TradeRecord]) -> float:
        """
        Máxima caída desde pico en la equity curve.

        Calcula el drawdown sobre la curva de retornos acumulados.
        Retorna 0.0 si no hay trades o equity solo sube.

        Valor siempre >= 0 (magnitud, no negativo).
        """
        if not trades:
            return 0.0

        # Equity curve: capital normalizado a 1.0
        equity  = 1.0
        peak    = 1.0
        max_dd  = 0.0

        for trade in trades:
            equity *= (1.0 + trade.pnl_pct)
            if equity > peak:
                peak = equity
            drawdown = (peak - equity) / peak
            if drawdown > max_dd:
                max_dd = drawdown

        return max_dd

    @staticmethod
    def win_rate(trades: list[TradeRecord]) -> Optional[float]:
        """Win rate: fracción de trades ganadores. None si no hay trades."""
        if not trades:
            return None
        return sum(1 for t in trades if t.is_winner) / len(trades)

    @staticmethod
    def total_pnl_pct(trades: list[TradeRecord]) -> float:
        """Suma acumulada de pnl_pct. 0.0 si no hay trades."""
        return sum(t.pnl_pct for t in trades)

    @staticmethod
    def pnl_usd(trades: list[TradeRecord], capital_usd: float) -> float:
        """P&L total en USD dado un capital base."""
        return PerformanceEngine.total_pnl_pct(trades) * capital_usd

    @staticmethod
    def equity_curve(trades: list[TradeRecord]) -> list[float]:
        """
        Curva de equity normalizada a 1.0.

        Útil para visualización o cálculos posteriores.
        Retorna [1.0] si no hay trades.
        """
        curve   = [1.0]
        equity  = 1.0
        for trade in trades:
            equity *= (1.0 + trade.pnl_pct)
            curve.append(round(equity, 6))
        return curve
