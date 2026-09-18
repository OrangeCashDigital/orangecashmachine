# -*- coding: utf-8 -*-
"""
trading/analytics/performance.py
===================================

PerformanceEngine — métricas de rendimiento de trading.

Diseño
------
Funciones puras + PerformanceSummary inmutable.
Sin estado, sin I/O, sin dependencias externas más allá de math.

F3 — semántica canónica de P&L (ADR-0025/0026/0027)
---------------------------------------------------
El realized P&L primario es MONETARIO y quantity-aware:

    realized_pnl_usd = closed_qty * (exit_price - WAC)

Por tanto:

  total_realized_usd : Σ de realized P&L en USD (neto si las fees son
                       conocidas, bruto si fee_status es UNKNOWN — UNKNOWN
                       nunca se asume como 0).
  total_return_pct   : total_realized_usd / capital_usd — DERIVADO de una
                       base monetaria coherente (el capital), no una suma
                       lineal de porcentajes de trades de distinto tamaño.

Las métricas por-trade (win_rate, avg_win/avg_loss, profit_factor, sharpe)
siguen siendo porcentuales por trade — son DERIVADAS y comparativas, no una
fuente económica alternativa.

La equity curve es MONETARIA: capital base + Σ acumulado de realized P&L USD.

Principios: SOLID · KISS · DRY · SafeOps · ADR-0025/0026/0027
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

    total_trades: int
    winning_trades: int
    losing_trades: int

    # P&L monetario primario (F3, ADR-0025/0026)
    total_realized_usd: Optional[float]  # Σ realized P&L USD
    total_return_pct: Optional[float]  # total_realized_usd / capital_usd

    win_rate: Optional[float]  # None si total_trades == 0
    avg_win_pct: Optional[float]
    avg_loss_pct: Optional[float]
    profit_factor: Optional[float]  # None si no hay pérdidas

    sharpe_ratio: Optional[float]  # None si < 2 trades
    max_drawdown: float  # siempre >= 0

    avg_duration_s: Optional[float]

    def __str__(self) -> str:
        wr = f"{self.win_rate:.1%}" if self.win_rate is not None else "N/A"
        sr = f"{self.sharpe_ratio:.2f}" if self.sharpe_ratio is not None else "N/A"
        mdd = f"{self.max_drawdown:.2%}"
        usd = f"{self.total_realized_usd:+.2f}" if self.total_realized_usd is not None else "N/A"
        ret = f"{self.total_return_pct:+.2%}" if self.total_return_pct is not None else "N/A"
        return (
            f"Performance(trades={self.total_trades} realized_usd={usd}"
            f" return={ret} win_rate={wr} sharpe={sr} max_dd={mdd})"
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
        mdd     = PerformanceEngine.max_drawdown(trades, capital_usd=10_000)
    """

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    @staticmethod
    def summarize(
        trades: list[TradeRecord],
        capital_usd: Optional[float] = None,
        periods_per_year: int = 252,  # días de trading por año
    ) -> PerformanceSummary:
        """
        Calcula todas las métricas de una vez.

        SafeOps: nunca lanza — historial vacío retorna summary con ceros.
        """
        if not trades:
            return PerformanceSummary(
                total_trades=0,
                winning_trades=0,
                losing_trades=0,
                total_realized_usd=0.0 if capital_usd is not None else None,
                total_return_pct=None,
                win_rate=None,
                avg_win_pct=None,
                avg_loss_pct=None,
                profit_factor=None,
                sharpe_ratio=None,
                max_drawdown=0.0,
                avg_duration_s=None,
            )

        winners = [t for t in trades if t.is_winner]
        losers = [t for t in trades if not t.is_winner]

        # P&L monetario primario (F3): neto si fees conocidas, bruto si
        # UNKNOWN — UNKNOWN nunca se asume como 0 (ADR-0026).
        total_realized_usd = sum(t.pnl_usd if t.pnl_usd is not None else t.gross_realized_usd for t in trades)
        total_return_pct = total_realized_usd / capital_usd if capital_usd and capital_usd > 0 else None

        win_rate = len(winners) / len(trades)
        avg_win = sum(t.pnl_pct for t in winners) / len(winners) if winners else None
        avg_loss = sum(t.pnl_pct for t in losers) / len(losers) if losers else None

        gross_profit = sum(t.pnl_pct for t in winners) if winners else 0.0
        gross_loss = abs(sum(t.pnl_pct for t in losers)) if losers else 0.0
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else None

        return PerformanceSummary(
            total_trades=len(trades),
            winning_trades=len(winners),
            losing_trades=len(losers),
            total_realized_usd=total_realized_usd,
            total_return_pct=total_return_pct,
            win_rate=win_rate,
            avg_win_pct=avg_win,
            avg_loss_pct=avg_loss,
            profit_factor=profit_factor,
            sharpe_ratio=PerformanceEngine.sharpe_ratio(trades, periods_per_year=periods_per_year),
            max_drawdown=PerformanceEngine.max_drawdown(trades, capital_usd=capital_usd),
            avg_duration_s=(sum(t.duration_s for t in trades) / len(trades)),
        )

    # ------------------------------------------------------------------
    # Individual metrics (pure functions)
    # ------------------------------------------------------------------

    @staticmethod
    def sharpe_ratio(
        trades: list[TradeRecord],
        risk_free: float = 0.0,
        periods_per_year: int = 252,
    ) -> Optional[float]:
        """
        Sharpe Ratio anualizado.

        sharpe = (mean(returns) - risk_free) / std(returns) * sqrt(N)

        Returns None si hay menos de 2 trades (std indefinida).
        """
        if len(trades) < 2:
            return None

        returns = [t.pnl_pct for t in trades]
        n = len(returns)
        mean_r = sum(returns) / n
        excess = mean_r - risk_free

        variance = sum((r - mean_r) ** 2 for r in returns) / (n - 1)
        std_r = math.sqrt(variance)

        if std_r == 0.0:
            return None

        return (excess / std_r) * math.sqrt(periods_per_year)

    @staticmethod
    def equity_curve(
        trades: list[TradeRecord],
        capital_usd: Optional[float] = None,
    ) -> list[float]:
        """
        Curva de equity MONETARIA (F3).

        Punto inicial = capital_usd (o 0.0 si no se provee capital).
        Cada siguiente punto = equity anterior + realized P&L del trade
        (neto si fees conocidas, bruto si UNKNOWN).

        Retorna [capital_usd] si no hay trades.
        """
        curve: list[float] = []
        equity = capital_usd if capital_usd is not None else 0.0
        curve.append(round(equity, 6))
        for trade in trades:
            pnl = trade.pnl_usd if trade.pnl_usd is not None else trade.gross_realized_usd
            equity += pnl
            curve.append(round(equity, 6))
        return curve

    @staticmethod
    def max_drawdown(
        trades: list[TradeRecord],
        capital_usd: Optional[float] = None,
    ) -> float:
        """
        Máxima caída desde pico en la equity curve.

        Retorna 0.0 si no hay trades o el equity solo sube.

        Valor siempre >= 0 (magnitud, no negativo).

        - Si capital_usd se provee: usa la equity curve MONETARIA (F3),
          drawdown relativo a la equity acumulada.
        - Si no se provee capital: fallback normalizado sobre pnl_pct por
          trade (compounding desde 1.0). Es una métrica COMPARATIVA, no una
          fuente económica de realized P&L (el primario es total_realized_usd).
        """
        if not trades:
            return 0.0

        if capital_usd is not None and capital_usd > 0:
            curve = PerformanceEngine.equity_curve(trades, capital_usd=capital_usd)
        else:
            # Fallback normalizado (métrica comparativa, no económica).
            curve = [1.0]
            equity = 1.0
            for trade in trades:
                equity *= 1.0 + trade.pnl_pct
                curve.append(equity)

        peak = curve[0]
        max_dd = 0.0
        for equity in curve[1:]:
            peak = max(peak, equity)
            if peak > 0:
                drawdown = (peak - equity) / peak
                max_dd = max(max_dd, drawdown)

        return max_dd

    @staticmethod
    def win_rate(trades: list[TradeRecord]) -> Optional[float]:
        """Win rate: fracción de trades ganadores. None si no hay trades."""
        if not trades:
            return None
        return sum(1 for t in trades if t.is_winner) / len(trades)

    @staticmethod
    def total_realized_usd(trades: list[TradeRecord]) -> float:
        """
        P&L realizado total en USD (F3, ADR-0025/0026).

        Σ de realized P&L en USD: neto si las fees son conocidas; si el
        fee_status es UNKNOWN se usa el gross (nunca se asume fee 0).
        """
        return sum(t.pnl_usd if t.pnl_usd is not None else t.gross_realized_usd for t in trades)

    @staticmethod
    def total_return_pct(trades: list[TradeRecord], capital_usd: float) -> float:
        """Rendimiento total derivado: total_realized_usd / capital_usd."""
        total = PerformanceEngine.total_realized_usd(trades)
        return total / capital_usd if capital_usd and capital_usd > 0 else 0.0
