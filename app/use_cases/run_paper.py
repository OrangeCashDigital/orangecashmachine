# -*- coding: utf-8 -*-
"""
app/use_cases/run_paper.py
===========================

Use case: ejecutar un ciclo de paper trading.

Responsabilidad
---------------
Ensamblar las dependencias y ejecutar un ciclo completo:
  GoldData → TradingEngine → TradeTracker → PerformanceEngine

Separación de concerns vs app/run_paper.py
-------------------------------------------
  app/run_paper.py       → CLI: argparse, logging, exit codes
  app/use_cases/run_paper.py → lógica de ensamblaje y ejecución

_SyntheticDataSource vive aquí — es un detalle de implementación
del use case (dry-run mode), no del CLI.

Principios: SRP · DIP · DRY · SafeOps · Composition Root
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Optional

from loguru import logger


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

@dataclass
class PaperRunResult:
    """
    Resultado completo de un ciclo de paper trading.

    Usado por el CLI para determinar el exit code y el logging final.
    """
    success:        bool
    error:          Optional[str]     = None
    engine_result:  Optional[object]  = None   # EngineResult
    performance:    Optional[object]  = None   # PerformanceSummary
    open_positions: Optional[dict]    = None
    oms_summary:    Optional[dict]    = None

    @property
    def exit_code(self) -> int:
        return 0 if self.success else 1


# ---------------------------------------------------------------------------
# Synthetic data source — dry-run mode
# ---------------------------------------------------------------------------

class SyntheticDataSource:
    """
    Fuente de datos sintética para dry-run y tests de integración.

    Genera un DataFrame OHLCV con tendencia alcista y cruce EMA garantizado.
    No requiere Iceberg ni conexión de red.

    Extraído aquí desde app/run_paper.py (SRP — el CLI no debe
    contener detalles de implementación de datos).
    """

    def load_features(
        self,
        exchange:    str,
        symbol:      str,
        timeframe:   str,
        market_type: str = "spot",
        **kwargs,
    ):
        import numpy as np
        import pandas as pd
        from datetime import datetime, timedelta, timezone

        n     = 100
        base  = 50_000.0
        dates = [
            datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(hours=i)
            for i in range(n)
        ]
        close = base + np.cumsum(np.random.normal(50, 200, n))
        close = np.maximum(close, 1.0)

        df = pd.DataFrame({
            "timestamp": dates,
            "open":      close * 0.999,
            "high":      close * 1.005,
            "low":       close * 0.995,
            "close":     close,
            "volume":    np.random.uniform(10, 100, n),
        })

        df["return_1"]        = df["close"].pct_change()
        df["log_return"]      = np.log(df["close"] / df["close"].shift(1))
        df["volatility_20"]   = df["log_return"].rolling(20, min_periods=5).std()
        df["high_low_spread"] = (df["high"] - df["low"]) / df["close"]
        tp = (df["high"] + df["low"] + df["close"]) / 3
        df["vwap"] = (
            (tp * df["volume"]).rolling(20, min_periods=5).sum()
            / df["volume"].rolling(20, min_periods=5).sum()
        )

        logger.debug(
            "[DRY-RUN] Datos sintéticos | rows={} symbol={} tf={}",
            len(df), symbol, timeframe,
        )
        return df


# ---------------------------------------------------------------------------
# Builder — ensamblaje de dependencias
# ---------------------------------------------------------------------------

def build_paper_engine(args: argparse.Namespace, tracker):
    """
    Ensambla TradingEngine + PortfolioService para paper trading.

    Separado de execute() (SRP):
      build_paper_engine → sabe cómo construir
      execute()          → sabe cómo correr

    Parameters
    ----------
    args    : namespace de argparse con todos los parámetros del ciclo
    tracker : TradeTracker ya instanciado (para conectar on_fill)

    Returns
    -------
    tuple[TradingEngine, PortfolioService]
    """
    from trading.risk.models import (
        RiskConfig, PositionConfig, OrderLimits, SignalFilterConfig,
    )
    from trading.engine import TradingEngine
    from portfolio.services.portfolio_service import PortfolioService

    risk_config = RiskConfig(
        position      = PositionConfig(
            max_position_pct   = args.max_risk_pct,
            max_open_positions = args.max_positions,
        ),
        signal_filter = SignalFilterConfig(
            min_confidence = args.min_confidence,
        ),
        order = OrderLimits(
            min_order_usd = 10.0,
            max_order_usd = args.capital * args.max_risk_pct,
        ),
    )

    if args.dry_run:
        data_source = SyntheticDataSource()
        logger.info("[DRY-RUN] Usando datos sintéticos — sin conexión a Iceberg")
    else:
        from trading.data.gold_adapter import GoldLoaderAdapter
        data_source = GoldLoaderAdapter(exchange=args.exchange)

    portfolio = PortfolioService(
        capital_usd = args.capital,
        exchange    = args.exchange,
    )

    # on_fill conecta OMS → TradeTracker Y → PortfolioService
    def on_fill_composite(order):
        tracker.on_fill(order)
        from trading.execution.order import OrderSide
        if order.side == OrderSide.BUY:
            portfolio.open_position(
                order_id    = order.order_id,
                symbol      = order.symbol,
                side        = "long",
                entry_price = order.fill_price,
                size_pct    = order.size_pct,
                entry_at    = order.fill_timestamp,
            )
        elif order.side == OrderSide.SELL:
            portfolio.close_position(order.order_id)

    engine = TradingEngine.build_paper(
        strategy_name = "ema_crossover",
        strategy_cfg  = {
            "symbol":      args.symbol,
            "timeframe":   args.timeframe,
            "fast_period": args.fast,
            "slow_period": args.slow,
        },
        data_source  = data_source,
        risk_config  = risk_config,
        capital_usd  = args.capital,
        exchange     = args.exchange,
        market_type  = args.market_type,
        on_fill      = on_fill_composite,
    )

    return engine, portfolio


# ---------------------------------------------------------------------------
# Use case — ejecutar ciclo completo
# ---------------------------------------------------------------------------

def execute(args: argparse.Namespace) -> PaperRunResult:
    """
    Ejecuta un ciclo completo de paper trading.

    Punto de entrada del use case — el CLI llama esto.
    Encapsula todo el flujo: build → run → analytics.

    SafeOps: nunca lanza — errores retornados en PaperRunResult.

    Returns
    -------
    PaperRunResult con todo lo necesario para que el CLI loguee y salga.
    """
    from trading.analytics.trade_tracker import TradeTracker
    from trading.analytics.performance import PerformanceEngine

    tracker = TradeTracker(exchange=args.exchange)

    try:
        engine, portfolio = build_paper_engine(args, tracker)
    except Exception as exc:
        logger.error("Error construyendo engine | {} — {}", type(exc).__name__, exc)
        return PaperRunResult(success=False, error=str(exc))

    logger.info("Engine listo | {}", engine)
    logger.info("Portfolio | {}", portfolio)

    try:
        engine_result = engine.run_once()
    except Exception as exc:
        logger.error("Error en run_once | {} — {}", type(exc).__name__, exc)
        return PaperRunResult(success=False, error=str(exc))

    # Analytics
    trades      = tracker.closed_trades
    performance = PerformanceEngine.summarize(trades, capital_usd=args.capital) if trades else None
    open_pos    = tracker.open_positions

    return PaperRunResult(
        success        = True,
        engine_result  = engine_result,
        performance    = performance,
        open_positions = open_pos,
        oms_summary    = engine.oms_summary,
    )
