# -*- coding: utf-8 -*-
"""
app/use_cases/run_live.py
==========================

Use case: ejecutar un ciclo de live trading.

Responsabilidad
---------------
Ensamblar las dependencias para live trading y ejecutar un ciclo:
  GoldData → TradingEngine(live) → TradeTracker → PortfolioService(Redis)

Diferencias vs run_paper.py
----------------------------
  - LiveExecutor en lugar de PaperExecutor
  - RedisPositionStore en lugar de InMemoryPositionStore
  - guard obligatorio — sin kill switch no hay live trading
  - risk_config obligatoria — no defaults permisivos
  - Sin SyntheticDataSource — siempre datos reales de Gold

SafeOps en live
---------------
- Fail-Fast en build: guard y risk_config obligatorios.
- Fail-Soft en execute: errores retornados en LiveRunResult, no lanzan.
- Toda orden enviada al exchange queda logueada con order_id.

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
class LiveRunResult:
    """
    Resultado completo de un ciclo de live trading.

    Usado por el CLI para determinar exit code y logging final.
    """
    success:        bool
    error:          Optional[str]    = None
    engine_result:  Optional[object] = None   # EngineResult
    performance:    Optional[object] = None   # PerformanceSummary
    open_positions: Optional[dict]   = None
    oms_summary:    Optional[dict]   = None

    @property
    def exit_code(self) -> int:
        return 0 if self.success else 1


# ---------------------------------------------------------------------------
# Builder — ensamblaje de dependencias live
# ---------------------------------------------------------------------------

def build_live_engine(args: argparse.Namespace, tracker):
    """
    Ensambla TradingEngine(live) + PortfolioService(Redis).

    Fail-Fast:
    - guard construido y validado antes de llamar a build_live()
    - risk_config explícita — no defaults

    Returns
    -------
    tuple[TradingEngine, PortfolioService]
    """
    from trading.risk.models import (
        RiskConfig, PositionConfig, OrderLimits, SignalFilterConfig,
    )
    from trading.engine import TradingEngine
    from trading.execution.order import OrderSide
    from trading.data.gold_adapter import GoldLoaderAdapter
    from market_data.safety.execution_guard import ExecutionGuard
    from portfolio.services.portfolio_service import PortfolioService
    from portfolio.infra.redis_store import RedisPositionStore

    # Construir guard — obligatorio en live
    guard = ExecutionGuard(
        max_errors      = args.max_errors,
        error_window_s  = 300,   # ventana de 5 min para contar errores
    )

    # RiskConfig explícita — no defaults
    risk_config = RiskConfig(
        position      = PositionConfig(
            max_position_pct   = args.max_risk_pct,
            max_open_positions = args.max_positions,
        ),
        signal_filter = SignalFilterConfig(
            min_confidence = args.min_confidence,
        ),
        order = OrderLimits(
            min_order_usd = args.min_order_usd,
            max_order_usd = args.capital * args.max_risk_pct,
        ),
    )

    # RedisPositionStore para persistencia cross-restart
    import redis as redis_lib
    redis_client = redis_lib.Redis(
        host            = args.redis_host,
        port            = args.redis_port,
        db              = args.redis_db,
        socket_timeout  = 3,
        decode_responses = False,
    )

    store = RedisPositionStore(
        redis_client = redis_client,
        exchange     = args.exchange,
    )

    portfolio = PortfolioService(
        capital_usd = args.capital,
        store       = store,
        exchange    = args.exchange,
    )

    # on_fill composite: TradeTracker + PortfolioService
    def on_fill_composite(order):
        tracker.on_fill(order)
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

    data_source = GoldLoaderAdapter(exchange=args.exchange)

    engine = TradingEngine.build_live(
        strategy_name = args.strategy,
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
        guard        = guard,
        on_fill      = on_fill_composite,
    )

    return engine, portfolio


# ---------------------------------------------------------------------------
# Use case — ejecutar ciclo completo
# ---------------------------------------------------------------------------

def execute(args: argparse.Namespace) -> LiveRunResult:
    """
    Ejecuta un ciclo de live trading.

    SafeOps: nunca lanza — errores retornados en LiveRunResult.
    """
    from trading.analytics.trade_tracker import TradeTracker
    from trading.analytics.performance import PerformanceEngine

    tracker = TradeTracker(exchange=args.exchange)

    try:
        engine, portfolio = build_live_engine(args, tracker)
    except Exception as exc:
        logger.error("Error construyendo engine live | {} — {}", type(exc).__name__, exc)
        return LiveRunResult(success=False, error=str(exc))

    logger.info("Engine live listo | {}", engine)
    logger.info("Portfolio (Redis) | {}", portfolio)

    try:
        engine_result = engine.run_once()
    except Exception as exc:
        logger.error("Error en run_once live | {} — {}", type(exc).__name__, exc)
        return LiveRunResult(success=False, error=str(exc))

    trades      = tracker.closed_trades
    performance = PerformanceEngine.summarize(trades, capital_usd=args.capital) if trades else None
    open_pos    = tracker.open_positions

    return LiveRunResult(
        success        = True,
        engine_result  = engine_result,
        performance    = performance,
        open_positions = open_pos,
        oms_summary    = engine.oms_summary,
    )
