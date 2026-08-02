# -*- coding: utf-8 -*-
"""
app/use_cases/execute_live.py
==============================

Use case: ejecutar un ciclo de live trading.

Responsabilidad
---------------
Ensamblar las dependencias para live trading y ejecutar un ciclo:
  GoldData → TradingEngine(live) → TradeTracker → PortfolioService(Redis)

Diferencias vs execute_paper.py
--------------------------------
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
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from trading.analytics.performance import PerformanceSummary
    from trading.engine import EngineResult

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

    success: bool
    error: Optional[str] = None
    engine_result: Optional["EngineResult"] = None
    performance: Optional["PerformanceSummary"] = None
    open_positions: Optional[dict] = None
    oms_summary: Optional[dict] = None

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
    - risk_config explícita — no defaults permisivos

    Returns
    -------
    tuple[TradingEngine, PortfolioService]
    """
    from portfolio.infra.redis_factory import build_redis_client
    from portfolio.infra.redis_store import RedisPositionStore
    from portfolio.services.portfolio_service import PortfolioService
    from trading.data.gold_adapter import GoldLoaderAdapter
    from trading.engine import TradingEngine
    from trading.execution.fill_sync import build_fill_sync
    from trading.risk.models import (
        OrderLimits,
        PositionConfig,
        RiskConfig,
        SignalFilterConfig,
    )

    from ocm.runtime.guard import ExecutionGuard

    # Fail-Fast: guard obligatorio en live — sin kill switch no hay ejecución
    guard = ExecutionGuard(
        max_errors=args.max_errors,
    )

    # RiskConfig explícita — no defaults permisivos en live
    risk_config = RiskConfig(
        position=PositionConfig(
            max_position_pct=args.max_risk_pct,
            max_open_positions=args.max_positions,
        ),
        signal_filter=SignalFilterConfig(
            min_confidence=args.min_confidence,
        ),
        order=OrderLimits(
            min_order_usd=args.min_order_usd,
            max_order_usd=args.capital * args.max_risk_pct,
        ),
    )

    # RedisPositionStore — persistencia cross-restart obligatoria en live
    redis_client = build_redis_client(
        host=args.redis_host,
        port=args.redis_port,
        db=args.redis_db,
    )

    store = RedisPositionStore(
        redis_client=redis_client,
        exchange=args.exchange,
    )

    portfolio = PortfolioService(
        capital_usd=args.capital,
        store=store,
        exchange=args.exchange,
    )

    # SSOT: callback compartido con paper trading — ver trading/execution/fill_sync.py
    on_fill_composite = build_fill_sync(tracker, portfolio)

    data_source = GoldLoaderAdapter(exchange=args.exchange)

    engine = TradingEngine.build_live(
        strategy_name=args.strategy,
        strategy_cfg={
            "symbol": args.symbol,
            "timeframe": args.timeframe,
            "fast_period": args.fast,
            "slow_period": args.slow,
        },
        data_source=data_source,
        risk_config=risk_config,
        capital_usd=args.capital,
        exchange=args.exchange,
        market_type=args.market_type,
        guard=guard,
        on_fill=on_fill_composite,
    )

    return engine, portfolio


# ---------------------------------------------------------------------------
# Use case — ejecutar ciclo completo
# ---------------------------------------------------------------------------


def execute(args: argparse.Namespace) -> LiveRunResult:
    """
    Ejecuta un ciclo de live trading.

    SafeOps: nunca lanza — errores retornados en LiveRunResult.

    Returns
    -------
    LiveRunResult con todo lo necesario para que el CLI loguee y salga.
    """
    from trading.analytics.performance import PerformanceEngine
    from trading.analytics.trade_tracker import TradeTracker

    tracker = TradeTracker(exchange=args.exchange)

    try:
        engine, portfolio = build_live_engine(args, tracker)
    except Exception as exc:
        logger.error(
            "Error construyendo engine live | {} — {}",
            type(exc).__name__,
            exc,
        )
        return LiveRunResult(success=False, error=str(exc))

    logger.info("Engine live listo | {}", engine)
    logger.info("Portfolio (Redis) | {}", portfolio)

    try:
        engine_result = engine.run_once()
    except Exception as exc:
        logger.error(
            "Error en run_once live | {} — {}",
            type(exc).__name__,
            exc,
        )
        return LiveRunResult(success=False, error=str(exc))

    trades = tracker.closed_trades
    performance = PerformanceEngine.summarize(trades, capital_usd=args.capital) if trades else None

    return LiveRunResult(
        success=True,
        engine_result=engine_result,
        performance=performance,
        open_positions=tracker.open_positions,
        oms_summary=engine.oms_summary,
    )
