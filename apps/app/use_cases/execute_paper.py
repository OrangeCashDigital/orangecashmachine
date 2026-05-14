# -*- coding: utf-8 -*-
"""
app/use_cases/execute_paper.py
================================

Use case: ejecutar un ciclo de paper trading.

Responsabilidad
---------------
Ensamblar las dependencias y ejecutar un ciclo completo:
    GoldData → TradingEngine → TradeTracker → PerformanceEngine

Separación de concerns
-----------------------
    app/cli/paper.py               → CLI: argparse, logging, exit codes
    app/use_cases/execute_paper.py → ensamblaje y ejecución (este módulo)

_SyntheticDataSource vive aquí — es un detalle de implementación
del use case (dry-run mode), no del CLI.

Principios: SRP · DIP · DRY · SSOT · SafeOps · Composition Root
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Optional

from loguru import logger

# Import estático — no dentro de closures (DRY · KISS · evita re-evaluación por fill)
from trading.execution.order import OrderSide


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
    error:          Optional[str]    = None
    engine_result:  Optional[object] = None   # EngineResult
    performance:    Optional[object] = None   # PerformanceSummary
    open_positions: Optional[dict]   = None
    oms_summary:    Optional[dict]   = None

    @property
    def exit_code(self) -> int:
        return 0 if self.success else 1


# ---------------------------------------------------------------------------
# Synthetic data source — dry-run mode only
# ---------------------------------------------------------------------------

class SyntheticDataSource:
    """
    Fuente de datos sintética para dry-run y tests de integración.

    Genera un DataFrame OHLCV con tendencia alcista y cruce EMA garantizado.
    No requiere Iceberg ni conexión de red.

    Reproducibilidad
    ----------------
    Seed fijo (42) — garantiza que dos runs con los mismos args produzcan
    los mismos datos sintéticos. Esencial para comparar resultados
    en dry-run y para tests deterministas.

    SafeOps: NUNCA usar en producción — solo dry_run=True o tests.
    """

    _SEED: int = 42   # SSOT del seed — cambiar aquí y en tests/trading/

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

        rng   = np.random.default_rng(self._SEED)   # reproducible, no muta global state
        n     = 100
        base  = 50_000.0
        dates = [
            datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(hours=i)
            for i in range(n)
        ]
        close = base + np.cumsum(rng.normal(50, 200, n))
        close = np.maximum(close, 1.0)

        df = pd.DataFrame({
            "timestamp": dates,
            "open":      close * 0.999,
            "high":      close * 1.005,
            "low":       close * 0.995,
            "close":     close,
            "volume":    rng.uniform(10, 100, n),
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
            "[DRY-RUN] SyntheticDataSource | rows={} symbol={} tf={} seed={}",
            len(df), symbol, timeframe, self._SEED,
        )
        return df


# ---------------------------------------------------------------------------
# Builder — ensamblaje de dependencias (Composition Root)
# ---------------------------------------------------------------------------

def build_paper_engine(args: argparse.Namespace, tracker):
    """
    Ensambla TradingEngine + PortfolioService para paper trading.

    Separado de execute() (SRP):
        build_paper_engine → sabe cómo construir
        execute()          → sabe cómo correr

    Fail-Fast: si dry_run=False y GoldLoaderAdapter no puede cargar datos,
    lanza explícitamente en lugar de dejar que el engine corra con None
    y genere 0 señales sin diagnóstico.

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
        logger.info(
            "[DRY-RUN] Usando datos sintéticos — sin conexión a Iceberg | seed={}",
            SyntheticDataSource._SEED,
        )
    else:
        # Fail-Fast: verificar que Gold tiene datos antes de construir el engine.
        # Sin este check, engine.run_once() corre normalmente pero genera 0 señales
        # sin ningún diagnóstico — difícil de debuggear.
        from trading.data.gold_adapter import GoldLoaderAdapter
        data_source = GoldLoaderAdapter(exchange=args.exchange)
        _probe_gold_data(data_source, args)

    portfolio = PortfolioService(
        capital_usd = args.capital,
        exchange    = args.exchange,
    )

    # Mapa BUY order_id → posición abierta, para cerrar correctamente en SELL.
    # SSOT del tracking buy→sell: este dict es la fuente de verdad del mapeo.
    # portfolio.close_position requiere el order_id del BUY (key de apertura),
    # no el del SELL — sin este mapeo las posiciones nunca cierran (bug silencioso).
    _open_order_ids: dict[str, str] = {}  # symbol → buy_order_id

    def on_fill_composite(order) -> None:
        """Callback OMS → TradeTracker + PortfolioService.

        Conecta el fill del OMS a ambas capas de analytics y portfolio.
        Usa _open_order_ids para resolver el buy_order_id en SELL — evita
        el bug de intentar cerrar una posición con el ID del SELL order.
        """
        # 1. TradeTracker — siempre primero (analytics independiente de portfolio)
        tracker.on_fill(order)

        # 2. Portfolio — sincroniza estado de posición abierta/cerrada
        if order.side == OrderSide.BUY:
            _open_order_ids[order.symbol] = order.order_id
            portfolio.open_position(
                order_id    = order.order_id,
                symbol      = order.symbol,
                side        = "long",
                entry_price = order.fill_price,
                size_pct    = order.size_pct,
                entry_at    = order.fill_timestamp,
            )
        elif order.side == OrderSide.SELL:
            # Usar el order_id del BUY original — las posiciones se indexan
            # por el ID de apertura, no por el ID de cierre.
            buy_order_id = _open_order_ids.pop(order.symbol, None)
            if buy_order_id is not None:
                portfolio.close_position(buy_order_id)
            else:
                logger.warning(
                    "on_fill_composite | SELL sin BUY previo registrado | "
                    "symbol={} sell_order_id={}",
                    order.symbol, order.order_id,
                )

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
# Helper privado — probe de disponibilidad Gold (Fail-Fast)
# ---------------------------------------------------------------------------

def _probe_gold_data(data_source, args: argparse.Namespace) -> None:
    """Verifica que Gold tiene datos antes de construir el engine.

    Fail-Fast: lanza RuntimeError si no hay datos disponibles.
    Produce un mensaje accionable con exchange/symbol/tf para diagnóstico
    en lugar del silencioso 0-señales del engine.

    Solo ejecuta en dry_run=False. En dry-run SyntheticDataSource
    siempre devuelve datos — no necesita probe.

    Raises
    ------
    RuntimeError : si Gold no devuelve datos para el par solicitado.
                   Mensaje incluye exchange/symbol/timeframe para diagnóstico.
    """
    try:
        probe = data_source.load_features(
            exchange    = args.exchange,
            symbol      = args.symbol,
            timeframe   = args.timeframe,
            market_type = args.market_type,
        )
    except Exception as exc:
        raise RuntimeError(
            f"Gold data unavailable | exchange={args.exchange} "
            f"symbol={args.symbol} tf={args.timeframe} "
            f"market_type={args.market_type} | "
            f"Correr './run.sh ocm' para ingestar datos primero. "
            f"Error: {exc}"
        ) from exc

    if probe is None or (hasattr(probe, "empty") and probe.empty):
        raise RuntimeError(
            f"Gold data empty | exchange={args.exchange} "
            f"symbol={args.symbol} tf={args.timeframe} "
            f"market_type={args.market_type} | "
            f"Correr './run.sh ocm' para ingestar datos primero."
        )

    logger.info(
        "Gold data OK | exchange={} symbol={} tf={} rows={}",
        args.exchange, args.symbol, args.timeframe,
        len(probe) if hasattr(probe, "__len__") else "?",
    )


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

    trades      = tracker.closed_trades
    performance = (
        PerformanceEngine.summarize(trades, capital_usd=args.capital)
        if trades else None
    )

    return PaperRunResult(
        success        = True,
        engine_result  = engine_result,
        performance    = performance,
        open_positions = tracker.open_positions,
        oms_summary    = engine.oms_summary,
    )
