# -*- coding: utf-8 -*-
"""
app/run_paper.py
================

Entrypoint CLI para paper trading con datos reales de Gold (Iceberg).

Arquitectura
------------
CLI args
  → GoldLoaderAdapter   (adapta GoldLoader al protocolo FeatureSource)
  → TradingEngine       (orquesta estrategia → riesgo → OMS)
  → TradeTracker        (empareja fills en TradeRecords via on_fill)
  → PerformanceEngine   (métricas: PnL, win rate, Sharpe, drawdown)

Uso
---
    uv run paper
    uv run paper --symbol ETH/USDT --timeframe 4h
    uv run paper --capital 50000 --market-type swap
    uv run paper --dry-run

    # o directamente:
    uv run python app/run_paper.py --dry-run

Exit codes
----------
    0 → ciclo completado (con o sin señales)
    1 → error fatal (datos no disponibles, error de estrategia)

Principios: SOLID · KISS · DRY · SafeOps · Composition Root
"""
from __future__ import annotations

import argparse
import sys


from loguru import logger


# ---------------------------------------------------------------------------
# Argparse
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description  = "OrangeCashMachine — Paper Trading",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
    )

    # Identidad del dataset
    p.add_argument("--exchange",     default="bybit",    help="Exchange")
    p.add_argument("--symbol",       default="BTC/USDT", help="Par de trading")
    p.add_argument("--timeframe",    default="1h",       help="Timeframe")
    p.add_argument("--market-type",  default="spot",
                   dest="market_type",
                   choices=["spot", "swap", "linear", "inverse"],
                   help="Tipo de mercado")

    # Estrategia
    p.add_argument("--fast",  type=int, default=9,  help="EMA rápida (períodos)")
    p.add_argument("--slow",  type=int, default=21, help="EMA lenta (períodos)")

    # Capital y riesgo
    p.add_argument("--capital",          type=float, default=10_000.0,
                   help="Capital virtual en USD")
    p.add_argument("--max-risk-pct",     type=float, default=0.05,
                   dest="max_risk_pct",
                   help="Máximo % del capital por posición (0.05 = 5%%)")
    p.add_argument("--max-positions",    type=int,   default=3,
                   dest="max_positions",
                   help="Máximo posiciones abiertas simultáneas")
    p.add_argument("--min-confidence",   type=float, default=0.8,
                   dest="min_confidence",
                   help="Confianza mínima de señal para actuar (0.0–1.0)")

    # Modo
    p.add_argument("--dry-run", action="store_true",
                   help="Simula sin conectar a Gold — usa datos sintéticos")
    p.add_argument("--debug",   action="store_true",
                   help="Nivel de log DEBUG")

    return p


# ---------------------------------------------------------------------------
# Dry-run data source (para --dry-run / tests sin Iceberg)
# ---------------------------------------------------------------------------

class _SyntheticDataSource:
    """
    Fuente de datos sintética para --dry-run.

    Genera un DataFrame OHLCV con tendencia alcista y cruce EMA garantizado.
    No requiere Iceberg ni conexión de red.
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
        # Tendencia alcista con leve ruido — garantiza cruce EMA 9/21
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

        # Features básicos que EMACrossoverStrategy necesita
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
            "[DRY-RUN] Datos sintéticos generados | rows={} symbol={} tf={}",
            len(df), symbol, timeframe,
        )
        return df


# ---------------------------------------------------------------------------
# Builder — construye el engine con todas las dependencias
# ---------------------------------------------------------------------------

def _build_engine(args: argparse.Namespace, tracker):
    """
    Construye TradingEngine con RiskConfig personalizada y TradeTracker.

    Separa la construcción del ciclo de ejecución (SRP).
    """
    from trading.risk.models import (
        RiskConfig, PositionConfig, OrderLimits, SignalFilterConfig
    )
    from trading.engine import TradingEngine

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
        data_source = _SyntheticDataSource()
        logger.info("[DRY-RUN] Usando datos sintéticos — sin conexión a Iceberg")
    else:
        from trading.data.gold_adapter import GoldLoaderAdapter
        data_source = GoldLoaderAdapter(exchange=args.exchange)

    return TradingEngine.build_paper(
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
        on_fill      = tracker.on_fill,   # conecta OMS → TradeTracker
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    """
    Punto de entrada principal.

    Returns
    -------
    int — exit code (0 = OK, 1 = error)
    """
    args = _build_parser().parse_args(argv)

    # Logging
    logger.remove()
    level = "DEBUG" if args.debug else "INFO"
    logger.add(sys.stderr, level=level, colorize=True,
               format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")

    logger.info(
        "Paper trading iniciando | exchange={} symbol={} tf={} "
        "market_type={} ema={}/{} capital={:.0f} dry_run={}",
        args.exchange, args.symbol, args.timeframe,
        args.market_type, args.fast, args.slow,
        args.capital, args.dry_run,
    )

    # TradeTracker — conectado al OMS via on_fill antes de construir el engine
    from trading.analytics.trade_tracker import TradeTracker
    from trading.analytics.performance import PerformanceEngine

    tracker = TradeTracker(exchange=args.exchange)

    # Construir engine — toda la dependencia assembly en un lugar
    try:
        engine = _build_engine(args, tracker)
    except Exception as exc:
        logger.error("Error construyendo engine | {} — {}", type(exc).__name__, exc)
        return 1

    logger.info("Engine listo | {}", engine)

    # Ejecutar ciclo
    try:
        result = engine.run_once()
    except Exception as exc:
        logger.error("Error en run_once | {} — {}", type(exc).__name__, exc)
        return 1

    # ── Resultado del ciclo ────────────────────────────────────────────────
    logger.info(
        "Ciclo completado | status={} signals={} submitted={} filled={} rejected={}",
        result.status,
        result.signals_generated,
        result.orders_submitted,
        result.orders_filled,
        result.orders_rejected,
    )

    if result.skipped:
        logger.warning("Ciclo skipped | reason={}", result.skip_reason)
        # Sin datos no es error fatal — exit 0 para Prefect/cron
        return 0

    if not result.orders:
        logger.info("Sin señales accionables en este ciclo.")
        return 0

    # Órdenes generadas — log detallado
    for order in result.orders:
        logger.info(
            "  {} {} {} @ {:.4f} | size={:.1%} | status={}",
            order.order_id,
            order.side.value.upper(),
            order.symbol,
            order.signal.price,
            order.size_pct,
            order.status.value,
        )

    # ── Performance summary ────────────────────────────────────────────────
    trades = tracker.closed_trades
    if trades:
        summary = PerformanceEngine.summarize(trades, capital_usd=args.capital)
        logger.info("── Performance ─────────────────────────────────")
        logger.info("  Trades cerrados : {}", summary.total_trades)
        logger.info("  Win rate        : {}",
                    f"{summary.win_rate:.1%}" if summary.win_rate is not None else "N/A")
        logger.info("  PnL total       : {:+.2%}", summary.total_pnl_pct)
        logger.info("  PnL USD         : {:+.2f}",
                    summary.pnl_usd if summary.pnl_usd is not None else 0.0)
        logger.info("  Sharpe ratio    : {}",
                    f"{summary.sharpe_ratio:.2f}" if summary.sharpe_ratio is not None else "N/A")
        logger.info("  Max drawdown    : {:.2%}", summary.max_drawdown)
        logger.info("  Profit factor   : {}",
                    f"{summary.profit_factor:.2f}" if summary.profit_factor is not None else "N/A")
        logger.info("────────────────────────────────────────────────")
    else:
        # Posiciones abiertas — BUYs sin SELL correspondiente aún
        open_pos = tracker.open_positions
        if open_pos:
            logger.info(
                "Posiciones abiertas (sin cerrar) | symbols={}",
                list(open_pos.keys()),
            )

    # OMS summary
    oms_s = engine.oms_summary
    logger.debug("OMS summary | {}", oms_s)

    return 0


if __name__ == "__main__":
    sys.exit(main())
