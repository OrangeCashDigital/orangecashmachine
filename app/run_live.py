# -*- coding: utf-8 -*-
"""
app/run_live.py
================

Entrypoint CLI para live trading con capital real.

⚠️  ADVERTENCIA: Este comando opera con capital real.
    Verificar configuración de riesgo antes de ejecutar.

Uso
---
    uv run live
    uv run live --symbol ETH/USDT --timeframe 4h
    uv run live --capital 5000 --max-risk-pct 0.02

Exit codes
----------
    0 → ciclo completado (con o sin señales)
    1 → error fatal (config inválida, exchange no disponible)

Principios: SRP · SafeOps · Composition Root
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
        description     = "OrangeCashMachine — Live Trading ⚠️  Capital Real",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
    )

    # Identidad
    p.add_argument("--exchange",    default="bybit",    help="Exchange")
    p.add_argument("--symbol",      default="BTC/USDT", help="Par de trading")
    p.add_argument("--timeframe",   default="1h",       help="Timeframe")
    p.add_argument("--market-type", default="spot",
                   dest="market_type",
                   choices=["spot", "swap", "linear", "inverse"],
                   help="Tipo de mercado")

    # Estrategia
    p.add_argument("--strategy", default="ema_crossover", help="Nombre de estrategia")
    p.add_argument("--fast",     type=int, default=9,     help="EMA rápida")
    p.add_argument("--slow",     type=int, default=21,    help="EMA lenta")

    # Capital y riesgo — conservadores por defecto en live
    p.add_argument("--capital",        type=float, default=1_000.0,
                   help="Capital en USD (verificar antes de ejecutar)")
    p.add_argument("--max-risk-pct",   type=float, default=0.02,
                   dest="max_risk_pct",
                   help="Máximo %% del capital por posición (live: conservador)")
    p.add_argument("--max-positions",  type=int,   default=2,
                   dest="max_positions",
                   help="Máximo posiciones simultáneas")
    p.add_argument("--min-confidence", type=float, default=0.9,
                   dest="min_confidence",
                   help="Confianza mínima de señal (live: más restrictivo)")
    p.add_argument("--min-order-usd",  type=float, default=10.0,
                   dest="min_order_usd",
                   help="Orden mínima en USD")
    p.add_argument("--max-errors",     type=int,   default=3,
                   dest="max_errors",
                   help="Errores consecutivos antes de activar guard/halt")

    # Redis
    p.add_argument("--redis-host", default="localhost", dest="redis_host")
    p.add_argument("--redis-port", type=int, default=6379, dest="redis_port")
    p.add_argument("--redis-db",   type=int, default=1,    dest="redis_db")

    # Debug
    p.add_argument("--debug", action="store_true", help="Log nivel DEBUG")

    return p


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    # Logging
    logger.remove()
    level = "DEBUG" if args.debug else "INFO"
    logger.add(
        sys.stderr, level=level, colorize=True,
        format="<red>{time:HH:mm:ss}</red> | <level>{level:<8}</level> | {message}",
    )

    # Advertencia explícita — capital real
    logger.warning("=" * 52)
    logger.warning("⚠️   LIVE TRADING — CAPITAL REAL")
    logger.warning(
        "    exchange={} symbol={} capital={:.0f} USD",
        args.exchange, args.symbol, args.capital,
    )
    logger.warning("=" * 52)

    logger.info(
        "Live trading iniciando | exchange={} symbol={} tf={} "
        "market_type={} strategy={} capital={:.0f}",
        args.exchange, args.symbol, args.timeframe,
        args.market_type, args.strategy, args.capital,
    )

    from app.use_cases.run_live import execute
    run_result = execute(args)

    if not run_result.success:
        logger.error("Live use case fallido | {}", run_result.error)
        return run_result.exit_code

    result = run_result.engine_result

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
        return 0

    if not result.orders:
        logger.info("Sin señales accionables en este ciclo.")
        return 0

    for order in result.orders:
        logger.info(
            "  [LIVE] {} {} {} @ {:.4f} | size={:.1%} | status={}",
            order.order_id,
            order.side.value.upper(),
            order.symbol,
            order.signal.price,
            order.size_pct,
            order.status.value,
        )

    summary = run_result.performance
    if summary:
        logger.info("── Performance ─────────────────────────────────")
        logger.info("  Trades cerrados : {}", summary.total_trades)
        logger.info("  Win rate        : {}",
                    f"{summary.win_rate:.1%}" if summary.win_rate is not None else "N/A")
        logger.info("  PnL total       : {:+.2%}", summary.total_pnl_pct)
        logger.info("  PnL USD         : {:+.2f}",
                    summary.pnl_usd if summary.pnl_usd is not None else 0.0)
        logger.info("  Max drawdown    : {:.2%}", summary.max_drawdown)
        logger.info("────────────────────────────────────────────────")
    else:
        open_pos = run_result.open_positions
        if open_pos:
            logger.info("Posiciones abiertas | symbols={}", list(open_pos.keys()))

    logger.debug("OMS summary | {}", run_result.oms_summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
