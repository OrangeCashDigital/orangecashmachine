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
# Builder y data source — delegados al use case (SRP)
# ---------------------------------------------------------------------------
# La lógica de ensamblaje vive en app/use_cases/run_paper.py.
# Este CLI solo parsea args, configura logging y reporta resultados.


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

    # Delegar al use case — este CLI solo reporta resultados
    from app.use_cases.run_paper import execute
    run_result = execute(args)

    if not run_result.success:
        logger.error("Use case fallido | {}", run_result.error)
        return run_result.exit_code

    result = run_result.engine_result

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
        return 0

    if not result.orders:
        logger.info("Sin señales accionables en este ciclo.")
        return 0

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
    summary = run_result.performance
    if summary:
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
        open_pos = run_result.open_positions
        if open_pos:
            logger.info("Posiciones abiertas | symbols={}", list(open_pos.keys()))

    logger.debug("OMS summary | {}", run_result.oms_summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
