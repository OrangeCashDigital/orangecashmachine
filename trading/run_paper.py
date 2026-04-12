# -*- coding: utf-8 -*-
"""
trading/run_paper.py
====================

Entrypoint para correr el PaperBot con datos reales de Gold (Iceberg).

Uso
---
    uv run python trading/run_paper.py
    uv run python trading/run_paper.py --symbol ETH/USDT --timeframe 4h
"""
from __future__ import annotations

import argparse

from loguru import logger

from data_platform.loaders.gold_loader import GoldLoader
from trading.execution.paper_bot import PaperBot
from trading.strategies.ema_crossover import EMACrossoverStrategy


def main() -> None:
    parser = argparse.ArgumentParser(description="OrangeCashMachine — Paper Bot")
    parser.add_argument("--exchange",  default="bybit",    help="Exchange")
    parser.add_argument("--symbol",    default="BTC/USDT", help="Par de trading")
    parser.add_argument("--timeframe", default="1h",       help="Timeframe")
    parser.add_argument("--fast",      type=int, default=9,  help="EMA rápida")
    parser.add_argument("--slow",      type=int, default=21, help="EMA lenta")
    args = parser.parse_args()

    logger.info(
        "PaperBot arrancando | exchange={} symbol={} timeframe={} ema={}/{}",
        args.exchange, args.symbol, args.timeframe, args.fast, args.slow,
    )

    strategy = EMACrossoverStrategy(
        symbol      = args.symbol,
        timeframe   = args.timeframe,
        fast_period = args.fast,
        slow_period = args.slow,
    )
    data_source = GoldLoader(exchange=args.exchange)
    bot = PaperBot(
        strategy    = strategy,
        data_source = data_source,
        exchange    = args.exchange,
    )

    orders = bot.run_once()

    summary = bot.summary()
    logger.info("PaperBot summary | {}", summary)

    if not orders:
        logger.info("Sin señales accionables en este ciclo.")


if __name__ == "__main__":
    main()
