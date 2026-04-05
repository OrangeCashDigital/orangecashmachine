#!/usr/bin/env python3
"""
data_platform/repair_silver.py
================================

Script standalone para reparar gaps en series OHLCV Silver.
Usa UnifiedPipeline con mode=repair, sin requerir Prefect.

Uso
---
    # Reparar una serie específica
    uv run python3 data_platform/repair_silver.py \
        --exchange kucoin --symbol BTC/USDT --timeframe 1m

    # Reparar todas las series con gaps (según validate_silver)
    uv run python3 data_platform/repair_silver.py --all

    # Dry-run: detectar gaps sin reparar
    uv run python3 data_platform/repair_silver.py \
        --exchange kucoin --symbol BTC/USDT --timeframe 1m --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import List, Optional, Tuple

from core.config.hydra_loader import load_appconfig_standalone
from market_data.adapters.exchange.ccxt_adapter import CCXTAdapter
from market_data.processing.pipelines.unified_pipeline import UnifiedPipeline
from data_platform.validate_silver import discover_series, validate_series


async def _repair_series(
    exchange_id: str,
    symbol:      str,
    market_type: str,
    timeframe:   str,
    config_path: Path,
    dry_run:     bool = False,
) -> bool:
    """Repara una serie. Retorna True si tuvo éxito."""

    config   = load_appconfig_standalone(env=None, config_dir=config_path)
    exc_cfg  = config.get_exchange(exchange_id)

    if exc_cfg is None:
        print(f"  ERROR: exchange '{exchange_id}' no encontrado en config")
        return False

    default_type = market_type if market_type != "spot" else None

    if dry_run:
        result = validate_series(exchange_id, symbol, market_type, timeframe)
        if result.gaps:
            print(f"  DRY-RUN gaps encontrados: {len(result.gaps)}")
            for g in result.gaps:
                print(f"    {g}")
        else:
            print("  DRY-RUN: sin gaps")
        return True

    adapter = CCXTAdapter(config=exc_cfg, default_type=default_type)
    try:
        await adapter.connect()
        pipeline = UnifiedPipeline(
            symbols         = [symbol],
            timeframes      = [timeframe],
            start_date      = config.pipeline.historical.start_date,
            exchange_client = adapter,
            max_concurrency = 1,
            market_type     = market_type,
            backfill_mode   = False,
        )
        summary = await pipeline.run(mode="repair")

        if summary.total_gaps_found == 0:
            print(f"  Sin gaps detectados en Silver")
        else:
            print(
                f"  gaps_found={summary.total_gaps_found} "
                f"gaps_healed={summary.total_gaps_healed} "
                f"rows={summary.total_rows}"
            )
        return summary.failed == 0

    finally:
        await adapter.close()


async def _repair_all(config_path: Path, dry_run: bool) -> int:
    """Detecta series con gaps via validate_silver y las repara."""
    series_list = discover_series()
    failures = 0

    for exc, sym, mkt, tf in series_list:
        result = validate_series(exc, sym, mkt, tf)
        if not result.gaps:
            continue

        print(f"\nReparando {result.key} | gaps={len(result.gaps)}")
        for g in result.gaps:
            print(f"  {g}")

        ok = await _repair_series(exc, sym, mkt, tf, config_path, dry_run)
        if not ok:
            failures += 1

    return failures


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reparador de gaps en series OHLCV Silver"
    )
    parser.add_argument("--exchange", "-e", help="Exchange (ej: kucoin)")
    parser.add_argument("--symbol",   "-s", help="Simbolo (ej: BTC/USDT)")
    parser.add_argument("--timeframe","-t", help="Timeframe (ej: 1m)")
    parser.add_argument(
        "--market-type", "-m",
        default="spot",
        help="Tipo de mercado: spot, swap (default: spot)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Reparar todas las series con gaps detectados",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Detectar gaps sin reparar",
    )
    parser.add_argument(
        "--config-dir",
        default="config",
        help="Directorio de configuracion (default: config)",
    )
    args = parser.parse_args()

    config_path = Path(args.config_dir)

    if args.all:
        print(f"Modo --all: detectando series con gaps...")
        failures = asyncio.run(_repair_all(config_path, args.dry_run))
        if failures:
            print(f"\n{failures} series fallaron")
            return 1
        print("\nReparacion completada")
        return 0

    if not all([args.exchange, args.symbol, args.timeframe]):
        parser.error("Requiere --exchange, --symbol y --timeframe (o --all)")

    action = "DRY-RUN" if args.dry_run else "Reparando"
    print(
        f"{action}: {args.exchange}/{args.symbol}/"
        f"{args.market_type}/{args.timeframe}"
    )

    ok = asyncio.run(_repair_series(
        exchange_id = args.exchange,
        symbol      = args.symbol,
        market_type = args.market_type,
        timeframe   = args.timeframe,
        config_path = config_path,
        dry_run     = args.dry_run,
    ))

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
