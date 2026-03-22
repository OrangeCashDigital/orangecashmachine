from __future__ import annotations

"""
market_data/orchestration/entrypoint.py
========================================

Entrypoint LOCAL — solo para desarrollo y debug.

En producción el flow es disparado por Prefect Server/Worker
via deployment. Este archivo NO forma parte del path de producción.

Uso
---
    python -m market_data.orchestration.entrypoint
    OCM_ENV=development python main.py

Principios
----------
SOLID  – SRP: orquesta el arranque local, no decide exit codes
KISS   – flujo lineal, sin lógica de negocio
DRY    – reutiliza core.logging, load_config y market_data_flow
SafeOps – run() retorna int, sys.exit solo en __main__
"""

import asyncio
import os
import sys
from pathlib import Path

from loguru import logger

from core.config.loader import load_config
from core.config.schema import AppConfig
from core.logging import setup_logging
from market_data.orchestration.flows.batch_flow import market_data_flow
from market_data.batch.storage.snapshot import SnapshotManager
from market_data.batch.storage.gold_storage import GoldStorage
from services.observability.metrics import push_metrics


async def _run_flow_local(config: AppConfig) -> None:
    """
    Ejecuta market_data_flow en modo local.
    SafeOps: snapshot y gold build no interrumpen el flujo.
    """
    env        = os.getenv("OCM_ENV", "development")
    config_dir = str(Path("config").resolve())

    logger.info(
        "Launching market_data_flow (local) | env={} config_dir={}",
        env, config_dir,
    )

    await market_data_flow(env=env, config_dir=config_dir)
    logger.info("market_data_flow completed")

    try:
        snapshot_id = SnapshotManager().create_snapshot()
        logger.info("Snapshot created | id={}", snapshot_id)
    except Exception as exc:
        logger.warning("Snapshot creation failed (non-critical) | {}", exc)

    try:
        gold = GoldStorage()
        for ex in config.exchanges:
            if ex.has_spot and ex.markets.spot_symbols:
                gold.build_all(
                    exchange=ex.name.value,
                    symbols=ex.markets.spot_symbols,
                    market_type="spot",
                    timeframes=config.pipeline.historical.timeframes,
                )
            if ex.has_futures and ex.markets.futures_symbols:
                gold.build_all(
                    exchange=ex.name.value,
                    symbols=ex.markets.futures_symbols,
                    market_type="swap",
                    timeframes=config.pipeline.historical.timeframes,
                )
        logger.info("Gold build completed")
    except Exception as exc:
        logger.warning("Gold build failed (non-critical) | {}", exc)


def run(config: AppConfig, debug: bool = False) -> int:
    """
    Ejecuta el pipeline en modo local.
    Retorna int — el caller decide qué hacer con el exit code.

    Returns
    -------
    int
        0 → éxito, 1 → error crítico, 130 → interrumpido (SIGINT).
    """
    log_level = "DEBUG" if debug else config.observability.logging.level
    setup_logging(debug=(log_level == "DEBUG"))

    logger.info(
        "OrangeCashMachine starting (local) | env={} exchanges={}",
        os.getenv("OCM_ENV", "development"),
        config.exchange_names,
    )

    exit_code = 0
    try:
        asyncio.run(_run_flow_local(config))
    except KeyboardInterrupt:
        logger.warning("Execution interrupted by user")
        exit_code = 130
    except Exception:
        logger.exception("Fatal error in Market Data Flow")
        exit_code = 1
    finally:
        push_metrics()
        logger.info("Shutdown complete | exit_code={}", exit_code)

    return exit_code


if __name__ == "__main__":
    _env        = os.getenv("OCM_ENV", "development")
    _config_dir = os.getenv("OCM_CONFIG_DIR", "config")
    _debug      = os.getenv("OCM_DEBUG", "false").lower() in ("1", "true", "yes")

    _config = load_config(env=_env, path=Path(_config_dir))
    sys.exit(run(config=_config, debug=_debug))
