from __future__ import annotations

"""
market_data/orchestration/entrypoint.py
========================================

Entrypoint LOCAL — solo para desarrollo y debug.

En producción el flow es disparado por Prefect Server/Worker
via deployment. Este archivo NO forma parte del path de producción.
"""

import asyncio
import sys
from pathlib import Path

from loguru import logger

from core.config.runtime import RunConfig
from core.config.loader import load_config
from core.config.schema import AppConfig
from market_data.orchestration.flows.batch_flow import market_data_flow
from market_data.batch.storage.snapshot import SnapshotManager
from market_data.batch.storage.gold_storage import GoldStorage
from services.observability.metrics import push_metrics


async def _run_flow_local(config: AppConfig, run_cfg: RunConfig) -> None:
    config_dir = str(Path("config").resolve())

    logger.info(
        "Launching market_data_flow (local) | env={} config_dir={}",
        run_cfg.env, config_dir,
    )

    await market_data_flow(env=run_cfg.env, config_dir=config_dir)
    logger.info("market_data_flow completed")

    try:
        snapshot_id = SnapshotManager().create_snapshot()
        logger.info("Snapshot created | id={}", snapshot_id)
    except Exception as exc:
        logger.warning(
            "Snapshot creation failed (non-critical) | type={} error={}",
            type(exc).__name__, exc,
        )

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
        logger.warning(
            "Gold build failed (non-critical) | type={} error={}",
            type(exc).__name__, exc,
        )


def run(config: AppConfig, debug: bool = False) -> int:
    """
    Ejecuta el pipeline en modo local.

    Parameters
    ----------
    config : AppConfig
        Configuración de aplicación ya cargada y validada.
    debug : bool
        Flag de debug resuelto por RunConfig — no leer OCM_DEBUG aquí.

    Returns
    -------
    int
        0 → éxito, 1 → error crítico, 130 → interrumpido (SIGINT).

    Notes
    -----
    bootstrap_logging es idempotente: si ya fue llamado desde main.py
    no reconfigura sinks. _BOOTSTRAP_DONE garantiza una sola inicialización
    aunque run() se llame directamente.
    """
    # RunConfig mínimo para pasar env al flow — sin releer el entorno
    # porque debug ya viene resuelto desde el caller.
    run_cfg = RunConfig.from_env()

    logger.info(
        "OrangeCashMachine starting (local) | env={} exchanges={}",
        run_cfg.env,
        config.exchange_names,
    )

    timeout = config.pipeline.timeouts.historical_pipeline
    exit_code = 0
    try:
        asyncio.run(
            asyncio.wait_for(
                _run_flow_local(config, run_cfg),
                timeout=timeout,
            )
        )
    except asyncio.TimeoutError:
        logger.error("Pipeline timed out | timeout={}s", timeout)
        exit_code = 1
    except KeyboardInterrupt:
        logger.warning("Execution interrupted by user (SIGINT)")
        exit_code = 130
    except Exception as exc:
        logger.exception(
            "Fatal error in Market Data Flow | type={} error={}",
            type(exc).__name__, exc,
        )
        exit_code = 1
    finally:
        # No pushear métricas si el usuario interrumpió (SIGINT):
        # el run estaba incompleto y las métricas parciales contaminarían
        # el Pushgateway con datos de un ciclo que no terminó.
        if exit_code != 130:
            for ex in config.exchanges:
                push_metrics(exchange=ex.name.value, gateway=run_cfg.pushgateway)
        else:
            logger.debug("Shutdown by SIGINT — metrics push skipped")
        logger.info("Shutdown complete | exit_code={}", exit_code)

    return exit_code


if __name__ == "__main__":
    from core.logging import bootstrap_logging, configure_logging
    _run_cfg = RunConfig.from_env()
    _config  = load_config(env=_run_cfg.env, path=_run_cfg.config_path)
    bootstrap_logging(debug=_run_cfg.debug, env=_run_cfg.env)
    configure_logging(cfg=_config.observability.logging, env=_run_cfg.env, debug=_run_cfg.debug)
    sys.exit(run(config=_config, debug=_run_cfg.debug))
