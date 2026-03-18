from __future__ import annotations

"""
orchestration/entrypoint.py (PRO+)

Entrypoint desacoplado de archivos YAML y orientado a configuración tipada.

Mejoras:
• Integración con loader (AppConfig)
• Auto-detección de entorno (OCM_ENV)
• Logging dinámico desde config
• Flujo desacoplado de filesystem
• SafeOps mejorado

Principios:
SOLID · KISS · DRY · SafeOps
"""

import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import Optional

from loguru import logger

from core.config.loader import load_config
from services.observability.metrics import push_metrics
from market_data.orchestration.flows.batch_flow import market_data_flow
from market_data.batch.storage.snapshot import SnapshotManager


# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------

LOG_DIR = Path("logs")

_LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level:<8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{line}</cyan> | "
    "{message}"
)


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

class InterceptHandler(logging.Handler):

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = str(record.levelno)

        logger.opt(exception=record.exc_info).log(level, record.getMessage())


def setup_logging(level: str = "INFO", log_dir: Optional[Path] = LOG_DIR) -> None:

    logger.remove()

    logger.add(
        sys.stderr,
        level=level,
        format=_LOG_FORMAT,
        colorize=True,
    )

    if log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)

        logger.add(
            log_dir / "ocm_{time:YYYY-MM-DD}.log",
            rotation="1 day",
            retention="14 days",
            compression="gz",
            level=level,
        )

    intercept = InterceptHandler()
    logging.basicConfig(handlers=[intercept], level=0, force=True)

    logger.debug("Logging initialized | level={}", level)


# -----------------------------------------------------------------------------
# Flow runner
# -----------------------------------------------------------------------------

async def _run_flow(config) -> None:

    logger.info("Launching market_data_flow | env={}", os.getenv("OCM_ENV"))

    await market_data_flow(config=config)

    logger.info("market_data_flow completed")

    # Snapshot (SafeOps: no rompe ejecución)
    try:
        snapshot_id = SnapshotManager().create_snapshot()
        logger.info("Snapshot created | id={}", snapshot_id)
    except Exception as exc:
        logger.warning("Snapshot failed | {}", exc)


# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------

def run(
    env: Optional[str] = None,
    debug: bool = False,
) -> None:

    # 🔥 Carga configuración tipada
    config = load_config(env=env)

    # 🔥 Logging dinámico desde config
    log_level = "DEBUG" if debug else config.observability.logging.level

    setup_logging(level=log_level)

    logger.info(
        "OrangeCashMachine starting | env={} exchanges={}",
        env or os.getenv("OCM_ENV"),
        config.exchange_names,
    )

    try:
        asyncio.run(_run_flow(config))

    except KeyboardInterrupt:
        logger.warning("Execution interrupted")
        sys.exit(0)

    except Exception:
        logger.exception("Fatal error in Market Data Flow")
        sys.exit(1)

    finally:
        push_metrics()
        logger.info("Shutdown complete")


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

if __name__ == "__main__":

    run(
        env=os.getenv("OCM_ENV", "development"),
        debug=True,
    )