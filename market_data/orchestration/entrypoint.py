from __future__ import annotations

"""
orchestration/entrypoint.py (PRO+ refactor)

Entrypoint desacoplado, seguro y tipado para OrangeCashMachine.

Mejoras:
• Logging dinámico desde configuración
• Manejo robusto de excepciones y snapshots
• Integración tipada con AppConfig
• SafeOps reforzado, desacopla filesystem
• Principios SOLID, KISS, DRY
"""

import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import Optional

from loguru import logger

from core.config.loader import load_config, AppConfig
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
    """Intercepta logging estándar y lo redirige a loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = str(record.levelno)
        logger.opt(exception=record.exc_info).log(level, record.getMessage())

def setup_logging(level: str = "INFO", log_dir: Optional[Path] = LOG_DIR) -> None:
    """Configura loguru y logging estándar, rotación de logs opcional."""
    logger.remove()
    logger.add(sys.stderr, level=level, format=_LOG_FORMAT, colorize=True)

    if log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)
        logger.add(
            log_dir / "ocm_{time:YYYY-MM-DD}.log",
            rotation="1 day",
            retention="14 days",
            compression="gz",
            level=level,
        )

    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
    logger.debug("Logging initialized | level={}", level)

# -----------------------------------------------------------------------------
# Flow runner
# -----------------------------------------------------------------------------
async def _run_flow(config: AppConfig) -> None:
    """Ejecuta el flujo principal de datos de mercado con SafeOps para snapshots."""
    env = os.getenv("OCM_ENV", "development")
    logger.info("Launching market_data_flow | env={}", env)

    await market_data_flow(config=config)
    logger.info("market_data_flow completed")

    # SafeOps: snapshot no interrumpe flujo
    try:
        snapshot_id = SnapshotManager().create_snapshot()
        logger.info("Snapshot created | id={}", snapshot_id)
    except Exception as exc:
        logger.warning("Snapshot creation failed | {}", exc)

# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
def run(config: AppConfig, debug: bool = False) -> None:
    """Recibe configuración ya cargada y ejecuta el flujo principal."""
    # config viene inyectado desde main.py — no se recarga aquí (DIP)

    # --- Logging dinámico ---
    log_level = "DEBUG" if debug else config.observability.logging.level
    setup_logging(level=log_level)

    logger.info(
        "OrangeCashMachine starting | env={} exchanges={}",
        os.getenv("OCM_ENV", "development"),
        config.exchange_names,
    )

    # --- Ejecutar flujo ---
    try:
        asyncio.run(_run_flow(config))

    except KeyboardInterrupt:
        logger.warning("Execution interrupted by user")
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