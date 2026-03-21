"""
market_data/orchestration/entrypoint.py
========================================

Entrypoint LOCAL — solo para desarrollo y debug.

En producción el flow es disparado por Prefect Server/Worker
via deployment. Este archivo NO forma parte del path de producción.

Uso
---
    # Debug directo
    python -m market_data.orchestration.entrypoint

    # Via main.py (desarrollo)
    OCM_ENV=development python main.py

Principios
----------
SOLID  – SRP: solo orquesta el arranque local
KISS   – flujo lineal, sin lógica de negocio
DRY    – reutiliza load_config y market_data_flow
SafeOps – snapshots y métricas no interrumpen el flujo principal
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import Optional

from loguru import logger

from core.config.loader import load_config
from core.config.schema import AppConfig
from services.observability.metrics import push_metrics
from market_data.orchestration.flows.batch_flow import market_data_flow
from market_data.batch.storage.snapshot import SnapshotManager


# ==========================================================
# Constants
# ==========================================================

LOG_DIR = Path("logs")

_LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level:<8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{line}</cyan> | "
    "{message}"
)


# ==========================================================
# Logging
# ==========================================================

class InterceptHandler(logging.Handler):
    """Intercepta logging estándar y lo redirige a loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = str(record.levelno)
        logger.opt(exception=record.exc_info).log(level, record.getMessage())


def setup_logging(level: str = "INFO", log_dir: Optional[Path] = LOG_DIR) -> None:
    """
    Configura loguru como sistema de logging global.

    Si ya hay handlers activos (ej: main.py ya configuro loguru),
    solo instala el InterceptHandler para stdlib sin resetear handlers.
    Si se llama standalone, configura consola + archivo completo.
    """
    already_configured = len(logger._core.handlers) > 0

    if not already_configured:
        logger.remove()
        logger.add(sys.stderr, level=level, format=_LOG_FORMAT, colorize=True)
        if log_dir:
            log_dir.mkdir(parents=True, exist_ok=True)
            logger.add(
                log_dir / "ocm_{time:YYYY-MM-DD}.log",
                rotation="1 day",
                retention="14 days",
                compression="gz",
                level="DEBUG",  # archivo siempre DEBUG para trazabilidad completa
            )

    # Siempre instalar InterceptHandler para capturar stdlib logging (prefect, ccxt, etc)
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
    logger.debug("Logging initialized | level={}", level)


# ==========================================================
# Flow runner (local)
# ==========================================================

async def _run_flow_local(config: AppConfig) -> None:
    """
    Ejecuta market_data_flow en modo local.

    Pasa env y config_dir explícitamente para que el flow
    sea portable — no depende del filesystem implícito.

    SafeOps: snapshot post-ejecución no interrumpe el flujo.
    """
    env        = os.getenv("OCM_ENV", "development")
    config_dir = str(Path("config").resolve())

    logger.info("Launching market_data_flow (local) | env={} config_dir={}", env, config_dir)

    # Si PREFECT_API_URL está seteado, Prefect registra el run en el server
    await market_data_flow(env=env, config_dir=config_dir)

    logger.info("market_data_flow completed")

    # SafeOps: snapshot no crítico — fallo no interrumpe
    try:
        snapshot_id = SnapshotManager().create_snapshot()
        logger.info("Snapshot created | id={}", snapshot_id)
    except Exception as exc:
        logger.warning("Snapshot creation failed (non-critical) | {}", exc)


# ==========================================================
# Entrypoint público (llamado desde main.py)
# ==========================================================

def run(config: AppConfig, debug: bool = False) -> None:
    """
    Ejecuta el pipeline en modo local.
    Recibe AppConfig ya validada — no recarga configuración (DIP).

    Parámetros
    ----------
    config : AppConfig
        Configuración validada, inyectada desde main.py.
    debug  : bool
        Activa logging DEBUG si es True.
    """
    log_level = "DEBUG" if debug else config.observability.logging.level
    setup_logging(level=log_level)

    logger.info(
        "OrangeCashMachine starting (local) | env={} exchanges={}",
        os.getenv("OCM_ENV", "development"),
        config.exchange_names,
    )

    try:
        asyncio.run(_run_flow_local(config))

    except KeyboardInterrupt:
        logger.warning("Execution interrupted by user")
        sys.exit(0)

    except Exception:
        logger.exception("Fatal error in Market Data Flow")
        sys.exit(1)

    finally:
        push_metrics()
        logger.info("Shutdown complete")


# ==========================================================
# CLI directo (sin main.py — solo debug)
# ==========================================================

if __name__ == "__main__":
    _env        = os.getenv("OCM_ENV", "development")
    _config_dir = os.getenv("OCM_CONFIG_DIR", "config")
    _debug      = os.getenv("OCM_DEBUG", "false").lower() == "true"

    _config = load_config(env=_env, path=Path(_config_dir))
    run(config=_config, debug=_debug)
