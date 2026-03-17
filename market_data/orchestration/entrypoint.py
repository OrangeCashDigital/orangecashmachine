"""
orchestration/entrypoint.py
===========================

Entrypoint operativo del sistema OrangeCashMachine.

Responsabilidad
---------------
Configurar el sistema de logging y lanzar el flow principal
de ingestión de datos de mercado.

Este archivo es el único punto del sistema que contiene
un bloque `if __name__ == "__main__"`.

Principios de ingeniería
------------------------
SOLID
    SRP – solo arranque del sistema
KISS
    sin abstracciones innecesarias
DRY
    configuración de logging centralizada
SafeOps
    manejo explícito de errores y códigos de salida
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional

from loguru import logger

from market_data.orchestration.flows.batch_flow import market_data_flow


# ==========================================================
# CONSTANTS
# ==========================================================

DEFAULT_CONFIG_PATH: Path = Path("config/settings.yaml")

LOG_DIR: Path = Path("logs")

_LOG_FORMAT_CONSOLE = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level:<8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{line}</cyan> | "
    "{message}"
)

_LOG_FORMAT_FILE = (
    "{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {name}:{line} | {message}"
)


# ==========================================================
# Loguru ↔ stdlib logging bridge
# ==========================================================

class InterceptHandler(logging.Handler):
    """
    Redirige logs del módulo `logging` hacia Loguru.

    Librerías externas (Prefect, CCXT, Pandas, etc.)
    usan `logging`. Este handler intercepta esos logs
    y los redirige al logger principal de Loguru.

    Referencia:
    https://loguru.readthedocs.io
    """

    def emit(self, record: logging.LogRecord) -> None:

        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = str(record.levelno)

        frame = logging.currentframe()
        depth = 2

        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(
            depth=depth,
            exception=record.exc_info,
        ).log(level, record.getMessage())


# ==========================================================
# Logging setup
# ==========================================================

def setup_logging(
    debug: bool = False,
    log_dir: Optional[Path] = LOG_DIR,
) -> None:
    """
    Configura Loguru como sistema global de logging.

    Incluye:
    - logs en consola con colores
    - logs rotativos en archivo
    - interceptación de logging estándar

    Parameters
    ----------
    debug : bool
        Activa nivel DEBUG.
    log_dir : Path | None
        Directorio de logs rotativos.
    """

    level = "DEBUG" if debug else "INFO"

    logger.remove()

    logger.add(
        sys.stderr,
        level=level,
        format=_LOG_FORMAT_CONSOLE,
        backtrace=True,
        diagnose=debug,
        colorize=True,
    )

    if log_dir:

        log_dir.mkdir(parents=True, exist_ok=True)

        logger.add(
            log_dir / "market_data_{time:YYYY-MM-DD}.log",
            rotation="1 day",
            retention="14 days",
            compression="gz",
            level=level,
            format=_LOG_FORMAT_FILE,
            encoding="utf-8",
            backtrace=True,
            diagnose=False,
        )

    intercept = InterceptHandler()

    logging.basicConfig(
        handlers=[intercept],
        level=0,
        force=True,
    )

    for name in ("prefect", "prefect.flow_runs", "prefect.task_runs"):

        prefect_logger = logging.getLogger(name)

        prefect_logger.handlers = [intercept]

        prefect_logger.propagate = False

    logger.debug("Logging configured | level={} log_dir={}", level, log_dir)


# ==========================================================
# Flow runner
# ==========================================================

async def _run_flow(config_path: Path) -> None:
    """
    Ejecuta el flow principal de ingestión.

    Parameters
    ----------
    config_path : Path
        Ruta al archivo de configuración.
    """

    logger.info(
        "Launching market_data_flow | config_path={}",
        config_path,
    )

    try:

        await market_data_flow(config_path=config_path)

    except Exception as exc:

        logger.exception(
            "market_data_flow failed | error={}",
            exc,
        )

        raise


# ==========================================================
# Public entrypoint
# ==========================================================

def run(
    debug: bool = False,
    config_path: Optional[str | Path] = None,
    log_dir: Optional[Path] = LOG_DIR,
) -> None:
    """
    Arranca el sistema OrangeCashMachine.

    Configura logging y ejecuta el flow principal.

    Parameters
    ----------
    debug : bool
        Activa logs DEBUG.
    config_path : str | Path | None
        Ruta al archivo de configuración.
    log_dir : Path | None
        Directorio de logs.
    """

    resolved_config = Path(config_path) if config_path else DEFAULT_CONFIG_PATH

    setup_logging(debug=debug, log_dir=log_dir)

    logger.info(
        "OrangeCashMachine starting | config={} debug={}",
        resolved_config,
        debug,
    )

    try:

        asyncio.run(_run_flow(resolved_config))

    except KeyboardInterrupt:

        logger.warning("Execution interrupted by user")

        sys.exit(0)

    except Exception:

        logger.error(
            "Market Data Flow terminated with errors. Check logs."
        )

        sys.exit(1)

    logger.info("OrangeCashMachine finished successfully.")


# ==========================================================
# CLI execution
# ==========================================================

if __name__ == "__main__":

    run(debug=True)

    # Producción (Prefect deployment)
    #
    # market_data_flow.serve(
    #     name="market-data-ingestion",
    #     cron="0 * * * *",
    #     parameters={"config_path": "config/settings.yaml"},
    # )